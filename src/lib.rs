pub mod stream;

pub use stream::StreamConfig;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use anyhow::{Result, Context, anyhow};
use redb::{Database, TableDefinition, ReadableTable};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use serde::{Serialize, Deserialize};
use iroh::{Endpoint, EndpointAddr};
use iroh::endpoint::{Connection, RecvStream};
use iroh::protocol::{ProtocolHandler, AcceptError, Router};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use futures_util::{StreamExt, SinkExt};

pub const KNOT_ALPN: &[u8] = b"jitpomi/studio/1";

use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};

pub fn base64_url_decode(s: &str) -> Result<Vec<u8>, String> {
    BASE64_URL_SAFE_NO_PAD.decode(s).map_err(|e| e.to_string())
}

pub fn unpack_addr(bytes: &[u8]) -> Result<EndpointAddr, String> {
    if bytes.is_empty() || bytes[0] != 1 {
        return Err("invalid version".to_string());
    }
    if bytes.len() < 33 {
        return Err("data too short".to_string());
    }
    let node_id_bytes: [u8; 32] = bytes[1..33].try_into().map_err(|_| "failed to read node id".to_string())?;
    let node_id = iroh::PublicKey::from_bytes(&node_id_bytes).map_err(|e| e.to_string())?;
    
    let addrs = if bytes.len() > 33 {
        serde_json::from_slice(&bytes[33..]).map_err(|e| e.to_string())?
    } else {
        std::collections::BTreeSet::new()
    };
    
    Ok(EndpointAddr {
        id: node_id,
        addrs,
    })
}

pub async fn bind_endpoint() -> Result<Endpoint> {
    let transport_config = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(std::time::Duration::from_secs(4))
        .max_idle_timeout(Some(std::time::Duration::from_secs(12).try_into().unwrap()))
        .build();
    
    let endpoint = Endpoint::builder(iroh::endpoint::presets::N0)
        .transport_config(transport_config)
        .bind()
        .await?;
    Ok(endpoint)
}

static CONNECTION_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    Handshake {
        knot_id: String,
        display_name: String,
        rope_type: String,
        session_id: String,
        metadata: String,
    },
    HandshakeResponse {
        approved: bool,
        assigned_rope_id: String,
        metadata: String,
    },
    Ping {
        client_timestamp: u64,
    },
    Pong {
        client_timestamp: u64,
        server_timestamp: u64,
    },
    Event {
        variant: String,
        data: String,
    },
    BinaryEvent {
        variant: String,
        metadata: String,
        payload: Vec<u8>,
    },
    KnotEvent {
        rope_id: String,
        variant: String,
        data: String,
    },
    KnotBinaryEvent {
        rope_id: String,
        variant: String,
        metadata: String,
        payload: Vec<u8>,
    },
}

#[derive(Clone)]
pub struct KnotProtocol {
    pub(crate) data_dir: PathBuf,
    pub(crate) event_tx: UnboundedSender<HubEvent>,
    pub(crate) metadata_fn: Arc<dyn Fn() -> String + Send + Sync + 'static>,
}

impl std::fmt::Debug for KnotProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KnotProtocol")
            .field("data_dir", &self.data_dir)
            .finish()
    }
}

impl ProtocolHandler for KnotProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let data_dir = self.data_dir.clone();
        let event_tx = self.event_tx.clone();
        let metadata_fn = self.metadata_fn.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(connection, data_dir, event_tx, metadata_fn).await {
                eprintln!("Error handling connection: {:?}", e);
            }
        });
        Ok(())
    }
}

async fn handle_connection(
    connection: Connection,
    data_dir: PathBuf,
    event_tx: UnboundedSender<HubEvent>,
    metadata_fn: Arc<dyn Fn() -> String + Send + Sync + 'static>,
) -> Result<()> {
    // 1. Accept the bidirectional control stream
    let (send_stream, recv_stream) = connection.accept_bi().await
        .context("failed to accept bidirectional stream")?;

    let mut framed_read = FramedRead::new(recv_stream, LengthDelimitedCodec::new());
    let mut framed_write = FramedWrite::new(send_stream, LengthDelimitedCodec::new());

    // 2. Read the handshake request
    let payload = framed_read.next().await
        .ok_or_else(|| anyhow!("connection closed before handshake"))?
        .context("failed to read handshake frame")?;

    let msg: ControlMessage = bincode::deserialize(&payload)
        .context("failed to parse handshake request")?;

    let (knot_id, display_name, rope_type, session_id, metadata) = match msg {
        ControlMessage::Handshake { knot_id, display_name, rope_type, session_id, metadata } => {
            (knot_id, display_name, rope_type, session_id, metadata)
        }
        _ => {
            return Err(anyhow!("expected handshake message as the first packet"));
        }
    };

    // Generate a unique rope ID for this connection
    let conn_id = CONNECTION_COUNTER.fetch_add(1, Ordering::SeqCst);
    let rope_id = format!("{}_{}_{}", knot_id, rope_type.to_lowercase().replace(' ', "_"), conn_id);

    println!("Accepted connection from knot: {} ({}) on {} (Rope ID: {})", 
             display_name, knot_id, rope_type, rope_id);

    // 3. Send the handshake response
    let response = ControlMessage::HandshakeResponse {
        approved: true,
        assigned_rope_id: rope_id.clone(),
        metadata: (metadata_fn)(),
    };
    let response_bytes = bincode::serialize(&response)
        .map_err(|e| anyhow!("failed to serialize handshake response: {}", e))?;
    framed_write.send(bytes::Bytes::from(response_bytes)).await?;

    // Spawn a control channel writer task for the host to send messages back to client
    let (tx, mut rx) = unbounded_channel::<ControlMessage>();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match bincode::serialize(&msg) {
                Ok(bytes) => {
                    if framed_write.send(bytes::Bytes::from(bytes)).await.is_err() { break; }
                }
                Err(e) => {
                    eprintln!("failed to serialize control message: {:?}", e);
                }
            }
        }
    });

    // Register active rope
    let _ = event_tx.send(HubEvent::RopeConnected {
        rope_id: rope_id.clone(),
        knot_id: knot_id.clone(),
        display_name: display_name.clone(),
        rope_type: rope_type.clone(),
        metadata,
        control_sender: tx.clone(),
    });

    // Keep referencing details for subsequent streams
    let rope_dir_name = format!("{}_{}", display_name, rope_type);
    let session_dir = data_dir.join(&session_id).join(rope_dir_name);

    tokio::fs::create_dir_all(&session_dir).await
        .context("failed to create rope session directory")?;

    let disconnected_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Spawn active control reader task to read incoming commands (like Pings)
    let tx_clone = tx.clone();
    let rope_id_clone = rope_id.clone();
    let event_tx_clone = event_tx.clone();
    let flag_reader = disconnected_flag.clone();
    tokio::spawn(async move {
        while let Some(frame_result) = framed_read.next().await {
            let payload = match frame_result {
                Ok(p) => p,
                Err(_) => break,
            };

            match bincode::deserialize::<ControlMessage>(&payload) {
                Ok(ControlMessage::Ping { client_timestamp }) => {
                    let pong = ControlMessage::Pong {
                        client_timestamp,
                        server_timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    };
                    let _ = tx_clone.send(pong);
                }
                Ok(ControlMessage::Event { variant, data }) => {
                    let _ = event_tx_clone.send(HubEvent::EventReceived {
                        rope_id: rope_id_clone.clone(),
                        variant,
                        data,
                    });
                }
                Ok(other) => {
                    println!("Host received control message from rope: {:?}", other);
                }
                Err(e) => {
                    eprintln!("Host failed to parse control message: {:?}", e);
                }
            }
        }

        // Clean up when rope disconnects
        if !flag_reader.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let _ = event_tx_clone.send(HubEvent::RopeDisconnected { rope_id: rope_id_clone });
        }
    });

    // 4. Accept streams loop
    loop {
        match connection.accept_uni().await {
            Ok(recv) => {
                let session_dir = session_dir.clone();
                let event_tx = event_tx.clone();
                let rope_id = rope_id.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_stream(recv, session_dir, event_tx, rope_id).await {
                        eprintln!("Error handling stream: {:?}", e);
                    }
                });
            }
            Err(e) => {
                println!("Connection closed for knot {}: {:?}", display_name, e);
                break;
            }
        }
    }

    Ok(())
}

pub const FRAMES: TableDefinition<[u8; 32], &[u8]> = TableDefinition::new("frames");
pub const TIMELINE: TableDefinition<u64, [u8; 32]> = TableDefinition::new("timeline");

pub struct DeduplicatedRecordingWriter {
    db: Database,
}

impl DeduplicatedRecordingWriter {
    pub fn new(path: &std::path::Path) -> Result<Self> {
        let db = Database::create(path).context("failed to create redb database")?;
        let write_txn = db.begin_write().context("failed to begin write transaction for schema")?;
        {
            let _ = write_txn.open_table(FRAMES).context("failed to create frames table")?;
            let _ = write_txn.open_table(TIMELINE).context("failed to create timeline table")?;
        }
        write_txn.commit().context("failed to commit schema transaction")?;
        Ok(Self { db })
    }

    pub fn write_frame(&self, timestamp_ms: u64, payload: &[u8]) -> Result<()> {
        let hash = blake3::hash(payload).into();
        let write_txn = self.db.begin_write().context("failed to begin write transaction for frame")?;
        {
            let mut table_frames = write_txn.open_table(FRAMES).context("failed to open frames table")?;
            if table_frames.get(&hash).context("failed to get frame hash")?.is_none() {
                table_frames.insert(&hash, payload).context("failed to insert frame payload")?;
            }
            let mut table_timeline = write_txn.open_table(TIMELINE).context("failed to open timeline table")?;
            table_timeline.insert(&timestamp_ms, &hash).context("failed to insert timeline entry")?;
        }
        write_txn.commit().context("failed to commit frame transaction")?;
        Ok(())
    }
}

async fn handle_stream(
    stream: RecvStream,
    session_dir: PathBuf,
    event_tx: UnboundedSender<HubEvent>,
    rope_id: String,
) -> Result<()> {
    let mut framed_read = FramedRead::new(stream, LengthDelimitedCodec::new());

    // 1. Read config payload
    let config_payload = framed_read.next().await
        .ok_or_else(|| anyhow!("stream closed before config header"))?
        .context("failed to read stream config frame")?;

    let config: StreamConfig = serde_json::from_slice(&config_payload)
        .context("failed to parse stream config header")?;

    let stream_id = config.stream_id.clone().unwrap_or_else(|| "1".to_string());

    // 2. Open output database
    let filename = format!("{}.redb", config.sanitized_name());
    let filepath = session_dir.join(filename);
    let writer = DeduplicatedRecordingWriter::new(&filepath)?;

    println!("Starting stream recording for {} ({:?}) to {:?}", 
             config.name, config.source_type, filepath);

    // Notify stream configured
    let _ = event_tx.send(HubEvent::RopeStreamConfigured {
        rope_id: rope_id.clone(),
        stream_id: stream_id.clone(),
        config: config.clone(),
    });

    // 3. Frame reading loop
    while let Some(frame_result) = framed_read.next().await {
        let frame = frame_result.context("failed to read frame")?;
        if frame.len() < 9 { continue; }
        
        let frame_type = frame[0];
        let mut ts_bytes = [0u8; 8];
        ts_bytes.copy_from_slice(&frame[1..9]);
        let timestamp_ms = u64::from_be_bytes(ts_bytes);
        let payload = frame[9..].to_vec();

        // Write to database
        writer.write_frame(timestamp_ms, &payload)?;
        
        // Notify frame received
        let _ = event_tx.send(HubEvent::FrameReceived {
            rope_id: rope_id.clone(),
            stream_id: stream_id.clone(),
            frame_type,
            timestamp_ms,
            payload,
        });
    }

    println!("Finished stream recording to {:?}", filepath);
    Ok(())
}

pub struct KnotClient {
    connection: Connection,
    control_tx: UnboundedSender<ControlMessage>,
    event_rx: UnboundedReceiver<ControlMessage>,
    rope_id: String,
    hub_metadata: String,
}

pub struct KnotStream {
    writer: FramedWrite<iroh::endpoint::SendStream, LengthDelimitedCodec>,
}

impl KnotClient {
    pub fn builder(endpoint: &Endpoint) -> KnotClientBuilder<'_> {
        KnotClientBuilder::new(endpoint)
    }

    pub async fn connect(
        endpoint: &Endpoint,
        hub_addr: EndpointAddr,
        knot_id: String,
        display_name: String,
        rope_type: String,
        session_id: String,
        metadata: String,
    ) -> Result<Self> {
        let connection = endpoint.connect(hub_addr, KNOT_ALPN).await
            .context("Failed to connect to Hub")?;
            
        let (send, recv) = connection.open_bi().await
            .context("Failed to open control stream")?;
            
        let mut framed_read = FramedRead::new(recv, LengthDelimitedCodec::new());
        let mut framed_write = FramedWrite::new(send, LengthDelimitedCodec::new());

        // Send handshake
        let handshake = ControlMessage::Handshake {
            knot_id,
            display_name,
            rope_type,
            session_id,
            metadata,
        };
        let handshake_bytes = bincode::serialize(&handshake)?;
        framed_write.send(bytes::Bytes::from(handshake_bytes)).await?;

        // Read handshake response
        let resp_payload = framed_read.next().await
            .ok_or_else(|| anyhow!("Connection closed before handshake response"))??;
        let resp: ControlMessage = bincode::deserialize(&resp_payload)?;
        
        let (approved, assigned_rope_id, hub_metadata) = match resp {
            ControlMessage::HandshakeResponse { approved, assigned_rope_id, metadata } => {
                (approved, assigned_rope_id, metadata)
            }
            _ => return Err(anyhow!("Expected HandshakeResponse from Hub")),
        };

        if !approved {
            return Err(anyhow!("Handshake rejected by Hub"));
        }

        // Spawn background reader loop to process incoming control messages and handle ping/pongs
        let (event_tx, event_rx) = unbounded_channel::<ControlMessage>();
        let (control_tx, mut control_rx) = unbounded_channel::<ControlMessage>();
        
        // Spawn sender task
        tokio::spawn(async move {
            while let Some(msg) = control_rx.recv().await {
                if let Ok(bytes) = bincode::serialize(&msg) {
                    if framed_write.send(bytes::Bytes::from(bytes)).await.is_err() {
                        break;
                    }
                }
            }
        });

        // Spawn reader task
        let control_tx_clone = control_tx.clone();
        tokio::spawn(async move {
            while let Some(frame_result) = framed_read.next().await {
                let payload = match frame_result {
                    Ok(p) => p,
                    Err(_) => break,
                };

                match bincode::deserialize::<ControlMessage>(&payload) {
                    Ok(ControlMessage::Ping { client_timestamp }) => {
                        let pong = ControlMessage::Pong {
                            client_timestamp,
                            server_timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64,
                        };
                        let _ = control_tx_clone.send(pong);
                    }
                    Ok(ControlMessage::Pong { .. }) => {}
                    Ok(event) => {
                        let _ = event_tx.send(event);
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(Self {
            connection,
            control_tx,
            event_rx,
            rope_id: assigned_rope_id,
            hub_metadata,
        })
    }

    pub fn rope_id(&self) -> &str {
        &self.rope_id
    }

    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    pub fn control_tx(&self) -> UnboundedSender<ControlMessage> {
        self.control_tx.clone()
    }

    pub fn hub_metadata(&self) -> &str {
        &self.hub_metadata
    }

    pub async fn next_event(&mut self) -> Option<ControlMessage> {
        self.event_rx.recv().await
    }

    pub fn send_event(&self, variant: String, data: String) -> Result<()> {
        let msg = ControlMessage::Event { variant, data };
        self.control_tx.send(msg)
            .map_err(|_| anyhow!("Control stream is closed"))
    }

    pub async fn create_stream(
        &self,
        stream_id: String,
        source_type: String,
        name: String,
        metadata: String
    ) -> Result<KnotStream> {
        let send_stream = self.connection.open_uni().await?;
        let mut writer = FramedWrite::new(send_stream, LengthDelimitedCodec::new());

        // Write StreamConfig header
        let config = StreamConfig {
            stream_id: Some(stream_id),
            source_type,
            name,
            metadata,
        };
        let config_bytes = serde_json::to_vec(&config)?;
        writer.send(bytes::Bytes::from(config_bytes)).await?;

        Ok(KnotStream { writer })
    }
}

impl KnotStream {
    pub async fn write_frame(&mut self, frame_type: u8, timestamp_ms: u64, payload: &[u8]) -> Result<()> {
        let mut frame = Vec::with_capacity(payload.len() + 9);
        frame.push(frame_type);
        frame.extend_from_slice(&timestamp_ms.to_be_bytes());
        frame.extend_from_slice(payload);
        self.writer.send(bytes::Bytes::from(frame)).await?;
        Ok(())
    }
}

pub struct KnotClientBuilder<'a> {
    endpoint: &'a Endpoint,
    hub_addr: Option<EndpointAddr>,
    hub_ticket: Option<String>,
    knot_id: String,
    display_name: String,
    rope_type: String,
    session_id: Option<String>,
    metadata: Option<String>,
}

impl<'a> KnotClientBuilder<'a> {
    pub fn new(endpoint: &'a Endpoint) -> Self {
        Self {
            endpoint,
            hub_addr: None,
            hub_ticket: None,
            knot_id: String::new(),
            display_name: String::new(),
            rope_type: String::new(),
            session_id: None,
            metadata: None,
        }
    }

    pub fn hub_addr(mut self, hub_addr: EndpointAddr) -> Self {
        self.hub_addr = Some(hub_addr);
        self
    }

    pub fn hub_ticket(mut self, ticket: impl Into<String>) -> Self {
        self.hub_ticket = Some(ticket.into());
        self
    }

    pub fn knot_id(mut self, knot_id: impl Into<String>) -> Self {
        self.knot_id = knot_id.into();
        self
    }

    pub fn display_name(mut self, display_name: impl Into<String>) -> Self {
        self.display_name = display_name.into();
        self
    }

    pub fn rope_type(mut self, rope_type: impl Into<String>) -> Self {
        self.rope_type = rope_type.into();
        self
    }

    pub fn session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    pub fn metadata(mut self, metadata: impl Into<String>) -> Self {
        self.metadata = Some(metadata.into());
        self
    }

    pub async fn connect(self) -> Result<KnotClient> {
        if self.knot_id.is_empty() {
            return Err(anyhow!("knot_id is required"));
        }
        if self.display_name.is_empty() {
            return Err(anyhow!("display_name is required"));
        }
        if self.rope_type.is_empty() {
            return Err(anyhow!("rope_type is required"));
        }

        let hub_addr = match (self.hub_addr, self.hub_ticket) {
            (Some(addr), _) => addr,
            (None, Some(ticket)) => {
                let decoded = base64_url_decode(&ticket)
                    .map_err(|e| anyhow!("invalid ticket encoding: {}", e))?;
                unpack_addr(&decoded)
                    .map_err(|e| anyhow!("failed to unpack ticket: {}", e))?
            }
            (None, None) => return Err(anyhow!("hub_addr or hub_ticket is required")),
        };

        let session_id = self.session_id.unwrap_or_else(|| "default_session".to_string());
        let metadata = self.metadata.unwrap_or_else(|| "{}".to_string());

        KnotClient::connect(
            self.endpoint,
            hub_addr,
            self.knot_id,
            self.display_name,
            self.rope_type,
            session_id,
            metadata,
        ).await
    }
}

#[derive(Debug, Clone)]
pub enum HubEvent {
    RopeConnected {
        rope_id: String,
        knot_id: String,
        display_name: String,
        rope_type: String,
        metadata: String,
        control_sender: UnboundedSender<ControlMessage>,
    },
    RopeDisconnected {
        rope_id: String,
    },
    RopeStreamConfigured {
        rope_id: String,
        stream_id: String,
        config: StreamConfig,
    },
    FrameReceived {
        rope_id: String,
        stream_id: String,
        frame_type: u8,
        timestamp_ms: u64,
        payload: Vec<u8>,
    },
    EventReceived {
        rope_id: String,
        variant: String,
        data: String,
    },
}

#[derive(Debug)]
pub struct KnotHub {
    router: Router,
}

impl KnotHub {
    pub async fn spawn<F>(endpoint: Endpoint, data_dir: PathBuf, metadata_fn: F) -> Result<(Self, UnboundedReceiver<HubEvent>)>
    where
        F: Fn() -> String + Send + Sync + 'static,
    {
        let (tx, rx) = unbounded_channel();
        let protocol = KnotProtocol {
            data_dir,
            event_tx: tx,
            metadata_fn: Arc::new(metadata_fn),
        };
        let router = Router::builder(endpoint)
            .accept(KNOT_ALPN.to_vec(), Arc::new(protocol))
            .spawn();
        Ok((Self { router }, rx))
    }

    pub async fn shutdown(self) -> Result<()> {
        self.router.shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableTableMetadata;

    #[test]
    fn test_stream_config_sanitization() {
        let config = StreamConfig {
            stream_id: None,
            source_type: "screen".to_string(),
            name: "LG UltraWide Display & Screen!".to_string(),
            metadata: "{}".to_string(),
        };
        assert_eq!(config.sanitized_name(), "lg_ultrawide_display_screen");
    }

    #[test]
    fn test_control_message_handshake_serialization() {
        let msg = ControlMessage::Handshake {
            knot_id: "p1".to_string(),
            display_name: "Alice".to_string(),
            rope_type: "Mac".to_string(),
            session_id: "s1".to_string(),
            metadata: "{}".to_string(),
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let parsed: ControlMessage = bincode::deserialize(&bytes).unwrap();
        match parsed {
            ControlMessage::Handshake { knot_id, display_name, rope_type, session_id, metadata } => {
                assert_eq!(knot_id, "p1");
                assert_eq!(display_name, "Alice");
                assert_eq!(rope_type, "Mac");
                assert_eq!(session_id, "s1");
                assert_eq!(metadata, "{}");
            }
            _ => panic!("Expected Handshake variant"),
        }
    }

    #[test]
    fn test_control_message_custom_variants() {
        // Test Event
        let msg_event = ControlMessage::Event { variant: "video_state".to_string(), data: "{\"on\":true}".to_string() };
        let bytes = bincode::serialize(&msg_event).unwrap();
        let parsed = bincode::deserialize::<ControlMessage>(&bytes).unwrap();
        match parsed {
            ControlMessage::Event { variant, data } => {
                assert_eq!(variant, "video_state");
                assert_eq!(data, "{\"on\":true}");
            }
            _ => panic!("Expected Event"),
        }

        // Test KnotEvent
        let msg_part_event = ControlMessage::KnotEvent {
            rope_id: "alice".to_string(),
            variant: "connected".to_string(),
            data: "{}".to_string(),
        };
        let bytes = bincode::serialize(&msg_part_event).unwrap();
        let parsed = bincode::deserialize::<ControlMessage>(&bytes).unwrap();
        match parsed {
            ControlMessage::KnotEvent { rope_id, variant, data } => {
                assert_eq!(rope_id, "alice");
                assert_eq!(variant, "connected");
                assert_eq!(data, "{}");
            }
            _ => panic!("Expected KnotEvent"),
        }

        // Test BinaryEvent
        let msg_binary = ControlMessage::BinaryEvent {
            variant: "host_frame".to_string(),
            metadata: "{}".to_string(),
            payload: vec![1, 2, 3],
        };
        let bytes = bincode::serialize(&msg_binary).unwrap();
        let parsed = bincode::deserialize::<ControlMessage>(&bytes).unwrap();
        match parsed {
            ControlMessage::BinaryEvent { variant, metadata, payload } => {
                assert_eq!(variant, "host_frame");
                assert_eq!(metadata, "{}");
                assert_eq!(payload, vec![1, 2, 3]);
            }
            _ => panic!("Expected BinaryEvent"),
        }

        // Test KnotBinaryEvent
        let msg_part_binary = ControlMessage::KnotBinaryEvent {
            rope_id: "bob".to_string(),
            variant: "client_frame".to_string(),
            metadata: "{}".to_string(),
            payload: vec![4, 5, 6],
        };
        let bytes = bincode::serialize(&msg_part_binary).unwrap();
        let parsed = bincode::deserialize::<ControlMessage>(&bytes).unwrap();
        match parsed {
            ControlMessage::KnotBinaryEvent { rope_id, variant, metadata, payload } => {
                assert_eq!(rope_id, "bob");
                assert_eq!(variant, "client_frame");
                assert_eq!(metadata, "{}");
                assert_eq!(payload, vec![4, 5, 6]);
            }
            _ => panic!("Expected KnotBinaryEvent"),
        }
    }

    #[test]
    fn test_deduplicated_recording() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join("test_recording.redb");
        if db_path.exists() {
            let _ = std::fs::remove_file(&db_path);
        }

        let frame_a = b"frame_data_payload_A";
        let frame_b = b"frame_data_payload_B";

        {
            let writer = DeduplicatedRecordingWriter::new(&db_path).unwrap();

            writer.write_frame(1000, frame_a).unwrap();
            writer.write_frame(2000, frame_b).unwrap();
            writer.write_frame(3000, frame_a).unwrap();
        }

        let db = Database::open(&db_path).unwrap();
        let read_txn = db.begin_read().unwrap();
        let frames_table = read_txn.open_table(FRAMES).unwrap();
        let timeline_table = read_txn.open_table(TIMELINE).unwrap();

        assert_eq!(frames_table.len().unwrap(), 2);
        assert_eq!(timeline_table.len().unwrap(), 3);

        let output_h264_path = temp_dir.join("test_export.h264");
        if output_h264_path.exists() {
            let _ = std::fs::remove_file(&output_h264_path);
        }

        let mut output_file = std::fs::File::create(&output_h264_path).unwrap();
        let iter = timeline_table.iter().unwrap();
        for entry_result in iter {
            let entry = entry_result.unwrap();
            let hash = entry.1.value();
            let payload = frames_table.get(&hash).unwrap().unwrap();
            use std::io::Write;
            output_file.write_all(payload.value()).unwrap();
        }
        use std::io::Write;
        output_file.flush().unwrap();

        let exported_bytes = std::fs::read(&output_h264_path).unwrap();
        let expected_bytes = [&frame_a[..], &frame_b[..], &frame_a[..]].concat();
        assert_eq!(exported_bytes, expected_bytes);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(&output_h264_path);
    }
}
