pub mod stream;
pub mod mock;

pub use stream::{StreamConfig, Capability, FrameHeader, Direction, AttributeSchema};

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use anyhow::{Result, Context, anyhow};
use redb::{Database, TableDefinition, ReadableTable};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use serde::{Serialize, Deserialize};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use futures_util::{StreamExt, SinkExt};

static CONNECTION_COUNTER: AtomicUsize = AtomicUsize::new(0);
static MSG_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn generate_msg_id() -> String {
    let count = MSG_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}", now_ms(), count)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ErrorCode {
    InvalidToken,
    DuplicateRopeId,
    UnsupportedCapability,
    UnauthorizedCommand,
    StreamRejected,
    ProtocolVersionMismatch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub msg_id: String,
    pub timestamp: u64,
    pub source_rope_id: String,
    pub connection_id: String,
    pub requires_ack: bool,
    pub payload: ControlMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    Tie {
        protocol_version: u32,
        knot_id: String,
        rope_id: String,
        node_id: String,
        join_token: String,
        capabilities: Vec<Capability>,
    },
    Welcome {
        connection_id: String,
        assigned_rope_id: String,
        session_metadata: String,
    },
    Reject {
        reason: ErrorCode,
        details: String,
    },
    StreamOpen {
        stream_id: String,
        topic: String,
        config_payload: String,
    },
    StreamAccepted {
        stream_id: String,
    },
    StreamClosed {
        stream_id: String,
        reason: String,
    },
    Command {
        command_id: String,
        target_capability_id: String,
        action: String,
        payload: String,
    },
    Ack {
        correlation_id: String,
        status: String,
        result_payload: String,
    },
    Heartbeat,
    Error {
        code: ErrorCode,
        message: String,
    },
    Goodbye,
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
}

#[derive(Clone)]
pub enum JoinPolicy {
    ApproveAll,
    TokenRequired { secret: String },
    Custom(Arc<dyn Fn(&str, &str, &[Capability]) -> Result<(), ErrorCode> + Send + Sync + 'static>),
}

impl std::fmt::Debug for JoinPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApproveAll => write!(f, "ApproveAll"),
            Self::TokenRequired { .. } => write!(f, "TokenRequired {{ .. }}"),
            Self::Custom(_) => write!(f, "Custom(Fn)"),
        }
    }
}

#[async_trait::async_trait]
pub trait KnotConnection: Send + Sync + 'static {
    type SendStream: tokio::io::AsyncWrite + Send + Sync + Unpin + 'static;
    type RecvStream: tokio::io::AsyncRead + Send + Sync + Unpin + 'static;

    async fn accept_bi(&self) -> Result<(Self::SendStream, Self::RecvStream)>;
    async fn accept_uni(&self) -> Result<Self::RecvStream>;
    async fn open_bi(&self) -> Result<(Self::SendStream, Self::RecvStream)>;
    async fn open_uni(&self) -> Result<Self::SendStream>;
    fn remote_node_id(&self) -> String;
    fn local_node_id(&self) -> String;
    async fn close(&self, _error_code: u32, _reason: &str) -> Result<()> {
        Ok(())
    }
}

pub async fn handle_connection<C: KnotConnection>(
    connection: C,
    data_dir: PathBuf,
    event_tx: UnboundedSender<HubEvent>,
    metadata_fn: Arc<dyn Fn() -> String + Send + Sync + 'static>,
    join_policy: JoinPolicy,
    cap_validator: Option<Arc<dyn Fn(&[Capability]) -> bool + Send + Sync + 'static>>,
) -> Result<()> {
    let remote_node_id_str = connection.remote_node_id();

    let (send_stream, recv_stream) = connection.accept_bi().await
        .context("failed to accept bidirectional stream")?;

    let mut framed_read = FramedRead::new(recv_stream, LengthDelimitedCodec::new());
    let mut framed_write = FramedWrite::new(send_stream, LengthDelimitedCodec::new());

    let payload = framed_read.next().await
        .ok_or_else(|| anyhow!("connection closed before handshake"))?
        .context("failed to read handshake frame")?;

    let envelope: Envelope = bincode::deserialize(&payload)
        .context("failed to parse join request envelope")?;

    // Rope is attempting to tie the knot with the logical Knot ID
    let (protocol_version, knot_id, rope_id, node_id, join_token, capabilities) = match envelope.payload {
        ControlMessage::Tie { protocol_version, knot_id, rope_id, node_id, join_token, capabilities } => {
            (protocol_version, knot_id, rope_id, node_id, join_token, capabilities)
        }
        _ => {
            return Err(anyhow!("expected Tie payload"));
        }
    };

    if node_id != remote_node_id_str {
        let reject_env = Envelope {
            msg_id: "reject-node".to_string(),
            timestamp: now_ms(),
            source_rope_id: "host".to_string(),
            connection_id: "pending".to_string(),
            requires_ack: false,
            payload: ControlMessage::Reject {
                reason: ErrorCode::InvalidToken,
                details: format!("Announced node_id {} does not match authenticated node_id {}", node_id, remote_node_id_str),
            },
        };
        let _ = framed_write.send(bytes::Bytes::from(bincode::serialize(&reject_env)?)).await;
        let _ = framed_write.close().await;
        let _ = connection.close(ErrorCode::InvalidToken as u32, "Handshake node ID mismatch").await;
        return Err(anyhow!("Handshake node ID mismatch"));
    }

    if protocol_version != 1 {
        let reject_env = Envelope {
            msg_id: "reject-ver".to_string(),
            timestamp: now_ms(),
            source_rope_id: "host".to_string(),
            connection_id: "pending".to_string(),
            requires_ack: false,
            payload: ControlMessage::Reject {
                reason: ErrorCode::ProtocolVersionMismatch,
                details: format!("Unsupported version: {}. Only version 1 is supported.", protocol_version),
            },
        };
        let _ = framed_write.send(bytes::Bytes::from(bincode::serialize(&reject_env)?)).await;
        let _ = framed_write.close().await;
        let _ = connection.close(ErrorCode::ProtocolVersionMismatch as u32, "Unsupported version").await;
        return Err(anyhow!("Unsupported version"));
    }

    match join_policy {
        JoinPolicy::TokenRequired { ref secret } => {
            if join_token != *secret {
                let reject_env = Envelope {
                    msg_id: "reject-tok".to_string(),
                    timestamp: now_ms(),
                    source_rope_id: "host".to_string(),
                    connection_id: "pending".to_string(),
                    requires_ack: false,
                    payload: ControlMessage::Reject {
                        reason: ErrorCode::InvalidToken,
                        details: "Invalid join token supplied".to_string(),
                    },
                };
                let _ = framed_write.send(bytes::Bytes::from(bincode::serialize(&reject_env)?)).await;
                let _ = framed_write.close().await;
                let _ = connection.close(ErrorCode::InvalidToken as u32, "Join token unauthorized").await;
                return Err(anyhow!("Join token unauthorized"));
            }
        }
        JoinPolicy::ApproveAll => {}
        JoinPolicy::Custom(ref validator) => {
            if let Err(err_code) = validator(&remote_node_id_str, &join_token, &capabilities) {
                let details = match err_code {
                    ErrorCode::InvalidToken => "Invalid join token validation failed".to_string(),
                    ErrorCode::UnsupportedCapability => "Capabilities failed validation check".to_string(),
                    _ => format!("Admission validation failed: {:?}", err_code),
                };
                let reject_env = Envelope {
                    msg_id: "reject-custom".to_string(),
                    timestamp: now_ms(),
                    source_rope_id: "host".to_string(),
                    connection_id: "pending".to_string(),
                    requires_ack: false,
                    payload: ControlMessage::Reject {
                        reason: err_code,
                        details,
                    },
                };
                let _ = framed_write.send(bytes::Bytes::from(bincode::serialize(&reject_env)?)).await;
                let _ = framed_write.close().await;
                let _ = connection.close(err_code as u32, "Admission validator rejected").await;
                return Err(anyhow!("Admission validator rejected"));
            }
        }
    }

    if let Some(ref validator) = cap_validator {
        if !validator(&capabilities) {
            let reject_env = Envelope {
                msg_id: "reject-caps".to_string(),
                timestamp: now_ms(),
                source_rope_id: "host".to_string(),
                connection_id: "pending".to_string(),
                requires_ack: false,
                payload: ControlMessage::Reject {
                    reason: ErrorCode::UnsupportedCapability,
                    details: "Capabilities failed validation check".to_string(),
                },
            };
            let _ = framed_write.send(bytes::Bytes::from(bincode::serialize(&reject_env)?)).await;
            let _ = framed_write.close().await;
            let _ = connection.close(ErrorCode::UnsupportedCapability as u32, "Capability validation failed").await;
            return Err(anyhow!("Capability validation failed"));
        }
    }

    let connection_id = format!("conn_{}", CONNECTION_COUNTER.fetch_add(1, Ordering::SeqCst));
    let assigned_rope_id = format!("{}_{}", knot_id, rope_id);
    let session_metadata = metadata_fn();

    let welcome_env = Envelope {
        msg_id: generate_msg_id(),
        timestamp: now_ms(),
        source_rope_id: "host".to_string(),
        connection_id: connection_id.clone(),
        requires_ack: false,
        payload: ControlMessage::Welcome {
            connection_id: connection_id.clone(),
            assigned_rope_id: assigned_rope_id.clone(),
            session_metadata,
        },
    };
    framed_write.send(bytes::Bytes::from(bincode::serialize(&welcome_env)?)).await?;

    let (control_sender_tx, mut control_sender_rx) = unbounded_channel::<Envelope>();
    tokio::spawn(async move {
        println!("DEBUG HOST: control_sender task started");
        while let Some(msg) = control_sender_rx.recv().await {
            if let Ok(bytes) = bincode::serialize(&msg) {
                if let Err(e) = framed_write.send(bytes::Bytes::from(bytes)).await {
                    println!("DEBUG HOST: control_sender send failed: {:?}", e);
                    break;
                }
            }
        }
        println!("DEBUG HOST: control_sender task exited because rx.recv() returned None");
    });

    let _ = event_tx.send(HubEvent::RopeConnected {
        rope_id: assigned_rope_id.clone(),
        knot_id: knot_id.clone(),
        node_id: remote_node_id_str,
        capabilities,
        control_sender: control_sender_tx.clone(),
    });

    let session_dir = data_dir.join(&knot_id);
    std::fs::create_dir_all(&session_dir).context("failed to create session directory")?;

    let flag_reader = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag_reader_clone = flag_reader.clone();
    let rope_id_clone_reader = assigned_rope_id.clone();
    let event_tx_clone_reader = event_tx.clone();
    let control_sender_tx_clone = control_sender_tx.clone();

    tokio::spawn(async move {
        while let Some(res) = framed_read.next().await {
            println!("DEBUG HOST: frame_result: {:?}", res);
            let data = match res {
                Ok(d) => d,
                Err(e) => {
                    println!("DEBUG HOST: frame read error: {:?}", e);
                    break;
                }
            };
            match bincode::deserialize::<Envelope>(&data) {
                Ok(env) => match env.payload {
                    ControlMessage::Ping { client_timestamp } => {
                        let pong = Envelope {
                            msg_id: format!("pong-{}", env.msg_id),
                            timestamp: now_ms(),
                            source_rope_id: "host".to_string(),
                            connection_id: env.connection_id.clone(),
                            requires_ack: false,
                            payload: ControlMessage::Pong {
                                client_timestamp,
                                server_timestamp: now_ms(),
                            },
                        };
                        let _ = control_sender_tx_clone.send(pong);
                    }
                    ControlMessage::StreamOpen { ref stream_id, ref topic, ref config_payload } => {
                        let _ = event_tx_clone_reader.send(HubEvent::StreamOpened {
                            rope_id: rope_id_clone_reader.clone(),
                            stream_id: stream_id.clone(),
                            topic: topic.clone(),
                            config_payload: config_payload.clone(),
                        });
                        let accept = Envelope {
                            msg_id: format!("accept-{}", stream_id),
                            timestamp: now_ms(),
                            source_rope_id: "host".to_string(),
                            connection_id: env.connection_id.clone(),
                            requires_ack: false,
                            payload: ControlMessage::StreamAccepted { stream_id: stream_id.clone() },
                        };
                        let _ = control_sender_tx_clone.send(accept);
                    }
                    ControlMessage::Event { variant, data } => {
                        let _ = event_tx_clone_reader.send(HubEvent::EventReceived {
                            rope_id: rope_id_clone_reader.clone(),
                            variant,
                            data,
                        });
                    }
                    _ => {}
                },
                Err(e) => {
                    eprintln!("Failed to parse control message: {:?}", e);
                }
            }
        }

        if !flag_reader_clone.swap(true, Ordering::SeqCst) {
            let _ = event_tx_clone_reader.send(HubEvent::RopeDisconnected { rope_id: rope_id_clone_reader });
        }
    });

    while let Ok(recv) = connection.accept_uni().await {
        let session_dir = session_dir.clone();
        let event_tx = event_tx.clone();
        let rope_id = assigned_rope_id.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_stream(recv, session_dir, event_tx, rope_id).await {
                eprintln!("Error handling stream: {:?}", e);
            }
        });
    }

    if !flag_reader.swap(true, Ordering::SeqCst) {
        let _ = event_tx.send(HubEvent::RopeDisconnected { rope_id: assigned_rope_id });
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
        let write_txn = db.begin_write().context("failed to begin write transaction")?;
        {
            let _ = write_txn.open_table(FRAMES).context("failed to create frames table")?;
            let _ = write_txn.open_table(TIMELINE).context("failed to create timeline table")?;
        }
        write_txn.commit().context("failed to commit transaction")?;
        Ok(Self { db })
    }

    pub fn write_frame(&self, timestamp_ms: u64, payload: &[u8]) -> Result<()> {
        let hash = blake3::hash(payload).into();
        let write_txn = self.db.begin_write().context("failed to begin write transaction")?;
        {
            let mut table_frames = write_txn.open_table(FRAMES).context("failed to open frames table")?;
            if table_frames.get(&hash).context("failed to get frame")?.is_none() {
                table_frames.insert(&hash, payload).context("failed to insert payload")?;
            }
            let mut table_timeline = write_txn.open_table(TIMELINE).context("failed to open timeline table")?;
            table_timeline.insert(&timestamp_ms, &hash).context("failed to insert timeline entry")?;
        }
        write_txn.commit().context("failed to commit frame transaction")?;
        Ok(())
    }
}

async fn handle_stream<R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static>(
    stream: R,
    session_dir: PathBuf,
    event_tx: UnboundedSender<HubEvent>,
    rope_id: String,
) -> Result<()> {
    let mut framed_read = FramedRead::new(stream, LengthDelimitedCodec::new());

    let config_payload = framed_read.next().await
        .ok_or_else(|| anyhow!("stream closed before config"))?
        .context("failed to read config")?;

    let config: StreamConfig = serde_json::from_slice(&config_payload)
        .context("failed to parse config")?;

    let stream_id = config.stream_id.clone().unwrap_or_else(|| "1".to_string());
    let filename = format!("{}.redb", config.sanitized_name());
    let filepath = session_dir.join(filename);
    let writer = DeduplicatedRecordingWriter::new(&filepath)?;

    while let Some(frame_result) = framed_read.next().await {
        let frame = frame_result.context("failed to read frame")?;
        if frame.len() < FrameHeader::SIZE { continue; }
        
        let header = FrameHeader::decode(&frame[..FrameHeader::SIZE])?;
        let payload = frame[FrameHeader::SIZE..].to_vec();

        writer.write_frame(header.timestamp_ms, &payload)?;
        
        let _ = event_tx.send(HubEvent::FrameReceived {
            rope_id: rope_id.clone(),
            stream_id: stream_id.clone(),
            header,
            payload,
        });
    }

    Ok(())
}

pub struct KnotClient<C: KnotConnection> {
    connection: C,
    control_tx: UnboundedSender<Envelope>,
    event_rx: tokio::sync::Mutex<UnboundedReceiver<Envelope>>,
    rope_id: String,
    connection_id: String,
    hub_metadata: String,
    pending_streams: Arc<std::sync::Mutex<std::collections::HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
}

pub struct KnotStream<W: tokio::io::AsyncWrite + Send + Sync + Unpin + 'static> {
    writer: FramedWrite<W, LengthDelimitedCodec>,
    stream_id_num: u32,
    seq_counter: u64,
    start_time: std::time::Instant,
}

impl<C: KnotConnection> KnotClient<C> {
    pub async fn tie_the_knot(
        connection: C,
        knot_id: String,
        rope_id: String,
        join_token: String,
        capabilities: Vec<Capability>,
    ) -> Result<Self> {
        let (send, recv) = connection.open_bi().await
            .context("Failed to open control stream")?;
            
        let mut framed_read = FramedRead::new(recv, LengthDelimitedCodec::new());
        let mut framed_write = FramedWrite::new(send, LengthDelimitedCodec::new());

        let node_id_str = connection.local_node_id();

        let join_env = Envelope {
            msg_id: "join-req".to_string(),
            timestamp: now_ms(),
            source_rope_id: rope_id.clone(),
            connection_id: "pending".to_string(),
            requires_ack: false,
            payload: ControlMessage::Tie {
                protocol_version: 1,
                knot_id,
                rope_id: rope_id.clone(),
                node_id: node_id_str,
                join_token,
                capabilities,
            },
        };
        let join_bytes = bincode::serialize(&join_env)?;
        framed_write.send(bytes::Bytes::from(join_bytes)).await?;

        let resp_payload = framed_read.next().await
            .ok_or_else(|| anyhow!("Connection closed before response"))??;
        let resp_env: Envelope = bincode::deserialize(&resp_payload)?;
        
        let (connection_id, assigned_rope_id, hub_metadata) = match resp_env.payload {
            ControlMessage::Welcome { connection_id, assigned_rope_id, session_metadata } => {
                (connection_id, assigned_rope_id, session_metadata)
            }
            ControlMessage::Reject { reason, details } => {
                return Err(anyhow!("Handshake rejected: {:?}. Details: {}", reason, details));
            }
            _ => return Err(anyhow!("Expected Welcome or Reject")),
        };

        let (event_tx, event_rx) = unbounded_channel::<Envelope>();
        let (control_tx, mut control_rx) = unbounded_channel::<Envelope>();
        
        tokio::spawn(async move {
            while let Some(msg) = control_rx.recv().await {
                if let Ok(bytes) = bincode::serialize(&msg) {
                    if framed_write.send(bytes::Bytes::from(bytes)).await.is_err() {
                        break;
                    }
                }
            }
        });

        let pending_streams = Arc::new(std::sync::Mutex::new(std::collections::HashMap::<String, tokio::sync::oneshot::Sender<()>>::new()));
        let pending_streams_clone = pending_streams.clone();

        let control_tx_clone = control_tx.clone();
        let assigned_rope_id_task = assigned_rope_id.clone();
        let connection_id_task = connection_id.clone();
        tokio::spawn(async move {
            while let Some(frame_result) = framed_read.next().await {
                println!("DEBUG CLIENT: frame_result: {:?}", frame_result);
                let payload = match frame_result {
                    Ok(p) => p,
                    Err(e) => {
                        println!("DEBUG CLIENT: frame read error: {:?}", e);
                        break;
                    }
                };

                match bincode::deserialize::<Envelope>(&payload) {
                    Ok(env) => match env.payload {
                        ControlMessage::StreamAccepted { ref stream_id } => {
                            let mut map = pending_streams_clone.lock().unwrap();
                            if let Some(tx) = map.remove(stream_id) {
                                let _ = tx.send(());
                            }
                            let _ = event_tx.send(env);
                        }
                        ControlMessage::Ping { client_timestamp } => {
                            let pong = Envelope {
                                msg_id: format!("pong-{}", env.msg_id),
                                timestamp: now_ms(),
                                source_rope_id: assigned_rope_id_task.clone(),
                                connection_id: connection_id_task.clone(),
                                requires_ack: false,
                                payload: ControlMessage::Pong {
                                    client_timestamp,
                                    server_timestamp: now_ms(),
                                },
                            };
                            let _ = control_tx_clone.send(pong);
                        }
                        ControlMessage::Pong { .. } => {}
                        _ => {
                            let _ = event_tx.send(env);
                        }
                    },
                    Err(e) => {
                        eprintln!("Client reader deserialization error: {:?}", e);
                        break;
                    }
                }
            }
            println!("DEBUG CLIENT: reader loop exited!");
        });

        Ok(Self {
            connection,
            control_tx,
            event_rx: tokio::sync::Mutex::new(event_rx),
            rope_id: assigned_rope_id,
            connection_id,
            hub_metadata,
            pending_streams,
        })
    }

    pub fn rope_id(&self) -> &str {
        &self.rope_id
    }

    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    pub fn connection(&self) -> &C {
        &self.connection
    }

    pub fn control_tx(&self) -> UnboundedSender<Envelope> {
        self.control_tx.clone()
    }

    pub fn hub_metadata(&self) -> &str {
        &self.hub_metadata
    }

    pub async fn next_event(&self) -> Option<Envelope> {
        let mut rx = self.event_rx.lock().await;
        rx.recv().await
    }

    pub fn send_event(&self, variant: String, data: String) -> Result<()> {
        let msg = Envelope {
            msg_id: generate_msg_id(),
            timestamp: now_ms(),
            source_rope_id: self.rope_id.clone(),
            connection_id: self.connection_id.clone(),
            requires_ack: false,
            payload: ControlMessage::Event { variant, data },
        };
        self.control_tx.send(msg).map_err(|_| anyhow!("Control stream is closed"))
    }

    pub fn send_ack(&self, correlation_id: String, status: String, result_payload: String) -> Result<()> {
        let msg = Envelope {
            msg_id: generate_msg_id(),
            timestamp: now_ms(),
            source_rope_id: self.rope_id.clone(),
            connection_id: self.connection_id.clone(),
            requires_ack: false,
            payload: ControlMessage::Ack { correlation_id, status, result_payload },
        };
        self.control_tx.send(msg).map_err(|_| anyhow!("Control stream is closed"))
    }

    pub async fn create_stream(
        &self,
        stream_id: String,
        capability_id: String,
        topic: String,
        format: String,
        attributes: std::collections::HashMap<String, String>
    ) -> Result<KnotStream<C::SendStream>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut map = self.pending_streams.lock().unwrap();
            map.insert(stream_id.clone(), tx);
        }

        let config = StreamConfig {
            stream_id: Some(stream_id.clone()),
            capability_id,
            topic: topic.clone(),
            format,
            attributes,
        };
        let config_bytes = serde_json::to_string(&config)?;

        let open_msg = Envelope {
            msg_id: generate_msg_id(),
            timestamp: now_ms(),
            source_rope_id: self.rope_id.clone(),
            connection_id: self.connection_id.clone(),
            requires_ack: true,
            payload: ControlMessage::StreamOpen {
                stream_id: stream_id.clone(),
                topic,
                config_payload: config_bytes,
            },
        };
        self.control_tx.send(open_msg)?;

        rx.await.context("failed to receive StreamAccepted response")?;

        let send_stream = self.connection.open_uni().await?;
        let mut writer = FramedWrite::new(send_stream, LengthDelimitedCodec::new());

        let config_bytes = serde_json::to_vec(&config)?;
        writer.send(bytes::Bytes::from(config_bytes)).await?;

        Ok(KnotStream {
            writer,
            stream_id_num: 1,
            seq_counter: 0,
            start_time: std::time::Instant::now(),
        })
    }
}

impl<W: tokio::io::AsyncWrite + Send + Sync + Unpin + 'static> KnotStream<W> {
    pub async fn write_frame(&mut self, frame_type: u8, _timestamp_ms: u64, payload: &[u8]) -> Result<()> {
        let header = FrameHeader {
            magic: [0x4B, 0x50],
            stream_id: self.stream_id_num,
            seq_num: self.seq_counter,
            timestamp_ms: self.start_time.elapsed().as_millis() as u64,
            frame_type,
            flags: 0,
            payload_len: payload.len() as u32,
        };
        self.seq_counter += 1;

        let mut frame = Vec::with_capacity(payload.len() + FrameHeader::SIZE);
        frame.extend_from_slice(&header.encode());
        frame.extend_from_slice(payload);
        self.writer.send(bytes::Bytes::from(frame)).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum HubEvent {
    RopeConnected {
        rope_id: String,
        knot_id: String,
        node_id: String,
        capabilities: Vec<Capability>,
        control_sender: UnboundedSender<Envelope>,
    },
    RopeDisconnected {
        rope_id: String,
    },
    StreamOpened {
        rope_id: String,
        stream_id: String,
        topic: String,
        config_payload: String,
    },
    FrameReceived {
        rope_id: String,
        stream_id: String,
        header: FrameHeader,
        payload: Vec<u8>,
    },
    EventReceived {
        rope_id: String,
        variant: String,
        data: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableTableMetadata;

    #[test]
    fn test_stream_config_sanitization() {
        let config = StreamConfig {
            stream_id: None,
            capability_id: "cam".to_string(),
            topic: "LG UltraWide Display & Screen!".to_string(),
            format: "h264".to_string(),
            attributes: std::collections::HashMap::new(),
        };
        assert_eq!(config.sanitized_name(), "lg_ultrawide_display_screen");
    }

    #[test]
    fn test_control_message_handshake_serialization() {
        let join = ControlMessage::Tie {
            protocol_version: 1,
            knot_id: "p1".to_string(),
            rope_id: "Alice".to_string(),
            node_id: "some_node".to_string(),
            join_token: "token".to_string(),
            capabilities: vec![],
        };
        let envelope = Envelope {
            msg_id: "test-msg".to_string(),
            timestamp: 100,
            source_rope_id: "Alice".to_string(),
            connection_id: "pending".to_string(),
            requires_ack: false,
            payload: join,
        };
        let bytes = bincode::serialize(&envelope).unwrap();
        let parsed: Envelope = bincode::deserialize(&bytes).unwrap();
        match parsed.payload {
            ControlMessage::Tie { knot_id, rope_id, .. } => {
                assert_eq!(knot_id, "p1");
                assert_eq!(rope_id, "Alice");
            }
            _ => panic!("Expected Tie"),
        }
    }

    #[test]
    fn test_control_message_custom_variants() {
        let envelope = Envelope {
            msg_id: "test-cmd".to_string(),
            timestamp: 100,
            source_rope_id: "Alice".to_string(),
            connection_id: "conn-1".to_string(),
            requires_ack: true,
            payload: ControlMessage::Command {
                command_id: "cmd-1".to_string(),
                target_capability_id: "cap-1".to_string(),
                action: "UNLOCK".to_string(),
                payload: "{}".to_string(),
            },
        };
        let bytes = bincode::serialize(&envelope).unwrap();
        let parsed: Envelope = bincode::deserialize(&bytes).unwrap();
        match parsed.payload {
            ControlMessage::Command { action, .. } => {
                assert_eq!(action, "UNLOCK");
            }
            _ => panic!("Expected Command"),
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

        let db = redb::Database::open(&db_path).unwrap();
        let read_txn = db.begin_read().unwrap();
        let frames_table = read_txn.open_table(FRAMES).unwrap();
        let timeline_table = read_txn.open_table(TIMELINE).unwrap();

        assert_eq!(frames_table.len().unwrap(), 2);
        assert_eq!(timeline_table.len().unwrap(), 3);

        let _ = std::fs::remove_file(&db_path);
    }

    #[test]
    fn test_spec_alignment() {
        let join = ControlMessage::Tie {
            protocol_version: 1,
            knot_id: "p1".to_string(),
            rope_id: "Alice".to_string(),
            node_id: "some_node".to_string(),
            join_token: "token".to_string(),
            capabilities: vec![],
        };
        let envelope = Envelope {
            msg_id: "test-msg".to_string(),
            timestamp: 100,
            source_rope_id: "Alice".to_string(),
            connection_id: "pending".to_string(),
            requires_ack: false,
            payload: join,
        };
        let bytes = bincode::serialize(&envelope).unwrap();
        let parsed: Envelope = bincode::deserialize(&bytes).unwrap();
        if let ControlMessage::Tie { knot_id, rope_id, node_id, join_token, capabilities, .. } = parsed.payload {
            assert_eq!(knot_id, "p1");
            assert_eq!(rope_id, "Alice");
            assert_eq!(node_id, "some_node");
            assert_eq!(join_token, "token");
            assert_eq!(capabilities.len(), 0);
        } else {
            panic!("Expected Tie");
        }
    }
}
