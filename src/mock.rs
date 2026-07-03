use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::io::{DuplexStream, ReadHalf, WriteHalf, split};
use anyhow::{Result, anyhow};
use crate::KnotConnection;

/// In-memory mock connection implementing the KnotConnection trait.
#[derive(Clone)]
pub struct MockConnection {
    pub local_id: String,
    pub remote_id: String,
    pub outgoing_bi_tx: mpsc::Sender<(WriteHalf<DuplexStream>, ReadHalf<DuplexStream>)>,
    pub outgoing_uni_tx: mpsc::Sender<ReadHalf<DuplexStream>>,
    pub incoming_bi_rx: Arc<Mutex<mpsc::Receiver<(WriteHalf<DuplexStream>, ReadHalf<DuplexStream>)>>>,
    pub incoming_uni_rx: Arc<Mutex<mpsc::Receiver<ReadHalf<DuplexStream>>>>,
}

#[async_trait::async_trait]
impl KnotConnection for MockConnection {
    type SendStream = WriteHalf<DuplexStream>;
    type RecvStream = ReadHalf<DuplexStream>;

    async fn accept_bi(&self) -> Result<(Self::SendStream, Self::RecvStream)> {
        let mut rx = self.incoming_bi_rx.lock().await;
        rx.recv().await.ok_or_else(|| anyhow!("Connection closed"))
    }

    async fn accept_uni(&self) -> Result<Self::RecvStream> {
        let mut rx = self.incoming_uni_rx.lock().await;
        rx.recv().await.ok_or_else(|| anyhow!("Connection closed"))
    }

    async fn open_bi(&self) -> Result<(Self::SendStream, Self::RecvStream)> {
        let (local_stream, remote_stream) = tokio::io::duplex(65536);
        let (remote_read, remote_write) = split(remote_stream);
        self.outgoing_bi_tx.send((remote_write, remote_read)).await.map_err(|_| anyhow!("Connection closed"))?;
        let (local_read, local_write) = split(local_stream);
        Ok((local_write, local_read))
    }

    async fn open_uni(&self) -> Result<Self::SendStream> {
        let (local_stream, remote_stream) = tokio::io::duplex(65536);
        let (remote_read, _remote_write) = split(remote_stream);
        self.outgoing_uni_tx.send(remote_read).await.map_err(|_| anyhow!("Connection closed"))?;
        let (_local_read, local_write) = split(local_stream);
        Ok(local_write)
    }

    fn remote_node_id(&self) -> String {
        self.remote_id.clone()
    }

    fn local_node_id(&self) -> String {
        self.local_id.clone()
    }
}

/// Simulated memory endpoint representing a network node.
pub struct MockEndpoint {
    pub node_id: String,
    pub connection_tx: mpsc::Sender<MockConnection>,
    pub connection_rx: Arc<Mutex<mpsc::Receiver<MockConnection>>>,
}

impl MockEndpoint {
    pub fn new(node_id: &str) -> (Self, mpsc::Sender<MockConnection>) {
        let (tx, rx) = mpsc::channel(16);
        let ep = Self {
            node_id: node_id.to_string(),
            connection_tx: tx.clone(),
            connection_rx: Arc::new(Mutex::new(rx)),
        };
        (ep, tx)
    }

    pub async fn accept(&self) -> Result<MockConnection> {
        let mut rx = self.connection_rx.lock().await;
        rx.recv().await.ok_or_else(|| anyhow!("Endpoint closed"))
    }
}

/// Connects two mock endpoints together, exchanging connection objects.
pub async fn connect(client_ep: &MockEndpoint, server_ep: &MockEndpoint) -> Result<MockConnection> {
    let (client_bi_tx, client_bi_rx) = mpsc::channel(16);
    let (client_uni_tx, client_uni_rx) = mpsc::channel(16);
    
    let (server_bi_tx, server_bi_rx) = mpsc::channel(16);
    let (server_uni_tx, server_uni_rx) = mpsc::channel(16);

    let client_conn = MockConnection {
        local_id: client_ep.node_id.clone(),
        remote_id: server_ep.node_id.clone(),
        outgoing_bi_tx: client_bi_tx,
        outgoing_uni_tx: client_uni_tx,
        incoming_bi_rx: Arc::new(Mutex::new(server_bi_rx)),
        incoming_uni_rx: Arc::new(Mutex::new(server_uni_rx)),
    };

    let server_conn = MockConnection {
        local_id: server_ep.node_id.clone(),
        remote_id: client_ep.node_id.clone(),
        outgoing_bi_tx: server_bi_tx,
        outgoing_uni_tx: server_uni_tx,
        incoming_bi_rx: Arc::new(Mutex::new(client_bi_rx)),
        incoming_uni_rx: Arc::new(Mutex::new(client_uni_rx)),
    };

    server_ep.connection_tx.send(server_conn).await.map_err(|_| anyhow!("Server offline"))?;

    Ok(client_conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_bi_directional_streaming() {
        let (client_ep, _) = MockEndpoint::new("client");
        let (server_ep, _) = MockEndpoint::new("server");

        let client_conn = connect(&client_ep, &server_ep).await.unwrap();
        let server_conn = server_ep.accept().await.unwrap();

        // 1. Client opens bi stream and writes to it
        let (mut client_tx, mut client_rx) = client_conn.open_bi().await.unwrap();
        let (mut server_tx, mut server_rx) = server_conn.accept_bi().await.unwrap();

        client_tx.write_all(b"hello from client").await.unwrap();
        let mut buf = [0u8; 17];
        server_rx.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello from client");

        // 2. Server writes back
        server_tx.write_all(b"hello from server").await.unwrap();
        let mut buf2 = [0u8; 17];
        client_rx.read_exact(&mut buf2).await.unwrap();
        assert_eq!(&buf2, b"hello from server");
    }

    #[tokio::test]
    async fn test_uni_directional_streaming() {
        let (client_ep, _) = MockEndpoint::new("client");
        let (server_ep, _) = MockEndpoint::new("server");

        let client_conn = connect(&client_ep, &server_ep).await.unwrap();
        let server_conn = server_ep.accept().await.unwrap();

        let mut client_tx = client_conn.open_uni().await.unwrap();
        let mut server_rx = server_conn.accept_uni().await.unwrap();

        client_tx.write_all(b"uni-directional payload").await.unwrap();
        let mut buf = [0u8; 23];
        server_rx.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"uni-directional payload");
    }

    #[tokio::test]
    async fn test_handshake_integration() {
        use futures_util::sink::SinkExt;
        use futures_util::stream::StreamExt;

        let (client_ep, _) = MockEndpoint::new("client-node");
        let (server_ep, _) = MockEndpoint::new("server-node");

        let client_conn = connect(&client_ep, &server_ep).await.unwrap();
        let server_conn = server_ep.accept().await.unwrap();

        let data_dir = std::env::temp_dir().join(format!("knot_test_{}", crate::now_ms()));
        std::fs::create_dir_all(&data_dir).unwrap();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();

        // Spawn handle_connection for the server
        let server_handle = tokio::spawn(async move {
            crate::handle_connection(
                server_conn,
                data_dir.clone(),
                event_tx,
                Arc::new(|| "{}".to_string()),
                crate::JoinPolicy::ApproveAll,
                None,
            ).await
        });

        // Simulating the client side of the handshake manually
        let (client_tx, client_rx) = client_conn.open_bi().await.unwrap();
        let mut framed_read = tokio_util::codec::FramedRead::new(client_rx, tokio_util::codec::LengthDelimitedCodec::new());
        let mut framed_write = tokio_util::codec::FramedWrite::new(client_tx, tokio_util::codec::LengthDelimitedCodec::new());

        // Send Tie message
        let tie = crate::Envelope {
            msg_id: "msg_1".to_string(),
            timestamp: 1000,
            source_rope_id: "rope-1".to_string(),
            connection_id: "".to_string(),
            requires_ack: false,
            payload: crate::ControlMessage::Tie {
                protocol_version: 1,
                knot_id: "knot-1".to_string(),
                rope_id: "rope-1".to_string(),
                node_id: "client-node".to_string(),
                join_token: "token".to_string(),
                capabilities: vec![],
            },
        };
        let tie_bytes = bincode::serialize(&tie).unwrap();
        framed_write.send(bytes::Bytes::from(tie_bytes)).await.unwrap();

        // Expect Welcome message
        let welcome_bytes = framed_read.next().await.unwrap().unwrap();
        let welcome: crate::Envelope = bincode::deserialize(&welcome_bytes).unwrap();
        if let crate::ControlMessage::Welcome { connection_id, assigned_rope_id, .. } = welcome.payload {
            assert_eq!(assigned_rope_id, "knot-1_rope-1");
            assert!(!connection_id.is_empty());
        } else {
            panic!("Expected Welcome, got {:?}", welcome.payload);
        }

        // Cleanup
        drop(framed_read);
        drop(framed_write);
        drop(client_conn);
        let _ = server_handle.await.unwrap();
        
        // Drain events to allow clean completion
        while let Some(_) = event_rx.recv().await {}
    }
}
