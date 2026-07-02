# Knot Protocol v1 Developer Experience (DX) Guide

This document is the official guide for developers building nodes, integrations, hosts, or custom network transport adapters on the **Knot Protocol (v1)**. The target goal is that any developer should be able to build a working, valid Rope in under **15 minutes**.

---

## 1. Core API Mental Model

The Knot Protocol separates logical orchestration logic from physical networking transport. The core protocol engine is generic over the `KnotConnection` trait, allowing developers to plug in different connection adapters (e.g., Iroh QUIC, WebSockets, Bluetooth, or local in-memory mock channels for unit tests).

### 1.1 Client Builder Pattern (Building a Rope)

To build a client connection, use the client builder specifying your custom `KnotConnection` transport type:

```rust
use knot_protocol::KnotClient;

let client = KnotClient::<MyCustomConnection>::join(session_ticket)
    .knot("stage-left")
    .rope_id("camera-01")
    .capability(Camera::h264_1080p())
    .connect_with_connection(my_custom_connection)
    .await?;
```

### 1.2 Host Handler Pattern (Building a Hub)

Similarly, host session routing is implemented generically, taking any accepted `KnotConnection` transport:

```rust
use knot_protocol::handle_connection;

// Spawned per incoming transport connection
tokio::spawn(async move {
    if let Err(e) = handle_connection(
        my_accepted_connection,
        data_dir,
        event_tx,
        join_policy,
        cap_validator,
    ).await {
        eprintln!("Connection handler exited with error: {:?}", e);
    }
});
```

---

## 2. Implementing a Custom Transport Adapter

To define a new transport adapter for `knot-protocol`, you must implement the `KnotConnection` trait.

### 2.1 The `KnotConnection` Trait Definition

```rust
#[async_trait::async_trait]
pub trait KnotConnection: Send + Sync + 'static {
    // Underlying send/receive stream types representing data pipes
    type SendStream: tokio::io::AsyncWrite + Send + Sync + Unpin + 'static;
    type RecvStream: tokio::io::AsyncRead + Send + Sync + Unpin + 'static;

    // Accept an incoming bidirectional control/data stream from the peer
    async fn accept_bi(&self) -> Result<(Self::SendStream, Self::RecvStream)>;

    // Accept an incoming unidirectional data stream from the peer
    async fn accept_uni(&self) -> Result<Self::RecvStream>;

    // Open a new outgoing bidirectional control/data stream to the peer
    async fn open_bi(&self) -> Result<(Self::SendStream, Self::RecvStream)>;

    // Open a new outgoing unidirectional data stream to the peer
    async fn open_uni(&self) -> Result<Self::SendStream>;

    // Get the cryptographic/logical node ID of the remote peer
    fn remote_node_id(&self) -> String;

    // Get the cryptographic/logical node ID of the local endpoint
    fn local_node_id(&self) -> String;
}
```

### 2.2 Minimal Memory-Channel Mock Connection Example

Here is a complete, minimal implementation of a custom connection adapter using in-memory channels (ideal for unit testing):

```rust
use knot_protocol::KnotConnection;
use tokio::io::{DuplexStream, duplex};
use tokio::sync::mpsc;
use anyhow::Result;

pub struct MemoryConnection {
    pub local_id: String,
    pub remote_id: String,
    // Incoming streams channels
    pub bi_rx: mpsc::Receiver<(DuplexStream, DuplexStream)>,
    pub uni_rx: mpsc::Receiver<DuplexStream>,
}

#[async_trait::async_trait]
impl KnotConnection for MemoryConnection {
    type SendStream = DuplexStream;
    type RecvStream = DuplexStream;

    async fn accept_bi(&self) -> Result<(Self::SendStream, Self::RecvStream)> {
        // Implementation details...
        todo!()
    }

    async fn accept_uni(&self) -> Result<Self::RecvStream> {
        // Implementation details...
        todo!()
    }

    async fn open_bi(&self) -> Result<(Self::SendStream, Self::RecvStream)> {
        let (client, server) = duplex(1024);
        Ok((client, server))
    }

    async fn open_uni(&self) -> Result<Self::SendStream> {
        let (client, _server) = duplex(1024);
        Ok(client)
    }

    fn remote_node_id(&self) -> String {
        self.remote_id.clone()
    }

    fn local_node_id(&self) -> String {
        self.local_id.clone()
    }
}
```

---

## 3. Using the Default Iroh Transport (`iroh-knot`)

For production P2P connections over internet NAT firewalls, use the concrete `iroh-knot` implementation.

### 3.1 Establishing a Client Connection (Rope)

```rust
use iroh_knot::IrohKnotClientJoinBuilder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = IrohKnotClientJoinBuilder::join("ticket_string...")
        .knot("studio")
        .rope_id("camera-1")
        .tie()
        .await?;
        
    println!("Successfully joined Knot session!");
    Ok(())
}
```

### 3.2 Spawning a Host (Hub)

```rust
use iroh_knot::{IrohKnotHub, bind_endpoint};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = bind_endpoint().await?;
    
    let (hub, mut events) = IrohKnotHub::spawn(
        endpoint,
        PathBuf::from("./hub_data"),
        || "{\"host_status\": \"ok\"}".to_string()
    ).await?;
    
    println!("Host Hub is listening for connections...");
    Ok(())
}
```

---

## 4. Structured Logging & Tracing

To ease session diagnostics across thousands of P2P nodes, all log events emitted by the crate MUST include context spans following this log layout template:

```
[KNOT] LEVEL [session: <session_id>] [knot: <knot_id>] [rope: <rope_id>] [conn: <connection_id>] Message text
```

*   **Example Output:**
    `[KNOT] INFO [session: s_1] [knot: driveway] [rope: cam_1] [conn: c_101] Opened unidirectional stream driveway_cam_feed`

---

## 5. Human-Readable Error Codes

All control channel errors contain a formal `ErrorCode` accompanied by an optional detailed string context. If a connection is rejected, the client receives:

```rust
struct Reject {
    reason: ErrorCode,
    details: String,
}
```

This guarantees developers receive clear reasons for connection failures (e.g., `ProtocolVersionMismatch` - "Client v1 is incompatible with Host minimum v2 requirement").

---

## 🧪 Testing Your Implementation

When verifying your custom transport adapter, run the conformance test suite:

```bash
cargo test -p knot-protocol --test conformance -- --test-threads=1
```

> [!IMPORTANT]
> **Always include the `--test-threads=1` flag** when running the integration/conformance tests. Because tests spin up actual local socket adapters, concurrent execution will lead to loopback timing issues, port collisions, and connection timeouts.
