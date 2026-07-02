# Knot Protocol v1 Developer Experience (DX) Guide

This document is the official guide for developers building nodes, integrations, and hosts on the **Knot Protocol (v1)**. The target goal is that any developer should be able to build a working, valid Rope in under **15 minutes**.

---

## 1. Core API Mental Model

The Knot Protocol exposes clean builder patterns to encapsulate protocol handshake negotiations, connection tracking, capability advertising, and stream allocations.

### 1.1 Client Builder Pattern (Building a Rope)

```rust
let client = KnotClient::join(session_ticket)
    .knot("stage-left")
    .rope_id("camera-01")
    .capability(Camera::h264_1080p())
    .connect()
    .await?;
```

### 1.2 Host Builder Pattern (Building a Hub)

```rust
let hub = KnotHub::new()
    .with_join_policy(policy)
    .on_command(handle_command)
    .on_stream(handle_stream)
    .serve(endpoint)
    .await?;
```

---

## 2. Minimal Working Implementations

### 2.1 The Smallest Valid Rope Client

A minimal client connecting to a Host under Knot `"living-room"` and declaring no capabilities:

```rust
use knot_protocol::KnotClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = knot_protocol::bind_endpoint().await?;
    
    let client = KnotClient::join("my-session-ticket")
        .knot("living-room")
        .rope_id("minimal-rope")
        .connect_with_endpoint(&endpoint)
        .await?;
        
    println!("Successfully joined session! Active Rope ID: {}", client.rope_id());
    Ok(())
}
```

### 2.2 The Smallest Valid Host

A minimal Host accepting connections and using a default approve-all join policy:

```rust
use knot_protocol::{KnotHub, JoinPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = knot_protocol::bind_endpoint().await?;
    
    let hub = KnotHub::new()
        .with_join_policy(JoinPolicy::ApproveAll)
        .serve(endpoint)
        .await?;
        
    println!("Host listening for incoming Ropes...");
    hub.await_shutdown().await?;
    Ok(())
}
```

---

## 3. Testing with Mock Transports

To allow developers to write tests without real UDP/IP Iroh sockets, Knot provides a memory-channel mock client/server transport utility:

```rust
#[tokio::test]
async fn test_in_memory_handshake() {
    let (client_conn, server_conn) = iroh::endpoint::Connection::mock_pair();
    
    let client_task = tokio::spawn(async move {
        KnotClient::join_with_connection(client_conn, "knot-1", "rope-1").await
    });
    
    let server_task = tokio::spawn(async move {
        KnotHub::accept_connection(server_conn).await
    });
    
    let (client_res, server_res) = tokio::join!(client_task, server_task);
    assert!(client_res.unwrap().is_ok());
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
