# Knot Protocol Specification (v1.0.0-draft)

This document defines the architecture, message schemas, state machines, and operational semantics of the **Knot Protocol**.

---

## 1. Protocol Architecture & Metaphor

The Knot Protocol coordinates multiple distributed physical devices under cohesive logical groups. It is built as an application-layer orchestration framework on top of **Iroh** (P2P QUIC).

* **Knot (Logical Group):** An application-defined zone, room, session, or space (e.g., `"zone-123"`). It has no physical network presence; it is a logical collection maintained by a central Host.
* **Rope (Physical Node):** An individual physical device or endpoint (e.g., camera, lock, sensor, or terminal) with a unique Iroh cryptographic public identity (`NodeId`). Ropes connect to the Host and declare their membership in a Knot.
* **Host (Central Registry/Hub):** A central coordinator node that accepts P2P connections from Ropes, validates their identities and membership tokens, manages the logical Knots, and routes events/commands.

---

## 2. Transport & ALPN Setup

Knot utilizes Iroh to establish direct, authenticated, encrypted P2P connections using ALPN negotiation.

* **ALPN Identifier:** `jitpomi/studio/1` (and sub-versions matching this prefix).
* **QUIC Settings:**
  * **Keep-Alive:** Endpoint transport configuration MUST maintain a keep-alive interval (default: `4s`).
  * **Max Idle Timeout:** Connections are dropped if no packets are received within `12s` without a valid heartbeat.
  * **Streams:** Upon connection, a single bidirectional QUIC stream is opened immediately to serve as the **Control Channel**. Dynamic unidirectional QUIC streams are opened on demand for **Data Channels**.

---

## 3. Handshake Lifecycle & State Machine

Every physical connection begins in a state of pending authentication. The handshake establishes the binding between a physical Rope (`NodeId`) and a logical Knot.

```mermaid
stateDiagram-v2
    [*] --> Disconnected
    Disconnected --> Connecting : iroh::Endpoint::connect
    Connecting --> ControlOpened : accept_bi / open_bi
    ControlOpened --> HandshakeSent : Send Handshake Request
    HandshakeSent --> Accepted : HandshakeResponse { approved: true }
    HandshakeSent --> Rejected : HandshakeResponse { approved: false }
    HandshakeSent --> Disconnected : Timeout / Connection Closed
    Accepted --> Active : Initialize Heartbeat & Event Loop
    Active --> Draining : Goodbye / Shutdown Initiated
    Draining --> Disconnected : Stream Closed
```

### 3.1 Handshake Request Packet (Client to Host)
Once the bidirectional control stream is opened, the Rope MUST immediately send a `Handshake` request. No other frames may be sent before this.

```rust
struct Handshake {
    protocol_version: u32,
    min_supported_version: u32,
    knot_id: String,
    rope_id: String,
    rope_node_id: String,  // Hex-encoded string of Iroh NodeId
    rope_type: String,     // e.g. "camera", "gate", "sensor"
    display_name: String,  // Human-readable device label
    session_id: String,    // Token defining the active media session
    capabilities: Capabilities,
    auth_proof: String,    // HMAC or JWT token proving authorization to join knot_id
    metadata: String,      // JSON string of arbitrary extra fields
}
```

### 3.2 Handshake Response Packet (Host to Client)
The Host evaluates the request against its local registry and security policies, then responds.

```rust
struct HandshakeResponse {
    approved: bool,
    assigned_rope_id: String, // Host-sanitized stable logical Rope identifier
    assigned_role: Role,      // Assigned security role
    metadata: String,         // Host-level configuration metadata
    error_message: Option<String>,
}
```

---

## 4. Control Channel & Messaging

The Control Channel is a long-lived bidirectional QUIC stream. It handles low-latency command routing, connection state tracking, and configuration changes.

### 4.1 Message Envelope
Every message sent over the control channel MUST wrap its payload in a generic envelope to guarantee routing context:

```rust
struct Envelope {
    msg_id: String,        // UUID or monotonic message sequence identifier
    kind: MessageKind,     // Enum indicating control action
    timestamp: u64,        // Unix timestamp in milliseconds
    source_rope_id: String,
    target_rope_id: Option<String>, // Option for peer-to-peer routing via Host
    requires_ack: bool,
    payload: Vec<u8>,      // Bincode/JSON serialized specific message structure
}
```

### 4.2 Control Message Types (`MessageKind`)
The protocol defines the following structured variants:

1. **`Hello` (Handshake Request):** Rope starts the handshake.
2. **`Welcome` (Handshake Response - Approved):** Host accepts the Rope.
3. **`Reject` (Handshake Response - Denied):** Host rejects connection.
4. **`Ping / Pong`:** Connection health check.
5. **`Heartbeat`:** Exchanged periodically (every `3s`) to maintain session state.
6. **`Event`:** Client-initiated state update or telemetry (fire-and-forget but reliable over QUIC).
7. **`Command`:** Host-initiated action (requires explicit Acknowledgment).
8. **`Ack`:** Acknowledgment of receipt and execution result of a specific `msg_id`.
9. **`CapabilityUpdate`:** Dynamic change in Rope capabilities (e.g. webcam resolution change).
10. **`Goodbye`:** Explicit notification before clean disconnection.
11. **`Error`:** Protocol-level errors or version mismatches.

### 4.3 Command-Ack Reliability Flow
For critical operations (e.g., unlocking a gate, triggering alarms), commands MUST be acknowledged:

1. **Sender** writes a `Command` envelope with `msg_id = "cmd-101"` and `requires_ack = true`.
2. **Sender** places `"cmd-101"` in a pending queue and starts a timeout timer (default: `2000ms`).
3. **Receiver** receives the command, processes it, and returns an `Ack` envelope targeting `msg_id = "cmd-101"`, containing success/failure status.
4. If the timer expires on the **Sender** without receiving an `Ack`, the Sender retry-sends the command up to 3 times, after which it marks the command as `Failed` and notifies the application.

---

## 5. Host Registry Model & Reconnect Semantics

### 5.1 Host Registry Schema
The Host maintains the active logical topology in memory (or backed by persistent storage):

```rust
struct HostRegistry {
    knots: HashMap<KnotId, KnotState>,
}

struct KnotState {
    knot_id: String,
    display_name: String,
    ropes: HashMap<RopeId, RopeState>,
    active_streams: HashMap<StreamId, StreamState>,
    policy: AccessPolicy,
}

struct RopeState {
    rope_id: String,
    node_id: iroh::PublicKey, // Cryptographic endpoint identity
    connection: iroh::endpoint::Connection,
    rope_type: String,
    role: Role,
    capabilities: Capabilities,
    status: RopeStatus,       // Online, Offline (Grace Period), Suspended
    last_seen: std::time::Instant,
}
```

### 5.2 Disconnection & Graceful Reconnections
If a connection is lost (transport failure, NAT timeout, etc.):

1. **Offline State:** The Host does NOT immediately delete the Rope from the registry. It marks `RopeState::status` as `Offline` and starts a `30-second` grace timer.
2. **Reservation:** During this grace period, the `rope_id` remains reserved under the Knot.
3. **Reclaim:** If the Rope reconnects within the 30-second window, it sends a `Handshake` containing the same `rope_node_id` and the previous `session_id`.
   * The Host verifies that the new connection's `PublicKey` matches the registered `node_id`.
   * Once validated, the connection is bound to the existing `RopeState`, status is set to `Online`, and active data stream paths are resumed.
4. **Takeover/Duplicate Attempt:** If a different cryptographic key (`NodeId`) tries to claim an active or reserved `rope_id`, the Host MUST immediately reject the connection with an `Error` code (`DuplicateRopeId`).

---

## 6. Capability & Version Negotiation

To prevent client/server skew, version and capability negotiation happen during the handshake:

* **ALPN Compatibility:** If the Host is running `jitpomi/studio/1` and a node connects with `jitpomi/studio/2`, the connection is rejected at the transport layer.
* **Handshake Version Matching:** The handshake requires `protocol_version` and `min_supported_version`.
  * If a client's `protocol_version` is lower than the Host's minimum required version, the Host responds with `HandshakeResponse { approved: false, error_message: Some("VersionMismatch") }`.
* **Dynamic Capabilities:** The Rope advertises a list of capabilities (e.g. `["video-encoder:h264", "sensor-reader", "bi-directional-audio"]`). The Host caches this in the registry. If a Host sends a command not listed in the Rope's capabilities, the Rope responds with a `ControlMessage::Error` (`UnsupportedCapability`).
