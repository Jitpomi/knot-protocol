# Knot Protocol v1 Wire Format Specification

This document specifies the serialization layouts, control message frames, and binary stream payloads of the **Knot Protocol (v1)**.

---

## 1. Control Channel Messaging (Bidirectional Stream)

The Control Channel is a reliable, bidirectional QUIC stream. To guarantee atomic message boundaries, all control packets are prefixed with a **4-byte big-endian unsigned length header**, followed by the serialized message bytes.

### 1.1 Message Envelope Structure

All control messages use a standard envelope structure:

```rust
struct Envelope {
    msg_id: String,             // Unique identifier for tracking and correlation
    timestamp: u64,             // Milliseconds elapsed since Unix Epoch
    source_rope_id: String,     // Sender stable device identity
    connection_id: String,      // Host-assigned connection identifier
    requires_ack: bool,         // Flag requesting confirmation
    payload: ControlMessage,    // Serialized control action
}
```

### 1.2 v1 Control Message Types (`ControlMessage`)

1. **`Tie` (Client to Host):**
   Initiates session admission by tying the knot.
   ```rust
   struct Tie {
       protocol_version: u32,
       knot_id: String,
       rope_id: String,
       node_id: String,
       join_token: String,
       capabilities: Vec<Capability>, // List of capabilities announced at join time
   }
   ```
2. **`Welcome` (Host to Client):**
   Handshake approved. Returns the connection metadata and assigned identity settings.
   ```rust
   struct Welcome {
       connection_id: String,
       assigned_rope_id: String,
       session_metadata: String,
   }
   ```
3. **`Reject` (Host to Client):**
   Handshake denied.
   ```rust
   struct Reject {
       reason: ErrorCode,
       details: String,
   }
   ```
4. **`StreamOpen` (Client to Host):**
   Requests authorization to establish a unidirectional data stream.
   ```rust
   struct StreamOpen {
       stream_id: String,
       topic: String,
       config_payload: String, // JSON payload detailing codec, channels, etc.
   }
   ```
5. **`StreamAccepted` (Host to Client):**
   Approves the unidirectional stream. The Rope MUST wait for this message before writing onto the data stream.
   ```rust
   struct StreamAccepted {
       stream_id: String,
   }
   ```
6. **`StreamClosed` (Either Peer):**
   Notifies clean termination of a stream.
   ```rust
   struct StreamClosed {
       stream_id: String,
       reason: String,
   }
   ```
7. **`Command` (Host to Client):**
   Commands an action (must match registered capabilities).
   ```rust
   struct Command {
       command_id: String,
       target_capability_id: String,
       action: String,
       payload: String,
   }
   ```
8. **`Ack` (Either Peer):**
   Confirms execution of a command.
   ```rust
   struct Ack {
       correlation_id: String, // Matches the command_id or msg_id
       status: String,         // "Success", "Failed", "Unauthorized"
       result_payload: String,
   }
   ```
9. **`Heartbeat` (Periodic):**
   Exchanged every `3` seconds to track session liveness.
10. **`Error` (Either Peer):**
    Signals errors or protocol violations.
    ```rust
    struct Error {
        code: ErrorCode,
        message: String,
    }
    ```
11. **`Goodbye` (Clean shutdown):**
    Gracefully terminates the connection.

### 1.3 Error Codes Enum (`ErrorCode`)

Protocol error codes are serialized as enums, rather than freeform strings:

*   **`InvalidToken`**: The Join Token signature, expiration, or subject is invalid.
*   **`DuplicateRopeId`**: Another connection is already active with the requested `rope_id`.
*   **`UnsupportedCapability`**: The requested operation is not advertised in the Rope's capabilities.
*   **`UnauthorizedCommand`**: The sender is not permitted by session policy to issue this command.
*   **`StreamRejected`**: The Host policy rejected the opening of the data stream.
*   **`ProtocolVersionMismatch`**: The client and Host version parameters are incompatible.

---

## 2. Unidirectional Data Stream Wire Format

Once `StreamAccepted` is received on the control channel, the client establishes a unidirectional stream. Data packets sent through this stream use the **Binary Frame Format**.

### 2.1 Binary Frame Header Layout (28 Bytes)

The frame header is exactly 28 bytes. The exact byte offsets are mapped below:

| Offset (Bytes) | Field Name | Type | Description |
| :--- | :--- | :--- | :--- |
| **`0 - 1`** | Magic Bytes | `[u8; 2]` | Always `0x4B 0x50` (`"KP"`) |
| **`2 - 5`** | Stream ID | `u32` | Unique numeric identifier for the data stream (Big-Endian) |
| **`6 - 13`** | Sequence Number | `u64` | Monotonically increasing index starting at `0` (Big-Endian) |
| **`14 - 21`** | Timestamp MS | `u64` | Session-relative offset in milliseconds (Big-Endian) |
| **`22`** | Frame Type | `u8` | Payload category (`0x01` = Keyframe, `0x02` = Delta, `0x03` = Event) |
| **`23`** | Flags | `u8` | Control flags (`Bit 0` = fragmented, `Bit 1` = last fragment) |
| **`24 - 27`** | Payload Length | `u32` | Size of the payload following this header in bytes (Big-Endian) |

Payload data begins at byte offset `28`.
