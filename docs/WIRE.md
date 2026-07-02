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

1. **`SessionJoin` (Client to Host):**
   Initiates session admission. Renamed from `KNOT_CONNECT`.
   ```rust
   struct SessionJoin {
       protocol_version: u32,
       rope_id: String,
       node_id: String,
       join_token: String,
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
       reason: String,
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
        code: String, // e.g. "UnsupportedCapability", "InvalidToken"
        message: String,
    }
    ```
11. **`Goodbye` (Clean shutdown):**
    Gracefully terminates the connection.

---

## 2. Unidirectional Data Stream Wire Format

Once `StreamAccepted` is received on the control channel, the client establishes a unidirectional stream. Data packets sent through this stream use the **Binary Frame Format**.

### 2.1 Binary Frame Header Layout (28 Bytes)

All multi-byte integers are encoded in **Big-Endian / Network Byte Order**:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      Magic Byte 'K' (0x4B)    |      Magic Byte 'P' (0x50)    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           Stream ID                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                         Sequence Number                       +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                          Timestamp MS                         +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Frame Type  |     Flags     |        Payload Length         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|        Payload Length (cont)  |  Payload Data...              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|                                                               |
```

### 2.2 Wire Fields

1. **Magic Bytes (2 bytes):** Always `0x4B 0x50` (`"KP"`).
2. **Stream ID (4 bytes):** Unique numeric identifier generated by the Host during `StreamAccepted`.
3. **Sequence Number (8 bytes):** Monotonically increasing number starting at `0`. Allows detecting frame loss and restoring order in playback or UDP datagram modes.
4. **Timestamp MS (8 bytes):** Session-relative time offset in milliseconds. Defined as the time elapsed since the `Welcome` handshake response packet was processed by the Rope.
5. **Frame Type (1 byte):**
   * `0x01` - Keyframe
   * `0x02` - Delta frame
   * `0x03` - Metadata event
   * `0x04` - File blob chunk
6. **Flags (1 byte):**
   * Bit 0: `IS_FRAGMENTED` (fragmented across transport envelopes)
   * Bit 1: `LAST_FRAGMENT` (final chunk of a fragmented frame)
   * Bit 2-7: Reserved
7. **Payload Length (4 bytes):** Length of the following raw data.
