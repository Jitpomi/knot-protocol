# Knot Protocol v1 Compliance Specification

This document defines the conformance requirements for implementations of the **Knot Protocol (v1)**. Any client (Rope) or server (Host) claiming compatibility with the protocol MUST fulfill all applicable requirements listed in this checklist.

---

## 1. Handshake & Session Orchestration Compliance

### 1.1 Client Requirements
*   [ ] **Handshake Initialization:** Upon establishing the bidirectional QUIC control stream, the client MUST transmit a `SessionJoin` control envelope as the very first message.
*   [ ] **Announced Identity:** The client MUST populate the `node_id` field inside the `SessionJoin` envelope with the string representation of its own authenticated cryptographic public key (Iroh Node ID).
*   [ ] **Capability Announce:** The client MUST include its full capability catalog (`capabilities` array) in the `SessionJoin` envelope at admission time.
*   [ ] **Token Transmission:** The client MUST supply the configured session token (`join_token`) inside the `SessionJoin` envelope to authorize entry.
*   [ ] **Version Announcement:** The client MUST declare `protocol_version = 1`. If the host returns a `Reject` indicating `ProtocolVersionMismatch`, the client MUST abort the connection.

### 1.2 Host Requirements
*   [ ] **Cryptographic Verification:** The Host MUST compare the announced `node_id` inside the `SessionJoin` payload against the actual remote public key obtained from the authenticated QUIC/TLS 1.3 transport. If they do not match, the Host MUST send a `Reject` with `ErrorCode::InvalidToken` and terminate the connection.
*   [ ] **Admission Control:** The Host MUST validate the `join_token` according to its active `JoinPolicy`. Rejections MUST return `ErrorCode::InvalidToken`.
*   [ ] **Identity Mapping:** Upon successful admission, the Host MUST assign a unique, transient `connection_id` and map the Rope's stable `rope_id` to its active session registry.
*   [ ] **Handshake Response:** The Host MUST respond with a `Welcome` control frame containing the assigned `connection_id`, the scoped `assigned_rope_id`, and any shared session metadata before accepting further control packets.

---

## 2. Control Channel Conformance

*   [ ] **Length-Prefixed Framing:** Every control message envelope MUST be written with a 4-byte big-endian unsigned integer representing the serialized envelope size, followed immediately by the serialized bytes.
*   [ ] **Message Serialization:** The default binary serialization format for the control channel envelope is **Bincode**.
*   [ ] **Heartbeat Keepalive:** Peers MUST exchange periodic `Ping`/`Pong` or `Heartbeat` frames. If no packet is received for `12` seconds, the connection MUST be treated as lost.

---

## 3. Dynamic Stream Negotiation

*   [ ] **Handshake Gating:** The client (Rope) MUST NOT open a unidirectional QUIC stream or write data frames until it has transmitted a `StreamOpen` control request and received a matching `StreamAccepted` response on the bidirectional control channel.
*   [ ] **Stream Metadata Injection:** The first payload frame written onto an approved unidirectional stream MUST be the serialized JSON configuration (`StreamConfig`), detailing the logical `topic`, selected `format`, `capability_id` reference, and dynamic stream `attributes`.
*   [ ] **Stream Sanitization:** Host implementations MUST sanitize the logical `topic` name inside `StreamConfig` to produce safe filenames (e.g. lowercase alphanumeric characters and single underscores only).

---

## 4. Binary Frame formatting (28-Byte Layout)

All data packets on unidirectional streams MUST be framed with the 28-byte binary header using **Big-Endian (Network Byte Order)** encoding:

*   [ ] **Magic Bytes:** Every data frame MUST start with the 2-byte magic code `0x4B 0x50` (`"KP"`). Packets with incorrect magic bytes MUST be discarded.
*   [ ] **Sequence Tracking:** The `seq_num` MUST increment monotonically starting at `0` for each unique stream to track packet loss.
*   [ ] **Relative Timestamps:** The `timestamp_ms` MUST represent the elapsed time in milliseconds since the `Welcome` response handshake was validated by the client.
*   [ ] **Offset Conformity:** Payload data MUST begin exactly at byte offset `28`.

---

## 5. Errors & Reconnections

*   [ ] **Conforming Error Codes:** Rejections and failures MUST map directly to defined `ErrorCode` variants:
    *   `InvalidToken`
    *   `DuplicateRopeId`
    *   `UnsupportedCapability`
    *   `UnauthorizedCommand`
    *   `StreamRejected`
    *   `ProtocolVersionMismatch`
*   [ ] **Takeover Control:** When a Rope reconnects using the same stable `rope_id` and cryptographic `node_id` before the Host's grace period expires, the Host MUST terminate the old connection handle and map the active registry session to the new connection instance.

---

## 6. Tracing & Logging Context

Compliance requires that every diagnostic event, tracer log, or error emitted by a client or host includes the following structured context when available:
1.  **`session_id`** (Logical session context)
2.  **`knot_id`** (Identified group partition)
3.  **`rope_id`** (Stable logical device)
4.  **`connection_id`** (Active QUIC session instance)
5.  **`msg_id`** (Message context for control channel actions)
