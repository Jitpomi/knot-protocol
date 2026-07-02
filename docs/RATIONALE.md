# Knot Protocol v1 Architecture Rationale

This document explains the architectural principles, design decisions, and trade-offs behind the **Knot Protocol (v1)**. It provides context for developers and contributors to understand *why* the protocol is designed this way.

---

## 1. The Object Model: Session, Knot, Rope

Many IoT and P2P orchestration protocols conflate a physical device with its role or location. Knot explicitly splits these concepts:

*   **Rope (Physical Presence):** Represents a physical machine running an Iroh/QUIC endpoint (e.g. a specific camera, sensor hub, or laptop). It maintains network state and handles data streaming.
*   **Knot (Logical Role/Group):** Represents a logical location, group, or role (e.g., `"room-A-presenter"`, `"yard-lights"`). Ropes declare their logical `knot_id` at handshake time.
*   **Session (Lifecycle & Scope):** Orchestrates active Knots and Ropes under a single coordinator (Host).

### Why Split Physical (Rope) from Logical (Knot)?
1.  **Multi-Device Participation:** Multiple physical devices can register under the same logical Knot. For example, a "Presenter" Knot could consist of two Ropes: a webcam and a presentation laptop.
2.  **Takeover and Reconnection:** If a physical device reboots or changes networks, its new connection (Rope) can assume the logical role of the old Knot registry entry seamlessly, maintaining operational state without breaking consumer endpoints.
3.  **Hardware Decoupling:** Consumers (like dashboards or recorder engines) subscribe to logical Knots and topics rather than hardcoding IP addresses or physical Node IDs.

---

## 2. Explicit Handshake & Node ID Comparison

Knot utilizes **Iroh** (which uses QUIC and TLS 1.3) for connection encryption and NAT hole-punching.

### Why does the Host explicitly compare the announced `node_id`?
Iroh establishes cryptographic trust at the connection layer using public keys. However, applications built on top can easily leak or misconfigure client identities if the application-level handshake doesn't cross-verify the underlying transport identity. 
*   By mandating that the Host compare the `node_id` inside `SessionJoin` against the remote node ID of the Iroh QUIC session, Knot prevents **identity spoofing**. A Rope cannot pretend to be another cryptographic node during admission.

### Why use a dynamic Join Token?
Cryptographic trust (the public key) does not equal authorization. A node may be known, but unauthorized to join a specific session. The `join_token` provides a lightweight, session-scoped authorization mechanism that can be rotated, revoked, or bound to dynamic policies (like time-based entry window closures) without needing to revoke the underlying network node identities.

---

## 3. Control Channel Gating: `StreamOpen` and `StreamAccepted`

In raw QUIC, any peer can open a unidirectional stream at any time. Knot restricts this behavior by requiring a control channel handshake before stream data begins.

### Why wait for `StreamAccepted`?
1.  **Host-Side Admission Control:** The Host might reject a stream if the Rope is not authorized to stream that capability, if the bandwidth budget is exceeded, or if there is no consumer active.
2.  **Resource Allocation:** The Host can initialize local databases, allocate deduplication directories, and prep routing tables *before* the first raw byte of data arrives, eliminating race conditions.
3.  **Preventing Stream Flooding:** Rejecting streams at the control channel prevents malicious or misconfigured nodes from consuming transport buffers by opening hundreds of uncoordinated streams.

---

## 4. The 28-Byte Binary Header Layout

High-frequency telemetry and media streams require zero-copy, low-overhead parsing.

### Why a fixed binary layout?
Using JSON, Protobuf, or CBOR inside high-rate streaming data frames consumes unnecessary CPU cycles and adds payload overhead. The 28-byte binary header can be read directly into a struct in memory:
*   **Big-Endian (Network Byte Order):** Standardizes multi-byte integer decoding across different CPU architectures (x86, ARM).
*   **Session-Relative Timestamps:** Rather than sending absolute Unix timestamps (which require wall-clock synchronization across nodes), Knot uses timestamps relative to when the `Welcome` handshake was processed. This guarantees easy multi-stream alignment and chronological playback ordering on the Host without NTP dependencies.

---

## 5. Alternative Protocol Comparisons

### Why not MQTT?
*   **Centralized Bottleneck:** MQTT relies on a centralized message broker. All traffic must route through the broker, increasing latency and cloud costs. Knot establishes direct **peer-to-peer (P2P)** connections.
*   **Head-of-Line Blocking:** MQTT typically runs over a single TCP socket. If one packet is dropped, all other unrelated topics are blocked. Knot uses QUIC streams, ensuring a drop on video Stream A does not affect telemetry Stream B.

### Why not DDS (Data Distribution Service)?
*   **Footprint and Complexity:** DDS is extremely powerful but has a massive specification footprint, complex XML/IDL configurations, and high resource requirements. Knot is designed to be lightweight enough to run on embedded sensors.
*   **NAT Traversal:** DDS is built for local-area networks (LANs). Running DDS across different remote locations requires complex VPNs. Knot uses Iroh tickets for out-of-the-box NAT hole-punching and firewalls traversal.

### Why not WebRTC Signaling?
*   **Signaling Overhead:** WebRTC requires an external signaling channel (WebSocket/HTTP), STUN/TURN servers, and SDP negotiations to punch holes. Knot handles both signaling (over the control channel) and transport over a single unified QUIC port using Iroh.
