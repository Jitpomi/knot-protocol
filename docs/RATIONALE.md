# Knot Protocol v1 Architecture Rationale

> **Knot is a transport-independent session orchestration protocol that coordinates multiple physical peers as coherent logical participants over secure peer-to-peer connections.**

This document explains the architectural principles, design goals, and trade-offs behind the **Knot Protocol (v1)**. It provides context for developers and contributors to understand the reasoning behind its design.

---

## 1. Philosophy & Layering

Knot treats networking as a **coordination problem**, not a transport problem. Transport is delegated entirely to the underlying network layer (Iroh/QUIC). Application business logic is handled at the application layer above. Knot defines only the orchestration and coordination layer between them.

### Protocol Layering Model

| Layer / Technology | Responsibility |
| :--- | :--- |
| **Application** | Business & presentation logic |
| **Knot Protocol** | Session orchestration, capability mapping, control, stream gating |
| **Codecs (e.g. H.264, PCM)** | Media encoding and decoding formats |
| **Iroh** | Peer discovery, NAT traversal, relay fallback, cryptographic endpoint identity |
| **QUIC / TLS 1.3** | Transport stream packetization, encryption, reliability |

---

## 2. Design Goals & Non-Goals

### Design Goals
*   **Local-First:** Built to prioritize peer-to-peer communication on local networks and remote links without relying on external cloud brokers.
*   **Peer-to-Peer by Default:** All media and data streams must flow directly between physical devices (Ropes) using NAT hole punching.
*   **Transport-Separated:** Knot deliberately separates orchestration from transport. The v1 reference implementation targets Iroh over QUIC, while the protocol itself avoids embedding transport-specific behavior into its orchestration model.
*   **Platform-Neutral:** Protocols and payloads must remain compatible regardless of client OS or host architecture.
*   **Session-Oriented:** Groups dynamic connections under a single logical session lifecycle.
*   **Capability-Driven:** Focuses on what a device can *do* (its capability) rather than what it *is* (its platform or device type).
*   **Extensible:** Allows adding optional capabilities or control envelopes without breaking wire-format backward compatibility.
*   **Lightweight:** Small enough to be implemented from scratch on embedded microcontrollers or IoT systems.

### Non-Goals
Knot intentionally does NOT handle:
*   **Audio/Video Codec Definition:** Knot frames carry raw payload bytes; it is up to the client and host application layer to negotiate and decode the codec formats.
*   **Replacing QUIC/TLS:** Cryptographic encryption and packet reliability are strictly delegated to standard QUIC.
*   **Media Synchronization:** Multi-stream clock alignment (lip-sync, sub-millisecond hardware clock phase alignment) is left to presentation engines.
*   **Database / Persistence:** Caching, long-term storage, and database writes are application host concerns.
*   **RPC Frameworks:** Knot is not a general-purpose Remote Procedure Call library.
*   **User Identity / Auth Providers:** Handles session join validation tokens, but does not manage user databases, OAuth, or credentials.

---

## 3. The Object Model: Session, Knot, Rope, Topic

Many IoT and P2P orchestration protocols conflate a physical device with its role or location. Knot explicitly splits these concepts:

*   **Rope (Physical Presence):** Represents a physical machine running an Iroh/QUIC endpoint (e.g. a specific camera, sensor hub, or laptop). It maintains network state and handles data streaming.
*   **Knot (Logical Role/Group):** Represents a logical location, group, or role (e.g., `"room-A-presenter"`, `"yard-lights"`). Ropes declare their logical `knot_id` at handshake time.
*   **Session (Lifecycle & Scope):** Orchestrates active Knots and Ropes under a single coordinator (Host).
*   **Topic (Semantic Target):** Represents the semantic name of a stream (e.g. `"primary-video"`, `"room-audio"`).

### Why Sessions?
A Session represents the lifetime of coordination rather than the lifetime of a network connection. Physical connections may disappear and reconnect while the Session continues to exist. This decoupling allows orchestration state to remain stable despite transient network failures.

### Why Split Physical (Rope) from Logical (Knot)?
1.  **Multi-Device Participation:** Multiple physical devices can register under the same logical Knot. For example, a "Presenter" Knot could consist of two Ropes: a webcam and a presentation laptop.
2.  **Takeover and Reconnection:** If a physical device reboots or changes networks, its new connection (Rope) can assume the logical role of the old Knot registry entry seamlessly, maintaining operational state without breaking consumer endpoints.
3.  **Hardware Decoupling:** Consumers subscribe to logical Knots and topics rather than hardcoding IP addresses or physical Node IDs.

### Why Topics?
Topics identify the semantic meaning of a stream rather than the physical source. Multiple Ropes may publish different streams under the same topic, allowing applications to subscribe to concepts such as `"primary-video"` or `"room-audio"` instead of individual physical devices.

---

## 4. Why Capabilities?

Capabilities describe what a Rope can do, not what operating system it runs.

Applications should reason about capability definitions:
*   `Camera` (streaming parameters)
*   `Microphone` (frequency, audio formats)
*   `Switch` (actions: ON, OFF)

rather than platform types:
*   `WindowsClient`
*   `AndroidApp`
*   `LinuxDaemon`

This abstraction guarantees that the Host and other peer participants remain portable and decoupled from hardware-specific implementations.

---

## 5. Why Iroh?

Knot builds on **Iroh** instead of exposing raw QUIC connections directly because Iroh solves the hardest parts of peer-to-peer networking:
1.  **NAT Traversal & Hole Punching:** Establish direct P2P connections behind strict routers and symmetric firewalls.
2.  **Relay Fallback (DERP):** If NAT hole-punching fails, Iroh transparently routes packets through secure, low-latency relay servers.
3.  **Cryptographic Identity:** Every endpoint is identified by a secure Ed25519 public key.
4.  **Endpoint Address tickets:** Simplifies node discovery via short, shareable tickets containing routing parameters.

By delegating these transport challenges to Iroh, the Knot protocol remains focused strictly on session coordination.

---

## 6. Handshake, Trust, and the Authorization Principle

> [!IMPORTANT]
> **The Authorization Principle:**
> *   **Cryptographic identity** proves *who* a Rope is.
> *   **Authorization** determines *what* that Rope is allowed to do.
> *   These concerns are intentionally separate.

### Why does the Host compare the announced `node_id`?
Iroh establishes trust at the transport layer using public keys. However, applications built on top can easily leak or misconfigure client identities if the application-level handshake doesn't cross-verify the underlying transport identity. 
*   By mandating that the Host compare the `node_id` inside `Tie` against the remote node ID of the Iroh QUIC session, Knot prevents **identity spoofing**. A Rope cannot pretend to be another cryptographic node during admission.

### Why use a dynamic Join Token?
Cryptographic trust (the public key) does not equal authorization. A node may be known, but unauthorized to join a specific session. The `join_token` provides a lightweight, session-scoped authorization mechanism that can be rotated, revoked, or bound to dynamic policies (like time-based entry window closures) without needing to revoke the underlying network node identities.

---

## 7. Stream Gating: `StreamOpen` and `StreamAccepted`

In raw QUIC, any peer can open a unidirectional stream at any time. Knot restricts this behavior by requiring a control channel handshake before stream data begins.

### Why wait for `StreamAccepted`?
1.  **Host-Side Admission Control:** The Host might reject a stream if the Rope is not authorized to stream that capability, if the bandwidth budget is exceeded, or if there is no consumer active.
2.  **Resource Allocation:** The Host can initialize local databases, allocate deduplication directories, and prep routing tables *before* the first raw byte of data arrives, eliminating race conditions.
3.  **Preventing Stream Flooding:** Rejecting streams at the control channel prevents malicious or misconfigured nodes from consuming transport buffers by opening hundreds of uncoordinated streams.

---

## 8. The 28-Byte Binary Header Layout

High-frequency telemetry and media streams require zero-copy, low-overhead parsing.

### Why a fixed binary layout?
Using JSON, Protobuf, or CBOR inside high-rate streaming data frames consumes unnecessary CPU cycles and adds payload overhead. The 28-byte binary header can be read directly into a struct in memory:
*   **Big-Endian (Network Byte Order):** Standardizes multi-byte integer decoding across different CPU architectures (x86, ARM).
*   **Session-Relative Timestamps:** Rather than sending absolute Unix timestamps (which require wall-clock synchronization across nodes), Knot uses timestamps relative to when the `Welcome` handshake was processed. This guarantees easy multi-stream alignment and chronological playback ordering on the Host without NTP dependencies.

---

## 9. Alternative Protocol Comparisons

### Why not MQTT?
*   **Centralized Bottleneck:** MQTT relies on a centralized message broker. All traffic must route through the broker, increasing latency and cloud costs. Knot establishes direct **peer-to-peer (P2P)** connections.
*   **Head-of-Line Blocking:** MQTT typically runs over a single TCP socket. If one packet is dropped, all other unrelated topics are blocked. Knot uses QUIC streams, ensuring a drop on video Stream A does not affect telemetry Stream B.

### Why not DDS (Data Distribution Service)?
*   **Footprint and Complexity:** DDS is extremely powerful but has a massive specification footprint, complex XML/IDL configurations, and high resource requirements. Knot is designed to be lightweight enough to run on embedded sensors.
*   **NAT Traversal:** DDS is built for local-area networks (LANs). Running DDS across different remote locations requires complex VPNs. Knot uses Iroh tickets for out-of-the-box NAT hole-punching and firewalls traversal.

### Why not WebRTC Signaling?
*   **Signaling Complexity:** WebRTC requires an external signaling channel (WebSocket/HTTP), STUN/TURN servers, and SDP negotiations to punch holes. Knot handles both signaling (over the control channel) and transport over a single unified QUIC port using Iroh.

---

## 10. Protocol Evolution

To maintain compatibility and prevent fragmentation, Knot protocol versions evolve according to strict rules:

### Minor Versions (e.g. v1.1, v1.2)
*   **Allowed Changes:** Add optional fields to existing control message envelopes, add optional capabilities, add optional control message types.
*   **Requirement:** Implementations MUST ignore unknown optional fields during deserialization. Older clients must remain compatible with newer hosts.

### Major Versions (e.g. v2.0)
*   **Allowed Changes:** Remove fields, change binary frame wire layouts, alter core state machines.
*   **Requirement:** Negotiated via a clean ALPN protocol update (e.g. `jitpomi/studio/2`).
