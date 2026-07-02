# Knot Protocol Terminology Glossary

This glossary defines the formal terminology used in the specifications, implementation APIs, and design rationale of the **Knot Protocol (v1)**.

---

### **A**
#### **Ack (Acknowledgment)**
A control message sent by a peer to confirm the successful reception and execution of a command. Carries a `correlation_id` matching the initial command message identifier.

---

### **C**
#### **Capability**
A declarative description of an action, sensor, or data source that a device (Rope) supports (e.g. `Camera`, `Microphone`, `RelaySwitch`). Rather than describing OS or platform types, capabilities outline the functional interface of the device.

#### **Connection**
An active peer-to-peer transport session established between two nodes. In the reference implementation, this is represented by an active QUIC connection managed via Iroh.

#### **Connection ID**
A transient, Host-assigned identifier unique to a single connection instance. Created upon successful handshake validation.

---

### **E**
#### **Envelope**
The standard outer wrapper for all control channel messages. Houses tracking metadata (`msg_id`, `timestamp`, `requires_ack`, `connection_id`) and the typed inner control message payload.

---

### **F**
#### **Frame**
A raw chunk of binary data (such as H.264 slice, PCM audio buffer, or telemetry struct) sent over a unidirectional data stream, prefixed with the 28-byte binary frame header.

---

### **H**
#### **Host**
The central coordinator that orchestrates the active Session. Manages the logical knot registry, validates join handshakes, approves streams, and routes operational commands.

---

### **J**
#### **Join Token**
A transient, session-scoped cryptographic secret passed by a connecting client during handshake. Validated by the Host to authorize session admission.

---

### **K**
#### **Knot**
A logical group, role, location, or container (e.g. `"yard-lights"`, `"zone-A"`) under which one or more physical devices (Ropes) register. Knots decouple application logic and subscriptions from physical network endpoints.

---

### **M**
#### **Message**
A structured control envelope packet exchanged bidirectionally over the control channel.

---

### **N**
#### **Node ID**
The permanent cryptographic identity (typically an Ed25519 public key) of a physical peer, derived from the secure transport layer certificate.

---

### **R**
#### **Rope**
A physical network node (device, sensor, controller, or client app) participating in a session. Each Rope connects to the Host and declares its logical `knot_id` membership.

---

### **S**
#### **Session**
The overarching lifecycle of coordination. A Session coordinates active Knots and Ropes under a single Host, spanning across physical reconnections and network disruptions.

#### **Stream**
An isolated, unidirectional data channel opened dynamically on the connection to carry sequential binary data frames.

---

### **T**
#### **Ticket**
A Base64 URL-Safe encoded string containing the Host's cryptographic public key (Node ID) and its set of direct listener network addresses. The Rope decodes this ticket to establish a direct peer-to-peer transport connection to the Host.

#### **Topic**
The logical name representing the semantic meaning of a stream (e.g. `"primary-video"`, `"ambient-audio"`) rather than its physical source device.
