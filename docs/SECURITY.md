# Knot Protocol Security Specification (v1.0.0-draft)

This document specifies the identity, authorization, access control, and credential management standards for the **Knot Protocol**.

---

## 1. Security Architecture Overview

The Knot Protocol achieves defense-in-depth by splitting security responsibilities between the transport layer and the application layer.

```
+-------------------------------------------------------------------+
| APPLICATION LAYER (Knot Protocol)                                 |
| - Join Token Validation (HMAC / RSA / Ed25519 signatures)          |
| - Role-Based Access Control (RBAC)                                 |
| - Command Isolation & Registry Policies                           |
+-------------------------------------------------------------------+
| TRANSPORT LAYER (Iroh Network Stack)                              |
| - TLS 1.3 QUIC Tunnel Encryption                                   |
| - Peer Public Key Authentication (NodeId = Ed25519 Public Key)     |
| - Relay Server Routing & Hole Punching (NAT traversal)            |
+-------------------------------------------------------------------+
```

---

## 2. Identity Verification & Peer Authentication

Every Rope (physical node) is identified by its **Iroh PublicKey** (a cryptographic keypair endpoint). 

1. **Transport Authentication:** During the TLS 1.3 handshake, the Iroh stack verifies that the connecting peer possesses the private key corresponding to its announced `NodeId`.
2. **Identity Stability:** The `NodeId` MUST serve as the immutable anchor of the Rope's identity. In the host registry, a Rope's logical ID (`rope_id`) is permanently bound to its `NodeId` for the duration of the session.

---

## 3. Join Token Validation

To coordinate access, the Knot Protocol requires an application-layer credentials check during the handshake. Simply possessing a host's Iroh connection ticket is **not** sufficient to join a Knot.

### 3.1 Token Structure
A Rope MUST supply a cryptographically signed **Join Token** inside the `auth_proof` field of the handshake request. The token contains the following JSON structure:

```json
{
  "iss": "knot-host-authority",
  "sub": "node-1a2b3c4d...",
  "knot_id": "zone-123",
  "allowed_rope_id": "camera-driveway",
  "role": "Publisher",
  "capabilities": ["video-encoder:h264", "sensor-reader"],
  "exp": 1782816000
}
```

### 3.2 Signature Verification
The host validates the token signature using the host's configured authentication scheme:
* **Symmetric (HMAC-SHA256):** Used in local-first, single-owner environments (e.g. smart homes) where host and rope share a pre-shared key (PSK).
* **Asymmetric (Ed25519 / RSA):** Used in enterprise deployments where a central provisioning authority issues signed tokens verified by the host's public key.

### 3.3 Handshake Security Checks
During the handshake, the Host MUST verify:
1. The token signature is valid.
2. The current epoch time is less than the token's `exp` timestamp.
3. The connecting peer's cryptographic `NodeId` matches the `sub` claim.
4. The requested `knot_id` matches the `knot_id` claim.
5. The requested capabilities do not exceed the `capabilities` claim.

If any check fails, the Host MUST terminate the connection immediately by returning a `HandshakeResponse { approved: false, error_message: Some("InvalidToken") }`.

---

## 4. Access Control Model (RBAC)

Knot enforces Role-Based Access Control (RBAC) at the host registry layer to partition stream publishing and command routing.

### 4.1 Security Roles
The protocol defines four canonical roles:

| Role | Description | Typical Devices |
| :--- | :--- | :--- |
| **Admin** | Full orchestration permissions, can configure Knot settings, revoke Ropes, and issue commands. | Local console, mobile controller app. |
| **Controller** | Can issue commands to Ropes under the same Knot and subscribe to telemetry/streams. | Smart home hubs, studio control panels. |
| **Publisher** | Can open unidirectional data streams and send telemetry events. Prohibited from issuing commands. | IP cameras, environmental sensors. |
| **Subscriber** | Can consume unidirectional data streams. Prohibited from sending events or commands. | Wall displays, recording storage nodes. |

### 4.2 Role Permission Matrix

| Permission | Admin | Controller | Publisher | Subscriber |
| :--- | :---: | :---: | :---: | :---: |
| **JoinKnot** | Yes | Yes | Yes | Yes |
| **PublishStream** | Yes | Yes | Yes | No |
| **SubscribeStream** | Yes | Yes | No | Yes |
| **SendEvent** | Yes | Yes | Yes | No |
| **SendCommand** | Yes | Yes | No | No |
| **ReceiveCommand** | Yes | Yes | Yes | No |
| **AdminKnot** | Yes | No | No | No |

---

## 5. Command Routing & Isolation Policies

To prevent security cross-talk or compromised client privilege escalation, the Host enforces strict routing boundaries:

1. **Intra-Knot Isolation:** A Rope with the `Controller` role can only send commands to other Ropes that belong to the *same* `knot_id` (unless explicitly authorized by a cross-knot bridging rule).
2. **Command Validation:** The Host inspects every command envelope received over a control channel. If a Rope with the `Publisher` role (e.g., a camera) attempts to send a `gate_lock_command` to the gate, the Host drops the packet and issues an `Error` response (`UnauthorizedAction`).
3. **Peer-to-Peer Stream Privacy:** A Rope cannot subscribe to another Rope's unidirectional stream directly without requesting a stream token from the Host. The Host verifies permissions before returning the Iroh transport parameters required to pull the stream.

---

## 6. Revocation & Expiry

* **Active Revocation:** An `Admin` can issue a `RevokeRope` command containing a target `rope_id`. The Host immediately marks the target Rope as `Suspended` in its registry, drops its P2P connection, and rejects subsequent connection attempts using that `NodeId`.
* **Token Expiration:** Upon token expiration (`exp`), the Host marks the Rope as expired. The control channel is kept open for a short grace period, sending a warning envelope (`ControlMessage::Error { code: TokenExpired }`), prompting the Rope to request a new token. If no new token is presented within `60 seconds`, the connection is closed.
