# Knot Protocol v1 Security Specification

This document defines the verification, credentials, access controls, and command restrictions governing the **Knot Protocol (v1)**.

---

## 1. Multi-Level Identity Verification

To prevent identity spoofing, routing collisions, and state confusion, the Host verifies three levels of identity during the handshake (`SessionJoin` command):

1. **`node_id` verification:** Natively handled by the TLS 1.3 layer of Iroh. The Host verifies that the connection initiator possesses the private key for the announced Ed25519 `NodeId`.
2. **`rope_id` validation:** The Rope requests its stable identity. The Host checks if the `rope_id` is registered and authorized under the session configuration.
3. **`connection_id` mapping:** Once approved, the Host generates a unique `connection_id` for this active QUIC socket. The Host maps this `connection_id` to the stable `rope_id` in the registry.

```
+-----------------------------------------------------------------+
| TLS 1.3 Transport Verification: node_id (Ed25519 PublicKey)     |
+-----------------------------------------------------------------+
                                |
                                v
+-----------------------------------------------------------------+
| Join Token Verification: knot_id, sub (matching node_id)        |
+-----------------------------------------------------------------+
                                |
                                v
+-----------------------------------------------------------------+
| Active Binding: knot_id -> rope_id -> node_id -> connection_id   |
+-----------------------------------------------------------------+
```

---

## 2. Join Token Verification Policy

Ropes must submit a valid, cryptographically signed `JoinToken` within the `join_token` field of the `SessionJoin` handshake command.

### 2.1 Token Validation Steps

The Host MUST perform the following validations:
1. Verify the signature (HMAC-SHA256 or Asymmetric Ed25519) matches the Host's authorized issuer key.
2. Confirm the token's expiration timestamp (`exp`) has not passed.
3. Verify the token's subject (`sub`) matches the cryptographic `node_id` of the connection.
4. Verify the token's `knot_id` matches the logical `Knot` the Rope is attempting to register under.
5. If validation succeeds, a `connection_id` is generated and mapped. If validation fails, the Host sends a `Reject` control frame and terminates the connection.

---

## 3. Capability-Based Authorization

Knot v1 enforces strict **Capability-Based Authorization** to isolate device actions and prevent privilege escalation.

### 3.1 Core Rules

1. **Registered Action Constraint:** A Rope may only receive a `Command` if that command targets a capability that the Rope explicitly registered in its capability table during session join.
   * *Example:* If a Camera Rope registers a capability that does not include the command `"unlock"`, the Host's policy engine will block any attempt to route an `"unlock"` command to it.
2. **Session Policy Verification:** Before routing or executing any `Command` or `StreamOpen` request, the Host checks the global `Session` policy. The policy dictates which roles or identities inside the session are authorized to issue the command or subscribe to the stream.
3. **Implicit Publisher Restriction:** Ropes with the `Publisher` role are blocked by the Host from sending any commands to other Ropes. They are restricted to registering capabilities, sending telemetry events, and publishing data streams.

---

## 4. Reconnection & Takeover Security

When a connection terminates unexpectedly (socket drop, network switch):

1. **Grace Period:** The Host marks the `rope_id`'s status as `Offline` but preserves the entry and its registered `Capabilities` in the registry for a `30-second` grace period.
2. **Identity Verification:** The Rope can reconnect and reclaim its `rope_id` only if it initiates the new handshake using the same cryptographic `node_id`.
3. **Session Hijacking Prevention:** If a client attempts to claim a reserved `rope_id` using a different `node_id` (even if they possess a valid ticket), the Host MUST reject the handshake with a `DuplicateRopeId` error and keep the original reservation intact.
4. **Takeover:** If a connection drop occurs and the same `node_id` connects under a new `connection_id` before the grace period expires, the old `connection_id` is invalidated, and the new connection immediately inherits the active registry state.
