# Knot Protocol Wire Format Specification (v1.0.0-draft)

This document defines the binary layout, framing, serialization, and stream transport rules for the **Knot Protocol**.

---

## 1. Transport Layer Framing

The Knot Protocol operates over **QUIC** (via Iroh). Framing differs depending on whether data is sent over the Control Channel (reliable bidirectional stream), a Data Stream (reliable unidirectional stream), or a Datagram Channel (unreliable packet transport).

---

## 2. Binary Frame Header Format

Unidirectional data streams and datagrams utilize a standardized binary header to frame raw payloads (such as H.264 video slices, PCM audio, or sensor arrays).

### 2.1 Header Byte Layout

The header is exactly **28 bytes** long, structured as follows (all multi-byte integers are encoded in **Network Byte Order / Big-Endian**):

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

### 2.2 Field Breakdown

* **Magic Bytes (2 bytes):** Always `0x4B 0x50` (ASCII representation of `"KP"` for Knot Protocol). Packets starting with incorrect magic bytes MUST be discarded immediately.
* **Stream ID (4 bytes):** Monotonically assigned identifier for the logical source stream (unique per Rope connection).
* **Sequence Number (8 bytes):** Monotonically increasing counter for frames inside this specific stream. Used to detect packet loss or out-of-order delivery.
* **Timestamp MS (8 bytes):** Session-relative offset in milliseconds elapsed since the handshake validation was finalized. Facilitates multi-stream alignment and synchronized recording playback.
* **Frame Type (1 byte):** Indicates payload classification:
  * `0x01`: Keyframe (e.g. video I-frame, complete sensor state).
  * `0x02`: Delta Frame (e.g. video P/B-frames, sensor delta changes).
  * `0x03`: Telemetry Event metadata.
  * `0x04`: Raw File/Blob chunk.
* **Flags (1 byte):** Bitmap for frame modifiers:
  * Bit 0: `IS_FRAGMENTED` (indicates the frame is split across multiple transport packets).
  * Bit 1: `LAST_FRAGMENT` (marks the final packet of a fragmented frame).
  * Bit 2-7: Reserved for future extensions.
* **Payload Length (4 bytes):** Unsigned 32-bit integer indicating the size of the payload following this header in bytes.

---

## 3. Control Channel Wire Coding

The Control Channel uses a long-lived bidirectional QUIC stream. To prevent stream parsing confusion and ensure atomic message delivery, frames are prefixed with their length.

1. **Framing Codec:** Every control message is written using a **Length-Delimited Codec**.
   * A 4-byte big-endian unsigned integer specifies the length of the serialized message.
   * Followed immediately by that many bytes of serialized message content.
2. **Serialization Format:** 
   * **Bincode** (or compact binary serialization) is used for transport-level structures to optimize overhead.
   * **JSON** may be used in development mode or for dynamic application metadata to ease debugging.

---

## 4. Unidirectional Data Streams (Reliable Transport)

Unidirectional streams are opened dynamically when a Rope publishes a data channel. They provide **reliable, ordered** delivery backed by QUIC streams.

### 4.1 Stream Lifecycle
1. **Opening:** The Rope opens a unidirectional stream.
2. **Metadata Header:** The first payload frame sent down the stream MUST be the serialized JSON configuration (`StreamConfig`), structured as:
   ```json
   {
     "stream_id": "cam-1-feed",
     "source_type": "camera",
     "name": "Driveway Camera Feed",
     "metadata": "{\"codec\":\"h264\",\"fps\":30}"
   }
   ```
3. **Binary Streaming:** All subsequent frames on this stream MUST follow the **Binary Frame Header Format** (without magic bytes, as the stream envelope is already established and isolated).
4. **Closing:** When the source is disabled, the Rope half-closes the QUIC stream. The Host processes any remaining queued packets and flushes the recording database.

---

## 5. Unreliable Datagram Channel (Real-time Transport)

For real-time low-latency media (such as live audio channels or low-latency video monitoring), the head-of-line blocking of reliable QUIC streams is undesirable. Knot supports an **Unreliable Datagram** transport profile.

1. **Transport binding:** Packets are transmitted over QUIC Datagram frames (`Datagram` frames in QUIC RFC 9000).
2. **Framing requirement:** Because datagrams do not guarantee delivery or ordering, every datagram packet MUST contain the full **28-byte Binary Frame Header** (including the `Magic Bytes` and `Sequence Number`).
3. **Fragment re-assembly:** If a frame exceeds the MTU (typically `1200 bytes`), the publisher fragments the payload, sets the `IS_FRAGMENTED` flag in the header, and increments the sequence number. The receiver uses the `Sequence Number` and `Timestamp MS` to reconstruct the fragments. If any fragment is missing, the entire frame is dropped.
