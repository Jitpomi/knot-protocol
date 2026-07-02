# Knot Protocol v1 Capability Schema Specification

This document defines the schema, types, and validation rules for **Capabilities** in the **Knot Protocol (v1)**. Drawing inspiration from OPC UA Part 5 and Matter, Knot models devices based on their capability advertisements rather than platform-specific code.

---

## 1. The Capability Object Schema

A Rope (physical node) registers one or more `Capability` instances when joining a session. Capabilities advertise what input/output streams are available, which commands can be accepted, and what attributes define its operating state.

### 1.1 Capability Structure

```rust
struct Capability {
    id: String,                  // Unique identifier for this capability instance (e.g., "cam-1")
    kind: String,                // Classification category (e.g., "camera", "lock", "light")
    direction: Direction,        // Data/Action direction
    formats: Vec<String>,        // Supported media payload formats / serialization mime-types
    commands: Vec<String>,       // List of command actions this capability can execute
    attributes: HashMap<String, AttributeSchema>, // Read/Write operating settings
}

enum Direction {
    Input,         // Rope only publishes data/streams (e.g. Sensor)
    Output,        // Rope only consumes streams or executes actions (e.g. Actuator)
    BiDirectional, // Rope receives and sends data (e.g. Intercom / Audio Transceiver)
}

struct AttributeSchema {
    data_type: String,  // e.g. "integer", "boolean", "float", "string"
    writable: bool,     // True if the Host can write/set this attribute
    description: String,// Human-readable context
}
```

---

## 2. Capability Integration & Validation Rules

1. **Self-Documentation:** A Rope MUST declare its capabilities during the `SessionJoin` process. Once admitted, this schema is cached in the Host registry.
2. **Access Control Filtering:** The Host verifies that any incoming `Command` or `StreamOpen` targets a capability matching the registered schema.
   * *Rule:* If a client attempts to send command `"set_brightness"` targeting capability `"gate-lock-1"`, but `"gate-lock-1"` does not advertise `"set_brightness"` in its `commands` list, the Host rejects the transaction immediately.

---

## 3. Concrete Schema Examples

Below are standard capability mappings for common studio and orchestration devices.

### 3.1 Security Camera

A camera is primarily an `Input` device that publishes a video feed and handles commands for pan-tilt operations.

```json
{
  "id": "driveway-cam",
  "kind": "camera",
  "direction": "Input",
  "formats": ["h264", "jpeg"],
  "commands": ["trigger_keyframe", "pan_tilt_zoom"],
  "attributes": {
    "fps": {
      "data_type": "integer",
      "writable": true,
      "description": "Frames per second"
    },
    "motion_detection_enabled": {
      "data_type": "boolean",
      "writable": true,
      "description": "Toggle local motion sensor tracking"
    }
  }
}
```

### 3.2 Smart Gate Lock

A lock is primarily an `Output` device that handles unlock commands and reports locking status.

```json
{
  "id": "front-gate-lock",
  "kind": "lock",
  "direction": "Output",
  "formats": ["json"],
  "commands": ["LOCK", "UNLOCK"],
  "attributes": {
    "lock_state": {
      "data_type": "string",
      "writable": false,
      "description": "Current status: LOCKED, UNLOCKED, JAMMED"
    }
  }
}
```

### 3.3 Floodlight Dimmer

A light acts as an `Output` device that accepts power/dimming commands.

```json
{
  "id": "backyard-floodlight",
  "kind": "light",
  "direction": "Output",
  "formats": ["json"],
  "commands": ["set_power", "set_brightness"],
  "attributes": {
    "power": {
      "data_type": "boolean",
      "writable": true,
      "description": "Light switch on/off state"
    },
    "brightness": {
      "data_type": "integer",
      "writable": true,
      "description": "Brightness level (0-100)"
    }
  }
}
```
