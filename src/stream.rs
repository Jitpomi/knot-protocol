use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub stream_id: Option<String>,
    pub source_type: String, // e.g. "screen", "webcam", "audio", "sensor"
    pub name: String,        // e.g. "lg_ultrawide", "facetime_hd"
    pub metadata: String,    // Generic application-specific metadata (JSON)
}

impl StreamConfig {
    pub fn sanitized_name(&self) -> String {
        let mut name = self.name
            .chars()
            .map(|c| if c.is_alphanumeric() { c.to_ascii_lowercase() } else { '_' })
            .collect::<String>();
        
        while name.contains("__") {
            name = name.replace("__", "_");
        }
        name.trim_matches('_').to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Direction {
    Input,
    Output,
    BiDirectional,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeSchema {
    pub data_type: String,
    pub writable: bool,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capability {
    pub id: String,
    pub kind: String,
    pub direction: Direction,
    pub formats: Vec<String>,
    pub commands: Vec<String>,
    pub attributes: HashMap<String, AttributeSchema>,
}

impl Capability {
    pub fn camera_h264_1080p(id: &str) -> Self {
        let mut attributes = HashMap::new();
        attributes.insert("fps".to_string(), AttributeSchema {
            data_type: "integer".to_string(),
            writable: true,
            description: "Frames per second".to_string(),
        });
        Self {
            id: id.to_string(),
            kind: "camera".to_string(),
            direction: Direction::Input,
            formats: vec!["h264".to_string(), "jpeg".to_string()],
            commands: vec!["trigger_keyframe".to_string(), "pan_tilt_zoom".to_string()],
            attributes,
        }
    }

    pub fn microphone_pcm_48khz(id: &str) -> Self {
        let mut attributes = HashMap::new();
        attributes.insert("gain".to_string(), AttributeSchema {
            data_type: "float".to_string(),
            writable: true,
            description: "Input gain level".to_string(),
        });
        Self {
            id: id.to_string(),
            kind: "microphone".to_string(),
            direction: Direction::Input,
            formats: vec!["pcm".to_string()],
            commands: vec![],
            attributes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub magic: [u8; 2],
    pub stream_id: u32,
    pub seq_num: u64,
    pub timestamp_ms: u64,
    pub frame_type: u8,
    pub flags: u8,
    pub payload_len: u32,
}

impl FrameHeader {
    pub const SIZE: usize = 28;

    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..2].copy_from_slice(&self.magic);
        bytes[2..6].copy_from_slice(&self.stream_id.to_be_bytes());
        bytes[6..14].copy_from_slice(&self.seq_num.to_be_bytes());
        bytes[14..22].copy_from_slice(&self.timestamp_ms.to_be_bytes());
        bytes[22] = self.frame_type;
        bytes[23] = self.flags;
        bytes[24..28].copy_from_slice(&self.payload_len.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() < Self::SIZE {
            return Err(anyhow::anyhow!("bytes too short for frame header"));
        }
        let mut magic = [0u8; 2];
        magic.copy_from_slice(&bytes[0..2]);
        if magic != [0x4B, 0x50] {
            return Err(anyhow::anyhow!("invalid magic bytes"));
        }
        let stream_id = u32::from_be_bytes(bytes[2..6].try_into()?);
        let seq_num = u64::from_be_bytes(bytes[6..14].try_into()?);
        let timestamp_ms = u64::from_be_bytes(bytes[14..22].try_into()?);
        let frame_type = bytes[22];
        let flags = bytes[23];
        let payload_len = u32::from_be_bytes(bytes[24..28].try_into()?);

        Ok(Self {
            magic,
            stream_id,
            seq_num,
            timestamp_ms,
            frame_type,
            flags,
            payload_len,
        })
    }
}
