use serde::{Serialize, Deserialize};

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
