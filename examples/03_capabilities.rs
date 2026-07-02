use knot_protocol::{KnotHub, KnotClient, JoinPolicy, Capability, HubEvent};
use iroh::Endpoint;
use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};

fn generate_ticket(endpoint: &Endpoint) -> String {
    let addr = endpoint.addr();
    let mut bytes = vec![1];
    bytes.extend_from_slice(addr.id.as_bytes());
    if let Ok(json_bytes) = serde_json::to_vec(&addr.addrs) {
        bytes.extend_from_slice(&json_bytes);
    }
    BASE64_URL_SAFE_NO_PAD.encode(bytes)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host_endpoint = knot_protocol::bind_endpoint().await?;
    let ticket = generate_ticket(&host_endpoint);

    let _hub = KnotHub::new()
        .with_join_policy(JoinPolicy::ApproveAll)
        .on_event(|event| {
            if let HubEvent::RopeConnected { rope_id, knot_id, capabilities, .. } = event {
                println!("[HOST] Rope '{}' joined Knot '{}' exposing capabilities:", rope_id, knot_id);
                for cap in capabilities {
                    println!("  - ID: {}, Kind: {}, Formats: {:?}, Commands: {:?}", 
                             cap.id, cap.kind, cap.formats, cap.commands);
                }
            }
        })
        .serve(host_endpoint)
        .await?;

    let rope_endpoint = knot_protocol::bind_endpoint().await?;
    
    // Register structured camera and microphone capabilities
    let _client = KnotClient::join(&ticket)
        .knot("studio-A")
        .rope_id("studio-rig-1")
        .capability(Capability::camera_h264_1080p("primary-camera"))
        .capability(Capability::microphone_pcm_48khz("boom-mic"))
        .endpoint(rope_endpoint)
        .connect()
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    Ok(())
}
