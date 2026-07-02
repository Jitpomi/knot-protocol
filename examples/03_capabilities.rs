use knot_protocol::{JoinPolicy, Capability, HubEvent};
use iroh_knot::{IrohKnotHub as KnotHub, IrohKnotClientJoinBuilder as KnotClient, bind_endpoint, generate_ticket};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host_endpoint = bind_endpoint().await?;
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

    let rope_endpoint = bind_endpoint().await?;
    
    // Register structured camera and microphone capabilities
    let _client = KnotClient::join(&ticket)
        .knot("studio-A")
        .rope_id("studio-rig-1")
        .capability(Capability::camera_h264_1080p("primary-camera"))
        .capability(Capability::microphone_pcm_48khz("boom-mic"))
        .endpoint(rope_endpoint)
        .tie()
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    Ok(())
}
