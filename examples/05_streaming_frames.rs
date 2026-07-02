use knot_protocol::{JoinPolicy, Capability, HubEvent};
use iroh_knot::{IrohKnotHub as KnotHub, IrohKnotClientJoinBuilder as KnotClient, bind_endpoint, generate_ticket};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host_endpoint = bind_endpoint().await?;
    let ticket = generate_ticket(&host_endpoint);

    let _hub = KnotHub::new()
        .with_join_policy(JoinPolicy::ApproveAll)
        .on_event(|event| {
            match event {
                HubEvent::StreamOpened { rope_id, stream_id, topic, config_payload: _ } => {
                    println!("[HOST] Stream approved and opened by Rope '{}': ID: {}, Topic: {}", 
                             rope_id, stream_id, topic);
                }
                HubEvent::FrameReceived { rope_id, stream_id, header, payload } => {
                    println!("[HOST] Received Frame from {}: Stream: {}, Seq: {}, TS: {}ms, Type: {}, Len: {} bytes", 
                             rope_id, stream_id, header.seq_num, header.timestamp_ms, header.frame_type, payload.len());
                }
                _ => {}
            }
        })
        .serve(host_endpoint)
        .await?;

    let rope_endpoint = bind_endpoint().await?;
    let client = KnotClient::join(&ticket)
        .knot("streaming-knot")
        .rope_id("source-node")
        .capability(Capability::camera_h264_1080p("cam-feed"))
        .endpoint(rope_endpoint)
        .tie()
        .await?;

    println!("[ROPE] Initiating StreamOpen handshake for 'cam_feed'...");
    let mut attrs = HashMap::new();
    attrs.insert("fps".to_string(), "30".to_string());
    
    let mut stream = client.create_stream(
        "cam_feed".to_string(),
        "cam-feed".to_string(),
        "primary-video".to_string(),
        "h264".to_string(),
        attrs,
    ).await?;

    println!("[ROPE] Writing 3 binary frames into the unidirectional stream...");
    for i in 0..3 {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        let dummy_payload = vec![0xAB, 0xCD, 0xEF, i as u8];
        stream.write_frame(1, i * 200, &dummy_payload).await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    Ok(())
}
