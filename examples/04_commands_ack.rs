use knot_protocol::{JoinPolicy, Capability, HubEvent, Envelope, ControlMessage};
use iroh_knot::{IrohKnotHub as KnotHub, IrohKnotClientJoinBuilder as KnotClient, bind_endpoint, generate_ticket};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host_endpoint = bind_endpoint().await?;
    let ticket = generate_ticket(&host_endpoint);

    let _hub = KnotHub::new()
        .with_join_policy(JoinPolicy::ApproveAll)
        .on_event(|event| {
            match event {
                HubEvent::RopeConnected { rope_id, control_sender, .. } => {
                    println!("[HOST] Rope '{}' joined. Sending trigger_keyframe command...", rope_id);
                    // Send command to Rope
                    let cmd_env = Envelope {
                        msg_id: "cmd_101".to_string(),
                        timestamp: now_ms(),
                        source_rope_id: "host".to_string(),
                        connection_id: "pending".to_string(),
                        requires_ack: true,
                        payload: ControlMessage::Command {
                            command_id: "cmd_101".to_string(),
                            target_capability_id: "camera-1".to_string(),
                            action: "trigger_keyframe".to_string(),
                            payload: "{}".to_string(),
                        },
                    };
                    let _ = control_sender.send(cmd_env);
                }
                _ => {}
            }
        })
        .serve(host_endpoint)
        .await?;

    let rope_endpoint = bind_endpoint().await?;
    let client = KnotClient::join(&ticket)
        .knot("studio")
        .rope_id("camera-rope")
        .capability(Capability::camera_h264_1080p("camera-1"))
        .endpoint(rope_endpoint)
        .tie()
        .await?;

    let control_tx = client.control_tx();
    let rope_id = client.rope_id().to_string();
    let conn_id = client.connection_id().to_string();

    // Rope event listener
    tokio::spawn(async move {
        while let Some(env) = client.next_event().await {
            match env.payload {
                ControlMessage::Command { command_id, action, .. } => {
                    println!("[ROPE] Received Command '{}' (action: {})", command_id, action);
                    // Send ACK
                    let ack = Envelope {
                        msg_id: format!("ack-{}", command_id),
                        timestamp: now_ms(),
                        source_rope_id: rope_id.clone(),
                        connection_id: conn_id.clone(),
                        requires_ack: false,
                        payload: ControlMessage::Ack {
                            correlation_id: command_id,
                            status: "Success".to_string(),
                            result_payload: "{\"status\":\"ok\"}".to_string(),
                        },
                    };
                    println!("[ROPE] Sending Ack for command '{}'...", ack.msg_id);
                    let _ = control_tx.send(ack);
                }
                ControlMessage::Ack { correlation_id, status, .. } => {
                    println!("[ROPE] Received Ack for correlation: {} - Status: {}", correlation_id, status);
                }
                _ => {}
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;
    Ok(())
}
