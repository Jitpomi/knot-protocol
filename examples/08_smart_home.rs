use knot_protocol::{JoinPolicy, Capability, HubEvent, Envelope, ControlMessage};
use iroh_knot::{IrohKnotHub as KnotHub, IrohKnotClientJoinBuilder as KnotClient, bind_endpoint, generate_ticket};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Starting Smart Home P2P Security System Simulation ---");
    let host_endpoint = bind_endpoint().await?;
    let ticket = generate_ticket(&host_endpoint);

    // Keep track of active clients so the host can command them
    let client_senders = Arc::new(Mutex::new(HashMap::new()));
    let senders_clone = client_senders.clone();

    // 1. Central Home Hub
    let _hub = KnotHub::new()
        .with_join_policy(JoinPolicy::ApproveAll)
        .on_event(move |event| {
            match event {
                HubEvent::RopeConnected { rope_id, control_sender, .. } => {
                    println!("[HUB] Smart Device '{}' connected to the system.", rope_id);
                    senders_clone.lock().unwrap().insert(rope_id, control_sender);
                }
                HubEvent::RopeDisconnected { rope_id } => {
                    println!("[HUB] Smart Device '{}' disconnected.", rope_id);
                    senders_clone.lock().unwrap().remove(&rope_id);
                }
                HubEvent::EventReceived { rope_id, variant, data } => {
                    println!("[HUB] Received Event from '{}': variant='{}', data='{}'", rope_id, variant, data);
                    if variant == "motion_detected" {
                        println!("[HUB] Motion Detected! Command floodlight and gate locks...");
                        
                        // Get senders
                        let map = senders_clone.lock().unwrap();
                        
                        // 1. Dim yard floodlight to 100%
                        if let Some(light_tx) = map.get("front-yard_floodlight-1") {
                            let cmd = Envelope {
                                msg_id: "hub_cmd_light".to_string(),
                                timestamp: now_ms(),
                                source_rope_id: "host".to_string(),
                                connection_id: "pending".to_string(),
                                requires_ack: false,
                                payload: ControlMessage::Event {
                                    variant: "light_dimming".to_string(),
                                    data: "{\"level\":100}".to_string(),
                                },
                            };
                            let _ = light_tx.send(cmd);
                        }
                        
                        // 2. Lock the front gate
                        if let Some(gate_tx) = map.get("front-gate_gate-actuator") {
                            let cmd = Envelope {
                                msg_id: "hub_cmd_gate".to_string(),
                                timestamp: now_ms(),
                                source_rope_id: "host".to_string(),
                                connection_id: "pending".to_string(),
                                requires_ack: false,
                                payload: ControlMessage::Event {
                                    variant: "gate_lock_command".to_string(),
                                    data: "{\"action\":\"LOCK\"}".to_string(),
                                },
                            };
                            let _ = gate_tx.send(cmd);
                        }
                    }
                }
                _ => {}
            }
        })
        .serve(host_endpoint)
        .await?;

    // 2. Camera Device
    let cam_endpoint = bind_endpoint().await?;
    let cam_client = KnotClient::join(&ticket)
        .knot("driveway")
        .rope_id("driveway-camera")
        .capability(Capability::camera_h264_1080p("camera-1"))
        .endpoint(cam_endpoint)
        .connect()
        .await?;
    let cam_client = Arc::new(cam_client);

    // 3. Smart Gate Lock Device
    let gate_endpoint = bind_endpoint().await?;
    let gate_client = KnotClient::join(&ticket)
        .knot("front-gate")
        .rope_id("gate-actuator")
        .endpoint(gate_endpoint)
        .connect()
        .await?;
    let gate_client = Arc::new(gate_client);

    // Listen for commands on the gate
    let gate_clone = gate_client.clone();
    tokio::spawn(async move {
        while let Some(env) = gate_clone.next_event().await {
            if let ControlMessage::Event { variant, data } = env.payload {
                if variant == "gate_lock_command" {
                    println!("[GATE] Command received: data='{}'", data);
                    if data.contains("LOCK") {
                        println!("[GATE] Front Gate Status: SECURED (LOCKED)");
                    }
                }
            }
        }
    });

    // 4. Yard Floodlight Device
    let light_endpoint = bind_endpoint().await?;
    let light_client = KnotClient::join(&ticket)
        .knot("front-yard")
        .rope_id("floodlight-1")
        .endpoint(light_endpoint)
        .connect()
        .await?;
    let light_client = Arc::new(light_client);

    // Listen for commands on the light
    let light_clone = light_client.clone();
    tokio::spawn(async move {
        while let Some(env) = light_clone.next_event().await {
            if let ControlMessage::Event { variant, data } = env.payload {
                if variant == "light_dimming" {
                    println!("[LIGHT] Command received: data='{}'", data);
                    if data.contains("100") {
                        println!("[LIGHT] Yard Floodlight Status: BRIGHT (100% POWER)");
                    }
                }
            }
        }
    });

    // Let connection settle
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Camera detects motion and reports it to Hub
    println!("\n[CAMERA] Motion detected in driveway! Sending event to Home Hub...");
    cam_client.send_event("motion_detected".to_string(), "{\"zone\":\"driveway\",\"confidence\":0.96}".to_string())?;

    // Wait for event propagation and execution
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    println!("\n--- Smart Home Simulation Terminated Successfully ---");
    Ok(())
}
