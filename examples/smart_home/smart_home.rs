use anyhow::Result;
use knot_protocol::{ControlMessage, KnotClient, KnotHub, HubEvent};
use iroh::Endpoint;
use iroh::EndpointAddr;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

struct RopeSession {
    knot_id: String,
    rope_type: String,
    sender: UnboundedSender<ControlMessage>,
}

// 1. Define the simulated Security Camera rope using the KnotClient builder API
async fn run_camera_rope(
    endpoint: Endpoint, 
    hub_addr: EndpointAddr,
    knot_id: String,
    display_name: String
) -> Result<()> {
    println!("[CAMERA ({})] 🔌 Connecting to Hub...", display_name);
    
    // Connect using the self-documenting Builder pattern with hub_addr
    let client = KnotClient::builder(&endpoint)
        .hub_addr(hub_addr)
        .knot_id(knot_id)
        .display_name(display_name.clone())
        .rope_type("camera")
        .session_id("session-42")
        .metadata("{\"fps\":30}")
        .connect()
        .await?;
    
    println!("[CAMERA ({})] 🎉 Handshake approved. Assigned Rope ID: {}", display_name, client.rope_id());

    // Create high-level stream writer
    let stream_slug = display_name.to_lowercase().replace(' ', "_");
    println!("[CAMERA ({})] 📊 Opening video feed stream...", display_name);
    let mut stream = client.create_stream(
        format!("{}_feed", stream_slug),
        "camera".to_string(),
        format!("{} Feed", display_name),
        "{\"codec\":\"h264\",\"fps\":30,\"width\":1920,\"height\":1080}".to_string(),
    ).await?;

    // Send 3 fake frames
    for i in 1..=3 {
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        let payload = vec![0x10, 0x20, 0x30, 0x40 + i as u8];
        println!("[CAMERA ({})] 📤 Sending frame {}...", display_name, i);
        stream.write_frame(if i == 1 { 1 } else { 2 }, i * 300, &payload).await?;
    }

    // Send a motion event over the control channel
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    println!("[CAMERA ({})] 📤 Sending motion alert event...", display_name);
    client.send_event(
        "motion_detected".to_string(),
        "{\"zone\":\"driveway\",\"confidence\":0.96}".to_string(),
    )?;

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    println!("[CAMERA ({})] 👋 Shutting down camera rope...", display_name);
    Ok(())
}

// 2. Define the simulated Smart Gate rope using the KnotClient builder API
async fn run_gate_rope(endpoint: Endpoint, hub_addr: EndpointAddr) -> Result<()> {
    let mut client = KnotClient::builder(&endpoint)
        .hub_addr(hub_addr)
        .knot_id("living-room")
        .display_name("Front Gate")
        .rope_type("gate")
        .connect()
        .await?;
    
    println!("[GATE] ✅ Registered with Hub.");

    // Loop reading commands
    while let Some(msg) = client.next_event().await {
        if let ControlMessage::Event { variant, data } = msg {
            if variant == "gate_lock_command" {
                #[derive(serde::Deserialize)]
                struct LockCommand { action: String }
                if let Ok(cmd) = serde_json::from_str::<LockCommand>(&data) {
                    println!("[GATE] 🚧 Action Executed: Gate set to {} successfully!", cmd.action);
                }
            }
        }
    }
    Ok(())
}

// 3. Define the simulated Security Light rope using the KnotClient builder API
async fn run_light_rope(
    endpoint: Endpoint, 
    hub_addr: EndpointAddr, 
    knot_id: String, 
    display_name: String
) -> Result<()> {
    let mut client = KnotClient::builder(&endpoint)
        .hub_addr(hub_addr)
        .knot_id(knot_id)
        .display_name(display_name.clone())
        .rope_type("light")
        .connect()
        .await?;
    
    println!("[LIGHT ({})] ✅ Registered with Hub.", display_name);

    // Loop reading commands
    while let Some(msg) = client.next_event().await {
        if let ControlMessage::Event { variant, data } = msg {
            match variant.as_str() {
                "light_dimming" => {
                    #[derive(serde::Deserialize)]
                    struct DimmingPayload { level: u8 }
                    if let Ok(payload) = serde_json::from_str::<DimmingPayload>(&data) {
                        println!("[LIGHT ({})] 💡 Brightness adjusted to {}%!", display_name, payload.level);
                    }
                }
                "light_schedule" => {
                    #[derive(serde::Deserialize)]
                    struct SchedulePayload { start_time: String, end_time: String }
                    if let Ok(payload) = serde_json::from_str::<SchedulePayload>(&data) {
                        println!("[LIGHT ({})] 📅 Scheduled light window: {} to {}", display_name, payload.start_time, payload.end_time);
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

// 4. Define the simulated Furnace rope using the KnotClient builder API
async fn run_furnace_rope(
    endpoint: Endpoint,
    hub_addr: EndpointAddr,
    knot_id: String,
    display_name: String
) -> Result<()> {
    let mut client = KnotClient::builder(&endpoint)
        .hub_addr(hub_addr)
        .knot_id(knot_id)
        .display_name(display_name.clone())
        .rope_type("furnace")
        .connect()
        .await?;
    
    println!("[FURNACE ({})] ✅ Registered with Hub.", display_name);

    while let Some(msg) = client.next_event().await {
        if let ControlMessage::Event { variant, data } = msg {
            if variant == "furnace_control" {
                #[derive(serde::Deserialize)]
                struct FurnaceCommand { action: String }
                if let Ok(cmd) = serde_json::from_str::<FurnaceCommand>(&data) {
                    println!("[FURNACE ({})] 🔥 State adjusted: set to {} successfully!", display_name, cmd.action);
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // A. Initialize temporary directory for storing redb logs
    let temp_dir = std::env::temp_dir().join("knot_example");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir)?;

    println!("[MAIN] Starting Knot Smart Home Suite simulation...");

    // B. Spawn the Hub Central Console (KnotHub) using native keepalive endpoint
    let hub_endpoint = knot_protocol::bind_endpoint().await?;
    
    // KnotHub automatically sets up ProtocolHandler, Router, ALPN, and spawning
    let (hub, mut hub_events) = KnotHub::spawn(hub_endpoint.clone(), temp_dir, || "{}".to_string()).await?;
    
    let hub_node_id = hub_endpoint.id();
    println!("[HUB] 🚀 Running on ALPN 'jitpomi/studio/1'. Node ID: {}", hub_node_id);

    // Spawn task to process hub events asynchronously (no Mutex or trait required!)
    tokio::spawn(async move {
        let mut ropes = HashMap::new();

        while let Some(event) = hub_events.recv().await {
            match event {
                HubEvent::RopeConnected { rope_id, knot_id, display_name, rope_type, control_sender, .. } => {
                    println!("\n[HUB] 🤝 Rope Connected!");
                    println!("      Rope ID   : {}", rope_id);
                    println!("      Knot ID   : {}", knot_id);
                    println!("      Name      : {}", display_name);
                    println!("      Type      : {}", rope_type);

                    ropes.insert(rope_id, RopeSession {
                        knot_id,
                        rope_type,
                        sender: control_sender,
                    });
                }
                HubEvent::RopeDisconnected { rope_id } => {
                    println!("\n[HUB] 👋 Rope Disconnected: {}", rope_id);
                    ropes.remove(&rope_id);
                }
                HubEvent::EventReceived { rope_id, variant, data } => {
                    println!("\n[HUB] 🔔 Received Event from Rope {}: variant={}, data={}", rope_id, variant, data);
                    
                    let sender_knot_id = ropes.get(&rope_id).map(|r| r.knot_id.clone());

                    if let Some(knot_id) = sender_knot_id {
                        if variant == "motion_detected" {
                            println!("[HUB] ⚠️ Motion detected in Knot '{}'! Orchestrating local responses...", knot_id);
                            
                            for (r_id, rope) in ropes.iter() {
                                if rope.knot_id == knot_id {
                                    if knot_id == "living-room" {
                                        if rope.rope_type == "gate" {
                                            println!("[HUB] → Instructing Gate lock '{}' to UNLOCK", r_id);
                                            let msg = ControlMessage::Event {
                                                variant: "gate_lock_command".to_string(),
                                                data: "{\"action\":\"UNLOCK\"}".to_string(),
                                            };
                                            let _ = rope.sender.send(msg);
                                        } else if rope.rope_type == "light" {
                                            println!("[HUB] → Instructing Floodlight '{}' to adjust dimming level to 100%", r_id);
                                            let msg = ControlMessage::Event {
                                                variant: "light_dimming".to_string(),
                                                data: "{\"level\":100}".to_string(),
                                            };
                                            let _ = rope.sender.send(msg);
                                        }
                                    } else if knot_id == "backyard" {
                                        if rope.rope_type == "furnace" {
                                            println!("[HUB] → Instructing Furnace '{}' to turn ON", r_id);
                                            let msg = ControlMessage::Event {
                                                variant: "furnace_control".to_string(),
                                                data: "{\"action\":\"ON\"}".to_string(),
                                            };
                                            let _ = rope.sender.send(msg);
                                        } else if rope.rope_type == "light" {
                                            println!("[HUB] → Instructing Backyard Light '{}' to turn OFF", r_id);
                                            let msg = ControlMessage::Event {
                                                variant: "light_dimming".to_string(),
                                                data: "{\"level\":0}".to_string(),
                                            };
                                            let _ = rope.sender.send(msg);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                HubEvent::FrameReceived { rope_id, stream_id, frame_type, timestamp_ms, payload } => {
                    println!(
                        "[HUB] 📥 Received Frame: Rope={}, Stream={}, Type={}, Time={}ms, Size={} bytes",
                        rope_id, stream_id, frame_type, timestamp_ms, payload.len()
                    );
                }
                HubEvent::RopeStreamConfigured { rope_id, stream_id, config } => {
                    println!(
                        "[HUB] ⚙️ Stream Configured: Rope={}, Stream={}, Name={}",
                        rope_id, stream_id, config.name
                    );
                }
            }
        }
    });

    // C. Get Hub Node address
    let hub_addr = hub_endpoint.addr();

    // D. Spawn Gate and Light Ropes in background
    let client_endpoint_gate = knot_protocol::bind_endpoint().await?;
    let hub_addr_gate = hub_addr.clone();
    tokio::spawn(async move {
        let _ = run_gate_rope(client_endpoint_gate, hub_addr_gate).await;
    });

    // Floodlight 1: Registered to Knot "living-room" (same as camera)
    let client_endpoint_light_lr = knot_protocol::bind_endpoint().await?;
    let hub_addr_light_lr = hub_addr.clone();
    tokio::spawn(async move {
        let _ = run_light_rope(
            client_endpoint_light_lr, 
            hub_addr_light_lr, 
            "living-room".to_string(), 
            "Living Room Floodlight".to_string()
        ).await;
    });

    // Floodlight 2: Registered to Knot "backyard" (different room, will respond to backyard camera)
    let client_endpoint_light_by = knot_protocol::bind_endpoint().await?;
    let hub_addr_light_by = hub_addr.clone();
    tokio::spawn(async move {
        let _ = run_light_rope(
            client_endpoint_light_by, 
            hub_addr_light_by, 
            "backyard".to_string(), 
            "Backyard Floodlight".to_string()
        ).await;
    });

    // Furnace: Registered to Knot "backyard"
    let client_endpoint_furnace = knot_protocol::bind_endpoint().await?;
    let hub_addr_furnace = hub_addr.clone();
    tokio::spawn(async move {
        let _ = run_furnace_rope(
            client_endpoint_furnace, 
            hub_addr_furnace, 
            "backyard".to_string(), 
            "Backyard Furnace".to_string()
        ).await;
    });

    // Wait a brief moment for gate, lights & furnace nodes to connect
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // E. Build and run Camera 1 Client Rope (Knot: "living-room")
    let client_endpoint_camera_lr = knot_protocol::bind_endpoint().await?;
    run_camera_rope(
        client_endpoint_camera_lr, 
        hub_addr.clone(), 
        "living-room".to_string(), 
        "Front Camera".to_string()
    ).await?;

    // F. Build and run Camera 2 Client Rope (Knot: "backyard")
    let client_endpoint_camera_by = knot_protocol::bind_endpoint().await?;
    run_camera_rope(
        client_endpoint_camera_by, 
        hub_addr, 
        "backyard".to_string(), 
        "Backyard Camera".to_string()
    ).await?;

    // Wait a brief moment for orchestration events to dispatch and display logs
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // G. Clean up
    hub.shutdown().await?;
    println!("[MAIN] Smart Home Suite simulation completed successfully.");
    Ok(())
}
