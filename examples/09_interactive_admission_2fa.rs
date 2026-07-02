use knot_protocol::{JoinPolicy, HubEvent, Envelope, ControlMessage};
use iroh_knot::{IrohKnotHub as KnotHub, IrohKnotClientJoinBuilder as KnotClient, bind_endpoint, generate_ticket};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Default)]
struct SessionState {
    // Map of rope_id -> (PIN, timestamp)
    pending_2fa: HashMap<String, (String, u64)>,
    // Set of fully trusted/verified rope_ids
    trusted_devices: HashMap<String, tokio::sync::mpsc::UnboundedSender<Envelope>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Starting Smart Home P2P 2FA Interactive Admission Simulation ---");
    let host_endpoint = bind_endpoint().await?;
    let ticket = generate_ticket(&host_endpoint);

    let session_state = Arc::new(Mutex::new(SessionState::default()));
    let state_clone = session_state.clone();

    // 1. Central Home Hub
    let _hub = KnotHub::new()
        .with_join_policy(JoinPolicy::ApproveAll)
        .on_event(move |event| {
            match event {
                HubEvent::RopeConnected { rope_id, control_sender, .. } => {
                    println!("[HUB] Physical transport link established for '{}'.", rope_id);
                    
                    // Check if it's the owner's phone (which is pre-trusted for this demo)
                    if rope_id.contains("owner-phone") {
                        println!("[HUB] Device '{}' is an Administrator. Admitting immediately.", rope_id);
                        state_clone.lock().unwrap().trusted_devices.insert(rope_id, control_sender);
                    } else {
                        // For any other device, generate a 2FA PIN and hold in pending
                        let pin = "582914".to_string(); // In production, generate randomly
                        println!("[HUB] Device '{}' requires 2FA. Generated challenge PIN: {}", rope_id, pin);
                        
                        let mut state = state_clone.lock().unwrap();
                        state.pending_2fa.insert(rope_id.clone(), (pin.clone(), now_ms()));
                        
                        // Send Verification Prompt to the Owner's Phone
                        if let Some(owner_tx) = state.trusted_devices.get("trusted_owner-phone") {
                            let prompt = Envelope {
                                msg_id: "hub_2fa_prompt".to_string(),
                                timestamp: now_ms(),
                                source_rope_id: "host".to_string(),
                                connection_id: "pending".to_string(),
                                requires_ack: false,
                                payload: ControlMessage::Event {
                                    variant: "verification_prompt".to_string(),
                                    data: format!("{{\"device\":\"{}\",\"pin\":\"{}\"}}", rope_id, pin),
                                },
                            };
                            let _ = owner_tx.send(prompt);
                        }
                        
                        // Send 2FA Challenge to the new device itself
                        let challenge = Envelope {
                            msg_id: "hub_2fa_challenge".to_string(),
                            timestamp: now_ms(),
                            source_rope_id: "host".to_string(),
                            connection_id: "pending".to_string(),
                            requires_ack: false,
                            payload: ControlMessage::Event {
                                    variant: "2fa_challenge".to_string(),
                                    data: "{}".to_string(),
                            },
                        };
                        let _ = control_sender.send(challenge);
                    }
                }
                HubEvent::RopeDisconnected { rope_id } => {
                    println!("[HUB] Device '{}' disconnected.", rope_id);
                    let mut state = state_clone.lock().unwrap();
                    state.trusted_devices.remove(&rope_id);
                    state.pending_2fa.remove(&rope_id);
                }
                HubEvent::EventReceived { rope_id, variant, data } => {
                    if variant == "2fa_response" {
                        println!("[HUB] Received 2FA Response from '{}': {}", rope_id, data);
                        
                        // Parse JSON PIN
                        #[derive(serde::Deserialize)]
                        struct TwoFactorResponse { pin: String }
                        
                        if let Ok(res) = serde_json::from_str::<TwoFactorResponse>(&data) {
                            let mut state = state_clone.lock().unwrap();
                            if let Some((expected_pin, _ts)) = state.pending_2fa.get(&rope_id) {
                                if res.pin == *expected_pin {
                                    println!("[HUB] SUCCESS: PIN matches! Device '{}' is now TRUSTED.", rope_id);
                                    state.pending_2fa.remove(&rope_id);
                                } else {
                                    println!("[HUB] FAILURE: Incorrect PIN from '{}'. Restricting/Rejecting connection.", rope_id);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        })
        .serve(host_endpoint)
        .await?;

    // 2. Administrator Phone (already trusted)
    let owner_endpoint = bind_endpoint().await?;
    let owner_client = KnotClient::join(&ticket)
        .knot("trusted")
        .rope_id("owner-phone")
        .endpoint(owner_endpoint)
        .connect()
        .await?;
    let owner_client = Arc::new(owner_client);

    // Listen for Verification Prompts on the owner's phone
    let owner_clone = owner_client.clone();
    tokio::spawn(async move {
        while let Some(env) = owner_clone.next_event().await {
            if let ControlMessage::Event { variant, data } = env.payload {
                if variant == "verification_prompt" {
                    #[derive(serde::Deserialize)]
                    struct PromptPayload { device: String, pin: String }
                    if let Ok(payload) = serde_json::from_str::<PromptPayload>(&data) {
                        println!("[OWNER PHONE] 🚨 ALERT: New device '{}' wants to join. PIN Code: {}", payload.device, payload.pin);
                    }
                }
            }
        }
    });

    // Let the owner phone register and settle
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 3. New Untrusted Device (New Smart Lock trying to join)
    let lock_endpoint = bind_endpoint().await?;
    let lock_client = KnotClient::join(&ticket)
        .knot("front-door")
        .rope_id("new-smart-lock")
        .endpoint(lock_endpoint)
        .connect()
        .await?;
    let lock_client = Arc::new(lock_client);

    // Listen for the 2FA Challenge on the new device
    let lock_clone = lock_client.clone();
    let lock_sender_clone = lock_client.clone();
    tokio::spawn(async move {
        while let Some(env) = lock_clone.next_event().await {
            if let ControlMessage::Event { variant, .. } = env.payload {
                if variant == "2fa_challenge" {
                    println!("[NEW LOCK] Received 2FA Challenge. Requesting PIN from user...");
                    
                    // Simulate out-of-band PIN entry
                    let user_entered_pin = "582914".to_string();
                    println!("[NEW LOCK] User entered PIN: {}. Sending response...", user_entered_pin);
                    
                    let response_payload = format!("{{\"pin\":\"{}\"}}", user_entered_pin);
                    let _ = lock_sender_clone.send_event("2fa_response".to_string(), response_payload);
                }
            }
        }
    });

    // Let the 2FA flow propagate and verify
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    println!("\n--- Smart Home 2FA Simulation Terminated Successfully ---");
    Ok(())
}
