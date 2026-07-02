use knot_protocol::{KnotHub, KnotClient, JoinPolicy};
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
    // 1. Start Host
    let host_endpoint = knot_protocol::bind_endpoint().await?;
    let ticket = generate_ticket(&host_endpoint);
    println!("[HOST] Booting on Node ID: {}", host_endpoint.id());
    
    let _hub = KnotHub::new()
        .with_join_policy(JoinPolicy::ApproveAll)
        .serve(host_endpoint)
        .await?;

    // 2. Start Rope Client
    let rope_endpoint = knot_protocol::bind_endpoint().await?;
    println!("[ROPE] Booting on Node ID: {}", rope_endpoint.id());
    
    let client = KnotClient::join(&ticket)
        .knot("living-room")
        .rope_id("camera-minimal")
        .endpoint(rope_endpoint)
        .connect()
        .await?;

    println!("[ROPE] Successfully joined session!");
    println!("[ROPE] Assigned Rope ID: {}", client.rope_id());
    println!("[ROPE] Connection ID   : {}", client.connection_id());

    Ok(())
}
