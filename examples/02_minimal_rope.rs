use knot_protocol::JoinPolicy;
use iroh_knot::{IrohKnotHub as KnotHub, IrohKnotClientJoinBuilder as KnotClient, bind_endpoint, generate_ticket};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Start Host
    let host_endpoint = bind_endpoint().await?;
    let ticket = generate_ticket(&host_endpoint);
    println!("[HOST] Booting on Node ID: {}", host_endpoint.id());
    
    let _hub = KnotHub::new()
        .with_join_policy(JoinPolicy::ApproveAll)
        .serve(host_endpoint)
        .await?;

    // 2. Start Rope Client
    let rope_endpoint = bind_endpoint().await?;
    println!("[ROPE] Booting on Node ID: {}", rope_endpoint.id());
    
    let client = KnotClient::join(&ticket)
        .knot("living-room")
        .rope_id("camera-minimal")
        .endpoint(rope_endpoint)
        .tie()
        .await?;

    println!("[ROPE] Successfully joined session!");
    println!("[ROPE] Assigned Rope ID: {}", client.rope_id());
    println!("[ROPE] Connection ID   : {}", client.connection_id());

    Ok(())
}
