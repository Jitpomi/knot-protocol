use knot_protocol::{KnotHub, JoinPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = knot_protocol::bind_endpoint().await?;
    let hub_node_id = endpoint.id();
    println!("[HOST] 🚀 Booting host on ALPN 'jitpomi/studio/1'. Node ID: {}", hub_node_id);

    let _hub = KnotHub::new()
        .with_join_policy(JoinPolicy::ApproveAll)
        .serve(endpoint)
        .await?;

    println!("[HOST] Listening for incoming Rope connections...");
    
    // Simulate keeping the host open for 3 seconds in this example
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    println!("[HOST] Shutting down.");
    Ok(())
}
