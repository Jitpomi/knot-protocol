use knot_protocol::{JoinPolicy, HubEvent};
use iroh_knot::{IrohKnotHub as KnotHub, IrohKnotClientJoinBuilder as KnotClient, bind_endpoint, generate_ticket};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host_endpoint = bind_endpoint().await?;
    let ticket = generate_ticket(&host_endpoint);

    let _hub = KnotHub::new()
        .with_join_policy(JoinPolicy::ApproveAll)
        .on_event(|event| {
            match event {
                HubEvent::RopeConnected { rope_id, node_id, .. } => {
                    println!("[HOST] Rope '{}' joined/reconnected. (Node ID: {})", rope_id, node_id);
                }
                HubEvent::RopeDisconnected { rope_id } => {
                    println!("[HOST] Rope '{}' disconnected. Offline grace timer initiated...", rope_id);
                }
                _ => {}
            }
        })
        .serve(host_endpoint)
        .await?;

    let rope_endpoint = bind_endpoint().await?;

    // 1. Establish initial connection
    println!("[ROPE] Connecting first instance...");
    let client1 = KnotClient::join(&ticket)
        .knot("office")
        .rope_id("laptop")
        .endpoint(rope_endpoint.clone())
        .connect()
        .await?;
    println!("[ROPE] Client 1 Connection ID: {}", client1.connection_id());

    // 2. Simulate drop/disconnect
    println!("[ROPE] Simulating connection drop (dropping Client 1)...");
    drop(client1);

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // 3. Reconnect before grace timer expires using same node_id and rope_id
    println!("[ROPE] Reconnecting Client 2...");
    let client2 = KnotClient::join(&ticket)
        .knot("office")
        .rope_id("laptop")
        .endpoint(rope_endpoint)
        .connect()
        .await?;
    println!("[ROPE] Client 2 Connection ID: {}", client2.connection_id());

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    Ok(())
}
