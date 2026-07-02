use knot_protocol::{KnotHub, KnotClient, JoinPolicy, generate_ticket};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host_endpoint = knot_protocol::bind_endpoint().await?;
    let ticket = generate_ticket(&host_endpoint);

    // Host enforces a token policy
    let _hub = KnotHub::new()
        .with_join_policy(JoinPolicy::TokenRequired {
            secret: "secure-auth-secret-123".to_string(),
        })
        .serve(host_endpoint)
        .await?;

    let rope_endpoint = knot_protocol::bind_endpoint().await?;

    println!("[ROPE] Attempting to connect with an INVALID join token...");
    let join_res = KnotClient::join(&ticket)
        .knot("office")
        .rope_id("laptop")
        .join_token("wrong-secret-token")
        .endpoint(rope_endpoint)
        .connect()
        .await;

    match join_res {
        Ok(_) => panic!("Connection should have failed!"),
        Err(e) => {
            println!("[ROPE] Connection rejected successfully! Error details:");
            println!("       {}", e);
        }
    }

    Ok(())
}
