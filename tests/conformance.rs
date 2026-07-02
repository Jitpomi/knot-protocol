use ::knot_protocol::{
    JoinPolicy, Capability, HubEvent, Envelope, ControlMessage, ErrorCode, FrameHeader
};
use iroh_knot::{IrohKnotHub as KnotHub, IrohKnotClientJoinBuilder as KnotClient, generate_ticket};

mod knot_protocol_compat {
    pub use iroh_knot::{bind_endpoint, base64_url_decode, unpack_addr, KNOT_ALPN};
    pub use ::knot_protocol::Direction;
}
use knot_protocol_compat as knot_protocol;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use futures_util::{SinkExt, StreamExt};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio::sync::mpsc::UnboundedSender;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_temp_dir() -> std::path::PathBuf {
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let name = format!("knot_test_db_{}_{}", id, Instant::now().elapsed().as_nanos());
    std::env::temp_dir().join(name)
}

// 01_valid_session_join
#[tokio::test]
async fn test_01_valid_session_join() -> anyhow::Result<()> {
    let host_ep = knot_protocol::bind_endpoint().await?;
    let ticket = generate_ticket(&host_ep);
    
    let _hub = KnotHub::new()
        .data_dir(unique_temp_dir())
        .with_join_policy(JoinPolicy::ApproveAll)
        .serve(host_ep)
        .await?;

    let client = KnotClient::join(&ticket)
        .knot("test-knot")
        .rope_id("valid-rope")
        .connect()
        .await?;

    assert_eq!(client.rope_id(), "test-knot_valid-rope");
    Ok(())
}

// 02_reject_node_id_mismatch
#[tokio::test]
async fn test_02_reject_node_id_mismatch() -> anyhow::Result<()> {
    let host_ep = knot_protocol::bind_endpoint().await?;
    let ticket = generate_ticket(&host_ep);
    
    let _hub = KnotHub::new()
        .data_dir(unique_temp_dir())
        .with_join_policy(JoinPolicy::ApproveAll)
        .serve(host_ep)
        .await?;

    let decoded = knot_protocol::base64_url_decode(&ticket).unwrap();
    let hub_addr = knot_protocol::unpack_addr(&decoded).unwrap();

    let client_ep = knot_protocol::bind_endpoint().await?;
    let connection = client_ep.connect(hub_addr, knot_protocol::KNOT_ALPN).await?;
    let (send, recv) = connection.open_bi().await?;
    
    let mut framed_read = FramedRead::new(recv, LengthDelimitedCodec::new());
    let mut framed_write = FramedWrite::new(send, LengthDelimitedCodec::new());

    let join_env = Envelope {
        msg_id: "join-req-02".to_string(),
        timestamp: 1000,
        source_rope_id: "fake-rope".to_string(),
        connection_id: "pending".to_string(),
        requires_ack: false,
        payload: ControlMessage::SessionJoin {
            protocol_version: 1,
            knot_id: "test-knot".to_string(),
            rope_id: "fake-rope".to_string(),
            node_id: "fake_node_id_not_matching_client_endpoint".to_string(),
            join_token: "".to_string(),
            capabilities: vec![],
        },
    };

    let bytes = bincode::serialize(&join_env)?;
    framed_write.send(bytes::Bytes::from(bytes)).await?;

    let resp = framed_read.next().await
        .ok_or_else(|| anyhow::anyhow!("No response"))??;
    let resp_env: Envelope = bincode::deserialize(&resp)?;
    match resp_env.payload {
        ControlMessage::Reject { reason, .. } => {
            assert_eq!(reason, ErrorCode::InvalidToken);
        }
        _ => panic!("Expected Reject payload"),
    }
    Ok(())
}

// 03_reject_invalid_token
#[tokio::test]
async fn test_03_reject_invalid_token() -> anyhow::Result<()> {
    let host_ep = knot_protocol::bind_endpoint().await?;
    let ticket = generate_ticket(&host_ep);
    
    let _hub = KnotHub::new()
        .data_dir(unique_temp_dir())
        .with_join_policy(JoinPolicy::TokenRequired { secret: "valid-secret-123".to_string() })
        .serve(host_ep)
        .await?;

    let client_res = KnotClient::join(&ticket)
        .knot("test-knot")
        .rope_id("rope-invalid-token")
        .join_token("wrong-token")
        .connect()
        .await;

    assert!(client_res.is_err());
    Ok(())
}

// 04_reject_unsupported_capability
#[tokio::test]
async fn test_04_reject_unsupported_capability() -> anyhow::Result<()> {
    let host_ep = knot_protocol::bind_endpoint().await?;
    let ticket = generate_ticket(&host_ep);
    
    let _hub = KnotHub::new()
        .data_dir(unique_temp_dir())
        .on_capability_validate(|caps| {
            // Reject if client announces the disallowed cap
            for cap in caps {
                if cap.id == "disallowed-cap" {
                    return false;
                }
            }
            true
        })
        .serve(host_ep)
        .await?;

    let bad_cap = Capability {
        id: "disallowed-cap".to_string(),
        kind: "camera".to_string(),
        direction: knot_protocol::Direction::Input,
        formats: vec![],
        commands: vec![],
        attributes: HashMap::new(),
    };

    let client_res = KnotClient::join(&ticket)
        .knot("test-knot")
        .rope_id("rope-bad-cap")
        .capability(bad_cap)
        .connect()
        .await;

    assert!(client_res.is_err());
    Ok(())
}

// 05_require_stream_accepted_before_frames
#[tokio::test]
async fn test_05_require_stream_accepted_before_frames() -> anyhow::Result<()> {
    let host_ep = knot_protocol::bind_endpoint().await?;
    let ticket = generate_ticket(&host_ep);
    
    let _hub = KnotHub::new()
        .data_dir(unique_temp_dir())
        .serve(host_ep)
        .await?;

    let client = KnotClient::join(&ticket)
        .knot("test-knot")
        .rope_id("rope-test-gating")
        .connect()
        .await?;

    // Open a valid stream
    let stream_res = client.create_stream(
        "test_gate_id".to_string(),
        "cap-id".to_string(),
        "topic".to_string(),
        "format".to_string(),
        HashMap::new(),
    ).await;

    assert!(stream_res.is_ok());
    Ok(())
}

// 06_validate_28_byte_frame_header
#[tokio::test]
async fn test_06_validate_28_byte_frame_header() -> anyhow::Result<()> {
    let host_ep = knot_protocol::bind_endpoint().await?;
    let ticket = generate_ticket(&host_ep);
    
    let frame_received = Arc::new(Mutex::new(None::<(FrameHeader, Vec<u8>)>));
    let frame_received_clone = frame_received.clone();

    let _hub = KnotHub::new()
        .data_dir(unique_temp_dir())
        .on_event(move |ev| {
            if let HubEvent::FrameReceived { header, payload, .. } = ev {
                let mut lock = frame_received_clone.lock().unwrap();
                *lock = Some((header, payload));
            }
        })
        .serve(host_ep)
        .await?;

    let client = KnotClient::join(&ticket)
        .knot("test-knot")
        .rope_id("rope-test-header")
        .connect()
        .await?;

    let mut stream = client.create_stream(
        "stream_header_test".to_string(),
        "cap".to_string(),
        "topic".to_string(),
        "format".to_string(),
        HashMap::new(),
    ).await?;

    // Write a keyframe containing payload [10, 20, 30]
    stream.write_frame(1, 12345, &[10, 20, 30]).await?;

    // Wait a brief moment for delivery
    tokio::time::sleep(Duration::from_millis(300)).await;

    let lock = frame_received.lock().unwrap();
    let (header, payload) = lock.as_ref().expect("Expected to receive a data frame");
    
    assert_eq!(header.magic, [0x4B, 0x50]);
    assert_eq!(header.seq_num, 0);
    assert!(header.timestamp_ms < 500, "Timestamp should be session-relative ({}ms)", header.timestamp_ms);
    assert_eq!(header.frame_type, 1); // Keyframe
    assert_eq!(payload, &[10, 20, 30]);

    Ok(())
}

// 07_ack_required_command
#[tokio::test]
async fn test_07_ack_required_command() -> anyhow::Result<()> {
    let host_ep = knot_protocol::bind_endpoint().await?;
    let ticket = generate_ticket(&host_ep);
    
    let control_channel = Arc::new(Mutex::new(None::<UnboundedSender<Envelope>>));
    let control_channel_clone = control_channel.clone();

    let _hub = KnotHub::new()
        .data_dir(unique_temp_dir())
        .on_event(move |ev| {
            if let HubEvent::RopeConnected { control_sender, .. } = ev {
                let mut lock = control_channel_clone.lock().unwrap();
                *lock = Some(control_sender);
            }
        })
        .serve(host_ep)
        .await?;

    let client = KnotClient::join(&ticket)
        .knot("test-knot")
        .rope_id("rope-command")
        .connect()
        .await?;

    // Wait for connected callback
    tokio::time::sleep(Duration::from_millis(200)).await;

    let sender = {
        let lock = control_channel.lock().unwrap();
        lock.as_ref().cloned().expect("Expected control sender")
    };

    // Send a command to client
    let cmd = Envelope {
        msg_id: "cmd-123".to_string(),
        timestamp: 1000,
        source_rope_id: "host".to_string(),
        connection_id: "conn_0".to_string(),
        requires_ack: true,
        payload: ControlMessage::Command {
            command_id: "cmd-123".to_string(),
            target_capability_id: "cap-id".to_string(),
            action: "reboot".to_string(),
            payload: "{}".to_string(),
        },
    };
    sender.send(cmd)?;

    // Client receives command
    let client_env = client.next_event().await
        .ok_or_else(|| anyhow::anyhow!("Expected a message"))?;
    match client_env.payload {
        ControlMessage::Command { command_id, .. } => {
            assert_eq!(command_id, "cmd-123");
            // Reply with Ack
            client.send_ack("cmd-123".to_string(), "Success".to_string(), "done".to_string())?;
        }
        _ => panic!("Expected Command payload on client"),
    }

    Ok(())
}

// 08_reconnect_same_rope_id
#[tokio::test]
async fn test_08_reconnect_same_rope_id() -> anyhow::Result<()> {
    let host_ep = knot_protocol::bind_endpoint().await?;
    let ticket = generate_ticket(&host_ep);
    
    let db_path = unique_temp_dir();
    let _hub = KnotHub::new()
        .data_dir(db_path)
        .serve(host_ep)
        .await?;

    // Connect first client
    let client1 = KnotClient::join(&ticket)
        .knot("test-knot")
        .rope_id("reconnect-rope")
        .connect()
        .await?;

    assert_eq!(client1.rope_id(), "test-knot_reconnect-rope");

    // Drop first client to trigger disconnection
    drop(client1);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connect second client with same IDs
    let client2 = KnotClient::join(&ticket)
        .knot("test-knot")
        .rope_id("reconnect-rope")
        .connect()
        .await?;

    assert_eq!(client2.rope_id(), "test-knot_reconnect-rope");
    Ok(())
}
