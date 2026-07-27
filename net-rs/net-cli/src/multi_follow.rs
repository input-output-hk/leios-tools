//! Multi-peer chain follower: connects to N nodes via the coordinator
//! and prints aggregated chain events.

use std::time::{Duration, Instant};

use net_core::multi_peer::types::{NetworkCommand, NetworkEvent};
use net_core::multi_peer::{spawn_coordinator, CoordinatorConfig};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    hosts: &[String],
    magic: u64,
    max_peers: usize,
    listen: Option<String>,
    duplex: bool,
    leios: bool,
    fetch_eb: bool,
    fetch_eb_txs: bool,
    max_handshaking: usize,
    max_connections_per_ip: usize,
    scheduler_args: &crate::scheduler_args::SchedulerArgs,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if hosts.is_empty() && listen.is_none() {
        return Err("at least one --host or --listen is required".into());
    }

    let config = CoordinatorConfig {
        network_magic: magic,
        max_peers,
        keepalive_interval: Duration::from_secs(20),
        sdu_timeout: Duration::from_secs(900),
        listen_address: listen.clone(),
        chain_store_capacity: 2160,
        block_body_retention_blocks: None,
        duplex,
        leios_enabled: leios,
        leios_dedup_window: 1000,
        leios_store_stats_log_interval: 0,
        traffic_class_overrides: scheduler_args.traffic_class_overrides()?,
        scheduler_type: scheduler_args.scheduler,
        max_handshaking,
        max_connections_per_ip,
        peer_delays: std::collections::HashMap::new(),
        tx_body_resolver: None,
        peer_rtt_observer: None,
        outbound_controls: None,
    };

    let mut handle = spawn_coordinator(config);

    // Add initial peers.
    for host in hosts {
        handle
            .commands
            .send(NetworkCommand::AddPeer {
                address: host.clone(),
            })
            .await?;
    }

    let listen_info = listen
        .map(|a| format!(", listening on {a}"))
        .unwrap_or_default();
    println!(
        "following chain from {} peer(s){listen_info}...",
        hosts.len()
    );
    let mut last_block_time = Instant::now();

    // Event loop.
    while let Some(event) = handle.events.recv().await {
        match event {
            NetworkEvent::PeerConnected { peer_id, address } => {
                println!("  {peer_id} connected: {address}");
            }
            NetworkEvent::PeerDisconnected { peer_id, reason } => {
                println!("  {peer_id} disconnected: {reason}");
            }
            NetworkEvent::TipAdvanced { tip, .. } => {
                let elapsed = last_block_time.elapsed();
                last_block_time = Instant::now();
                println!(
                    "  block #{:<8} {}  +{:.1}s",
                    tip.block_no,
                    tip.point,
                    elapsed.as_secs_f64(),
                );
            }
            NetworkEvent::RolledBack { point, tip, .. } => {
                println!("  rollback to {point}  tip: {tip}");
            }
            NetworkEvent::BlockReceived { point, body } => {
                println!("  block received: {} ({} bytes)", point, body.raw.len());
            }
            NetworkEvent::PeersDiscovered { from, peers } => {
                println!("  discovered {} peer(s) from {from}:", peers.len());
                for peer in &peers {
                    println!("    {peer}");
                }
            }
            NetworkEvent::TransactionReceived { peer_id, body, .. } => {
                println!("  tx received from {peer_id} ({} bytes)", body.len());
            }
            NetworkEvent::LeiosBlockAnnounced { .. } => {
                println!("  leios: EB announced via RB header");
            }
            NetworkEvent::LeiosBlockOffered { peer_id, point } => {
                println!("  leios: EB offered at {point} by {peer_id}");
                if fetch_eb {
                    // Construct a LeiosFetch (MsgLeiosBlockRequest) for the
                    // offered EB so its MsgLeiosBlock reply can be captured.
                    let _ = handle
                        .commands
                        .send(NetworkCommand::FetchLeiosBlock { peer_id, point })
                        .await;
                }
            }
            NetworkEvent::LeiosBlockTxsOffered { peer_id, point } => {
                println!("  leios: EB transactions offered at {point} by {peer_id}");
                if fetch_eb_txs {
                    // Request the first chunk of txs (indices 0..63) so the
                    // MsgLeiosBlockTxs reply can be captured.
                    let bitmap = std::collections::BTreeMap::from([(0u16, u64::MAX)]);
                    let _ = handle
                        .commands
                        .send(NetworkCommand::FetchLeiosBlockTxs {
                            peer_id,
                            point,
                            bitmap,
                        })
                        .await;
                }
            }
            NetworkEvent::LeiosBlockReceived { point, block, .. } => {
                println!("  leios: EB received at {point} ({} bytes)", block.len());
            }
            NetworkEvent::LeiosVotesReceived { peer_id, votes } => {
                // Surface per-vote voter_id (the CIP-0164 committee seat index,
                // word16) and the announcing-RB hash prefix so callers can see
                // *which* committee seats are voting, not just how many votes.
                // voter_id decodes independently of BLS-signature validity.
                for v in &votes {
                    println!(
                        "  leios: vote from {peer_id} voter_id={} rb={:02x}{:02x}{:02x}{:02x}",
                        v.voter_id,
                        v.announcing_rb_hash[0],
                        v.announcing_rb_hash[1],
                        v.announcing_rb_hash[2],
                        v.announcing_rb_hash[3],
                    );
                }
                println!("  leios: {} vote(s) received from {peer_id}", votes.len());
            }
            NetworkEvent::LeiosBlockTxsReceived {
                peer_id,
                point,
                transactions,
            } => {
                println!(
                    "  leios: EB txs received from {peer_id} at {point} ({} txs)",
                    transactions.len()
                );
            }
            NetworkEvent::BlockFetchFailed { from, to, .. } => {
                if from == to {
                    println!("  block fetch failed: {from}");
                } else {
                    println!("  block fetch failed: {from}..{to}");
                }
            }
            NetworkEvent::TxsRequested { .. } => {} // handled by net-node, not net-cli
            NetworkEvent::PeerSnapshot { .. } | NetworkEvent::IntersectionFound { .. } => {}
        }
    }

    Ok(())
}
