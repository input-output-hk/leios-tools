//! Duplex peer task: runs both initiator (client) and responder (server)
//! protocols on a single connection.
//!
//! Reuses the client sub-tasks from `peer_task` alongside the server
//! handlers from `server_handlers` on one mux, using `connect_duplex()`
//! or `accept_duplex()` for connection setup.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use crate::multi_peer::SyncMethodConfig;
use crate::mux::scheduler::TrafficClass;
use crate::mux::MuxConfig;
use crate::protocols::peersharing::PeerAddress;
use crate::protocols::txsubmission::PendingTx;
use crate::types::Point;

use super::command_dispatch::{dispatch_command, ClientProtocolSenders};
use super::connect::{self, DuplexConnection};
use super::peer_task::server_protocol_configs;
use super::peer_task::{
    client_protocol_configs, spawn_blockfetch, spawn_chainsync, spawn_keepalive, spawn_leios_fetch,
    spawn_leios_notify, spawn_peersharing, spawn_txsubmission, LeiosFetchCommand,
};
use super::server_handlers;
use super::types::{PeerCommand, PeerEvent};
use super::PeerId;
use crate::store::chain_store::ChainStore;
use crate::store::leios_store::LeiosStore;

/// Configuration for a duplex peer task that connects outbound.
pub(crate) struct DuplexTaskConfig {
    pub peer_id: PeerId,
    pub address: String,
    pub network_magic: u64,
    pub keepalive_interval: Duration,
    pub sdu_timeout: Duration,
    pub sync_method: SyncMethodConfig,
    pub chain_store: Arc<ChainStore>,
    pub peer_provider: Arc<dyn Fn(u8) -> Vec<PeerAddress> + Send + Sync>,
    pub event_sender: mpsc::Sender<(PeerId, PeerEvent)>,
    pub command_receiver: mpsc::Receiver<PeerCommand>,
    pub leios_enabled: bool,
    pub leios_store: Option<Arc<LeiosStore>>,
    pub traffic_class_overrides: std::collections::HashMap<u16, TrafficClass>,
    pub scheduler_type: crate::mux::scheduler::SchedulerType,
    /// Optional behaviour handle consulted by the ChainSync server's
    /// per-peer RB-header advertisement.  `None` = no transform.
    pub outbound_controls: Option<crate::peer::server_handlers::OutboundControls>,
    /// Node-wide tx-submission dedup set, shared across all peers so a tx
    /// pulled from one peer isn't re-fetched from every other.
    pub tx_dedup: Arc<Mutex<crate::peer::server_handlers::TxDedup>>,
}

/// Configuration for a duplex peer task from an already-accepted connection.
pub(crate) struct AcceptedDuplexTaskConfig {
    pub peer_id: PeerId,
    pub connection: DuplexConnection,
    pub keepalive_interval: Duration,
    pub sync_method: SyncMethodConfig,
    pub chain_store: Arc<ChainStore>,
    pub peer_provider: Arc<dyn Fn(u8) -> Vec<PeerAddress> + Send + Sync>,
    pub event_sender: mpsc::Sender<(PeerId, PeerEvent)>,
    pub command_receiver: mpsc::Receiver<PeerCommand>,
    pub leios_enabled: bool,
    pub leios_store: Option<Arc<LeiosStore>>,
    /// See [`DuplexTaskConfig::outbound_controls`].
    pub outbound_controls: Option<crate::peer::server_handlers::OutboundControls>,
    /// See [`DuplexTaskConfig::tx_dedup`].
    pub tx_dedup: Arc<Mutex<crate::peer::server_handlers::TxDedup>>,
}

/// Run a duplex peer task. Connects outbound, then runs both client and
/// server protocol sub-tasks on the same mux.
pub(crate) async fn run_duplex_task(config: DuplexTaskConfig) {
    let peer_id = config.peer_id;
    let event_sender = config.event_sender.clone();

    // Connect with duplex registration.
    let mux_config = MuxConfig {
        sdu_timeout: config.sdu_timeout,
        ..MuxConfig::default()
    };

    let mut client_protos = client_protocol_configs(config.leios_enabled);
    let mut server_protos = server_protocol_configs(config.leios_enabled);
    for p in client_protos.iter_mut().chain(server_protos.iter_mut()) {
        if let Some(tc) = config.traffic_class_overrides.get(&p.id) {
            p.traffic_class = *tc;
        }
    }

    let conn: DuplexConnection = match connect::connect_duplex(
        &config.address,
        config.network_magic,
        &client_protos,
        &server_protos,
        mux_config,
        config.scheduler_type,
    )
    .await
    {
        Ok(conn) => conn,
        Err(e) => {
            let reason = format!("connect duplex: {e}");
            let _ = event_sender
                .send((peer_id, PeerEvent::Failed { reason }))
                .await;
            return;
        }
    };

    // Record the concrete IP this connection landed on — `config.address`
    // may be a round-robin DNS name, so the resolved peer differs per
    // (re)connection.  Lets per-peer Leios logs (keyed by peer_id) be
    // attributed to a specific backend.
    tracing::info!(
        %peer_id,
        host = %config.address,
        resolved_ip = %conn.resolved_addr,
        "peer connected (duplex)"
    );

    let downstream = super::new_downstream_flag();
    let _ = event_sender
        .send((
            peer_id,
            PeerEvent::Connected {
                mux_stats: conn.running.stats.clone(),
                downstream: downstream.clone(),
                peer_sharing: conn.peer_sharing,
            },
        ))
        .await;

    run_duplex_protocols(
        conn,
        DuplexProtocolParams {
            peer_id: config.peer_id,
            keepalive_interval: config.keepalive_interval,
            leios_enabled: config.leios_enabled,
            sync_method: config.sync_method,
            chain_store: config.chain_store,
            peer_provider: config.peer_provider,
            leios_store: config.leios_store,
            event_sender: config.event_sender,
            command_receiver: config.command_receiver,
            outbound_controls: config.outbound_controls,
            downstream,
            tx_dedup: config.tx_dedup,
        },
    )
    .await;
}

/// Run a duplex peer task from an already-accepted connection.
pub(crate) async fn run_accepted_duplex_task(config: AcceptedDuplexTaskConfig) {
    let peer_id = config.peer_id;
    let event_sender = config.event_sender.clone();
    let conn = config.connection;

    let downstream = super::new_downstream_flag();
    let _ = event_sender
        .send((
            peer_id,
            PeerEvent::Connected {
                mux_stats: conn.running.stats.clone(),
                downstream: downstream.clone(),
                peer_sharing: conn.peer_sharing,
            },
        ))
        .await;

    run_duplex_protocols(
        conn,
        DuplexProtocolParams {
            peer_id: config.peer_id,
            keepalive_interval: config.keepalive_interval,
            leios_enabled: config.leios_enabled,
            sync_method: config.sync_method,
            chain_store: config.chain_store,
            peer_provider: config.peer_provider,
            leios_store: config.leios_store,
            event_sender: config.event_sender,
            command_receiver: config.command_receiver,
            outbound_controls: config.outbound_controls,
            downstream,
            tx_dedup: config.tx_dedup,
        },
    )
    .await;
}

/// Shared parameters for duplex protocol wiring.
struct DuplexProtocolParams {
    peer_id: PeerId,
    keepalive_interval: Duration,
    leios_enabled: bool,
    sync_method: SyncMethodConfig,
    chain_store: Arc<ChainStore>,
    peer_provider: Arc<dyn Fn(u8) -> Vec<PeerAddress> + Send + Sync>,
    leios_store: Option<Arc<LeiosStore>>,
    event_sender: mpsc::Sender<(PeerId, PeerEvent)>,
    command_receiver: mpsc::Receiver<PeerCommand>,
    outbound_controls: Option<crate::peer::server_handlers::OutboundControls>,
    downstream: super::DownstreamFlag,
    tx_dedup: Arc<Mutex<crate::peer::server_handlers::TxDedup>>,
}

/// Shared protocol wiring for duplex connections (both outbound and accepted).
async fn run_duplex_protocols(conn: DuplexConnection, params: DuplexProtocolParams) {
    let DuplexProtocolParams {
        peer_id,
        keepalive_interval,
        leios_enabled,
        sync_method,
        chain_store,
        peer_provider,
        leios_store,
        event_sender,
        mut command_receiver,
        outbound_controls,
        downstream,
        tx_dedup,
    } = params;
    // --- Initiator (client) sub-tasks ---
    let mut init_channels = conn.initiator_channels.into_iter();
    let (cs_send, cs_recv) = init_channels.next().expect("chainsync initiator channel");
    let (ka_send, ka_recv) = init_channels.next().expect("keepalive initiator channel");
    let (bf_send, bf_recv) = init_channels.next().expect("blockfetch initiator channel");
    let (ps_send, ps_recv) = init_channels.next().expect("peersharing initiator channel");
    let (ts_send, ts_recv) = init_channels
        .next()
        .expect("txsubmission initiator channel");

    // See peer_task.rs for sizing rationale: small caps here back-pressure
    // the dispatch loop's `.send().await`, which stalls the per-peer
    // command channel and trips "command channel full" disconnects.
    let (fetch_sender, fetch_receiver) = mpsc::channel::<(Point, Point)>(256);
    let (peer_share_sender, peer_share_receiver) = mpsc::channel::<u8>(16);
    let (tx_submit_sender, tx_submit_receiver) = mpsc::channel::<PendingTx>(1024);
    let (cs_reintersect_sender, cs_reintersect_receiver) = mpsc::channel::<()>(16);

    let mut cs_client = spawn_chainsync(
        cs_send,
        cs_recv,
        peer_id,
        sync_method,
        chain_store.clone(),
        event_sender.clone(),
        cs_reintersect_receiver,
    );
    let mut ka_client = spawn_keepalive(
        ka_send,
        ka_recv,
        peer_id,
        keepalive_interval,
        event_sender.clone(),
    );
    let bf_client = spawn_blockfetch(
        bf_send,
        bf_recv,
        peer_id,
        fetch_receiver,
        event_sender.clone(),
    );
    let ps_client = spawn_peersharing(
        ps_send,
        ps_recv,
        peer_id,
        peer_share_receiver,
        event_sender.clone(),
    );
    let ts_client = spawn_txsubmission(
        ts_send,
        ts_recv,
        peer_id,
        tx_submit_receiver,
        event_sender.clone(),
    );

    // Conditionally spawn Leios client sub-tasks.
    let leios_client_handles = if leios_enabled {
        let (ln_send, ln_recv) = init_channels
            .next()
            .expect("leios_notify initiator channel");
        let (lf_send, lf_recv) = init_channels.next().expect("leios_fetch initiator channel");
        let (lf_cmd_sender, lf_cmd_receiver) = mpsc::channel::<LeiosFetchCommand>(256);
        let ln_handle = spawn_leios_notify(ln_send, ln_recv, peer_id, event_sender.clone());
        let lf_handle = spawn_leios_fetch(
            lf_send,
            lf_recv,
            peer_id,
            lf_cmd_receiver,
            event_sender.clone(),
        );
        Some((ln_handle, lf_handle, lf_cmd_sender))
    } else {
        None
    };

    // --- Responder (server) sub-tasks ---
    let mut resp_channels = conn.responder_channels.into_iter();
    let (cs_srv_send, cs_srv_recv) = resp_channels.next().expect("chainsync responder channel");
    let (bf_srv_send, bf_srv_recv) = resp_channels.next().expect("blockfetch responder channel");
    let (ts_srv_send, ts_srv_recv) = resp_channels
        .next()
        .expect("txsubmission responder channel");
    let (ps_srv_send, ps_srv_recv) = resp_channels.next().expect("peersharing responder channel");
    let (ka_srv_send, ka_srv_recv) = resp_channels.next().expect("keepalive responder channel");

    let cs_server = tokio::spawn(server_handlers::serve_chainsync(
        cs_srv_send,
        cs_srv_recv,
        chain_store.clone(),
        peer_id,
        outbound_controls.clone(),
        downstream.clone(),
    ));
    let bf_server = tokio::spawn(server_handlers::serve_blockfetch(
        bf_srv_send,
        bf_srv_recv,
        chain_store.clone(),
        peer_id,
        outbound_controls.clone(),
        downstream.clone(),
    ));
    let ts_server = tokio::spawn(server_handlers::serve_txsubmission(
        ts_srv_send,
        ts_srv_recv,
        peer_id,
        event_sender.clone(),
        tx_dedup.clone(),
    ));
    let ps_server = tokio::spawn(server_handlers::serve_peersharing(
        ps_srv_send,
        ps_srv_recv,
        peer_id,
        peer_provider,
    ));
    let mut ka_server = tokio::spawn(server_handlers::serve_keepalive(
        ka_srv_send,
        ka_srv_recv,
        peer_id,
        downstream.clone(),
    ));

    // Conditionally spawn Leios server sub-tasks.
    let mut leios_server_handles = if leios_enabled {
        let (ln_srv_send, ln_srv_recv) = resp_channels
            .next()
            .expect("leios_notify responder channel");
        let (lf_srv_send, lf_srv_recv) =
            resp_channels.next().expect("leios_fetch responder channel");
        let store = leios_store.expect("leios_store required when leios_enabled");
        let ln_outbound_controls = outbound_controls.clone();
        let ln_store = store.clone();
        let ln_server = tokio::spawn(async move {
            server_handlers::serve_leios_notify(
                ln_srv_send,
                ln_srv_recv,
                ln_store,
                peer_id,
                ln_outbound_controls.as_ref(),
            )
            .await
        });
        let lf_server = tokio::spawn(server_handlers::serve_leios_fetch(
            lf_srv_send,
            lf_srv_recv,
            store,
            peer_id,
        ));
        Some((ln_server, lf_server))
    } else {
        None
    };

    // Cancellation-safe teardown: register every sub-task's AbortHandle in
    // a guard so they are reaped no matter how this task exits.  The
    // explicit cleanup at the end of the loop runs on the normal path; if
    // this task is instead cancelled mid-`select!` (the coordinator aborts
    // our JoinHandle), that cleanup is skipped and dropped JoinHandles only
    // detach — the guard's Drop is what actually stops the sub-tasks then.
    // See `super::AbortGuard`.
    let mut guard = super::AbortGuard::new();
    guard.push(cs_client.abort_handle());
    guard.push(ka_client.abort_handle());
    guard.push(bf_client.abort_handle());
    guard.push(ps_client.abort_handle());
    guard.push(ts_client.abort_handle());
    if let Some((ln_handle, lf_handle, _)) = &leios_client_handles {
        guard.push(ln_handle.abort_handle());
        guard.push(lf_handle.abort_handle());
    }
    guard.push(cs_server.abort_handle());
    guard.push(bf_server.abort_handle());
    guard.push(ts_server.abort_handle());
    guard.push(ps_server.abort_handle());
    guard.push(ka_server.abort_handle());
    if let Some((ln_server, lf_server)) = &leios_server_handles {
        guard.push(ln_server.abort_handle());
        guard.push(lf_server.abort_handle());
    }

    // Build shared command senders for dispatch.
    let senders = ClientProtocolSenders {
        peer_id,
        fetch: fetch_sender,
        peer_share: peer_share_sender,
        tx_submit: tx_submit_sender,
        leios_fetch: leios_client_handles.as_ref().map(|(_, _, lf)| lf.clone()),
        chainsync_reintersect: cs_reintersect_sender,
    };

    // Main select loop: dispatch commands and detect liveness-critical
    // sub-task exit.  We watch:
    // - cs_client (our outbound chainsync) — if it dies, the connection
    //   is effectively useless to us as a follower.
    // - ka_client (our outbound keepalive) — we promised the relay
    //   periodic pings; if we stop sending, they will eventually drop
    //   us anyway, so tear down promptly.
    // - ka_server (our inbound keepalive responder) — the keepalive
    //   liveness watchdog.  If it exits (e.g. 97s without an inbound
    //   MsgKeepAlive after the relay activated the protocol), that's
    //   the relay's side of the keepalive going silent.  Per spec we
    //   must drop the connection.
    // A clean Quit ends only LeiosNotify. Stop polling its completed handle;
    // errors (including exhausted history) must tear down the shared bearer.
    let mut watch_leios_notify = leios_server_handles.is_some();
    let teardown_reason: String;
    loop {
        tokio::select! {
            cmd = command_receiver.recv() => {
                let was_none = cmd.is_none();
                let was_disconnect = matches!(cmd, Some(PeerCommand::Disconnect));
                if !dispatch_command(cmd, &senders).await {
                    teardown_reason = if was_none {
                        "command channel closed".to_string()
                    } else if was_disconnect {
                        "disconnect requested".to_string()
                    } else {
                        "command dispatch failed".to_string()
                    };
                    break;
                }
            }
            result = async {
                match &mut leios_server_handles {
                    Some((notify, _)) => notify.await,
                    None => std::future::pending().await,
                }
            }, if watch_leios_notify => {
                match result {
                    Ok(Ok(())) => watch_leios_notify = false,
                    Ok(Err(error)) => {
                        teardown_reason = format!("leios_notify responder: {error}");
                        break;
                    }
                    Err(error) => {
                        teardown_reason = format!("leios_notify responder task failed: {error}");
                        break;
                    }
                }
            }
            result = &mut cs_client => {
                teardown_reason = match &result {
                    Err(e) => format!("chainsync client panicked: {e}"),
                    Ok(_) => "chainsync client exited".to_string(),
                };
                if let Err(e) = result {
                    tracing::warn!("peer {peer_id}: chainsync client panicked: {e}");
                }
                break;
            }
            result = &mut ka_client => {
                teardown_reason = match &result {
                    Err(e) => format!("keepalive client panicked: {e}"),
                    Ok(_) => "keepalive client exited".to_string(),
                };
                if let Err(e) = result {
                    tracing::warn!("peer {peer_id}: keepalive client panicked: {e}");
                } else {
                    tracing::warn!(
                        "peer {peer_id}: keepalive client exited — tearing down connection"
                    );
                }
                break;
            }
            result = &mut ka_server => {
                teardown_reason = match &result {
                    Err(e) => format!("keepalive responder panicked: {e}"),
                    Ok(_) => "keepalive responder exited (liveness watchdog)".to_string(),
                };
                if let Err(e) = result {
                    tracing::warn!("peer {peer_id}: keepalive server panicked: {e}");
                } else {
                    tracing::warn!(
                        "peer {peer_id}: keepalive responder exited (liveness watchdog) — tearing down connection"
                    );
                }
                break;
            }
        }
    }

    // Deterministically tell the coordinator this peer is gone so it
    // removes and reconnects it. Previously teardown relied on an aborted
    // sub-task racing a mux-error `Failed` event through `event_sender`
    // first; when teardown aborted every sub-task before any event escaped
    // (e.g. the keepalive liveness watchdog firing on a silent relay), the
    // coordinator never learned the peer died — leaving a zombie peer in
    // its table that was never removed or reconnected. `remove_peer` is
    // idempotent, so a duplicate `Failed` from a racing sub-task is safe.
    let _ = event_sender
        .send((
            peer_id,
            PeerEvent::Failed {
                reason: teardown_reason,
            },
        ))
        .await;

    // Clean up all sub-tasks.
    cs_client.abort();
    ka_client.abort();
    bf_client.abort();
    ps_client.abort();
    ts_client.abort();
    if let Some((ln_handle, lf_handle, _)) = leios_client_handles {
        ln_handle.abort();
        lf_handle.abort();
    }
    cs_server.abort();
    bf_server.abort();
    ts_server.abort();
    ps_server.abort();
    ka_server.abort();
    if let Some((ln_handle, lf_handle)) = leios_server_handles {
        ln_handle.abort();
        lf_handle.abort();
    }
    conn.running.abort();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bearer::mem::MemBearer;
    use crate::mux::scheduler::PriorityWfq;
    use crate::mux::{CodecRecv, CodecSend, Mux, RunningMux, MODE_INITIATOR, MODE_RESPONDER};
    use crate::protocols::keepalive::{self, Message as KaMsg};
    use crate::protocols::leios_notify::{self, Message as LnMsg};
    use crate::store::leios_store::MAX_NOTIFICATIONS;

    struct NotifyPeer {
        notify: (CodecSend, CodecRecv),
        keepalive: (CodecSend, CodecRecv),
        events: mpsc::Receiver<(PeerId, PeerEvent)>,
        peer: tokio::task::JoinHandle<()>,
        remote_mux: RunningMux,
        // Keep every registered direction alive, even when this test does not
        // drive it. Dropping all protocol channels would mask missing teardown.
        _channels: Vec<(CodecSend, CodecRecv)>,
        _commands: mpsc::Sender<PeerCommand>,
        _guard: super::super::AbortGuard,
    }

    impl NotifyPeer {
        fn open(store: Arc<LeiosStore>) -> Self {
            let config = MuxConfig {
                sdu_timeout: Duration::from_secs(120),
                ..MuxConfig::default()
            };
            let mut local = Mux::new(config.clone(), PriorityWfq::default(), MODE_INITIATOR);
            let mut remote = Mux::new(config, PriorityWfq::default(), MODE_RESPONDER);
            let mut initiator_channels = Vec::new();
            let mut responder_channels = Vec::new();
            let mut channels = Vec::new();
            let mut guard = super::super::AbortGuard::new();
            let (chain, _) = ChainStore::new(10);
            for protocol in client_protocol_configs(true) {
                let (send, recv) = local.register_with_mode(&protocol, MODE_INITIATOR);
                initiator_channels.push((CodecSend::new(send), CodecRecv::new(recv)));
                let (send, recv) = remote.register_with_mode(&protocol, MODE_RESPONDER);
                let (send, recv) = (CodecSend::new(send), CodecRecv::new(recv));
                // Keep the peer's monitored tasks healthy throughout the test.
                let task = match protocol.id {
                    crate::protocols::chainsync::PROTOCOL_ID => {
                        tokio::spawn(server_handlers::serve_chainsync(
                            send,
                            recv,
                            chain.clone(),
                            PeerId(1),
                            None,
                            super::super::new_downstream_flag(),
                        ))
                    }
                    keepalive::PROTOCOL_ID => tokio::spawn(server_handlers::serve_keepalive(
                        send,
                        recv,
                        PeerId(1),
                        super::super::new_downstream_flag(),
                    )),
                    _ => {
                        channels.push((send, recv));
                        continue;
                    }
                };
                guard.push(task.abort_handle());
            }
            let mut notify = None;
            let mut keepalive = None;
            for protocol in server_protocol_configs(true) {
                let (send, recv) = local.register_with_mode(&protocol, MODE_RESPONDER);
                responder_channels.push((CodecSend::new(send), CodecRecv::new(recv)));
                let (send, recv) = remote.register_with_mode(&protocol, MODE_INITIATOR);
                let pair = (CodecSend::new(send), CodecRecv::new(recv));
                match protocol.id {
                    leios_notify::PROTOCOL_ID => notify = Some(pair),
                    keepalive::PROTOCOL_ID => keepalive = Some(pair),
                    _ => channels.push(pair),
                }
            }
            let (local_bearer, remote_bearer) = MemBearer::pair();
            let connection = DuplexConnection {
                running: local.run(local_bearer),
                initiator_channels,
                responder_channels,
                resolved_addr: "127.0.0.1:3001".parse().unwrap(),
                peer_sharing: 1,
            };
            let remote_mux = remote.run(remote_bearer);
            let (event_sender, events) = mpsc::channel(1024);
            let (commands, command_receiver) = mpsc::channel(16);
            let peer = tokio::spawn(run_duplex_protocols(
                connection,
                DuplexProtocolParams {
                    peer_id: PeerId(7),
                    keepalive_interval: Duration::from_secs(10),
                    leios_enabled: true,
                    sync_method: SyncMethodConfig::Genesis,
                    chain_store: chain,
                    peer_provider: Arc::new(|_| Vec::new()),
                    leios_store: Some(store),
                    event_sender,
                    command_receiver,
                    outbound_controls: None,
                    downstream: super::super::new_downstream_flag(),
                    tx_dedup: Arc::new(Mutex::new(server_handlers::TxDedup::new(16))),
                },
            ));
            Self {
                notify: notify.unwrap(),
                keepalive: keepalive.unwrap(),
                events,
                peer,
                remote_mux,
                _channels: channels,
                _commands: commands,
                _guard: guard,
            }
        }

        async fn ping(&mut self, cookie: u16) {
            self.keepalive
                .0
                .send(&KaMsg::MsgKeepAlive { cookie })
                .await
                .unwrap();
            assert_eq!(
                tokio::time::timeout(
                    Duration::from_secs(1),
                    self.keepalive.1.recv::<KaMsg>(keepalive::SIZE_LIMIT)
                )
                .await
                .unwrap()
                .unwrap(),
                KaMsg::MsgKeepAliveResponse { cookie },
            );
        }

        async fn request(&self) {
            self.notify
                .0
                .send(&LnMsg::MsgLeiosNotificationRequestNext)
                .await
                .unwrap();
        }
    }

    impl Drop for NotifyPeer {
        fn drop(&mut self) {
            self.peer.abort();
            self.remote_mux.abort();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn leios_notify_history_exhaustion_tears_down_duplex_peer() {
        let (store, _) = LeiosStore::new(1);
        let mut remote = NotifyPeer::open(store.clone());
        remote.ping(1).await;
        remote.request().await;
        // Wait for the initial idle response to prove the responder snapped
        // its cursor before populating history. Other protocols remain active.
        let first = tokio::time::timeout(
            Duration::from_secs(60),
            remote.notify.1.recv::<LnMsg>(leios_notify::SIZE_LIMIT),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(first, LnMsg::MsgLeiosVotes { votes } if votes.is_empty()));
        tokio::time::timeout(Duration::from_secs(5), async {
            for id in 0..MAX_NOTIFICATIONS {
                let mut hash = [0; 32];
                hash[..8].copy_from_slice(&(id as u64).to_be_bytes());
                let point = Point::Specific { slot: 42, hash };
                store.inject_block(point, vec![0xa0], None);
                remote.request().await;
                assert!(matches!(
                    remote
                        .notify
                        .1
                        .recv::<LnMsg>(leios_notify::SIZE_LIMIT)
                        .await
                        .unwrap(),
                    LnMsg::MsgLeiosBlockOffer { .. }
                ));
            }
            remote.ping(2).await;
            remote.request().await;
            // A new block overflows the per-connection history even though
            // the store's own capacity is only one and the slot has not moved.
            store.inject_block(
                Point::Specific {
                    slot: 42,
                    hash: [255; 32],
                },
                vec![0xa0],
                None,
            );
            loop {
                let (peer, event) = remote
                    .events
                    .recv()
                    .await
                    .expect("peer must report failure");
                if let PeerEvent::Failed { reason } = event {
                    assert_eq!(peer, PeerId(7));
                    assert_eq!(
                        reason,
                        "leios_notify responder: connection history capacity exceeded"
                    );
                    break;
                }
            }
            (&mut remote.peer).await.unwrap();
            // The remote must observe the shared bearer closing, not hang on
            // its unanswered request while KeepAlive sustains the connection.
            assert!(remote
                .notify
                .1
                .recv::<LnMsg>(leios_notify::SIZE_LIMIT)
                .await
                .is_err());
        })
        .await
        .expect("history exhaustion must report failure and close the connection");
    }

    #[tokio::test(start_paused = true)]
    async fn leios_notify_quit_keeps_duplex_connection_alive() {
        for requests in [0, 3] {
            let (store, _) = LeiosStore::new(1);
            let mut remote = NotifyPeer::open(store);
            remote.ping(1).await;
            tokio::time::timeout(Duration::from_secs(1), async {
                for _ in 0..requests {
                    remote.request().await;
                }
                remote.notify.0.send(&LnMsg::MsgQuit).await.unwrap();
                for _ in 0..requests {
                    assert!(matches!(
                        remote
                            .notify
                            .1
                            .recv::<LnMsg>(leios_notify::SIZE_LIMIT)
                            .await
                            .unwrap(),
                        LnMsg::MsgCanceled
                    ));
                }
                assert!(matches!(
                    remote
                        .notify
                        .1
                        .recv::<LnMsg>(leios_notify::SIZE_LIMIT)
                        .await
                        .unwrap(),
                    LnMsg::MsgDone
                ));
            })
            .await
            .expect("clean shutdown must drain all outstanding requests");
            // Give the owner time to consume the completed JoinHandle, then
            // check it neither tears down nor polls that handle a second time.
            for cookie in 2..5 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                remote.ping(cookie).await;
                assert!(!remote.peer.is_finished());
            }
            while let Ok((_, event)) = remote.events.try_recv() {
                assert!(!matches!(event, PeerEvent::Failed { .. }), "{event:?}");
            }
        }
    }
}
