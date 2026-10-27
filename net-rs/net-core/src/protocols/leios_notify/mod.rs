//! LeiosNotify mini-protocol (protocol ID 18).
//!
//! Announcement protocol for Leios data. The server notifies the client of
//! available endorser blocks, transactions, and votes. The client pulls
//! notifications using a bounded request pipeline.

pub mod codec;

use std::time::Duration;

use crate::mux::{CodecRecv, CodecSend};
use crate::protocols::{Agency, Protocol, ProtocolError, Runner};
use crate::types::{Point, Vote, WrappedHeader};

/// LeiosNotify protocol ID in the multiplexer.
pub const PROTOCOL_ID: u16 = 18;

/// Ingress buffer limit for LeiosNotify.
pub const INGRESS_LIMIT: usize = 65_536;

/// Max message size for all LeiosNotify states.
pub const SIZE_LIMIT: usize = 65_535;

/// Maximum number of votes in a single MsgLeiosVotes.
pub const MAX_VOTES: usize = 1024;

/// Bound the wait for the server to acknowledge MsgQuit.
pub const TIMEOUT_QUIT: Duration = Duration::from_secs(120);

// --- State machine ---

/// LeiosNotify protocol states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// Client can request next notification or terminate. Client has agency.
    StIdle,
    /// Server must send one notification. Server has agency.
    StBusy,
    /// Client has quit; server must acknowledge. Server has agency.
    StQuit,
    /// Protocol complete. Nobody has agency.
    StDone,
}

/// LeiosNotify protocol messages.
#[derive(Debug, Clone)]
pub enum Message {
    /// Client requests the next notification. [3]
    MsgLeiosNotificationRequestNext,
    /// Server announces an RB header containing an EB announcement. [4, header]
    MsgLeiosBlockAnnouncement { header: WrappedHeader },
    /// Server offers an endorser block for download. [5, point, eb_size]
    MsgLeiosBlockOffer { point: Point, eb_size: u32 },
    /// Server offers an EB's transactions for download. [6, point]
    MsgLeiosBlockTxsOffer { point: Point },
    /// Server delivers votes inline (no offer/fetch round-trip).
    /// [7, [vote, ...]] where vote = [announcing_rb_hash, voter_id, signature].
    MsgLeiosVotes { votes: Vec<Vote> },
    /// Server acknowledges termination. [0]
    MsgDone,
    /// Client requests termination. [1]
    MsgQuit,
    /// Server cancels the outstanding notification request. [2]
    MsgCanceled,
}

// --- Protocol trait ---

/// The LeiosNotify protocol definition.
pub struct LeiosNotify;

impl Protocol for LeiosNotify {
    type State = State;
    type Message = Message;

    fn initial_state() -> State {
        State::StIdle
    }

    fn agency(state: &State) -> Agency {
        match state {
            State::StIdle => Agency::Client,
            State::StBusy | State::StQuit => Agency::Server,
            State::StDone => Agency::Nobody,
        }
    }

    fn transition(state: &State, msg: &Message) -> Result<State, ProtocolError> {
        match (state, msg) {
            (State::StIdle, Message::MsgLeiosNotificationRequestNext) => Ok(State::StBusy),
            (State::StIdle, Message::MsgQuit) => Ok(State::StQuit),
            (State::StQuit, Message::MsgDone) => Ok(State::StDone),
            (State::StBusy, Message::MsgCanceled) => Ok(State::StIdle),
            (State::StBusy, Message::MsgLeiosBlockAnnouncement { .. }) => Ok(State::StIdle),
            (State::StBusy, Message::MsgLeiosBlockOffer { .. }) => Ok(State::StIdle),
            (State::StBusy, Message::MsgLeiosBlockTxsOffer { .. }) => Ok(State::StIdle),
            (State::StBusy, Message::MsgLeiosVotes { .. }) => Ok(State::StIdle),
            _ => Err(ProtocolError::InvalidMessage(format!(
                "{msg:?} not valid in state {state:?}"
            ))),
        }
    }

    fn size_limit(_state: &State) -> usize {
        SIZE_LIMIT
    }

    fn timeout(state: &State) -> Option<Duration> {
        match state {
            // StBusy has no timeout: the server may legitimately wait for new
            // Leios data before responding (long-poll pattern, same as
            // ChainSync MsgAwaitReply).
            State::StIdle | State::StBusy | State::StDone => None,
            State::StQuit => Some(TIMEOUT_QUIT),
        }
    }
}

// --- Client helpers ---

/// Events returned by `request_next`.
#[derive(Debug, Clone)]
pub enum LeiosNotifyEvent {
    /// An RB header announcing an EB.
    BlockAnnouncement { header: WrappedHeader },
    /// An endorser block is available for download.  `eb_size` is the
    /// EB byte size as declared by the offerer; redundant with the
    /// header's `announced_eb` tuple but useful as live telemetry for
    /// operators (e.g. comparing against `rb_body_max_bytes` to spot
    /// producers overflowing more aggressively than expected).
    BlockOffer { point: Point, eb_size: u32 },
    /// An EB's transactions are available for download.
    BlockTxsOffer { point: Point },
    /// Votes delivered inline (no fetch needed).
    Votes { votes: Vec<Vote> },
}

/// Request the next notification from the server.
pub async fn request_next(
    runner: &mut Runner<LeiosNotify>,
) -> Result<LeiosNotifyEvent, ProtocolError> {
    runner
        .send(&Message::MsgLeiosNotificationRequestNext)
        .await?;
    let msg = runner.recv().await?;
    if matches!(msg, Message::MsgCanceled) {
        done(runner).await?;
        return Err(ProtocolError::Terminated);
    }
    notification_event(msg)
}

fn notification_event(msg: Message) -> Result<LeiosNotifyEvent, ProtocolError> {
    match msg {
        Message::MsgLeiosBlockAnnouncement { header } => {
            Ok(LeiosNotifyEvent::BlockAnnouncement { header })
        }
        Message::MsgLeiosBlockOffer { point, eb_size } => {
            Ok(LeiosNotifyEvent::BlockOffer { point, eb_size })
        }
        Message::MsgLeiosBlockTxsOffer { point } => Ok(LeiosNotifyEvent::BlockTxsOffer { point }),
        Message::MsgLeiosVotes { votes } => Ok(LeiosNotifyEvent::Votes { votes }),
        other => Err(ProtocolError::InvalidMessage(format!(
            "expected notification, got {other:?}"
        ))),
    }
}

/// Keep enough outstanding request credits to receive notification bursts.
/// The reference server drops events when the client has no free credits,
/// so a one-request-at-a-time client can lose a quorum's final vote.
pub const PIPELINE_DEPTH: usize = 100;

/// Bounded pipelined notification client. Each response consumes exactly one
/// request credit, which is replenished before handing the event to the caller.
/// The ordinary `Runner` models one exchange and cannot represent concurrent
/// outstanding requests; this client tracks those obligations explicitly.
/// Receive limits and message decoding are identical to the serial client.
/// Waiting for a notification is an untimed long poll. `shutdown` sends Quit,
/// drains outstanding responses, and waits for Done within `TIMEOUT_QUIT`.
pub struct PipelinedClient {
    send: CodecSend,
    recv: CodecRecv,
    outstanding: usize,
    state: ClientState,
}

#[derive(Clone, Copy)]
enum ClientState {
    Running,
    Draining {
        deadline: tokio::time::Instant,
        quit_sent: bool,
    },
    Done,
}

struct RequestCredits(usize);

impl minicbor::Encode<()> for RequestCredits {
    fn encode<W: minicbor::encode::Write>(
        &self,
        encoder: &mut minicbor::Encoder<W>,
        ctx: &mut (),
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        for _ in 0..self.0 {
            Message::MsgLeiosNotificationRequestNext.encode(encoder, ctx)?;
        }
        Ok(())
    }
}

impl PipelinedClient {
    pub fn new(send: CodecSend, recv: CodecRecv) -> Self {
        Self {
            send,
            recv,
            outstanding: 0,
            state: ClientState::Running,
        }
    }

    pub async fn next(&mut self) -> Result<LeiosNotifyEvent, ProtocolError> {
        if !matches!(self.state, ClientState::Running) {
            self.shutdown().await?;
            return Err(ProtocolError::Terminated);
        }
        if self.outstanding < PIPELINE_DEPTH {
            // Queue the initial request window as one small CBOR stream chunk.
            // One mux segment per two-byte request needlessly exhausts bounded
            // ingress segment queues before the peer's reader can run.
            self.send
                .send(&RequestCredits(PIPELINE_DEPTH - self.outstanding))
                .await?;
            self.outstanding = PIPELINE_DEPTH;
        }
        let msg: Message = self.recv.recv(SIZE_LIMIT).await?;
        if matches!(msg, Message::MsgCanceled) {
            // This cancellation already answers one request. Do not replace
            // it: finish the exchange, including the other outstanding replies.
            self.outstanding -= 1;
            self.shutdown().await?;
            return Err(ProtocolError::Terminated);
        }
        let event = notification_event(msg)?;
        self.outstanding -= 1;
        self.send
            .send(&Message::MsgLeiosNotificationRequestNext)
            .await?;
        self.outstanding += 1;
        Ok(event)
    }

    /// Stop requesting notifications and complete the shutdown handshake.
    /// Already queued notifications are drained without delivery, as in the
    /// reference client. Repeated calls do not send another Quit or reset the
    /// deadline, including when an earlier call's future was canceled.
    pub async fn shutdown(&mut self) -> Result<(), ProtocolError> {
        if matches!(self.state, ClientState::Running) {
            self.state = ClientState::Draining {
                deadline: tokio::time::Instant::now() + TIMEOUT_QUIT,
                quit_sent: false,
            };
        }
        let ClientState::Draining {
            deadline,
            quit_sent,
        } = self.state
        else {
            return Ok(());
        };
        tokio::time::timeout_at(deadline, async {
            if !quit_sent {
                self.send.send(&Message::MsgQuit).await?;
                self.state = ClientState::Draining {
                    deadline,
                    quit_sent: true,
                };
            }
            while self.outstanding > 0 {
                let msg: Message = self.recv.recv(SIZE_LIMIT).await?;
                if !matches!(msg, Message::MsgCanceled) {
                    // A notification sent before Quit still answers a request.
                    // This also rejects Done before all requests are answered.
                    notification_event(msg)?;
                }
                self.outstanding -= 1;
            }
            match self.recv.recv::<Message>(SIZE_LIMIT).await? {
                Message::MsgDone => {
                    self.state = ClientState::Done;
                    Ok(())
                }
                other => Err(ProtocolError::InvalidMessage(format!(
                    "expected MsgDone after draining notifications, got {other:?}"
                ))),
            }
        })
        .await
        .map_err(|_| ProtocolError::Timeout(TIMEOUT_QUIT))?
    }
}

/// Request termination and wait for the server's acknowledgement.
pub async fn done(runner: &mut Runner<LeiosNotify>) -> Result<(), ProtocolError> {
    runner.send(&Message::MsgQuit).await?;
    match runner.recv().await? {
        Message::MsgDone => Ok(()),
        other => Err(ProtocolError::InvalidMessage(format!(
            "expected MsgDone, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bearer::mem::MemBearer;
    use crate::mux::scheduler::RoundRobin;
    use crate::mux::{CodecRecv, CodecSend, Mux, MuxConfig, MODE_INITIATOR, MODE_RESPONDER};
    use crate::protocols::Role;

    #[test]
    fn agency_correct() {
        assert_eq!(LeiosNotify::agency(&State::StIdle), Agency::Client);
        assert_eq!(LeiosNotify::agency(&State::StBusy), Agency::Server);
        assert_eq!(LeiosNotify::agency(&State::StDone), Agency::Nobody);
    }

    #[test]
    fn valid_transitions() {
        assert_eq!(
            LeiosNotify::transition(&State::StIdle, &Message::MsgLeiosNotificationRequestNext)
                .unwrap(),
            State::StBusy
        );
        assert_eq!(
            LeiosNotify::transition(&State::StIdle, &Message::MsgQuit).unwrap(),
            State::StQuit
        );
        assert_eq!(
            LeiosNotify::transition(
                &State::StBusy,
                &Message::MsgLeiosBlockAnnouncement {
                    header: WrappedHeader::opaque(vec![0x80]),
                }
            )
            .unwrap(),
            State::StIdle
        );
        assert_eq!(
            LeiosNotify::transition(
                &State::StBusy,
                &Message::MsgLeiosBlockOffer {
                    point: Point::Specific {
                        slot: 1,
                        hash: [0; 32],
                    },
                    eb_size: 0,
                }
            )
            .unwrap(),
            State::StIdle
        );
        assert_eq!(
            LeiosNotify::transition(
                &State::StBusy,
                &Message::MsgLeiosBlockTxsOffer {
                    point: Point::Specific {
                        slot: 1,
                        hash: [0; 32],
                    },
                }
            )
            .unwrap(),
            State::StIdle
        );
        assert_eq!(
            LeiosNotify::transition(&State::StBusy, &Message::MsgLeiosVotes { votes: vec![] })
                .unwrap(),
            State::StIdle
        );
    }

    #[test]
    fn invalid_transitions() {
        // Server messages not valid in StIdle
        assert!(LeiosNotify::transition(
            &State::StIdle,
            &Message::MsgLeiosBlockOffer {
                point: Point::Specific {
                    slot: 1,
                    hash: [0; 32]
                },
                eb_size: 0,
            }
        )
        .is_err());
        // Client messages not valid in StBusy
        assert!(
            LeiosNotify::transition(&State::StBusy, &Message::MsgLeiosNotificationRequestNext,)
                .is_err()
        );
        assert!(LeiosNotify::transition(&State::StBusy, &Message::MsgDone).is_err());
    }

    #[test]
    fn size_limits() {
        assert_eq!(LeiosNotify::size_limit(&State::StIdle), SIZE_LIMIT);
        assert_eq!(LeiosNotify::size_limit(&State::StBusy), SIZE_LIMIT);
    }

    #[test]
    fn timeouts() {
        assert_eq!(LeiosNotify::timeout(&State::StIdle), None);
        assert_eq!(LeiosNotify::timeout(&State::StBusy), None);
        assert_eq!(LeiosNotify::timeout(&State::StDone), None);
    }

    fn test_config() -> MuxConfig {
        MuxConfig {
            // Keep the bearer alive long enough to test the protocol deadline.
            sdu_timeout: TIMEOUT_QUIT * 2,
            ..MuxConfig::default()
        }
    }

    fn make_leios_notify_mux_pair() -> (
        (CodecSend, CodecRecv),
        (CodecSend, CodecRecv),
        crate::mux::RunningMux,
        crate::mux::RunningMux,
    ) {
        let (bearer_a, bearer_b) = MemBearer::pair();

        let proto = crate::peer::peer_task::server_protocol_configs(true)
            .into_iter()
            .find(|config| config.id == PROTOCOL_ID)
            .expect("LeiosNotify must have a production channel configuration");

        let mut mux_a = Mux::new(test_config(), RoundRobin::default(), MODE_INITIATOR);
        let (send_a, recv_a) = mux_a.register(&proto);
        let running_a = mux_a.run(bearer_a);

        let mut mux_b = Mux::new(test_config(), RoundRobin::default(), MODE_RESPONDER);
        let (send_b, recv_b) = mux_b.register(&proto);
        let running_b = mux_b.run(bearer_b);

        (
            (CodecSend::new(send_a), CodecRecv::new(recv_a)),
            (CodecSend::new(send_b), CodecRecv::new(recv_b)),
            running_a,
            running_b,
        )
    }

    async fn receive_requests(recv: &mut CodecRecv, count: usize) {
        for _ in 0..count {
            assert!(matches!(
                recv.recv::<Message>(SIZE_LIMIT).await.unwrap(),
                Message::MsgLeiosNotificationRequestNext
            ));
        }
    }

    #[test]
    fn batched_requests_use_current_wire_tag() {
        assert_eq!(
            minicbor::to_vec(RequestCredits(3)).unwrap(),
            [0x81, 0x03, 0x81, 0x03, 0x81, 0x03]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pipeline_shutdown_drains_idle_or_full_window() {
        for requests in [0, PIPELINE_DEPTH] {
            let ((cs, cr), (ss, mut sr), ra, rb) = make_leios_notify_mux_pair();
            let (ready, ready_rx) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                receive_requests(&mut sr, requests).await;
                ready.send(()).unwrap();
                assert!(matches!(
                    sr.recv::<Message>(SIZE_LIMIT).await.unwrap(),
                    Message::MsgQuit
                ));
                for i in 0..requests {
                    // Data already queued before Quit remains a valid reply.
                    let reply = if i % 2 == 0 {
                        Message::MsgLeiosVotes { votes: vec![] }
                    } else {
                        Message::MsgCanceled
                    };
                    ss.send(&reply).await.unwrap();
                }
                ss.send(&Message::MsgDone).await.unwrap();
                assert!(tokio::time::timeout(
                    Duration::from_millis(20),
                    sr.recv::<Message>(SIZE_LIMIT)
                )
                .await
                .is_err());
            });
            let mut client = PipelinedClient::new(cs, cr);
            tokio::time::timeout(Duration::from_secs(1), async {
                if requests > 0 {
                    // Stop an actual long poll after the server has received
                    // the initial window, before any response has arrived.
                    tokio::select! {
                        ready = ready_rx => ready.unwrap(),
                        result = client.next() => panic!("unexpected notification: {result:?}"),
                    }
                }
                client.shutdown().await.unwrap();
                assert_eq!(client.outstanding, 0);
                client.shutdown().await.unwrap();
                assert!(matches!(
                    client.next().await,
                    Err(ProtocolError::Terminated)
                ));
                server.await.unwrap();
            })
            .await
            .expect("shutdown must drain without waiting for notification keepalives");
            ra.abort();
            rb.abort();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pipeline_cancellation_accounts_for_previously_delivered_replies() {
        for delivered in [0, 3] {
            let ((cs, cr), (ss, mut sr), ra, rb) = make_leios_notify_mux_pair();
            let server = tokio::spawn(async move {
                receive_requests(&mut sr, PIPELINE_DEPTH).await;
                for _ in 0..delivered {
                    ss.send(&Message::MsgLeiosVotes { votes: vec![] })
                        .await
                        .unwrap();
                    receive_requests(&mut sr, 1).await;
                }
                ss.send(&Message::MsgCanceled).await.unwrap();
                assert!(matches!(
                    sr.recv::<Message>(SIZE_LIMIT).await.unwrap(),
                    Message::MsgQuit
                ));
                // The first cancellation has already completed one request.
                for _ in 1..PIPELINE_DEPTH {
                    ss.send(&Message::MsgCanceled).await.unwrap();
                }
                ss.send(&Message::MsgDone).await.unwrap();
            });
            let mut client = PipelinedClient::new(cs, cr);
            tokio::time::timeout(Duration::from_secs(1), async {
                for _ in 0..delivered {
                    assert!(matches!(
                        client.next().await.unwrap(),
                        LeiosNotifyEvent::Votes { .. }
                    ));
                }
                assert!(matches!(
                    client.next().await,
                    Err(ProtocolError::Terminated)
                ));
                assert_eq!(client.outstanding, 0);
                server.await.unwrap();
            })
            .await
            .expect("cancellation must finish the whole pipeline");
            ra.abort();
            rb.abort();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pipeline_shutdown_rejects_premature_done_and_excess_replies() {
        for early_done in [true, false] {
            let ((cs, cr), (ss, mut sr), ra, rb) = make_leios_notify_mux_pair();
            let server = tokio::spawn(async move {
                receive_requests(&mut sr, PIPELINE_DEPTH).await;
                ss.send(&Message::MsgCanceled).await.unwrap();
                assert!(matches!(
                    sr.recv::<Message>(SIZE_LIMIT).await.unwrap(),
                    Message::MsgQuit
                ));
                if early_done {
                    ss.send(&Message::MsgDone).await.unwrap();
                } else {
                    // One more cancellation than the remaining obligations.
                    for _ in 0..PIPELINE_DEPTH {
                        ss.send(&Message::MsgCanceled).await.unwrap();
                    }
                }
            });
            let mut client = PipelinedClient::new(cs, cr);
            assert!(matches!(
                client.next().await,
                Err(ProtocolError::InvalidMessage(_))
            ));
            server.await.unwrap();
            ra.abort();
            rb.abort();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pipeline_shutdown_deadline_covers_drain_and_acknowledgement() {
        for finish_draining in [false, true] {
            let ((cs, cr), (ss, mut sr), ra, rb) = make_leios_notify_mux_pair();
            let server = tokio::spawn(async move {
                receive_requests(&mut sr, PIPELINE_DEPTH).await;
                ss.send(&Message::MsgCanceled).await.unwrap();
                assert!(matches!(
                    sr.recv::<Message>(SIZE_LIMIT).await.unwrap(),
                    Message::MsgQuit
                ));
                if finish_draining {
                    tokio::time::sleep(TIMEOUT_QUIT - Duration::from_secs(1)).await;
                    for _ in 1..PIPELINE_DEPTH {
                        ss.send(&Message::MsgCanceled).await.unwrap();
                    }
                }
                // Keep the channel alive but never send the missing reply/Done.
                std::future::pending::<()>().await;
            });
            let mut client = PipelinedClient::new(cs, cr);
            let start = tokio::time::Instant::now();
            assert!(
                matches!(client.next().await, Err(ProtocolError::Timeout(d)) if d == TIMEOUT_QUIT)
            );
            assert_eq!(start.elapsed(), TIMEOUT_QUIT);
            server.abort();
            ra.abort();
            rb.abort();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pipeline_shutdown_can_resume_without_repeating_quit() {
        let ((cs, cr), (ss, mut sr), ra, rb) = make_leios_notify_mux_pair();
        let (resume, resume_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            receive_requests(&mut sr, PIPELINE_DEPTH).await;
            ss.send(&Message::MsgCanceled).await.unwrap();
            assert!(matches!(
                sr.recv::<Message>(SIZE_LIMIT).await.unwrap(),
                Message::MsgQuit
            ));
            for _ in 1..PIPELINE_DEPTH / 2 {
                ss.send(&Message::MsgCanceled).await.unwrap();
            }
            resume_rx.await.unwrap();
            for _ in PIPELINE_DEPTH / 2..PIPELINE_DEPTH {
                ss.send(&Message::MsgCanceled).await.unwrap();
            }
            ss.send(&Message::MsgDone).await.unwrap();
            assert!(tokio::time::timeout(
                Duration::from_millis(20),
                sr.recv::<Message>(SIZE_LIMIT)
            )
            .await
            .is_err());
        });
        let mut client = PipelinedClient::new(cs, cr);
        assert!(tokio::time::timeout(Duration::from_secs(1), client.next())
            .await
            .is_err());
        assert_eq!(client.outstanding, PIPELINE_DEPTH / 2);
        resume.send(()).unwrap();
        client.shutdown().await.unwrap();
        assert_eq!(client.outstanding, 0);
        server.await.unwrap();
        ra.abort();
        rb.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn pipeline_shutdown_with_real_responder() {
        let ((cs, cr), (ss, sr), ra, rb) = make_leios_notify_mux_pair();
        let (store, _rx) = crate::store::leios_store::LeiosStore::new(100);
        let server = tokio::spawn(crate::peer::server_handlers::serve_leios_notify(
            ss,
            sr,
            store.clone(),
            crate::peer::PeerId(7),
            None,
        ));
        let mut client = PipelinedClient::new(cs, cr);
        tokio::time::timeout(Duration::from_secs(1), async {
            let (event, _) = tokio::join!(client.next(), async {
                tokio::time::sleep(Duration::from_millis(1)).await;
                store.inject_block(
                    Point::Specific {
                        slot: 21,
                        hash: [21; 32],
                    },
                    vec![0xa0],
                    None,
                );
            });
            assert!(matches!(
                event.unwrap(),
                LeiosNotifyEvent::BlockOffer { .. }
            ));
            client.shutdown().await.unwrap();
            server.await.unwrap();
        })
        .await
        .expect("the pipelined client and responder must agree on outstanding replies");
        ra.abort();
        rb.abort();
    }

    #[tokio::test]
    async fn pipeline_preserves_vote_burst_and_replenishes_credits() {
        const VOTES: usize = PIPELINE_DEPTH * 2 + 7;
        let ((cs, cr), (ss, mut sr), ra, rb) = make_leios_notify_mux_pair();
        let server = tokio::spawn(async move {
            // Reproduce the reference credit gate: gather the initial credits
            // before releasing a burst. A serial client stalls here and cannot
            // preserve the burst that a zero-credit pump would discard.
            for _ in 0..PIPELINE_DEPTH {
                assert!(matches!(
                    sr.recv::<Message>(SIZE_LIMIT).await.unwrap(),
                    Message::MsgLeiosNotificationRequestNext
                ));
            }
            for start in (0..VOTES).step_by(PIPELINE_DEPTH) {
                let end = (start + PIPELINE_DEPTH).min(VOTES);
                for voter_id in start..end {
                    ss.send(&Message::MsgLeiosVotes {
                        votes: vec![Vote {
                            announcing_rb_hash: [0x76; 32],
                            voter_id: voter_id as u16,
                            vote_signature: vec![voter_id as u8; 48],
                        }],
                    })
                    .await
                    .unwrap();
                }
                // Every delivered response must restore precisely one credit.
                receive_requests(&mut sr, end - start).await;
            }
            assert!(tokio::time::timeout(
                Duration::from_millis(20),
                sr.recv::<Message>(SIZE_LIMIT)
            )
            .await
            .is_err());
            // Invalid server agency/message is still rejected.
            ss.send(&Message::MsgLeiosNotificationRequestNext)
                .await
                .unwrap();
        });
        let mut client = PipelinedClient::new(cs, cr);
        tokio::time::timeout(Duration::from_secs(2), async {
            for voter_id in 0..VOTES {
                match client.next().await.unwrap() {
                    LeiosNotifyEvent::Votes { votes } => {
                        assert_eq!(votes[0].voter_id, voter_id as u16)
                    }
                    other => panic!("expected votes, got {other:?}"),
                }
                assert_eq!(client.outstanding, PIPELINE_DEPTH);
                // Simulate an event consumer that yields between deliveries.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            assert!(matches!(
                client.next().await,
                Err(ProtocolError::InvalidMessage(_))
            ));
            server.await.unwrap();
        })
        .await
        .expect("pipelined burst completes");
        ra.abort();
        rb.abort();
    }

    #[tokio::test]
    async fn leios_notify_all_announcement_types() {
        let ((cs, cr), (ss, sr), ra, rb) = make_leios_notify_mux_pair();

        let test_hash: [u8; 32] = {
            let mut h = [0u8; 32];
            h[0] = 0xAB;
            h[31] = 0xCD;
            h
        };

        let test_hash_clone = test_hash;
        let server = tokio::spawn(async move {
            let mut runner = Runner::<LeiosNotify>::new(Role::Server, ss, sr);

            // 1. Block announcement
            let msg = runner.recv().await.unwrap();
            assert!(matches!(msg, Message::MsgLeiosNotificationRequestNext));
            runner
                .send(&Message::MsgLeiosBlockAnnouncement {
                    header: WrappedHeader::opaque(vec![0x82, 0x05, 0x00]),
                })
                .await
                .unwrap();

            // 2. Block offer
            let msg = runner.recv().await.unwrap();
            assert!(matches!(msg, Message::MsgLeiosNotificationRequestNext));
            runner
                .send(&Message::MsgLeiosBlockOffer {
                    point: Point::Specific {
                        slot: 42,
                        hash: test_hash_clone,
                    },
                    eb_size: 1234,
                })
                .await
                .unwrap();

            // 3. Block txs offer
            let msg = runner.recv().await.unwrap();
            assert!(matches!(msg, Message::MsgLeiosNotificationRequestNext));
            runner
                .send(&Message::MsgLeiosBlockTxsOffer {
                    point: Point::Specific {
                        slot: 43,
                        hash: test_hash_clone,
                    },
                })
                .await
                .unwrap();

            // 4. Inline votes
            let msg = runner.recv().await.unwrap();
            assert!(matches!(msg, Message::MsgLeiosNotificationRequestNext));
            runner
                .send(&Message::MsgLeiosVotes {
                    votes: vec![
                        Vote {
                            announcing_rb_hash: [0x11; 32],
                            voter_id: 1,
                            vote_signature: vec![0xAB; 48],
                        },
                        Vote {
                            announcing_rb_hash: [0x22; 32],
                            voter_id: 2,
                            vote_signature: vec![0xCD; 48],
                        },
                    ],
                })
                .await
                .unwrap();

            // 5. Quit / Done
            let msg = runner.recv().await.unwrap();
            assert!(matches!(msg, Message::MsgQuit));
            runner.send(&Message::MsgDone).await.unwrap();
        });

        let client = tokio::spawn(async move {
            let mut runner = Runner::<LeiosNotify>::new(Role::Client, cs, cr);

            // 1. Block announcement
            let event = request_next(&mut runner).await.unwrap();
            match event {
                LeiosNotifyEvent::BlockAnnouncement { header } => {
                    assert_eq!(header.raw, vec![0x82, 0x05, 0x00]);
                }
                other => panic!("expected BlockAnnouncement, got {other:?}"),
            }

            // 2. Block offer
            let event = request_next(&mut runner).await.unwrap();
            match event {
                LeiosNotifyEvent::BlockOffer { point, eb_size } => {
                    assert_eq!(
                        point,
                        Point::Specific {
                            slot: 42,
                            hash: test_hash,
                        }
                    );
                    assert_eq!(eb_size, 1234);
                }
                other => panic!("expected BlockOffer, got {other:?}"),
            }

            // 3. Block txs offer
            let event = request_next(&mut runner).await.unwrap();
            match event {
                LeiosNotifyEvent::BlockTxsOffer { point } => {
                    assert_eq!(
                        point,
                        Point::Specific {
                            slot: 43,
                            hash: test_hash,
                        }
                    );
                }
                other => panic!("expected BlockTxsOffer, got {other:?}"),
            }

            // 4. Inline votes
            let event = request_next(&mut runner).await.unwrap();
            match event {
                LeiosNotifyEvent::Votes { votes } => {
                    assert_eq!(votes.len(), 2);
                    assert_eq!(
                        votes[0],
                        Vote {
                            announcing_rb_hash: [0x11; 32],
                            voter_id: 1,
                            vote_signature: vec![0xAB; 48],
                        }
                    );
                    assert_eq!(
                        votes[1],
                        Vote {
                            announcing_rb_hash: [0x22; 32],
                            voter_id: 2,
                            vote_signature: vec![0xCD; 48],
                        }
                    );
                }
                other => panic!("expected Votes, got {other:?}"),
            }

            done(&mut runner).await.unwrap();
        });

        client.await.unwrap();
        server.await.unwrap();
        ra.abort();
        rb.abort();
    }

    #[tokio::test]
    async fn leios_notify_immediate_done() {
        let ((cs, cr), (ss, sr), ra, rb) = make_leios_notify_mux_pair();

        let server = tokio::spawn(async move {
            let mut runner = Runner::<LeiosNotify>::new(Role::Server, ss, sr);
            let msg = runner.recv().await.unwrap();
            assert!(matches!(msg, Message::MsgQuit));
            runner.send(&Message::MsgDone).await.unwrap();
        });

        let client = tokio::spawn(async move {
            let mut runner = Runner::<LeiosNotify>::new(Role::Client, cs, cr);
            done(&mut runner).await.unwrap();
        });

        client.await.unwrap();
        server.await.unwrap();
        ra.abort();
        rb.abort();
    }

    #[test]
    fn shutdown_agency_and_transitions() {
        assert_eq!(LeiosNotify::agency(&State::StQuit), Agency::Server);
        assert_eq!(LeiosNotify::timeout(&State::StQuit), Some(TIMEOUT_QUIT));
        assert_eq!(
            LeiosNotify::transition(&State::StBusy, &Message::MsgCanceled).unwrap(),
            State::StIdle
        );
        assert_eq!(
            LeiosNotify::transition(&State::StQuit, &Message::MsgDone).unwrap(),
            State::StDone
        );
        // MsgDone is only valid in StQuit.
        assert!(LeiosNotify::transition(&State::StIdle, &Message::MsgDone).is_err());
        assert!(LeiosNotify::transition(&State::StIdle, &Message::MsgCanceled).is_err());
        assert!(
            LeiosNotify::transition(&State::StQuit, &Message::MsgLeiosNotificationRequestNext)
                .is_err()
        );
    }

    #[tokio::test]
    async fn canceled_request_completes_shutdown() {
        let ((cs, cr), (ss, sr), ra, rb) = make_leios_notify_mux_pair();
        let server = async {
            let mut runner = Runner::<LeiosNotify>::new(Role::Server, ss, sr);
            assert!(matches!(
                runner.recv().await.unwrap(),
                Message::MsgLeiosNotificationRequestNext
            ));
            runner.send(&Message::MsgCanceled).await.unwrap();
            assert!(matches!(runner.recv().await.unwrap(), Message::MsgQuit));
            runner.send(&Message::MsgDone).await.unwrap();
            assert!(runner.is_done());
        };
        let client = async {
            let mut runner = Runner::<LeiosNotify>::new(Role::Client, cs, cr);
            assert!(matches!(
                request_next(&mut runner).await,
                Err(ProtocolError::Terminated)
            ));
            assert!(runner.is_done());
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(client, server);
        })
        .await
        .unwrap();
        ra.abort();
        rb.abort();
    }
}
