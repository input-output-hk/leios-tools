//! LeiosNotify mini-protocol (protocol ID 18).
//!
//! Announcement protocol for Leios data. The server notifies the client of
//! available endorser blocks, transactions, and votes. The client pulls
//! notifications one at a time.

pub mod codec;

use std::time::Duration;

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
    match msg {
        Message::MsgLeiosBlockAnnouncement { header } => {
            Ok(LeiosNotifyEvent::BlockAnnouncement { header })
        }
        Message::MsgLeiosBlockOffer { point, eb_size } => {
            Ok(LeiosNotifyEvent::BlockOffer { point, eb_size })
        }
        Message::MsgLeiosBlockTxsOffer { point } => Ok(LeiosNotifyEvent::BlockTxsOffer { point }),
        Message::MsgLeiosVotes { votes } => Ok(LeiosNotifyEvent::Votes { votes }),
        Message::MsgCanceled => {
            // Finish the shutdown handshake rather than treating cancellation
            // as corrupt data or spinning on further requests.
            done(runner).await?;
            Err(ProtocolError::Terminated)
        }
        other => Err(ProtocolError::InvalidMessage(format!(
            "expected notification, got {other:?}"
        ))),
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
    use crate::mux::scheduler::{RoundRobin, TrafficClass};
    use crate::mux::{
        CodecRecv, CodecSend, Mux, MuxConfig, ProtocolConfig, MODE_INITIATOR, MODE_RESPONDER,
    };
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
            sdu_timeout: std::time::Duration::from_secs(2),
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

        let proto = ProtocolConfig {
            id: PROTOCOL_ID,
            traffic_class: TrafficClass::Priority,
            ingress_limit: INGRESS_LIMIT,
            egress_queue_size: 16,
        };

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
    fn w41_shutdown_agency_and_transitions() {
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
        // This is the old [0] RequestNext that w41 relays rejected in StIdle.
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
