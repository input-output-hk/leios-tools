//! Per-connection LeiosNotify ordering. Preparation never records delivery:
//! only a successful send to the ordered mux advances the sent history.

use std::collections::{BTreeMap, BTreeSet};

use super::server_handlers::OutboundControls;
use crate::protocols::leios_notify::Message;
use crate::store::leios_store::{
    LeiosNotification, LeiosStore, NotificationEntry, MAX_NOTIFICATIONS,
};
use crate::types::{Point, WrappedHeader};
use shared_consensus::PeerId;

type Key = (u64, [u8; 32]);

#[derive(Clone, Copy)]
struct PendingOffer {
    // Only two provenance bits are needed; never retain unbounded source lists.
    from_peer: bool,
    local: bool,
    size: u32,
}

#[derive(Default)]
struct BlockState {
    announced: bool,
    body_sent: bool,
    txs_sent: bool,
    body: Option<PendingOffer>,
    txs: Option<PendingOffer>,
}

#[derive(Default)]
pub(super) struct NotifyState {
    blocks: BTreeMap<Key, BlockState>,
    // Ranking-block identity, separate from the endorser-block identity above.
    headers: BTreeSet<Key>,
}

fn point_key(point: &Point) -> Option<Key> {
    match point {
        Point::Specific { slot, hash } => Some((*slot, *hash)),
        Point::Origin => None,
    }
}

fn announced_key(header: &WrappedHeader) -> Option<Key> {
    let parsed = header.parsed.as_ref()?;
    Some((parsed.slot, parsed.announced_eb?.0))
}

fn header_key(header: &WrappedHeader) -> Key {
    header
        .point()
        .as_ref()
        .and_then(point_key)
        .unwrap_or_else(|| (0, net_codec::blake2b_256(&header.raw)))
}

impl NotifyState {
    pub(super) fn prune(&mut self, cutoff: u64) {
        self.blocks.retain(|(slot, _), _| *slot >= cutoff);
        self.headers.retain(|(slot, _)| *slot >= cutoff);
    }

    fn block(&mut self, key: Key) -> Result<&mut BlockState, ()> {
        // Never forget live sent history to make room: that permits duplicates.
        if !self.blocks.contains_key(&key) && self.blocks.len() == MAX_NOTIFICATIONS {
            return Err(());
        }
        Ok(self.blocks.entry(key).or_default())
    }

    fn prepare_header(&mut self, header: WrappedHeader) -> Result<Option<Message>, ()> {
        if self.headers.contains(&header_key(&header)) {
            return Ok(None);
        }
        if self.headers.len() == MAX_NOTIFICATIONS {
            return Err(());
        }
        if let Some(key) = announced_key(&header) {
            self.block(key)?;
        }
        Ok(Some(Message::MsgLeiosBlockAnnouncement { header }))
    }

    /// Consume one log entry. Missing prerequisites defer offers, never the
    /// cursor: unrelated votes/announcements remain able to make progress.
    pub(super) fn observe(
        &mut self,
        entry: NotificationEntry,
        peer: PeerId,
        controls: Option<&OutboundControls>,
        cutoff: u64,
    ) -> Result<Option<Message>, ()> {
        let from_peer = entry.sources.contains(&peer);
        let local = entry.sources.is_empty();
        let echo = controls.is_some_and(|c| c.control.borrow().leios.echo_to_source);
        if from_peer && !echo {
            return Ok(None);
        }
        match entry.notification {
            LeiosNotification::BlockOffer { point, eb_size } => {
                if let Some(key) = point_key(&point).filter(|(slot, _)| *slot >= cutoff) {
                    let state = self.block(key)?;
                    if !state.body_sent {
                        state.body = Some(PendingOffer {
                            from_peer,
                            local,
                            size: eb_size,
                        });
                    }
                }
                Ok(None)
            }
            LeiosNotification::BlockTxsOffer { point } => {
                if let Some(key) = point_key(&point).filter(|(slot, _)| *slot >= cutoff) {
                    let state = self.block(key)?;
                    if !state.txs_sent {
                        state.txs = Some(PendingOffer {
                            from_peer,
                            local,
                            size: 0,
                        });
                    }
                }
                Ok(None)
            }
            LeiosNotification::BlockAnnouncement { header } => {
                if header.parsed.as_ref().is_some_and(|h| h.slot < cutoff)
                    || (local
                        && controls.is_some_and(|c| c.control.borrow().leios.withhold_announce))
                {
                    return Ok(None);
                }
                self.prepare_header(header)
            }
            LeiosNotification::Votes {
                retention_slot,
                votes,
            } => Ok((retention_slot >= cutoff).then_some(Message::MsgLeiosVotes { votes })),
        }
    }

    /// Recheck deferred offers on store/control changes and after each send.
    /// A missing header for one EB must not prevent another EB from progressing.
    pub(super) fn next_offer(
        &mut self,
        store: &LeiosStore,
        controls: Option<&OutboundControls>,
    ) -> Result<Option<Message>, ()> {
        let control = controls
            .map(|c| c.control.borrow().leios.clone())
            .unwrap_or_default();
        for (&(slot, hash), state) in &mut self.blocks {
            for (txs, pending) in [(false, &mut state.body), (true, &mut state.txs)] {
                let Some(offer) = *pending else { continue };
                if !store.has_offer(slot, hash, txs) {
                    // Cache eviction can be temporary. Re-injection may merge
                    // into a log entry this connection has already consumed,
                    // so keep unsent intent until delivery or retention expiry.
                    continue;
                }
                if offer.from_peer && !control.echo_to_source {
                    continue;
                }
                // Preserve the explicit producer-side adversarial control.
                // It never authorizes an unannounced relayed offer or counts
                // a withheld header as sent.
                if !state.announced && !(offer.local && control.withhold_announce) {
                    let Some(header) = store.announcement(slot, hash) else {
                        continue;
                    };
                    if self.headers.len() == MAX_NOTIFICATIONS {
                        return Err(());
                    }
                    return Ok(Some(Message::MsgLeiosBlockAnnouncement { header }));
                }
                let point = Point::Specific { slot, hash };
                return Ok(Some(if txs {
                    Message::MsgLeiosBlockTxsOffer { point }
                } else {
                    Message::MsgLeiosBlockOffer {
                        point,
                        eb_size: control.offer_eb_size.apply(offer.size),
                    }
                }));
            }
        }
        Ok(None)
    }

    /// Call only after CodecSend::send succeeds. Queued/skipped/canceled
    /// notifications are not evidence of delivery on this connection.
    pub(super) fn sent(&mut self, message: &Message) {
        match message {
            Message::MsgLeiosBlockAnnouncement { header } => {
                self.headers.insert(header_key(header));
                if let Some(state) = announced_key(header).and_then(|key| self.blocks.get_mut(&key))
                {
                    state.announced = true;
                }
            }
            Message::MsgLeiosBlockOffer { point, .. } => {
                if let Some(state) = point_key(point).and_then(|key| self.blocks.get_mut(&key)) {
                    state.body_sent = true;
                    state.body = None;
                }
            }
            Message::MsgLeiosBlockTxsOffer { point } => {
                if let Some(state) = point_key(point).and_then(|key| self.blocks.get_mut(&key)) {
                    state.txs_sent = true;
                    state.txs = None;
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::leios_header;

    #[test]
    fn preparing_an_announcement_is_not_delivery() {
        let (store, _) = LeiosStore::new(10);
        let point = Point::Specific {
            slot: 21,
            hash: [42; 32],
        };
        let header = leios_header(&point, 1);
        store.announce_block(header.clone(), None);
        store.inject_block(point.clone(), vec![0xa0], None);
        let mut state = NotifyState::default();
        let prepared = state
            .observe(
                NotificationEntry {
                    sources: vec![],
                    notification: LeiosNotification::BlockAnnouncement { header },
                },
                PeerId(7),
                None,
                0,
            )
            .unwrap()
            .unwrap();
        state
            .observe(
                NotificationEntry {
                    sources: vec![],
                    notification: LeiosNotification::BlockOffer { point, eb_size: 1 },
                },
                PeerId(7),
                None,
                0,
            )
            .unwrap();
        // A prepared announcement can be canceled or fail to send. Neither
        // permits the queued body to cross the prerequisite gate.
        assert!(matches!(
            state.next_offer(&store, None).unwrap(),
            Some(Message::MsgLeiosBlockAnnouncement { .. })
        ));
        assert!(state.headers.is_empty());
        state.sent(&prepared);
        let offer = state.next_offer(&store, None).unwrap().unwrap();
        assert!(matches!(offer, Message::MsgLeiosBlockOffer { .. }));
        assert!(matches!(
            state.next_offer(&store, None).unwrap(),
            Some(Message::MsgLeiosBlockOffer { .. })
        ));
        state.sent(&offer);
        assert!(state.next_offer(&store, None).unwrap().is_none());
        state.prune(22);
        assert!(state.blocks.is_empty() && state.headers.is_empty());
    }

    #[test]
    fn deferred_offer_history_is_bounded_without_forgetting_live_entries() {
        let mut state = NotifyState::default();
        for slot in 0..MAX_NOTIFICATIONS as u64 {
            state
                .observe(
                    NotificationEntry {
                        sources: vec![],
                        notification: LeiosNotification::BlockOffer {
                            point: Point::Specific {
                                slot,
                                hash: [0; 32],
                            },
                            eb_size: 1,
                        },
                    },
                    PeerId(7),
                    None,
                    0,
                )
                .unwrap();
        }
        let extra = NotificationEntry {
            sources: vec![],
            notification: LeiosNotification::BlockTxsOffer {
                point: Point::Specific {
                    slot: MAX_NOTIFICATIONS as u64,
                    hash: [0; 32],
                },
            },
        };
        assert!(state.observe(extra.clone(), PeerId(7), None, 0).is_err());
        assert_eq!(state.blocks.len(), MAX_NOTIFICATIONS);
        state.prune(1);
        assert!(state.observe(extra, PeerId(7), None, 1).is_ok());
    }
}
