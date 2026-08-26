//! Content-addressed store for Leios data (endorser blocks, votes).
//!
//! Separate from `ChainStore` because Leios data is keyed by `(slot, hash)`,
//! not part of a linear chain. Praos has no equivalent — all Praos data lives
//! on the chain itself.
//!
//! The coordinator writes (injects EBs, votes).
//! Server-side protocol handlers read (block lookups, vote lookups,
//! notification subscriptions).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use shared_consensus::PeerId;

use crate::protocols::leios_fetch::bitmap;
use crate::types::{Point, Vote, WrappedHeader};
use shared_consensus::mempool::{TxBody, TxId};
use shared_consensus::types::hex_prefix;
use tracing::info;

/// Resolves a transaction body by its 32-byte hash. The Leios store calls
/// this when a peer asks for an EB's txs and only the manifest is cached
/// locally — typically the host application's mempool answers.
pub trait TxBodyResolver: Send + Sync {
    /// Return the body for `tx_id`, or `None` if unknown.
    fn resolve_body(&self, tx_id: &TxId) -> Option<TxBody>;
}

/// A notification about available Leios data, served by LeiosNotify.
#[derive(Debug, Clone)]
pub enum LeiosNotification {
    /// An endorser block is available for download. `eb_size` is the
    /// byte length of the encoded EB body — required by CIP-0164 so a
    /// peer can pre-size its fetch buffer; advertising `0` makes the
    /// dev relay drop the connection.
    BlockOffer { point: Point, eb_size: u32 },
    /// An EB's transactions are available for download.
    BlockTxsOffer { point: Point },
    /// CIP-0164 EB **announcement** — the fast discovery pulse. Carries the
    /// RB `header` whose `announced_eb = (hash, size)` field commits to an EB,
    /// diffused ahead of (and independent from) the EB-body `BlockOffer`.
    /// Distinct from `BlockOffer`: this advertises that an EB *exists* (by the
    /// header's commitment), not that its body is available to fetch. Not
    /// deduplicated — the producer decides how many to enqueue (one honestly,
    /// two for an OCIN equivocation), so `offer_point` returns `None` for it.
    BlockAnnouncement { header: WrappedHeader },
    /// Votes delivered inline (no offer/fetch round-trip).
    /// `retention_slot` is the tip slot when the votes were injected;
    /// wire votes carry no slot, so this is what ages the notification
    /// out of the slot-window prune alongside everything else.
    Votes {
        retention_slot: u64,
        votes: Vec<Vote>,
    },
}

/// A queued notification with every peer that has advertised this data
/// to us so far (empty `sources` when locally produced).
/// `serve_leios_notify` consults `sources` to skip re-offering data back
/// to any peer that supplied it.
///
/// `BlockOffer` and `BlockTxsOffer` entries are deduplicated by point in
/// `push_notification`: a second advertisement for the same EB appends
/// its source to an existing queued entry instead of pushing another
/// entry, so a downstream peer never sees the same offer twice in a
/// burst.  `Votes` entries are not deduplicated — different injections
/// carry distinct vote sets.
#[derive(Debug, Clone)]
pub struct NotificationEntry {
    pub sources: Vec<PeerId>,
    pub notification: LeiosNotification,
}

/// Key for block lookups. `Ord` so retransmit bookkeeping can hold these in a
/// `BTreeMap` and iterate oldest-slot-first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct BlockKey {
    slot: u64,
    hash: [u8; 32],
}

/// Retransmit bookkeeping for an EB WE produced: when we first offered it,
/// how many times we have re-offered, and whether any peer has fetched it.
#[derive(Debug, Clone, Copy)]
struct OwnOffer {
    first_offered_slot: u64,
    retries: u8,
    fetched: bool,
}

struct LeiosStoreInner {
    /// Endorser blocks keyed by (slot, hash).
    blocks: HashMap<BlockKey, Vec<u8>>,
    /// Retransmit state for locally produced EB offers, keyed like `blocks`.
    /// Only our own EBs are tracked — re-offering a peer's EB back into the
    /// network is not ours to do.
    own_offers: BTreeMap<BlockKey, OwnOffer>,
    /// Transaction bodies per EB, keyed by manifest index. Sparse — a
    /// receiver accumulating partial bitmap responses populates only the
    /// indices it has seen so far. The producer populates `0..N` in one
    /// shot. `get_block_txs` falls through to manifest+resolver for any
    /// index missing here, so partial holdings still serve subsets to
    /// downstream peers.
    block_txs: HashMap<BlockKey, BTreeMap<u32, TxBody>>,
    /// Per-EB ordered tx hash list. Populated by receivers after decoding
    /// a fetched EB manifest. Pairs with `tx_body_resolver` to serve the
    /// bodies indirectly without keeping a duplicate copy.
    eb_tx_hashes: HashMap<BlockKey, Vec<TxId>>,
    /// Votes keyed by `(announcing_rb_hash, voter_id)` for dedup and
    /// re-serving. The `u64` in the value is the tip slot observed when
    /// the vote was injected — used as the retention key so votes age
    /// out on the same slot window as the RBs they reference (the wire
    /// vote carries no slot of its own).
    votes: HashMap<([u8; 32], u16), (u64, Vote)>,
    /// Notification queue for the LeiosNotify server.  Front-pruned
    /// alongside the slot-window eviction of the other maps so
    /// long-running connections don't accumulate notifications for
    /// data that no longer lives in the store.
    notifications: VecDeque<NotificationEntry>,
    /// Total notifications popped off `notifications`' front so far —
    /// used to translate `notifications_after`'s logical cursor into
    /// the deque's local index.  Subscribers track a monotonically
    /// increasing logical position; when their cursor falls behind
    /// this count, `notifications_after` advances it to the front.
    notifications_pruned_count: usize,
    /// Max number of blocks to retain.
    capacity: usize,
    /// Monotonically increasing counter for change notifications.
    version: u64,
    /// Highest slot observed in any inject — drives slot-based eviction.
    max_slot: u64,
    /// Slot-window retention. Entries older than `max_slot - retention_slots`
    /// are evicted on every `bump_version`. Bounds memory under sustained
    /// EB / vote load (each EB carries ~600 votes; without slot eviction,
    /// receivers accumulate the full history forever).
    retention_slots: u64,
    /// Log a stats line every Nth `bump_version` call. `0` disables.
    stats_log_interval: u64,
}

impl LeiosStoreInner {
    /// True when every collection is empty — used by `tick_slot` to skip
    /// the eviction sweep once the store has drained.
    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
            && self.block_txs.is_empty()
            && self.eb_tx_hashes.is_empty()
            && self.votes.is_empty()
            && self.notifications.is_empty()
    }
}

/// Snapshot of internal map sizes — for memory diagnostics.
///
/// `notifications_bytes_estimate` is a precise byte sum over the
/// notifications log: each `BlockOffer` / `BlockTxsOffer` is fixed-size,
/// but `Votes` carries a variable-length `Vec<Vote>` so its payload
/// bytes are summed directly.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LeiosStoreStats {
    pub blocks: usize,
    pub block_txs: usize,
    pub eb_tx_hashes: usize,
    pub votes: usize,
    pub notifications: usize,
    pub notifications_bytes_estimate: usize,
    pub max_slot: u64,
}

/// Default slot-window retention for `LeiosStore`. Sized for the Linear Leios
/// pipeline (13 slots end-to-end) plus comfortable headroom — peers fetching
/// EBs / votes / manifests need only a window long enough to complete the
/// pipeline. Smaller than `LeiosTracker`'s 1000-slot dedup window because
/// the tracker stores tiny offer IDs while this store holds full bodies.
pub const DEFAULT_RETENTION_SLOTS: u64 = 100;

/// Hard ceiling on the `notifications` deque, independent of the slot
/// window. Slot-window eviction is the primary bound, but it relies on
/// front-only popping and on `max_slot` advancing; a peer that floods
/// offers within the active window, or injects with out-of-order slots
/// that bury an evictable entry behind a higher-slot front, could still
/// inflate the deque. This backstop guarantees a fixed upper bound on
/// notification memory regardless of inject order or offer rate — the
/// same role `capacity` plays for `blocks`. Sized well above normal
/// operation (cluster runs peak around ~120 notifications/node).
pub const MAX_NOTIFICATIONS: usize = 10_000;

/// Thread-safe content-addressed store for Leios data.
///
/// Follows the same Mutex + watch pattern as `ChainStore`.
pub struct LeiosStore {
    inner: Mutex<LeiosStoreInner>,
    notify: watch::Sender<u64>,
    /// Optional callback that resolves a tx body by its hash. Retained as an
    /// extension point: `get_block_txs` now serves EB txs ONLY from stored
    /// `block_txs` (own EBs pinned at produce time, received EBs fully fetched),
    /// because resolving a received EB's manifest against the mempool served
    /// wrong bodies (`MsgLeiosBlockTxs` hash mismatch → connection tear-down). A
    /// future hash-verified resolver could reuse this hook.
    #[allow(dead_code)]
    tx_body_resolver: Option<Arc<dyn TxBodyResolver>>,
}

impl LeiosStore {
    /// Create a new Leios store with the given block capacity.
    ///
    /// Returns the store (wrapped in `Arc`) and a subscription receiver
    /// for change notifications.
    pub fn new(capacity: usize) -> (Arc<Self>, watch::Receiver<u64>) {
        Self::new_with_resolver(capacity, None)
    }

    /// Create a Leios store with an optional `TxBodyResolver` for serving
    /// EB tx requests on receiver nodes that cache only the manifest.
    pub fn new_with_resolver(
        capacity: usize,
        tx_body_resolver: Option<Arc<dyn TxBodyResolver>>,
    ) -> (Arc<Self>, watch::Receiver<u64>) {
        Self::new_with_retention(capacity, tx_body_resolver, DEFAULT_RETENTION_SLOTS, 0)
    }

    /// Full constructor: explicit slot-window retention and stats logging
    /// interval. `stats_log_interval` of `0` disables stats logging.
    pub fn new_with_retention(
        capacity: usize,
        tx_body_resolver: Option<Arc<dyn TxBodyResolver>>,
        retention_slots: u64,
        stats_log_interval: u64,
    ) -> (Arc<Self>, watch::Receiver<u64>) {
        let (notify_sender, notify_receiver) = watch::channel(0u64);
        let store = Arc::new(Self {
            inner: Mutex::new(LeiosStoreInner {
                blocks: HashMap::new(),
                own_offers: BTreeMap::new(),
                block_txs: HashMap::new(),
                eb_tx_hashes: HashMap::new(),
                votes: HashMap::new(),
                notifications: VecDeque::new(),
                notifications_pruned_count: 0,
                capacity,
                version: 0,
                max_slot: 0,
                retention_slots,
                stats_log_interval,
            }),
            notify: notify_sender,
            tx_body_resolver,
        });
        (store, notify_receiver)
    }

    /// Inject an endorser block. Generates a BlockOffer notification.
    ///
    /// `source` is the peer that delivered the block (`None` for locally
    /// produced EBs); `serve_leios_notify` filters its notification
    /// stream to that peer so we don't re-offer the block back to its
    /// origin.
    ///
    /// The `point` must be `Point::Specific { slot, hash }`. If `Point::Origin`
    /// is passed, the block is silently dropped.
    pub fn inject_block(&self, point: Point, block: Vec<u8>, source: Option<PeerId>) {
        let (slot, hash) = match &point {
            Point::Specific { slot, hash } => (*slot, *hash),
            Point::Origin => return,
        };
        let eb_size = u32::try_from(block.len()).unwrap_or_else(|_| {
            tracing::warn!(
                len = block.len(),
                "EB exceeds u32::MAX; clamping advertised eb_size"
            );
            u32::MAX
        });
        let mut inner = self.inner.lock().unwrap();
        let key = BlockKey { slot, hash };
        let was_new = inner.blocks.insert(key, block).is_none();
        inner.max_slot = inner.max_slot.max(slot);
        // Start retransmit bookkeeping for an EB of our own. A `BlockOffer` is
        // sent once per peer and `push_notification` deliberately drops any
        // re-advertisement, so a peer whose fetch decision does not act on that
        // single offer never requests the body — and an EB nobody fetches can
        // never be voted on, let alone certified.
        if source.is_none() && was_new {
            let first_offered_slot = inner.max_slot;
            inner.own_offers.insert(
                key,
                OwnOffer {
                    first_offered_slot,
                    retries: 0,
                    fetched: false,
                },
            );
        }
        Self::push_notification(
            &mut inner,
            source,
            LeiosNotification::BlockOffer { point, eb_size },
            was_new,
        );
        self.bump_version(&mut inner);
    }

    /// Enqueue a CIP-0164 EB **announcement** (`MsgLeiosBlockAnnouncement`) for
    /// diffusion. `header` is the RB header whose `announced_eb` commits to an
    /// EB — the fast discovery pulse, distinct from and ahead of the EB-body
    /// `BlockOffer` that `inject_block` generates.
    ///
    /// `source` is the peer that delivered the header (`None` for locally
    /// produced RBs); the no-echo gate in `serve_leios_notify` uses it to avoid
    /// re-announcing back to the origin. Announcements are **not** deduplicated,
    /// so calling this twice for one election (with distinct headers) diffuses
    /// both — the mechanism an OCIN-equivocation adversary relies on.
    pub fn announce_block(&self, header: WrappedHeader, source: Option<PeerId>) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(Point::Specific { slot, .. }) = header.point() {
            inner.max_slot = inner.max_slot.max(slot);
        }
        Self::push_notification(
            &mut inner,
            source,
            LeiosNotification::BlockAnnouncement { header },
            true,
        );
        self.bump_version(&mut inner);
    }

    /// Merge transaction bodies for an endorser block, indexed by their
    /// position in the EB manifest. Producers call once with indices
    /// `0..N` populated; receivers call repeatedly as partial bitmap
    /// responses arrive. Existing entries are preserved on conflict
    /// (first writer wins).
    ///
    /// A `BlockTxsOffer` notification fires only on the first call for
    /// a given EB. Subsequent merges are silent — peers already know we
    /// have *something* for this EB; their next fetch sees the new
    /// coverage.
    ///
    /// The `point` must be `Point::Specific { slot, hash }`. If
    /// `Point::Origin` is passed, the transactions are silently dropped.
    /// `source` tags the resulting `BlockTxsOffer` with the peer that
    /// supplied the bodies (`None` for locally produced) so
    /// `serve_leios_notify` can skip re-offering them back to that peer.
    pub fn inject_block_txs(
        &self,
        point: Point,
        indexed: BTreeMap<u32, TxBody>,
        source: Option<PeerId>,
    ) {
        let (slot, hash) = match &point {
            Point::Specific { slot, hash } => (*slot, *hash),
            Point::Origin => return,
        };
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.block_txs.entry(BlockKey { slot, hash }).or_default();
        let was_new = entry.is_empty();
        for (idx, body) in indexed {
            entry.entry(idx).or_insert(body);
        }
        info!("leios_store: injecting block_txs {point}: txs {}, from {source:?}", entry.len());
        inner.max_slot = inner.max_slot.max(slot);
        Self::push_notification(
            &mut inner,
            source,
            LeiosNotification::BlockTxsOffer { point },
            was_new,
        );
        self.bump_version(&mut inner);
    }

    /// Convenience for the producer path: inject a complete ordered body
    /// list, indices `0..bodies.len()`. Equivalent to constructing a
    /// `BTreeMap` and calling `inject_block_txs`.
    pub fn inject_block_txs_full(&self, point: Point, bodies: Vec<TxBody>, source: Option<PeerId>) {
        let indexed: BTreeMap<u32, TxBody> = bodies
            .into_iter()
            .enumerate()
            .map(|(i, b)| (i as u32, b))
            .collect();

        info!("leios_store: injecting full block_txs {point}: txs {}, from {source:?}", indexed.len());
        self.inject_block_txs(point, indexed, source);
    }

    /// Inject votes for inline re-serving. Generates a `Votes` notification
    /// carrying the full vote bodies (deduped by `(slot, eb_hash, voter_id)`).
    ///
    /// `source` tags the notification with the peer that sent these
    /// votes (`None` if locally produced); `serve_leios_notify` filters
    /// the connection's stream to skip echoing them back.
    pub fn inject_votes(&self, votes: Vec<Vote>, source: Option<PeerId>) {
        if votes.is_empty() {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        // Tag each vote with the current tip slot for retention (the wire
        // vote has no slot; votes trail their announcing RB by a few
        // slots, so the tip slot is a faithful window key).
        let seen_slot = inner.max_slot;
        // Filter to genuinely-new votes — anything already in `inner.votes`
        // has been advertised previously and would generate a duplicate
        // wire notification if we re-pushed the full batch.
        let mut new_votes = Vec::with_capacity(votes.len());
        for vote in votes {
            let key = (vote.announcing_rb_hash, vote.voter_id);
            if !inner.votes.contains_key(&key) {
                new_votes.push(vote.clone());
            }
            inner.votes.insert(key, (seen_slot, vote));
        }
        if !new_votes.is_empty() {
            Self::push_notification(
                &mut inner,
                source,
                LeiosNotification::Votes {
                    retention_slot: seen_slot,
                    votes: new_votes,
                },
                true,
            );
        }
        self.bump_version(&mut inner);
    }

    /// Look up an endorser block by (slot, hash).
    pub fn get_block(&self, slot: u64, hash: &[u8; 32]) -> Option<Vec<u8>> {
        let mut inner = self.inner.lock().unwrap();
        let key = BlockKey { slot, hash: *hash };
        // Serving the body is our acknowledgement that the offer landed: stop
        // retransmitting it (see `reoffer_unfetched`).
        if let Some(own) = inner.own_offers.get_mut(&key) {
            own.fetched = true;
        }
        inner.blocks.get(&key).cloned()
    }

    /// Re-offer any EB of ours that no peer has fetched yet.
    ///
    /// A `BlockOffer` reaches each peer exactly once — `push_notification`
    /// drops re-advertisements to keep wire traffic down — and the consumer
    /// only requests the body if its fetch decision acts on that offer while
    /// it is live. When it does not, the EB is never requested, never
    /// validated, never voted on, and so never certified: measured on the
    /// proto-devnet, the Haskell peers accepted the announcement and the offer
    /// for 9 of 15 net-rs EBs and then sent no `MsgLeiosBlockRequest` at all,
    /// with successes scattered through the run rather than stopping at a
    /// cliff — the signature of a race, not an exhausted fetch budget.
    ///
    /// So retransmit, on the same principle as any unacknowledged send: re-push
    /// the offer every `RETRY_INTERVAL_SLOTS` until a peer actually fetches the
    /// body, at most `MAX_RETRIES` times. Serving the body counts as the
    /// acknowledgement (`get_block`). Bounded on both axes, and entries age out
    /// with the retention window, so a peer that never fetches costs a few
    /// extra notifications rather than unbounded traffic.
    ///
    /// Only our OWN EBs are retransmitted; relaying someone else's is not ours
    /// to do.
    pub fn reoffer_unfetched(&self, current_slot: u64) {
        /// Slots between retransmits. Comfortably longer than a fetch
        /// round-trip on a local cluster, short enough to land several retries
        /// inside the EB's voting window.
        const RETRY_INTERVAL_SLOTS: u64 = 3;
        /// Retransmits per EB, after the original offer.
        const MAX_RETRIES: u8 = 3;

        let mut inner = self.inner.lock().unwrap();
        let cutoff = inner.max_slot.saturating_sub(inner.retention_slots);
        // Forget anything aged out of retention, fetched or not: its body is
        // gone from `blocks`, so re-offering it would advertise data we can no
        // longer serve.
        inner
            .own_offers
            .retain(|key, own| key.slot > cutoff && !own.fetched && own.retries < MAX_RETRIES);

        let due: Vec<(BlockKey, u32)> = inner
            .own_offers
            .iter()
            .filter(|(key, own)| {
                let next_due = own.first_offered_slot
                    + RETRY_INTERVAL_SLOTS * u64::from(own.retries + 1);
                current_slot >= next_due && inner.blocks.contains_key(key)
            })
            .map(|(key, _)| {
                let size = inner
                    .blocks
                    .get(key)
                    .map(|b| u32::try_from(b.len()).unwrap_or(u32::MAX))
                    .unwrap_or(0);
                (*key, size)
            })
            .collect();
        if due.is_empty() {
            return;
        }
        // Visible at info: whether this retransmit path fires at all is the
        // difference between "the one-shot offer was the problem" and "it was
        // not", and a debug-level line answers neither on a node running at
        // info.
        let mut reoffered = 0usize;
        let still_unfetched = inner.own_offers.len();
        for (key, eb_size) in due {
            if let Some(own) = inner.own_offers.get_mut(&key) {
                own.retries = own.retries.saturating_add(1);
            }
            let point = Point::Specific {
                slot: key.slot,
                hash: key.hash,
            };
            tracing::debug!(
                slot = key.slot,
                eb_size,
                "re-offering EB no peer has fetched"
            );
            reoffered += 1;
            // `was_new = true` forces past the re-advertisement drop in
            // `push_notification`: that guard exists to suppress redundant
            // offers, and this offer is the opposite — the previous one
            // demonstrably did not produce a fetch.
            inner
                .notifications
                .push_back(NotificationEntry {
                    sources: Vec::new(),
                    notification: LeiosNotification::BlockOffer { point, eb_size },
                });
        }
        tracing::info!(
            reoffered,
            still_unfetched,
            "re-offered EB bodies no peer has fetched"
        );
        Self::evict_old(&mut inner);
        self.bump_version(&mut inner);
    }

    /// Record the ordered tx-hash list of an EB's manifest (for our own
    /// voting, and for serving via the `TxBodyResolver` when we actually
    /// hold the bodies).
    ///
    /// This does NOT emit a `BlockTxsOffer`. Recording a manifest means we
    /// know an EB's tx *hashes*, not that we hold its tx *bodies* — for a
    /// relayed EB whose txs never entered our mempool (e.g. a peer's
    /// overflow EB), the resolver can't produce them. Advertising txs we
    /// can't serve makes peers request them, miss, and (per CIP-0164) get
    /// disconnected — which collaterally drops their in-flight votes on our
    /// OWN EBs, the exact cause of lost EB certifications. Offers are emitted
    /// only by `inject_block_txs*`, which store the bodies, so we never
    /// advertise what we can't serve. `_source` is retained for API
    /// stability.
    pub fn record_eb_manifest(&self, point: Point, tx_hashes: Vec<TxId>, _source: Option<PeerId>) {
        let (slot, hash) = match &point {
            Point::Specific { slot, hash } => (*slot, *hash),
            Point::Origin => return,
        };
        let mut inner = self.inner.lock().unwrap();
        inner.eb_tx_hashes.insert(BlockKey { slot, hash }, tx_hashes);
        inner.max_slot = inner.max_slot.max(slot);
        self.bump_version(&mut inner);
    }

    /// Look up transactions for an endorser block, filtered by the
    /// CIP-0164 sparse bitmap. Returns `None` if the EB is unknown
    /// (neither sparse `block_txs` nor a manifest is recorded).
    ///
    /// For each requested index, prefers a body from the sparse
    /// `block_txs` map; falls through to manifest + `TxBodyResolver`
    /// for indices not yet held there. Returns the union — bodies in
    /// ascending index order, silently dropping indices whose body
    /// neither path can supply (partial response).
    pub fn get_block_txs(
        &self,
        slot: u64,
        hash: &[u8; 32],
        bitmap: &BTreeMap<u16, u64>,
        peer_id: PeerId,
    ) -> Option<Vec<TxBody>> {
        let key = BlockKey { slot, hash: *hash };
        let block_txs = {
            let inner = self.inner.lock().unwrap();
            inner.block_txs.get(&key).cloned()
        };
        // Serve ONLY from stored bodies (`block_txs`): our own EBs — whose bodies
        // are pinned at produce time — and received EBs we have fully fetched.
        // We deliberately do NOT fall back to the mempool tx-body resolver: for a
        // RECEIVED EB we may hold the manifest but not the exact committed bodies,
        // and serving resolver-guessed txs yields a `MsgLeiosBlockTxs` hash
        // mismatch that tears the mux connection down. Serve the COMPLETE
        // requested set in bitmap order, or `None` so the responder disconnects
        // (CIP-0164) — never a partial/misaligned or wrong-body response.
        let Some(block_txs) = block_txs else {
            return None;
        };
        let selected: Vec<TxBody> = bitmap::iter_indices(bitmap)
            .map(|i| block_txs.get(&i).cloned())
            .collect::<Option<Vec<TxBody>>>()?;
        info!("leios_store: getting block {slot}/{} txs {}/{}; to {peer_id}", hex_prefix(hash), selected.len(), block_txs.len());
        Some(selected)
    }

    /// Look up the ordered tx-hash manifest for an EB, if recorded.
    /// Receivers consult this to map a fetched body's content hash to
    /// its position in the EB before merging into `block_txs`.
    pub fn get_eb_manifest(&self, slot: u64, hash: &[u8; 32]) -> Option<Vec<TxId>> {
        let inner = self.inner.lock().unwrap();
        let key = BlockKey { slot, hash: *hash };
        inner.eb_tx_hashes.get(&key).cloned()
    }

    /// Snapshot of internal map sizes — for memory diagnostics.
    pub fn stats(&self) -> LeiosStoreStats {
        let inner = self.inner.lock().unwrap();
        // Each ring-buffer slot costs sizeof(LeiosNotification);
        // `notification_heap_bytes` adds only the extra `Votes` payload
        // on top, so the enum size isn't counted twice.
        let per_entry_overhead = std::mem::size_of::<NotificationEntry>();
        let notifications_bytes_estimate = inner.notifications.len() * per_entry_overhead
            + inner
                .notifications
                .iter()
                .map(|e| notification_heap_bytes(&e.notification))
                .sum::<usize>()
            + std::mem::size_of::<VecDeque<NotificationEntry>>();
        LeiosStoreStats {
            blocks: inner.blocks.len(),
            block_txs: inner.block_txs.len(),
            eb_tx_hashes: inner.eb_tx_hashes.len(),
            votes: inner.votes.len(),
            notifications: inner.notifications.len(),
            notifications_bytes_estimate,
            max_slot: inner.max_slot,
        }
    }

    /// Get notifications after the given logical index (exclusive).
    ///
    /// The cursor `after` is monotonically increasing across the
    /// connection's lifetime — never reset, never shifted by pruning.
    /// If the caller's cursor has fallen behind the prune frontier,
    /// it's bumped up to the frontier so subsequent `*after += 1`
    /// increments stay aligned with the logical position of items the
    /// caller actually consumes.  Index `0` still means "from the
    /// earliest still-retained notification".
    pub fn notifications_after(&self, after: &mut usize) -> Vec<NotificationEntry> {
        let inner = self.inner.lock().unwrap();
        if *after < inner.notifications_pruned_count {
            *after = inner.notifications_pruned_count;
        }
        let local = *after - inner.notifications_pruned_count;
        if local >= inner.notifications.len() {
            // Clamp to the next-write index so a consumer that overshot
            // (asked for an index beyond `notification_count()`)
            // reconverges on the next inject instead of staying stuck
            // forever.
            *after = inner.notifications_pruned_count + inner.notifications.len();
            return Vec::new();
        }
        inner.notifications.range(local..).cloned().collect()
    }

    /// Total notifications ever pushed (including those since pruned).
    pub fn notification_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.notifications_pruned_count + inner.notifications.len()
    }

    /// Advance the retention clock to `current_slot`.  Triggers slot-window
    /// eviction even when no injects are happening, so a node that stops
    /// receiving Leios data (peer disconnects, partition) doesn't freeze
    /// its retention window at the last seen `max_slot`.  Cluster runs
    /// showed nodes with `slot − max_slot` of 100+ — retention stalled and
    /// stale notifications stayed retained.  Host should call once per
    /// wall-clock slot from its slot ticker.
    ///
    /// Does **not** bump the version counter or wake watch subscribers:
    /// no new notification was added, so there's nothing for a subscriber
    /// to consume.  Eviction is silent — already-delivered notifications
    /// disappearing from the back of the buffer doesn't concern readers.
    pub fn tick_slot(&self, current_slot: u64) {
        let mut inner = self.inner.lock().unwrap();
        if current_slot > inner.max_slot {
            inner.max_slot = current_slot;
            // Skip the O(n) retain sweep when nothing is retained — the
            // common idle/partition case this method exists for. Once the
            // store drains, every subsequent wall-clock tick is a cheap
            // no-op rather than four empty `retain` passes per slot.
            if !inner.is_empty() {
                Self::evict_old(&mut inner);
            }
        }
    }

    /// Subscribe to change notifications.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.notify.subscribe()
    }

    /// Current version (monotonically increasing).
    pub fn version(&self) -> u64 {
        self.inner.lock().unwrap().version
    }

    /// Push a notification, applying content-based dedup before adding a
    /// new entry to the queue.  `was_new` tells us whether the inject
    /// just brought genuinely-new data into storage; combined with the
    /// queue scan it covers both dedup regimes:
    ///
    /// 1. Matching entry still queued (multi-peer advertisements during
    ///    the same fan-out): append `source` to the queued entry's
    ///    `sources` list and return.  Preserves no-echo coverage for
    ///    every peer that supplied the data while only one wire
    ///    notification reaches downstream peers.
    /// 2. Matching entry already drained, `was_new = false`: the data
    ///    has been advertised once before and is not new in storage —
    ///    a re-advertisement now would push a redundant wire offer to
    ///    every connected peer.  Drop silently.
    /// 3. Matching entry already drained, `was_new = true`: only
    ///    possible if storage was pruned between the original and this
    ///    inject (slot retention).  Push a fresh entry.
    ///
    /// Also filters out notifications whose referenced slots all sit
    /// below the retention cutoff — the front-only pop_front loop in
    /// `evict_old` can't reach a notification queued at the back, so a
    /// late-arriving offer for data already past retention has to be
    /// dropped here.  Caller must have updated `max_slot` already so
    /// the cutoff reflects the post-inject state.
    fn push_notification(
        inner: &mut LeiosStoreInner,
        source: Option<PeerId>,
        notif: LeiosNotification,
        was_new: bool,
    ) {
        let cutoff = inner.max_slot.saturating_sub(inner.retention_slots);
        if cutoff > 0 && notification_evictable(&notif, cutoff) {
            return;
        }
        if let Some(point) = offer_point(&notif) {
            for entry in inner.notifications.iter_mut().rev() {
                if same_offer_kind(&entry.notification, &notif)
                    && offer_point(&entry.notification) == Some(point)
                {
                    if let Some(peer) = source {
                        if !entry.sources.contains(&peer) {
                            entry.sources.push(peer);
                        }
                    }
                    return;
                }
            }
            if !was_new {
                return;
            }
        }
        let sources = source.map_or_else(Vec::new, |p| vec![p]);
        inner.notifications.push_back(NotificationEntry {
            sources,
            notification: notif,
        });
    }

    /// Run slot-window eviction across all maps plus the capacity backstop
    /// on `blocks`.  Pure data work — does not touch `version` or the
    /// watch channel.  Used by both `bump_version` (after an inject) and
    /// `tick_slot` (silent wall-clock advance).
    fn evict_old(inner: &mut LeiosStoreInner) {
        let cutoff = inner.max_slot.saturating_sub(inner.retention_slots);
        if cutoff > 0 {
            inner.blocks.retain(|key, _| key.slot >= cutoff);
            inner.block_txs.retain(|key, _| key.slot >= cutoff);
            inner.eb_tx_hashes.retain(|key, _| key.slot >= cutoff);
            inner.votes.retain(|_, (slot, _)| *slot >= cutoff);
            // Front-prune `notifications` for entries that reference
            // only data older than the cutoff.  `push_notification`
            // refuses anything already below cutoff, so any below-cutoff
            // survivors must have aged past the boundary while at the
            // front — `pop_front` is both safe and O(evicted) without
            // scanning the whole deque.
            while let Some(front) = inner.notifications.front() {
                if notification_evictable(&front.notification, cutoff) {
                    inner.notifications.pop_front();
                    inner.notifications_pruned_count += 1;
                } else {
                    break;
                }
            }
        }

        // Capacity backstop on `notifications` (independent of slot
        // window). Front-only popping keeps absolute indexing intact:
        // every pop increments `notifications_pruned_count`, and a
        // subscriber whose cursor falls behind is fast-forwarded by
        // `notifications_after`. Drops the oldest offers first, matching
        // the slot-window prune direction.
        while inner.notifications.len() > MAX_NOTIFICATIONS {
            inner.notifications.pop_front();
            inner.notifications_pruned_count += 1;
        }

        // Capacity backstop on `blocks` (independent of slot window). Evict the
        // OLDEST blocks first. `blocks` is a HashMap, whose `keys()` iterate in
        // arbitrary order — taking the first N would drop random (possibly
        // just-offered) EBs. Sort by BlockKey (slot, then hash) so we shed the
        // lowest slots, matching the slot-window prune direction.
        if inner.blocks.len() > inner.capacity {
            let excess = inner.blocks.len() - inner.capacity;
            let mut keys: Vec<BlockKey> = inner.blocks.keys().cloned().collect();
            keys.sort_unstable(); // ascending BlockKey == oldest slot first
            for key in keys.into_iter().take(excess) {
                inner.blocks.remove(&key);
                inner.block_txs.remove(&key);
                info!("leios_store: removing old block_txs {}/{}", key.slot, hex_prefix(&key.hash));
            }
        }
    }

    fn bump_version(&self, inner: &mut LeiosStoreInner) {
        inner.version += 1;
        Self::evict_old(inner);

        // Optional diagnostic: emit a stats line every Nth bump so we can
        // spot unbounded growth from outside. `0` disables.
        if inner.stats_log_interval > 0 && inner.version.is_multiple_of(inner.stats_log_interval) {
            let cutoff = inner.max_slot.saturating_sub(inner.retention_slots);
            tracing::info!(
                version = inner.version,
                max_slot = inner.max_slot,
                cutoff,
                blocks = inner.blocks.len(),
                block_txs = inner.block_txs.len(),
                eb_tx_hashes = inner.eb_tx_hashes.len(),
                votes = inner.votes.len(),
                notifications = inner.notifications.len(),
                "leios_store: stats"
            );
        }

        let version = inner.version;
        let _ = self.notify.send(version);
    }
}

/// The point an offer-type notification refers to. `None` for `Votes`
/// (which carries vote bodies, not a single point).  Used by
/// `push_notification` for offer dedup.
fn offer_point(n: &LeiosNotification) -> Option<&Point> {
    match n {
        LeiosNotification::BlockOffer { point, .. }
        | LeiosNotification::BlockTxsOffer { point } => Some(point),
        // Announcements and votes are never offer-deduplicated.
        LeiosNotification::BlockAnnouncement { .. } | LeiosNotification::Votes { .. } => None,
    }
}

/// True iff `a` and `b` are the same offer variant (both `BlockOffer`
/// or both `BlockTxsOffer`).  Returns `false` if either is `Votes`.
fn same_offer_kind(a: &LeiosNotification, b: &LeiosNotification) -> bool {
    matches!(
        (a, b),
        (
            LeiosNotification::BlockOffer { .. },
            LeiosNotification::BlockOffer { .. }
        ) | (
            LeiosNotification::BlockTxsOffer { .. },
            LeiosNotification::BlockTxsOffer { .. }
        )
    )
}

/// True iff every slot the notification references is below `cutoff`
/// — i.e. the notification only points at data that's already been
/// evicted from the slot-window-pruned maps and can never be served.
fn notification_evictable(n: &LeiosNotification, cutoff: u64) -> bool {
    match n {
        LeiosNotification::BlockOffer { point, .. }
        | LeiosNotification::BlockTxsOffer { point } => match point {
            Point::Specific { slot, .. } => *slot < cutoff,
            Point::Origin => true,
        },
        LeiosNotification::Votes { retention_slot, .. } => *retention_slot < cutoff,
        // Age out by the announcing header's own slot. An unparseable header
        // (point() == None) can't be aged by slot, so treat it as evictable —
        // otherwise a peer could crowd the notifications queue with
        // syntactically-decodable but unparseable headers that never prune.
        LeiosNotification::BlockAnnouncement { header } => match header.point() {
            Some(Point::Specific { slot, .. }) => slot < cutoff,
            _ => true,
        },
    }
}

/// Extra heap bytes beyond the fixed `size_of::<LeiosNotification>()`
/// slot in the deque.  Zero for `BlockOffer` / `BlockTxsOffer` (no heap
/// payload); for `Votes` it's the `Vec<Vote>` allocation (each `Vote` is
/// fixed-size, no internal heap).  The caller adds the fixed per-entry
/// size separately — keeping the two parts separate avoids double-counting
/// the enum size.
fn notification_heap_bytes(n: &LeiosNotification) -> usize {
    match n {
        LeiosNotification::BlockOffer { .. } | LeiosNotification::BlockTxsOffer { .. } => 0,
        LeiosNotification::Votes { votes, .. } => votes.len() * std::mem::size_of::<Vote>(),
        // The raw header CBOR is the announcement's heap payload.
        LeiosNotification::BlockAnnouncement { header } => header.raw.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_id(b: u8) -> TxId {
        TxId::new_with_array([b; 32])
    }

    fn tx_id_from_arr(arr: [u8; 32]) -> TxId {
        TxId::new_with_array(arr)
    }

    #[test]
    fn inject_and_get_block() {
        let (store, _rx) = LeiosStore::new(100);
        let hash = [0xABu8; 32];
        let block = vec![1, 2, 3, 4];
        let point = Point::Specific { slot: 42, hash };

        store.inject_block(point, block.clone(), None);

        assert_eq!(store.get_block(42, &hash), Some(block));
        assert_eq!(store.get_block(99, &hash), None);
    }

    #[test]
    fn get_block_txs_with_select_all_returns_all() {
        let (store, _rx) = LeiosStore::new(100);
        let hash = [0xCDu8; 32];
        let txs = vec![
            TxBody::new_with_vec(vec![10, 20]),
            TxBody::new_with_vec(vec![30, 40]),
        ];
        let point = Point::Specific { slot: 42, hash };

        store.inject_block_txs_full(point, txs.clone(), None);

        let bitmap = bitmap::select_all(txs.len() as u32);
        assert_eq!(store.get_block_txs(42, &hash, &bitmap, PeerId(1)), Some(txs));
        assert_eq!(store.get_block_txs(99, &hash, &bitmap, PeerId(3)), None);
    }

    #[test]
    fn get_block_txs_empty_bitmap_returns_empty() {
        let (store, _rx) = LeiosStore::new(100);
        let hash = [0xCDu8; 32];
        let txs = vec![
            TxBody::new_with_vec(vec![10, 20]),
            TxBody::new_with_vec(vec![30, 40]),
        ];
        let point = Point::Specific { slot: 42, hash };

        store.inject_block_txs_full(point, txs, None);

        let bitmap = BTreeMap::new();
        assert_eq!(store.get_block_txs(42, &hash, &bitmap, PeerId(7)), Some(Vec::new()));
    }

    #[test]
    fn get_block_txs_filters_by_bitmap_and_orders_ascending() {
        let (store, _rx) = LeiosStore::new(100);
        let hash = [0xEFu8; 32];
        let txs: Vec<TxBody> = (0..70u8).map(|i| TxBody::new_with_vec(vec![i])).collect();
        let point = Point::Specific { slot: 1, hash };

        store.inject_block_txs_full(point, txs, None);

        // Pick out-of-order indices spanning two segments to check ordering.
        let bitmap = bitmap::from_indices(&[65, 0, 63]);
        let got = store.get_block_txs(1, &hash, &bitmap, PeerId(11)).unwrap();
        assert_eq!(
            got,
            vec![
                TxBody::new_with_vec(vec![0u8]),
                TxBody::new_with_vec(vec![63u8]),
                TxBody::new_with_vec(vec![65u8])
            ]
        );
    }

    struct StubResolver(HashMap<TxId, TxBody>);
    impl TxBodyResolver for StubResolver {
        fn resolve_body(&self, tx_id: &TxId) -> Option<TxBody> {
            self.0.get(tx_id).cloned()
        }
    }

    #[test]
    fn get_block_txs_manifest_only_returns_none() {
        // A cached manifest WITHOUT stored bodies is not servable: we serve EB
        // txs only from `block_txs` (own EBs pinned at produce time; received EBs
        // fully fetched), never by resolving the manifest against the mempool —
        // that served wrong bodies for received EBs and tore the mux down.
        let h0 = [0x10u8; 32];
        let h1 = [0x20u8; 32];
        let h2 = [0x30u8; 32];
        let bodies = HashMap::from([
            (tx_id_from_arr(h0), TxBody::new_with_vec(vec![1u8])),
            (tx_id_from_arr(h1), TxBody::new_with_vec(vec![2u8])),
            (tx_id_from_arr(h2), TxBody::new_with_vec(vec![3u8])),
        ]);
        let resolver: Arc<dyn TxBodyResolver> = Arc::new(StubResolver(bodies));
        let (store, _rx) = LeiosStore::new_with_resolver(100, Some(resolver));

        let eb_hash = [0xEEu8; 32];
        let point = Point::Specific {
            slot: 5,
            hash: eb_hash,
        };
        store.record_eb_manifest(
            point,
            vec![tx_id_from_arr(h0), tx_id_from_arr(h1), tx_id_from_arr(h2)],
            None,
        );

        // Manifest present but no block_txs → None (resolver is not consulted).
        let bitmap = bitmap::from_indices(&[0, 2]);
        assert!(store.get_block_txs(5, &eb_hash, &bitmap, PeerId(23)).is_none());
    }

    #[test]
    fn get_block_txs_block_txs_takes_precedence_over_manifest() {
        // Producer-style store with both block_txs (full bodies) and a
        // manifest cache. The direct path should win.
        let resolver: Arc<dyn TxBodyResolver> = Arc::new(StubResolver(HashMap::new()));
        let (store, _rx) = LeiosStore::new_with_resolver(100, Some(resolver));
        let eb_hash = [0xABu8; 32];
        let point = Point::Specific {
            slot: 1,
            hash: eb_hash,
        };
        store.inject_block_txs_full(
            point.clone(),
            vec![
                TxBody::new_with_vec(vec![100u8]),
                TxBody::new_with_vec(vec![200u8]),
            ],
            None,
        );
        // Pretend we also have manifest hashes (would normally be set
        // separately; here we make sure the block_txs path wins).
        store.record_eb_manifest(point, vec![tx_id(0), tx_id(0)], None);

        let bitmap = bitmap::from_indices(&[0, 1]);
        let got = store.get_block_txs(1, &eb_hash, &bitmap, PeerId(42)).unwrap();
        assert_eq!(
            got,
            vec![
                TxBody::new_with_vec(vec![100u8]),
                TxBody::new_with_vec(vec![200u8])
            ]
        );
    }

    #[test]
    fn get_block_txs_returns_none_when_neither_path_has_eb() {
        let resolver: Arc<dyn TxBodyResolver> = Arc::new(StubResolver(HashMap::new()));
        let (store, _rx) = LeiosStore::new_with_resolver(100, Some(resolver));
        let bitmap = bitmap::from_indices(&[0]);
        assert!(store.get_block_txs(99, &[0xFF; 32], &bitmap, PeerId(5)).is_none());
    }

    #[test]
    fn get_block_txs_returns_none_when_an_index_is_missing() {
        let (store, _rx) = LeiosStore::new(100);
        let hash = [0xAA; 32];
        let txs = vec![
            TxBody::new_with_vec(vec![1u8]),
            TxBody::new_with_vec(vec![2u8]),
        ];
        let point = Point::Specific { slot: 5, hash };
        store.inject_block_txs_full(point, txs, None);

        // Bit 99 is past the available 2 txs. We must NOT serve a partial set —
        // a shorter/misaligned response is rejected downstream as a
        // MsgLeiosBlockTxs mismatch and tears the mux down. An unservable index
        // yields None so the responder disconnects (CIP-0164).
        let bitmap = bitmap::from_indices(&[0, 99]);
        assert!(store.get_block_txs(5, &hash, &bitmap, PeerId(17)).is_none());
    }

    #[test]
    fn inject_block_txs_partial_then_partial_unions_holdings() {
        let (store, _rx) = LeiosStore::new(100);
        let hash = [0x01u8; 32];
        let point = Point::Specific { slot: 7, hash };

        // First batch: indices 0 and 2.
        let mut first = BTreeMap::new();
        first.insert(0u32, TxBody::new_with_vec(vec![0xA0]));
        first.insert(2u32, TxBody::new_with_vec(vec![0xA2]));
        store.inject_block_txs(point.clone(), first, None);

        // Second batch: indices 1 and 3.
        let mut second = BTreeMap::new();
        second.insert(1u32, TxBody::new_with_vec(vec![0xA1]));
        second.insert(3u32, TxBody::new_with_vec(vec![0xA3]));
        store.inject_block_txs(point, second, None);

        let bitmap = bitmap::from_indices(&[0, 1, 2, 3]);
        let got = store.get_block_txs(7, &hash, &bitmap, PeerId(19)).unwrap();
        assert_eq!(
            got,
            vec![
                TxBody::new_with_vec(vec![0xA0]),
                TxBody::new_with_vec(vec![0xA1]),
                TxBody::new_with_vec(vec![0xA2]),
                TxBody::new_with_vec(vec![0xA3])
            ]
        );
    }

    #[test]
    fn inject_block_txs_emits_offer_only_on_first_call() {
        let (store, _rx) = LeiosStore::new(100);
        let hash = [0x02u8; 32];
        let point = Point::Specific { slot: 8, hash };

        let mut a = BTreeMap::new();

        a.insert(0u32, TxBody::new_with_vec(vec![0xB0]));
        store.inject_block_txs(point.clone(), a, None);

        let mut b = BTreeMap::new();
        b.insert(1u32, TxBody::new_with_vec(vec![0xB1]));
        store.inject_block_txs(point, b, None);

        // One BlockTxsOffer notification, not two.
        let txs_offers = store
            .notifications_after(&mut 0)
            .into_iter()
            .filter(|e| matches!(e.notification, LeiosNotification::BlockTxsOffer { .. }))
            .count();
        assert_eq!(txs_offers, 1);
    }

    #[test]
    fn inject_block_txs_does_not_overwrite_existing_index() {
        let (store, _rx) = LeiosStore::new(100);
        let hash = [0x03u8; 32];
        let point = Point::Specific { slot: 9, hash };

        let mut a = BTreeMap::new();
        a.insert(0u32, TxBody::new_with_vec(vec![0xC0]));
        store.inject_block_txs(point.clone(), a, None);

        // Conflicting body for index 0 — first writer wins.
        let mut b = BTreeMap::new();
        b.insert(0u32, TxBody::new_with_vec(vec![0xFF]));
        store.inject_block_txs(point, b, None);

        let bitmap = bitmap::from_indices(&[0]);
        let got = store.get_block_txs(9, &hash, &bitmap, PeerId(29)).unwrap();
        assert_eq!(got, vec![TxBody::new_with_vec(vec![0xC0])]);
    }

    #[test]
    fn get_block_txs_partial_block_txs_returns_none() {
        // Sparse block_txs (indices 0 and 2) can't satisfy a request for 0,1,2:
        // there is no manifest+resolver union anymore. A missing index -> None
        // (the responder disconnects) rather than serving a misaligned set.
        let h0 = [0x10u8; 32];
        let h1 = [0x20u8; 32];
        let h2 = [0x30u8; 32];
        let resolver: Arc<dyn TxBodyResolver> = Arc::new(StubResolver(HashMap::from([(
            tx_id_from_arr(h1),
            TxBody::new_with_vec(vec![0xD1]),
        )])));
        let (store, _rx) = LeiosStore::new_with_resolver(100, Some(resolver));

        let eb_hash = [0xDDu8; 32];
        let point = Point::Specific {
            slot: 11,
            hash: eb_hash,
        };
        store.record_eb_manifest(
            point.clone(),
            vec![tx_id_from_arr(h0), tx_id_from_arr(h1), tx_id_from_arr(h2)],
            None,
        );

        let mut partial = BTreeMap::new();
        partial.insert(0u32, TxBody::new_with_vec(vec![0xD0]));
        partial.insert(2u32, TxBody::new_with_vec(vec![0xD2]));
        store.inject_block_txs(point, partial, None);

        // Index 1 is not in block_txs → the whole request is unservable.
        let bitmap = bitmap::from_indices(&[0, 1, 2]);
        assert!(store.get_block_txs(11, &eb_hash, &bitmap, PeerId(37)).is_none());
    }

    #[test]
    fn get_eb_manifest_returns_recorded_hashes() {
        let (store, _rx) = LeiosStore::new(100);
        let eb_hash = [0xE1u8; 32];
        let point = Point::Specific {
            slot: 13,
            hash: eb_hash,
        };
        let manifest = vec![tx_id(0xAA), tx_id(0xBB)];
        store.record_eb_manifest(point, manifest.clone(), None);

        assert_eq!(store.get_eb_manifest(13, &eb_hash), Some(manifest));
        assert_eq!(store.get_eb_manifest(99, &eb_hash), None);
    }

    /// Test helper: a vote from voter index `voter_id`, with an
    /// announcing-RB hash derived from `rb` so distinct RBs get distinct
    /// dedup keys. (The wire vote carries no slot; the store tags
    /// retention from the tip at inject time.)
    fn vote(rb: u64, voter_id: u16) -> Vote {
        let mut announcing_rb_hash = [0u8; 32];
        announcing_rb_hash[..8].copy_from_slice(&rb.to_le_bytes());
        Vote {
            announcing_rb_hash,
            voter_id,
            vote_signature: vec![0xAB; 48],
        }
    }

    #[test]
    fn inject_votes_stores_and_dedups() {
        let (store, _rx) = LeiosStore::new(100);
        store.inject_votes(vec![vote(100, 1), vote(101, 2)], None);
        assert_eq!(store.stats().votes, 2);

        // Re-injecting the same votes is idempotent (keyed by slot/eb/voter).
        store.inject_votes(vec![vote(100, 1)], None);
        assert_eq!(store.stats().votes, 2);

        // A distinct voter at the same slot is a new entry.
        store.inject_votes(vec![vote(100, 3)], None);
        assert_eq!(store.stats().votes, 3);
    }

    #[test]
    fn inject_block_records_eb_size_and_source_on_notification() {
        // The BlockOffer the responder emits must carry the real EB
        // byte length (so the peer can pre-size its fetch buffer; the
        // dev relay rejects 0-sized offers) and the source peer (so
        // serve_leios_notify can skip echoing the offer back).
        let (store, _rx) = LeiosStore::new(100);
        let point = Point::Specific {
            slot: 5,
            hash: [0x11; 32],
        };
        let block = vec![0xAB; 1234];
        store.inject_block(point, block, Some(PeerId(7)));

        let entries = store.notifications_after(&mut 0);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sources, vec![PeerId(7)]);
        match entries[0].notification {
            LeiosNotification::BlockOffer { eb_size, .. } => assert_eq!(eb_size, 1234),
            ref other => panic!("expected BlockOffer, got {other:?}"),
        }
    }

    #[test]
    fn inject_block_dedups_by_point_across_sources() {
        // Two peers advertising the same EB must collapse into a single
        // queued notification, with both peers recorded as sources.
        let (store, _rx) = LeiosStore::new(100);
        let point = Point::Specific {
            slot: 7,
            hash: [0xAB; 32],
        };
        store.inject_block(point.clone(), vec![0xCD; 200], Some(PeerId(11)));
        store.inject_block(point.clone(), vec![0xCD; 200], Some(PeerId(22)));

        let entries = store.notifications_after(&mut 0);
        assert_eq!(
            entries.len(),
            1,
            "second advertisement must dedup against the first"
        );
        assert_eq!(entries[0].sources, vec![PeerId(11), PeerId(22)]);
        match &entries[0].notification {
            LeiosNotification::BlockOffer { eb_size, .. } => assert_eq!(*eb_size, 200),
            other => panic!("expected BlockOffer, got {other:?}"),
        }
    }

    #[test]
    fn inject_block_dedup_does_not_add_duplicate_source() {
        // A peer advertising the same EB twice must not appear twice in
        // the entry's `sources`.
        let (store, _rx) = LeiosStore::new(100);
        let point = Point::Specific {
            slot: 3,
            hash: [0x44; 32],
        };
        store.inject_block(point.clone(), vec![0xEE; 50], Some(PeerId(9)));
        store.inject_block(point.clone(), vec![0xEE; 50], Some(PeerId(9)));

        let entries = store.notifications_after(&mut 0);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sources, vec![PeerId(9)]);
    }

    #[test]
    fn inject_block_different_points_each_get_their_own_entry() {
        // Sanity — the dedup is per-point, not global.
        let (store, _rx) = LeiosStore::new(100);
        let a = Point::Specific {
            slot: 1,
            hash: [0x11; 32],
        };
        let b = Point::Specific {
            slot: 2,
            hash: [0x22; 32],
        };
        store.inject_block(a, vec![0xAA; 10], Some(PeerId(1)));
        store.inject_block(b, vec![0xBB; 10], Some(PeerId(1)));

        let entries = store.notifications_after(&mut 0);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn inject_block_dedup_does_not_merge_across_kinds() {
        // A BlockOffer for point P and a BlockTxsOffer for point P are
        // different notifications and must remain distinct entries.
        let (store, _rx) = LeiosStore::new(100);
        let point = Point::Specific {
            slot: 5,
            hash: [0x55; 32],
        };
        store.inject_block(point.clone(), vec![0xCC; 30], Some(PeerId(3)));
        // BlockTxsOffer now comes only from a path that stores bodies.
        store.inject_block_txs_full(
            point.clone(),
            vec![TxBody::new_with_vec(vec![0xDD])],
            Some(PeerId(4)),
        );

        let entries = store.notifications_after(&mut 0);
        assert_eq!(entries.len(), 2);
        assert!(matches!(
            entries[0].notification,
            LeiosNotification::BlockOffer { .. }
        ));
        assert!(matches!(
            entries[1].notification,
            LeiosNotification::BlockTxsOffer { .. }
        ));
    }

    #[test]
    fn record_eb_manifest_does_not_offer_txs() {
        // Recording a manifest means we know an EB's tx *hashes*, not that we
        // hold its tx *bodies* — so it must NOT advertise a BlockTxsOffer, or
        // we'd offer txs we can't serve (miss -> peer disconnect). Offers come
        // only from inject_block_txs*, which store the bodies.
        let (store, _rx) = LeiosStore::new(100);
        let point = Point::Specific {
            slot: 9,
            hash: [0x99; 32],
        };
        store.record_eb_manifest(
            point.clone(),
            vec![TxId::new_with_array([0x11; 32])],
            Some(PeerId(5)),
        );
        let entries = store.notifications_after(&mut 0);
        assert!(
            entries
                .iter()
                .all(|e| !matches!(e.notification, LeiosNotification::BlockTxsOffer { .. })),
            "record_eb_manifest must not emit a BlockTxsOffer"
        );
        // The manifest is still recorded for our own use (voting / resolver).
        assert_eq!(
            store.get_eb_manifest(9, &[0x99; 32]),
            Some(vec![TxId::new_with_array([0x11; 32])])
        );
    }

    #[test]
    fn inject_votes_distinct_batches_each_get_their_own_entry() {
        // Vote notifications carry payload — distinct injections produce
        // distinct entries when their votes are content-different.
        let (store, _rx) = LeiosStore::new(100);
        store.inject_votes(vec![vote(10, 1)], Some(PeerId(1)));
        store.inject_votes(vec![vote(11, 2)], Some(PeerId(1)));

        let entries = store.notifications_after(&mut 0);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn inject_votes_filters_already_seen_votes_from_notification() {
        // Re-sending the exact same vote (same key) must not produce a
        // second wire notification — the duplicate is silently
        // absorbed because the vote is already in storage.
        let (store, _rx) = LeiosStore::new(100);
        store.inject_votes(vec![vote(10, 1)], Some(PeerId(1)));
        store.inject_votes(vec![vote(10, 1)], Some(PeerId(2)));

        let entries = store.notifications_after(&mut 0);
        assert_eq!(
            entries.len(),
            1,
            "duplicate vote must not generate a second notification"
        );
    }

    #[test]
    fn inject_votes_overlapping_batch_only_propagates_new_votes() {
        // First batch [v1, v2], second batch [v2, v3]: the second
        // notification must carry only [v3], not the redundant v2.
        let (store, _rx) = LeiosStore::new(100);
        let v1 = vote(10, 1);
        let v2 = vote(10, 2);
        let v3 = vote(10, 3);
        store.inject_votes(vec![v1.clone(), v2.clone()], Some(PeerId(1)));
        store.inject_votes(vec![v2.clone(), v3.clone()], Some(PeerId(2)));

        let entries = store.notifications_after(&mut 0);
        assert_eq!(entries.len(), 2);
        match &entries[1].notification {
            LeiosNotification::Votes { votes, .. } => {
                assert_eq!(votes.len(), 1, "second batch must filter out v2");
                assert_eq!(votes[0].voter_id, v3.voter_id);
            }
            other => panic!("expected Votes, got {other:?}"),
        }
    }

    #[test]
    fn notifications_accumulate() {
        let (store, _rx) = LeiosStore::new(100);
        let hash = [0u8; 32];
        let point = Point::Specific { slot: 1, hash };

        store.inject_block(point, vec![0x01], None);
        store.inject_votes(vec![vote(10, 2)], None);

        let all = store.notifications_after(&mut 0);
        assert_eq!(all.len(), 2);
        assert!(matches!(
            all[0].notification,
            LeiosNotification::BlockOffer {
                point: Point::Specific { slot: 1, .. },
                ..
            }
        ));
        assert!(matches!(
            all[1].notification,
            LeiosNotification::Votes { .. }
        ));

        let after_first = store.notifications_after(&mut 1);
        assert_eq!(after_first.len(), 1);

        let after_all = store.notifications_after(&mut 2);
        assert!(after_all.is_empty());
    }

    #[test]
    fn slot_retention_prunes_old_data() {
        // Tight retention window so the test stays small.
        let (store, _rx) = LeiosStore::new_with_retention(1000, None, 5, 0);

        // Inject votes/blocks at slot 1, then advance the clock far past
        // the retention window. Old entries must be evicted.
        store.inject_votes(vec![vote(1, 0xAA)], None);
        store.inject_block(
            Point::Specific {
                slot: 1,
                hash: [0x11; 32],
            },
            vec![0xB0],
            None,
        );
        store.record_eb_manifest(
            Point::Specific {
                slot: 1,
                hash: [0x22; 32],
            },
            vec![tx_id(0xCC)],
            None,
        );

        // Pre-eviction sanity.
        assert_eq!(store.stats().votes, 1);
        assert!(store.get_block(1, &[0x11; 32]).is_some());

        // Inject something far in the future — past the retention cutoff.
        // max_slot becomes 100; cutoff = 100 - 5 = 95; slot=1 entries evicted.
        store.inject_block(
            Point::Specific {
                slot: 100,
                hash: [0x33; 32],
            },
            vec![0xD0],
            None,
        );

        assert_eq!(
            store.stats().votes,
            0,
            "old vote should be evicted past retention window"
        );
        assert!(
            store.get_block(1, &[0x11; 32]).is_none(),
            "old block should be evicted past retention window"
        );
        assert!(
            store
                .get_block_txs(1, &[0x22; 32], &bitmap::select_all(64), PeerId(43))
                .is_none(),
            "old eb_tx_hashes should be evicted past retention window"
        );

        // Recent entry stays.
        assert!(store.get_block(100, &[0x33; 32]).is_some());
    }

    #[test]
    fn slot_retention_prunes_notifications() {
        // Tight retention window so a few slot advances trigger eviction.
        let (store, _rx) = LeiosStore::new_with_retention(1000, None, 5, 0);

        // Three notifications at slot 1.
        store.inject_block(
            Point::Specific {
                slot: 1,
                hash: [0x11; 32],
            },
            vec![0xB0],
            None,
        );
        store.inject_block(
            Point::Specific {
                slot: 1,
                hash: [0x12; 32],
            },
            vec![0xB1],
            None,
        );
        store.inject_votes(vec![vote(1, 0xAA)], None);
        assert_eq!(store.notification_count(), 3);

        // Inject a recent block to push max_slot past the retention
        // window — cutoff = 100 - 5 = 95, all slot-1 notifications
        // are now stale.
        store.inject_block(
            Point::Specific {
                slot: 100,
                hash: [0x33; 32],
            },
            vec![0xD0],
            None,
        );

        // The slot-1 notifications were front-pruned; only the
        // slot-100 one remains.  The logical count still reflects
        // every notification ever pushed.
        assert_eq!(store.notification_count(), 4);
        let mut cursor = 0;
        let pending = store.notifications_after(&mut cursor);
        assert_eq!(pending.len(), 1);
        assert_eq!(cursor, 3, "cursor advanced past the prune frontier");
        assert!(matches!(
            pending[0].notification,
            LeiosNotification::BlockOffer {
                point: Point::Specific { slot: 100, .. },
                ..
            }
        ));
    }

    #[test]
    fn late_slot_inject_drops_notification_below_cutoff() {
        // max_slot advances to 100 first; a subsequent inject at slot 1 lands
        // below cutoff=95 and must be dropped at the source — the front-only
        // eviction loop in `evict_old` can't reach a late-slot entry at the
        // back of the deque.
        let (store, _rx) = LeiosStore::new_with_retention(1000, None, 5, 0);
        store.inject_block(
            Point::Specific {
                slot: 100,
                hash: [0xAA; 32],
            },
            vec![0xA0],
            None,
        );
        let count_before = store.notification_count();

        store.inject_block(
            Point::Specific {
                slot: 1,
                hash: [0xBB; 32],
            },
            vec![0xB0],
            None,
        );

        // Notification was filtered at the source — count unchanged.
        assert_eq!(store.notification_count(), count_before);
    }

    #[test]
    fn notifications_after_overshoot_clamps_to_next_write() {
        let (store, _rx) = LeiosStore::new(100);
        store.inject_block(
            Point::Specific {
                slot: 1,
                hash: [0u8; 32],
            },
            vec![0xA0],
            None,
        );

        // Overshoot: only 1 notification exists (next-write index 1)
        // but we ask for everything ≥ 10.
        let mut cursor = 10usize;
        let entries = store.notifications_after(&mut cursor);
        assert!(entries.is_empty());
        assert_eq!(
            cursor, 1,
            "cursor should clamp to next-write index, not echo the overshoot"
        );
    }

    #[tokio::test]
    async fn tick_slot_does_not_wake_subscribers() {
        // tick_slot advances the retention clock but adds no new notifications,
        // so it must not wake watch subscribers — those are waiting for new
        // data, and there isn't any.
        let (store, _rx) = LeiosStore::new_with_retention(1000, None, 5, 0);
        let mut sub = store.subscribe();

        store.tick_slot(100);

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), sub.changed()).await;
        assert!(
            result.is_err(),
            "tick_slot must not signal on the watch channel"
        );
    }

    #[test]
    fn tick_slot_still_evicts_old_data() {
        // Eviction must run even without a watch wake-up.
        let (store, _rx) = LeiosStore::new_with_retention(1000, None, 5, 0);
        store.inject_block(
            Point::Specific {
                slot: 1,
                hash: [0x11; 32],
            },
            vec![0xB0],
            None,
        );
        assert!(store.get_block(1, &[0x11; 32]).is_some());

        store.tick_slot(100);

        assert!(
            store.get_block(1, &[0x11; 32]).is_none(),
            "tick_slot should still evict past retention"
        );
    }

    #[test]
    fn notifications_capped_by_capacity_backstop() {
        // Flood offers all within the retention window (same slot, distinct
        // hashes) so slot-window eviction never fires. The capacity backstop
        // must still bound the deque, and absolute indexing must stay intact:
        // `notification_count` (pruned + retained) reflects every push.
        let (store, _rx) = LeiosStore::new_with_retention(MAX_NOTIFICATIONS, None, 1000, 0);
        let pushed = MAX_NOTIFICATIONS + 250;
        for i in 0..pushed {
            let mut hash = [0u8; 32];
            hash[0] = (i & 0xff) as u8;
            hash[1] = ((i >> 8) & 0xff) as u8;
            store.inject_block(Point::Specific { slot: 10, hash }, vec![0xAB], None);
        }

        let stats = store.stats();
        assert!(
            stats.notifications <= MAX_NOTIFICATIONS,
            "deque must stay within the backstop: {} > {}",
            stats.notifications,
            MAX_NOTIFICATIONS
        );
        assert_eq!(
            store.notification_count(),
            pushed,
            "pruned + retained must account for every pushed notification"
        );

        // A subscriber whose cursor fell behind the pruned front is
        // fast-forwarded rather than reading a stale local index.
        let mut cursor = 0usize;
        let entries = store.notifications_after(&mut cursor);
        assert!(
            cursor >= pushed - MAX_NOTIFICATIONS,
            "cursor fast-forwarded past pruned front"
        );
        assert_eq!(entries.len(), stats.notifications);
    }

    #[test]
    fn long_run_stays_bounded_under_sustained_load() {
        // Stress test for the eviction guarantee that motivates this PR.
        // Simulates a node receiving sustained Leios traffic for many
        // retention windows in a row: votes + EBs every slot, plus a
        // wall-clock `tick_slot` running ahead.  Both the data maps and
        // the notifications deque must stay O(retention) — not O(slots).
        const RETENTION: u64 = 100;
        const SLOTS: u64 = 10_000;
        let (store, _rx) = LeiosStore::new_with_retention(10_000, None, RETENTION, 0);

        for slot in 0..SLOTS {
            store.inject_votes(vec![vote(slot, (slot & 0xFFFF) as u16)], None);
            store.inject_block(
                Point::Specific {
                    slot,
                    hash: [0xCC; 32],
                },
                vec![0xDD; 100],
                None,
            );
            // Wall-clock leads inject slots; exercises tick_slot eviction.
            if slot % 7 == 0 {
                store.tick_slot(slot + 5);
            }
        }

        let stats = store.stats();
        let bound = (RETENTION * 2) as usize;
        assert!(
            stats.votes <= bound,
            "votes leaked: {} > {} after {} slots",
            stats.votes,
            bound,
            SLOTS
        );
        assert!(
            stats.blocks <= bound,
            "blocks leaked: {} > {} after {} slots",
            stats.blocks,
            bound,
            SLOTS
        );
        assert!(
            stats.notifications <= bound * 2,
            "notifications leaked: {} > {} after {} slots",
            stats.notifications,
            bound * 2,
            SLOTS
        );
        let byte_bound = stats.notifications * 10_000 + 4096;
        assert!(
            stats.notifications_bytes_estimate <= byte_bound,
            "notifications_bytes_estimate looks inflated: {} > {}",
            stats.notifications_bytes_estimate,
            byte_bound
        );
    }

    #[tokio::test]
    async fn subscribe_notifies_on_inject() {
        let (store, _rx) = LeiosStore::new(100);
        let mut sub = store.subscribe();

        store.inject_block(
            Point::Specific {
                slot: 1,
                hash: [0u8; 32],
            },
            vec![0x01],
            None,
        );

        let result = tokio::time::timeout(std::time::Duration::from_secs(1), sub.changed()).await;
        assert!(result.is_ok());
        assert!(*sub.borrow() > 0);
    }

    #[test]
    fn announce_block_enqueues_announcement_and_is_not_deduped() {
        let (store, _rx) = LeiosStore::new(100);
        // Opaque headers (parsed = None) suffice here: we exercise only the
        // enqueue + read path, not header parsing.
        store.announce_block(WrappedHeader::opaque(vec![0xA1, 0xA1]), None);
        // A second, distinct announcement must NOT be deduplicated — this is the
        // OCIN-equivocation mechanism (two announcements, one election).
        store.announce_block(WrappedHeader::opaque(vec![0xB2, 0xB2]), None);

        let mut idx = 0;
        let announced: Vec<Vec<u8>> = store
            .notifications_after(&mut idx)
            .iter()
            .filter_map(|e| match &e.notification {
                LeiosNotification::BlockAnnouncement { header } => Some(header.raw.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            announced,
            vec![vec![0xA1, 0xA1], vec![0xB2, 0xB2]],
            "both distinct announcements diffuse; none deduplicated"
        );
    }

    /// An EB nobody fetches is re-offered, up to the retry cap; one that IS
    /// fetched stops immediately. This is the difference between a peer that
    /// missed a single offer eventually requesting the body and never
    /// requesting it at all.
    #[test]
    fn unfetched_own_eb_is_reoffered_until_fetched_then_stops() {
        let (store, _rx) = LeiosStore::new(64);
        let hash = [7u8; 32];
        let point = Point::Specific { slot: 10, hash };
        store.inject_block(point.clone(), vec![0xAB; 128], None);

        let count_offers = |store: &LeiosStore| {
            let inner = store.inner.lock().unwrap();
            inner
                .notifications
                .iter()
                .filter(|e| matches!(&e.notification,
                    LeiosNotification::BlockOffer { point: p, .. }
                        if matches!(p, Point::Specific { slot, .. } if *slot == 10)))
                .count()
        };
        assert_eq!(count_offers(&store), 1, "the original offer");

        // Too soon: the retry interval has not elapsed.
        store.reoffer_unfetched(11);
        assert_eq!(count_offers(&store), 1, "must not retransmit early");

        // Due at first_offered_slot + 3.
        store.reoffer_unfetched(13);
        assert_eq!(count_offers(&store), 2);
        store.reoffer_unfetched(16);
        assert_eq!(count_offers(&store), 3);

        // A peer fetches it — that is the acknowledgement, so retransmits stop
        // even though the cap has not been reached.
        assert!(store.get_block(10, &hash).is_some());
        store.reoffer_unfetched(19);
        store.reoffer_unfetched(22);
        assert_eq!(count_offers(&store), 3, "fetched EB must not be re-offered");
    }

    /// Retransmits are capped, so a peer that never fetches costs a bounded
    /// number of extra notifications rather than an unbounded stream.
    #[test]
    fn reoffers_are_capped() {
        let (store, _rx) = LeiosStore::new(64);
        let point = Point::Specific { slot: 5, hash: [1u8; 32] };
        store.inject_block(point, vec![0u8; 32], None);
        for slot in 6..60 {
            store.reoffer_unfetched(slot);
        }
        let inner = store.inner.lock().unwrap();
        let offers = inner
            .notifications
            .iter()
            .filter(|e| matches!(&e.notification, LeiosNotification::BlockOffer { .. }))
            .count();
        assert_eq!(offers, 4, "original offer + at most 3 retransmits");
    }

    /// A peer's EB is not ours to re-advertise.
    #[test]
    fn peer_ebs_are_never_reoffered() {
        let (store, _rx) = LeiosStore::new(64);
        let point = Point::Specific { slot: 8, hash: [2u8; 32] };
        store.inject_block(point, vec![0u8; 32], Some(PeerId(1)));
        for slot in 9..30 {
            store.reoffer_unfetched(slot);
        }
        let inner = store.inner.lock().unwrap();
        assert!(inner.own_offers.is_empty(), "no retransmit state for a peer's EB");
        let offers = inner
            .notifications
            .iter()
            .filter(|e| matches!(&e.notification, LeiosNotification::BlockOffer { .. }))
            .count();
        assert_eq!(offers, 1, "only the relayed offer");
    }
}
