//! In-memory chain state shared between the coordinator and server-side
//! protocol handlers.
//!
//! The coordinator writes (appends blocks, performs rollbacks).
//! Server-side protocol handlers read (intersection lookups, block ranges,
//! subscribe to change notifications).
//!
//! # Maintained invariants
//!
//! - **Bounded capacity**: FIFO eviction when `blocks.len() > capacity`.
//! - **No duplicate points**: `append_block()` checks for existing point.
//! - **Thread safety**: all access through `Mutex<ChainStoreInner>`.
//! - **Change notification**: every mutation signals the watch channel.
//! - **Rollback truncation**: `rollback_to()` keeps the target point and
//!   everything before it; updates `last_rollback_target`.
//!
//! # Known gaps
//!
//! - **`block_no` is caller-provided**: not validated for monotonicity.
//!   Not decremented on rollback (high-water-mark semantics).
//! - **`get_range` fallback is intentionally imprecise**: when `from` is
//!   not on the local chain (peer on a different fork), returns the prefix
//!   `[0..=to]` so the peer can walk back via `prev_hash`. This can return
//!   more blocks than needed.
//! - **`intersection_candidates` may reference evicted blocks**: the
//!   exponential lookback pattern generates points from the stored VecDeque,
//!   so evicted blocks are not included, but the Origin fallback ensures
//!   intersection always succeeds eventually.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::types::{BlockBody, Point, Tip, WrappedHeader};

/// A block stored in the chain.
#[derive(Debug, Clone)]
pub struct StoredBlock {
    pub point: Point,
    pub header: WrappedHeader,
    pub body: BlockBody,
}

/// The block to serve a ChainSync follower next, resolved against the current
/// (possibly eviction-renumbered) store under a single lock. See
/// [`ChainStore::next_after_cursor`].
// Returned by value and matched immediately (one per ChainSync request), never
// stored or moved through collections — the large `Next` variant is fine, and
// boxing it would only add a needless allocation on the serve hot path.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum NextForCursor {
    /// Serve this block — the one immediately after the follower's cursor.
    Next(StoredBlock),
    /// Cursor is valid but no block exists past it yet — caller awaits.
    AtTip,
    /// Cursor point is no longer in the store — the follower must be rolled back.
    Gone,
}

/// How many recently rolled-back / evicted blocks to retain for BlockFetch
/// after they leave the live chain. A downstream peer that adopted one of our
/// headers via ChainSync may BlockFetch its body a moment later — if we've since
/// reorged past it, the live chain no longer holds it and answering `NoBlocks`
/// for a block we announced is a protocol violation the peer resets on. Keeping
/// the bodies briefly lets us still serve them. ~13KB/block ⇒ a few MB here.
const ORPHAN_CACHE_CAP: usize = 512;

struct ChainStoreInner {
    blocks: VecDeque<StoredBlock>,
    capacity: usize,
    /// Running block number (monotonically increasing, not reset on eviction).
    block_no: u64,
    /// Tip point after the most recent rollback truncation. Server handlers use
    /// this as the MsgRollBackward target (the true fork point).
    last_rollback_target: Option<Point>,
    /// Bodies of blocks recently removed from the live chain (rolled back or
    /// evicted), retained so BlockFetch can still serve a block we announced
    /// before a reorg. Bounded to `ORPHAN_CACHE_CAP`, newest at the back.
    orphan_bodies: VecDeque<StoredBlock>,
    /// True when this node joined the chain ABOVE genesis (sync-at-tip / from a
    /// point), so its store will only ever hold blocks starting at some N>0.
    /// Such a store must never offer or serve `Origin` to a downstream: while
    /// still empty at boot it would otherwise claim to root at genesis, hand a
    /// follower an `Origin` cursor, then mis-serve its first mid-chain block as
    /// genesis's successor — which the follower rejects with UnexpectedBlockNo,
    /// tearing the connection down. Set once at boot from the sync method.
    anchored_above_genesis: bool,
}

impl ChainStoreInner {
    /// Retain `removed` blocks (dropped from the live chain) in the orphan
    /// cache for later BlockFetch, trimming oldest entries past the cap.
    fn retain_orphans(&mut self, removed: impl IntoIterator<Item = StoredBlock>) {
        for b in removed {
            self.orphan_bodies.push_back(b);
        }
        while self.orphan_bodies.len() > ORPHAN_CACHE_CAP {
            self.orphan_bodies.pop_front();
        }
    }
}

/// Thread-safe in-memory chain state.
///
/// All methods that read or mutate the chain acquire a `Mutex` lock.
/// Operations under the lock are fast (no I/O), so contention is minimal.
/// Change notifications are delivered via a `watch::channel`.
pub struct ChainStore {
    inner: Mutex<ChainStoreInner>,
    notify: watch::Sender<u64>,
}

impl ChainStore {
    /// Create a new chain store with the given block capacity.
    ///
    /// Returns the store (wrapped in `Arc`) and a subscription receiver
    /// for change notifications.
    pub fn new(capacity: usize) -> (Arc<Self>, watch::Receiver<u64>) {
        let (notify_sender, notify_receiver) = watch::channel(0u64);
        let store = Arc::new(Self {
            inner: Mutex::new(ChainStoreInner {
                blocks: VecDeque::new(),
                capacity,
                block_no: 0,
                last_rollback_target: None,
                orphan_bodies: VecDeque::new(),
                anchored_above_genesis: false,
            }),
            notify: notify_sender,
        });
        (store, notify_receiver)
    }

    /// Record that this node joined the chain above genesis (sync-at-tip / from
    /// a point). A store so marked never offers or serves `Origin` to a
    /// downstream follower — see the `anchored_above_genesis` field docs.
    pub fn set_anchored_above_genesis(&self, anchored: bool) {
        let count = {
            let mut inner = self.inner.lock().unwrap();
            inner.anchored_above_genesis = anchored;
            inner.blocks.len() as u64
        };
        // Flipping this flag can change `is_seeded()` (e.g. false->true when a
        // node turns out to be genesis-rooted, `anchored = false`). A parked
        // ChainSync server only re-checks `is_seeded()` on a watch notification,
        // so wake it here — otherwise an already-parked `MsgFindIntersect` would
        // wait forever.
        let _ = self.notify.send(count);
    }

    /// Whether this store holds a servable chain yet. A node that joined above
    /// genesis (sync-at-tip) intersects at the network tip immediately but does
    /// not hold its first block until it has fetched + validated it — a window
    /// of seconds at boot. During that window its `tip()` is `Origin`/genesis,
    /// and answering a downstream's ChainSync intersection then advertises
    /// tip=Genesis, which a downstream at a real tip rejects as `ForkTooDeep`
    /// and cold-backs-off for minutes (orphaning every block we forge meanwhile
    /// — see `serve_chainsync`). So an anchored store is "seeded" only once it
    /// holds at least one block. A genesis-rooted node (`anchored = false`) is
    /// legitimately at Origin from the start and is always seeded.
    pub fn is_seeded(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        !inner.anchored_above_genesis || !inner.blocks.is_empty()
    }

    /// Append a block to the chain. Evicts the oldest block if over capacity.
    /// `block_no` is the caller-provided chain height (not an internal counter).
    /// Returns `false` if the point is already stored (no-op).
    pub fn append_block(
        &self,
        point: Point,
        header: WrappedHeader,
        body: BlockBody,
        block_no: u64,
    ) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.blocks.iter().any(|b| b.point == point) {
            return false;
        }
        inner.block_no = block_no;
        inner.blocks.push_back(StoredBlock {
            point,
            header,
            body,
        });
        while inner.blocks.len() > inner.capacity {
            if let Some(evicted) = inner.blocks.pop_front() {
                inner.retain_orphans([evicted]);
            }
        }
        let count = inner.block_no;
        drop(inner);
        let _ = self.notify.send(count);
        true
    }

    /// Roll back the chain to the given point. Removes all blocks after it.
    /// If the point is Origin, clears all blocks.
    /// Returns the new tip point.
    pub fn rollback_to(&self, point: &Point) -> Point {
        let mut inner = self.inner.lock().unwrap();
        if *point == Point::Origin {
            let removed: Vec<StoredBlock> = inner.blocks.drain(..).collect();
            inner.retain_orphans(removed);
            inner.last_rollback_target = Some(Point::Origin);
            drop(inner);
            let _ = self.notify.send(0);
            return Point::Origin;
        }
        // Find the position of the target point and truncate after it, retaining
        // the removed suffix (blocks we may have already announced) for BlockFetch.
        if let Some(pos) = inner.blocks.iter().position(|b| b.point == *point) {
            let removed: Vec<StoredBlock> = inner.blocks.drain(pos + 1..).collect();
            inner.retain_orphans(removed);
        }
        let tip_point = inner
            .blocks
            .back()
            .map(|b| b.point.clone())
            .unwrap_or(Point::Origin);
        inner.last_rollback_target = Some(tip_point.clone());
        let count = inner.block_no;
        drop(inner);
        let _ = self.notify.send(count);
        tip_point
    }

    /// Roll back the chain by `depth` blocks. Returns the new tip point.
    pub fn rollback(&self, depth: usize) -> Point {
        let mut inner = self.inner.lock().unwrap();
        let new_len = inner.blocks.len().saturating_sub(depth);
        let removed: Vec<StoredBlock> = inner.blocks.drain(new_len..).collect();
        inner.retain_orphans(removed);
        let tip_point = inner
            .blocks
            .back()
            .map(|b| b.point.clone())
            .unwrap_or(Point::Origin);
        inner.last_rollback_target = Some(tip_point.clone());
        let count = inner.block_no;
        drop(inner);
        let _ = self.notify.send(count);
        tip_point
    }

    /// Get the current chain tip.
    pub fn tip(&self) -> Tip {
        let inner = self.inner.lock().unwrap();
        match inner.blocks.back() {
            Some(block) => Tip {
                point: block.point.clone(),
                block_no: inner.block_no,
            },
            None => Tip {
                point: Point::Origin,
                block_no: 0,
            },
        }
    }

    /// Find the best intersection between the given points and the chain.
    /// Points are checked in order; first match wins (so callers should
    /// order from most recent to oldest).
    ///
    /// Returns `Some((Origin, tip))` if `Origin` appears in `points` and no
    /// earlier candidate matched — this means the only common point is
    /// genesis. In the praos consensus layer, an Origin intersection maps
    /// to `anchor=None`, which currently only allows a switch when
    /// `adopted_tip_hash` is `None` (fresh node).
    pub fn find_intersection(&self, points: &[Point]) -> Option<(Point, Tip)> {
        let inner = self.inner.lock().unwrap();
        let tip = match inner.blocks.back() {
            Some(block) => Tip {
                point: block.point.clone(),
                block_no: inner.block_no,
            },
            None => Tip {
                point: Point::Origin,
                block_no: 0,
            },
        };

        // Whether our chain actually roots at genesis, i.e. our earliest stored
        // block is the genesis child (block 0 / prev = genesis). A node that
        // joined mid-chain and never adopted block 0 (or evicted it) is anchored
        // above genesis: offering Origin as an intersection would then roll our
        // first block (e.g. block 2) forward where the client expects block 0,
        // which the client rejects with UnexpectedBlockNo. In that case we do NOT
        // claim Origin — we answer IntersectNotFound (the client syncs elsewhere).
        // Contiguous chain of `len` blocks ending at `block_no` has its earliest
        // block at `block_no - (len - 1)`; it roots at genesis iff that is 0, i.e.
        // `block_no + 1 == len`. (block_no-based, not header parsing, so it holds
        // for opaque headers too.) Empty chain: nothing to mis-serve.
        // A store that joined above genesis never roots at genesis, even while
        // still empty at boot — offering Origin then would hand a follower a
        // cursor we later mis-serve (see `anchored_above_genesis`).
        let len = inner.blocks.len() as u64;
        let roots_at_genesis =
            !inner.anchored_above_genesis && (len == 0 || inner.block_no + 1 == len);
        for candidate in points {
            if *candidate == Point::Origin {
                if roots_at_genesis {
                    return Some((Point::Origin, tip));
                }
                continue; // anchored above genesis — Origin is not a valid intersection
            }
            if inner.blocks.iter().any(|b| b.point == *candidate) {
                return Some((candidate.clone(), tip));
            }
        }
        None
    }

    /// Get the index of a point in the chain.
    /// Returns `None` for Origin (before the first block).
    pub fn index_of(&self, point: &Point) -> Option<usize> {
        let inner = self.inner.lock().unwrap();
        if *point == Point::Origin {
            return None;
        }
        inner.blocks.iter().position(|b| b.point == *point)
    }

    /// Resolve a ChainSync follower's read cursor (the last point we served it)
    /// and return the block to serve next — **atomically, under one lock**.
    ///
    /// A follower's position must be tracked by *point*, not a cached absolute
    /// index: capacity eviction `pop_front`s the oldest blocks and renumbers
    /// the rest, so a cached index goes stale even though the cursor's block is
    /// still present. Resolving by point keeps the cursor valid across eviction
    /// instead of mistaking the renumber for a rollback.
    ///
    /// Resolving the cursor and fetching the successor in a *single* lock is
    /// load-bearing: doing them as two calls (resolve → index, then fetch by
    /// index) lets a concurrent `append_block` eviction shift indices between
    /// them, skipping or duplicating a header.
    pub fn next_after_cursor(&self, read_point: &Option<Point>) -> NextForCursor {
        let inner = self.inner.lock().unwrap();
        // Index of the cursor point (None ⇒ Origin ⇒ before the first block);
        // the block to serve is the one immediately after it.
        let after = match read_point {
            None => 0,
            Some(p) if *p == Point::Origin => 0,
            Some(p) => match inner.blocks.iter().position(|b| b.point == *p) {
                // Still present (possibly renumbered by eviction).
                Some(i) => i + 1,
                // Genuinely gone: real rollback/truncation, or the follower fell
                // so far behind its cursor was evicted past the window.
                None => return NextForCursor::Gone,
            },
        };
        // Serving the front block (`after == 0`) means the follower is at
        // Origin. A node that joined above genesis (sync-at-tip) must never do
        // this: its front block is some N>0, and handing it to an Origin
        // follower is rejected with UnexpectedBlockNo. A follower can hold an
        // Origin cursor from when our store was still empty at boot, so guard
        // the serve too (find_intersection stops OFFERING Origin) and await
        // instead — the follower re-intersects at a real point once we hold one
        // it shares. (A genesis-rooted node that merely evicted its early blocks
        // is NOT anchored and keeps serving from the front, as before.)
        if after == 0 && inner.anchored_above_genesis {
            return NextForCursor::AtTip;
        }
        match inner.blocks.get(after) {
            Some(b) => NextForCursor::Next(b.clone()),
            None => NextForCursor::AtTip,
        }
    }

    /// A rollback target that is guaranteed servable: the last recorded
    /// rollback target if it is Origin or still present in the store, else
    /// `Origin`. Capacity eviction never updates `last_rollback_target`, so it
    /// can point at a since-evicted block; rolling a follower back to an
    /// unservable point would re-resolve to `Gone` forever. Origin always
    /// resolves (serve from the front), so it is the safe fallback.
    pub fn servable_rollback_target(&self) -> Point {
        let inner = self.inner.lock().unwrap();
        match &inner.last_rollback_target {
            Some(p) if *p == Point::Origin => Point::Origin,
            Some(p) if inner.blocks.iter().any(|b| b.point == *p) => p.clone(),
            _ => Point::Origin,
        }
    }

    /// Get blocks after the given index (exclusive).
    /// `None` means Origin — returns all blocks from the beginning.
    pub fn blocks_after(&self, after_index: Option<usize>) -> Vec<StoredBlock> {
        let inner = self.inner.lock().unwrap();
        let start = match after_index {
            Some(i) => i + 1,
            None => 0,
        };
        if start >= inner.blocks.len() {
            return Vec::new();
        }
        inner.blocks.range(start..).cloned().collect()
    }

    /// Get blocks in a range (inclusive on both endpoints).
    pub fn get_range(&self, from: &Point, to: &Point) -> Vec<StoredBlock> {
        let inner = self.inner.lock().unwrap();
        // BlockFetch must serve exactly the requested tip, never a substitute.
        // If `to` is not on the live chain (we reorged past it, or it was
        // evicted), return empty so the caller serves the retained body via
        // `get_orphans` instead. The old behaviour — streaming up to the
        // current tip — hands the peer a block whose hash doesn't match the
        // header it asked for, which it treats as a violation and resets on.
        let end = match inner.blocks.iter().position(|b| b.point == *to) {
            Some(e) => e,
            None => return Vec::new(),
        };
        // If `from` is on this chain, slice from there. Otherwise return the
        // whole prefix up to `end` — the client may be on a fork whose `from`
        // we don't know, and giving it the chain prefix lets it walk back
        // through prev_hash to find a common ancestor.
        let start = inner
            .blocks
            .iter()
            .position(|b| b.point == *from)
            .filter(|&s| s <= end)
            .unwrap_or(0);
        inner.blocks.range(start..=end).cloned().collect()
    }

    /// BlockFetch fallback: serve a block that has left the live chain (rolled
    /// back or evicted) but is still in the orphan cache. This exists for the
    /// single-block reorged-past fetch (`from == to`) — honouring a BlockFetch
    /// for a header we announced via ChainSync and then reorged past, so we do
    /// not answer `NoBlocks` (which the downstream treats as a protocol
    /// violation and resets on).
    ///
    /// BlockFetch is all-or-nothing: it must serve the exact requested sequence
    /// or nothing. For a multi-block range (`from != to`) we return empty rather
    /// than the endpoint blocks that happen to be cached — a partial/misaligned
    /// range hands the peer a body whose hash doesn't match what it asked for.
    /// (A future contiguous-range orphan fetch would need prev_hash verification
    /// across the cache; the cache's purpose today is the single-block case.)
    pub fn get_orphans(&self, from: &Point, to: &Point) -> Vec<StoredBlock> {
        if from != to {
            return Vec::new();
        }
        let inner = self.inner.lock().unwrap();
        inner
            .orphan_bodies
            .iter()
            .filter(|b| b.point == *to)
            .cloned()
            .collect()
    }

    /// Produce ChainSync intersection candidates from the local chain,
    /// ordered newest-to-oldest: `[tip, tip-1, tip-2, tip-4, tip-8, ...]`
    /// with exponential lookback, capped at `max` points, and always ending
    /// with `Point::Origin` as the ultimate fallback.
    ///
    /// `find_intersection` returns the first candidate that matches, so
    /// newest-first ordering selects the most recent common ancestor — the
    /// shortest possible re-sync window.
    pub fn intersection_candidates(&self, max: usize) -> Vec<Point> {
        let inner = self.inner.lock().unwrap();
        let len = inner.blocks.len();
        let mut out: Vec<Point> = Vec::with_capacity(max.min(len) + 1);
        if len > 0 {
            // Tip (newest) is at `len - 1`. Walk back by 1, 2, 4, 8, ...
            // until we run out of chain or hit `max`.
            let mut offset: usize = 0;
            let mut step: usize = 1;
            while offset < len && out.len() + 1 < max {
                let idx = len - 1 - offset;
                out.push(inner.blocks[idx].point.clone());
                offset = offset.saturating_add(step);
                step = step.saturating_mul(2);
            }
        }
        out.push(Point::Origin);
        out
    }

    /// Check whether a read cursor is still valid by comparing the Point at
    /// the stored index. Returns false if the index is out of bounds OR a
    /// different block now occupies that position (rollback + re-append).
    pub fn is_valid_index(&self, index: Option<usize>, cursor_point: &Option<Point>) -> bool {
        let inner = self.inner.lock().unwrap();
        match (index, cursor_point) {
            (None, _) => true, // Origin is always valid
            (Some(i), Some(p)) => inner.blocks.get(i).is_some_and(|b| b.point == *p),
            (Some(i), None) => i < inner.blocks.len(),
        }
    }

    /// The fork point of the most recent rollback (tip after truncation).
    /// Server handlers use this as the MsgRollBackward target.
    pub fn last_rollback_target(&self) -> Option<Point> {
        self.inner.lock().unwrap().last_rollback_target.clone()
    }

    /// Subscribe to chain change notifications.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.notify.subscribe()
    }

    /// How many peers are currently running a ChainSync **server** session
    /// against us — i.e. how many peers could actually receive a block we
    /// forge. `serve_chain_sync` holds one subscription per served peer, so
    /// this is that count.
    ///
    /// Block diffusion is pull-based: a block reaches the network only because
    /// some peer is following our chain. With no consumer, forging produces a
    /// block nobody can fetch — it is orphaned, and anything we build on it
    /// goes with it.
    pub fn chain_sync_consumers(&self) -> usize {
        self.notify.receiver_count()
    }

    /// Get the total number of blocks that have been appended (including evicted).
    pub fn block_count(&self) -> u64 {
        self.inner.lock().unwrap().block_no
    }

    /// Get the number of blocks currently stored.
    pub fn stored_count(&self) -> usize {
        self.inner.lock().unwrap().blocks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_point(slot: u64) -> Point {
        Point::Specific {
            slot,
            hash: [slot as u8; 32],
        }
    }

    fn make_block(slot: u64) -> (Point, WrappedHeader, BlockBody) {
        let point = make_point(slot);
        let header = WrappedHeader::opaque(vec![0xA0]);
        let body = BlockBody::opaque(vec![0xBF, 0xFF]);
        (point, header, body)
    }

    #[test]
    fn empty_chain_tip_is_origin() {
        let (store, _rx) = ChainStore::new(100);
        let tip = store.tip();
        assert_eq!(tip.point, Point::Origin);
        assert_eq!(tip.block_no, 0);
    }

    #[test]
    fn append_advances_tip() {
        let (store, _rx) = ChainStore::new(100);
        let (p1, h1, b1) = make_block(1);
        store.append_block(p1.clone(), h1, b1, 1);

        let tip = store.tip();
        assert_eq!(tip.point, p1);
        assert_eq!(tip.block_no, 1);

        let (p2, h2, b2) = make_block(2);
        store.append_block(p2.clone(), h2, b2, 2);
        let tip = store.tip();
        assert_eq!(tip.point, p2);
        assert_eq!(tip.block_no, 2);
    }

    #[test]
    fn rolled_back_block_still_servable_from_orphan_cache() {
        // Announce-then-reorg: a block leaves the live chain via rollback but a
        // downstream that saw its header must still be able to BlockFetch it,
        // else we'd answer NoBlocks (a protocol violation the peer resets on).
        let (store, _rx) = ChainStore::new(100);
        let (p1, h1, b1) = make_block(1);
        let (p2, h2, b2) = make_block(2);
        store.append_block(p1.clone(), h1, b1, 1);
        store.append_block(p2.clone(), h2, b2.clone(), 2);

        // Reorg past block 2.
        store.rollback_to(&p1);
        assert!(store.get_range(&p2, &p2).is_empty(), "live chain dropped it");

        // But the orphan cache still serves its body (single-block request).
        let served = store.get_orphans(&p2, &p2);
        assert_eq!(served.len(), 1);
        assert_eq!(served[0].point, p2);
        assert_eq!(served[0].body.raw, b2.raw);
    }

    #[test]
    fn evicted_block_retained_in_orphan_cache() {
        // Capacity-eviction also feeds the orphan cache (helps a slow follower).
        let (store, _rx) = ChainStore::new(2);
        let (p1, h1, b1) = make_block(1);
        let (p2, h2, b2) = make_block(2);
        let (p3, h3, b3) = make_block(3);
        store.append_block(p1.clone(), h1, b1, 1);
        store.append_block(p2, h2, b2, 2);
        store.append_block(p3, h3, b3, 3); // evicts p1

        assert!(store.get_range(&p1, &p1).is_empty(), "p1 evicted from live");
        assert_eq!(store.get_orphans(&p1, &p1).len(), 1, "p1 retained for fetch");
    }

    #[test]
    fn intersection_candidates_empty_chain() {
        let (store, _rx) = ChainStore::new(100);
        let candidates = store.intersection_candidates(10);
        assert_eq!(candidates, vec![Point::Origin]);
    }

    #[test]
    fn intersection_candidates_exponential_lookback() {
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=20 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }

        // max=10 → up to 9 chain points + Origin at the end.
        // Offsets from tip: 0, 1, 3, 7, 15 (next would be 31 > 19, stop)
        // → slots: 20, 19, 17, 13, 5, then Origin.
        let candidates = store.intersection_candidates(10);
        assert_eq!(
            candidates,
            vec![
                make_point(20),
                make_point(19),
                make_point(17),
                make_point(13),
                make_point(5),
                Point::Origin,
            ]
        );
    }

    #[test]
    fn intersection_candidates_caps_at_max() {
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=50 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }
        let candidates = store.intersection_candidates(4);
        // 3 chain points + Origin = 4 total.
        assert_eq!(candidates.len(), 4);
        assert_eq!(candidates[0], make_point(50));
        assert_eq!(*candidates.last().unwrap(), Point::Origin);
    }

    #[test]
    fn append_deduplicates_by_point() {
        let (store, _rx) = ChainStore::new(100);
        let (p1, h1, b1) = make_block(1);
        assert!(store.append_block(p1.clone(), h1.clone(), b1.clone(), 1));

        // Same point again — should be a no-op.
        assert!(!store.append_block(p1, h1, b1, 1));

        let tip = store.tip();
        assert_eq!(tip.block_no, 1);
        assert_eq!(store.stored_count(), 1);
    }

    #[test]
    fn capacity_eviction() {
        let (store, _rx) = ChainStore::new(3);
        for slot in 1..=5 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }
        // Should have blocks 3, 4, 5 (evicted 1 and 2).
        assert_eq!(store.stored_count(), 3);
        assert_eq!(store.block_count(), 5);

        // Block 1 is gone.
        assert!(store.index_of(&make_point(1)).is_none());
        // Block 3 is first.
        assert_eq!(store.index_of(&make_point(3)), Some(0));
    }

    #[test]
    fn next_after_cursor_survives_eviction_renumber() {
        // Regression (ChainSync server / us-as-relay): capacity eviction
        // renumbers indices but a still-present cursor must stay valid (and
        // resolve+fetch atomically), not be mistaken for a rollback (which
        // spuriously sent followers to Origin).
        let next_point = |n: NextForCursor| match n {
            NextForCursor::Next(b) => Some(b.point),
            _ => None,
        };
        let (store, _rx) = ChainStore::new(3);
        for slot in 1..=3 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }
        // Follower caught up to block 2 → next block to serve is block 3.
        let cursor = Some(make_point(2));
        assert_eq!(
            next_point(store.next_after_cursor(&cursor)),
            Some(make_point(3))
        );

        // Append block 4: evicts block 1, so block 2 shifts index 1 -> 0.
        let (p, h, b) = make_block(4);
        store.append_block(p, h, b, 4);
        assert!(store.index_of(&make_point(1)).is_none(), "block 1 evicted");
        // Cursor (block 2) is still present (renumbered) — still resolves to its
        // successor, block 3. This is the case the old stale-index path got wrong.
        assert_eq!(
            next_point(store.next_after_cursor(&cursor)),
            Some(make_point(3))
        );

        // Origin cursor serves from the front (now block 2 after eviction).
        assert_eq!(
            next_point(store.next_after_cursor(&None)),
            Some(make_point(2))
        );
        // Cursor at the tip (block 4) → nothing after → AtTip.
        assert!(matches!(
            store.next_after_cursor(&Some(make_point(4))),
            NextForCursor::AtTip
        ));

        // Append two more (5, 6): evicts blocks 2 and 3. The cursor's block 2 is
        // now genuinely gone -> Gone -> caller rolls the follower back.
        for slot in 5..=6 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }
        assert!(matches!(
            store.next_after_cursor(&cursor),
            NextForCursor::Gone
        ));
    }

    #[test]
    fn servable_rollback_target_falls_back_to_origin_when_evicted() {
        // Copilot #2/#3: a Gone-on-eviction must not roll the follower back to a
        // since-evicted rollback target (that loops Gone forever). The target is
        // returned only if still present; otherwise Origin (always servable).
        let (store, _rx) = ChainStore::new(3);
        for slot in 1..=3 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }
        // No genuine rollback recorded → Origin.
        assert_eq!(store.servable_rollback_target(), Point::Origin);

        // A real rollback to block 2 records it as the target; it's present.
        store.rollback_to(&make_point(2));
        assert_eq!(store.servable_rollback_target(), make_point(2));

        // Re-grow past capacity until block 2 is evicted; the stale target must
        // now fall back to Origin rather than an unservable point.
        for slot in 3..=6 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }
        assert!(store.index_of(&make_point(2)).is_none(), "block 2 evicted");
        assert_eq!(store.servable_rollback_target(), Point::Origin);
    }

    #[test]
    fn rollback_to_point() {
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=5 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }

        let new_tip = store.rollback_to(&make_point(3));
        assert_eq!(new_tip, make_point(3));
        assert_eq!(store.stored_count(), 3);

        // Blocks 4 and 5 are gone.
        assert!(store.index_of(&make_point(4)).is_none());
        assert!(store.index_of(&make_point(5)).is_none());
    }

    #[test]
    fn rollback_to_origin() {
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=3 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }

        let new_tip = store.rollback_to(&Point::Origin);
        assert_eq!(new_tip, Point::Origin);
        assert_eq!(store.stored_count(), 0);
    }

    #[test]
    fn rollback_by_depth() {
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=5 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }

        let new_tip = store.rollback(2);
        assert_eq!(new_tip, make_point(3));
        assert_eq!(store.stored_count(), 3);
    }

    #[test]
    fn find_intersection_matches_first() {
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=5 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }

        // Should find point 4 first (it's listed before point 2).
        let result = store.find_intersection(&[make_point(4), make_point(2)]);
        assert!(result.is_some());
        let (found, tip) = result.unwrap();
        assert_eq!(found, make_point(4));
        assert_eq!(tip.block_no, 5);
    }

    #[test]
    fn find_intersection_origin_fallback() {
        let (store, _rx) = ChainStore::new(100);
        // Genesis-rooted chain (block_no 0,1,2): Origin IS a valid intersection.
        for slot in 1..=3 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot - 1);
        }

        let result = store.find_intersection(&[make_point(99), Point::Origin]);
        assert!(result.is_some());
        let (found, _) = result.unwrap();
        assert_eq!(found, Point::Origin);
    }

    #[test]
    fn find_intersection_no_origin_when_anchored_above_genesis() {
        let (store, _rx) = ChainStore::new(100);
        // Chain anchored at block_no 2 (joined mid-chain, never adopted block 0).
        // Origin is NOT a valid intersection: serving from it would roll block 2
        // forward where the client expects block 0 (UnexpectedBlockNo).
        for slot in 1..=3 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot + 1); // block_no 2,3,4
        }
        assert_eq!(
            store.find_intersection(&[make_point(99), Point::Origin]),
            None,
            "anchored-above-genesis chain must not claim Origin"
        );
    }

    #[test]
    fn anchored_store_never_offers_or_serves_origin_when_empty() {
        // Regression (boot relay race / UnexpectedBlockNo): a sync-at-tip node's
        // store is EMPTY when a downstream first intersects. Without the anchored
        // flag it would claim Origin (empty ⇒ "roots at genesis"), hand the
        // follower an Origin cursor, then — once it injects its first mid-chain
        // block N>0 — serve that as genesis's successor, which the follower
        // rejects with UnexpectedBlockNo and resets the connection.
        let (store, _rx) = ChainStore::new(100);
        store.set_anchored_above_genesis(true);

        // Empty anchored store: Origin is NOT offered as an intersection.
        assert_eq!(
            store.find_intersection(&[make_point(50), Point::Origin]),
            None,
            "empty anchored store must not offer Origin"
        );
        // And an Origin/None cursor is never served the front block.
        assert!(matches!(
            store.next_after_cursor(&None),
            NextForCursor::AtTip
        ));

        // After injecting a mid-chain block (N>0), still no Origin mis-serve —
        // the follower must re-intersect at a real point it shares with us.
        let (p, h, b) = make_block(356);
        store.append_block(p, h, b, 356);
        assert!(
            matches!(store.next_after_cursor(&None), NextForCursor::AtTip),
            "anchored store must not serve its first mid-chain block to an Origin cursor"
        );
        assert_eq!(
            store.find_intersection(&[Point::Origin]),
            None,
            "anchored store never claims Origin even once populated mid-chain"
        );
        // A follower that intersects at the real block WE hold is served forward.
        assert_eq!(
            store.find_intersection(&[make_point(356)]).map(|(p, _)| p),
            Some(make_point(356))
        );
    }

    #[test]
    fn find_intersection_no_match() {
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=3 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }

        let result = store.find_intersection(&[make_point(99), make_point(100)]);
        assert!(result.is_none());
    }

    #[test]
    fn blocks_after_index() {
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=5 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }

        // After index 2 (block at slot 3) → blocks at slots 4, 5.
        let after = store.blocks_after(Some(2));
        assert_eq!(after.len(), 2);
        assert_eq!(after[0].point, make_point(4));
        assert_eq!(after[1].point, make_point(5));

        // After None (Origin) → all blocks.
        let all = store.blocks_after(None);
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn get_range_inclusive() {
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=5 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }

        let range = store.get_range(&make_point(2), &make_point(4));
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].point, make_point(2));
        assert_eq!(range[2].point, make_point(4));
    }

    #[test]
    fn get_range_unknown_to_returns_empty() {
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=3 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }

        // `to` is not on the live chain: BlockFetch must not substitute a
        // different tip. Return empty here — the caller serves the requested
        // body from the orphan cache (get_orphans) if it was reorged past.
        let range = store.get_range(&make_point(99), &make_point(100));
        assert!(range.is_empty());
    }

    #[test]
    fn get_range_empty_store_returns_empty() {
        let (store, _rx) = ChainStore::new(100);
        let range = store.get_range(&make_point(1), &make_point(2));
        assert!(range.is_empty());
    }

    /// When `to` is in the store but `from` is not (because `from` is on a
    /// fork the server doesn't know about, or was rolled back), the server
    /// should still return what it has up to `to` so the client can use that
    /// to walk further back. Otherwise the client gets MsgNoBlocks and stays
    /// stuck on a fork it can't bridge.
    #[test]
    fn get_range_returns_prefix_when_from_unknown() {
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=5 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }

        // from is not in the store, to is.
        let range = store.get_range(&make_point(99), &make_point(4));
        assert!(
            !range.is_empty(),
            "should return a prefix of the chain up to `to`"
        );
        assert_eq!(range.last().unwrap().point, make_point(4));
    }

    #[test]
    fn is_valid_index_after_rollback() {
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=5 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }

        let point5 = Some(make_point(5));
        let point3 = Some(make_point(3));
        assert!(store.is_valid_index(Some(4), &point5)); // index 4 = block 5
        store.rollback(2); // remove blocks 4, 5
        assert!(!store.is_valid_index(Some(4), &point5)); // out of bounds
        assert!(store.is_valid_index(Some(2), &point3)); // block 3 still there
        assert!(store.is_valid_index(None, &None)); // Origin always valid

        // Verify last_rollback_target is block 3 (tip after truncation).
        assert_eq!(store.last_rollback_target(), Some(make_point(3)));
    }

    #[test]
    fn rollback_reappend_detected_by_point_matching() {
        // The key bug: rollback + re-append at the same index must be detected.
        let (store, _rx) = ChainStore::new(100);
        for slot in 1..=5 {
            let (p, h, b) = make_block(slot);
            store.append_block(p, h, b, slot);
        }

        // Cursor at index 4 = block at slot 5.
        let old_point = Some(make_point(5));
        assert!(store.is_valid_index(Some(4), &old_point));

        // Rollback removes block 5, then a different block occupies index 4.
        store.rollback(1); // remove block 5, now [1,2,3,4]
        let (p, h, b) = make_block(50); // different block at slot 50
        store.append_block(p, h, b, 50); // now [1,2,3,4,block_50]

        // Same index 4, but different block — must detect as invalid.
        assert!(!store.is_valid_index(Some(4), &old_point));

        // Rollback target is block 4 (tip after truncation).
        assert_eq!(store.last_rollback_target(), Some(make_point(4)));
    }

    #[tokio::test]
    async fn subscribe_notifies_on_change() {
        let (store, _rx) = ChainStore::new(100);
        let mut sub = store.subscribe();

        let (p, h, b) = make_block(1);
        store.append_block(p, h, b, 1);

        // Should wake up.
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), sub.changed()).await;
        assert!(result.is_ok());
        assert_eq!(*sub.borrow(), 1);
    }
}
