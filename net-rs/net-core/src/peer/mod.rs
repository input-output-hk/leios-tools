//! Per-peer protocol handling.
//!
//! Manages individual peer connections: connection setup, protocol
//! sub-tasks (initiator, responder, duplex), and server-side handlers.
//! The multi-peer coordination layer lives in the `multi_peer` module.

pub(crate) mod command_dispatch;
pub mod connect;
pub(crate) mod duplex_task;
pub(crate) mod peer_task;
pub mod server_handlers;
pub mod types;

pub use shared_consensus::PeerId;
pub use types::{PeerCommand, PeerEvent};

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

/// How far a downstream peer has promoted *us* as its upstream, observed from
/// our responder side: `Cold` (connected only) → `Warm` (it's sending us
/// keepalives) → `Hot` (it's pulling our chain via ChainSync/BlockFetch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownstreamState {
    Cold,
    Warm,
    Hot,
}

impl DownstreamState {
    pub fn from_u8(v: u8) -> Self {
        // The flag only ever climbs (writers use `fetch_max`), so clamp any
        // unexpected high value (a future state added without updating this
        // match, say) to the highest known state rather than dropping it to
        // Cold — reporting a regression would be more misleading than saturating.
        match v {
            0 => Self::Cold,
            1 => Self::Warm,
            _ => Self::Hot,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Warm => "warm",
            Self::Hot => "hot",
        }
    }
}

/// Shared per-connection downstream-promotion flag (0=cold, 1=warm, 2=hot).
/// Server handlers escalate it with `fetch_max` (monotonic within a
/// connection); a fresh connection allocates a new flag, so reconnects reset to
/// cold. The coordinator reads it when building a peer snapshot — same pattern
/// as the shared `MuxStats` byte counters.
pub type DownstreamFlag = Arc<AtomicU8>;

pub fn new_downstream_flag() -> DownstreamFlag {
    Arc::new(AtomicU8::new(0))
}
/// Escalate to at least Warm (downstream started keepalive).
pub fn mark_downstream_warm(f: &DownstreamFlag) {
    f.fetch_max(1, Ordering::Relaxed);
}
/// Escalate to Hot (downstream is pulling our chain).
pub fn mark_downstream_hot(f: &DownstreamFlag) {
    f.fetch_max(2, Ordering::Relaxed);
}

/// RAII guard that aborts a set of tokio tasks when it is dropped.
///
/// A per-peer task spawns ~11 protocol sub-tasks and, on the normal
/// teardown path, explicitly `abort()`s each one before returning. But if
/// the per-peer task is itself cancelled while suspended in its `select!`
/// loop — the coordinator `abort()`s its `JoinHandle` on a racing `Failed`
/// event or a full command channel, the dominant paths under connection
/// churn — that explicit cleanup never runs. A dropped `JoinHandle` only
/// *detaches* its task (tokio does not abort on drop), so the sub-tasks
/// keep running as orphans.
///
/// [`crate::mux::RunningMux`]'s own `Drop` closes the socket, which reaps
/// any sub-task blocked on the bearer (recv or egress). But a sub-task
/// suspended on `event_sender.send().await` to a *full coordinator
/// channel* — exactly the backpressure that connection churn produces — is
/// unblocked by neither the socket close nor the detached handle. Without
/// this guard it would linger forever, leaking one protocol sub-task (and
/// the decoded message bodies it holds) per churned connection.
///
/// Registering every sub-task's [`tokio::task::AbortHandle`] here at spawn
/// time makes teardown cancellation-safe on *every* exit path: the guard
/// is dropped when the per-peer task's stack unwinds, normal or aborted,
/// and aborts them all. `AbortHandle::abort` is idempotent, so it composes
/// harmlessly with the mux's socket-close cascade.
pub(crate) struct AbortGuard {
    handles: Vec<tokio::task::AbortHandle>,
}

impl AbortGuard {
    pub(crate) fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    /// Register a task to be aborted when this guard is dropped.
    pub(crate) fn push(&mut self, handle: tokio::task::AbortHandle) {
        self.handles.push(handle);
    }
}

impl Drop for AbortGuard {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

/// Connection mode determines which protocol roles the peer task runs.
///
/// In Cardano (V10+), TCP direction doesn't restrict protocol roles.
/// Duplex mode runs both initiator and responder protocols on one
/// connection — each protocol ID registered twice, once per mux direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionMode {
    /// We initiated TCP, run client-side (initiator) protocols only.
    InitiatorOnly,
    /// They connected to us, run server-side (responder) protocols only.
    ResponderOnly,
    /// Both directions on one connection (not implemented initially).
    Duplex,
}

/// Errors from the peer layer.
#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("protocol error: {0}")]
    Protocol(#[from] crate::protocols::ProtocolError),

    #[error("mux error: {0}")]
    Mux(#[from] crate::mux::MuxError),

    #[error("connection failed: {0}")]
    Connection(String),

    #[error("coordinator shut down")]
    Shutdown,

    #[error("peer {0} disconnected")]
    Disconnected(PeerId),
}

#[cfg(test)]
mod downstream_tests {
    use super::*;

    #[test]
    fn downstream_flag_escalates_monotonically() {
        let f = new_downstream_flag();
        let read = |f: &DownstreamFlag| DownstreamState::from_u8(f.load(Ordering::Relaxed));
        assert_eq!(read(&f), DownstreamState::Cold);
        mark_downstream_warm(&f);
        assert_eq!(read(&f), DownstreamState::Warm);
        mark_downstream_hot(&f);
        assert_eq!(read(&f), DownstreamState::Hot);
        // Never downgrades: a late warm signal can't undo hot.
        mark_downstream_warm(&f);
        assert_eq!(read(&f), DownstreamState::Hot);
    }

    #[test]
    fn downstream_state_from_u8_clamps_unknown_to_hot() {
        assert_eq!(DownstreamState::from_u8(0), DownstreamState::Cold);
        assert_eq!(DownstreamState::from_u8(1), DownstreamState::Warm);
        assert_eq!(DownstreamState::from_u8(2), DownstreamState::Hot);
        // Out-of-range values saturate to the highest known state, not Cold —
        // the flag only climbs, so an unknown-high value is "more than Hot".
        assert_eq!(DownstreamState::from_u8(3), DownstreamState::Hot);
        assert_eq!(DownstreamState::from_u8(255), DownstreamState::Hot);
    }

    #[test]
    fn downstream_state_str() {
        assert_eq!(DownstreamState::Cold.as_str(), "cold");
        assert_eq!(DownstreamState::Warm.as_str(), "warm");
        assert_eq!(DownstreamState::Hot.as_str(), "hot");
    }
}

#[cfg(test)]
mod abort_guard_tests {
    use super::*;
    use tokio::sync::mpsc;

    /// The guard must reap a sub-task that is blocked on a *full* channel —
    /// the case neither a socket close nor a merely-detached JoinHandle can
    /// resolve, and the whole reason the guard exists. Model a protocol
    /// sub-task wedged on `event_sender.send().await` to a full coordinator
    /// channel, then drop the guard and confirm the task is cancelled.
    #[tokio::test]
    async fn guard_reaps_task_blocked_on_full_channel() {
        // Capacity-1 channel with its one slot already taken and no
        // receiver draining it: the next send blocks forever.
        let (tx, _rx) = mpsc::channel::<u32>(1);
        tx.send(0).await.unwrap();

        let stuck = tokio::spawn(async move {
            // Never completes on its own — exactly like a sub-task fanning
            // an event in to a stalled coordinator.
            tx.send(1).await
        });

        let mut guard = AbortGuard::new();
        guard.push(stuck.abort_handle());

        // Dropping the guard (as happens when the per-peer task's stack
        // unwinds on cancellation) must abort the wedged task.
        drop(guard);

        let joined = stuck.await;
        assert!(
            joined.is_err() && joined.unwrap_err().is_cancelled(),
            "guard drop should have cancelled the task blocked on the full channel"
        );
    }

    /// A guard that is never dropped early (the normal path) leaves its
    /// registered tasks running — the guard only acts on drop.
    #[tokio::test]
    async fn guard_does_not_abort_before_drop() {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _ = done_rx.await;
        });

        let mut guard = AbortGuard::new();
        guard.push(task.abort_handle());

        // Guard still alive: signal the task to finish on its own terms.
        done_tx.send(()).unwrap();
        let joined = task.await;
        assert!(
            joined.is_ok(),
            "task should complete normally while the guard is still held"
        );

        // Guard drops here with an already-finished handle — abort is a
        // harmless no-op (idempotent), so this must not panic.
        drop(guard);
    }
}
