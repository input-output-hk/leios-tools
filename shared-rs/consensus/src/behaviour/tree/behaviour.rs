//! The behaviour tree and its reactive tick/halt semantics.
//!
//! The compiled tree is an **owned tree of [`Behaviour`] nodes** (references in
//! the config are expanded into independent instances at compile time, so each
//! instance owns its node-local state). Evaluation is **reactive**: every tick
//! re-evaluates a composite from its first child, carrying no resume cursor —
//! the only state that persists between ticks is a `Join`'s succeeded-set and a
//! `ForTicks`'s elapsed count (and an action's own progress). A `Condition`
//! precondition is therefore re-checked each tick and can `halt` a running
//! subtree (the reactive abort).
//!
//! Full operational semantics:
//! `specs/001-behavior-tree-engine/design/bt-grammar-and-semantics.md` §5.

use super::actions::LeafAction;
use super::condition::Condition;
use super::control::ControlSignal;
use super::env::{ConsensusCtx, ParamOverrides, TreeContext};
use super::Status;

/// A behaviour's id (the `[behaviours.<id>]` key). Expanded instances share the
/// id of the definition they came from; it is used for telemetry, not identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BehaviourId(pub String);

impl BehaviourId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<S: Into<String>> From<S> for BehaviourId {
    fn from(s: S) -> Self {
        BehaviourId(s.into())
    }
}

/// A single node in the compiled tree, generic over the tree instantiation's
/// context family `C` (what Conditions read) and effect `E` (what Actions
/// write). The type defaults bind the consensus instantiation
/// (`ConsensusCtx`/`ControlSignal`), so the bare name `Behaviour` resolves to
/// it and existing consumers need no type annotations.
#[derive(Debug)]
pub struct Behaviour<C: TreeContext = ConsensusCtx, E = ControlSignal> {
    pub id: BehaviourId,
    pub kind: BehaviourKind<C, E>,
}

/// The node kinds. Composites and the `ForTicks` decorator own their (already
/// expanded) children; leaves own their evaluator.
#[derive(Debug)]
pub enum BehaviourKind<C: TreeContext = ConsensusCtx, E = ControlSignal> {
    /// Ordered OR / fallback.
    Selector(Vec<Behaviour<C, E>>),
    /// Ordered AND.
    Sequence(Vec<Behaviour<C, E>>),
    /// Concurrent AND, fail-fast. `succeeded[i]` tracks children held done
    /// until the join resets; it always has the same length as `children`.
    Join {
        children: Vec<Behaviour<C, E>>,
        succeeded: Vec<bool>,
    },
    /// Duration cap: run `child` for at most `count` ticks of active life.
    ForTicks {
        count: u32,
        elapsed: u32,
        child: Box<Behaviour<C, E>>,
    },
    /// Pure timing gate: `Running` for `count` ticks of active life, then
    /// `Success`. Contributes **nothing** to the `ControlSignal` — it neither
    /// perturbs nor forces honest. As the first child of a `Sequence` it is a
    /// relative "wait N slots" delay (`Sequence[ Wait(100), attack ]`); inside a
    /// `Join` it delays one branch without clobbering the others' contribution.
    Wait { count: u32, elapsed: u32 },
    /// Immediate predicate (never `Running`).
    Condition(Box<dyn Condition<C>>),
    /// A leaf action (effect contributor).
    Action(Box<dyn LeafAction<C, E>>),
}

impl<C: TreeContext, E> Behaviour<C, E> {
    pub fn new(id: impl Into<BehaviourId>, kind: BehaviourKind<C, E>) -> Self {
        Self {
            id: id.into(),
            kind,
        }
    }
}

impl<C: TreeContext, E> BehaviourKind<C, E> {
    /// Build a `Join`, initialising the succeeded-set to match the children.
    pub fn join(children: Vec<Behaviour<C, E>>) -> Self {
        let succeeded = vec![false; children.len()];
        BehaviourKind::Join {
            children,
            succeeded,
        }
    }

    /// Build a `ForTicks` decorator with a zeroed elapsed count.
    pub fn for_ticks(count: u32, child: Behaviour<C, E>) -> Self {
        BehaviourKind::ForTicks {
            count,
            elapsed: 0,
            child: Box::new(child),
        }
    }

    /// Build a `Wait` timing gate with a zeroed elapsed count.
    pub fn wait(count: u32) -> Self {
        BehaviourKind::Wait { count, elapsed: 0 }
    }
}

impl<C: TreeContext, E> Behaviour<C, E> {
    /// Tick this node, accumulating any active leaf's contribution into `out`.
    pub fn tick(&mut self, ctx: &C::View<'_>, out: &mut E) -> Status {
        match &mut self.kind {
            BehaviourKind::Sequence(children) => tick_sequence(children, ctx, out),
            BehaviourKind::Selector(children) => tick_selector(children, ctx, out),
            BehaviourKind::Join {
                children,
                succeeded,
            } => tick_join(children, succeeded, ctx, out),
            BehaviourKind::ForTicks {
                count,
                elapsed,
                child,
            } => tick_for_ticks(*count, elapsed, child, ctx, out),
            BehaviourKind::Wait { count, elapsed } => tick_wait(*count, elapsed),
            BehaviourKind::Condition(expr) => {
                if expr.eval(ctx) {
                    Status::Success
                } else {
                    Status::Failure
                }
            }
            BehaviourKind::Action(action) => {
                // Apply any live param overrides addressed to this leaf's id
                // before it contributes, so a running attack retunes in place
                // (leaf counters/RNG are preserved — only named fields change).
                apply_action_overrides(&self.id, action.as_mut(), ctx);
                action.contribute(ctx, out)
            }
        }
    }

    /// Abort this node: recursively stop and reset it. An `Action` stops
    /// contributing and resets its progress; a `Condition` is a no-op; a
    /// composite halts all children (and clears carried state).
    pub fn halt(&mut self) {
        match &mut self.kind {
            BehaviourKind::Sequence(children) | BehaviourKind::Selector(children) => {
                for c in children {
                    c.halt();
                }
            }
            BehaviourKind::Join {
                children,
                succeeded,
            } => {
                for c in children.iter_mut() {
                    c.halt();
                }
                for s in succeeded.iter_mut() {
                    *s = false;
                }
            }
            BehaviourKind::ForTicks { elapsed, child, .. } => {
                child.halt();
                *elapsed = 0;
            }
            BehaviourKind::Wait { elapsed, .. } => {
                *elapsed = 0;
            }
            BehaviourKind::Condition(_) => {}
            BehaviourKind::Action(action) => action.reset(),
        }
    }
}

/// Apply any live overrides addressed to `id` (store keys `"<id>.<field>"`) to
/// `action` before it contributes. No-op when the context exposes none; the
/// leaf coerces each TOML scalar to its field type via `set_param`. Reads the
/// overrides through the [`ParamOverrides`] seam so the generic tick never
/// reaches into a concrete context's internals.
fn apply_action_overrides<C: TreeContext, E>(
    id: &BehaviourId,
    action: &mut dyn LeafAction<C, E>,
    ctx: &C::View<'_>,
) {
    for (field, value) in ctx.action_overrides(&id.0) {
        action.set_param(&field, &value);
    }
}

fn tick_sequence<C: TreeContext, E>(
    children: &mut [Behaviour<C, E>],
    ctx: &C::View<'_>,
    out: &mut E,
) -> Status {
    let n = children.len();
    for i in 0..n {
        match children[i].tick(ctx, out) {
            Status::Success => continue,
            Status::Failure => {
                halt_from(children, i + 1);
                return Status::Failure;
            }
            Status::Running => {
                halt_from(children, i + 1);
                return Status::Running;
            }
        }
    }
    Status::Success
}

fn tick_selector<C: TreeContext, E>(
    children: &mut [Behaviour<C, E>],
    ctx: &C::View<'_>,
    out: &mut E,
) -> Status {
    let n = children.len();
    for i in 0..n {
        match children[i].tick(ctx, out) {
            Status::Failure => continue,
            Status::Success => {
                halt_from(children, i + 1);
                return Status::Success;
            }
            Status::Running => {
                halt_from(children, i + 1);
                return Status::Running;
            }
        }
    }
    Status::Failure
}

fn tick_join<C: TreeContext, E>(
    children: &mut [Behaviour<C, E>],
    succeeded: &mut [bool],
    ctx: &C::View<'_>,
    out: &mut E,
) -> Status {
    let n = children.len();
    for i in 0..n {
        if succeeded[i] {
            continue;
        }
        match children[i].tick(ctx, out) {
            Status::Failure => {
                // Fail-fast: kill all children and reset.
                for c in children.iter_mut() {
                    c.halt();
                }
                for s in succeeded.iter_mut() {
                    *s = false;
                }
                return Status::Failure;
            }
            Status::Success => succeeded[i] = true,
            Status::Running => {}
        }
    }
    if succeeded.iter().all(|s| *s) {
        // All done — reset so the node can run again if re-entered.
        for s in succeeded.iter_mut() {
            *s = false;
        }
        Status::Success
    } else {
        Status::Running
    }
}

fn tick_for_ticks<C: TreeContext, E>(
    count: u32,
    elapsed: &mut u32,
    child: &mut Behaviour<C, E>,
    ctx: &C::View<'_>,
    out: &mut E,
) -> Status {
    if *elapsed >= count {
        // Budget already spent: stable "done".
        child.halt();
        return Status::Success;
    }
    let s = child.tick(ctx, out);
    *elapsed += 1;
    match s {
        Status::Running if *elapsed < count => Status::Running,
        Status::Running => {
            // Hit the budget on this tick.
            child.halt();
            Status::Success
        }
        // Child finished early: propagate its terminal status.
        terminal => terminal,
    }
}

/// `Wait`: a childless timing gate. `Running` for a full `count` ticks of active
/// life, then a stable `Success`. Contributes nothing to `out` — the accumulated
/// signal during the wait is whatever other active branches produce (in a bare
/// `Sequence` prefix that is the honest default, because nothing else is active).
/// Being childless and contribution-free, it is independent of the tree's
/// context/effect types, so it needs no generic parameters.
///
/// The contract is literal: `Wait(N)` waits **N slots**. In `Sequence[ Wait(N),
/// X ]`, `X` first activates on the `(N+1)`-th tick — N full slots elapse first.
fn tick_wait(count: u32, elapsed: &mut u32) -> Status {
    if *elapsed >= count {
        // Full budget already spent: stable "done", hand off to the next sibling.
        return Status::Success;
    }
    *elapsed += 1;
    Status::Running
}

fn halt_from<C: TreeContext, E>(children: &mut [Behaviour<C, E>], start: usize) {
    for c in &mut children[start..] {
        c.halt();
    }
}

/// The effective, validated tree, ticked as a unit once per slot. Generic over
/// the instantiation's context family `C` and effect `E`; the defaults bind the
/// consensus instantiation so the bare name resolves to it.
#[derive(Debug)]
pub struct BehaviourTree<C: TreeContext = ConsensusCtx, E = ControlSignal> {
    name: String,
    seed: u64,
    root: Behaviour<C, E>,
}

impl<C: TreeContext, E> BehaviourTree<C, E> {
    pub fn new(name: impl Into<String>, seed: u64, root: Behaviour<C, E>) -> Self {
        Self {
            name: name.into(),
            seed,
            root,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Abort the whole tree (reset all carried state).
    pub fn halt(&mut self) {
        self.root.halt();
    }
}

impl<C: TreeContext, E: Default> BehaviourTree<C, E> {
    /// Tick the whole tree once. The only place decisions are made: returns the
    /// root's status and the slot's accumulated effect.
    pub fn tick(&mut self, ctx: &C::View<'_>) -> (Status, E) {
        let mut out = E::default();
        let status = self.root.tick(ctx, &mut out);
        (status, out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behaviour::tree::actions::{HonestAction, LeafAction};
    use crate::behaviour::tree::condition::ConditionExpr;
    use crate::behaviour::tree::control::ControlSignal;
    use crate::behaviour::tree::env::{
        ConsensusCtx, DynamicEnv, EnvValue, NativeChainState, TickCtx,
    };
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    /// A leaf returning a fixed status, recording tick + reset counts.
    #[derive(Debug)]
    struct Spy {
        status: Status,
        ticks: Arc<AtomicU32>,
        resets: Arc<AtomicU32>,
    }

    impl Spy {
        fn new(status: Status) -> (Self, Arc<AtomicU32>, Arc<AtomicU32>) {
            let ticks = Arc::new(AtomicU32::new(0));
            let resets = Arc::new(AtomicU32::new(0));
            (
                Spy {
                    status,
                    ticks: ticks.clone(),
                    resets: resets.clone(),
                },
                ticks,
                resets,
            )
        }
    }

    impl LeafAction<ConsensusCtx, ControlSignal> for Spy {
        fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
            self.ticks.fetch_add(1, Ordering::SeqCst);
            // Mark a contribution so we can observe it in `out`.
            out.leios.echo_to_source = true;
            self.status
        }
        fn reset(&mut self) {
            self.resets.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn leaf(status: Status) -> Behaviour {
        let (spy, _, _) = Spy::new(status);
        Behaviour::new("leaf", BehaviourKind::Action(Box::new(spy)))
    }

    fn action(b: impl LeafAction<ConsensusCtx, ControlSignal> + 'static) -> Behaviour {
        Behaviour::new("a", BehaviourKind::Action(Box::new(b)))
    }

    fn run(node: &mut Behaviour, state: &NativeChainState) -> (Status, ControlSignal) {
        let env = DynamicEnv::new();
        let ctx = TickCtx {
            env: &env,
            state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = node.tick(&ctx, &mut out);
        (s, out)
    }

    fn run_env(
        node: &mut Behaviour,
        env: &DynamicEnv,
        state: &NativeChainState,
    ) -> (Status, ControlSignal) {
        let ctx = TickCtx {
            env,
            state,
            seed: 0,
            action_params: None,
        };
        let mut out = ControlSignal::default();
        let s = node.tick(&ctx, &mut out);
        (s, out)
    }

    // ---- Sequence (ordered AND) ----

    #[test]
    fn sequence_all_success_is_success() {
        let mut seq = Behaviour::new(
            "s",
            BehaviourKind::Sequence(vec![
                Behaviour::new("h1", BehaviourKind::Action(Box::new(HonestAction))),
                Behaviour::new("h2", BehaviourKind::Action(Box::new(HonestAction))),
            ]),
        );
        let (s, _) = run(&mut seq, &NativeChainState::default());
        assert_eq!(s, Status::Success);
    }

    #[test]
    fn sequence_fails_on_first_failure_and_halts_later() {
        let (running, _, late_resets) = Spy::new(Status::Running);
        let mut seq = Behaviour::new(
            "s",
            BehaviourKind::Sequence(vec![
                leaf(Status::Failure),
                Behaviour::new("late", BehaviourKind::Action(Box::new(running))),
            ]),
        );
        let (s, _) = run(&mut seq, &NativeChainState::default());
        assert_eq!(s, Status::Failure);
        // The later child was halted, not ticked.
        assert_eq!(late_resets.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn sequence_running_halts_later_children() {
        let (running, run_ticks, _) = Spy::new(Status::Running);
        let (late, late_ticks, late_resets) = Spy::new(Status::Success);
        let mut seq = Behaviour::new(
            "s",
            BehaviourKind::Sequence(vec![
                Behaviour::new("r", BehaviourKind::Action(Box::new(running))),
                Behaviour::new("late", BehaviourKind::Action(Box::new(late))),
            ]),
        );
        let (s, _) = run(&mut seq, &NativeChainState::default());
        assert_eq!(s, Status::Running);
        assert_eq!(run_ticks.load(Ordering::SeqCst), 1);
        assert_eq!(late_ticks.load(Ordering::SeqCst), 0);
        assert_eq!(late_resets.load(Ordering::SeqCst), 1);
    }

    // ---- Selector (ordered OR) ----

    #[test]
    fn selector_first_success_short_circuits() {
        let (late, late_ticks, _) = Spy::new(Status::Success);
        let mut sel = Behaviour::new(
            "sel",
            BehaviourKind::Selector(vec![
                leaf(Status::Success),
                Behaviour::new("late", BehaviourKind::Action(Box::new(late))),
            ]),
        );
        let (s, _) = run(&mut sel, &NativeChainState::default());
        assert_eq!(s, Status::Success);
        assert_eq!(late_ticks.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn selector_all_fail_is_failure() {
        let mut sel = Behaviour::new(
            "sel",
            BehaviourKind::Selector(vec![leaf(Status::Failure), leaf(Status::Failure)]),
        );
        let (s, _) = run(&mut sel, &NativeChainState::default());
        assert_eq!(s, Status::Failure);
    }

    // ---- Join (concurrent AND, fail-fast) ----

    #[test]
    fn join_running_until_all_succeed_then_resets() {
        // child A succeeds immediately; child B runs once then succeeds.
        let (a, a_ticks, _) = Spy::new(Status::Success);
        let mut join = Behaviour::new(
            "j",
            BehaviourKind::join(vec![
                Behaviour::new("a", BehaviourKind::Action(Box::new(a))),
                leaf(Status::Running),
            ]),
        );
        // Tick 1: A succeeds, B running → Running.
        let (s1, _) = run(&mut join, &NativeChainState::default());
        assert_eq!(s1, Status::Running);
        assert_eq!(a_ticks.load(Ordering::SeqCst), 1);
        // Tick 2: A is held done (not re-ticked); B still running → Running.
        let (s2, _) = run(&mut join, &NativeChainState::default());
        assert_eq!(s2, Status::Running);
        assert_eq!(
            a_ticks.load(Ordering::SeqCst),
            1,
            "succeeded child re-ticked"
        );
    }

    #[test]
    fn join_all_success_is_success() {
        let mut join = Behaviour::new(
            "j",
            BehaviourKind::join(vec![leaf(Status::Success), leaf(Status::Success)]),
        );
        let (s, _) = run(&mut join, &NativeChainState::default());
        assert_eq!(s, Status::Success);
    }

    #[test]
    fn join_fail_fast_halts_all() {
        let (a, _, a_resets) = Spy::new(Status::Success);
        let mut join = Behaviour::new(
            "j",
            BehaviourKind::join(vec![
                Behaviour::new("a", BehaviourKind::Action(Box::new(a))),
                leaf(Status::Failure),
            ]),
        );
        let (s, _) = run(&mut join, &NativeChainState::default());
        assert_eq!(s, Status::Failure);
        // The succeeded sibling was halted on fail-fast.
        assert_eq!(a_resets.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn join_ticks_both_running_actions_each_tick() {
        // The duplex-follower shape: two always-Running actions under a Join.
        let (a, a_ticks, _) = Spy::new(Status::Running);
        let (b, b_ticks, _) = Spy::new(Status::Running);
        let mut join = Behaviour::new("j", BehaviourKind::join(vec![action(a), action(b)]));
        for _ in 0..3 {
            let (s, out) = run(&mut join, &NativeChainState::default());
            assert_eq!(s, Status::Running);
            assert!(out.leios.echo_to_source);
        }
        assert_eq!(a_ticks.load(Ordering::SeqCst), 3);
        assert_eq!(b_ticks.load(Ordering::SeqCst), 3);
    }

    // ---- ForTicks (duration cap) ----

    #[test]
    fn for_ticks_caps_a_running_child() {
        let (running, ticks, resets) = Spy::new(Status::Running);
        let mut node = Behaviour::new(
            "ft",
            BehaviourKind::for_ticks(
                2,
                Behaviour::new("r", BehaviourKind::Action(Box::new(running))),
            ),
        );
        // Ticks 1..=2 within budget → Running.
        assert_eq!(
            run(&mut node, &NativeChainState::default()).0,
            Status::Running
        );
        assert_eq!(
            run(&mut node, &NativeChainState::default()).0,
            Status::Success
        );
        // Budget hit on tick 2: child halted, returns Success thereafter.
        assert_eq!(
            run(&mut node, &NativeChainState::default()).0,
            Status::Success
        );
        assert_eq!(ticks.load(Ordering::SeqCst), 2, "child ticked past budget");
        assert!(resets.load(Ordering::SeqCst) >= 1);
    }

    #[test]
    fn for_ticks_propagates_early_terminal() {
        let mut node = Behaviour::new("ft", BehaviourKind::for_ticks(5, leaf(Status::Failure)));
        assert_eq!(
            run(&mut node, &NativeChainState::default()).0,
            Status::Failure
        );
    }

    // ---- Wait (pure timing gate) ----

    #[test]
    fn wait_runs_for_count_ticks_then_stable_success() {
        let mut node = Behaviour::new("w", BehaviourKind::wait(3));
        let st = NativeChainState::default();
        // A full 3 ticks of Running (3 slots waited), then a stable Success.
        assert_eq!(run(&mut node, &st).0, Status::Running);
        assert_eq!(run(&mut node, &st).0, Status::Running);
        assert_eq!(run(&mut node, &st).0, Status::Running);
        assert_eq!(run(&mut node, &st).0, Status::Success);
        assert_eq!(run(&mut node, &st).0, Status::Success);
    }

    #[test]
    fn wait_contributes_nothing() {
        // A pure timing gate must never perturb the control signal — the slot's
        // accumulated signal is the honest default while a bare Wait is active.
        let mut node = Behaviour::new("w", BehaviourKind::wait(2));
        let (s, out) = run(&mut node, &NativeChainState::default());
        assert_eq!(s, Status::Running);
        assert_eq!(out, ControlSignal::default());
    }

    #[test]
    fn wait_stages_a_relative_delay_in_a_sequence() {
        // Sequence[ Wait(2), attack ]: 2 full slots elapse before the attack (a
        // contributing Spy) activates on the 3rd tick — a relative "wait N slots".
        let (attack, attack_ticks, _) = Spy::new(Status::Running);
        let mut seq = Behaviour::new(
            "s",
            BehaviourKind::Sequence(vec![
                Behaviour::new("w", BehaviourKind::wait(2)),
                action(attack),
            ]),
        );
        let st = NativeChainState::default();
        // Ticks 1..=2: still waiting — attack not ticked, no contribution.
        for _ in 0..2 {
            let (s, out) = run(&mut seq, &st);
            assert_eq!(s, Status::Running);
            assert!(!out.leios.echo_to_source);
        }
        assert_eq!(attack_ticks.load(Ordering::SeqCst), 0);
        // Tick 3: wait done, sequence advances → attack active and contributing.
        let (s3, out3) = run(&mut seq, &st);
        assert_eq!(s3, Status::Running);
        assert!(out3.leios.echo_to_source);
        assert_eq!(attack_ticks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn wait_in_join_does_not_clobber_a_concurrent_sibling() {
        // "Do whatever you're doing for N slots": a Wait branch delays one arm
        // without suppressing another arm that is actively perturbing from tick 1.
        let (attacker, _, _) = Spy::new(Status::Running);
        let mut join = Behaviour::new(
            "j",
            BehaviourKind::join(vec![
                action(attacker),
                Behaviour::new("w", BehaviourKind::wait(5)),
            ]),
        );
        let (_, out) = run(&mut join, &NativeChainState::default());
        // The concurrent attacker's contribution survives while the other waits.
        assert!(out.leios.echo_to_source);
    }

    #[test]
    fn wait_resets_elapsed_on_halt() {
        let mut node = Behaviour::new("w", BehaviourKind::wait(3));
        run(&mut node, &NativeChainState::default());
        node.halt();
        if let BehaviourKind::Wait { elapsed, .. } = &node.kind {
            assert_eq!(*elapsed, 0, "halt must reset Wait elapsed");
        } else {
            panic!("expected Wait");
        }
    }

    // ---- Condition (immediate) ----

    #[test]
    fn condition_is_immediate_success_or_failure() {
        let expr = ConditionExpr::parse("cardano.current_slot >= 100").unwrap();
        let mut cond = Behaviour::new("c", BehaviourKind::Condition(Box::new(expr)));
        let below = NativeChainState {
            current_slot: 50,
            ..Default::default()
        };
        let at = NativeChainState {
            current_slot: 100,
            ..Default::default()
        };
        assert_eq!(run(&mut cond, &below).0, Status::Failure);
        assert_eq!(run(&mut cond, &at).0, Status::Success);
    }

    // ---- Reactive abort ----

    #[test]
    fn reactive_abort_halts_running_subtree_when_precondition_flips() {
        // Selector[ Sequence[ Condition(slot >= trigger), runningAction ], honest ]
        let (adv, adv_ticks, adv_resets) = Spy::new(Status::Running);
        let expr = ConditionExpr::parse("cardano.current_slot >= env.trigger").unwrap();
        let mut tree = Behaviour::new(
            "root",
            BehaviourKind::Selector(vec![
                Behaviour::new(
                    "attack",
                    BehaviourKind::Sequence(vec![
                        Behaviour::new("cond", BehaviourKind::Condition(Box::new(expr))),
                        Behaviour::new("adv", BehaviourKind::Action(Box::new(adv))),
                    ]),
                ),
                Behaviour::new("honest", BehaviourKind::Action(Box::new(HonestAction))),
            ]),
        );

        let mut env = DynamicEnv::new();
        env.insert("trigger", EnvValue::U64(100));
        let armed = NativeChainState {
            current_slot: 100,
            ..Default::default()
        };
        let disarmed = NativeChainState {
            current_slot: 99,
            ..Default::default()
        };

        // Armed: condition holds, adversarial action runs and contributes.
        let (s, out) = run_env(&mut tree, &env, &armed);
        assert_eq!(s, Status::Running);
        assert!(out.leios.echo_to_source);
        assert_eq!(adv_ticks.load(Ordering::SeqCst), 1);

        // Precondition flips to Failure: the Sequence fails, the running action
        // is halted (reset), and the Selector falls back to honest.
        let (s2, out2) = run_env(&mut tree, &env, &disarmed);
        assert_eq!(s2, Status::Success);
        assert!(
            !out2.leios.echo_to_source,
            "adversarial effect should vanish"
        );
        assert_eq!(
            adv_ticks.load(Ordering::SeqCst),
            1,
            "halted action re-ticked"
        );
        assert_eq!(
            adv_resets.load(Ordering::SeqCst),
            1,
            "action not reset on abort"
        );
    }

    // ---- halt resets carried state ----

    #[test]
    fn halt_resets_join_and_for_ticks_state() {
        let mut join = Behaviour::new(
            "j",
            BehaviourKind::join(vec![leaf(Status::Success), leaf(Status::Running)]),
        );
        // Mark one child succeeded.
        let _ = run(&mut join, &NativeChainState::default());
        join.halt();
        if let BehaviourKind::Join { succeeded, .. } = &join.kind {
            assert!(
                succeeded.iter().all(|s| !s),
                "succeeded not cleared by halt"
            );
        } else {
            panic!("expected Join");
        }

        let mut ft = Behaviour::new("ft", BehaviourKind::for_ticks(3, leaf(Status::Running)));
        let _ = run(&mut ft, &NativeChainState::default());
        ft.halt();
        if let BehaviourKind::ForTicks { elapsed, .. } = &ft.kind {
            assert_eq!(*elapsed, 0, "elapsed not reset by halt");
        } else {
            panic!("expected ForTicks");
        }
    }

    // ---- BehaviourTree wrapper ----

    #[test]
    fn behaviour_tree_ticks_root_and_returns_signal() {
        let mut tree: BehaviourTree = BehaviourTree::new(
            "t",
            42,
            Behaviour::new("honest", BehaviourKind::Action(Box::new(HonestAction))),
        );
        assert_eq!(tree.name(), "t");
        assert_eq!(tree.seed(), 42);
        let env = DynamicEnv::new();
        let state = NativeChainState::default();
        let (s, out) = tree.tick(&TickCtx {
            env: &env,
            state: &state,
            seed: 42,
            action_params: None,
        });
        assert_eq!(s, Status::Success);
        assert_eq!(out, ControlSignal::default());
    }

    /// A leaf with one tunable param, observable via `praos.reorg_depth`.
    #[derive(Debug)]
    struct Tunable(i64);
    impl LeafAction<ConsensusCtx, ControlSignal> for Tunable {
        fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
            out.praos.reorg_depth = Some(self.0 as u64);
            Status::Running
        }
        fn set_param(&mut self, field: &str, value: &toml::Value) {
            if field == "v" {
                if let Some(v) = value.as_integer() {
                    self.0 = v;
                }
            }
        }
    }

    #[test]
    fn tick_applies_action_param_overrides_by_id() {
        use crate::behaviour::tree::env::ActionParamStore;
        use std::collections::BTreeMap;
        use std::sync::RwLock;

        let mut node = Behaviour::new("p", BehaviourKind::Action(Box::new(Tunable(1))));
        let env = DynamicEnv::new();
        let state = NativeChainState::default();

        // No store → the leaf's compiled default.
        let mut out = ControlSignal::default();
        node.tick(&TickCtx::new(&env, &state, 0), &mut out);
        assert_eq!(out.praos.reorg_depth, Some(1));

        // An override addressed to this node's id is applied before contribute;
        // an override for a different id is ignored.
        let store: ActionParamStore = Arc::new(RwLock::new(BTreeMap::from([
            ("p.v".to_string(), toml::Value::Integer(9)),
            ("other.v".to_string(), toml::Value::Integer(7)),
        ])));
        let ctx = TickCtx {
            env: &env,
            state: &state,
            seed: 0,
            action_params: Some(&store),
        };
        let mut out = ControlSignal::default();
        node.tick(&ctx, &mut out);
        assert_eq!(out.praos.reorg_depth, Some(9));
    }
}
