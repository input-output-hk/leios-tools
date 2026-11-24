# Red Team implementation notes — T5–T10

Companion to [`threat-coverage.md`](./threat-coverage.md), [`t11-t16-redteam.md`](./t11-t16-redteam.md)
and [`t22-t26-redteam.md`](./t22-t26-redteam.md). Covers threats **T5–T10** from the Ouroboros Leios
[threat model](https://github.com/input-output-hk/ouroboros-leios/blob/main/docs/threat-model.md):
how the Red Team would implement each, and which environment is the right vehicle.

Environment levels referenced below are defined in [`environment-model.md`](./environment-model.md).

## Grouping

T5–T10 split into two categories from the threat model:

- **Equivocation (T5–T8)** — produce/cast *conflicting* artifacts (EBs, votes) or misuse compromised
  keys. Mitigation is "equivocation detection per Leios design" (except T7/T8).
- **Inaction and Nuisance (T9–T10)** — decline to produce an EB / decline to vote when eligible.
  Mitigation is "reduced rewards".

## Certification safety model (governs T5–T7)

The equivocation threats T5–T7 all hinge on the same small set of mechanisms. Stating them once:

- **Certificates function only via RB inclusion.** In the current protocol a Leios certificate's sole
  job is to authorise boosting an EB into a ranking block; it is not consumed as a standalone
  finality/attestation signal (that is **Peras**, a separate voting system — not this). So a
  certificate only ever "matters" once its RB wins chain selection.
- **Ordering safety = RB chain selection.** Which fork/EB is canonical is settled by Praos chain
  selection, exactly as for any fork — *"only RB inclusion affects chain selection, not certificate
  existence alone."*
- **Certificate uniqueness (≤ 1 certified EB per position) = 75% quorum + the honest single-vote
  rule.** Two disjoint vote-sets cannot both clear 75% (`0.75 + 0.75 > 1`), *provided each honest
  voter's weight lands on at most one EB per position*. That proviso — an honest voter votes for the
  one EB it adopts — is the load-bearing assumption. **This is basic, passive dedup, distinct from the
  three mechanisms it is easy to conflate:**
  1. **Single-vote rule** (passive, per-position) — makes the 75% arithmetic bite. *The load-bearer.*
  2. **Accounting dedup `(voter, eb_hash)`** — don't count one voter twice for one EB (the T6 mechanism).
  3. **Reactive equivocation detection** — on *seeing* two EBs from one producer, withhold from both.
- **Reactive equivocation detection is a deterrent, not a safety mechanism (in this protocol).**
  Uniqueness already follows from (1) + the 75% quorum, so reactive detection adds no safety we don't
  have; its only effect is to deny an equivocator even its *own* single certificate (trading a wasted
  slot for punishment). It is **self-triggering only** — framing an honest producer would need their
  keys (that is **T8**) — so the sole "attack" it enables is self-DoS, **strictly dominated by T9**.
- **Two contradictory certificates need ≥ 50% adversarial stake via T5+T7.** Forcing both EB_a and
  EB_b past 75% requires the adversary to double-vote both (T7) atop EB equivocation (T5):
  `H + 2A ≥ 1.5 ⇒ A ≥ 0.5`. Reactive detection hardens this further (also demands a perfect network
  partition), but at ≥ 50% stake it is moot. For any realistic adversary, uniqueness holds from the
  threshold alone.

**Open spec question (confirm, don't assume):** do honest voters vote for **at most one EB per
position**, and how is that adopted EB chosen? Everything above rests on that single-vote rule — not on
reactive detection.

---

## T5 — EB equivocation  *(Equivocation)*

- **Capability:** Registered SPO with stake for block production, ability to forge blocks (KES, VRF keys)
- **Impact:** lower throughput, resource waste. With equivocation detection enabled, nodes withhold
  votes from the equivocated EBs entirely, so none certifies. Without it, honest voters vote for the EB
  they adopt; at most one variant can reach the 75% quorum (see *Certification safety model* above), so
  again at most one certifies and the rest are orphaned by chain selection. **So the consensus outcome
  is essentially the same either way** — which answers the old "why is detection useful?" question:
  in this protocol reactive detection is not safety-load-bearing (the 75% quorum + honest single-vote
  give certificate uniqueness; RB chain selection gives ordering). It only denies the equivocator its
  *own* certificate — a self-inflicted throughput penalty, dominated by T9. The equivocation DoS floor
  is therefore just "a wasted slot," identical to declining to produce (T9), unless combined with T7 at
  ≥ 50% stake.
- **Mitigation:** equivocation detection per Leios design.
- **Model description:** block producers create multiple conflicting Endorser Blocks and distribute
  them across *different network segments* to disrupt the protocol.
- **Gating:** Partially topology-gated.  Being well positioned allows us to create maximum damage, but it can also
  have an effect scattergun across enough peers.
- **Implementation:** Implement "EB equivocation" as an action in shared-rs/consensus, producing variant EBs (which could just be different selections of tx from mempool) and passing them to distinct sets of peers, as the existing "RB equivocation" does (or should do).  We need to be a block producer and can equivocate at RB level with different EBs announcements.  We could do it as a relay by editing other people's EBs in the LeiosNotify/LeiosFetch process, but that will fail the EB hash test immediately and probably get us dropped as upstream.  We could have a side-channel mechanism which will tell other Piranha relays which one to propagate.
- **Red Team by environment:**
  - **1: sim-rs** — Enable EB equivocation in a selection of nodes and see the effect on EB adoption, with equivocation detection enabled and disabled.
  - **2: Piranha (local)** — ditto, with live demo
  - **3: Piranha + ignite** — Insert a Piranha node with EB equivocation into a network of real nodes and verify that their equivocation detection fires.
  - **4: Piranha + testnet** — Become an upstream of one or more testnet relays and offer bogus EBs to them
- **Gap / effort:** We need to be a block producer, and would produce variant EBs by selecting different tx from mempool, then forge multiple RB variants with different eb_announcement hashes, and use the existing RBEquivocation action to differentially propagate them
- **Recommended path:** After we are a block producer, build variant EB production and equivocation into shared/consensus and test in sim & local Piranha cluster first.  Then test against real nodes in Ignite and (if we can gain reliable upstream trust) Testnet.

---

## T6 — Vote equivocation  *(Equivocation)*

- **Capability:** registered SPO with voting eligibility + BLS key (needed to sign at all).
- **Impact (as modelled):** interfere with certificate creation.
- **Mitigation:** equivocation detection per Leios design.
- **Model description:** voting participants generate conflicting votes to interfere with certificate
  creation and destabilize the certification mechanism.

**This threat collapses — there is no independent, constructible instance.** "Vote equivocation" means
one voter emitting more than one vote where it may cast one *for the same EB* (same `eb_hash`, same
slot). Two facts close it:

1. **You cannot construct two distinct valid votes for the same `(voter, eb_hash, slot)`.** A vote is a
   BLS signature over fixed fields — `voter_id`, `eb_hash`, `eb_slot` (vote body `types.rs:158`;
   eligibility/vote signature over `(node_id, eb_hash, eb_slot)` at `leios.rs:769`,
   `committee::npv_eligibility_signature`). BLS signatures are deterministic and unique: signing the
   same message twice yields the *same* bytes, and exactly one valid signature exists per
   `(pubkey, message)`. So there is no second, different-but-valid vote to make — and a relay cannot
   forge one either (no private key; BLS is non-malleable under canonical encoding + subgroup checks).
2. **A bit-identical duplicate is already deduped on the hottest path in the system.** Vote diffusion
   is content-addressed and flood-based, so every node receives each vote from many peers every slot;
   `(voter, eb_hash)` dedup (`aggregation.rs`, "Duplicate voter → `None`") is therefore continuously
   load-bearing in *honest* operation. An adversarial duplicate only probes a path that must already
   work or nothing certifies. There is no outer framing / separable message-id to dodge it with — the
   vote *is* the message and its identifier derives from its content.

So any genuinely *different* valid vote from one voter must change a signed field → a different
`eb_hash` (that is **T7**, voting on multiple EBs) or requires the private key (that is **T8**, key
compromise). Same-EB vote equivocation degenerates to a duplicate (self-tested by diffusion), to T7,
or to T8. The threat model's own hedge points the same way: *"only RB inclusion affects chain
selection, not certificate existence alone."*

- **Action item (spec confirmation, not a live test):** verify from the spec docs that (a) the vote
  signature binds `(voter, eb_hash, eb_slot)` and (b) quorum accounting dedups at the accounting layer
  by `(voter, eb_hash)` — **not** by `(voter, slot)` alone (which would let a vote leak across
  EBs/forks and fold T7 into T6). Our clean-room impl does both (`types.rs:158`, `aggregation.rs`), but
  a correct clean-room build proves neither the spec's intent nor production `cardano-node`'s
  behaviour: the clean-room node exists to check the spec against production and to be the honest base
  we bend into the attacks above, not as a golden reference. The value here is catching a spec
  ambiguity or a production divergence — not a runtime failure we expect.

---

## T7 — Double voting  *(Equivocation)*

- **Capability:** registered SPO with voting eligibility + BLS key, plus a supply of multiple EBs to
  vote on (see Implementation).
- **Impact:** multiple certificates, resource waste.
- **Mitigation:** chain selection prioritizes RB inclusion.
- **Model description:** adversaries cast votes on multiple conflicting Endorser Blocks within the same
  voting period, producing wasteful certificate proliferation.
- **This is the "vote for multiple *different* EBs" case** — distinct `eb_hash`es, each vote
  individually valid. It is *not* signature-equivocation (every vote is well-formed), which is why the
  mitigation is chain selection, not equivocation detection, and why `(voter, eb_hash)` dedup
  deliberately permits it: each EB has its own election (`aggregation.rs`), so one voter can add weight
  to two.
- **Different EBs in the same voting period are necessarily on different chain forks.** Under Linear
  Leios a chain references one certified EB per pipeline position, so two *distinct* certified EBs for
  that position can only be adopted on divergent RB histories. The competing EBs come from either
  (a) **EB equivocation by a producer** — the T5 variants, making T7 the voting-side extension of T5 —
  or (b) a **pre-existing Praos fork**. Either way we put our vote weight on both forks.
- **Why the impact is only resource waste:** it is RB inclusion / Praos chain selection — not
  certificate existence — that decides which EB is adopted (*"only RB inclusion affects chain
  selection, not certificate existence alone"*). The losing fork's certificate is orphaned. Double
  voting can even *help* a fork we would not otherwise have voted for; it never forces a consensus
  split, because a fork is exactly what Praos already resolves.
- **Gating:** stake-gated, not topology-gated. Positioning changes only *which* peers see *which* vote,
  not the outcome — chain selection governs regardless.
- **Implementation:** an extension of T5. Once the equivocating producer holds multiple valid EBs
  (distinct hashes) for the slot, cast a valid vote for each and send them differentially — and/or fan
  them out via the Piranha side-channel exactly as in T5. With a pre-existing fork, simply vote for the
  EB on each fork.
- **Red Team by environment:**
  - **1: sim-rs** — with T5 EB equivocation, vote for each variant EB; measure certificate
    proliferation and confirm chain selection collapses to one adopted EB.
  - **2: Piranha (local)** — ditto, with live demo.
  - **3: Piranha + ignite** — vote for competing EBs against real nodes; confirm only the RB-included
    EB's certificate survives and the other is orphaned.
  - **4: Piranha + testnet** — opportunistic: when a real fork exists, or once we can produce
    equivocated EBs and gain upstream trust.
- **Gap / effort:** as T5 (be a producer, generate variant EBs), plus produce one valid vote per EB
  hash — low marginal effort once T5 exists.
- **Recommended path:** build on T5; demonstrate in sim / local Piranha that competing certificates are
  orphaned by chain selection, then confirm against real nodes in Ignite.

---

## T8 — BLS key compromise  *(Equivocation)*

- **Capability:** cryptographic attack. **Impact:** unauthorized vote creation.
  **Mitigation:** key rotation, defensive equivocation.
- **Model description:** cryptographic compromise of BLS private keys enables unauthorized vote
  creation; defensive equivocation by legitimate holders invalidates both honest and adversarial votes
  until key rotation completes.
- **Note:** capability is a *cryptographic attack* on BLS keys, not a stake/topology capability —
  likely audit-only / out of scope for the live environments. <!-- TODO (decide): confirm scope;
  compare to the T15 "certificate/quorum forgery" Tier-C framing in the T11–T16 doc. -->
- **Gating:** <!-- TODO (decide) -->
- **Red Team by environment:**
  - **sim-rs** — <!-- TODO -->
  - **Piranha (local)** — <!-- TODO -->
  - **Piranha + ignite** — <!-- TODO -->
  - **Piranha + testnet** — <!-- TODO -->
- **Gap / effort:** <!-- TODO -->
- **Recommended path:** <!-- TODO -->

---

## T9 — Decline to create EB  *(Inaction and Nuisance)*

- **Capability:** stake for block production. **Impact:** lower throughput.
  **Mitigation:** reduced rewards.
- **Model description:** producer nodes fail to generate Endorser Blocks when eligible, reducing
  protocol throughput.
- **Gating:** <!-- TODO (decide): inaction attack — likely stake-gated, not topology-gated. -->
- **Red Team by environment:**
  - **sim-rs** — <!-- TODO -->
  - **Piranha (local)** — <!-- TODO -->
  - **Piranha + ignite** — <!-- TODO -->
  - **Piranha + testnet** — <!-- TODO -->
- **Gap / effort:** <!-- TODO -->
- **Recommended path:** <!-- TODO -->

---

## T10 — Decline to vote  *(Inaction and Nuisance)*

- **Capability:** stake for voting eligibility. **Impact:** lower throughput.
  **Mitigation:** reduced rewards.
- **Model description:** eligible voting participants abstain from casting required votes, degrading
  certification capacity.
- **Gating:** <!-- TODO (decide): inaction attack — likely stake-gated, not topology-gated. -->
- **Red Team by environment:**
  - **sim-rs** — <!-- TODO -->
  - **Piranha (local)** — <!-- TODO -->
  - **Piranha + ignite** — <!-- TODO -->
  - **Piranha + testnet** — <!-- TODO -->
- **Gap / effort:** <!-- TODO -->
- **Recommended path:** <!-- TODO -->

---

## Summary

<!-- TODO (decide): fill remaining rows once the per-threat gating/vehicle calls are made, mirroring the
summary tables in the T11–T16 and T22–T26 docs. -->

| Threat | Topology-gated? | Primary vehicle | Live-testnet route |
|---|---|---|---|
| T5 EB equivocation | Partially (position maximises spread) | sim → local → ignite (must be a block producer; build EB-equivocation action) | if reliable upstream trust can be gained |
| T6 vote equivocation | **N/A — collapses** (dup / T7 / T8) | none — spec confirmation only | n/a |
| T7 double voting | No (stake-gated) | sim → ignite (extension of T5) | opportunistic (real fork / after T5) |
| T8 BLS key compromise | TODO | TODO | TODO |
| T9 decline to create EB | TODO | TODO | TODO |
| T10 decline to vote | TODO | TODO | TODO |
