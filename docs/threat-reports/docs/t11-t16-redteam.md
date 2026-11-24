# Red Team implementation notes — T11–T16

Companion to [`threat-coverage.md`](./threat-coverage.md) and
[`t22-t26-redteam.md`](./t22-t26-redteam.md). Covers threats **T11–T16** from the Ouroboros Leios
[threat model](https://github.com/input-output-hk/ouroboros-leios/blob/main/docs/threat-model.md):
how the Red Team would implement each, and which environment is the right vehicle.

---

## T11 — Create invalid EB _(Inaction and Nuisance)_

The treat needs to check at which stage the invalid EB is rejected. If it is rejected at the CBOR
decode stage, then the wasted work is minimal. If it is rejected at the cryptographic verification
stage, then the wasted work is significant. The goal is to craft many types of invalid EBs that
should fail at various stages, including an EB that passes the initial checks and only fails at the
cryptographic verification stage.

- **Requirements:** stake for block production, invalid EBs.
- **Impact:** lower throughput (wasted slot), resource waste — an invalid endorser block consumes
  validation cycles before cryptographic checks discard it.
- **Mitigation:** reduced rewards; ensure correct and efficient validation before forward.
- **Attack:** "invalid is the goal". Piranha's _native_ EB is already invalid: fake VRF lottery,
  placeholder `vrf_vkey = [0u8;32]` (`production.rs:282`), zeroed body signature. The invalid EB
  needs to be **decoded and be fetched** so the victim runs (and fails) validation.
- **Red Team by environment:**
  - **Piranha + Ignite**: announce invalid EBs to real Haskell nodes and measure the
    fetch-then-reject work and forwarding termination.
  - **Piranha + Testnet**: attemptable directly, similar as with Ignite, but checking real
    resources.
- **Gap / effort:** **LOW** for a garbage EB that gets rejected early; **MEDIUM** to craft a
  _decodable-but-invalid_ EB (valid CBOR envelope, bad VRF/signature) that survives to the
  cryptographic stage and thereby maximizes wasted validation work.

---

## T12 — Create invalid vote _(Inaction and Nuisance)_

Similar in nature as T11.

- **Requirements:** stake for voting eligibility, invalid votes.
- **Impact:** lower throughput, resource waste — invalid votes burden nodes with useless
  signature-verification work but are quickly rejected. Network traffic storm.
- **Mitigation:** ensure correct and efficient validation before forwarding, potential rate limiting
  if traffic storm saturates the network (?).
- **Attack — "invalid is the goal".** Produce invalid votes that costs a verifier real work.
  Meaningful against a real verifier, a Piranha-only cluster runs no verification. Piranha _already_
  emits votes with an **empty BLS signature** (`consensus/leios/mod.rs:481–483`,
  `vote_signature: Vec::new()`), but an empty signature likely fails a **length/format check at
  decode** and is dropped _before_ any BLS work — cheap early-reject, not the "useless verification
  work" the threat describes. The faithful attack needs a **structurally-valid vote with a bogus
  48-byte signature** that reaches the BLS check and then fails.
- **Red Team by environment:**
  - **Piranha + Ignite**: send decodable-but-invalid votes to real Haskell verifiers and measure the
    wasted BLS verification before rejection; check if invalid votes are not forwarded.
  - **Piranha + Testnet**: attemptable directly.
- **Gap / effort:** **MEDIUM** — the faithful attack is a decodable vote (valid structure, bogus
  48-byte signature) that reaches real BLS verification and fails; a raw empty-signature flood is
  **LOW** but is early-rejected at decode and does **not** exercise the verification-cost the threat
  is about.

---

## T13 — Reference invalid transactions in EB _(Inaction, Nuisance, Network sturation)_

Apart from checking implementation of rejecting invalid transactions, T13 requires also checking how
much network traffic is generated when the EB references non-existent txs. This includes the traffic
storm caused by the nodes attempting to fetch non-existent transactions and the increased load on
the network due to the increased number of invalid transactions.

- **Requirements:** stake for block production, block production.
- **Impact:** lower throughput, resource waste, potentially undefined behavior of the nodes when
  many txs are invalid or non-existent. **Mitigation:** reduced rewards; nodes validate transaction
  references and reject invalid EBs, tx fetching rate limiting (?).
- **Attack — "valid wrapper, bad payload".** EB must be **valid and accepted** so the victim
  proceeds to resolve/validate its referenced transactions and only _then_ finds them bad or
  non-existent. Producing a valid, accepted EB needs real VRF eligibility + signing.
- **Red Team by environment:**
  - **sim-rs** — measure network saturation on non-existent txs.
  - **Piranha + Ignite** — emit valid EBs referencing bad/non-existent tx ids; measure the wasted
    reference-resolution work on real nodes.
  - **Piranha + Testnet** — in addition, measure the network saturation.
  - _Cheaper partial route:_ let a real honest producer build a normal EB, then have Piranha (as
    relay) **withhold the referenced txs** from victims so they are non-existent _to them_ — reuses
    the T20/T22 withholding machinery and needs no modified producer, but only reproduces the "can't
    fetch the referenced txs" effect, not a genuinely invalid reference.
- **Gap / effort:** **MEDIUM–HIGH** — the blocker is a _valid EB with deliberately bad references_,
  i.e. a modified real producer (VRF+KES); the withhold approximation above is **LOW** but
  lower-fidelity.

---

## T14 — Include invalid certificate in RB _(Inaction and Nuisance)_

T14 threat is a cryptographic attack to either break the cryptography primitives (unfeasible) or
steal the keys. The test on the node or network level can only show if the certificate validation
prevents on-chain acceptance or network congestion. This section focuses on networking effects, but
as such T14 might not be applicable to a red team at all.

- **Requirements:** stake for block production, block production.
- **Impact:** lower throughput, resource waste — an invalid certificate inside a ranking block
  creates verification overhead.
- **Mitigation:** certificate verification, rejecting before forwarding.
- **Attack — "valid wrapper, bad payload".** The RB itself must be a **valid Praos block** (real VRF
  eligibility + KES signature) that carries an **invalid Leios certificate**.
- **Red Team by environment:**
  - **Piranha + Ignite** — emitting valid RBs carrying invalid certs; place Piranha as its egress.
    Controlled, reproducible.
  - **Piranha + Testnet** — Real pool registration + stake activation, and it needs the adversarial
    producer fork.
- **Gap / effort:** **HIGH** — the blocker is a _valid RB with a deliberately bad cert_, which needs
  an adversarial Piranha producer (VRF+KES).

---

## T15 — Forge certificate without quorum _(Safety)_

T15 threat can be split into two distinct surfaces:

Surface 1 (BLS break) About whether the cryptographic primitive itself can be broken — forging an
aggregate signature that verifies without the votes. It is therefore an audit question, not Red team
scope.

Surface 2 (quorum accounting) is the part that can be actually attacked; it reduces to "does the
quorum-counting logic have a bug?" Double-counting, accepting duplicate/equivocated/non-member
votes, mis-weighting stake, >= vs > off-by-one. All of those are implementation-correctness defects
in the code that tallies committee weight and compares it to the 3/4 threshold.

The below section focuses on quorum accounting.

- **Requirements:** stake for voting, votes.
- **Impact:** manipulate transaction inclusion — a certificate that _verifies_ without a real
  committee quorum would let an adversary place transactions on-chain unauthorized (a **safety**
  violation).
- **Mitigation:**: quorum logic thoroughly tested.
- **Attack**: Quorum-accounting attack (bug). A certificate is only honest if the _verifier
  correctly counts committee weight_. If the verification logic can be tricked into treating **<
  quorum** as quorum — by double-counting a voter, accepting duplicate / equivocated votes as
  distinct, admitting non-committee votes, mis-attributing stake weight, or a `>=`/`>` threshold
  off-by-one — then a minority of real seats yields a "valid" certificate.
- **Red Team by environment:**
  - **Piranha + Ignite** — Run quorum checks against real Cardano (Leios) nodes.

---

## T16 — Omit transactions from EB _(Omission and Manipulation)_

The threat is about making economic advantage off-chain, that is the profitability lives at the dApp
layer, not the protocol, by withholding transactions. E.g. specific transaction can give the
attacker an opportunity to perform a 'front-running'/'insider trading' attack.

Testing it by the Red Team is not justified, as this is not a threat, but protocol and
implementation feature.
