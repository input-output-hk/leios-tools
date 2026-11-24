# Leios Threat Model — Adversarial Tooling Coverage

Tracks every threat from the Ouroboros Leios
[threat model](https://github.com/input-output-hk/ouroboros-leios/blob/main/docs/threat-model.md)
(33 threats, T1–T33) against the adversarial tooling, their status and additional comments.

## Columns

- **Type** — threat category (A–F) from [`threat-types.md`](./threat-types.md);
  `?` where no single category clearly applies.
- **Who** — the person working on the threat.
- **Status** — implementation status of adversarial tooling for the threat:
  `analyzing`, `reviewing`, `not started`, `in progress`, `done`, `blocked`, `not relevant`.
The four tool columns follow the realism levels in
[`environment-model.md`](./environment-model.md):

- **Sim** — the safe simulator (`leios-tools/sim-rs` + the `shared-consensus`
  behaviour-tree engine). Used for protocol/crypto/economic threats studied without
  a real network (modelled validation, assumed stake/topology).
- **Piranha** — the local Piranha-Cluster (`net-rs`: `net-node` / `net-cluster`,
  local multi-node clusters). Our own nodes with **faked** validation; used to build
  and demonstrate the attack mechanism and small controlled topologies.
- **Piranha + Ignite** — Piranha nodes among **real Haskell Leios nodes** with real
  ledger/validation, arbitrary assigned stake and full topology control (including
  man-in-the-middle placement). The controllable, reproducible rehearsal/enabler
  environment.
- **Piranha + Testnet** — Piranha connected to a **real testnet**: real nodes and
  ledger, stake via faucet, but **no topology control**. The high-value target for
  threats needing real ledger semantics, value/MEV, governance or economic
  conditions.

Each tool cell holds a very concise note on why that tool is needed; a `-`
means the tool is not an appropriate vehicle for the threat.

- **Comments** — free-form notes.
- tool columns shall point out which tool capability is exercised by the threat. 
  Over time, we shall standarize those capabilities so that we have a collection of them to pick from.

## Threat coverage table

| ID | Threat | Type | Who | Status | Sim | Piranha | Piranha + Ignite | Piranha + Testnet | Comments |
|----|--------|------|-----|--------|-----|---------|------------------|-------------------|----------|
| T1 | Any threat to Praos | A |  | not started | Consensus logic | Live forks/propagation | Real chain | TODO |  |
| T2 | VRF grinding on EB eligibility | A |  | not relevant | Eligibility/crypto logic | Observable effect | - | TODO | Since VRF is deterministic: once nonce is fixed, one can compute own win/lose for every future slot exactly |
| T3 | VRF grinding on voting eligibility | A |  | not relevant | Eligibility/crypto logic | Observable effect | - | TODO | See T2 |
| T4 | VRF key compromise | F |  | not relevant | - | - | - | TODO | Nothing to check for us? Key compromises are cyber security attacks, which need to be mitigated by security standards compliance (see OWASP ASVS, whole 11.1, 11.2, 13.3, etc) and pentests |
| T5 | EB equivocation | A | prc | analysing | Protocol behaviour | Per-peer routing | - | TODO |  |
| T6 | Vote equivocation | A | prc | analysing | Protocol behaviour | Per-peer routing | - | TODO |  |
| T7 | Double voting | A | prc  | analysing | Voting logic | Cross-peer effect | - | TODO |  |
| T8 | BLS key compromise | F | prc | not started | Cert/crypto logic | Effect observable | - | TODO |  |
| T9 | Decline to create EB | C | prc | not started | Producer behaviour | Throughput effect | - | TODO |  |
| T10 | Decline to vote | C | prc | not started | Voter behaviour | Certification effect | - | TODO |  |
| T11 | Create invalid EB | B | cet | in progress | N/A | N/A | EB emit (w43) | EB emit | The w43 campaign recorded rejections at announcement, decode, and body arrival, with no node crashes. The current harness supports `huge`, `oversized`, `hollow`, and `malformed`. The `duplicate` runs are historical. Open items include cryptographic validation and a body-size mismatch with a non-empty body. [Campaign report](../t11/report.md) |
| T12 | Create invalid vote | B | kp | analyzing | N/A | N/A | vote emit | vote emit | Send decodable-but-invalid votes to real Haskell verifiers |
| T13 | Reference invalid transactions in EB | D | kp | analyzing | network saturation | N/A | EB emit | EB emit | Emit valid EBs referencing bad/non-existent txs, network effects |
| T14 | Include invalid certificate in RB | B | kp | analyzing | N/A | N/A | EB emit | EB emit | Emitting valid RBs carrying invalid certs, network effects |
| T15 | Forge certificate without quorum | F | kp | analyzing | N/A | N/A | N/A | vote emit | Run quorum checks against real Cardano (Leios) nodes |
| T16 | Omit transactions from EB | E | kp | analyzing | N/A | N/A | N/A | N/A | Not a threat, but protocol and implementation feature. |
| T17 | Reorder transactions in EB | E |  | not started | Mechanism | Mechanism | MEV needs real value | TODO |  |
| T18 | Insert or replace transactions in EB | E |  | not started | Mechanism | Mechanism | MEV needs real value | TODO |  |
| T19 | Ignore certificates, include txs in RB only | C |  | not started | Producer strategy | Throughput effect | - | TODO |  |
| T20 | Withhold announced EB or endorsed transactions | A |  | not started | - | Network-delivery control | Real conditions | TODO |  |
| T21 | Selectively withhold data from voting committee | A |  | in progress | - | Per-path withholding / topology | Realistic scale | TODO | t22 attack (committee params) |
| T22 | Selectively withhold data from honest nodes | A | - | in progress | - | Per-peer withholding (t22); local topology | Place relay + assign stake: feed committee, starve honest | - | Topology-gated. Lowest-hanging attack. |
| T23 | Withhold then release large number of EBs | D | - | not started | Burst / freshest-first dynamics | Burst dynamics (faked EBs + validation) |  | Real staked Leios BP node produces EBs; Piranha bursts |  |
| T24 | Submit duplicate transactions | D | cet | done | - | Duplicate-submission harness (validation faked) | Controlled real-node dedup; tunable mempool | Real dedup; valid tx via cardano-cli | Done on the proto-devnet with `make -C proto-devnet t24` ([`../t24/README.md`](../t24/README.md)). Three fresh runs agree: node-to-node TxSubmission deduplicates (MITIGATED on one connection and on three concurrent connections), while local submission validates every duplicate (WASTE, 1:1, about 0.15 ms each). Attackers are zero-stake net-nodes as trustable local roots; S uses the duplicate-admission flag and `run_client` tracing (leios-tools PR #123). T11's historical `duplicate` runs cover repeated tx ids inside an endorser block |
| T25 | Submit invalid transactions | D | cet | in progress | - | BT tx-flood: garbage flood via TxSubmission → mempool displacement (merged) | Controlled real-node validation; tunable | Real validate-before-propagate | Cheapest DoS. BT `tx-flood` action; see [`../tx-injection/PLAN.md`](../tx-injection/PLAN.md) |
| T26 | Submit conflicting transactions | D | - | not started | - | Conflicting-submission harness (validation faked) | Real ledger + Linear Leios; tunable | Real fee/ledger resolution; cardano-cli txs (faucet fees) | Needs valid conflicting txs + fees (cardano-cli: two txs on same UTxO). See [`../tx-injection/PLAN.md`](../tx-injection/PLAN.md) |
| T27 | Mempool partitioning via network control | D | kp | done | Topology/partition control | - | - | TODO | net-cluster partitioning + EU-isolate fixtures |
| T28 | Honeypot contract creating transaction races | E |  | not started | - | - | Needs real contracts/capital | TODO |  |
| T29 | Exploit implementation differences between node versions | B |  | not started | - | Cross-version divergence (live nodes) | Cross-version divergence (live nodes) | TODO |  |
| T30 | Coordinate governance attacks to prevent hard fork | G |  | not started | - | - | Needs real governance state | TODO |  |
| T31 | Honest demand exceeds SPO storage capabilities | D |  | not started | Capacity/growth modelling | Load testing | - | TODO | tx-injection sustained flood — see [`../tx-injection/PLAN.md`](../tx-injection/PLAN.md) |
| T32 | Silently accumulate BLS keys to forge certificate | F |  | not started | Adaptive crypto corruption | - | - | TODO |  |
| T33 | Silently accumulate VRF keys for eligibility | F |  | not started | Adaptive crypto corruption | - | - | TODO |  |
| T100 | Zero-sized EB | B | prc | in progress | - | - | - | Demonstrated, crashes TestNet | A bug, which must be fixed/routinely tested |
| T101 | Parallel EB validation | A | shd | not started | Mechanism | Block production | Block production | Block production | Seems to be fixed in new protocol, no action is possible then |
| T102 | EB announcement slot-skew (timing) | A | cet | in progress | - | Fabricate announcement with skewed header slot (future / far-past / age≤L); fake validation accepts | Real Δhdr / age≤L timing gates on Haskell nodes | Real node timing rejection | BT `announce-slot-skew`; pre-gate window (epic 8jo). Non-forging (fabricated header, no election win). |
| T103 | EB announcement flood (junk-message DoS) | D | cet | in progress | - | Flood fabricated announcements per tick (junk-message DoS on ~10k elections × ~50 peers) | Real junk-detection + ≤2-per-election server discipline | Real relay accounting under flood | BT `announce-flood`; pre-gate window (epic 8jo). Non-forging. |
| T104 | Malformed cert/inline RB (cert-XOR-inline violation) | B | cet | not started | - | Forge RB violating cert-XOR-inline (cert + inline txs; or cert with certified_eb=false); fake validation accepts → soundness gap | Real node rejects invalid block (needs forging pool) | Real rejection (blocked on forging) | Should be rejected as invalid. Producer-gated (block forging). Overlaps T11/T14/T19. Bead `zts`. |
