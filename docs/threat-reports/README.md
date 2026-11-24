# Leios Red Team — Threat Reports

Public archive of the Cardano Leios Red Team's threat reports: per-threat
investigations, penetration-test findings, and adversarial-campaign results
against the [Ouroboros Leios](https://github.com/input-output-hk/ouroboros-leios)
protocol and its prototype implementations.

Threat identifiers (T1–T34) refer to the
[Leios threat model](https://github.com/input-output-hk/ouroboros-leios/blob/main/docs/threat-model.md).
The consolidated assessment of all threats is the
[threat-model summary](https://github.com/input-output-hk/ouroboros-leios/blob/main/docs/threat-model-summary.md)
in the ouroboros-leios repository.

## Report index

| Threat | Report |
|---|---|
| T5–T10 (per-threat notes) | [`docs/t5-t10-redteam.md`](docs/t5-t10-redteam.md) |
| T10 — Decline to vote | [`t10/t10.md`](t10/t10.md) (data: [`t10/README.md`](t10/README.md)) |
| T11 — Create invalid EB | [`t11/report.md`](t11/report.md); family design [`eb-attacks/PLAN.md`](eb-attacks/PLAN.md), [`eb-attacks/MALFORMED-EB-PLAN.md`](eb-attacks/MALFORMED-EB-PLAN.md) |
| T12 — Create invalid vote | [`voting/vote-accumulation/report.md`](voting/vote-accumulation/report.md) (design: [`plan.md`](voting/vote-accumulation/plan.md)) |
| T13 — Reference invalid transactions in EB | [`eb-attacks/dummy-tx-eb/report.md`](eb-attacks/dummy-tx-eb/report.md) |
| T14 — Include invalid certificate in RB | [`t14/README.md`](t14/README.md) (plan: [`t14/test-plan.md`](t14/test-plan.md), results: [`t14/wrong-len-cert-results.md`](t14/wrong-len-cert-results.md)) |
| T15 — Forge certificate without quorum | [`t15/single-bls-cert.md`](t15/single-bls-cert.md) |
| T17–T21 (per-threat notes) | [`docs/t17-t21-redteam.md`](docs/t17-t21-redteam.md) |
| T19/T20 cert-suppression & withholding | [`attack-results.md`](attack-results.md) |
| T20 — Withhold announced EB / endorsed txs | [`t20/T20-test-results.md`](t20/T20-test-results.md) (how-to: [`t20/T20-howto.md`](t20/T20-howto.md)) |
| T22–T26 (per-threat notes) | [`docs/t22-t26-redteam.md`](docs/t22-t26-redteam.md) |
| T23 — Withhold then release EBs | [`t23/report.md`](t23/report.md) (design: [`t23/plan.md`](t23/plan.md)) |
| T24 — Submit duplicate transactions | [`t24/README.md`](t24/README.md) |
| T27 — Mempool partitioning via network control | [`t27/t27.md`](t27/t27.md) (final report; per-run reports in [`t27/`](t27/), live-network study in [`t27a/tx-flooding.md`](t27a/tx-flooding.md)) |
| TX-injection family (T24/T25/T26/T31 tooling & method) | [`tx-injection/PLAN.md`](tx-injection/PLAN.md), [`tx-injection/local-node-injection.md`](tx-injection/local-node-injection.md), [`tx-injection/COOP-ATTACK-PLAN.md`](tx-injection/COOP-ATTACK-PLAN.md) |
| Coordinated live T27 flood runbook | [`t20/T27-coordinated-tx-flood.md`](t20/T27-coordinated-tx-flood.md) |
| Agent-driven combined-behaviour campaign | [`campaign-2026-09-20/README.md`](campaign-2026-09-20/README.md) (results: [`results.md`](campaign-2026-09-20/results.md)) |
| Early tx-flooding study | [`reports/tx-flooding.md`](reports/tx-flooding.md) |
| Threat taxonomy & coverage | [`docs/threat-types.md`](docs/threat-types.md), [`docs/threat-coverage.md`](docs/threat-coverage.md), [`docs/environment-model.md`](docs/environment-model.md) |
| T100+ (novel attack variants) | [`docs/t100-199-redteam.md`](docs/t100-199-redteam.md) |
| EB announcement research note | [`docs/eb-announcement.md`](docs/eb-announcement.md) |

## Scope notes

- These are **reports of what the Red Team observed**, at the prototype revisions
  and configurations each report identifies. Negative findings are limited to the
  exercised cases and conditions; see each report's limitations section.
- The **attack tooling itself is not published**. Reports reference internal
  harnesses, scripts and keys by name for provenance; the code and key material
  remain private.
- Raw evidence bundles (logs, tarballs) and raw simulation data are retained
  internally and are not part of this archive; report figures and tables are
  derived from them.

## Environment legend

Reports refer to these testing environments (see
[`docs/environment-model.md`](docs/environment-model.md) for the full model):

- **Simulation** — `sim-rs` discrete-event Leios simulator (this repo).
- **net-cluster** — local multi-node clusters of our adversarial Rust nodes
  (`net-node`) over real mini-protocols (internal harness).
- **proto-devnet** — local mixed devnet: adversarial Rust nodes against real
  Haskell Leios nodes (internal harness).
- **Testnet (Musashi)** — the shared Leios testnet: real Haskell nodes and
  ledger, adversarial nodes hosted remotely.

## License

Content in this directory is part of the `leios-tools` repository and licensed
under the repository's Apache 2.0 license.
