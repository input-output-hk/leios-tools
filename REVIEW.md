# Review instructions

Rules for the automated Claude PR review
(`.github/workflows/claude-review.yaml`). Project context is in `CLAUDE.md`
and `net-rs/CLAUDE.md`.

## What this repo is

Public Rust tooling for Ouroboros Praos and Leios, in three separate Cargo
workspaces: `shared-rs/` (consensus, VRF, KES, BLS, tcp-model), `net-rs/`
(net-codec, net-core, net-cli) and `sim-rs/` (the simulator). The private
`leios-adversarial-tools` repo builds its `net-node` against the `shared-rs`
and `net-rs` crates through path dependencies.

## What Important means here

- Wire-format errors: CBOR that doesn't match the Cardano CDDL or what the
  Haskell node accepts, wrong era tags, wrong field counts or lengths.
- Cryptography in `shared-rs` that accepts invalid input or mishandles keys:
  VRF, KES and BLS verification, domain separation, key material.
- Consensus arithmetic: slot, epoch and KES-period math, leader thresholds,
  chain selection, integer overflow.
- Panics or unbounded allocation on peer-supplied input in `net-codec` or
  `net-core`, which parse untrusted network data.
- Public API changes in `shared-rs`, `net-codec` or `net-core` that would break
  `net-node` downstream without saying so in the PR.
- Secret key material committed anywhere; only public bytes (vkeys, op-certs)
  belong in tests.

Everything else is Nit at most. Post at most five Nits per review and give the
rest as a count in the summary.

## Do not report

- Formatting, or anything `cargo fmt` and `cargo clippy` enforce.
- Generated test vectors under `*/tests/vectors/`, except that a change to one
  should come with a matching code change.
