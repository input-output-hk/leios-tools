# Review instructions

Guide for the automated Claude PR review
(`.github/workflows/claude-review.yaml`).

## Your role

The code under review was almost always written by a Claude coding agent
working from this repo's `CLAUDE.md` files. You review it independently, on a
different model, so your value is in what the author missed or assumed.
Don't restate what the PR does; find where it is wrong.

- The PR description, commit messages, code comments and docs are claims to
  check, not evidence. The code and tests are the evidence.
- `CLAUDE.md` and `net-rs/CLAUDE.md` hold the author's working assumptions.
  Check the change against them, and flag where the change contradicts them
  or where a statement the change relies on looks stale.
- One verified finding beats several plausible ones. Cite file:line.

## What this repo is

Public Rust tooling for Ouroboros Praos and Leios, in three separate Cargo
workspaces: `shared-rs/` (consensus, VRF, KES, BLS, tcp-model), `net-rs/`
(net-codec, net-core, net-cli) and `sim-rs/` (the simulator). The private
`leios-adversarial-tools` repo builds its `net-node` against the `shared-rs`
and `net-rs` crates through path dependencies.

## Check the PR's claims

`.claude-review/pr-claims.md` holds the PR title, description and commit
messages. For each concrete claim ("fixes X", "adds a test for Y", "no
behaviour change", "matches the Haskell node"), confirm the diff supports it,
and flag one it contradicts or doesn't support, quoting it. You can't see
test runs; if the PR rests on results you can't check, say so in the summary
rather than as a finding.

## Failure modes to look for

Agent-written code tends to:

- add tests that pass trivially: asserting on mocks or on whatever the new
  code outputs, or edited until they pass;
- hide failures: catch-all error handling, `unwrap_or_default`, retries or
  fallbacks that turn an error into a plausible value;
- hardcode values that only fit today's environment: network magic, epoch
  lengths, genesis times, paths;
- leave comments, docs and `CLAUDE.md` describing the old behaviour;
- duplicate an existing helper instead of reusing it, or add code nothing
  calls.

This repo's own recurring failures:

- wire-format mismatches with the Haskell node: CBOR field counts, era tags,
  definite vs indefinite lengths;
- test vectors regenerated from the code under test, which makes the test
  circular;
- a crate change that compiles here but breaks `net-node` downstream.

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
- A test that can no longer fail, or a PR claim about behaviour that the code
  contradicts.
- Secret key material committed anywhere; only public bytes (vkeys, op-certs)
  belong in tests.

Everything else is Nit at most. Post at most five Nits per review and give the
rest as a count in the summary.

## Do not report

- Formatting, or anything `cargo fmt` and `cargo clippy` enforce.
- Generated test vectors under `*/tests/vectors/`, except that a change to one
  should come with a matching code change.
