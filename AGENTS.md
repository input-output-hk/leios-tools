# Agent Instructions

## Scope and Project Context

These instructions apply throughout this repository. Before working in a scoped
area, read its additional instructions, even when the session starts at the
repository root:

- `net-rs/AGENTS.md`: network codecs, protocols, and command-line tools.
- `sim-rs/AGENTS.md`: simulator engines, determinism, and benchmarks.
- `shared-rs/consensus/AGENTS.md`: shared consensus and behavior-tree contracts.

These files adapt the corresponding `CLAUDE.md` guidance for Codex. Keep shared
project rules consistent when updating either version. Historical implementation
phases, test counts, and branch pairings should be checked against current source
and run output before being treated as current requirements.

Read `README.md` for repository boundaries and
`specs/001-behavior-tree-engine/plan.md` for the behavior-tree design context.

## Three Independent Cargo Workspaces

There is no repository-root Cargo workspace. Run Cargo in the relevant directory:

- `shared-rs/`: `consensus`, `vrf`, `kes`, `bls`, and `tcp-model` (crate names
  `shared-consensus`, `shared-vrf`, `shared-kes`, `shared-bls`, `tcp-model`).
- `net-rs/`: `net-codec`, `net-core`, and `net-cli`.
- `sim-rs/`: `sim-core` and `sim-cli`.

The networking and simulator crates depend on shared crates through relative
path dependencies. Preserve this layout and the parameter symlinks into
`data/simulation/`.

From the repository root, use explicit workspace manifests:

```sh
cargo test --manifest-path shared-rs/Cargo.toml --workspace
cargo test --manifest-path net-rs/Cargo.toml --workspace
cargo test --manifest-path sim-rs/Cargo.toml --workspace
```

For just consensus, run `cargo test` from `shared-rs/consensus/`, or use
`cargo test --manifest-path shared-rs/Cargo.toml -p shared-consensus` from the
root. Do not run `cargo test -p shared-consensus` from `net-rs/`: it is a path
dependency there, not a workspace member with runnable development dependencies.

Run the affected workspace's tests, Clippy, and formatting checks for code changes.
For changes to shared APIs, also check their consumers. The build workflow in
`.github/workflows/build.yaml` builds all three workspaces with `--all-targets`;
`.github/workflows/sim-rs.yaml` runs simulator tests. Consult those files for the
current CI toolchain rather than assuming an old version from a handoff.

## Downstream Consumer Coupling

Piranha (`net-node`) in the sibling private `leios-adversarial-tools` repository
consumes this checkout's consensus, cryptography, codec, and network crates via
path dependencies. Editing them directly changes its build.

- A change that compiles here can break the downstream node. Build and test the
  affected consumers when changing shared APIs, if that checkout is available.
  From this root, the downstream node build is
  `cargo build --manifest-path ../leios-adversarial-tools/net-rs/Cargo.toml -p net-node`.
- The normal public integration target is `main`; the old `prc/block-production`
  pairing is superseded. Respect active feature branches and existing work;
  do not switch a checkout merely to follow a historical pairing note.
- If a downstream binary does not reflect a dependency edit, verify its build
  inputs and rebuild. Touch the changed file or clean the affected crate if
  Cargo has reused a stale artifact.
- Track and commit changes separately in each repository. Read the private
  repository's instructions before modifying or operating its tools.
- **Never commit testnet secret keys here.** Only public bytes, such as
  verification keys and operational certificates, belong in committed fixtures.
  The private repository's intentionally committed pool signing keys remain
  secrets; do not copy their contents into this public repository or reports.

The simulator runs without connecting to real networks. The public `net-cli`
can connect to real peers; inspect addresses and network magic before using its
live-network examples. Test nodes, cluster orchestration, the dashboard, and
`.bt` authoring sources belong to the private repository. Generated behavior
TOML configs are runtime artifacts and must not be committed here.

## Working Across Directories

Use the repository root as the workspace when changing several crates. If the
current sandbox does not permit an authorized sibling-repository edit, request
the required tool permission; do not use Claude-specific sandbox flags.
Preserve existing changes and stashes, stage only task-related files, and use
non-interactive file operations so commands cannot hang on confirmation prompts.
