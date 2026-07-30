<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/001-behavior-tree-engine/plan.md`
<!-- SPECKIT END -->

## Workspace layout (two separate Cargo workspaces)

This repo has **two independent Cargo workspaces**, not one:

- **`shared-rs/`** — members `consensus`, `vrf`, `kes`, `bls`, `tcp-model`
  (crate names `shared-consensus`, `shared-vrf`, …).
- **`net-rs/`** — members `net-codec`, `net-core`, `net-cli`. Has its own
  detailed `net-rs/CLAUDE.md` — read it when working there.

**Test-command gotcha:** `shared-rs` crates are **not** members of the `net-rs`
workspace, so `cargo test -p shared-consensus` from `net-rs/` **fails** with
"requires dev-dependencies and is not a member of the workspace". Test them in
their own dir:

```sh
cd shared-rs/consensus && cargo test    # NOT: cargo test -p shared-consensus from net-rs
```

## Downstream consumer coupling

`net-node` (aka **Piranha**) in the sibling **`leios-adversarial-tools`** repo
consumes `shared-consensus`, `shared-vrf`, `shared-kes`, `shared-bls`,
`net-codec`, and `net-core` from this checkout via **path deps**
(`../../../leios-tools/...`). Consequences:

- Editing these crates **directly changes that node's build.** A change that
  compiles here can break the node; when in doubt, also build
  `net-node` against your edit.
- The block-production effort keeps both repos on the branch
  **`prc/block-production`** (this side is PR #77). Keep this checkout on that
  branch so the node builds.
- After editing a crate, a downstream `cargo` build may reuse a **stale**
  artifact — `touch` the changed file if a rebuilt binary doesn't reflect the
  change.
- **Never commit testnet secret keys here** — only public bytes (vkeys,
  op-cert) belong in committed tests. The testnet pool's key material lives in
  the adversarial repo's `keys/` (committed there deliberately so its forge
  test runs out-of-the-box; treat as secrets regardless).
