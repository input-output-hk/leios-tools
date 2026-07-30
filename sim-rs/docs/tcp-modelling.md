# TCP / transport modelling in sim-rs

The Rust simulator (`sim-core`) models the transport behaviour of each directed
link with one of **three** regimes. They are dispatched, per link, behind a
single enum — `ConnectionKind` in `sim-core/src/network/connection.rs` — and
selected from configuration by `ConnectionKind::from_config`
(`connection.rs:761`).

> **Path convention.** Unless otherwise noted, source paths below (`sim-core/…`,
> `parameters/…`, bare `connection.rs`/`config.rs`/`tcp.rs`) are relative to the
> `sim-rs/` directory that contains this doc. The exception is `shared-rs/…`,
> which lives at the repository root, alongside `sim-rs/`.

```rust
pub enum ConnectionKind<TProtocol, TMessage> {
    /// Latency + fair-bandwidth-sharing model.
    Simple(Connection<TProtocol, TMessage>),
    /// TCP congestion-window model (slow-start, cwnd, BDP, ACK pipelining).
    Tcp(TcpConnection<TMessage>),
}
```

The analytic "envelope" model is not a third enum variant: it attaches on top of
`Simple` (`ConnectionKind::from_config` → `Connection::with_envelope`).

`tcp-congestion-control` (the congestion-window model; internal bool `use_tcp`)
and `tcp-envelope` are **mutually exclusive** — if `tcp-congestion-control` is
on, any configured envelope is ignored (`debug_assert!` at `connection.rs:768`).

---

## The three models

### 1. Simple — latency + fair bandwidth sharing

- **Source:** `struct Connection`, `sim-core/src/network/connection.rs:57`.
- Models one-way `latency` plus an optional `bandwidth_bps` cap. Bandwidth is
  fair-shared across the active mini-protocol queues
  (`compute_bandwidth_delay`, `connection.rs:313`).
- If a link has **no** `bandwidth-bytes-per-second`, it is **latency-only**
  (instant serialisation) — `Connection::send`, `connection.rs:164`.
- No congestion window or slow-start of its own.
- This is the fallback whenever `tcp-congestion-control` is false and no
  envelope is attached.

### 2. TCP congestion-window model

- **Core algorithm:** `sim-core/src/tcp.rs` — a direct port of the Haskell
  `simulation/src/ModelTCP.hs`.
- **Channel wrapper:** `struct TcpConnection`,
  `sim-core/src/network/tcp_connection.rs:37`.
- Forecasts send timing accounting for serialisation delay, one-way
  propagation, and a slow-start congestion window with idle-reset (RFC 6298
  RTO, `tcp_connection.rs:62`).
  - `SEGMENT_SIZE = 1460`, initial cwnd = 10 segments (RFC 6928),
    `tcp.rs:50`/`tcp.rs:53`.
  - `TcpConnProps::new` auto-sizes the receiver window to `max(2·BDP,
    10·MSS)` so it never constrains throughput (`tcp.rs:39`).
- Does **not** model loss, retransmission, or delayed ACKs (`tcp.rs:6`).
- When this model is on but a link has **no** `bandwidth-bytes-per-second`, it
  falls back to `DEFAULT_TCP_BANDWIDTH = 1024 * 1000` bytes/s (~8 Mbit/s),
  `tcp_connection.rs:33`. Note the asymmetry with the Simple model: an
  unspecified bandwidth becomes a ~8 Mbit cap here, but is uncapped
  (latency-only) under Simple.

### 3. Analytic TCP "envelope" model

- **Separate crate:** `shared-rs/tcp-model/` (declared at
  `sim-core/Cargo.toml:21`: `tcp-model = { path = "../../shared-rs/tcp-model" }`).
- Rather than simulating a TCP state machine, it captures the **effects** of
  three transport events as bandwidth-multiplier curves plus additive latency
  stalls, overlaid on a Simple connection (`shared-rs/tcp-model/src/lib.rs:1`):
  - **Cold start** — a slow-start ramp on first use of a link.
  - **Idle reset** — that ramp re-paid after a silent gap.
  - **Loss** — a head-of-line stall for one RTO, then AIMD-style linear
    bandwidth recovery.
- Wired in via `Connection::with_envelope` / `EnvelopeWiring`
  (`connection.rs:73`).
- Config type `LinkEnvelopeCfg` (`shared-rs/tcp-model/src/config.rs:9`) with two
  factories:
  - `defaults_for(latency, bps)` — derives slow-start depth/release from the
    bandwidth-delay product (`config.rs:28`). Requires `bps > 0` and
    `latency > 0`.
  - `disabled()` — fires no envelopes; byte-for-byte identical to a plain
    Simple connection (`config.rs:85`).

---

## Defaults — is each model on?

| Model | Code (serde) default | Effective default for a normal run |
|---|---|---|
| **2. TCP congestion-window** (`tcp-congestion-control`) | `false` (`config.rs:1681`) | **`true`** — see below |
| **3. TCP envelope** (`tcp-envelope`) | off / `None` (global and per-link) | off |
| **1. Simple** | — (the fallback) | active only when both of the above are off |

**The effective default is `true`, not `false`.** The serde fallback
(`default_tcp_congestion_control() -> false`) rarely applies in practice: runs
layer their parameters (via figment) on top of `parameters/config.default.yaml`,
which ships:

```yaml
tcp-congestion-control: true   # parameters/config.default.yaml:15
```

The JSON schema documents `"default": true` as well
(`parameters/config.schema.json`). So a normal run uses the congestion-window
model unless you explicitly set `tcp-congestion-control: false`. The `false`
code default only bites if you feed a bare parameters file that both omits the
key **and** isn't merged onto the shipped default.

The envelope model is off unless you explicitly add a `tcp-envelope` block
**and** the link has nonzero bandwidth and latency. Even when configured, its
loss component is inert until you raise `loss-prob-per-segment` above its `0.0`
default.

> **No example configs exercise these knobs.** The only real occurrences are
> the `true` in the two shipped defaults — `sim-rs/parameters/config.default.yaml`
> and the byte-identical copy at the repo root, `data/simulation/config.default.yaml`.
> No config under `parameters/`, `test_data/`, or `sim-cli/configs/` sets a
> `tcp-envelope` block or flips the flag — envelope usage lives only in unit
> tests (`connection.rs`, `config.rs` `tcp_envelope_tests`).

---

## Configuration

### `tcp-congestion-control` — model 2 on/off (parameters YAML)

Global boolean, `RawParameters.tcp_congestion_control` (`config.rs:170`),
applied uniformly to **every** link (`config.rs:1588`):

```yaml
tcp-congestion-control: true    # false → use the Simple model
```

Unlike bandwidth and the envelope, this is not per-link.

### Per-link latency / bandwidth — models 1 and 2 inputs (topology YAML)

Producer links use `RawLinkInfo` (`config.rs:761`, kebab-case):

```yaml
producers:
  node-b:
    latency-ms: 12.0
    bandwidth-bytes-per-second: 1000000   # optional; omit for latency-only
```

### `tcp-envelope` — model 3 (two layers)

Struct `RawTcpEnvelope` (`config.rs:779`,
`#[serde(rename_all = "kebab-case", deny_unknown_fields)]`). Every field is an
optional override on top of `LinkEnvelopeCfg::defaults_for(latency, bps)`:

```
loss-prob-per-segment   mss-bytes              initial-cwnd-segments
idle-reset-threshold-ms rto-ms                 loss-bw-depth
cold-bw-depth           cold-release-ms        cold-release-shape
loss-release-ms         loss-release-shape
```

`*-shape` fields take a `Curve`: `step`, `linear`, or `geometric`
(`shared-rs/tcp-model/src/curve.rs:18`).

Two layers, merged in `Topology::from_raw` (`config.rs:978`) as
`defaults_for(latency, bps)` → apply **global** → apply **per-link**:

- **Global** — `RawParameters.tcp_envelope` (`config.rs:385`), applies to every
  link that has a bandwidth set:

  ```yaml
  tcp-envelope:
    loss-prob-per-segment: 0.005
  ```

- **Per-link override** — a `tcp-envelope` block inside a producer entry in the
  topology YAML (`RawLinkInfo.tcp_envelope`, `config.rs:766`):

  ```yaml
  producers:
    node-b:
      latency-ms: 50
      bandwidth-bytes-per-second: 12500000
      tcp-envelope:
        loss-prob-per-segment: 0.001
        cold-release-ms: 200
        cold-release-shape: geometric
  ```

Validation in `RawTcpEnvelope::apply` (`config.rs:809`):
`loss-prob-per-segment` must be in `[0, 1]`; `mss-bytes` must be `> 0`.

> A link with no `bandwidth-bytes-per-second`, or zero latency, gets **no**
> envelope even if a block is configured (`config.rs:981`).

### Internal link config

The resolved per-link struct is `LinkConfiguration` (`config.rs:1731`):
`bandwidth_bps: Option<u64>`, `use_tcp: bool`,
`tcp_envelope: Option<tcp_model::LinkEnvelopeCfg>`. It flows through
`NetworkCoordinator::add_edge` → `EdgeConfig`
(`network/coordinator.rs:55`) → `ConnectionKind::from_config`
(`coordinator.rs:143`).

---

## Reference defaults (factory / `Default` impls)

- `TcpState::default()` → cwnd = IW = 14 600 bytes (`tcp.rs:65`).
- `TcpConnProps::new` → `receiver_window = max(2·BDP, 10·1460)` (`tcp.rs:39`).
- `LinkEnvelopeCfg::defaults_for` — MSS 1460, IW 10 segments,
  `idle_reset_threshold = rto = max(1s, 2·latency)`,
  `loss_prob_per_segment = 0.0`, `loss_bw_depth = 0.5`,
  `cold_release_shape = Geometric`, `loss_release_shape = Linear`
  (`shared-rs/tcp-model/src/config.rs:56`).
- `LinkEnvelopeCfg::disabled` — `idle_reset_threshold = Duration::MAX`, all
  depths 1.0, loss 0.0 (`config.rs:85`).

## Choosing a model

- **Higher fidelity, matches the Haskell sim:** `tcp-congestion-control: true`
  (the shipped default).
- **Lightweight cold-start / idle / loss *effects* over the cheap bandwidth
  model:** leave `tcp-congestion-control: false` and add `tcp-envelope` blocks.
- **Pure latency + bandwidth shaping:** both off.

## Source map

| Concern | Location |
|---|---|
| Model dispatch / enum | `sim-core/src/network/connection.rs:742` |
| Simple connection | `sim-core/src/network/connection.rs:57` |
| Congestion-window algorithm | `sim-core/src/tcp.rs` |
| Congestion-window channel | `sim-core/src/network/tcp_connection.rs` |
| Envelope crate | `shared-rs/tcp-model/src/` |
| Parameter parsing / merge | `sim-core/src/config.rs` |
| Link wiring | `sim-core/src/network/coordinator.rs` |
| Shipped default (`= true`) | `parameters/config.default.yaml:15` |
