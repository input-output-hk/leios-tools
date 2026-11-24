# T11 — Create invalid EB

**Status:** prelim. Campaign 2026-11-23, 13 fresh-devnet runs across 5 variants.
**Target:** 3 Haskell block-producing nodes, cardano-node 11.2.0.164 (rc-2026w43
bundle, git 65dd69d1), holding 80% of stake, on the local proto-devnet.
**Attacker:** net-rs (node-0), a 20%-stake pool that announces the invalid
endorser blocks (EBs) on the ranking blocks (RBs) it forges.
**Threat (threat model T11):** "Create invalid EB". Effect: lower throughput,
resource waste. Resources: stake for block production. Mitigation: reduced
rewards, validate before forward.
**Harness:** `make t11-eb-test` / `make t11-campaign`
(`proto-devnet/scripts/t11-eb-test.sh`, `t11-campaign.sh`, `t11_report.py`,
`test_t11_report.py`).

---

## 1. Finding

On w43, every invalid EB net-rs produced was stopped before it could be
endorsed, at one of three stages, and nothing net-rs sent crashed a Haskell node
or split the honest chain. Across all 13 runs no attack EB was validated, voted
or certified; no fatal error or crash occurred; no link between two honest nodes
was torn down; and the three nodes finished on the same tip.

Where each invalid EB is rejected, on w43:

| Variant | Stage | Reason logged | Cost before rejection |
|---|---|---|---|
| `duplicate` | body arrival | `MsgLeiosBlock duplicate tx hashes: [...]` | 1 body fetch; 1 connection drop |
| `huge` (100k entries) | announcement | `LeiosEbTooBig` | no body fetch; ~4–5 drops |
| `oversized` (13,889 entries) | announcement | `LeiosEbTooBig` | no body fetch; ~3–4 drops |
| `hollow`, declared ≥ limit | announcement | `LeiosEbTooBig` | no body fetch; ~3–4 drops |
| `hollow`, declared < limit | decode | `LeiosEb: item count 0 not in [1, N]` | 1 body fetch; ~3–4 drops |
| `malformed` garbage/zero-body/bad-type | decode | `DeserialiseFailure "expected map len"` | 1 body fetch; 1 drop |
| `malformed` truncate | — (fetch stalls) | `ExceededTimeLimit` or none | re-requests; no clean reject |

Legend: *stage* — where the honest node rejects the EB; *announcement* = the RB
header carrying the EB commitment, checked by the Praos header validator before
any Leios body is fetched; *decode* = the LeiosFetch body CBOR parse; *body
arrival* = the post-decode duplicate-key check in `processLeiosBlock`; *drops* =
`Net.Mux.Remote.ExceptionExit` teardowns of the net-rs connection, per node × EB;
*limit* = the devnet genesis `maxEndorserBlockReferencesSize` (100,000 B).

Verdicts (each a fresh devnet; three per variant is the smoke-integrity bar):

| Variant | Runs | PASS | Non-PASS (reason) |
|---|---|---|---|
| `duplicate` | 3 | 2 | 1 INCONCLUSIVE (pre-harness run, growth unmeasured) |
| `huge` | 3 | 2 | 1 INCONCLUSIVE (only 2 EBs reached a node; gate ≥ 3) |
| `oversized` | 3 | 2 | 1 FAIL (growth 0.49× baseline; §5.4, host-load-confounded) |
| `hollow` | 3 | 3 | — |
| `malformed` | 1 | 0 | 1 INCONCLUSIVE (truncate stalls; the other 6 sub-phases reject cleanly) |

Every non-PASS verdict is a sampling or measurement artifact, not a harm: no run
showed an attack EB endorsed, a crash, an honest-link teardown or honest-tip
disagreement. The one FAIL (oversized run 3) is a chain-growth dip that
coincided with the run's host-load peak and did not reproduce in the other two
oversized runs (§5.4); attribution to the attack is unconfirmed and the honest
reading is host-load noise.

**What this establishes on w43.** The size and item limits that on w34 were
enforced late (at the LeiosFetch byte gate, or not at all until a 13,888-entry
vector overflowed while serving) now sit in the header check
(`LeiosEbTooBig`/`LeiosEbTxsTooBig`) and in the derived fetch-buffer bound, both
reached before any body transfer. The w34 oversized-EB crash
([`eb-attacks/oversized-eb/report.md`](../eb-attacks/oversized-eb/report.md))
does not reproduce. The post-decode duplicate-key check
(`LeiosDemoLogic.hs:870`, commit fe727a3e3) rejects a repeated transaction with
an explicit reason.

**What is not established.** Cryptographic-stage rejection (bad VRF or body
signature) is never reached, because net-rs's EBs are stopped earlier. The
`malformed` truncate case stalls the fetch rather than rejecting cleanly, and its
resource cost under a sustained flood is unmeasured (bead
`leios_adversarial_tools-bi9.6`). All runs ran under host load 28–76 from
unrelated processes, so the chain-growth measurement is noisy (§5.4, §8).

### August 27 Trojan Horse result on Musashi

On August 27, 2026, the Red Team demonstrated a transaction-validation gap in
the Leios prototype on the Musashi testnet. The ledger recorded the transfer in
block **80,676**. The team deliberately limited the transfer to **1,000 test
ADA** to minimize the test’s impact.

From Christopher Tilt’s August 27 Red Team update at 1:48 p.m. (excerpt):

> Red1 (PRI0) was able to “steal” 1000 Ada from IOG1 today via the EB Trojan
> Horse attack. I chose 1000 Ada so as not to do any real damage and not have to
> scramble to give it back. We could have taken it all :elmofire: but there was
> no point in being really Red and nasty. This is not a serious exploit - it
> just demonstrates the current state of Leios that it doesn’t yet validate
> TXs when they arrive via an EB. @sebastian already has a PR under development
> (or maybe even review now) that will address this current gap. Once that PR
> lands in a release, we can re-run this attack to verify it’s closed. It’s
> more like a test-driven development than any real exploit. Nick and Sebastian
> already saw this coming.
>
> [Operational details omitted.]
>
> So, this is a big milestone for us - being a real forging BP that votes, etc.
> It’s default mode is honest now - it votes and certifies. [Operational details
> omitted.] For the curious, it was block #80676 that contains a single
> transaction.

Here, EB means endorser block, TX means transaction, BP means block producer,
and PR means pull request. The comment about a larger transfer reflects the
operator’s assessment.

In the September 17 local retest, the committee fetched the transactions,
checked their spending authorization, and withheld its votes because the
required signatures were missing. **Zero of 129 unauthorized transactions
settled.** The September 17 run record also describes a Musashi attempt in
which a relay failure prevented the transactions from reaching the committee.
That attempt was inconclusive for validation.

## 2. Threat and scope

T11 is the threat-model entry "Create invalid EB": a block producer, which needs
stake to win the EB lottery, announces an endorser block that honest nodes must
not endorse. The question the threat model poses is **at which stage** the
invalid EB is rejected and **how much work** an honest node does before it is,
since an EB rejected at CBOR decode wastes far less than one that reaches
cryptographic validation ([`docs/t11-t16-redteam.md`](../docs/t11-t16-redteam.md)).

The w43 campaign covered five variants of invalid endorser blocks.

| Variant | What makes the EB invalid |
|---|---|
| `duplicate` (historical) | the manifest named one valid transaction twice |
| `huge` | a phantom manifest of 100,000 entries (~3.8 MB), above the 2.5 MB LeiosFetch message limit |
| `oversized` | a phantom manifest of 13,889 entries (~514 KB), above the devnet's `maxEndorserBlockReferencesSize` (100,000 B) but under the message limit |
| `hollow` | an empty manifest (1-byte body) announced with a false, larger `eb_size` |
| `malformed` | a manifest whose CBOR is corrupted (garbage, zero-body, bad-type), announced under the hash of the corrupted bytes or of the clean bytes |

The `duplicate` variant was first filed as a T24 experiment (duplicate
transactions). Because duplicates inside an EB need EB-producer stake and make
the EB itself invalid, it belongs here, not under the client-side T24 threat;
see [`t24/README.md`](../t24/README.md).

Out of scope for the November w43 campaign:
- Client-side submission of duplicate, invalid or conflicting transactions over
  TxSubmission (T24/T25/T26).
- Cryptographic-stage invalidity (bad VRF or body signature): net-rs's EBs are
  rejected earlier, at the announcement or at decode, so the crypto stage is not
  reached (§Open items). Planned as `invalid-crypto-eb`
  ([`eb-attacks/MALFORMED-EB-PLAN.md`](../eb-attacks/MALFORMED-EB-PLAN.md) step 3).
- A size lie with a non-empty body under the header limit (bead
  `leios_adversarial_tools-bi9.5`), and the `malformed` truncate stall (bead
  `leios_adversarial_tools-bi9.6`).
- Shared testnets and sustained denial-of-service measurement.

## 3. Method

### 3.1 One run

The current harness uses a **fresh** devnet for each run, in four phases.
The historical `duplicate` runs predate the current build.

1. **Reset and bring up.** `make reset` wipes to a clean genesis; `make up`
   starts 3 Haskell nodes (node1–3) plus net-rs as a 4th pool with the standard
   80/20 stake split. The genesis is not otherwise changed. A run refuses to
   wipe a devnet someone else is using unless `T11_WIPE=1`.
2. **Baseline.** `T11_BASELINE` seconds (default 600) with net-rs honest. This
   is the per-run control: chain-growth rate, per-node memory and CPU, and
   connection teardowns are all measured here first, on the same binaries and
   the same host load, before any attack.
3. **Attack.** The variant is armed through the net-cluster control API. Each
   sub-phase (one declared size, one corruption, …) is armed on its own, after
   stopping the previous one, and runs until `T11_EBS_PER_PHASE` attack EBs
   carrying that sub-phase's parameters appear in net-rs's log, or
   `T11_PHASE_TIMEOUT` elapses. The harness disarms the variant at the end.
4. **Recovery.** `T11_RECOVERY` seconds (default 300) with net-rs honest again,
   to confirm the mesh re-forms and the tips re-converge.

Every `T11_POLL` seconds (default 30) a sampler records each node's tip, memory
and CPU, and the host load, to `timeline.jsonl`. At the end the run records
whether the three tips agree and whether all processes are alive, copies every
node log, net-rs's log and config into `evidence/` (gzipped), and writes the
report. The Mac is kept awake for the run (`caffeinate`).

### 3.2 Why net-rs is the only source of the invalid EB

net-rs produces the invalid EBs on the RBs it forges, so reach is bounded by its
20% RB-lottery share. The honest Haskell nodes never produce one. In the
historical `duplicate` runs, transaction gossip and ranking-block transaction
inclusion were disabled. The repeated transactions were offered to the Haskell
nodes only inside net-rs endorser blocks.

### 3.3 Verdict rules (`t11_report.py`)

For each attack EB the tool records, per node, the outcome: `endorsed`
(EbValidated, a vote scheduled, or certified), `rejected` (the node dropped its
connection to net-rs with an exception naming that EB), `body-rejected`
(FetchBodyArrival with every byte invalid), `body-accepted` (the body was
accepted on arrival), `not-voted`, `undecided` (a body offer, request, or
wire receipt has no recorded processing outcome), `unoffered` (the node heard
of the EB only from honest peers that had rejected it, so no peer offered it
the body) or `not-reached`.

Each connection teardown (`Net.Mux.Remote.ExceptionExit`) is tied to an attack
EB by, in order: the EB hash or the repeated-id list in its exception text; the
slot a header error names (net-rs announces each EB at the slot of its own RB);
or the EB most recently fetched or announced on that connection within two
minutes. A teardown between two honest nodes is distinguished from one on a link
to net-rs.

- **FAIL** if an honest node crashes or logs a fatal error; an attack EB is
  validated, voted or certified; an attack EB tears down a link between two
  honest nodes; an invalid body is accepted on arrival; the chain grows at under
  half its baseline rate over a window long enough to expect ≥ 10 blocks; the
  tips disagree after recovery; honest memory grows past the limit; or
  (`duplicate`) a repeated transaction id lands on the ledger.
- **INCONCLUSIVE** if the attack is not confirmed, too few attack EBs are
  fetched or rejected by an honest node, a body offer, request, or wire receipt
  has no recorded processing outcome, a sub-phase produces no reaching EB, a
  rejection carries the wrong reason for the variant, or evidence is missing.
- **PASS** otherwise.

Exit 0 PASS, 1 FAIL, 2 INCONCLUSIVE. Through `make` any non-PASS exits non-zero;
read the verdict line or the JSON `status`.

## 4. How to run it manually (no AI control)

Run the commands in this section from `proto-devnet/`. The harness resets
the devnet before each run.

### 4.1 Single-run output and offline reporting

The current harness supports `huge`, `oversized`, `hollow`, and `malformed`.
Requests for the historical `duplicate` variant exit before touching the devnet.
The report tool still supports analysis of the saved duplicate evidence.
The following command lists the options for reviewing saved logs.

```bash
python3 scripts/t11_report.py report --help
```

A devnet run prints its phases, writes `~/leios-t11/<variant>-<timestamp>/report.txt`
(and `.json`, `timeline.jsonl`, `evidence/`), and exits 0/1/2. Add `T11_WIPE=1`
if a devnet is already running and you mean to wipe it.

### 4.2 Three runs of several variants

```bash
make t11-campaign                                   # huge, oversized, hollow x3 each
make t11-campaign T11_VARIANTS="malformed" T11_RUNS=1
```

The campaign runs each variant `T11_RUNS` times, round-robin, each on a fresh
devnet, and writes `summary.txt` (one line per run) under
`~/leios-t11/campaign-<timestamp>/`.

### 4.3 Knobs

| Variable | Default | Meaning |
|---|---|---|
| `T11_VARIANT` | — | which variant (required for `t11-eb-test`) |
| `T11_BASELINE` / `T11_RECOVERY` | 600 / 300 | baseline and recovery seconds |
| `T11_POLL` | 30 | sampler interval (seconds) |
| `T11_EBS_PER_PHASE` | 5 / 3 / 2 | attack EBs to collect per sub-phase (huge, oversized / hollow / malformed) |
| `T11_PHASE_TIMEOUT` | 1200 | seconds to wait per sub-phase |
| `T11_N_TXS` | 100000 / 13889 | manifest entries (huge / oversized) |
| `T11_DECLARED` | `50000 2000000 2000000000` | hollow declared sizes (one sub-phase each) |
| `T11_MALFORMED` | garbage/zero-body/bad-type × both hash modes | malformed sub-phases |
| `T11_MIN_EBS` | 3 | attack EBs that must reach a node for a verdict |
| `T11_OUT` | `~/leios-t11/<variant>-<ts>/` | output directory |
| `T11_KEEP` | 0 | 1 = leave the devnet up at the end |
| `T11_WIPE` | 0 | 1 = allow wiping a running devnet |

### 4.4 Reading the report

The report lists, per attack EB, each node's outcome and the time from the body
request to the connection drop; the rejection reasons; the teardowns per phase
split by honest/adversary link; chain growth against baseline; per-node memory
and CPU; and the final tip agreement. Every table has a legend. The verdict is
the last line.

### 4.5 Inspect the traces by hand

```bash
D=~/leios-t11/<variant>-<timestamp>/evidence
# net-rs: each invalid EB it announced
gzcat $D/netrs-node-0.log.gz | sed -E 's/\x1b\[[0-9;]*m//g' | grep -a 'behaviour: announcing'
# one EB's fate on one node (16-hex prefix from the line above)
gzcat $D/node1.log.gz | grep -a '<ebhash-prefix>'
# the rejection reason
gzcat $D/node1.log.gz | grep -a 'Net.Mux.Remote.ExceptionExit' | \
  python3 -c 'import sys,json;[print(json.loads(l)["data"]["event"]["exception"][:120]) for l in sys.stdin]'
# a crash would show here
gzcat $D/node1.log.gz | grep -a -E 'ExceptionInLinkedThread|DiffusionErrored|Leios\.hs:[0-9]'
```

## 5. Results

### 5.1 Every run

Campaign `~/leios-t11/campaign-20261123/`, 2026-11-23 UTC, adversarial-tools
836806c5–579b2eb2, leios-tools b6fcb51, cardano-node 11.2.0.164 (w43). The 12
saved campaign runs were checked against `report.rescored.json` with the updated
`t11_report.py`. The earlier duplicate row uses the recorded pre-harness result.

| Run | Verdict | EBs (reached) | Outcomes | Reason classes | Drops / pair | Growth × base | Tips | Alive |
|---|---|---|---|---|---|---|---|---|
| duplicate 2026-11-22¹ | INCONCLUSIVE² | 10 (10) | rejected 30 | duplicate-tx-hashes 30 | 1.0 | n/a | ✓ | 3/3 |
| duplicate run 1 | PASS | 13 (13) | rejected 39 | duplicate-tx-hashes 39 | 1.0 | 1.45 | ✓ | 3/3 |
| duplicate run 2 | PASS | 13 (13) | rejected 38, unoffered 1 | duplicate-tx-hashes 38 | 1.0 | 1.09 | ✓ | 3/3 |
| huge run 1 | PASS | 4 (4) | rejected 12 | eb-too-big 36, announcement-invalid 12 | 4.0 | 1.09 | ✓ | 3/3 |
| huge run 2 | PASS | 5 (5) | rejected 15 | eb-too-big 48, announcement-invalid 15 | 4.2 | 0.72 | ✓ | 3/3 |
| huge run 3 | INCONCLUSIVE | 2 (2) | rejected 6 | eb-too-big 27, announcement-invalid 6 | 5.5 | 0.99 | ✓ | 3/3 |
| oversized run 1 | PASS | 5 (5) | rejected 15 | eb-too-big 30, announcement-invalid 15 | 3.0 | 1.30 | ✓ | 3/3 |
| oversized run 2 | PASS | 4 (4) | rejected 12 | eb-too-big 29, announcement-invalid 12 | 3.4 | 0.96 | ✓ | 3/3 |
| oversized run 3 | FAIL | 5 (5) | rejected 15 | eb-too-big 43, announcement-invalid 15 | 3.9 | **0.49** | ✓ | 3/3 |
| hollow run 1 | PASS | 9 (9) | rejected 27 | eb-too-big 79, announcement-invalid 18, decode-item-count 8, exceeded-time-limit 1 | 3.9 | 1.98 | ✓ | 3/3 |
| hollow run 2 | PASS | 9 (9) | rejected 27 | eb-too-big 96, announcement-invalid 18, decode-item-count 9 | 4.6 | 0.96 | ✓ | 3/3 |
| hollow run 3 | PASS | 9 (9) | rejected 27 | eb-too-big 53, announcement-invalid 16, decode-item-count 9 | 2.9 | 1.05 | ✓ | 3/3 |
| malformed run 1 | INCONCLUSIVE | 17 (13) | rejected 39, undecided 12 | decode-failure 36, exceeded-time-limit 3 | 1.0 | 1.62 | ✓ | 3/3 |

Legend: *EBs (reached)* = attack EBs net-rs produced, and how many some honest
node fetched or rejected; *Outcomes* = per node × EB (3 nodes), the tool's
classification (§3.3); *Reason classes* = every connection-teardown exception,
counted (an announcement-stage rejection re-fires on each reconnect, so counts
exceed the EB count); *Drops / pair* = mean teardowns per rejected node × EB;
*Growth × base* = attack-phase chain-growth rate over the baseline rate (judged
only when the baseline predicts ≥ 10 attack blocks); *Tips* = the three nodes
agree after recovery; *Alive* = node processes up at the end. ¹ the earlier
`dup-eb-test` run, re-read here. ² The pre-harness run logged no growth samples.
Its rejection and ledger observations are included in §5.2.

### 5.2 Duplicate (3 runs, all reject at body arrival)

Across the three runs net-rs produced 36 EBs that repeated a transaction id;
every one was rejected by all three nodes with the explicit
`MsgLeiosBlock duplicate tx hashes: [...]` exception, 108 of 108 node × EB pairs,
one connection drop each. In every case the id list in the exception equalled the
repeated-id list net-rs logged for that EB. No repeated transaction id reached
the ledger in any run (0 of 600 pair ids landed, each run), and 595–599 of 600
pairs rode a single EB (an EB boundary split the rest; a split pair's two single
copies are valid and land at most once). The check is in `ouroboros-consensus`,
`LeiosDemoLogic.hs:870` (processLeiosBlock, commit fe727a3e3, 2026-08-10): the
decoder keeps the repeated map keys and a post-decode check rejects them.

Duplicate run 2 had one `unoffered` node × EB pair: node3's link to net-rs was
mid-reconnect when that EB was announced, so node3 heard of the EB only from
node1 and node2 — which had already rejected it and therefore never offered its
body — and had nothing of its own to judge. The other two nodes rejected that EB
normally.

### 5.3 Huge, oversized, hollow (rejected at the announcement or at decode)

The huge (100,000-entry, ~3.8 MB) and oversized (13,889-entry, ~514 KB)
manifests both exceed the devnet genesis `maxEndorserBlockReferencesSize`
(100,000 B), so the Praos header validator rejects the announcing RB with
`LeiosEbTooBig` when ChainSync delivers its header — before any Leios body is
fetched. net-rs reconnects, re-announces, and is rejected again; this is the
~3–5 drops per node × EB in the table. The honest chain is the Haskell majority
and keeps advancing throughout.

Hollow splits by declared size:
- **declared = 50,000 B** (under the 100,000 B header limit): the announcement
  passes the header check, the body is fetched, and the decoder rejects it with
  `LeiosEb: item count 0 not in [1, N]` — an EB with an empty manifest is invalid
  on w43. So a declared-size lie below the header limit is caught not by a
  size-mismatch check but because the hollow body is empty. A size lie with a
  *non-empty* body under the limit is therefore untested here (bead
  `leios_adversarial_tools-bi9.5`).
- **declared = 2,000,000 and 2,000,000,000 B** (above the limit): rejected at the
  announcement with `LeiosEbTooBig`, like huge/oversized.

### 5.4 Chain growth and host load (the one FAIL)

Oversized run 3 is the only FAIL, on the growth gate: the attack-phase chain grew
at 0.49× the baseline rate, just under the 0.5 floor. Every one of its 5 EBs was
still rejected at the announcement, the tips agreed, all processes lived, and no
honest-to-honest link was torn down. The dip (node1 tip 48→50 over ~5 min) falls
exactly on the run's host-load peak (load 60–76), while the baseline and the fast
recovery that followed (+3, +1, +4, +5 blocks per sample) ran at load ~45.
Oversized runs 1 and 2 showed no stall (growth 1.30, 0.96), and no other variant
stalled. The stall is 1 of 3 oversized runs, coincides with the load peak, and is
most consistent with host-load noise; attribution to the oversized-announcement
churn is **unconfirmed**. Every run of the campaign ran under load 28–76 from
unrelated host processes (cloudd, Firefox, Steam), which makes the growth ratio
the noisiest measurement in the set. A quiet-host re-run is filed as bead
`leios_adversarial_tools-bi9.7`.

### 5.5 Malformed (6 sub-phases reject at decode; truncate stalls)

Malformed run 1 armed all 8 corruption × hash sub-phases. The six structural
corruptions — garbage, zero-body and bad-type, each announced under the hash of
the corrupted bytes and under the hash of the clean bytes — were all rejected at
CBOR decode on all three nodes (36 of 36 node × EB), with
`DeserialiseFailure "expected map len"`, within 1–8 ms of the body request.
Decode rejection fired under both hash modes, so on w43 there is no separate
content-address (hash-gate) stage ahead of the decoder that the malformed-EB plan
posited: a body whose hash matches the commitment and one whose hash does not are
both carried to the decoder and rejected there.

The `truncate` corruption behaves differently and makes the run INCONCLUSIVE: net
-rs serves a body cut to half its length, which breaks the outer `MsgLeiosBlock`
framing, so the honest node's fetch never completes a decodable message. The node
re-requests until it either times out (`ExceededTimeLimit`, one EB) or the window
ends (the 12 `undecided` pairs). No harm resulted (chain grew 1.62× baseline,
tips agreed, no crash), but this is a fetch stall, not a clean rejection, so
`truncate` is dropped from the default malformed set and tracked separately (bead
`leios_adversarial_tools-bi9.6`). The six structural sub-phases are a conclusive
result on their own.

## 6. Evidence

Each run preserved `report.txt` / `.rescored.txt` (+ `.json`), `timeline.jsonl`
and `evidence/` (every node log, net-rs's log and `netrs.toml`, gzipped) under
its run directory. The 2026-11-22 duplicate run's evidence excerpt is
[`evidence/duplicate-run1-excerpts.txt`](evidence/duplicate-run1-excerpts.txt);
its full logs are on the operator's machine under `~/leios-magazines/`.

Representative traces, one per stage (verbatim, trimmed):

```
# body-arrival duplicate rejection (duplicate; node1 log)
Net.Mux.Remote.ExceptionExit  exception="MsgLeiosBlock duplicate tx hashes: [00d51064...,...]"

# announcement rejection (huge/oversized/hollow≥limit; node1 log)
ChainSync.Client.Exception  HeaderError ... LeiosEbTooBig ...
Net.Mux.Remote.ExceptionExit  exception="HeaderError (... SlotNo <ebSlot> ...) ... LeiosEbTooBig ..."

# decode rejection, empty body (hollow<limit; node1 log)
Net.Mux.Remote.ExceptionExit  exception="DecoderFailure (LeiosFetch ...) (DeserialiseFailure 3 \"LeiosEb: item count 0 not in [1, 17142]\")"

# decode rejection, corrupted body (malformed garbage/zero-body/bad-type; node1 log)
Net.Mux.Remote.ExceptionExit  exception="DecoderFailure (LeiosFetch ...) (DeserialiseFailure 2 \"expected map len\")"
```

No `ExceptionInLinkedThread`, `DiffusionErrored` or `Leios.hs:<n>` fatal line
appears in any node log in any run.

## 7. Comparison with the w34 EB-attack reports

The earlier EB-attack reports ([`eb-attacks/`](../eb-attacks/),
2026-09-03) ran the oversized, huge and hollow probes on cardano-node **w34**,
on devnets that had been running for some time, one run each, with the verdict
read by hand. They found the oversized (13,889-entry) EB stored and then
crashing the serving path with `index out of bounds (13888,13888)`, stalling the
chain; the huge EB rejected at the LeiosFetch byte gate; and the hollow EB not
pre-allocated.

On w43 those crash and late-rejection paths are no longer reached:

| Probe | w34 (2026-09-03) | w43 (2026-11-23) |
|---|---|---|
| oversized (13,889 entries) | stored, then `index out of bounds (13888,13888)` crashed the LeiosFetch serving path, stalling the chain | rejected at the announcement (`LeiosEbTooBig`), before any body fetch; no crash |
| huge (over the byte limit) | rejected at the LeiosFetch byte gate (`ExceededSizeLimit`) after net-rs began serving | rejected at the announcement, before any fetch |
| hollow (false declared size) | no pre-allocation; a size above the wire limit tripped a `size mismatch` disconnect after fetching | ≥ limit rejected at the announcement; < limit rejected at decode (empty body) |

The move is explained by two upstream changes to `ouroboros-consensus`
(`origin/leios-prototype`): the EB-size and item limits are now checked in the
Praos header validator (`LeiosEbTooBig` / `LeiosEbTxsTooBig`), and the fetch
buffers are sized from the 2.5 MB LeiosFetch message limit rather than a fixed
13,888-entry vector (commit 20388a17c, 2026-09-09), closing the window the w34
vector overflow lived in. The related T13 tx-decoder crash
([`eb-attacks/dummy-tx-eb/report.md`](../eb-attacks/dummy-tx-eb/report.md),
ouroboros-leios issue #1080) was fixed separately (#2272, 2026-09-17); that fix
is not re-verified here (T13, not T11).

## 8. Open items

1. **One malformed run.** The six structural sub-phases are conclusive, but the
   smoke-integrity bar is three fresh runs; the clean default set (garbage,
   zero-body, bad-type) has not been run 3× (bead `leios_adversarial_tools-bi9.3`).
2. **Host load.** Every run ran under load 28–76 from unrelated processes, which
   makes chain growth noisy and produced the one FAIL (§5.4). A quiet-host re-run
   of the boundary variants is needed before any liveness claim about the
   announcement-reconnect churn (bead `leios_adversarial_tools-bi9.7`).
3. **Cryptographic stage untested.** net-rs's EBs are rejected at the
   announcement, decode or body arrival, so a decodable-but-crypto-invalid EB
   (bad VRF or body signature) — the "maximize wasted validation work" case — is
   never exercised (`invalid-crypto-eb`,
   [`eb-attacks/MALFORMED-EB-PLAN.md`](../eb-attacks/MALFORMED-EB-PLAN.md) step 3).
4. **Size lie with a non-empty body** under the header limit is untested, because
   the hollow body is empty and fails decode first (bead
   `leios_adversarial_tools-bi9.5`).
5. **Truncate fetch stall** is uncharacterised: the re-request loop caused no harm
   here, but its cost under a sustained flood is unmeasured (bead
   `leios_adversarial_tools-bi9.6`).
6. **Reconnect churn.** An announcement-stage rejection drops and re-forms the
   net-rs connection ~3–5 times per EB. It cost no honest liveness in these runs,
   but at higher adversary stake or EB rate the churn would scale; not measured.
7. **Side finding (not T11):** an honest net-rs EB can exceed the devnet genesis
   `maxEndorserBlockTxsSize` (1 MB) under the centrifuge and is then disconnected
   with `ExnLeiosClosureTooBig`; net-rs does not bound its EB closure by the
   protocol parameter (bead `leios_adversarial_tools-45i`).

## 9. Next steps

1. Re-run the boundary variants (huge, oversized, hollow) on a quiet host, 3×
   each, to separate the announcement-reconnect churn from host-load noise in the
   growth gate (bead `leios_adversarial_tools-bi9.7`).
2. Run the clean malformed set (garbage, zero-body, bad-type) 3× for the
   smoke-integrity bar (bead `leios_adversarial_tools-bi9.3`).
3. Build `invalid-crypto-eb` to reach the cryptographic stage, the one T11 stage
   this campaign does not exercise.
4. Add the non-empty-body size-lie and characterise the truncate stall (beads
   `leios_adversarial_tools-bi9.5`, `.6`).
5. Fix net-rs's EB closure bound (bead `leios_adversarial_tools-45i`), unrelated
   to T11 but surfaced by it.
