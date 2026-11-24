# EB Announcement (`MsgLeiosBlockAnnouncement`) — research note

Research into the Leios **EB announcement** path (CIP-0164): what the message
is, its timing/authorization model, why it is producer-gated, the attack surface
it opens, and what the send-path implementation would take. Read-only research —
no code changes here.

> **Terminology (kept strictly separate).** Two distinct EB signals:
> - **EB announcement** = `MsgLeiosBlockAnnouncement` — carries the RB **header**
>   (with its `announced_eb` field); the fast discovery pulse, bounded by Δhdr.
> - **EB(-body) offer** = `MsgLeiosBlockOffer` — carries `point + eb_size`;
>   fires when the EB **body** is available and triggers `LeiosFetch`.
>
> "EB announcement" always means the first; never reuse it for the offer.

## 1. What the message carries

`MsgLeiosBlockAnnouncement { header: WrappedHeader }` — the payload **is a Praos
RB header**. The Leios content is one field of that header body
([`net-rs/net-codec/src/header.rs`](../../../net-rs/net-codec/src/header.rs) — the codec lives in this
`leios-tools` repo this crate path-depends on):

```rust
announced_eb: Option<([u8; 32], u32)>   // (EB hash, EB size in bytes)
```

That is a **commitment** to an EB — a 32-byte hash + a byte size — **not** the
EB. No tx list, no manifest, no tx hashes. It sits alongside `issuer_vkey`
(field 3), `vrf_vkey` (4), `vrf_result` (5) in the signed header body, wrapped as
`[header_body, body_signature]`.

| Signal | Carries | Delivered by |
|---|---|---|
| **Announcement** (`announced_eb` in header) | EB hash + EB size only | `MsgLeiosBlockAnnouncement` (fast pulse) |
| **EB body** (manifest + tx list — the fake-EB payload) | the actual txs / tx-hash manifest | `MsgLeiosBlockOffer` → `LeiosFetch` |

Legend: **EB** = endorser block; **RB** = ranking block (a Praos block); **tx** =
transaction; **manifest** = the EB's list of tx hashes; **fast pulse** = the
header-first discovery message bounded by **Δhdr** (the CIP-0164 header-diffusion
budget); **`LeiosFetch`** = the pull protocol that fetches an EB body after an
offer.

## 2. Announcement vs. offer — when each happens

The **announcement** (`MsgLeiosBlockAnnouncement`) and the **body offer**
(`MsgLeiosBlockOffer`) look like a sequence but are better understood as
**simultaneous at the source and a race everywhere else**.

**At the producer there is essentially no gap.** When the slot leader wins slot
*s*, `try_produce_block` builds the RB header (carrying `announced_eb`) **and**
the EB body in one shot, so the producer enqueues the announcement (the header)
and the offer (body available) at the same instant. "Which is first" is not
meaningful at the origin.

The difference is **what each one means**, and therefore **how far each can
travel**:

| | Announcement | Offer |
|---|---|---|
| Payload | the RB **header** (`announced_eb = hash + size`) | `point + eb_size` |
| Asserts | "an EB *exists*, committed by this header" | "I **have the EB body** — fetch it from me" |
| Precondition to send | you have the **header** | you have the **body** |
| Triggers on receipt | discovery; starts the voting clock | a `LeiosFetch` pull of the body |

Legend: **announcement** = `MsgLeiosBlockAnnouncement`; **offer** =
`MsgLeiosBlockOffer`; **body** = the EB's transaction manifest + bodies (large);
**header** = the RB header (small); **`point`** = `(slot, hash)` identifying the EB.

**Downstream, the announcement wins the race** — not by an injected delay, but
by that "precondition to send" row. A relay can *announce* the instant it hears
the header (tiny), but it can only *offer* once it has actually **fetched the
body** (large). So the header out-runs body availability hop by hop. That head
start is the announcement's whole purpose: get "EB X is coming" to voters within
the Δhdr budget, so they can fetch and vote before the deadline.

Timeline for one EB at a node several hops from the producer:

```
slot s ── producer wins VRF slot; builds RB header (announced_eb=X) + EB body X
        └─ enqueues announcement(header) AND offer(body) together   ← simultaneous at source

           header (announcement) diffuses fast ─────────►
           body must be fetched hop-by-hop (large)  ─ ─ ─ ─ ─►  (slower)

downstream node:
  t1  ANNOUNCEMENT arrives   "EB X exists"            ← earliest (bounded by Δhdr)
  t2  OFFER arrives          "peer P now has body X"  ← later (body had to reach P first)
  t3  node issues LeiosFetch → pulls body X from P
  t4  node has body → validates → votes    (must land before announced_s + 3·Δhdr + L_vote)
```

**Relation to the RB.** The announcement *is* the RB header — it rides on (is)
the header, so its timing is exactly Praos header-first diffusion. The offer is
about the **EB body**, a distinct large object the header points at by hash,
which diffuses on its own via `LeiosFetch` (pulled after an offer) — in parallel
with the RB's *own* body (pulled via `BlockFetch`):

```
RB header (= the announcement)   ← leads, fast pulse
        ├── RB body   via BlockFetch    ┐ both large bodies,
        └── EB body   via LeiosFetch    ┘ pulled on demand, later, in parallel
              (LeiosFetch is triggered by the offer)
```

**One line:** the announcement and offer are born together at the producer, but
the announcement carries only a header so it out-runs the body-bearing offer
across the network — which is the point, since voters must *learn of* the EB
(and start fetching) well before the body finishes propagating.

## 3. Timing & authorization: why it is your VRF slot (not EB signing)

The announcement is **not a separate object racing the RB — it *is* the RB's
header.** In `net-node/src/production.rs::try_produce_block`:

1. `run_lottery(rb_prob)` — **returns early if you lose the slot.**
2. Only on a win is the header built, `announced_eb` computed from the EB
   manifest, and embedded in the signed header.

So the header — and therefore the announcement — only exists **after** you win
the VRF slot. Consequences:

- **Can it precede the RB?** Depends what "RB" means. Relative to the RB **body**
  (full tx block): yes — the announcement (header) naturally leads the body, like
  Praos header-first diffusion (ChainSync header → BlockFetch body); that head
  start is the point. Relative to the RB **as an authorized object** (the signed
  header itself): no — the announcement *is* that header; nothing exists to send
  before it. There is no headerless announcement in the wire protocol.
- **Why must it be your VRF slot?** Because you are diffusing a **Praos block
  header**, and headers are gated by leader election: the header embeds a VRF
  proof (you won slot *s*) and is KES-signed. A receiver validates VRF + KES +
  slot before accepting. A header for a slot you did not win is rejected.
- **Is the gate EB signing? No.** The gate is the *header's* VRF+KES, not the EB
  signature. The right to announce comes from winning the RB slot; the EB is
  merely the payload the header commits to.

**Corollary for adversarial work:** announce-path attacks are inherently
**producer-only and stake-gated** — you can only originate announcements in slots
you actually lead (lead-rate ≈ stake fraction), so you cannot amplify beyond your
stake. You can only *manipulate the announcements you are entitled to*. This keeps
the interesting attacks in the sub-25% regime (≥ 25% trivially denies Leios).

## 4. Attack surface

**Normal behaviour (baseline, not an attack):** a node **always sends**
`MsgLeiosBlockAnnouncement` for every EB it produces, and **processes** received
ones. The attacks below perturb the *sending* or *content* of that announcement.

**Scope — the announcement is *not* where the fake-EB body attacks live.** The
announcement carries only `(hash, size)` — a commitment to the header, no txs.
The **Phantom / Dummy / Mega** family stuffs the EB **body** with fabricated txs
and is advertised via **`MsgLeiosBlockOffer`**, not the announcement (Mega not
implemented). Those are body/offer attacks (see the fake-eb work + results
table), and are out of scope here. The announcement-path attacks are:

1. **Withhold** (`withhold-eb-block-announce`) — produce the EB and serve its
   body, but **suppress the announcement** (`leios.withhold_announce = true`).
   Censors the fast discovery pulse without touching the offer. Distinct from
   *dangling* (which withholds the body, not the announcement).
2. **Equivocate** (`announce-equivocate`) — mint and announce a *second* RB header
   for the same slot + issuer committing to a *different* `announced_eb` (OCIN
   equivocation, ≤ 2 rule, `net-rs/docs/leios-changes.md`). The action itself
   builds the conflicting header (`BlockProducer::produce_announce_equivocation`)
   and diffuses it on the announcement channel — it does not need a separate
   attack to create the conflict. Whether voters *act* on it is a different
   question (net-rs doesn't consume announcements for voting yet) — see the
   subsection below.
3. **Fake announce** (`fake-eb-announce`) — emit `MsgLeiosBlockAnnouncement` for a
   fabricated EB *without winning the slot*: no stake, no production, decoupled
   from the lottery. Real nodes reject the header at the VRF/KES check (only the
   elected leader can sign a valid RB header, §3); net-rs's fake validation may
   accept it — the sharpest test of the authorization gate. Runs on its own path,
   independent of the normal produce-and-announce flow.
4. **Size lie** (`announce-size-lie`) — bake a false `announced_eb` **size** into
   the produced header (announce-path counterpart of the offer-path
   `lie-about-eb-size`). Under-declare → peers under-size the fetch buffer;
   over-declare → deterrence/waste; `size = 0` → dev relay drops the connection.
5. **Dangling** (`announce-dangling`) — announce normally but **never serve the
   body** (suppress the `BlockOffer`/inject). Peers learn of the EB, find nothing
   to fetch, and can miss the voting deadline — censorship via the *missing body*
   (contrast *withhold*, which suppresses the *announcement*).

### Equivocation and the announcement's real role (per CIP-0164 + PR #2132)

An earlier draft of this note overclaimed that the announcement *message* is what
confers votability and that "the offer can't route around it." The primary
sources correct that; the accurate picture:

**What a vote is cast on — the RB header, not the EB.** CIP-0164: *"The
`endorser_block_hash` identifies the header that announces the EB instead of
identifying the EB's hash directly"* (*"multiple RB headers could announce the
same EB"*). The signed **RB header** is the object of the vote; the EB-Offer is
only body transport.

**What the announcement adds — speed, not candidacy.** RB headers diffuse
*separately from ChainSync* — CIP-0164: *"RB headers diffuse independently of
standard ChainSync… This separate mechanism enables rapid EB discovery within the
strict timing bound Δhdr"* — and it lets a node *"receive all EBs from competing
forks, not only those in their current preferred chain."* So the announcement is
a **fast, fork-agnostic courier of the RB header** for rapid discovery **and**
equivocation detection. The header (via *any* path) is what enables voting.

**Equivocation is an RB-header / Praos-election phenomenon**, not
announcement-message-specific. CIP-0164 defines it as *"a block producer creates
multiple conflicting blocks for the same slot and distributes different versions
to different parts of the network."* The defence:
- *Voting is gated on the check.* A committee member may vote only once the
  *"equivocation detection period of 3 L_hdr slots"* has elapsed **and** *"it has
  not detected any equivocating RB header for the same slot."*
- *OCIN bound.* **OCIN** (Operational Certificate Issue Number): a node *"should
  ignore an EB announcement that is less than the OCIN on its settled ledger
  state or more than one greater… After not ignoring two announcements with the
  same election, [it] should ignore (including not relaying) any subsequent
  announcements for that election."*

The announcement's fast, ChainSync-independent diffusion is what lets the
`3 L_hdr` check see conflicting headers *in time* — that is its equivocation role.
`announce-equivocate` mints the second conflicting RB header itself (same slot +
issuer, different `announced_eb`) and diffuses it on the announcement channel, so
the *conflict* exists on the wire. What's missing today is the *reaction*:
because net-rs voters take candidacy from the ChainSync-diffused RB header and do
not yet consume announcements (see status below), an announce-only conflict isn't
seen by the voting path. To make voters react *now*, also diffuse the conflicting
header via ChainSync (pair with `rb-equivocator`); once net-core consumes
announcements + runs the `3 L_hdr` check, `announce-equivocate` bites on its own.

**Status — why EBs certify today without announcements.** Neither net-core nor the
Haskell prototype yet *leverages* announcements: PR #2132 is titled *"Leios: add
EB announcement diffusion (but don't otherwise leverage them yet),"* and *"before
it, announcements don't exist at all."* The rule that would make the announcement
load-bearing — **an EB-Offer is only valid once its announcement has been
received** — is stated design intent, and *"the prototype doesn't yet enforce
that."* Until it lands, voting runs off the RB header (via ChainSync) + offers, so
the announce-path attacks below (`withhold-eb-block-announce`,
`announce-equivocate`, `fake-eb-announce`, `announce-size-lie`,
`announce-dangling`) are **forward-looking**: they exercise the send/recv
plumbing, but nothing downstream consumes an announcement for validity yet, so
they are not live findings until net-core mirrors the Haskell offer-gating +
`3 L_hdr` consumption. (`withhold` is the exception that already has an
observable effect today: a withheld announcement simply isn't sent, so a
receiver never logs it.)

## 5. net-rs fidelity caveat (applies to every announce-path result)

net-rs does **not** cryptographically enforce the VRF/KES gate:
`net-node/src/validation.rs` is fake/timed validation, and the header encoder
writes placeholder `vrf_vkey`/`vrf_result` (`production.rs`, `header.rs`). So an
adversarial net-node *could* emit a header for a slot it did not win, or a
hash-mismatched announcement, and prototype peers may accept it — but that is a
**net-rs soundness gap** (a differential vs Haskell/testnet, like the Dummy Tx EB
finding), **not** a real protocol vulnerability. Real Cardano rejects the forged
header at the VRF/KES/hash check. State this explicitly in any announce-path
verdict to avoid overclaiming.

## 6. Send path — as implemented

Before this work the message was **defined, encoded, decoded, received, and
state-machined**, but the **server never sent it** — net-rs relied on ChainSync
header diffusion + `BlockOffer`. The send path is now implemented across two
coupled PRs (leios-tools#67 for the crate; an internal Red-Team PR for the
producer wiring). The `net-core`/`shared-rs` paths below live in this repo
(`leios-tools`); the `net-node`/`behaviours` paths are internal Red-Team
code, cited for provenance.

1. [`net-rs/net-core/src/store/leios_store.rs`](../../../net-rs/net-core/src/store/leios_store.rs) —
   `LeiosNotification::BlockAnnouncement { header }` + `LeiosStore::announce_block`.
   **Not** deduplicated (so a producer can enqueue two for one election — the
   OCIN-equivocation mechanism).
2. [`net-rs/net-core/src/peer/server_handlers.rs`](../../../net-rs/net-core/src/peer/server_handlers.rs) —
   `notification_to_ln_msg` maps it to `LnMsg::MsgLeiosBlockAnnouncement`;
   `next_outbound_notification` serves it (no-echo / source-filter unchanged).
3. **Trigger:** producer origination. `net-node/src/main.rs` calls
   `handle.leios_store.announce_block(header, None)` directly whenever it produces
   an RB with `announced_eb` set (the served store is directly reachable, so no
   coordinator command was needed). This is the honest pulse; relay-on-reception
   is deferred (would study relay-layer censorship).

**BT knobs** (fields on `LeiosControl`, set by leaf actions, read by the producer):
- `announce_eb_size: EbSizePolicy` — the `announce-size-lie` action; applied at
  production, independent of the offer-path `offer_eb_size`.
- `announce_dangling: bool` — the `announce-dangling` action; suppresses the body
  offer while still announcing.
- `announce_equivocate: bool` — the `announce-equivocate` action; emits a second
  announcement via `BlockProducer::produce_announce_equivocation`.

Honest default: all three off (announce truthfully, serve the body, announce once).

## References

- `net-rs/docs/leios-changes.md` — `MsgLeiosBlockAnnouncement` in the LeiosNotify
  state machine; OCIN rules; Praos-independence timing.
- CIP-0164 (network section); ouroboros-consensus PR #2132 ("Leios: add EB
  announcement diffusion") — the same PR cited in §4/§6.
- Results table: [`../attack-results.md`](../attack-results.md)
  → Penetration Tests → EB Size Lie.
