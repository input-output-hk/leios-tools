# LeiosNotify (Protocol ID 18) — CIP-0164

Client-driven polling for Leios data announcements. Server notifies the client about available endorser blocks (EBs), EB transactions, and votes. The client uses these offers to decide what to fetch via [LeiosFetch](../leios_fetch/). Votes are delivered inline.

The codec follows `encodeLeiosNotify` / `decodeLeiosNotify` in [ouroboros-consensus c54515e](https://github.com/IntersectMBO/ouroboros-consensus/blob/c54515e35498e921cbd651a2c19c3360a41d35b8/ouroboros-consensus/src/ouroboros-consensus/LeiosDemoOnlyTestNotify.hs).

## Files

| File | Description |
|------|-------------|
| `mod.rs` | State machine (`State`, `Message`), `Protocol` impl, `LeiosNotifyEvent` helper enum |
| `codec.rs` | CBOR encode/decode for LeiosNotify messages |

**Legend:** CBOR = Concise Binary Object Representation; impl = implementation.

## State Machine

```mermaid
stateDiagram-v2
    [*] --> StIdle
    StIdle --> StBusy : MsgLeiosNotificationRequestNext
    StIdle --> StQuit : MsgQuit
    StQuit --> StDone : MsgDone
    StBusy --> StIdle : MsgCanceled
    StBusy --> StIdle : MsgLeiosBlockAnnouncement
    StBusy --> StIdle : MsgLeiosBlockOffer
    StBusy --> StIdle : MsgLeiosBlockTxsOffer
    StBusy --> StIdle : MsgLeiosVotes
    StDone --> [*]
```

## Agency Table

| State | Agency | Message | Next State |
|-------|--------|---------|------------|
| StIdle | **Client** | MsgLeiosNotificationRequestNext | StBusy |
| StIdle | **Client** | MsgQuit | StQuit |
| StQuit | **Server** | MsgDone | StDone |
| StBusy | **Server** | MsgCanceled | StIdle |
| StBusy | **Server** | MsgLeiosBlockAnnouncement(header) | StIdle |
| StBusy | **Server** | MsgLeiosBlockOffer(point, eb_size) | StIdle |
| StBusy | **Server** | MsgLeiosBlockTxsOffer(point) | StIdle |
| StBusy | **Server** | MsgLeiosVotes(votes) | StIdle |
| StDone | Nobody | — | — |

**Legend:** Agency = the side allowed to send the next message; `St` and `Msg` are the state and message prefixes used in the Rust enums. `point` identifies an endorser block by slot and hash; `eb_size` is its offered byte size; `header` is a ranking-block header; `votes` is a list of votes delivered inline.

## Wire Format

| Message | CBOR array |
|---------|------------|
| MsgDone | `[0]` |
| MsgQuit | `[1]` |
| MsgCanceled | `[2]` |
| MsgLeiosNotificationRequestNext | `[3]` |
| MsgLeiosBlockAnnouncement | `[4, header]` |
| MsgLeiosBlockOffer | `[5, point, eb_size]` |
| MsgLeiosBlockTxsOffer | `[6, point]` |
| MsgLeiosVotes | `[7, votes]` |

**Legend:** CBOR = Concise Binary Object Representation; `point` = `[slot, hash32]`, where `hash32` is a 32-byte hash; `header` is a ranking-block header; `eb_size` is the offered endorser-block byte size. Each entry in `votes` is `[announcing_ranking_block_hash, voter_id, signature]`, identifying the announcing block, voter, and vote signature.

Tests use byte vectors derived from the Haskell encoder and captured payloads combined with explicit message tags.

## Limits

- **Max message size**: 65,535 bytes
- **Ingress limit**: 65,536 bytes
- **Max votes per message**: 1,024
- **Timeout**: no timeout in idle or busy (notification long polling); quit waits at most 120s for the server acknowledgement

## Messages

- **MsgLeiosBlockAnnouncement** — ranking-block (RB) header that announces an EB (carries the EB hash in the CIP-0164 header extension)
- **MsgLeiosBlockOffer** — an EB is available for fetching (`point` and `eb_size`)
- **MsgLeiosBlockTxsOffer** — an EB's transactions are available (`point`)
- **MsgLeiosVotes** — a batch of votes delivered inline, each containing the announcing RB hash, voter identifier, and signature

## Client Helper

- `request_next(runner) -> Result<LeiosNotifyEvent, ProtocolError>` — poll for the next notification. On `MsgCanceled`, complete the `MsgQuit`/`MsgDone` exchange and return `ProtocolError::Terminated`.
- `LeiosNotifyEvent` enum: `BlockAnnouncement`, `BlockOffer`, `BlockTxsOffer`, `Votes`
- `done(runner) -> Result<(), ProtocolError>` — send `MsgQuit` and wait for `MsgDone`.

The helpers are non-pipelined; they do not send a concurrent `MsgQuit` while a request is outstanding.
