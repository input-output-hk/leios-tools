# LeiosFetch (Protocol ID 19) — CIP-0164

Client-driven request/response for Leios data retrieval. Supports fetching individual endorser blocks (EBs) and selective transaction fetching via bitmap addressing. Complements [LeiosNotify](../leios_notify/) which provides the offers.

The codec follows `encodeLeiosFetch` / `decodeLeiosFetch` in [ouroboros-consensus a337d69](https://github.com/IntersectMBO/ouroboros-consensus/blob/a337d69f4946d5a1b20ecaaeae635dd47822e3d2/ouroboros-consensus/src/ouroboros-consensus/LeiosDemoOnlyTestFetch.hs). Vote-fetch and block-range streaming messages are not implemented here.

## Files

| File | Description |
|------|-------------|
| `mod.rs` | State machine (`State`, `Message`), `Protocol` implementation |
| `codec.rs` | CBOR encode/decode for LeiosFetch messages |

**Legend:** CBOR = Concise Binary Object Representation.

## State Machine

```mermaid
stateDiagram-v2
    [*] --> StIdle
    StIdle --> StBlock : MsgLeiosBlockRequest
    StIdle --> StBlockTxs : MsgLeiosBlockTxsRequest
    StIdle --> StDone : MsgDone
    StBlock --> StIdle : MsgLeiosBlock
    StBlockTxs --> StIdle : MsgLeiosBlockTxs
    StDone --> [*]
```

## Agency Table

| State | Agency | Message | Next State |
|-------|--------|---------|------------|
| StIdle | **Client** | MsgLeiosBlockRequest(point) | StBlock |
| StIdle | **Client** | MsgLeiosBlockTxsRequest(point, bitmap) | StBlockTxs |
| StIdle | **Client** | MsgDone | StDone |
| StBlock | **Server** | MsgLeiosBlock(block) | StIdle |
| StBlockTxs | **Server** | MsgLeiosBlockTxs(transactions) | StIdle |
| StDone | Nobody | — | — |

**Legend:** Agency = the side allowed to send the next message; `St` and `Msg` are the state and message prefixes used in the Rust enums. `point` identifies an endorser block by slot and hash; `bitmap` selects transactions. The client retains the requested point and bitmap; neither is echoed in the transaction response.

## Wire Format

| Message | CBOR array |
|---------|------------|
| MsgDone | `[0]` |
| MsgLeiosBlockRequest | `[1, point]` |
| MsgLeiosBlock | `[2, block]` |
| MsgLeiosBlockTxsRequest | `[3, point, bitmaps]` |
| MsgLeiosBlockTxs | `[4, transactions]` |

**Legend:** CBOR = Concise Binary Object Representation; `point` = `[slot, hash32]`, where `hash32` is a 32-byte hash; `block` = the endorser-block manifest map `{tx_hash => tx_size}` (transaction hash to transaction size); `bitmaps` = the transaction selection map described below; `transactions` = a list of tag-24-wrapped byte strings, `[tag24(bytes(block_transaction)), ...]`. Each `block_transaction` retains its exact four-field CBOR bytes, including the trailing validity boolean used in its manifest identity. Block manifests retain their raw CBOR bytes for the consumer to interpret.

The w42/w43 framing is checked against captured Haskell request/response vectors with byte-exact re-encoding; see the [capture notes](../../../test_data/README.md).

## Limits

These are the limits enforced by this Rust implementation:

- **Max message size**: 65,535 bytes (idle), 25,165,824 bytes (data states, 24 MiB)
- **Ingress limit**: 25,165,824 bytes (24 MiB)
- **Max endorser-block manifest size**: 16,777,216 bytes (16 MiB)
- **Max individual transaction size**: 65,536 bytes (64 KiB)
- **Max bitmap entries**: 1,024
- **Max transactions**: 65,536
- **Timeout**: server states 120s (large transfers)

## Bitmap TX Addressing

`MsgLeiosBlockTxsRequest` carries a `BTreeMap<u16, u64>` bitmap for selective transaction fetching. Each entry maps a 16-bit segment index to a 64-bit bitmask, allowing the client to request specific transactions from an EB without downloading the entire set. This is critical for bandwidth efficiency when a node already has most transactions from prior dissemination.
