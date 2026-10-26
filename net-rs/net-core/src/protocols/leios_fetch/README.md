# LeiosFetch (protocol 19)

Request/response protocol for endorser blocks and their transactions. This
implementation uses the **rc-2026w41** wire format and requires matching peers.
Offers arrive separately through [LeiosNotify](../leios_notify/).

The codec follows `encodeLeiosFetch` / `decodeLeiosFetch` in
[ouroboros-consensus c54515e](https://github.com/IntersectMBO/ouroboros-consensus/blob/c54515e35498e921cbd651a2c19c3360a41d35b8/ouroboros-consensus/src/ouroboros-consensus/LeiosDemoOnlyTestFetch.hs).

| Message | Wire value | Transition |
| --- | --- | --- |
| Done | `[0]` | Idle → Done |
| BlockRequest | `[1, point]` | Idle → Block |
| Block | `[2, block]` | Block → Idle |
| BlockTxsRequest | `[3, point, bitmaps]` | Idle → BlockTxs |
| BlockTxs | `[4, point, bitmaps, transactions]` | BlockTxs → Idle |

Wire values are Concise Binary Object Representation (CBOR) arrays. A `point`
is `[slot, hash32]`; `bitmaps` maps 16-bit segment indices to 64-bit selection
masks; `transactions` is a list of transaction bodies. Transition names omit
the `St` prefix used by the Rust enum.

The client has agency in Idle; the server has agency in Block and BlockTxs.
Done has no agency. Responses have a 120-second timeout. Block and transaction
bodies retain their raw CBOR bytes; the consumer interprets their contents.
There are no vote-fetch or block-range messages in this implementation.

Decoding bounds block bodies to 16 MiB, individual transactions to 64 KiB,
transaction lists to 65,536 entries, and bitmaps to 1,024 entries. The in-flight
ingress buffer has a separate 24 MiB cap. Exact message-tag tests use vectors
derived from the Haskell encoder, not fresh live-network captures.
