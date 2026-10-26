# LeiosNotify (protocol 18)

Client polling for Leios announcements, endorser-block offers, transaction
offers, and votes. This implementation uses the **rc-2026w41** wire format.
It is incompatible with earlier prototype message tags.

The codec follows `encodeLeiosNotify` / `decodeLeiosNotify` in
[ouroboros-consensus c54515e](https://github.com/IntersectMBO/ouroboros-consensus/blob/c54515e35498e921cbd651a2c19c3360a41d35b8/ouroboros-consensus/src/ouroboros-consensus/LeiosDemoOnlyTestNotify.hs).

| Message | Wire value | Transition |
| --- | --- | --- |
| Done | `[0]` | Quit → Done |
| Quit | `[1]` | Idle → Quit |
| Canceled | `[2]` | Busy → Idle |
| RequestNext | `[3]` | Idle → Busy |
| BlockAnnouncement | `[4, header]` | Busy → Idle |
| BlockOffer | `[5, point, size]` | Busy → Idle |
| BlockTxsOffer | `[6, point]` | Busy → Idle |
| Votes | `[7, votes]` | Busy → Idle |

Wire values are Concise Binary Object Representation (CBOR) arrays. A `point`
is `[slot, hash32]`; `size` is the offered endorser-block byte size. A vote is
`[announcing_ranking_block_hash, voter_id, signature]`. Transition names omit
the `St` prefix used by the Rust enum.

The client has agency in Idle; the server has agency in Busy and Quit.
Done has no agency. Idle and Busy have no timeout (notification long polling).
Quit waits at most 120 seconds for the server acknowledgement. Messages are
limited to 65,535 bytes, with a 65,536-byte ingress buffer and at most 1,024 votes.

`request_next` returns an announcement, offer, or votes. On Canceled it completes
the Quit/Done exchange and returns `ProtocolError::Terminated`.
`done` sends Quit and waits for Done. The helpers are non-pipelined; they do not
send a concurrent Quit while a request is outstanding.

Tests use exact vectors derived from the Haskell encoder and explicitly labelled
historical payloads with updated tags. These are not fresh live-network captures.
