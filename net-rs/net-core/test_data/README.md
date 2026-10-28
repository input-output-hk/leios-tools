# w43 LeiosFetch capture

`leios-fetch-w43-response.cbor` is an unmodified response from a running Musashi
Haskell relay at cardano-node revision `65dd69d103e41a749d428ffd05de153ef7c2ab45`
on 2026-10-28. The corresponding request and provenance are in
`leios-fetch-w43-request.cbor` and `leios-fetch-w43.json`. Only public network
bytes are included; no signing keys.

The capture opened a node-to-node version-16 connection (magic 164), received
an announcement and offer via LeiosNotify, fetched the manifest, then requested
the first transaction with bitmap `{0: 0x8000000000000000}`. Bits are most
significant first. The response is `[4, [tag24(bytes(block_transaction))]]`.
The extracted transaction is `../../net-codec/test_data/leios-transaction-w43.cbor`.
Its manifest hash covers the exact four-field block transaction, including the
trailing validity boolean; TxSubmission sends three fields and uses the
ledger body hash as its advertised identifier.

The reference encoder is `encodeLeiosFetch` at ouroboros-consensus revision
`a337d69f4946d5a1b20ecaaeae635dd47822e3d2`. Tests decode the real bytes and
require byte-for-byte re-encoding, check the request bitmap and transaction
hash, and reject the old response framing and invalid transaction tags.
