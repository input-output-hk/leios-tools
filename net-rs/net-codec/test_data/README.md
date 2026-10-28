# Transaction vectors

Captured from a fresh three-node Haskell proto-devnet on 2026-10-26, node
11.1.0.164 (revision 648fc48b045148a3b2acc5f3b04b039d50375bfc).
`haskell-inline-tx.cbor` is a ranking-block transaction; `haskell-leios-tx.cbor`
is the identical transaction served through Leios. These are public signed
transaction bytes, with no signing keys.

Inline hash: `f460007374b9b25563a80054f6e312980858197fc54fcd159f0e22bb6fb3ea60`.
Leios hash: `40a24e87d2f9c243c28297ae0522628a285fb08e04633c4b5a6c601fb338fd28`.
Ledger body hash: `32a141e24ef834b51b1f1e5cb237c6102315bd1cc3aa20eccf7a987ff6af7ada`.

# Leios header vector

`leios-header-w41.hex` contains a public ranking-block header extracted from a
Haskell node’s chain database during a clean mixed Rust/Haskell proto-devnet
run on 2026-10-28. The node was cardano-node 11.2.0.164, release candidate
2026w41 (revision 715b8d2); the block number is 13 and slot is 270.

The inner header bytes are preserved exactly. They are wrapped as the
node-to-node header pair `[7, #6.24(bytes)]` (block era 8, header era index 7).
The header announces an endorser block and includes its real public keys,
proofs and signatures. It contains no signing keys.

The strict decoder follows `Cardano.Protocol.Leios.BlockHeader` and the
Dijkstra schema in [cardano-ledger 505fa722](https://github.com/IntersectMBO/cardano-ledger/blob/505fa72260632fca6df33e18e18719730bf9a460/libs/cardano-protocol/src/Cardano/Protocol/Leios/BlockHeader.hs).
