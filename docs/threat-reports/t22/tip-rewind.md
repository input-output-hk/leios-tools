The Praos "Tip-Rewind" attack proposal:

0. Formally this attack may be classified as a T22 variant: Leios vulnerability
   undermines Praos security, although it does not exactly correspond to original T22.

1. Make a new "Tip-Rewind" behaviour (parameterised by R - number of blocks), 
   which will:

   a) Get the most current tip from adopted chain, and steps back by R blocks.

      NetCluster extension, requires following longest chain; this info is available
      through telemetry and already aggregated for net-ui demonstration)

   b) Send hash of (tip-R) block to all participating nodes in Piranha cluster

   c) The nodes (net-rs) should abandon the current tip and all blocks between
      (tip..tip-R) and may produce only independent blocks.

      This behaviour can be easily included into current implementation, since there is
      already a piece of code for this in `tip_hash_for_block_production()` function and
      around it; we can just extend that logic to include additional functionality.

2. The full attack should consists of a combination of "Tip-Rewind" attack, T35 
   attack, and (possibly) T27 tx-flood. 

   a) T35 increases probability of other nodes not-producing blocks, and Piranha nodes 
      will get possibility to grow an alternative chain of blocks.

   b) Mainnet (since 10.5) uses Genesis consensus mode as default
      `https://github.com/IntersectMBO/cardano-node/blob/4d3ad24938188e2c42db1dd9927f52903f0538a4/nix/nixos/cardano-node-service.nix#L851`
      (current Mainnet version is 11.0). So the chain adoption rule changed: now is not the longest, 
      but most dense chain (with some conditions on k value: most recent forks may still be resolved
      by longest chain rule). Hence, the chain produced by adversarial nodes while honest nodes 
      are deactivated may also should include valid transactions.

      The easiest way to include valid transactions -- to include pre-prepared txs
      from magazines (thus T27 tx-flood attack may be necessary).

      This protocol update is not merged into ouroboros-leios, however it can happen at later 
      moment, and it may influence this attack.

3. Implementation: test implementation in net-rs (relatively easy), then attempt on
   proto-dev-net.
