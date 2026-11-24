# Transaction Flooding Attack - Preliminary Report
Red Team, IO|Labs <br>
August,2026

## Abstract
Using unstaked adversarial Piranha nodes, the Leios Testnet was flooded with valid transactions (TXs) from two
geographically remote points on the network during a time when voting committee membership was ranging between
75% and 77% (some nodes were not voting; confirmed Red Team PIR0 was voting - don't blame us!).
Just prior to the flood, some transactions were already present
from the centrifuge and Leios Endorser Block (EB) certification was nominal. We observed that the additional TX
flooding pushed the votes below the threshold of 75% and that EB certification was denied during the period of
flooding. After several skipped slots, Praos blocks began to catchup inclusion of the TXs. Once the attack was
stopped, EB certification eventually resumed. This demonstrates that the EB certification may be compromised by
non-staked nodes when the voting committee is near the threshold - a situation that should be mitigated by some
means beyond the scope of this experiment (e.g. incentives).

## Introduction
The Leios protocol requires that transactions be diffused throughout the network before they can be certified
into an Endorser Block (EB). Two forms of diffusion exist: the forwarding of transactions from one node to
another downstream node and via the fetch transactions request. In either case, the goal is to get transactions
out to all nodes (especially voting nodes) in time for nodes to validate and vote on EBs that contain them.

The transaction (TX) flood attack attempts to push new transactions into the network at a rate that stresses
transaction diffusion. If this can be done from distant (in terms of network span) points in the network, then
there may exist times when nodes do not have all of the TXs referenced by an EB. This could happen if a node
announces an EB with TXs it recently acquired, but those TXs hadn't made it to all nodes yet. In that case, a
voting node might not vote because it can't validate the EB. That would not harm the Praos functionality, but
it could deny the improved performance of Leios.

The motivation for such an attack is uncertain, but could be launched by entities that wish to undermine
Cardano credibility. The cost of the attack is fairly small, only incurring fees during the outage period
induced, but long term sustained denial of EBs seems unlikely. Machine cost is trivial - these attacks
were run on laptops including both the relay node and either cardano-cli or net-rs generators. Magazines
are built and signed in advance so run-time attack mode is cheap.

## Experiment
Two sets of transactions were created using Red Team funded addresses, each with between 1000 and 3000 TXs
that fan out UTXOs and then recollect them; only fees are lost from the funded address. We call a set of
transactions a "magazine". Two honest Leios prototype Haskell nodes were setup on the Leios Testnet, each
with it's own magazine, in two distinct parts of the world (hopefully modeling a large span across the network)
- one in South America and one in North America. The Haskell nodes were configured as relays and given a small
set of peers in the testnet. The nodes were non block-producing and had no stake.

Two modes of execution were run, with similar results. 1) scripts submitted TXs directly to the Haskell relay
node via the cardano-cli, and 2) net-rs used Leios protocol messages to forward TXs to the relay. In both
cases, the Haskell Leios prototype version w32 did the heavy lifting of TX diffusion to its named and
discovered peers.

Kleioscan was used to monitor TX adoption and EB certification/inclusion. Net-cluster was used to observe forks,
and EB certification.

## Results

During planning, we observed centrifuge activity and coincidentally there were only 75-77% voting committee
member nodes - just enough for quorum.  We took this opportunity to start the attack and even though TX
submission seemed throttled, the effect on the network was immediately obvious. EB certification and inclusion
went to zero for the duration of the attack (approximately 5 minutes). TX adoption continued via Praos with
~430 TXs per ranking block (RB).

After the TX flood completed sending all TXs in the magazines (each of the two finished at slightly different
times), there continued a period of non EB certification but within several minutes it resumed until it went
again to zero because the centrifuge was not active (and we assume that our submitted TXs had all been
included by either RBs or EBs). We verified that the TXs were adopted.

## Conclusion
It seems that for an marginally-represented voting committee that leaves the participating voters above 75% by
only 2-3%, the Leios protocol is vulnerable to a TX flood attack that pushes the voting count below the needed
75% quorum threshold and thus denies EB certification. While the Praos RB production continues to include the
TXs, the results is temporary loss of the increased bandwidth of Leios. This was only two machines attacking;
there could be higher impact with more machines coordinating an attack from the perimeter of the network.
