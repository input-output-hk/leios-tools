# Red Team threat types

We define a taxonomy of threats to make them easier to discuss:

## Type A: Protocol threats

Threats that reveal problems in the protocol definition itself, which
either mean it is possible to shut down Leios completely (with less
than 25% of stake, where we know it's possible anyway), or worst case
affect Praos block production (with less than 50% of stake).

These are the worst (best?) kind - if the protocol design itself is
genuinely vulnerable to attack, there may be nothing implementations
can do to mitigate it, and a hardfork might be required to fix it.

## Type B: Implementation vulnerabilities

The classic penetration attacks, sending badly formed, oversized or floods
of requests to try to break the specific implementation of the protocol.  Not
really covered in the threat model (which assumes the implementation is perfect),
but a good route to follow.

## Type C: Own traffic withdrawal

Not really a threat exactly, just "cutting off your nose to spite your face" -
not doing what a good citizen node should, and thereby reducing throughput
in proportion to the stake you control *but no more*.  Not particularly worth
implementing except to prove that there isn't some amplification which leads to
it being a Type A.

## Type D: Denial of service attacks

Flooding the network with either duplicate or bad data (e.g. transactions) in order
to overwhelm processing capacity.  Also covers resource-exhaustion that isn't
adversarial at all - e.g. honest demand growing faster than nodes can store or
process it - since the failure mode (capacity overwhelmed) and the mitigations
(capacity, back-pressure, limits) are the same.

## Type E: MEV / Front-running / Censorship attacks

A set of behaviours (not directly attacks on the network) where someone either
uses prior information or re-orders transactions to obtain some arbitrage
ability (e.g. MEV, front-running swaps).  Very complex to implement!

This also extends to *censorship*: selectively omitting particular transactions
from an EB (or the block one produces) to suppress or delay them.  It's the flip
side of re-ordering - manipulating *which* transactions get included, rather than
in *what order* - and is often a building block for the arbitrage behaviours
above.

## Type F: Cryptographic & Key hacks

Classic attacks on private keys (KES, VRF, BLS) which allow the attacker to
masquerade as a legitimate participant and insert their own nefarious data.
Primarily down to operational security (which the Red Team could try to penetrate, or
at least check that guideliness and tools follow good practice), but we should
also validate that the relevant signature checks are sufficient for security (Type A)
and actually implemented properly (Type B).

## Type G: Governance attacks

Attacks that operate through the on-chain governance machinery rather than the
protocol or network layer - e.g. coordinating voting stake to block or delay a
hard fork, capture committees, or otherwise steer governance outcomes to prevent
the network from fixing itself.  Distinct from the technical threats above:
the "exploit" is political/economic coordination, and mitigation lives largely
in governance design and process rather than in node implementations.


