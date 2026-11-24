# Threat T22

Threat T22 is a potential vulnerability to a multi-step attack, which can 
lead to Praos protocol disruption.

## Attack target: Praos disruption

Leios security argument explicitly requires that Praos network cannot be
compromised. It includes any disruption to the protocol, not necessarily
a major one. So, even a short-lived fork in a Praos network (provably 
caused by Leios protocol disruption) is already a violation of the argument.

## Attack description

Vulnerability: A Praos block cannot be valid in Leios network unless the 
node knows all its prerequisites (including all certified EBs from previous
blocks). 

Imagine a blockchain fragment:

```P0 + EB0 -> P1 + EB0 certificate```

If EB0, referenced from Praos block P0, is not known to the node (or at least
one transaction from EB0 is not known to the node), P1 will not be considered 
valid. This allows to attack validity of P1 (Praos block) by attacking 
propagation of EB0 (Leios attachment).

Attack: create a situation, where nodes with 75% of stake ("knowing" nodes) 
will receive and certify EB0, and the remaining 25% nodes ("uninformed" nodes)
will not receive EB0 in 13 slots (3 * Lhdr + Lvote + Ldiff) + time for new
Praos block forging; if an "uninformed" node makes a block
before receiving EB0 or remaining Txs, it will not be able to continue 
from P1, but continue from P0 instead, creating a fork.

The blockchain from the perspective of that node:

```
P0 + (missing EB0) -> P1 + EB0 certificate (full chain unavailable)
        |
        \-----------> P1' (must avoid P1 and use last fully known block)
```

The real delay of EB0 + Txs propagation to a block forging node defines
severity of the attack: average distance between blocks is about 23 slots,
as it follows from Musashi net stats:

Leios Musashi network:

| block no | slot |
|---|---|
| 34000 | 740495 |
| 35000 | 763817 |

Average distance $\frac{763817-740495}{1000} = 23.322$

### Attack 1 (preliminary). Unbounded EB propagation delay

Preliminary attack, which should demonstrate extent of possible adversarial action
in the network. Does not constitute T22 by itself.

Main method: overflow a node with requests (repeated requests, requests
for known info, etc), which will eventually overcome abilities of a node to 
respond. So, valid requests of honest nodes may be left without answer.

Auxiliary method 1: Tx-flood, similar to T27 attack.

Auxiliary method 2: T20-like additional attack, withholding of EB0 or Txs
by adversarial nodes, participating as upstreams for honest nodes.

### Attack 2 (main attack). Measured EB propagation delay: achieve exactly 75% of certification votes

In order to use the full potential of the attack, one must have a 
Praos block with certified EB: certified, but unavailable to remaining 
25% "uninformed" nodes.

#### Unavailability to 25%

Requests flooding allows to block nodes in the middle of the network even
without having them as downstreams and not knowing their real connections.

Additional resources: based on network topology, block key nodes to prohibit 
and delay propagation as much as possible. Delay delivery of blocks and Txs 
till the end of 3 * Lhdr + Lvote window, when possible.

#### Certification by 75%

Adversarial nodes can use separate protocols to exchange EBs and Txs between 
themselves in undisrupted manner -- and supply EBs+Txs to desired honest nodes 
if necessary.

#### Attack control

A central server ("net-cluster") should observe the status of block certification,
and control extent of the attack: intensifying it when 75% are approached,
and easing it when certification is far from complete.

Certification votes are distributed over the network, so maybe the central server
will have information about certification status without additional protocols.

## Attack implementation plan.

Let us give more details to Attacks 1 and 2 from previous chapter and construct
an implementation plan for the attack.

Attack 2 seems to be harder to implement in real network. However, this 
attack can be (relatively easy) attempted in test network with controlled topology.

### Step 1. Net-rs emulation.

Set-up network topology and directly control connections. Control data
propagation by deliberate response refusal.

Purpose: see real Praos disruption in the best possible situation.

#### Implementation idea

Net-cluster listens to votes telemetry events, and when they come closer
to 75% (a question of experimentation), it activates EB- and tx- transmission
attack: blocks all EB transfers by means of adversarial action. 

With probability of 1/4 the next node will be from those 25% of nodes, which did not 
receive all info about the new EB. And its new block will make a fork.

Tx and EB propagation blocking can be released:

1. immediately after net-cluster receives
that the block (whether it's making the fork or not); it's highly improbable that such
attack can have long-lasting effects (so that a block/tx will not be delivered 
for hundreds of slots).

2. after some slots passed (e.g. 30); easier to implement.

If 1/4 of block production will have forks in this mode, this would be considered
a successful attack.

### Step 2. Net-rs emulation by network overload emulation.

Control data propagation by proper overloading emulation (introduce a
configuration parameter, limiting maximal number of outgoing data).

Purpose: test the limits of the attack in controlled topology (but without
direct connection manipulation).

#### Implementation idea

Development of Impl 1: use bandwidth limits and txs/requests flood
instead of direct control.

### Step 3. Request overflowing for Haskell node.

Test limits of Haskell node overflowing with requests. Separate task
from Step 1/Step 2, can be done in parallel.

### Step 4. Haskell nodes network experiment.

After steps 1 - 3, implement this attack on local Ignite/proto-devnet 
(Haskell nodes with controlled topology), and then on Musashi testnet.

This step may require improvements into certification votes propagation:
make sure that all nodes listen to cast votes and transfer them to the
net-cluster central server. Then the server will be able to count votes
of external nodes.
