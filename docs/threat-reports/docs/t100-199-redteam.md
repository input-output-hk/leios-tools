# Additional threats and attacks.

In addition to original threat model, some new threats/attacks can be invented
(discovered, found). This document contains description of those attacks.

- **Capability:**  **Impact:**  **Mitigation:** 
- **Red Team by environment:**
  - **sim-rs** — 
  - **Piranha (local)** — 
  - **Piranha + ignite** — **primary vehicle**: 
  - **Piranha + testnet** — **target**: 
- **Gap / effort:** **LOW**  **MEDIUM** 
- **Recommended path:** rehearse on **ignite** (real validation) → attempt on **testnet**.

## T100 -- Zero-sized EB.

This bug was encountered in real-life experiment (Leios node working in 
TestNet). This issue does not pose direct threat (except for excess load
on the network), however, marginal issues like this can be exploited by
attackers in parallel with other attacks (e.g., to block nodes, which
can otherwise produce correct blocks and spoil the attack).

- **Capability:** Disturb blocks validation **Impact:** Node crashes **Mitigation:** 
  Bugfix in existing Haskell node implementation
- **Red Team by environment:**
  - **sim-rs** — need to develop a testcase
  - **Piranha (local)** — need to develop a testcase
  - **Piranha + ignite** — **primary vehicle**: 
  - **Piranha + testnet** — tried, crashes the network **target**: attemtable directly
- **Gap / effort:** **LOW** develop a testcase
- **Recommended path:** Include into some testset.

---

## T101 -- Parallel EB validation

This attack seems to be outdated, due to planned protocol change: 
new votes format cast votes for RB's, not for EB's.

### Attack description

In current protocol implementation, the votes are cast for EB's -- not for
RB's and Points/forks. That is, if there are two RB's, which share the 
same EB, then the votes will certify EB's in the both branches 
simultaneously:

```
                                         (vote by A)
                /-->  RB2a (slot 20) + EB2 -----> RB3a (slot 30), certified
                |                                                 by A and B
                |
RB1 (slot 10) --+
                |                        (vote by B)
                \-->  RB2b (slot 20) + EB2 -----> RB3b (slot 30), certified
                                                                  by B and A
```

In the diagram above, both RB's in slot 20 (RB2a, RB2b) share the same EB 
(EB2). Imagine, node A points to RB2a, and node B points to RB2b. Then,
despite there is a fork, a vote cast by node A for EB2 certification in upper
branch, is still can be used for certification of block RB2b, in lower branch.

This feature of the protocol can be very dangerous, which can be demonstrated
by another diagram:

```
RB1 ------+------------------------------>  RB2 (slot 20)+EB2    ---> RB3
(slot 10) |                               (contains Tx1..Tx100)    (certified)
          |
          \-->  RB15 (slot 15, no EB) --->  RB2b (slot 20)+EB2   ---> RB3b
                   (contains Tx1)         (contains Tx1..Tx100)    (certified)
```

When an adversary node finds that some EB2 is certified (and it contains some
transaction which can be a target for the attack -- let it be Tx1), then it can:

1. Create a fork (branching off RB1, the predecessor of RB2);

2. Add a range block with desired transactions *between* RB1 and EB2 (for example,
we can take the same Tx1 transaction -- it will not give profit to the adversary
party, but it will make Ledger state invalid) -- it's block RB15 on the diagram;

3. Add a range block RB2b (similar to correct block RB2, but produced by the
adversary party) and some block RB3b, which will reference/certify EB2 block
(the votes can be copied from RB3 verbatim).

The alternative chain is:

1. Technically valid;

2. Longer -- that is, preferred over the main chain;

3. Contains adversarial transaction.

Note: if it would be a normal forerunning transaction inclusion, the damage
would be limited. The second Tx1 will not make into RB2b after being included
into RB15: its replay would be invalid in RB2b with high probability, thus it 
will be dismissed by mempool. 

However, the EB's are certified by the voting committee and *must* be included 
after certification without any checks. Thus, block EB2 must be applied after 
block RB2b, but it cannot be correctly applied, leading to invalid Ledger/node 
state. This can be further exploited by adversary party.

### Attack info

- **Capability:** Invalid Tx injection 
    **Impact:** Incorrect (or malicious) Ledger state 
    **Mitigation:** Protocol change: make votes assigned to Point/Tip/RB instead of EB.
- **Red Team by environment:**
  - **sim-rs** — not important
  - **Piranha (local)** — implement test (imitation)
  - **Piranha + ignite** — **primary vehicle**: implement block generation, then implement full test;
  - **Piranha + testnet** — **target**: replay test from ignite.
- **Gap / effort:** 
     **LOW** Inform development protocol team, demonstrate fork/frontrunning/replay on sim/Piranha 
     **MEDIUM/HARD** Demonstrate fork on TestNet (frontrunning tx, or 
        possibly with incorrent/replay Tx injection)
- **Recommended path:** implement test on Pirahna, rehearse on **ignite** (real validation) → attempt on **testnet**.

## T102 -- Efficient shadowing of transactions using Leios

If one owns 25% of stake in Praos, then she generates 1/4 of all blocks. Thus she
can delay generation of a transaction for 25% in average (she can ignore this
transaction at block generation).

However, in case of Leios protocol, this stake (25%+1) allows to cancel certification
of any EB, which contains unwanted tx. Thus, only Praos blocks can have that
transaction included.

- **Capability:** Delay/possibly refuse transaction inclusion 
    **Impact:** Transaction frontrunning, or transaction censorship 
    **Mitigation:** 
- **Red Team by environment:**
  - **sim-rs** — TBD
  - **Piranha (local)** — Implement attach mechanism, estimate impact of the 
         attack on throughput, 
         latency, and transaction inclusion probability: depending on the stake
         size (quite probably, 25% is not necessary, due to limited availability
         of the real life nodes; maybe a lower stake is quite ok IRL)
  - **Piranha + ignite** — **primary vehicle**: try it on real network in controllable
         environment;
  - **Piranha + testnet** — **target**: try to repeat results on testnet.
  - - **Gap / effort:**
      **LOW** Demostrate potential mechanism on Piranha (local).
      **MEDIUM/HARD** Demonstrate tx shadowing/censorship on real network (ignite/testnet),
         estimate stake, needed for that in real network (how much stake is needed).
- **Recommended path:** implement test on Pirahna, rehearse on **ignite** (real validation) → attempt on **testnet**.
