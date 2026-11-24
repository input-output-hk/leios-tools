# Red Team implementation notes — T17–T21

Companion to [`threat-coverage.md`](./threat-coverage.md). Covers threats **T17–T21** from the
Ouroboros Leios [threat model](https://github.com/input-output-hk/ouroboros-leios/blob/main/docs/threat-model.md):
how the Red Team would implement each, and which environment is the right vehicle.

---

## T17 -- Reorder transactions in EB

No different from same problem in Praos. 

- **Capability:**   **Impact:**  **Mitigation:** 
- **Red Team by environment:**
  - **sim-rs** — 
  - **Piranha (local)** — 
  - **Piranha + ignite** — **primary vehicle**: 
  - **Piranha + testnet** — **target**: 
- **Gap / effort:** **LOW**  **MEDIUM** 
- **Recommended path:** rehearse on **ignite** (real validation) → attempt on **testnet**.

## T18 -- Insert or replace transactions in EB

- **Capability:**   **Impact:**  **Mitigation:** 
- **Red Team by environment:**
  - **sim-rs** — 
  - **Piranha (local)** — 
  - **Piranha + ignite** — **primary vehicle**: 
  - **Piranha + testnet** — **target**: 
- **Gap / effort:** **LOW**  **MEDIUM** 
- **Recommended path:** rehearse on **ignite** (real validation) → attempt on **testnet**.

## T19 -- Ignore certificates, include txs in RB only *(Inaction and Nuisance)*

This threat is more like a bug (implementation/misconfiguration) than a real attack.
Some part of nodes can have improperly configured software, which does not implement
(activate) some parts of the protocol. We need to estimate the impact of such misconfiguration 
on the network performance and throughput.

Similar attacks: T9, T10.

- **Capability:** Stake for block production.
  **Impact:** Reduced throughput (if certficiates are respected, but txs are not included); 
              forks (if certificate is ignored, and same transaction is processed twice).
  **Mitigation:** Reduced rewards, hard requirements (no EB proposed for a whole epoch -- no
      validation in next epoch).
- **Red Team by environment:**
  - **sim-rs** — TBD
  - **Piranha (local)** — Demonstrate reduced performance, demonstrate forks.
  - **Piranha + ignite** — **primary vehicle**: Measure impact on throughput and forks in partially 
                         controlled environment.
  - **Piranha + testnet** — **target**: Demonstrate drop in performance (as well as forks)
                         on real network.
- **Gap / effort:** **LOW** Estimate performance reduction on local node;
              **MEDIUM** Real network demonstration.
- **Recommended path:** demonstrate on local Piranha, rehearse on **ignite** (real validation) → attempt on **testnet**.

## T20 -- Withhold announced EB or endorsed transactions

Apart from being an attack, this threat can be also a bug (misconfiguration) in a real network.
A possible scenario: if some highly effective tx production node issues an EB with the newly
created transactions, but the transaction didn't have enough time to propagate through the network, 
then the txs for this EB may have no chances to be distributed across the
whole network in time, needed for voting.
This honest situation is indistinguishable from the attack, when the transactions are deliberately
withheld or delayed.

In a particular situation (if the Txs mentioned in EB are withheld and never disclosured)
this threat intersects with T13 (providing invalid transactions in EB).

- **Capability:** Stake for block production, high tx production rate.
  **Impact:** Reduced throughput, wasted network resources, spikes in network traffic 
              (potential N^2 unsuccessful requests for Txs, that are announced but withheld --
              everyone will ask everyone).
  **Mitigation:** 
      1. Proper mempool procedures (delays before txs inclusion into RB/EB) for
              block-generating node for honest scenario;
      2. Nodes ranking for missing txs from announced EBs for adversarial scenario
              (e.g., if a node does not routinely disclosure its Txs in timely manner,
              nodes would tend to ignore its EBs).
      3. Skip voting if the node does not possess more than a threshold of the announced Txs (e.g., 
              if no txs from EB are known -- or over 75% of txs from announced EB are not known).
      4. Tx fetch traffic bounding (variation of the previous point): too many failed requests
              may result it vote skip. This mitigation can also be used for adversary actions,
              research is necessary. 
- **Red Team by environment:**
  - **sim-rs** — TBD
  - **Piranha (local)** — Demonstrate maximal performance loss for adversarial scenarios
              (withholding/delaying of EBs and Txs -- set by parameters). 
           
              Demonstrate/estimate performance loss in honest scenarios:
                 a) a portion of txs is announced along with EB production (e.g., put to a special 
                    queue in Piranha node, and then included into EB and announced to the network
                    at the same moment) -- the withheld proportion of the txs should be
                    specified as a parameter;
                 b) combine point (a) with a poorly connected network topology (e.g., two big chunks
                    of block-producing nodes are connected by a single bridge/node), so that txs
                    distribution is additionally complicated.
  - **Piranha + ignite** — **primary vehicle**: Honest scenarios (productive node in highly separated
              network topology), the productive node is a properly configured Haskell node; 
              reproduce adversarial scenarios.
  - **Piranha + testnet** — **target**: Honest scenarios are harder to implement (since network is
              well-connected), the impact of network topology can be measured here.
              Reproduce adversarial scenarios.
- **Gap / effort:** **MEDIUM** Demonstrate/measure performance loss in poorly connected network (honest scenario),
                    requries properly configured Haskell node as a productive node;
                    **HARD** Adversarial scenarios (requres block production capability for Piranha node).
- **Recommended path:** model on **local**, rehearse on **ignite** (real validation) → attempt on **testnet**.

## T21 -- Selectively withhold data from voting committee

- **Capability:** Prohibit certification of EBs by part of the network 
  **Impact:** Throughput decrease, committee fragmentation (in combination with T22), 
      possible Praos influence.
  **Mitigation:** Increase connectivity of the network
- **Red Team by environment:**
  - **sim-rs** — in progress
  - **Piranha (local)** — Model impact (performance, fragmentation, Praos influence)
      in totally controlled environment: when adversarial actions
      can be introduced at bridges of the network topology (cutting off some parts
      of the network), and find the extent of the influence that can be achieved
      in ideal conditions.
  - **Piranha + ignite** — **primary vehicle**: try to achieve the necessary effect in
      partially controlled environment: introduce Piranha nodes at the network bridges,
      also control Peer discovery protocols. We can expect that results, 
      similar to that of local Piranha, can be achieved.
  - **Piranha + testnet** — **target**: try to add Piranha nodes as upstream nodes for
      the network and employ the same tactic as in Piranha + ignite. Some temporary 
      and local effects are expected to be achieved.
- **Gap / effort:** **LOW**  **MEDIUM** 
- **Recommended path:** achieve maximal results on **Piranha (local)** in combination with
      T22, repeat on **ignite** (real validation) →  attempt on **testnet** (partial 
      success there is possible).
