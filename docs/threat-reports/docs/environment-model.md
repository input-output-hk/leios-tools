# Red Team environment model

We define an environment model based on levels of realism.  Our attack effort is between levels 1 and 4.  Moving up through the levels gives us increased realism but decreased capability.

## Levels of realism

| Level | Environment | Real Haskell Leios nodes? | Real ledger/validation? | Stake controllable? | Topology controllable? | Role |
|---|---|---|---|---|---|---|
|0| **Research** | no | no | no | no | Desk research, design and Formal Methods work |
|1| **sim-rs** | no (behaviour-tree engine + model) | no (modelled) | assumed | assumed | protocol / parameter / dynamics modelling |
|2| **Piranha** (local Piranha-Cluster) | no (our nodes) | **faked** | yes (config) | yes (small local) | mechanism + small controlled clusters |
|3| **Piranha + ignite** | **yes** | **yes** | **yes — assign arbitrary stake** | **yes — full topology + man-in-the-middle** | the **enabler / rehearsal**: controllable, reproducible, safe |
|4| **Piranha + testnet** | **yes** | **yes** | **yes — faucet test-ADA (not real currency), so we can build ~1/3 stake** | **no** | the **target**: breaking the live testnet is the high-value outcome |
|5| **Mainnet** | **yes** | **yes** | **no** | **no** | Live network.  Doubtful we can or should ever attack this. |

## Key consequences:

- Both testnet and ignite have real nodes, real ledger, **and controllable stake** (testnet via
  faucet). **Stake is not the discriminator.**
- The *only* thing testnet cannot give us is **topology control** (plus reproducibility and the
  courtesy of not spamming a shared network).
- So the decisive question per threat is: **is the attack topology-gated** — does it require our
  nodes to occupy a *specific* position (a man-in-the-middle / relay chokepoint) in the graph?
  - **Topology-gated** → ignite is the enabler; testnet is the target but positioning is the blocker
    (landing Piranha nodes in the right spots on a live net can take hours/days, or be infeasible).
  - **Not topology-gated** → attempt directly on testnet for the status payoff; ignite is a
    safe/controlled rehearsal; Piranha alone is only a mechanism sketch (its validation is faked).
