# ADR 0003: Independent finality and relay eligibility

Status: Accepted; independent source and destination confirmation policies implemented.

Each chain selects `confirmations(N)`, `safe`, or `finalized`. `N >= 1`; an observation at height `h` is final under confirmations only if a verified canonical head at `h + N - 1` includes its block. For tag policies, fetch the tagged head and verify ancestry includes the observation. Tag support and behavior are endpoint capabilities, not assumptions. Unsupported or regressing tags degrade readiness and prevent promotion; there is no silent switch to confirmations.

Source finality establishes `SOURCE_FINAL` only. Destination finality is required for `COMPLETED` and can be configured differently. Adapter relay eligibility can additionally require a challenge/delay after source finality. It is not either chain's finality. Policy changes require explicit versioning and replay of affected projections. A later contradictory canonical view triggers reorg reconciliation even after a prior finality claim. Configured thresholds are risk policy, not universal chain constants.

The current runtime applies `confirmations(N)` independently on each chain with `head - evidence block + 1`, after verifying that the evidence block remains canonical. The local fixture uses `N=3` for both chains. Transitions store the applicable observations and exact heads, counts, and policy versions. Supported reorgs recompute and may withdraw `SOURCE_FINAL` or `COMPLETED`. Tag-based policies remain outside the runtime.
