# ADR 0005: Retain branches and fail closed beyond rollback horizon

Status: Accepted; source and destination handling implemented.

Each chain stores hash-addressed headers with parent hash and a canonical flag. Observations are immutable and retained when their block becomes orphaned. The operator configures a maximum reorg depth and retention horizon of at least that depth plus catch-up overlap; neither value is silently reduced. Reconciliation finds a common ancestor and atomically switches canonical flags, checkpoints, and affected projections. `A -> B -> A` reactivates existing A rows.

A source reorg removes source finality and its relay expectation. A destination reorg removes the completion justification. If the common ancestor is absent or depth exceeds the configured bound, the watcher stops promotion, fails readiness, records the divergent heads, and requires operator intervention or a deliberate rescan from a trusted anchor. It never fabricates a canonical branch from incomplete history.

The source search walks backward nearest-first from the checkpoint and stops after the configured depth or start-block predecessor. The local policy is eight blocks. Both source and destination watchers retain hash-addressed branches and recompute affected message state on supported reorgs. Unsupported divergence records critical evidence and leaves the old checkpoint unchanged.
