# Failure matrix

`Implemented` means the permanent local suite exercises the row. `Deferred` identifies behavior outside the current runtime boundary.

| Scenario | Expected behavior | Readiness | Evidence | Status |
| --- | --- | --- | --- | --- |
| Source HTTP timeout | Keep checkpoint; retry the same interval on a later poll. | False after failure. | Checkpoint and health. | Implemented. |
| Stale source head below checkpoint | Reject without branch mutation. | False. | Last checkpoint and error. | Implemented. |
| Wrong source chain | Hard reject; do not initialize or advance the stream. | False. | Configuration and endpoint result. | Implemented. |
| Anchor mismatch | Hard reject; do not advance. | False. | Expected/observed genesis. | Implemented by validation path. |
| Malformed or inconsistent header | Reject the unit; keep checkpoint. | False. | Last checkpoint and RPC error. | Implemented. |
| Wrong contract or block hash in log | Reject the unit; keep checkpoint. | False. | Immutable prior evidence. | Implemented. |
| Empty source block | Persist canonical header and advance checkpoint. | True after catch-up. | `chain_blocks`, checkpoint. | Implemented. |
| Multiple events in one block | Persist each immutable observation once. | True after catch-up. | Observation/message counts. | Implemented. |
| Duplicate source backfill | Produce no duplicate observation or business transition. | True. | Unique keys and stable revision. | Implemented. |
| Source reorg depth 1 | Preserve A, activate B, recompute affected messages. | True after convergence. | Reorg row, canonical flags, transitions. | Implemented. |
| Source reorg depth 2 | Same bounded reconciliation. | True after convergence. | Recorded depth 2. | Implemented. |
| Source reorg depth 8 | Reconcile at configured boundary. | True after convergence. | Recorded depth 8. | Implemented. |
| Reorg deeper than 8 | Preserve old checkpoint, record critical divergence, stop polling. | False until intervention. | `source_reorgs`, manual health. | Implemented. |
| Reorg before finality | Withdraw event before it reaches `SOURCE_FINAL`. | True after convergence. | Orphan observation and state transition. | Implemented. |
| Reorg after finality | Withdraw `SOURCE_FINAL` and recompute from current evidence. | True after convergence. | Reorg-linked transition and prior history. | Implemented. |
| Exact A to B to A return | Reactivate retained A rows without duplicate message. | True after convergence. | Two reorg rows and stable identities. | Implemented. |
| Failure during a source unit | Roll back all block/evidence/checkpoint changes. | False during failure. | Prior checkpoint only. | Implemented by transaction rollback test. |
| Restart between bounded commits | Resume at checkpoint plus one with no gap. | False until catch-up, then true. | Monotonic checkpoint and stable transitions. | Implemented. |
| Chain advances during downtime | Process every missing block after restart; finalize eligible source. | True after catch-up. | Blocks 100 through 115 fixture. | Implemented. |
| Destination evidence arrives first | Retain `UNMATCHED_DESTINATION`; correlate when source appears; complete only after both policies. | Chain readiness independent. | Retained destination claim and later transition. | Implemented in fixture and two-Anvil E2E. |
| Source WebSocket disconnect | HTTP remains the recovery path. | No subscription state exists. | Architecture boundary. | Deferred live hints; correctness independent of them. |
| Destination HTTP timeout | Keep checkpoint and prior projection; retry same range. | Destination false; source independent. | Prior checkpoint and health. | Implemented. |
| Stale destination head | Reject head below checkpoint without revoking completion. | Destination false. | Prior checkpoint. | Implemented. |
| Destination reorg depth 1/2/8 | Preserve orphan branch, activate replacement, recompute. | True after convergence. | Destination reorg rows and transitions. | Implemented. |
| Destination reorg deeper than 8 | Preserve old checkpoint, record critical divergence, stop polling. | Destination false; source independent. | Manual-intervention health and reorg row. | Implemented. |
| Destination A to B to A | Reactivate original evidence without duplicate message. | True after convergence. | Two reorg records and stable identities. | Implemented. |
| Destination reorg before finality | Withdraw `DEST_SEEN`; never reach `COMPLETED` on orphan evidence. | True after convergence. | Orphan observation and reorg-linked transition. | Implemented. |
| Destination reorg after completion | Withdraw `COMPLETED` and retain source finality. | True after convergence. | `SOURCE_FINAL` transition and retained orphan. | Implemented. |
| Source reorg after completion | Withdraw `COMPLETED`; retain destination evidence as unmatched. | True after convergence. | `UNMATCHED_DESTINATION` transition. | Implemented. |
| Destination checkpoint restart/gap | Resume at checkpoint plus one in bounded contiguous batches. | False until catch-up. | Durable checkpoint and observations. | Implemented. |
| Crash around destination reorg | Serializable unit commits all effects or none; fresh watcher converges. | False until catch-up. | Canonical flags, checkpoint, transition. | Implemented by restart and rollback boundaries. |
| One chain down | Leave other chain checkpoint and evidence independently writable. | Global false; per-chain condition visible. | Chain-scoped status. | Implemented by independent paths. |
| Completion and reorg race | Serialize message recomputation and retry database serialization conflicts. | Based on resulting chain state. | Canonical predicate and transition history. | Implemented under race detector. |
| Relay exceeds SLA | At the inclusive configured deadline, open one STUCK episode and initial outbox item if both watchers are caught up and healthy. | True. | SLA anchor, policy snapshot, episode, alert. | Implemented. |
| Late relay | Resolve STUCK and advance through `DEST_SEEN` to `COMPLETED`. | True after catch-up. | Historical resolved episode and transition. | Implemented. |
| Destination unavailable across SLA | Suppress fresh STUCK until observation recovers and catches up. | False during outage. | Health, unchanged outbox. | Implemented. |
| Restart before or during STUCK | Keep durable anchor, episode, and outbox identity; resume classification/delivery. | False until catch-up. | Same expectation and episode IDs. | Implemented. |
| Alert response lost | Retry with the same stable idempotency key after backoff. | Observation readiness independent. | One outbox row with two attempts. | Implemented. |
| Alert sink unavailable | Bounded transient backoff, then exhausted; permanent client/config errors stop immediately. | Observation readiness independent. | Attempt count, failure kind. | Implemented. |
| Source reorg while STUCK | Withdraw source finality and relay expectation, resolve STUCK, cancel pending alert. | True after convergence. | Orphan source evidence and resolution reason. | Implemented. |
| Destination reorg after recovery | Withdraw completion; if still late, create a new STUCK episode. | True after convergence. | Two distinct episodes. | Implemented. |
| Completion/SLA race | Serialize with message row lock; completed message has no open STUCK. | Based on final canonical state. | Transition and episode history. | Implemented. |
| Deep reorg manual intervention | One chain incident and initial alert, with readiness false. | False. | Reorg, chain incident, outbox. | Implemented. |
| API readiness degradation | Liveness stays 200; readiness returns 503 for watcher outage or manual intervention. | False. | HTTP integration test. | Implemented. |

Liveness reports only process health. Endpoint, checkpoint, database, catch-up, and deep-reorg conditions determine source readiness.

| One RPC endpoint unavailable | Cool down that endpoint and try the next validated endpoint for the bounded unit. | Affected chain ready after catch-up. | Endpoint status and failover metric. | Implemented. |
| Providers disagree on a shared header | Stop before canonical write; use no provider vote. | Affected chain false. | Endpoint disagreement health; unchanged checkpoint. | Implemented. |
| All endpoints unavailable | Preserve durable evidence and suppress new evidence-dependent STUCK classifications. | Affected chain false. | Stream health and unchanged checkpoint. | Implemented. |
| Deep reorg operator recovery | Validate exact checkpoint and trusted ancestor, retain old evidence, replay the selected branch. | False until complete replay. | Dry-run plan, incident lifecycle, revised canonical flags. | Implemented. |
| Repeated recovery command | Return already recovered without another write. | According to watcher catch-up. | Stable observation and transition counts. | Implemented. |
