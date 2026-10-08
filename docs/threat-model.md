# Threat model

## Trust boundary

BridgeWatch trusts configured source and destination RPC access, the local chain/contract configuration, the MockBridge ABI, PostgreSQL durability, and operator-selected finality and relay policies. It validates each endpoint and retains contradictory evidence, but it does not run consensus or cryptographically prove RPC completeness.

| Threat | Implemented control | Residual risk |
| --- | --- | --- |
| Wrong chain endpoint | Compare `eth_chainId` with each configured chain ID; reject and fail that watcher's readiness. | A compromised endpoint may consistently impersonate a configured network. |
| Wrong chain anchor | Read block zero, compare the required configured hash in the service runner, and persist the genesis identity. | A compromised endpoint may still present a self-consistent false view after the anchor. |
| Stale source or destination provider | Reject a reported head below that chain's durable checkpoint; expose lag and fail current-process readiness until catch-up. Prior completion is not revoked solely because an endpoint is stale. | A plausible but lagging head at or above the checkpoint can delay new evidence. |
| Omitted destination logs | Scan every committed destination block from its checkpoint and filter its configured contract. | A trusted endpoint can return a plausible block while omitting a relevant log; BridgeWatch cannot cryptographically detect this. |
| Forged block context | Require numbered/hash header agreement, nonzero hashes, exact number, and parent-linked ancestry. | A single trusted endpoint can fabricate a self-consistent chain view. |
| Forged or misplaced log context | Require configured address, fetched block number/hash, nonzero transaction hash, non-removed status, and successful ABI decoding. | A provider can fabricate a log that is fully consistent with its fabricated chain view. |
| Source or destination short reorg | Find a chain-specific bounded common ancestor, retain both branches, switch that chain's canonical flags atomically, and recompute from both current branches. | A later reorg may revoke a classification that previously satisfied policy, including completion. |
| Deep reorg | Record critical old/new-head evidence, set `MANUAL_INTERVENTION`, preserve the old checkpoint, and stop polling. Explicit recovery checks the chosen ancestor and exact checkpoint before replay. | An operator must independently choose a trustworthy branch and restart the service after executing recovery. |
| Provider disagreement | Compare independently validated headers at a shared height before durable writes; degrade that chain and use no majority vote. | Providers can collude or omit logs consistently; disagreement may temporarily reduce availability. |
| All RPC endpoints unavailable | Preserve checkpoints and message state, degrade readiness, and resume bounded catch-up when a provider recovers. | Current chain visibility is unavailable until a trusted endpoint returns. |
| Checkpoint corruption | Require the checkpoint hash to resolve to a canonical block at the same height before processing or readiness. | Storage corruption beyond relational constraints requires restore or rescan. |
| WebSocket loss | No subscription is used for correctness; HTTP polling resumes from the checkpoint. | Polling has higher detection latency than a healthy live hint. |
| False destination completion | Require a matching successful canonical destination claim, independently satisfied destination confirmations, valid source finality, and no blocking conflict or duplicate execution. Store both observations and justifying heads. | Three confirmations on each chain are local risk choices and do not guarantee irreversibility. |
| Source reorg after destination execution | Recompute from both branches under message-scoped coordination; retain destination evidence as unmatched if source evidence disappears. | Completion may be revoked after it was previously justified. |
| Conflicting or duplicate destination execution | Retain each distinct observation and open `CONFLICT` or `DUPLICATE_EXECUTION`; block completion while open. | Operator investigation is needed for a raw same-key observation conflict. |
| Independent watcher outage | Keep checkpoints and process health separate; global readiness requires both watchers. | During outage, the last durable chain view may be stale. |
| Finality/reorg race | Serializable transactions, checkpoint row locks, and message row locks couple evidence, canonicality, projection, and transitions. | Competing endpoint views can still arrive sequentially, so state may change as new canonical evidence commits. |
| Duplicate or conflicting input | Immutable observation identity, idempotent inserts, conflict retention, and projection gating. | Conflict resolution remains an operator procedure. |
| Process or database failure | Bounded serializable transactions couple evidence and checkpoint; restart replays the next interval. | Loss outside PostgreSQL durability/backups is outside this design. |
| False STUCK during observation outage | Both watchers must complete healthy catch-up before a new episode opens; existing episodes remain durable with degraded diagnosis. | A single plausible but omitting RPC can still hide destination evidence. |
| Alert storm or duplicate scheduler work | One open episode per message/kind, one outbox subject, bounded due scans, row locks, and unique constraints. | Many genuinely late messages can still produce many distinct alerts. |
| Alert loss around crash or uncertain response | Episode and outbox commit together; short leased claims retry after expiry; stable idempotency key. | External delivery is at least once and can duplicate if the sink ignores the key. Permanent/exhausted rows need operator review. |
| Webhook configuration abuse | Only the operator-supplied HTTPS or loopback HTTP URL is accepted; query credentials and redirects are rejected, payload/timeout are bounded. | An operator can still configure a malicious HTTPS destination; network egress policy is external. |
| Stale operational state | API reports durable projection with current watcher health and last successful observation. | API reads are not one global snapshot across watchers and database queries. |
| API resource exhaustion | Strict parameters, maximum 100 items, bounded evidence/anomaly lists, HTTP timeouts, read-only routes. | Large numbers of repeated bounded requests still require upstream rate limiting. |
| High-cardinality telemetry | Metrics use configured protocol, chain, pair, state, endpoint alias, method, kind, and result. | Logs may contain identifiers and require access controls. |

RPC URLs and credentials must not be used as metric labels or committed in configuration examples. The metric endpoint label is a bounded local alias. Identifiers may appear only in access-controlled structured diagnostics.

BridgeWatch does not establish bridge economic security, authorize a relay, guarantee delivery, or replace chain consensus.

MockBridge is a deterministic local fixture. This repository has no production history or independent audit. Multiple RPC endpoints provide availability and cross-checks, not Byzantine safety. Alert outbox delivery is at least once and may be repeated after an uncertain response.
