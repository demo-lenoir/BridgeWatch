# BridgeWatch v1 specification

## Implementation status

Phase 4 adds relay expectation, operational delay classification, durable alert delivery, and a read-only API to the normalized evidence core and independent source-chain and destination-chain watchers. The local reference reads chains `31337` and `31338` through separate narrow HTTP interfaces, validates each actual genesis anchor and configured MockBridge address, scans from explicit start blocks, persists contiguous canonical ancestry and observations under separate checkpoints, reconciles supported reorgs up to eight blocks, and applies independently configurable three-confirmation policies.

The implemented runtime states are `NO_CANONICAL_EVIDENCE`, `UNMATCHED_DESTINATION`, `SOURCE_SEEN`, `SOURCE_FINAL`, `RELAY_PENDING`, `DEST_SEEN`, and `COMPLETED`. `STUCK` is a separate, recoverable operational anomaly that can coexist with `RELAY_PENDING`. `DEST_SEEN` requires a matching successful execution on the canonical destination branch and source finality, while destination finality is still pending. `COMPLETED` requires matching canonical evidence on both chains, both confirmation policies satisfied, and no blocking conflict or duplicate-execution anomaly.

## Problem and scope

A cross-chain workflow is not one atomic transaction. Source evidence can be delayed, duplicated, malformed, omitted by an endpoint, or invalidated by a reorg. BridgeWatch answers which durable evidence currently justifies a message classification. It does not implement bridge security, custody, proof verification, relay submission, or chain consensus.

The implemented scope covers two configured contracts, PostgreSQL evidence and independent checkpoints, bounded HTTP catch-up, canonical ancestry and supported reorg recovery on both chains, independent confirmation finality, cross-chain correlation, relay timing, alert outbox delivery, a read-only API, bounded metrics, and deterministic local verification. WebSocket hints, multiple active RPC endpoints, and relay submission are deferred.

## Identity and evidence

Observation identity is `(chain_id, block_hash, tx_hash, log_index)`. Message identity is `(protocol, source_chain_id, destination_chain_id, message_id)`. Block identity is `(chain_id, block_hash)`, which permits competing hashes at one height. Every stored block includes number, parent hash, canonical flag, and observation time.

A runtime log contributes only when its endpoint chain matches the corresponding configuration, its block passes number/hash/parent validation, `eth_getBlockByHash` agrees with the numbered lookup, the log refers to that exact block and configured contract, and the existing MockBridge adapter accepts its encoding. Historical orphan observations remain stored but do not contribute to the current projection.

## Checkpoint and catch-up invariant

Each stream stores chain ID, contract address, explicit start block, actual genesis hash, confirmation policy, maximum reorg depth, and health. Each chain has its own checkpoint storing the last committed contiguous canonical block number and hash. Changing persisted stream identity requires an explicit rescan policy.

Each watcher run validates its endpoint identity and required reads, obtains a head, and fetches at most `MaxBlocksPerRun` blocks. RPC I/O and decoding occur before a serializable database transaction. That transaction stores every header and observation in the unit, marks the canonical path, recomputes affected messages, appends transitions/anomalies, and advances that chain's checkpoint. A committed checkpoint at `N` means all relevant configured evidence through `N` is represented durably. A failed unit leaves the prior checkpoint intact and restart repeats the same interval idempotently.

## Canonicality and reorgs

Normal extension requires the first fetched parent to equal the durable checkpoint hash and every following parent to equal the preceding fetched hash. If the remote hash at the checkpoint height differs, the watcher searches backward from the old tip for the nearest hash shared by the endpoint branch and durable canonical branch. Search is limited by the configured depth and never scans below the persisted start predecessor.

For a supported reorg on either chain, one transaction marks the replaced path noncanonical, reuses or inserts the new hash-addressed path, preserves orphan observations, recomputes every affected message from current canonical evidence on both chains, records the old/new heads and ancestor, and moves only that chain's checkpoint. Exact return `A -> B -> A` reactivates existing A rows. If no common ancestor is found within the bound, the watcher records critical divergence evidence, changes its stream and checkpoint to `MANUAL_INTERVENTION`, stops its polling loop, and does not advance.

## Source finality

For canonical source block `s` and validated canonical head `h`, confirmations are:

`h.number - s.number + 1`

The subtraction is evaluated only when `h.number >= s.number`. The local policy promotes at three confirmations: heads `s+1`, `s+2`, and `s+3` represent two, three, and four confirmations respectively. Promotion to `SOURCE_FINAL` additionally requires no unresolved conflict and healthy durable source state. The transition stores the source observation and justifying head hash/number.

`SOURCE_FINAL` means the configured policy was satisfied using then-canonical evidence. It is revocable. A later supported source reorg can withdraw completion and leave a canonical destination execution as `UNMATCHED_DESTINATION`. Destination confirmations use the same arithmetic against the independent destination head. At two confirmations a matching execution yields `DEST_SEEN`; at three it may yield `COMPLETED`. A destination reorg may return the message to `SOURCE_FINAL`. Transitions retain both observations, applicable head hashes/numbers, required and observed confirmations, policy versions, and reorg references.

## Endpoint trust and readiness

Endpoint eligibility requires the configured chain ID, optional expected genesis match, readable head, consistent numbered/hash header reads, populated ancestry fields, and working filtered log reads. Wrong-chain and anchor mismatches are fatal for the polling loop. Timeouts, malformed responses, stale heads, checkpoint inconsistency, and database failures make readiness false without changing process liveness.

Readiness also requires successful endpoint validation and complete catch-up in the current process for both chains, PostgreSQL availability, checkpoints pointing to canonical blocks at the same height, healthy stream/checkpoint states, and no unresolved deep reorg. Per-chain status retains head, checkpoint, lag, health, and manual-intervention state. One trusted endpoint can still present a plausible chain while omitting relevant logs; BridgeWatch cannot cryptographically detect that omission.

## Relay expectation and operational diagnosis

The classifier runs only after both watchers have completed healthy catch-up. `SOURCE_FINAL` messages with a configured protocol and chain-pair policy receive `relay_eligible_at` anchored to the durable `source_final_at`, plus a snapshot of SLA milliseconds, policy version, and `expected_by`. Local MockBridge uses 30 seconds as a demo operational threshold. A later policy version applies to new expectations; an already persisted expectation keeps its snapshot. Restart does not reset the timer.

At `now >= expected_by`, `STUCK` opens only when the source remains final, relay eligibility exists, no valid destination execution is present, both observations are ready, and there is no blocking conflict, duplicate execution, or manual intervention. The boundary is inclusive. Precedence is manual intervention, conflict, duplicate execution, unavailable observation, STUCK, then ordinary lifecycle. An already open STUCK remains in history through an outage, while new classification is suppressed until healthy catch-up. Late destination execution resolves the episode and advances to `DEST_SEEN`, then independent destination finality permits `COMPLETED`. Source reorg removes eligibility and resolves STUCK; destination reorg can create a distinct new episode.

Each STUCK episode and its initial outbox item commit in one serializable transaction. The worker claims bounded rows with leases, sends outside the transaction, then acknowledges with a lease fence. Delivery is durable at-least-once with a stable idempotency key; an uncertain response can cause a repeat external delivery. Transient failures retry with bounded exponential backoff, while permanent failures and exhausted attempts stop. Deep reorg intervention creates one chain-level incident and alert, not one per message.

The read-only API exposes bounded message and anomaly pages, independent chain status, liveness, readiness, and metrics. Liveness only indicates the HTTP process is serving. Readiness requires PostgreSQL and both watchers' current safe observation capability. An API response is a snapshot of durable state and current watcher health; it does not cryptographically prove RPC completeness.

## Acceptance boundary

`make verify-phase4` is the executable acceptance gate. It inherits earlier checks, verifies five migrations on PostgreSQL 18.6, runs two-Anvil HTTP ingestion and operational recovery, exercises deterministic SLA/outage/restart/reorg/race/alert/API scenarios, runs the race detector and parser/ancestry/range fuzz smoke, and checks documentation and repository hygiene. Relay submission remains absent from runtime code.
