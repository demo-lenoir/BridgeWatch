# Architecture and state ownership

## Implemented boundary

BridgeWatch has independent source and destination HTTP observation paths and checkpoints. Each chain can use a bounded pool of configured RPC endpoints. The MockBridge adapter is the decoder. The store recomputes each message from durable canonical evidence on both chains under a message row lock. `internal/operations` handles policy activation, delayed-relay classification, and alert delivery; `internal/api` reads the durable result.

```text
configured source and destination HTTP endpoint pools
        |
        v
narrow Reader -> validated headers/logs -> MockBridge adapter
        |                                      |
        +---------- canonical block unit ------+
                              |
                              v
              serializable PostgreSQL transaction
              chain-scoped blocks + observations
              projection + transitions + anomaly
              independent checkpoint
```

Destination observations join the same message identity only after independent chain, contract, header, log, and ABI validation. A destination observation without canonical source evidence remains `UNMATCHED_DESTINATION`; later source catch-up correlates the durable claims without replaying destination logs.

## Source reader and watcher

Each `Reader` interface contains only `ChainID`, `BlockNumber`, `HeaderByNumber`, `HeaderByHash`, and bounded `FilterLogs`. Each `HTTPReader` implements it with go-ethereum. Core watcher and repository logic depend on these narrow interfaces.

Each synchronization run is serialized by the watcher. It validates chain identity, genesis, required header/log reads, and the durable checkpoint. It reads a remote head and fetches at most the configured number of consecutive blocks. Each block header is checked by number and hash, each parent must link to the previous validated hash, and each log must match the configured address and fetched block. RPC work and ABI decoding finish before the database transaction begins.

With multiple endpoints, each candidate must match the configured chain and genesis, support the required header and log methods, and report a head at or above the durable checkpoint. The watcher compares validated headers at the highest height common to eligible candidates before writing. Disagreement degrades that chain's readiness without voting. A candidate is pinned for a whole bounded synchronization unit. Failure or stale head triggers bounded failover to the next candidate; transient failures cool down for five seconds. Wrong chain and wrong anchor are hard rejected. A successful empty log range remains a provider assertion, not proof of global completeness.

WebSocket delivery is deferred. HTTP polling is the complete path, so no notification queue or subscription is required to recover a gap. A later live-hint component may only wake this same HTTP catch-up operation.

## Durable transaction

A normal unit on either chain uses one serializable transaction to:

1. lock and revalidate the checkpoint;
2. verify the durable canonical ancestor;
3. insert hash-addressed blocks and immutable observations for that chain;
4. set the new path canonical;
5. advance only that chain's checkpoint to the unit tip;
6. recompute affected messages from both canonical branches and independent policies;
7. append changed state and anomaly evidence.

The unit contains at most 64 blocks by validation; the local harness uses eight. RPC calls never hold a database transaction. A rollback leaves neither partial block evidence nor a checkpoint advance. Replaying a committed unit is idempotent.

## Canonical ancestry

Each checkpoint hash is its chain's expected canonical tip. A matching remote hash is normal extension. A mismatch starts a nearest-first backward search, bounded by `MaxReorgDepth` and the start-block predecessor. Each candidate height compares the validated remote hash with that chain's durable canonical hash.

Once a common ancestor is found, one transaction marks the replaced suffix noncanonical, inserts or reactivates the new suffix, stores a chain-specific reorg record, recomputes affected messages, and advances only that chain's checkpoint. Blocks and observations from both branches remain stored. Returning from A to B and back to the exact A hashes reuses evidence without duplicating the message.

Failure to find an ancestor in the bound records `CRITICAL` divergence evidence, changes stream/checkpoint health to `MANUAL_INTERVENTION`, leaves the old checkpoint in place, and stops the polling loop. No provider voting or automatic reset is performed.

The [manual recovery command](recovery.md) validates an operator-selected trusted ancestor and exact checkpoint, retains the old branch, and atomically moves the checkpoint back to the ancestor with degraded health. The watcher must then replay the new branch through ordinary validation and projection before readiness returns.

## Projection and finality

The shared canonical projector determines claim correlation from currently canonical blocks on both chains. It applies source and destination confirmation policies independently. It emits `SOURCE_FINAL` only for a canonical source claim satisfying source policy; `DEST_SEEN` additionally requires a matching successful canonical destination execution; `COMPLETED` requires destination policy as well and no blocking conflict or duplicate-execution anomaly.

Each chain uses `head - evidence block + 1`. A transition stores both applicable observations, head hashes/numbers, observed and required confirmations, and policy versions. Reorg recomputation uses the same evidence query and projection path. It does not apply inverse state rules. A source reorg can turn `COMPLETED` into `UNMATCHED_DESTINATION`; a destination reorg can turn it into `SOURCE_FINAL`.

## Health and metrics

Readiness requires a successful endpoint check and full catch-up in the current process for both watchers, PostgreSQL connectivity, matching checkpoint/block evidence, healthy persisted stream/checkpoint status, and no deep reorg. Each watcher exposes head, checkpoint, lag, health, and manual-intervention status. Liveness remains independent of RPC availability.

The paths expose bounded-label metrics for each validated head, checkpoint lag, reconciled reorg count, RPC request result/latency, message transitions, unmatched destination observations, and duration from durable source finality eligibility to durable completion. Labels use chain ID, configured endpoint alias, method, status, protocol, state, and chain pair. Message, address, transaction, and block identifiers are excluded.

`bridgewatch_delivery_seconds` records each transition into `COMPLETED`, using `relay_eligible_at` when present and otherwise `source_final_at`; a reorg followed by a new completion records another sample. `bridgewatch_stuck_age_seconds` records one sample when each STUCK episode opens. Current open STUCK counts come from PostgreSQL on scrape, with zero-valued series for configured pairs.

## Operational path

After healthy catch-up, a bounded scheduler selects finalized source messages with configured relay policy. It snapshots the SLA and version, anchors `relay_eligible_at` to durable source finality, and projects `RELAY_PENDING`. A second indexed, bounded scan examines due messages. It verifies current stream/checkpoint health and blocking anomalies before opening a STUCK episode and one outbox item in a serializable transaction. Row locks, an open-episode unique index, and a unique alert subject coordinate multiple workers. A canonical destination observation or source reorg resolves STUCK and cancels any pending initial alert within the projection transaction.

Outbox workers claim bounded due rows under a short transaction with a fenced lease, call the sink after commit, and acknowledge with the same lease token. Stable keys support downstream deduplication, but response loss permits repeated external delivery. Manual deep-reorg incidents are chain-scoped and enqueue one alert. The HTTP API uses the durable query model and bounded cursor pages; handlers do not decide lifecycle state. Current watcher health supplies diagnosis and readiness context.

## Future boundary

WebSocket hints and production bridge integration are outside this local fixture. Bridge transaction submission is outside BridgeWatch's monitoring scope.
