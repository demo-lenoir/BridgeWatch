# PostgreSQL schema contract

Migrations `0001` through `0005` are executable in order on PostgreSQL 18. Migration `0005` adds relay policies, durable expectation snapshots, operational event identities, chain incidents, and outbox leasing without rewriting earlier migrations.

| Table | Role |
| --- | --- |
| `chain_blocks` | Hash-addressed history keyed by `(chain_id, block_hash)`, with number, parent, observed time, and canonical flag. A partial unique index permits one canonical hash per height while retaining competing branches. |
| `chain_checkpoints` | Last committed contiguous block number/hash, update time, health, and finality-policy version. It references a stored block. |
| `source_streams` | Immutable runtime identity for the single source stream: chain, contract, explicit start block, actual genesis hash, confirmations, maximum reorg depth, and health. |
| `source_reorgs` | Old/proposed heads, optional common ancestor, depth, `WARNING` or `CRITICAL` severity, disposition, and detection time. |
| `destination_streams` | Independent destination chain, contract, start block, actual genesis hash, confirmations, reorg bound, and health. |
| `destination_reorgs` | Destination old/proposed heads, ancestor, depth, severity, disposition, and detection time. |
| `message_observations` | Immutable log identity `(chain_id, block_hash, tx_hash, log_index)` and raw evidence. Orphan observations remain linked to their blocks. |
| `message_claims` | Decoded source or destination claim tied to one observation and message. |
| `cross_chain_messages` | Current projection and revision, source/destination finality times, relay eligibility anchor, SLA/version snapshot, and due time. Source reorg clears the expectation. |
| `message_transitions` | Append-only state revisions with source/destination observation IDs, head hashes/numbers, required and observed confirmations, policy versions, and optional chain-specific reorg references. |
| `observation_conflicts` | Retained same-key raw conflicts. |
| `message_anomalies` | One open episode per message/kind. STUCK episodes retain anchor, threshold, policy version, resolution time/reason, and global event identity. |
| `relay_policies` | Current positive millisecond SLA and monotonically increasing version keyed by protocol and chain pair. |
| `chain_incidents` | One open manual-intervention incident per chain and role, referencing the deep-reorg evidence. |
| `message_alerts` | One logical initial alert per anomaly or chain incident, stable idempotency key, retry count, due time, fenced lease, status, and bounded failure detail. |

Each chain transaction locks its own checkpoint row, checks that its hash is still canonical at its height, and verifies the supplied ancestor before changing that chain's branch flags. Checkpoint update, observations, message recomputation, transition evidence, and supported-reorg record commit together. Message row locks and serializable retries coordinate cross-chain races.

Stream configuration is rechecked on every synchronization. A changed contract, start block, genesis, confirmation count, or reorg bound is rejected with an explicit rescan requirement. A deep reorg keeps the checkpoint on the last accepted branch and writes `MANUAL_INTERVENTION` to both stream and checkpoint health.

Hashes and addresses remain fixed-length `bytea`; chain IDs and uint256 values remain exact numeric values. No floating-point value is used for durable evidence.

The partial due index on `(expected_by,id)` supports bounded `RELAY_PENDING` classification. The outbox due index supports bounded claims. `operational_event_seq` orders message and chain anomalies in one cursor space. Migration reversal is verified on a fresh temporary cluster; reversal of populated operational history is destructive and should be planned as a data migration.
