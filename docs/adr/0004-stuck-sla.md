# ADR 0004: Stuck is an operational SLA classification

Status: Implemented in Phase 4.

The SLA clock starts at durable `relay_eligible_at`, currently equal to the persisted `source_final_at` for the MockBridge reference; this adapter has no extra delay or challenge. The threshold is configured by protocol and chain pair, with positive millisecond duration and policy version. `expected_by` and the policy snapshot persist with the message. At exactly `expected_by` the inclusive predicate may open STUCK. `STUCK` means expected matching destination evidence has not appeared by the deadline; it is not cryptographic failure or proof of failed delivery.

When either required chain is unhealthy, diagnosis becomes `CHAIN_UNAVAILABLE`. The original anchor is retained; new stuck alerts are suppressed until healthy reconciliation confirms absence. An already open STUCK episode is retained. After recovery, a past deadline can produce one episode. Canonical late execution resolves STUCK and advances to `DEST_SEEN`; destination finality later permits `COMPLETED`. Reorg invalidating the source expectation closes its episode. A destination reorg after recovery may open a new episode. Alert identity is stable per episode; transactional outbox insertion is unique and external delivery is at least once, with a stable key for sink-side deduplication.
