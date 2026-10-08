# Test strategy

`make verify-phase5` is the complete committed-tree local gate. It inherits `verify-phase4`, `verify-phase3`, `verify-phase2`, `verify-phase1`, and `verify-phase0`, preserving architecture hygiene, OpenAPI checks, PostgreSQL tests, source Anvil integration, and source/destination adapter fuzzing.

The inherited Phase 2 checks perform:

- Solidity build with Foundry 1.8.5;
- formatting, module verification, and `go vet`;
- fresh PostgreSQL 18 migration up/down through migration `0003`;
- all Go tests and all tests under the race detector;
- real Anvil 1.8.5 deployment, source event, HTTP ingestion, mining, and source finality;
- deterministic fork tests for reorgs before/after finality, depths 1, 2, and 8, deep fail-closed behavior, and A to B to A return;
- restart, bounded checkpoint catch-up, transaction rollback, empty block, multiple-event, duplicate, stale/malformed endpoint, destination-first, concurrency, and shutdown tests;
- range and common-ancestor fuzz smoke at 100 executions each;
- repository language, personal-path, credential-pattern, documentation-link, and source-only completion-boundary guards.

Phase 3 adds a fresh four-migration PostgreSQL 18 cluster, a two-Anvil normal and destination-first E2E, destination confirmation boundaries and fork tests, source and destination completion withdrawal, deep destination divergence, checkpoint restart/gap tests, duplicate and conflicting execution tests, concurrent finality/reorg tests, race-detector execution, destination range/ancestor fuzz smoke, migration reversal, and runtime scope guards.

Phase 4 adds a fresh five-migration PostgreSQL 18.6 cluster and repeats all Go tests and the race detector with operational state present. `TestOperationalSLABoundaryLateDeliveryAndRestart` checks one millisecond before, at, and after the 30-second deadline, restart with the durable anchor, one episode/outbox item, and late recovery. Other database tests cover destination outage, source and destination reorgs around STUCK, replacement source evidence, two classifier workers, completion before and after the SLA, response loss, retry outcomes, lease expiry after a crash, two alert workers, and a chain-scoped manual incident. `TestReadOnlyAPIAndShutdown` checks message evidence, 404, bounds, cursor pagination, filters, anomalies, status, liveness, readiness degradation, metrics, and bounded HTTP shutdown. The two-Anvil test now checks the operational path, actual API response, metrics, and deterministic count checksum.

The real Anvil harness proves RPC compatibility, authentic ABI logs, deployment-block start semantics, independent chain identities, checkpoint persistence, and successful completion. The in-process fork readers supply explicit deterministic A/B branches because exact ancestry patterns are easier to assert there than through Anvil snapshot behavior.

Phase 5 adds multi-RPC tests for stale and unavailable endpoints, wrong chain and anchor quarantine, cross-provider header disagreement, source/destination isolation, and recovery after cooldown. Manual recovery tests exercise dry run, deep source and destination reorgs, idempotent repeat, STUCK episode recomputation, completion withdrawal, and readiness only after replay. `TestFinalDemo` runs one two-Anvil scenario with duplicate and conflict injection, both reorg directions, restart, failover, API, metrics, exact counts, and a fixed checksum. `TestPhase5BenchmarkWorkload` writes a raw result only after its exact count, state, anomaly, transition, and payload checksum gates pass.

`make clean-clone-verify` archives committed `HEAD` into a temporary checkout, resolves modules, builds, and reruns Phase 4, the final demo, and benchmark smoke without checkout-local output. `make local-release` builds both Linux architectures and a non-root container, scans source and image, validates an SPDX SBOM, and binds unsigned provenance to the exact commit and artifact digests. The complete Phase 5 gate runs all of these commands.

PostgreSQL integration tests truncate their tables between cases and use temporary clusters created by the verifier. Package execution is serialized where tests share a database. Each watcher serializes its own synchronization calls; the cross-chain tests run both watchers concurrently against PostgreSQL and verify the final canonical predicate.

Fuzz properties require ranges to stay ordered and bounded without overflow. Common-ancestor results must be the nearest represented shared height, lie within the configured search distance, and report no success for divergence outside the bound.

Operational loops use at most 500 candidates per call; the runner uses 100 classification candidates and 25 alert claims per tick. HTTP pages default to 50 and cap at 100. Alert attempts cap at five in the runner, with fenced leases and bounded exponential delay. No loop starts a goroutine per message. The API also has a seeded database test with controlled watcher status, separate from its checks in the two-Anvil operational test.
