# Changelog

## Unreleased

- Established the architecture, schema, API contract, and validation foundation.
- Added MockBridge v1 claim decoding, durable observation ingestion, conflict classification, transactional projection, and PostgreSQL concurrency tests.
- Added bounded source HTTP catch-up, durable stream configuration/checkpoints, canonical ancestry, shallow reorg reconciliation, revocable source finality, source metrics, and a pinned Anvil integration harness.
- Added an independent destination HTTP watcher, checkpoint, canonical ancestry, and confirmation policy. Matching execution can reach `DEST_SEEN` and `COMPLETED`; supported reorgs on either chain recompute and may withdraw completion.
- Added two-Anvil normal and destination-first integration, deterministic destination fork and concurrency tests, bounded metrics, and the `verify-phase3` acceptance gate.
- Added durable relay policy snapshots, outage-aware STUCK episodes, chain-scoped deep-reorg incidents, fenced alert outbox delivery, read-only operational HTTP API, bounded metrics, two-Anvil late-delivery recovery, and the `verify-phase4` acceptance gate.
- Added the reproducible two-chain demo, correctness-gated benchmark, clean-checkout replay, container build, vulnerability scans, SPDX SBOM, and unsigned local release metadata.
