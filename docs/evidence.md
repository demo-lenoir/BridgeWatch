# Local evidence map

The named tests run from `make verify-phase4` against fresh PostgreSQL 18.6 clusters. Earlier evidence remains covered through inherited gates.

| Claim | Permanent evidence |
| --- | --- |
| Actual local HTTP chain and event contract work | `TestRealAnvilSourceCatchup` starts Anvil 1.8.5, deploys the event fixture, persists its real genesis anchor, ingests `SOURCE_SEEN`, mines two blocks, and reaches `SOURCE_FINAL`. |
| Confirmation arithmetic is exact | `TestSourceBackfillFinalityBoundary` checks two, three, and four confirmations and the stored justifying head. |
| Empty and multiple-event blocks are durable and duplicate-safe | `TestSourceEmptyAndMultipleEventBlocksAreIdempotent`. |
| Wrong endpoint or malformed evidence cannot advance the checkpoint | `TestSourceRejectsWrongEndpointAndMalformedEvidence` covers chain ID, header, contract, block hash, timeout, and stale head. |
| Checkpoint corruption fails closed | `TestSourceCheckpointCorruptionFailsClosed` points the checkpoint to noncanonical evidence and verifies readiness failure. |
| Persisted stream identity cannot change silently | `TestSourceStreamConfigurationCannotChangeOnRestart`. |
| Reorg before and after finality recomputes from evidence | `TestSourceReorgBeforeAndAfterFinality` preserves orphan observations and withdraws the prior state. |
| Depths 1, 2, and 8 reconcile | `TestSourceReorgDepthsAndBranchReturn` verifies recorded depth and projection. |
| A to B to A reuses historical identities | The depth-1 branch-return subcase verifies one message/observation and two reorg records. |
| Deep divergence fails closed | `TestSourceDeepReorgFailsClosed` preserves the checkpoint, fails readiness, and records critical manual-intervention evidence. |
| A failed database unit is atomic | `TestSourceBatchRollbackAndRestart` proves that invalid second-block ancestry rolls back the first block and checkpoint, then restart recovers. |
| Restart between committed units has no gap | `TestSourceRestartBetweenBoundedCommitsAndAfterReorg` creates a fresh watcher after each two-block commit, across finality, and after reorg reconciliation. |
| Downtime and head advance preserve finality progress | `TestSourceRestartGapAndFinality` resumes a start-block-100 stream through head 115 and reaches `SOURCE_FINAL`. |
| Concurrent catch-up is idempotent and shutdown is bounded | `TestSourceConcurrentSyncAndShutdown`. |
| Destination-first evidence cannot complete a message | `TestSourceDestinationFirstRemainsIncomplete` ends at `SOURCE_FINAL` and asserts zero `COMPLETED` rows. |
| Metrics use bounded labels | `TestSourceMetricsBoundedLabels` checks all five source metric families and rejects a URL as endpoint label. |
| Range and ancestry helpers obey bounds | `FuzzBoundRange` and `FuzzCommonAncestor`. |
| `COMPLETED` requires independent finality on both chains | `TestDestinationFinalityBoundaryRestartAndEvidence` checks N−1/N/N+1 and both stored observation/head confirmation sets. `TestRealTwoAnvilCompletionAndDestinationFirst` proves the full operational path on two real local EVM chains. |
| Destination-first evidence converges | `TestDestinationFirstConvergesWithoutReingestion` and `TestRealTwoAnvilCompletionAndDestinationFirst` retain unmatched evidence, then correlate it after source finality. |
| Destination reorg before finality prevents completion | `TestDestinationReorgBeforeAndAfterCompletion/before` checks no `COMPLETED` transition and retained orphan evidence. |
| Destination reorg after completion withdraws it | `TestDestinationReorgBeforeAndAfterCompletion/after` checks the reorg-linked return to `SOURCE_FINAL`. |
| Source reorg after completion withdraws it independently | `TestSourceReorgWithdrawsCompleted` returns to `UNMATCHED_DESTINATION` while destination canonical evidence remains. |
| Destination depths 1, 2, 8 and exact A to B to A converge | `TestDestinationForkDepthAndReturn` checks message/observation counts and two destination reorg records. |
| Deep destination reorg fails closed with per-chain status | `TestDestinationDeepReorgAndStaleProvider` checks manual health, stationary checkpoint, unready destination, and ready source. |
| Duplicate and conflicting execution cannot silently complete | `TestDestinationAlternateExecutionAndConflict` checks distinct observations plus open conflict and duplicate-execution anomalies. `TestDuplicateCanonicalDestinationExecutionBlocksCompletion` isolates identical logical claims in distinct canonical logs, blocks completion, and resolves the episode after one becomes orphaned. |
| Restart after completion preserves state | `TestDestinationFinalityBoundaryRestartAndEvidence` creates a new watcher and checks no duplicate transition. |
| Restart during reorg and offline gap converges | `TestDestinationGapCatchupAndRestartDuringReorg` checks bounded checkpoint recovery, orphan retention, and remaining completion. |
| Destination unit rollback is atomic | `TestDestinationApplyRollbackIsAtomic` fails the second block of a direct batch, checks no partial block/checkpoint commit, then catches up from the same checkpoint. |
| Deep destination divergence does not erase prior completion by absence alone | `TestDestinationDeepReorgAndStaleProvider` advances the healthy source after destination manual intervention and confirms the previously justified completion remains. |
| Cross-chain finality/reorg races do not leave false completion | `TestConcurrentSourceReorgAndDestinationFinality` and `TestConcurrentCompletionAndDestinationReorg` repeat concurrent watcher commits and check the final canonical predicate under the race detector. |
| Destination endpoint evidence is validated before checkpoint commit | `TestDestinationEndpointRejectsInvalidEvidence` covers wrong chain, anchor, header, contract, block hash, and log timeout. |
| Destination arithmetic and labels are bounded | `FuzzDestinationRange`, `FuzzDestinationAncestor`, and `TestDestinationMetricsBoundedLabelsAndSharedRegistry`. The destination-first test also checks shared source/destination transition counters. |

| Relay anchor, SLA boundary, restart, and late delivery | `TestOperationalSLABoundaryLateDeliveryAndRestart` checks durable source-final anchor, one millisecond before/exactly at/after deadline, restart, one alert, `DEST_SEEN`, `COMPLETED`, and resolved history. |
| Destination outage suppresses a fresh STUCK | `TestDestinationOutageSuppressesNewStuck` degrades destination observation across the deadline and opens the episode only after catch-up. |
| Reorg invalidation and a new delay episode | `TestStuckSourceReorgAndDestinationReorgNewEpisode` checks destination reorg after completion opens episode two, then source reorg resolves and clears eligibility. |
| Replacement source evidence resets the SLA | `TestStuckSourceReincludedOnNewCanonicalBranchResetsAnchor` checks the same message on a new canonical source observation receives a new finality anchor and cannot reuse the old deadline. |
| Concurrent classification and completion | `TestTwoStuckWorkersAndCompletionRace` checks one alert under two classifiers and no open STUCK after concurrent completion. |
| Completion before SLA | `TestCompletionBeforeSLANoEpisode` commits destination completion before the due scan and checks no STUCK episode or alert. |
| Durable alert identity and retry | `TestAlertResponseLostRetriesSameIdentity`, `TestAlertDeliveryOutcomesAndRestart`, and `TestConcurrentAlertWorkersClaimOnce` check stable identity, transient/timeout/permanent/exhausted outcomes, restart after acknowledgement, and worker claim fencing. |
| Crash after outbox claim | `TestAlertLeaseExpiryAfterWorkerCrash` verifies an unacknowledged leased row remains unavailable before expiry and is delivered with its original identity after expiry. |
| Chain-scoped manual intervention | `TestManualIncidentAlertIsChainScoped` checks one destination incident and one outbox item on repeated deep-reorg detection. |
| Read-only API and health | `TestReadOnlyAPIAndShutdown` checks message detail, pagination, filters, anomaly/status/metric reads, readiness degradation, and graceful serving cancellation. |
| Local operational E2E | `TestRealTwoAnvilCompletionAndDestinationFirst` checks `SOURCE_SEEN → SOURCE_FINAL → RELAY_PENDING + STUCK → DEST_SEEN → COMPLETED`, API and metrics on the actual chain evidence, one delivered alert, one resolved episode, and checksum `e8432cddf67f1d29d89c345771f9e843cf605c0cf2bfb3ddf2b2a4a832422242`. |

The verifier also runs `go test -race -p 1 ./...`, all Phase 1 parser fuzz smoke, migration reversal, link checks, credential-pattern checks, personal-path checks, and public-artifact language checks. The API has both a seeded integration test and checks against actual two-Anvil evidence. No live external webhook or public chain is claimed.

## Phase 5 evidence

| Claim | Permanent evidence |
| --- | --- |
| Independent RPC failover, stale rejection, and outage recovery | `TestSourceRPCFailoverStaleOutageAndRecovery` and `TestDestinationRPCFailoverPreservesSource`. |
| Wrong chain and genesis quarantine | `TestSourceRPCWrongChainAndAnchorQuarantined`. |
| Header disagreement fails before checkpoint mutation | `TestSourceRPCHeaderDisagreementFailsClosed` and `TestDestinationRPCHeaderDisagreementFailsClosed`. |
| Endpoint credentials stay out of public errors | `TestPublicErrorDoesNotExposeEndpointCredentials`. |
| Deep destination recovery withdraws completion and restores readiness after replay | `TestManualDestinationRecoveryWithdrawsCompletion` validates dry run, execute, incident closure, and repeated no-op. |
| Deep source recovery recomputes STUCK while retaining history | `TestManualSourceRecoveryRecomputesStuck`. |
| Final two-Anvil demonstration has exact counts and checksum | `TestFinalDemo` and `scripts/phase5_local.py` validate `out/phase5/demo.json`. |
| Local benchmark rejects incorrect runs | `TestPhase5BenchmarkWorkload` plus `scripts/phase5_local.py` validate raw JSON before computing a median. |
| Clean checkout and local release evidence | `scripts/clean_clone_verify.py`, `scripts/release_phase5.py`, and `make verify-phase5` bind results to committed `HEAD`. |

These local checks do not demonstrate a public deployment, a production bridge adapter, or an external alert sink acknowledgement.
