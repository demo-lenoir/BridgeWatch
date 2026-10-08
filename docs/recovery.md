# Manual chain recovery

A reorg deeper than `BRIDGEWATCH_MAX_REORG_DEPTH` puts the affected chain in `MANUAL_INTERVENTION`, records a critical reorg and a chain incident, and stops the service. The durable checkpoint and all historical evidence remain intact. Recovery is an operator decision about a trusted common ancestor; the command does not choose one automatically.

1. Keep the monitor stopped. Verify the chain ID, genesis anchor, MockBridge contract, and branch you intend to trust through an independent operational channel. Use a configured endpoint ID for that branch.
2. Record the exact persisted checkpoint number and hash, and select a common ancestor at or after the configured start block predecessor. The selected hash must be canonical in the retained database and present at that height on the chosen endpoint.
3. Run the dry run with the same environment variables used for the service:

```sh
go run ./cmd/bridgewatch reconcile-chain \
  --role DESTINATION \
  --endpoint-id primary \
  --expected-checkpoint-number "$CHECKPOINT_NUMBER" \
  --expected-checkpoint-hash "$CHECKPOINT_HASH" \
  --ancestor-number "$ANCESTOR_NUMBER" \
  --ancestor-hash "$ANCESTOR_HASH"
```

The JSON plan reports the current checkpoint, affected canonical blocks, distinct messages that need projection recomputation, and the proposed restart point. Dry run uses a read-only transaction and does not alter database state. A mismatch in chain, genesis, contract, finality, depth, checkpoint, ancestor, or retained ancestry aborts the operation. At most 100,000 blocks and 100,000 message projections may be handled in one operation.

4. Review the plan and run the identical command with `--execute`. Execution serializes on the checkpoint and atomically marks the old canonical suffix noncanonical, moves the checkpoint to the selected ancestor with `DEGRADED` health, and recomputes affected messages from current canonical evidence. It preserves old blocks, observations, transitions, anomalies, and alerts. Repeating the exact operation returns `already_recovered` without another mutation.
5. Restart BridgeWatch. The watcher validates and scans consecutive blocks from the selected ancestor to the current head. Readiness remains false until full catch-up succeeds. The manual incident closes when the affected chain becomes healthy at the head. Inspect `/v1/status`, `/health/ready`, message evidence, and alert outbox before restoring external access.

Source recovery uses `--role SOURCE` and a source endpoint ID. The command works on one affected chain only; the other chain's checkpoint is not moved. A `COMPLETED` state whose evidence was invalidated can be withdrawn, and an open `STUCK` episode can resolve through ordinary projection recomputation. Historical episodes stay available for investigation.

The selected RPC provider remains a trusted observation source. Recovery cannot prove that its branch or logs are globally complete. Concurrent operator invocations or a restarted watcher can cause a serialization failure; inspect state and rerun the dry run before retrying execution.
