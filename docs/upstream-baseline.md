# Upstream baseline

Verified against the installed/runtime tools and pinned release artifacts on 2026-10-06.

| Component | Phase 3 baseline | Applied consequence |
| --- | --- | --- |
| Go | 1.27.1 darwin/arm64 | `go.mod` declares Go 1.27 and toolchain 1.27.1. |
| go-ethereum | v1.17.7 | Pinned in `go.mod`; supplies the narrow HTTP implementation and EVM types. |
| Foundry/Anvil | v1.8.5, commit `51a52c59cffd940f76eddd0b4bb1791aa4b5ac7f` | `verify-phase3` inherits the pinned release check and uses it for two-chain integration. |
| Solidity | 0.8.30 | Pinned in `foundry.toml` for both event fixtures. |
| PostgreSQL | 18.6 Homebrew | Fresh-cluster migration and integration baseline. |
| pgx/v5 | v5.11.0 | Pinned PostgreSQL driver. |
| Prometheus Go client | v1.24.1 | Pinned bounded metrics on both chains. |

Each reader uses `eth_chainId`, `eth_blockNumber`, `eth_getBlockByNumber`, `eth_getBlockByHash`, and bounded `eth_getLogs` with its configured address filter. Each watcher checks method results rather than assuming provider behavior. Provider range limits remain operational behavior, so each committed run is capped by `MaxBlocksPerRun` and a failed interval is never skipped.

WebSocket subscription semantics remain relevant to the accepted design: notifications are live, connection-scoped hints and cannot replace historical reconciliation. Phase 3 relies entirely on HTTP polling.

The local confirmation counts and reorg bounds are product risk settings. No `safe` or `finalized` tag semantics are claimed by the Phase 3 runtime.

Phase 5 pins the container builder to the `golang:1.27.1-alpine` image digest in `Dockerfile`, keeps the runtime image at `scratch` with UID/GID 65532, and upgrades transitive `github.com/gorilla/websocket` to v1.5.3 after local vulnerability scanning identified the older v1.4.2 release. Local release evidence records exact scanner versions and the source commit used for the final build. MockBridge remains the only implemented protocol adapter; production protocol deployment configuration is future integration work.
