# ADR 0002: Narrow protocol adapter boundary

Status: Accepted for v1 design.

The conceptual Go contract is:

```go
type Adapter interface {
    DecodeSource(context.Context, types.Log) (SourceMessage, error)
    DecodeDestination(context.Context, types.Log) (DestinationExecution, error)
    Correlate(SourceMessage, DestinationExecution) (MatchResult, error)
    RelayExpectation(SourceMessage) RelayPolicy
}
```

`SourceMessage` includes the source evidence reference, pair, ID, sender, recipient, payload hash, and optional token/uint256 amount. `DestinationExecution` includes its own evidence reference, candidate key, execution status, and payload hash. `MatchResult` has explicit `MATCH`, `NO_MATCH`, and `CONFLICT` outcomes with a reason. `RelayPolicy` supplies eligibility delay/challenge and SLA category, without declaring chain finality.

The adapter validates configured contract address, event signature, ABI shape, chain pair, and protocol version. Unsupported shapes fail closed. The core persists raw/decoded evidence and owns canonicality, finality, state, SLA, alerts, and API. The only v1 implementation target is `MockBridgeAdapter`; no reflection-heavy universal framework is planned. Decoding cannot authorize completion on its own.

The small `EventLog` wrapper exists because go-ethereum's `types.Log` does not contain a chain ID or parent hash. Both watchers supply these only after validating endpoint identity, numbered/hash header agreement, parent linkage, and the log's block hash. MockBridge v1 events use one signature topic and non-indexed ABI data: `MessageSent(bytes32,uint256,address,address,bytes32,address,uint256)` and `MessageExecuted(bytes32,uint256,address,bytes32,bool)`. The adapter rejects extra topics, wrong byte length, wrong configured address/chain, and malformed ABI values. It accepts only configured version 1.
