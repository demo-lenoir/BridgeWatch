# Security

BridgeWatch is a local reference monitor through Phase 4. Do not use it alone to make production bridge security or delivery decisions. It monitors configured evidence and cannot prove economic or validator security or detect every omission by a trusted RPC. STUCK is an operational delay classification, not proof of lost funds or failed execution. External alert delivery is at least once; the receiving sink must honor the stable idempotency key to suppress duplicates.

Please report a suspected vulnerability privately to the repository owner through a private channel rather than filing a public issue with exploit details. Do not include credentials or live RPC tokens. The [threat model](docs/threat-model.md) records known trust boundaries.
