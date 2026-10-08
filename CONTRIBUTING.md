# Contributing

Changes should preserve the evidence/state invariants in [SPEC.md](SPEC.md) and document any changed trust or transaction boundary in an ADR. Keep protocol-specific decoding inside the adapter. Add tests for new runtime behavior and run `make verify-phase1` for core changes. Use normal descriptive commits; do not include credentials or machine-specific paths.
