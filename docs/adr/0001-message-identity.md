# ADR 0001: Separate observation and message identity

Status: Accepted for v1 design.

Observation uniqueness is `(chain_id, block_hash, tx_hash, log_index)`. The business key is `(protocol, source_chain_id, destination_chain_id, message_id)`. Hashes and IDs are validated fixed-length bytes; chain IDs and log indexes are nonnegative integers. The mock source contract emits a bytes32 ID bound by its event to the chain pair, sender, recipient, and payload hash. The adapter accepts that ID only from a configured source contract and signature. A destination event supplies a candidate ID, not authority for source identity. A future adapter without a native ID must use a domain-separated deterministic digest of immutable source evidence and document destination matching before inclusion.

This separation preserves orphan observations and exact branch return, while deduplicating RPC redelivery. Two canonical source claims with the same key but different payload or participants raise `CONFLICT`; no field is overwritten. An identical observation key with differing bytes also raises `CONFLICT`. A message key is never accepted from an API client as evidence.

MockBridge v1 treats the bytes32 ID emitted by the configured source contract as a protocol-assigned identifier. The adapter does not claim that the ID is a hash of the payload or that the contract prevents reuse. It binds the ID to the source event's chain pair and fields as an observation claim, then requires destination payload, recipient, version, and pair to match. Reuse with differing fields is a retained conflict. Fixture IDs are fixed bytes for deterministic tests; a later local mock deployment may generate them with a nonce, but that generation is not assumed by this adapter.
