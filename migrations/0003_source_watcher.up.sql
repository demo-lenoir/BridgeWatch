BEGIN;
CREATE TABLE source_streams (
    chain_id numeric(78,0) PRIMARY KEY CHECK (chain_id > 0),
    contract_address bytea NOT NULL CHECK (octet_length(contract_address) = 20),
    start_block bigint NOT NULL CHECK (start_block >= 1),
    genesis_hash bytea NOT NULL CHECK (octet_length(genesis_hash) = 32),
    confirmations bigint NOT NULL CHECK (confirmations >= 1),
    max_reorg_depth bigint NOT NULL CHECK (max_reorg_depth >= 1),
    status text NOT NULL CHECK (status IN ('HEALTHY','DEGRADED','MANUAL_INTERVENTION')),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE source_reorgs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id numeric(78,0) NOT NULL REFERENCES source_streams(chain_id),
    old_head_hash bytea NOT NULL CHECK (octet_length(old_head_hash) = 32),
    proposed_head_hash bytea NOT NULL CHECK (octet_length(proposed_head_hash) = 32),
    ancestor_hash bytea CHECK (ancestor_hash IS NULL OR octet_length(ancestor_hash) = 32),
    depth bigint NOT NULL CHECK (depth >= 1),
    severity text NOT NULL CHECK (severity IN ('WARNING','CRITICAL')),
    disposition text NOT NULL CHECK (disposition IN ('RECONCILED','MANUAL_INTERVENTION')),
    detected_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX source_reorgs_chain_time ON source_reorgs(chain_id, detected_at DESC);

ALTER TABLE message_transitions
    ADD COLUMN justifying_head_hash bytea CHECK (justifying_head_hash IS NULL OR octet_length(justifying_head_hash) = 32),
    ADD COLUMN justifying_head_number bigint CHECK (justifying_head_number IS NULL OR justifying_head_number >= 0),
    ADD COLUMN source_reorg_id bigint REFERENCES source_reorgs(id),
    ADD CONSTRAINT transition_head_pair CHECK ((justifying_head_hash IS NULL) = (justifying_head_number IS NULL));
COMMIT;
