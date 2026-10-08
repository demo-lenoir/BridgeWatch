BEGIN;

CREATE TABLE destination_streams (
    chain_id numeric(78,0) PRIMARY KEY CHECK (chain_id > 0),
    contract_address bytea NOT NULL CHECK (octet_length(contract_address) = 20),
    start_block bigint NOT NULL CHECK (start_block >= 1),
    genesis_hash bytea NOT NULL CHECK (octet_length(genesis_hash) = 32),
    confirmations bigint NOT NULL CHECK (confirmations >= 1),
    max_reorg_depth bigint NOT NULL CHECK (max_reorg_depth >= 1),
    status text NOT NULL CHECK (status IN ('HEALTHY','DEGRADED','MANUAL_INTERVENTION')),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE destination_reorgs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id numeric(78,0) NOT NULL REFERENCES destination_streams(chain_id),
    old_head_hash bytea NOT NULL CHECK (octet_length(old_head_hash) = 32),
    proposed_head_hash bytea NOT NULL CHECK (octet_length(proposed_head_hash) = 32),
    ancestor_hash bytea CHECK (ancestor_hash IS NULL OR octet_length(ancestor_hash) = 32),
    depth bigint NOT NULL CHECK (depth >= 1),
    severity text NOT NULL CHECK (severity IN ('WARNING','CRITICAL')),
    disposition text NOT NULL CHECK (disposition IN ('RECONCILED','MANUAL_INTERVENTION')),
    detected_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX destination_reorgs_chain_time ON destination_reorgs(chain_id, detected_at DESC);

ALTER TABLE message_transitions
    ADD COLUMN source_evidence_observation_id bigint REFERENCES message_observations(id),
    ADD COLUMN destination_evidence_observation_id bigint REFERENCES message_observations(id),
    ADD COLUMN destination_head_hash bytea CHECK (destination_head_hash IS NULL OR octet_length(destination_head_hash) = 32),
    ADD COLUMN destination_head_number bigint CHECK (destination_head_number IS NULL OR destination_head_number >= 0),
    ADD COLUMN destination_reorg_id bigint REFERENCES destination_reorgs(id),
    ADD COLUMN source_required_confirmations bigint CHECK (source_required_confirmations IS NULL OR source_required_confirmations >= 1),
    ADD COLUMN source_observed_confirmations bigint CHECK (source_observed_confirmations IS NULL OR source_observed_confirmations >= 1),
    ADD COLUMN destination_required_confirmations bigint CHECK (destination_required_confirmations IS NULL OR destination_required_confirmations >= 1),
    ADD COLUMN destination_observed_confirmations bigint CHECK (destination_observed_confirmations IS NULL OR destination_observed_confirmations >= 1),
    ADD COLUMN source_policy_version integer CHECK (source_policy_version IS NULL OR source_policy_version > 0),
    ADD COLUMN destination_policy_version integer CHECK (destination_policy_version IS NULL OR destination_policy_version > 0),
    ADD CONSTRAINT transition_destination_head_pair CHECK ((destination_head_hash IS NULL) = (destination_head_number IS NULL)),
    ADD CONSTRAINT transition_source_confirmation_pair CHECK ((source_required_confirmations IS NULL) = (source_observed_confirmations IS NULL)),
    ADD CONSTRAINT transition_destination_confirmation_pair CHECK ((destination_required_confirmations IS NULL) = (destination_observed_confirmations IS NULL));

COMMIT;
