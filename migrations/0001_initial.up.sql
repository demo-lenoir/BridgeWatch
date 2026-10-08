BEGIN;

CREATE TABLE chain_blocks (
    chain_id numeric(78,0) NOT NULL CHECK (chain_id >= 0),
    block_hash bytea NOT NULL CHECK (octet_length(block_hash) = 32),
    block_number bigint NOT NULL CHECK (block_number >= 0),
    parent_hash bytea NOT NULL CHECK (octet_length(parent_hash) = 32),
    canonical boolean NOT NULL DEFAULT false,
    observed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, block_hash),
    UNIQUE (chain_id, block_hash, block_number)
);
CREATE UNIQUE INDEX chain_blocks_one_canonical_height
    ON chain_blocks (chain_id, block_number) WHERE canonical;
CREATE INDEX chain_blocks_parent_idx ON chain_blocks (chain_id, parent_hash);

CREATE TABLE chain_checkpoints (
    chain_id numeric(78,0) PRIMARY KEY CHECK (chain_id >= 0),
    block_hash bytea NOT NULL CHECK (octet_length(block_hash) = 32),
    block_number bigint NOT NULL CHECK (block_number >= 0),
    health text NOT NULL CHECK (health IN ('HEALTHY','DEGRADED','MANUAL_INTERVENTION')),
    finality_policy_version integer NOT NULL CHECK (finality_policy_version > 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (chain_id, block_hash, block_number) REFERENCES chain_blocks (chain_id, block_hash, block_number)
);

CREATE TABLE cross_chain_messages (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    protocol text NOT NULL CHECK (length(protocol) BETWEEN 1 AND 64),
    source_chain_id numeric(78,0) NOT NULL CHECK (source_chain_id >= 0),
    destination_chain_id numeric(78,0) NOT NULL CHECK (destination_chain_id >= 0),
    message_id bytea NOT NULL CHECK (octet_length(message_id) = 32),
    sender bytea CHECK (sender IS NULL OR octet_length(sender) = 20),
    recipient bytea CHECK (recipient IS NULL OR octet_length(recipient) = 20),
    payload_hash bytea CHECK (payload_hash IS NULL OR octet_length(payload_hash) = 32),
    token bytea CHECK (token IS NULL OR octet_length(token) = 20),
    amount numeric(78,0) CHECK (amount IS NULL OR (amount >= 0 AND amount <= 115792089237316195423570985008687907853269984665640564039457584007913129639935)),
    state text NOT NULL CHECK (state IN ('NO_CANONICAL_EVIDENCE','UNMATCHED_DESTINATION','SOURCE_SEEN','SOURCE_FINAL','RELAY_PENDING','DEST_SEEN','COMPLETED')),
    state_revision bigint NOT NULL DEFAULT 0 CHECK (state_revision >= 0),
    source_final_at timestamptz,
    relay_eligible_at timestamptz,
    destination_final_at timestamptz,
    sla_policy_version integer CHECK (sla_policy_version IS NULL OR sla_policy_version > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (protocol, source_chain_id, destination_chain_id, message_id),
    CHECK (source_chain_id <> destination_chain_id),
    CHECK (state IN ('NO_CANONICAL_EVIDENCE','UNMATCHED_DESTINATION') OR (sender IS NOT NULL AND recipient IS NOT NULL AND payload_hash IS NOT NULL)),
    CHECK (relay_eligible_at IS NULL OR source_final_at IS NOT NULL)
);
CREATE INDEX cross_chain_messages_state_idx ON cross_chain_messages (state, id);

CREATE TABLE message_observations (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id numeric(78,0) NOT NULL CHECK (chain_id >= 0),
    block_hash bytea NOT NULL CHECK (octet_length(block_hash) = 32),
    tx_hash bytea NOT NULL CHECK (octet_length(tx_hash) = 32),
    log_index bigint NOT NULL CHECK (log_index >= 0),
    emitter bytea NOT NULL CHECK (octet_length(emitter) = 20),
    event_signature bytea NOT NULL CHECK (octet_length(event_signature) = 32),
    topics bytea[] NOT NULL CHECK (cardinality(topics) BETWEEN 1 AND 4 AND array_lower(topics, 1) = 1),
    log_data bytea NOT NULL CHECK (octet_length(log_data) <= 65536),
    raw_digest bytea NOT NULL CHECK (octet_length(raw_digest) = 32),
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (chain_id, block_hash, tx_hash, log_index),
    FOREIGN KEY (chain_id, block_hash) REFERENCES chain_blocks (chain_id, block_hash),
    CHECK (event_signature = topics[1])
);

CREATE TABLE message_claims (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_pk bigint NOT NULL REFERENCES cross_chain_messages (id),
    observation_id bigint NOT NULL REFERENCES message_observations (id),
    role text NOT NULL CHECK (role IN ('SOURCE','DESTINATION')),
    payload_hash bytea NOT NULL CHECK (octet_length(payload_hash) = 32),
    sender bytea CHECK (sender IS NULL OR octet_length(sender) = 20),
    recipient bytea CHECK (recipient IS NULL OR octet_length(recipient) = 20),
    token bytea CHECK (token IS NULL OR octet_length(token) = 20),
    amount numeric(78,0) CHECK (amount IS NULL OR (amount >= 0 AND amount <= 115792089237316195423570985008687907853269984665640564039457584007913129639935)),
    execution_status text CHECK (execution_status IS NULL OR execution_status IN ('SUCCESS','FAILURE')),
    decoded_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (observation_id, role),
    CHECK ((role = 'SOURCE' AND execution_status IS NULL AND sender IS NOT NULL AND recipient IS NOT NULL)
        OR (role = 'DESTINATION' AND execution_status IS NOT NULL))
);
CREATE INDEX message_claims_message_idx ON message_claims (message_pk, role);

CREATE TABLE observation_conflicts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    observation_id bigint NOT NULL REFERENCES message_observations (id),
    message_pk bigint REFERENCES cross_chain_messages (id),
    incoming_digest bytea NOT NULL CHECK (octet_length(incoming_digest) = 32),
    incoming_topics bytea[] NOT NULL CHECK (cardinality(incoming_topics) BETWEEN 1 AND 4 AND array_lower(incoming_topics, 1) = 1),
    incoming_data bytea NOT NULL CHECK (octet_length(incoming_data) <= 65536),
    detected_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (observation_id, incoming_digest)
);

CREATE TABLE message_anomalies (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_pk bigint NOT NULL REFERENCES cross_chain_messages (id),
    kind text NOT NULL CHECK (kind IN ('STUCK','FAILED','REORGED','DUPLICATE_EXECUTION','CONFLICT')),
    episode integer NOT NULL CHECK (episode > 0),
    reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 512),
    opened_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    UNIQUE (message_pk, kind, episode),
    CHECK (resolved_at IS NULL OR resolved_at >= opened_at)
);
CREATE UNIQUE INDEX message_anomalies_one_open_episode
    ON message_anomalies (message_pk, kind) WHERE resolved_at IS NULL;

CREATE TABLE message_transitions (
    message_pk bigint NOT NULL REFERENCES cross_chain_messages (id),
    revision bigint NOT NULL CHECK (revision > 0),
    from_state text,
    to_state text NOT NULL,
    reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 512),
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (message_pk, revision)
);

CREATE TABLE message_alerts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    anomaly_id bigint NOT NULL UNIQUE REFERENCES message_anomalies (id),
    idempotency_key bytea NOT NULL UNIQUE CHECK (octet_length(idempotency_key) = 32),
    status text NOT NULL CHECK (status IN ('PENDING','DELIVERED','CANCELLED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    delivered_at timestamptz
);

COMMIT;
