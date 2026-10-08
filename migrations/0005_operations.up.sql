BEGIN;

CREATE TABLE relay_policies (
    protocol text NOT NULL CHECK (length(protocol) BETWEEN 1 AND 64),
    source_chain_id numeric(78,0) NOT NULL CHECK (source_chain_id > 0),
    destination_chain_id numeric(78,0) NOT NULL CHECK (destination_chain_id > 0),
    version integer NOT NULL CHECK (version > 0),
    sla_ms bigint NOT NULL CHECK (sla_ms > 0),
    configured_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (protocol, source_chain_id, destination_chain_id),
    CHECK (source_chain_id <> destination_chain_id)
);

ALTER TABLE cross_chain_messages
    ADD COLUMN relay_sla_ms bigint CHECK (relay_sla_ms IS NULL OR relay_sla_ms > 0),
    ADD COLUMN relay_policy_version integer CHECK (relay_policy_version IS NULL OR relay_policy_version > 0),
    ADD COLUMN expected_by timestamptz,
    ADD CONSTRAINT relay_expectation_pair CHECK (
        (relay_eligible_at IS NULL AND relay_sla_ms IS NULL AND relay_policy_version IS NULL AND expected_by IS NULL)
        OR (relay_eligible_at IS NOT NULL AND relay_sla_ms IS NOT NULL AND relay_policy_version IS NOT NULL AND expected_by IS NOT NULL)
    );
CREATE INDEX cross_chain_messages_due_idx ON cross_chain_messages(expected_by, id)
    WHERE state = 'RELAY_PENDING' AND expected_by IS NOT NULL;

CREATE SEQUENCE operational_event_seq AS bigint;
ALTER TABLE message_anomalies
    ADD COLUMN event_id bigint NOT NULL DEFAULT nextval('operational_event_seq') UNIQUE,
    ADD COLUMN sla_anchor timestamptz,
    ADD COLUMN sla_threshold_ms bigint CHECK (sla_threshold_ms IS NULL OR sla_threshold_ms > 0),
    ADD COLUMN relay_policy_version integer CHECK (relay_policy_version IS NULL OR relay_policy_version > 0),
    ADD COLUMN resolution_reason text CHECK (resolution_reason IS NULL OR length(resolution_reason) BETWEEN 1 AND 256),
    ADD CONSTRAINT stuck_episode_policy CHECK (
        kind <> 'STUCK' OR (sla_anchor IS NOT NULL AND sla_threshold_ms IS NOT NULL AND relay_policy_version IS NOT NULL)
    );
CREATE INDEX message_anomalies_event_idx ON message_anomalies(event_id);

CREATE TABLE chain_incidents (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id bigint NOT NULL DEFAULT nextval('operational_event_seq') UNIQUE,
    chain_id numeric(78,0) NOT NULL CHECK (chain_id > 0),
    role text NOT NULL CHECK (role IN ('SOURCE','DESTINATION')),
    kind text NOT NULL CHECK (kind = 'MANUAL_INTERVENTION'),
    source_reorg_id bigint REFERENCES source_reorgs(id),
    destination_reorg_id bigint REFERENCES destination_reorgs(id),
    reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 512),
    opened_at timestamptz NOT NULL,
    resolved_at timestamptz,
    CHECK ((role='SOURCE' AND source_reorg_id IS NOT NULL AND destination_reorg_id IS NULL)
        OR (role='DESTINATION' AND destination_reorg_id IS NOT NULL AND source_reorg_id IS NULL))
);
CREATE UNIQUE INDEX chain_incidents_one_open ON chain_incidents(chain_id,role,kind) WHERE resolved_at IS NULL;

ALTER TABLE message_alerts
    ALTER COLUMN anomaly_id DROP NOT NULL,
    ADD COLUMN chain_incident_id bigint UNIQUE REFERENCES chain_incidents(id),
    ADD COLUMN attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN lease_token bytea CHECK (lease_token IS NULL OR octet_length(lease_token) = 16),
    ADD COLUMN lease_until timestamptz,
    ADD COLUMN last_error text CHECK (last_error IS NULL OR length(last_error) <= 512),
    ADD COLUMN failure_kind text CHECK (failure_kind IS NULL OR failure_kind IN ('TRANSIENT','PERMANENT','EXHAUSTED')),
    ADD CONSTRAINT alert_one_subject CHECK ((anomaly_id IS NULL) <> (chain_incident_id IS NULL)),
    ADD CONSTRAINT alert_lease_pair CHECK ((lease_token IS NULL) = (lease_until IS NULL));
CREATE INDEX message_alerts_due_idx ON message_alerts(next_attempt_at, id)
    WHERE status='PENDING';

COMMIT;
