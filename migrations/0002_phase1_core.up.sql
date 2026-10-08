BEGIN;
ALTER TABLE message_claims ADD COLUMN protocol_version integer NOT NULL DEFAULT 1 CHECK (protocol_version > 0);
ALTER TABLE message_transitions ADD COLUMN evidence_observation_id bigint REFERENCES message_observations (id);
COMMIT;
