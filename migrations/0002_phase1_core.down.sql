BEGIN;
ALTER TABLE message_transitions DROP COLUMN evidence_observation_id;
ALTER TABLE message_claims DROP COLUMN protocol_version;
COMMIT;
