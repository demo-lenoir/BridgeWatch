BEGIN;
ALTER TABLE message_transitions
    DROP CONSTRAINT transition_destination_confirmation_pair,
    DROP CONSTRAINT transition_source_confirmation_pair,
    DROP CONSTRAINT transition_destination_head_pair;
ALTER TABLE message_transitions
    DROP COLUMN destination_policy_version,
    DROP COLUMN source_policy_version,
    DROP COLUMN destination_observed_confirmations,
    DROP COLUMN destination_required_confirmations,
    DROP COLUMN source_observed_confirmations,
    DROP COLUMN source_required_confirmations,
    DROP COLUMN destination_reorg_id,
    DROP COLUMN destination_head_number,
    DROP COLUMN destination_head_hash,
    DROP COLUMN destination_evidence_observation_id,
    DROP COLUMN source_evidence_observation_id;
DROP TABLE destination_reorgs;
DROP TABLE destination_streams;
COMMIT;
