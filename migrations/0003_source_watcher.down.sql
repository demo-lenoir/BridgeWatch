BEGIN;
ALTER TABLE message_transitions DROP CONSTRAINT transition_head_pair;
ALTER TABLE message_transitions DROP COLUMN source_reorg_id, DROP COLUMN justifying_head_number, DROP COLUMN justifying_head_hash;
DROP TABLE source_reorgs;
DROP TABLE source_streams;
COMMIT;
