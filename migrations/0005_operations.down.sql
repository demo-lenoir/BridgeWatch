BEGIN;
DROP INDEX message_alerts_due_idx;
ALTER TABLE message_alerts
    DROP CONSTRAINT alert_lease_pair,
    DROP CONSTRAINT alert_one_subject,
    DROP COLUMN failure_kind,
    DROP COLUMN last_error,
    DROP COLUMN lease_until,
    DROP COLUMN lease_token,
    DROP COLUMN next_attempt_at,
    DROP COLUMN attempt_count,
    DROP COLUMN chain_incident_id,
    ALTER COLUMN anomaly_id SET NOT NULL;
DROP TABLE chain_incidents;
ALTER TABLE message_anomalies
    DROP CONSTRAINT stuck_episode_policy,
    DROP COLUMN resolution_reason,
    DROP COLUMN relay_policy_version,
    DROP COLUMN sla_threshold_ms,
    DROP COLUMN sla_anchor,
    DROP COLUMN event_id;
DROP SEQUENCE operational_event_seq;
DROP INDEX cross_chain_messages_due_idx;
ALTER TABLE cross_chain_messages
    DROP CONSTRAINT relay_expectation_pair,
    DROP COLUMN expected_by,
    DROP COLUMN relay_policy_version,
    DROP COLUMN relay_sla_ms;
DROP TABLE relay_policies;
COMMIT;
