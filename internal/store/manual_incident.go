package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"bridgewatch/internal/domain"
	"github.com/jackc/pgx/v5"
)

// RecordManualIncidentTx groups deep divergence by chain instead of message.
func (s *Postgres) RecordManualIncidentTx(ctx context.Context, tx pgx.Tx, chain domain.ChainID, role string, sourceReorgID, destinationReorgID *int64) error {
	if !chain.Valid() || (role != "SOURCE" && role != "DESTINATION") || (role == "SOURCE" && (sourceReorgID == nil || destinationReorgID != nil)) || (role == "DESTINATION" && (destinationReorgID == nil || sourceReorgID != nil)) {
		return errors.New("invalid manual intervention incident")
	}
	now := s.clock.Now().UTC()
	var incidentID int64
	err := tx.QueryRow(ctx, `INSERT INTO chain_incidents(chain_id,role,kind,source_reorg_id,destination_reorg_id,reason,opened_at)
		VALUES($1::numeric,$2,'MANUAL_INTERVENTION',$3,$4,'canonical divergence exceeds automatic reorg bound',$5)
		ON CONFLICT (chain_id,role,kind) WHERE resolved_at IS NULL DO NOTHING RETURNING id`, chain.String(), role, sourceReorgID, destinationReorgID, now).Scan(&incidentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	key := sha256.Sum256([]byte(fmt.Sprintf("MANUAL_INTERVENTION:%d", incidentID)))
	_, err = tx.Exec(ctx, `INSERT INTO message_alerts(chain_incident_id,idempotency_key,status,created_at,next_attempt_at) VALUES($1,$2,'PENDING',$3,$3)`, incidentID, key[:], now)
	return err
}
