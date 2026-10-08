package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"bridgewatch/internal/domain"
	"github.com/jackc/pgx/v5"
)

// PersistSourceClaimTx writes immutable evidence without projecting it. The caller
// must switch canonical flags and recompute every affected message before commit.
func (s *Postgres) PersistSourceClaimTx(ctx context.Context, tx pgx.Tx, claim domain.SourceClaim) (messagePK, observationID int64, err error) {
	if err = claim.Validate(); err != nil {
		return 0, 0, err
	}
	if err = upsertBlock(ctx, tx, claim.Observation); err != nil {
		return 0, 0, err
	}
	observationID, duplicate, conflict, err := insertObservation(ctx, tx, claim.Observation)
	if err != nil {
		return 0, 0, err
	}
	if conflict {
		err = tx.QueryRow(ctx, "SELECT message_pk FROM message_claims WHERE observation_id=$1 AND role='SOURCE'", observationID).Scan(&messagePK)
		if err != nil {
			return 0, 0, fmt.Errorf("find conflicting source claim: %w", err)
		}
		_, err = tx.Exec(ctx, `INSERT INTO observation_conflicts(observation_id,message_pk,incoming_digest,incoming_topics,incoming_data) VALUES($1,$2,$3,$4,$5) ON CONFLICT(observation_id,incoming_digest) DO NOTHING`, observationID, messagePK, bytesOf(claim.Observation.Digest()), topicBytes(claim.Observation.Topics), claim.Observation.Data)
		if err != nil {
			return 0, 0, err
		}
		if err = openAnomaly(ctx, tx, messagePK, "CONFLICT", "same observation key with differing bytes"); err != nil {
			return 0, 0, err
		}
		return messagePK, observationID, nil
	}
	if duplicate {
		var protocol, sourceID, destID string
		var messageID []byte
		err = tx.QueryRow(ctx, `SELECT mc.message_pk,m.protocol,m.source_chain_id::text,m.destination_chain_id::text,m.message_id FROM message_claims mc JOIN cross_chain_messages m ON m.id=mc.message_pk WHERE mc.observation_id=$1 AND mc.role='SOURCE'`, observationID).Scan(&messagePK, &protocol, &sourceID, &destID, &messageID)
		if err != nil {
			return 0, 0, err
		}
		if protocol != claim.Key.Protocol || sourceID != claim.Key.SourceChainID.String() || destID != claim.Key.DestinationChainID.String() || !bytes.Equal(messageID, claim.Key.MessageID[:]) {
			return 0, 0, errors.New("replayed observation decoded to another message")
		}
		return messagePK, observationID, nil
	}
	messagePK, _, _, err = ensureMessage(ctx, tx, claim.Key)
	if err != nil {
		return 0, 0, err
	}
	if err = insertClaim(ctx, tx, messagePK, observationID, "SOURCE", &claim, nil); err != nil {
		return 0, 0, err
	}
	return messagePK, observationID, nil
}
