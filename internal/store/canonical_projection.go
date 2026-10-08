package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	"bridgewatch/internal/domain"
	"github.com/jackc/pgx/v5"
)

type RecomputeCause struct {
	Reason             string
	SourceReorgID      *int64
	DestinationReorgID *int64
}

type ProjectionChange struct {
	MessagePK       int64
	Changed         bool
	ResolvedStuck   bool
	From            domain.State
	To              domain.State
	Protocol        string
	ChainPair       string
	DeliverySeconds *float64
}

type chainPolicy struct {
	present    bool
	headNumber uint64
	headHash   domain.Hash
	required   uint64
	version    int
	manual     bool
}

type sourceEvidence struct {
	claim       domain.SourceClaim
	blockNumber uint64
	blockHash   domain.Hash
	observation int64
}

type destinationEvidence struct {
	claim       domain.DestinationClaim
	blockNumber uint64
	blockHash   domain.Hash
	observation int64
}

// RecomputeCanonicalMessageTx derives the complete lifecycle from the two
// durable canonical branches and their independent checkpoint policies.
func (s *Postgres) RecomputeCanonicalMessageTx(ctx context.Context, tx pgx.Tx, messagePK int64, cause RecomputeCause) (ProjectionChange, error) {
	if cause.Reason == "" || len(cause.Reason) > 512 {
		return ProjectionChange{}, errors.New("invalid projection reason")
	}
	var protocol, sourceText, destinationText, currentText string
	var messageID []byte
	var revision int64
	var relayEligibleAt sql.NullTime
	err := tx.QueryRow(ctx, `SELECT protocol,source_chain_id::text,destination_chain_id::text,message_id,state,state_revision,relay_eligible_at FROM cross_chain_messages WHERE id=$1 FOR UPDATE`, messagePK).Scan(&protocol, &sourceText, &destinationText, &messageID, &currentText, &revision, &relayEligibleAt)
	if err != nil {
		return ProjectionChange{}, err
	}
	sourceID, err := domain.ChainIDFromDecimal(sourceText)
	if err != nil {
		return ProjectionChange{}, err
	}
	destinationID, err := domain.ChainIDFromDecimal(destinationText)
	if err != nil {
		return ProjectionChange{}, err
	}
	key := domain.MessageKey{Protocol: protocol, SourceChainID: sourceID, DestinationChainID: destinationID, MessageID: toHash(messageID)}
	sources, destinations, err := loadCanonicalEvidence(ctx, tx, messagePK, key)
	if err != nil {
		return ProjectionChange{}, err
	}
	sourcePolicy, err := loadSourcePolicy(ctx, tx, sourceID)
	if err != nil {
		return ProjectionChange{}, err
	}
	destinationPolicy, err := loadDestinationPolicy(ctx, tx, destinationID)
	if err != nil {
		return ProjectionChange{}, err
	}

	baseSources := make([]domain.SourceClaim, len(sources))
	for i := range sources {
		baseSources[i] = sources[i].claim
	}
	baseDestinations := make([]domain.DestinationClaim, len(destinations))
	for i := range destinations {
		baseDestinations[i] = destinations[i].claim
	}
	base := domain.Project(baseSources, baseDestinations)
	if base.Conflict {
		if err = openAnomaly(ctx, tx, messagePK, "CONFLICT", "incompatible canonical source and destination claims"); err != nil {
			return ProjectionChange{}, err
		}
	}
	if base.DuplicateExecution {
		if err = openAnomaly(ctx, tx, messagePK, "DUPLICATE_EXECUTION", "distinct canonical destination execution observations"); err != nil {
			return ProjectionChange{}, err
		}
	}
	if base.ObservedFailure {
		if err = openAnomaly(ctx, tx, messagePK, "FAILED", "matching canonical destination execution reported failure"); err != nil {
			return ProjectionChange{}, err
		}
	}
	if !base.DuplicateExecution {
		if _, err = tx.Exec(ctx, `UPDATE message_anomalies SET resolved_at=now() WHERE message_pk=$1 AND kind='DUPLICATE_EXECUTION' AND resolved_at IS NULL`, messagePK); err != nil {
			return ProjectionChange{}, err
		}
	}
	if !base.Conflict {
		if _, err = tx.Exec(ctx, `UPDATE message_anomalies SET resolved_at=now() WHERE message_pk=$1 AND kind='CONFLICT' AND resolved_at IS NULL AND NOT EXISTS(SELECT 1 FROM observation_conflicts WHERE message_pk=$1)`, messagePK); err != nil {
			return ProjectionChange{}, err
		}
	}
	if cause.SourceReorgID != nil || cause.DestinationReorgID != nil {
		if err = openAnomaly(ctx, tx, messagePK, "REORGED", "canonical chain evidence changed"); err != nil {
			return ProjectionChange{}, err
		}
	}
	var blockingAnomaly bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM message_anomalies WHERE message_pk=$1 AND kind IN ('CONFLICT','DUPLICATE_EXECUTION') AND resolved_at IS NULL)`, messagePK).Scan(&blockingAnomaly); err != nil {
		return ProjectionChange{}, err
	}

	sourceFinal, sourceIndex, sourceConfirmations := selectFinalSource(sources, sourcePolicy)
	destinationSeen, destinationFinal, destinationIndex, destinationConfirmations := selectDestination(sources, destinations, destinationPolicy)
	resetExpectation := false
	if cause.SourceReorgID != nil && relayEligibleAt.Valid && sourceIndex >= 0 {
		var previous sql.NullInt64
		previousErr := tx.QueryRow(ctx, `SELECT source_evidence_observation_id FROM message_transitions WHERE message_pk=$1 ORDER BY revision DESC LIMIT 1`, messagePK).Scan(&previous)
		if previousErr != nil && !errors.Is(previousErr, pgx.ErrNoRows) {
			return ProjectionChange{}, previousErr
		}
		resetExpectation = !previous.Valid || previous.Int64 != sources[sourceIndex].observation
		if resetExpectation {
			relayEligibleAt.Valid = false
		}
	}
	current := domain.State(currentText)
	if !sourceFinal {
		destinationSeen = false
		destinationFinal = false
	}
	if sourcePolicy.manual && current != domain.SourceFinal && current != domain.RelayPending && current != domain.DestinationSeen && current != domain.Completed {
		sourceFinal = false
	}
	if destinationPolicy.manual {
		// An unresolved branch cannot justify a new destination promotion.
		// Previously justified state remains until canonical evidence changes.
		destinationSeen = destinationSeen && (current == domain.DestinationSeen || current == domain.Completed)
		destinationFinal = destinationFinal && current == domain.Completed
	}
	projection := domain.ProjectLifecycle(base, domain.LifecycleEvidence{
		SourceFinal:      sourceFinal,
		RelayEligible:    relayEligibleAt.Valid,
		DestinationSeen:  destinationSeen,
		DestinationFinal: destinationFinal,
		Blocked:          blockingAnomaly,
	})
	if err = projection.Validate(); err != nil {
		return ProjectionChange{}, err
	}

	var sender, recipient, payload, token []byte
	var amount any
	if len(sources) > 0 {
		sender = bytesOfAddress(sources[0].claim.Sender)
		recipient = bytesOfAddress(sources[0].claim.Recipient)
		payload = bytesOf(sources[0].claim.PayloadHash)
		if sources[0].claim.Token != nil {
			token = bytesOfAddress(*sources[0].claim.Token)
			amount = sources[0].claim.Amount.String()
		}
	}
	now := s.clock.Now().UTC()
	resolvedStuck := false
	_, err = tx.Exec(ctx, `UPDATE cross_chain_messages SET state=$2,sender=$3,recipient=$4,payload_hash=$5,token=$6,amount=$7::numeric,source_final_at=CASE WHEN $8 THEN CASE WHEN $11 THEN $10 ELSE COALESCE(source_final_at,$10) END ELSE NULL END,destination_final_at=CASE WHEN $9 THEN COALESCE(destination_final_at,$10) ELSE NULL END,relay_eligible_at=CASE WHEN $8 AND NOT $11 THEN relay_eligible_at ELSE NULL END,relay_sla_ms=CASE WHEN $8 AND NOT $11 THEN relay_sla_ms ELSE NULL END,relay_policy_version=CASE WHEN $8 AND NOT $11 THEN relay_policy_version ELSE NULL END,expected_by=CASE WHEN $8 AND NOT $11 THEN expected_by ELSE NULL END,updated_at=$10 WHERE id=$1`, messagePK, string(projection.State), sender, recipient, payload, token, amount, sourceFinal, destinationFinal, now, resetExpectation)
	if err != nil {
		return ProjectionChange{}, err
	}
	if projection.State != domain.RelayPending {
		reason := "relay expectation ended"
		if !sourceFinal {
			reason = "source finality withdrawn"
		}
		if projection.State == domain.DestinationSeen || projection.State == domain.Completed {
			reason = "destination execution observed"
		}
		if blockingAnomaly {
			reason = "blocking evidence anomaly"
		}
		rows, resolveErr := tx.Query(ctx, `UPDATE message_anomalies SET resolved_at=$2,resolution_reason=$3 WHERE message_pk=$1 AND kind='STUCK' AND resolved_at IS NULL RETURNING id`, messagePK, now, reason)
		if resolveErr != nil {
			return ProjectionChange{}, resolveErr
		}
		var resolved []int64
		for rows.Next() {
			var id int64
			if resolveErr = rows.Scan(&id); resolveErr != nil {
				rows.Close()
				return ProjectionChange{}, resolveErr
			}
			resolved = append(resolved, id)
		}
		resolveErr = rows.Err()
		rows.Close()
		if resolveErr != nil {
			return ProjectionChange{}, resolveErr
		}
		if len(resolved) > 0 {
			resolvedStuck = true
			if _, err = tx.Exec(ctx, `UPDATE message_alerts SET status='CANCELLED',lease_token=NULL,lease_until=NULL WHERE anomaly_id=ANY($1::bigint[]) AND status='PENDING'`, resolved); err != nil {
				return ProjectionChange{}, err
			}
		}
	}

	change := ProjectionChange{MessagePK: messagePK, Changed: current != projection.State, ResolvedStuck: resolvedStuck, From: current, To: projection.State, Protocol: protocol, ChainPair: sourceID.String() + "_" + destinationID.String()}
	if !change.Changed {
		return change, nil
	}
	var sourceObservation, destinationObservation any
	var sourceHeadHash, destinationHeadHash any
	var sourceHeadNumber, destinationHeadNumber any
	var sourceRequired, sourceObserved, destinationRequired, destinationObserved any
	var sourceVersion, destinationVersion any
	if sourceIndex >= 0 {
		sourceObservation = sources[sourceIndex].observation
		if sourcePolicy.present && sourcePolicy.headNumber >= sources[sourceIndex].blockNumber {
			sourceHeadHash = bytesOf(sourcePolicy.headHash)
			sourceHeadNumber = int64(sourcePolicy.headNumber)
			sourceRequired = int64(sourcePolicy.required)
			sourceObserved = int64(sourceConfirmations)
			sourceVersion = sourcePolicy.version
		}
	}
	if destinationIndex >= 0 {
		destinationObservation = destinations[destinationIndex].observation
		if destinationPolicy.present && destinationPolicy.headNumber >= destinations[destinationIndex].blockNumber {
			destinationHeadHash = bytesOf(destinationPolicy.headHash)
			destinationHeadNumber = int64(destinationPolicy.headNumber)
			destinationRequired = int64(destinationPolicy.required)
			destinationObserved = int64(destinationConfirmations)
			destinationVersion = destinationPolicy.version
		}
	}
	legacyEvidence := sourceObservation
	if legacyEvidence == nil {
		legacyEvidence = destinationObservation
	}
	var from any = string(current)
	if revision == 0 {
		from = nil
	}
	if _, err = tx.Exec(ctx, `UPDATE cross_chain_messages SET state_revision=$2 WHERE id=$1`, messagePK, revision+1); err != nil {
		return ProjectionChange{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO message_transitions(message_pk,revision,from_state,to_state,reason,evidence_observation_id,source_evidence_observation_id,destination_evidence_observation_id,justifying_head_hash,justifying_head_number,destination_head_hash,destination_head_number,source_reorg_id,destination_reorg_id,source_required_confirmations,source_observed_confirmations,destination_required_confirmations,destination_observed_confirmations,source_policy_version,destination_policy_version,recorded_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)`, messagePK, revision+1, from, string(projection.State), cause.Reason, legacyEvidence, sourceObservation, destinationObservation, sourceHeadHash, sourceHeadNumber, destinationHeadHash, destinationHeadNumber, cause.SourceReorgID, cause.DestinationReorgID, sourceRequired, sourceObserved, destinationRequired, destinationObserved, sourceVersion, destinationVersion, now)
	if err != nil {
		return ProjectionChange{}, err
	}
	if projection.State == domain.Completed {
		var anchor time.Time
		if err = tx.QueryRow(ctx, `SELECT COALESCE(relay_eligible_at,source_final_at) FROM cross_chain_messages WHERE id=$1`, messagePK).Scan(&anchor); err != nil {
			return ProjectionChange{}, err
		}
		seconds := now.Sub(anchor).Seconds()
		if seconds < 0 {
			seconds = 0
		}
		change.DeliverySeconds = &seconds
	}
	return change, nil
}

func loadCanonicalEvidence(ctx context.Context, tx pgx.Tx, messagePK int64, key domain.MessageKey) ([]sourceEvidence, []destinationEvidence, error) {
	rows, err := tx.Query(ctx, `SELECT mc.role,mc.payload_hash,mc.sender,mc.recipient,mc.token,mc.amount::text,mc.execution_status,mc.protocol_version,b.block_number,b.block_hash,mc.observation_id FROM message_claims mc JOIN message_observations o ON o.id=mc.observation_id JOIN chain_blocks b ON b.chain_id=o.chain_id AND b.block_hash=o.block_hash WHERE mc.message_pk=$1 AND b.canonical ORDER BY mc.observation_id`, messagePK)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var sources []sourceEvidence
	var destinations []destinationEvidence
	for rows.Next() {
		var role string
		var payload, sender, recipient, token, blockHash []byte
		var amount, status sql.NullString
		var version uint32
		var blockNumber, observation int64
		if err = rows.Scan(&role, &payload, &sender, &recipient, &token, &amount, &status, &version, &blockNumber, &blockHash, &observation); err != nil {
			return nil, nil, err
		}
		if role == "SOURCE" {
			claim := domain.SourceClaim{Key: key, Sender: toAddress(sender), Recipient: toAddress(recipient), PayloadHash: toHash(payload), ProtocolVersion: version}
			if len(token) > 0 {
				t := toAddress(token)
				claim.Token = &t
			}
			if amount.Valid {
				n, ok := new(big.Int).SetString(amount.String, 10)
				if !ok {
					return nil, nil, errors.New("stored source amount invalid")
				}
				claim.Amount = n
			}
			sources = append(sources, sourceEvidence{claim: claim, blockNumber: uint64(blockNumber), blockHash: toHash(blockHash), observation: observation})
		} else {
			claim := domain.DestinationClaim{Key: key, Recipient: toAddress(recipient), PayloadHash: toHash(payload), Success: status.String == "SUCCESS", ProtocolVersion: version}
			destinations = append(destinations, destinationEvidence{claim: claim, blockNumber: uint64(blockNumber), blockHash: toHash(blockHash), observation: observation})
		}
	}
	return sources, destinations, rows.Err()
}

func loadSourcePolicy(ctx context.Context, tx pgx.Tx, chain domain.ChainID) (chainPolicy, error) {
	return loadPolicy(ctx, tx, `SELECT cp.block_number,cp.block_hash,cp.finality_policy_version,s.confirmations,cp.health,s.status,b.canonical FROM source_streams s JOIN chain_checkpoints cp USING(chain_id) JOIN chain_blocks b ON b.chain_id=cp.chain_id AND b.block_hash=cp.block_hash AND b.block_number=cp.block_number WHERE s.chain_id=$1::numeric`, chain)
}

func loadDestinationPolicy(ctx context.Context, tx pgx.Tx, chain domain.ChainID) (chainPolicy, error) {
	return loadPolicy(ctx, tx, `SELECT cp.block_number,cp.block_hash,cp.finality_policy_version,s.confirmations,cp.health,s.status,b.canonical FROM destination_streams s JOIN chain_checkpoints cp USING(chain_id) JOIN chain_blocks b ON b.chain_id=cp.chain_id AND b.block_hash=cp.block_hash AND b.block_number=cp.block_number WHERE s.chain_id=$1::numeric`, chain)
}

func loadPolicy(ctx context.Context, tx pgx.Tx, query string, chain domain.ChainID) (chainPolicy, error) {
	var number, required int64
	var hash []byte
	var version int
	var checkpointHealth, streamHealth string
	var canonical bool
	err := tx.QueryRow(ctx, query, chain.String()).Scan(&number, &hash, &version, &required, &checkpointHealth, &streamHealth, &canonical)
	if errors.Is(err, pgx.ErrNoRows) {
		return chainPolicy{}, nil
	}
	if err != nil {
		return chainPolicy{}, err
	}
	if number < 0 || required < 1 || !canonical {
		return chainPolicy{}, fmt.Errorf("chain %s finality checkpoint is inconsistent", chain.String())
	}
	return chainPolicy{present: true, headNumber: uint64(number), headHash: toHash(hash), required: uint64(required), version: version, manual: checkpointHealth == "MANUAL_INTERVENTION" || streamHealth == "MANUAL_INTERVENTION"}, nil
}

func selectFinalSource(sources []sourceEvidence, policy chainPolicy) (bool, int, uint64) {
	selected := -1
	var confirmations uint64
	for i := range sources {
		observed := confirmationsAt(policy, sources[i].blockNumber)
		if selected < 0 || observed > confirmations {
			selected, confirmations = i, observed
		}
		if policy.present && observed >= policy.required {
			return true, i, observed
		}
	}
	return false, selected, confirmations
}

func selectDestination(sources []sourceEvidence, destinations []destinationEvidence, policy chainPolicy) (seen, final bool, selected int, confirmations uint64) {
	selected = -1
	for i := range destinations {
		matched := false
		for j := range sources {
			if domain.Correlate(sources[j].claim, destinations[i].claim) == domain.Match && destinations[i].claim.Success {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		seen = true
		observed := confirmationsAt(policy, destinations[i].blockNumber)
		if selected < 0 || observed > confirmations {
			selected, confirmations = i, observed
		}
		if policy.present && observed >= policy.required {
			final = true
			selected, confirmations = i, observed
		}
	}
	return seen, final, selected, confirmations
}

func confirmationsAt(policy chainPolicy, block uint64) uint64 {
	if !policy.present || policy.headNumber < block {
		return 0
	}
	return policy.headNumber - block + 1
}
