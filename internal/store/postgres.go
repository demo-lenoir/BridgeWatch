package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	"bridgewatch/internal/clock"
	"bridgewatch/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	pool  *pgxpool.Pool
	clock clock.Clock
}

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool, clock: clock.Real{}} }

func (s *Postgres) SetClock(value clock.Clock) {
	if value != nil {
		s.clock = value
	}
}

type Result string

const (
	Inserted               Result = "INSERTED"
	IdenticalDuplicate     Result = "IDENTICAL_DUPLICATE"
	ConflictingObservation Result = "CONFLICTING_OBSERVATION"
)

type IngestResult struct {
	Result     Result
	State      domain.State
	Correlated bool
	Conflict   bool
}

type Snapshot struct {
	Key               domain.MessageKey
	State             domain.State
	Revision          int64
	Projection        domain.Projection
	SourceClaims      int64
	DestinationClaims int64
	Observations      int64
	Transitions       int64
	Conflicts         int64
	DuplicateEpisodes int64
}

func (s *Postgres) LoadMessage(ctx context.Context, key domain.MessageKey) (Snapshot, error) {
	var snap Snapshot
	snap.Key = key
	var id int64
	var state string
	err := s.pool.QueryRow(ctx, `SELECT id,state,state_revision FROM cross_chain_messages WHERE protocol=$1 AND source_chain_id=$2::numeric AND destination_chain_id=$3::numeric AND message_id=$4`, key.Protocol, key.SourceChainID.String(), key.DestinationChainID.String(), bytesOf(key.MessageID)).Scan(&id, &state, &snap.Revision)
	if err != nil {
		return snap, err
	}
	snap.State = domain.State(state)
	rows, err := s.pool.Query(ctx, `SELECT role,payload_hash,sender,recipient,token,amount::text,execution_status,protocol_version FROM message_claims WHERE message_pk=$1 ORDER BY observation_id`, id)
	if err != nil {
		return snap, err
	}
	var sources []domain.SourceClaim
	var destinations []domain.DestinationClaim
	for rows.Next() {
		var role string
		var payload, sender, recipient, token []byte
		var amount, status sql.NullString
		var version uint32
		if err = rows.Scan(&role, &payload, &sender, &recipient, &token, &amount, &status, &version); err != nil {
			rows.Close()
			return snap, err
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
					rows.Close()
					return snap, errors.New("stored amount invalid")
				}
				claim.Amount = n
			}
			sources = append(sources, claim)
		} else {
			destinations = append(destinations, domain.DestinationClaim{Key: key, Recipient: toAddress(recipient), PayloadHash: toHash(payload), Success: status.String == "SUCCESS", ProtocolVersion: version})
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return snap, err
	}
	snap.Projection = domain.Project(sources, destinations)
	snap.SourceClaims = int64(len(sources))
	snap.DestinationClaims = int64(len(destinations))
	err = s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM message_claims WHERE message_pk=$1),(SELECT count(*) FROM message_transitions WHERE message_pk=$1),(SELECT count(*) FROM message_anomalies WHERE message_pk=$1 AND kind='CONFLICT' AND resolved_at IS NULL),(SELECT count(*) FROM message_anomalies WHERE message_pk=$1 AND kind='DUPLICATE_EXECUTION' AND resolved_at IS NULL)`, id).Scan(&snap.Observations, &snap.Transitions, &snap.Conflicts, &snap.DuplicateEpisodes)
	return snap, err
}

// LoadUnmatchedDestinations provides a bounded restart reconciliation queue.
func (s *Postgres) LoadUnmatchedDestinations(ctx context.Context, limit int) ([]domain.MessageKey, error) {
	if limit < 1 || limit > 1000 {
		return nil, errors.New("limit must be between 1 and 1000")
	}
	rows, err := s.pool.Query(ctx, `SELECT protocol,source_chain_id::text,destination_chain_id::text,message_id FROM cross_chain_messages WHERE state='UNMATCHED_DESTINATION' ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []domain.MessageKey
	for rows.Next() {
		var protocol, source, destination string
		var message []byte
		if err = rows.Scan(&protocol, &source, &destination, &message); err != nil {
			return nil, err
		}
		s, err := domain.ChainIDFromDecimal(source)
		if err != nil {
			return nil, err
		}
		d, err := domain.ChainIDFromDecimal(destination)
		if err != nil {
			return nil, err
		}
		keys = append(keys, domain.MessageKey{Protocol: protocol, SourceChainID: s, DestinationChainID: d, MessageID: toHash(message)})
	}
	return keys, rows.Err()
}

func (s *Postgres) IngestSource(ctx context.Context, claim domain.SourceClaim) (IngestResult, error) {
	if err := claim.Validate(); err != nil {
		return IngestResult{}, err
	}
	return s.ingest(ctx, claim.Key, claim.Observation, "SOURCE", &claim, nil)
}
func (s *Postgres) IngestDestination(ctx context.Context, claim domain.DestinationClaim) (IngestResult, error) {
	if err := claim.Validate(); err != nil {
		return IngestResult{}, err
	}
	return s.ingest(ctx, claim.Key, claim.Observation, "DESTINATION", nil, &claim)
}

func (s *Postgres) ingest(ctx context.Context, key domain.MessageKey, observation domain.Observation, role string, source *domain.SourceClaim, dest *domain.DestinationClaim) (IngestResult, error) {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		result, err := s.ingestOnce(ctx, key, observation, role, source, dest)
		if err == nil {
			return result, nil
		}
		last = err
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || (pgerr.Code != "40001" && pgerr.Code != "40P01") {
			return IngestResult{}, err
		}
		select {
		case <-ctx.Done():
			return IngestResult{}, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return IngestResult{}, fmt.Errorf("ingestion retry exhausted: %w", last)
}

func (s *Postgres) ingestOnce(ctx context.Context, key domain.MessageKey, o domain.Observation, role string, source *domain.SourceClaim, dest *domain.DestinationClaim) (result IngestResult, err error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return result, fmt.Errorf("begin ingestion: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", key.LockID()); err != nil {
		return result, fmt.Errorf("lock message: %w", err)
	}
	if err = upsertBlock(ctx, tx, o); err != nil {
		return result, err
	}
	obsID, duplicate, conflict, err := insertObservation(ctx, tx, o)
	if err != nil {
		return result, err
	}
	if conflict {
		var messagePK int64
		err = tx.QueryRow(ctx, "SELECT message_pk FROM message_claims WHERE observation_id=$1 LIMIT 1", obsID).Scan(&messagePK)
		if err != nil {
			return result, fmt.Errorf("find conflicting claim: %w", err)
		}
		topics := topicBytes(o.Topics)
		_, err = tx.Exec(ctx, `INSERT INTO observation_conflicts (observation_id,message_pk,incoming_digest,incoming_topics,incoming_data) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (observation_id,incoming_digest) DO NOTHING`, obsID, messagePK, bytesOf(o.Digest()), topics, o.Data)
		if err != nil {
			return result, fmt.Errorf("record observation conflict: %w", err)
		}
		if err = openAnomaly(ctx, tx, messagePK, "CONFLICT", "same observation key with differing bytes"); err != nil {
			return result, err
		}
		if err = tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit observation conflict: %w", err)
		}
		return IngestResult{Result: ConflictingObservation, Conflict: true}, nil
	}
	if duplicate {
		var existingRole, existingState, existingProtocol, sourceChain, destinationChain string
		var existingMessageID []byte
		var messagePK int64
		err = tx.QueryRow(ctx, `SELECT mc.message_pk,mc.role,m.state,m.protocol,m.source_chain_id::text,m.destination_chain_id::text,m.message_id FROM message_claims mc JOIN cross_chain_messages m ON m.id=mc.message_pk WHERE mc.observation_id=$1`, obsID).Scan(&messagePK, &existingRole, &existingState, &existingProtocol, &sourceChain, &destinationChain, &existingMessageID)
		if err != nil {
			return result, fmt.Errorf("load duplicate claim: %w", err)
		}
		if existingRole != role || existingProtocol != key.Protocol || sourceChain != key.SourceChainID.String() || destinationChain != key.DestinationChainID.String() || !bytes.Equal(existingMessageID, key.MessageID[:]) {
			return result, errors.New("same observation decoded to a different message claim")
		}
		sources, destinations, err := loadClaims(ctx, tx, messagePK, key)
		if err != nil {
			return result, err
		}
		projection := domain.Project(sources, destinations)
		if err = tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit duplicate: %w", err)
		}
		return IngestResult{Result: IdenticalDuplicate, State: domain.State(existingState), Correlated: projection.Correlated, Conflict: projection.Conflict}, nil
	}
	messagePK, previous, revision, err := ensureMessage(ctx, tx, key)
	if err != nil {
		return result, err
	}
	if err = insertClaim(ctx, tx, messagePK, obsID, role, source, dest); err != nil {
		return result, err
	}
	sources, destinations, err := loadClaims(ctx, tx, messagePK, key)
	if err != nil {
		return result, err
	}
	projection := domain.Project(sources, destinations)
	if err = projection.Validate(); err != nil {
		return result, err
	}
	if projection.Conflict {
		if err = openAnomaly(ctx, tx, messagePK, "CONFLICT", "incompatible claims for normalized message identity"); err != nil {
			return result, err
		}
	}
	if projection.DuplicateExecution {
		if err = openAnomaly(ctx, tx, messagePK, "DUPLICATE_EXECUTION", "distinct destination execution observations"); err != nil {
			return result, err
		}
	}
	if previous != projection.State {
		var sender, recipient, payload, token []byte
		var amount any
		if len(sources) > 0 {
			sender = bytesOfAddress(sources[0].Sender)
			recipient = bytesOfAddress(sources[0].Recipient)
			payload = bytesOf(sources[0].PayloadHash)
			if sources[0].Token != nil {
				token = bytesOfAddress(*sources[0].Token)
				amount = sources[0].Amount.String()
			}
		}
		_, err = tx.Exec(ctx, `UPDATE cross_chain_messages SET state=$1,state_revision=$2,sender=COALESCE(sender,$3),recipient=COALESCE(recipient,$4),payload_hash=COALESCE(payload_hash,$5),token=COALESCE(token,$6),amount=COALESCE(amount,$7::numeric),updated_at=now() WHERE id=$8`, string(projection.State), revision+1, sender, recipient, payload, token, amount, messagePK)
		if err != nil {
			return result, fmt.Errorf("update projection: %w", err)
		}
		var from any = string(previous)
		if revision == 0 {
			from = nil
		}
		_, err = tx.Exec(ctx, `INSERT INTO message_transitions(message_pk,revision,from_state,to_state,reason,evidence_observation_id) VALUES($1,$2,$3,$4,$5,$6)`, messagePK, revision+1, from, string(projection.State), "evidence recompute", obsID)
		if err != nil {
			return result, fmt.Errorf("record transition: %w", err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit ingestion: %w", err)
	}
	return IngestResult{Result: Inserted, State: projection.State, Correlated: projection.Correlated, Conflict: projection.Conflict}, nil
}

func upsertBlock(ctx context.Context, tx pgx.Tx, o domain.Observation) error {
	_, err := tx.Exec(ctx, `INSERT INTO chain_blocks(chain_id,block_hash,block_number,parent_hash,canonical) VALUES($1::numeric,$2,$3,$4,false) ON CONFLICT (chain_id,block_hash) DO NOTHING`, o.Key.ChainID.String(), bytesOf(o.Key.BlockHash), int64(o.BlockNumber), bytesOf(o.ParentHash))
	if err != nil {
		return fmt.Errorf("insert block: %w", err)
	}
	var number int64
	var parent []byte
	err = tx.QueryRow(ctx, `SELECT block_number,parent_hash FROM chain_blocks WHERE chain_id=$1::numeric AND block_hash=$2`, o.Key.ChainID.String(), bytesOf(o.Key.BlockHash)).Scan(&number, &parent)
	if err != nil {
		return fmt.Errorf("load block: %w", err)
	}
	if number != int64(o.BlockNumber) || !bytes.Equal(parent, o.ParentHash[:]) {
		return errors.New("block hash has conflicting metadata")
	}
	return nil
}

func insertObservation(ctx context.Context, tx pgx.Tx, o domain.Observation) (id int64, duplicate, conflict bool, err error) {
	digest := o.Digest()
	topics := topicBytes(o.Topics)
	err = tx.QueryRow(ctx, `INSERT INTO message_observations(chain_id,block_hash,tx_hash,log_index,emitter,event_signature,topics,log_data,raw_digest) VALUES($1::numeric,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (chain_id,block_hash,tx_hash,log_index) DO NOTHING RETURNING id`, o.Key.ChainID.String(), bytesOf(o.Key.BlockHash), bytesOf(o.Key.TxHash), int64(o.Key.LogIndex), bytesOfAddress(o.Emitter), bytesOf(o.Topics[0]), topics, o.Data, bytesOf(digest)).Scan(&id)
	if err == nil {
		return id, false, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, false, fmt.Errorf("insert observation: %w", err)
	}
	var existingDigest, emitter, data []byte
	var existingTopics [][]byte
	err = tx.QueryRow(ctx, `SELECT id,raw_digest,emitter,topics,log_data FROM message_observations WHERE chain_id=$1::numeric AND block_hash=$2 AND tx_hash=$3 AND log_index=$4`, o.Key.ChainID.String(), bytesOf(o.Key.BlockHash), bytesOf(o.Key.TxHash), int64(o.Key.LogIndex)).Scan(&id, &existingDigest, &emitter, &existingTopics, &data)
	if err != nil {
		return 0, false, false, fmt.Errorf("load observation: %w", err)
	}
	identical := bytes.Equal(existingDigest, digest[:]) && bytes.Equal(emitter, o.Emitter[:]) && bytes.Equal(data, o.Data) && len(existingTopics) == len(topics)
	if identical {
		for i := range topics {
			if !bytes.Equal(existingTopics[i], topics[i]) {
				identical = false
				break
			}
		}
	}
	return id, identical, !identical, nil
}

func ensureMessage(ctx context.Context, tx pgx.Tx, key domain.MessageKey) (id int64, state domain.State, revision int64, err error) {
	_, err = tx.Exec(ctx, `INSERT INTO cross_chain_messages(protocol,source_chain_id,destination_chain_id,message_id,state) VALUES($1,$2::numeric,$3::numeric,$4,'NO_CANONICAL_EVIDENCE') ON CONFLICT (protocol,source_chain_id,destination_chain_id,message_id) DO NOTHING`, key.Protocol, key.SourceChainID.String(), key.DestinationChainID.String(), bytesOf(key.MessageID))
	if err != nil {
		return 0, "", 0, fmt.Errorf("ensure message: %w", err)
	}
	var raw string
	err = tx.QueryRow(ctx, `SELECT id,state,state_revision FROM cross_chain_messages WHERE protocol=$1 AND source_chain_id=$2::numeric AND destination_chain_id=$3::numeric AND message_id=$4 FOR UPDATE`, key.Protocol, key.SourceChainID.String(), key.DestinationChainID.String(), bytesOf(key.MessageID)).Scan(&id, &raw, &revision)
	return id, domain.State(raw), revision, err
}

func insertClaim(ctx context.Context, tx pgx.Tx, messagePK, obsID int64, role string, source *domain.SourceClaim, dest *domain.DestinationClaim) error {
	var payload, sender, recipient, token []byte
	var amount, status any
	var version uint32
	if source != nil {
		payload = bytesOf(source.PayloadHash)
		sender = bytesOfAddress(source.Sender)
		recipient = bytesOfAddress(source.Recipient)
		version = source.ProtocolVersion
		if source.Token != nil {
			token = bytesOfAddress(*source.Token)
			amount = source.Amount.String()
		}
	} else {
		payload = bytesOf(dest.PayloadHash)
		recipient = bytesOfAddress(dest.Recipient)
		version = dest.ProtocolVersion
		if dest.Success {
			status = "SUCCESS"
		} else {
			status = "FAILURE"
		}
	}
	_, err := tx.Exec(ctx, `INSERT INTO message_claims(message_pk,observation_id,role,payload_hash,sender,recipient,token,amount,execution_status,protocol_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8::numeric,$9,$10)`, messagePK, obsID, role, payload, sender, recipient, token, amount, status, version)
	if err != nil {
		return fmt.Errorf("insert claim: %w", err)
	}
	return nil
}

func loadClaims(ctx context.Context, tx pgx.Tx, messagePK int64, key domain.MessageKey) ([]domain.SourceClaim, []domain.DestinationClaim, error) {
	rows, err := tx.Query(ctx, `SELECT role,payload_hash,sender,recipient,token,amount::text,execution_status,protocol_version FROM message_claims WHERE message_pk=$1 ORDER BY observation_id`, messagePK)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var sources []domain.SourceClaim
	var destinations []domain.DestinationClaim
	for rows.Next() {
		var role string
		var payload, sender, recipient, token []byte
		var amount, status sql.NullString
		var version uint32
		if err = rows.Scan(&role, &payload, &sender, &recipient, &token, &amount, &status, &version); err != nil {
			return nil, nil, err
		}
		if role == "SOURCE" {
			s := domain.SourceClaim{Key: key, Sender: toAddress(sender), Recipient: toAddress(recipient), PayloadHash: toHash(payload), ProtocolVersion: version}
			if len(token) > 0 {
				t := toAddress(token)
				s.Token = &t
			}
			if amount.Valid {
				n, ok := new(big.Int).SetString(amount.String, 10)
				if !ok {
					return nil, nil, errors.New("stored amount invalid")
				}
				s.Amount = n
			}
			sources = append(sources, s)
		} else {
			destinations = append(destinations, domain.DestinationClaim{Key: key, Recipient: toAddress(recipient), PayloadHash: toHash(payload), Success: status.String == "SUCCESS", ProtocolVersion: version})
		}
	}
	return sources, destinations, rows.Err()
}

func openAnomaly(ctx context.Context, tx pgx.Tx, messagePK int64, kind, reason string) error {
	_, err := tx.Exec(ctx, `INSERT INTO message_anomalies(message_pk,kind,episode,reason)
		SELECT $1,$2,COALESCE(MAX(episode),0)+1,$3 FROM message_anomalies WHERE message_pk=$1 AND kind=$2
		ON CONFLICT (message_pk,kind) WHERE resolved_at IS NULL DO NOTHING`, messagePK, kind, reason)
	if err != nil {
		return fmt.Errorf("record anomaly: %w", err)
	}
	return nil
}

func bytesOf(hash domain.Hash) []byte              { return append([]byte(nil), hash[:]...) }
func bytesOfAddress(address domain.Address) []byte { return append([]byte(nil), address[:]...) }
func topicBytes(topics []domain.Hash) [][]byte {
	result := make([][]byte, len(topics))
	for i := range topics {
		result[i] = bytesOf(topics[i])
	}
	return result
}
func toAddress(raw []byte) domain.Address { var a domain.Address; copy(a[:], raw); return a }
func toHash(raw []byte) domain.Hash       { var h domain.Hash; copy(h[:], raw); return h }
