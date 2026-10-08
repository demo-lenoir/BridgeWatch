package recovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"

	"bridgewatch/internal/clock"
	"bridgewatch/internal/domain"
	"bridgewatch/internal/store"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Header struct {
	Number           uint64
	Hash, ParentHash domain.Hash
}
type Reader interface {
	ChainID(context.Context) (domain.ChainID, error)
	BlockNumber(context.Context) (uint64, error)
	HeaderByNumber(context.Context, uint64) (Header, error)
	HeaderByHash(context.Context, domain.Hash) (Header, error)
	FilterLogs(context.Context, uint64, uint64, common.Address) error
}

type Request struct {
	Role                                     string
	ChainID                                  domain.ChainID
	Contract                                 common.Address
	Genesis                                  domain.Hash
	StartBlock, Confirmations, MaxReorgDepth uint64
	ExpectedCheckpoint                       uint64
	ExpectedCheckpointHash                   domain.Hash
	Ancestor                                 uint64
	AncestorHash                             domain.Hash
}

func (r Request) Validate() error {
	if (r.Role != "SOURCE" && r.Role != "DESTINATION") || !r.ChainID.Valid() || r.Contract == (common.Address{}) || r.Genesis == (domain.Hash{}) || r.StartBlock < 1 || r.StartBlock > math.MaxInt64 || r.Confirmations < 1 || r.Confirmations > math.MaxInt64 || r.MaxReorgDepth < 1 || r.MaxReorgDepth > 64 || r.ExpectedCheckpoint > math.MaxInt64 || r.ExpectedCheckpointHash == (domain.Hash{}) || r.AncestorHash == (domain.Hash{}) || r.Ancestor < r.StartBlock-1 || r.Ancestor > r.ExpectedCheckpoint || r.ExpectedCheckpoint-r.Ancestor > 100000 {
		return errors.New("invalid manual recovery request")
	}
	return nil
}

type Plan struct {
	Role                  string `json:"role"`
	ChainID               string `json:"chain_id"`
	CurrentCheckpoint     uint64 `json:"current_checkpoint"`
	CurrentCheckpointHash string `json:"current_checkpoint_hash"`
	Ancestor              uint64 `json:"ancestor"`
	AncestorHash          string `json:"ancestor_hash"`
	AffectedBlocks        uint64 `json:"affected_blocks"`
	AffectedMessages      int64  `json:"affected_messages"`
	ProjectionCandidates  int64  `json:"projection_candidates"`
	AlreadyRecovered      bool   `json:"already_recovered"`
	Executed              bool   `json:"executed"`
}

type Service struct {
	db        *pgxpool.Pool
	reader    Reader
	clock     clock.Clock
	projector *store.Postgres
}

func New(db *pgxpool.Pool, reader Reader, businessClock clock.Clock) (*Service, error) {
	if db == nil || reader == nil || businessClock == nil {
		return nil, errors.New("recovery dependencies required")
	}
	projector := store.NewPostgres(db)
	projector.SetClock(businessClock)
	return &Service{db: db, reader: reader, clock: businessClock, projector: projector}, nil
}

func tables(role string) (stream, reorg, claimRole string) {
	if role == "SOURCE" {
		return "source_streams", "source_reorgs", "SOURCE"
	}
	return "destination_streams", "destination_reorgs", "DESTINATION"
}

func (s *Service) verifyRemote(ctx context.Context, r Request) error {
	chain, err := s.reader.ChainID(ctx)
	if err != nil || chain != r.ChainID {
		return errors.New("recovery endpoint chain mismatch")
	}
	genesis, err := s.reader.HeaderByNumber(ctx, 0)
	if err != nil || genesis.Number != 0 || genesis.Hash != r.Genesis {
		return errors.New("recovery endpoint anchor mismatch")
	}
	byHash, err := s.reader.HeaderByHash(ctx, genesis.Hash)
	if err != nil || byHash != genesis {
		return errors.New("recovery endpoint genesis inconsistency")
	}
	head, err := s.reader.BlockNumber(ctx)
	if err != nil || head < r.Ancestor {
		return errors.New("recovery endpoint is stale")
	}
	ancestor, err := s.reader.HeaderByNumber(ctx, r.Ancestor)
	if err != nil || ancestor.Number != r.Ancestor || ancestor.Hash != r.AncestorHash {
		return errors.New("recovery ancestor is not on selected endpoint branch")
	}
	byHash, err = s.reader.HeaderByHash(ctx, ancestor.Hash)
	if err != nil || byHash != ancestor {
		return errors.New("recovery ancestor header inconsistency")
	}
	if err = s.reader.FilterLogs(ctx, r.Ancestor, r.Ancestor, r.Contract); err != nil {
		return fmt.Errorf("recovery endpoint log method: %w", err)
	}
	return nil
}

func (s *Service) inspect(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, r Request) (Plan, int64, error) {
	stream, reorg, claimRole := tables(r.Role)
	p := Plan{Role: r.Role, ChainID: r.ChainID.String(), Ancestor: r.Ancestor, AncestorHash: fmt.Sprintf("0x%x", r.AncestorHash)}
	var contract, genesis, cpHash []byte
	var start, confirmations, depth, cpNumber int64
	var streamHealth, checkpointHealth string
	err := q.QueryRow(ctx, `SELECT contract_address,genesis_hash,start_block,confirmations,max_reorg_depth,status FROM `+stream+` WHERE chain_id=$1::numeric`, r.ChainID.String()).Scan(&contract, &genesis, &start, &confirmations, &depth, &streamHealth)
	if err != nil {
		return p, 0, err
	}
	if !bytes.Equal(contract, r.Contract.Bytes()) || !bytes.Equal(genesis, r.Genesis[:]) || start != int64(r.StartBlock) || confirmations != int64(r.Confirmations) || depth != int64(r.MaxReorgDepth) {
		return p, 0, errors.New("recovery stream configuration differs")
	}
	err = q.QueryRow(ctx, `SELECT block_number,block_hash,health FROM chain_checkpoints WHERE chain_id=$1::numeric`, r.ChainID.String()).Scan(&cpNumber, &cpHash, &checkpointHealth)
	if err != nil {
		return p, 0, err
	}
	p.CurrentCheckpoint = uint64(cpNumber)
	p.CurrentCheckpointHash = fmt.Sprintf("0x%x", cpHash)
	if streamHealth != "MANUAL_INTERVENTION" || checkpointHealth != "MANUAL_INTERVENTION" {
		var previous []byte
		err = q.QueryRow(ctx, `SELECT ancestor_hash FROM `+reorg+` WHERE chain_id=$1::numeric AND severity='CRITICAL' AND disposition='MANUAL_INTERVENTION' AND old_head_hash=$2 ORDER BY id DESC LIMIT 1`, r.ChainID.String(), r.ExpectedCheckpointHash[:]).Scan(&previous)
		if err == nil && bytes.Equal(previous, r.AncestorHash[:]) {
			p.AlreadyRecovered = true
			return p, 0, nil
		}
		return p, 0, errors.New("chain is not in manual intervention")
	}
	if uint64(cpNumber) != r.ExpectedCheckpoint || !bytes.Equal(cpHash, r.ExpectedCheckpointHash[:]) {
		return p, 0, errors.New("current checkpoint differs from approved recovery request")
	}
	var criticalID int64
	err = q.QueryRow(ctx, `SELECT id FROM `+reorg+` WHERE chain_id=$1::numeric AND severity='CRITICAL' AND disposition='MANUAL_INTERVENTION' ORDER BY id DESC LIMIT 1`, r.ChainID.String()).Scan(&criticalID)
	if err != nil {
		return p, 0, fmt.Errorf("missing critical reorg: %w", err)
	}
	rows, err := q.Query(ctx, `SELECT block_number,block_hash,parent_hash FROM chain_blocks WHERE chain_id=$1::numeric AND canonical AND block_number BETWEEN $2 AND $3 ORDER BY block_number`, r.ChainID.String(), int64(r.Ancestor), cpNumber)
	if err != nil {
		return p, 0, err
	}
	var previousHash []byte
	var observed uint64
	for rows.Next() {
		var number int64
		var hash, parent []byte
		if err = rows.Scan(&number, &hash, &parent); err != nil {
			rows.Close()
			return p, 0, err
		}
		if number != int64(r.Ancestor+observed) || (observed == 0 && !bytes.Equal(hash, r.AncestorHash[:])) || (observed > 0 && !bytes.Equal(parent, previousHash)) {
			rows.Close()
			return p, 0, errors.New("persisted canonical ancestry is inconsistent")
		}
		previousHash = hash
		observed++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return p, 0, err
	}
	if observed != uint64(cpNumber)-r.Ancestor+1 || !bytes.Equal(previousHash, cpHash) {
		return p, 0, errors.New("persisted checkpoint ancestry is incomplete")
	}
	p.AffectedBlocks = observed - 1
	err = q.QueryRow(ctx, `SELECT count(DISTINCT mc.message_pk) FROM message_claims mc JOIN message_observations o ON o.id=mc.observation_id JOIN chain_blocks b ON b.chain_id=o.chain_id AND b.block_hash=o.block_hash WHERE mc.role=$1 AND b.chain_id=$2::numeric AND b.canonical AND b.block_number>$3`, claimRole, r.ChainID.String(), int64(r.Ancestor)).Scan(&p.AffectedMessages)
	if err != nil {
		return p, 0, err
	}
	if p.AffectedMessages > 100000 {
		return p, 0, errors.New("recovery projection batch exceeds limit")
	}
	p.ProjectionCandidates = p.AffectedMessages
	return p, criticalID, nil
}

func (s *Service) Plan(ctx context.Context, r Request) (Plan, error) {
	if err := r.Validate(); err != nil {
		return Plan{}, err
	}
	if err := s.verifyRemote(ctx, r); err != nil {
		return Plan{}, err
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Plan{}, err
	}
	defer tx.Rollback(ctx)
	p, _, err := s.inspect(ctx, tx, r)
	if err != nil {
		return p, err
	}
	return p, tx.Commit(ctx)
}

func (s *Service) Execute(ctx context.Context, r Request) (Plan, error) {
	if err := r.Validate(); err != nil {
		return Plan{}, err
	}
	if err := s.verifyRemote(ctx, r); err != nil {
		return Plan{}, err
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Plan{}, err
	}
	defer tx.Rollback(ctx)
	// Serialize with watcher checkpoint commits before inspecting and changing the branch.
	var locked int64
	if err = tx.QueryRow(ctx, `SELECT block_number FROM chain_checkpoints WHERE chain_id=$1::numeric FOR UPDATE`, r.ChainID.String()).Scan(&locked); err != nil {
		return Plan{}, err
	}
	p, criticalID, err := s.inspect(ctx, tx, r)
	if err != nil {
		return p, err
	}
	if p.AlreadyRecovered {
		return p, tx.Commit(ctx)
	}
	stream, reorg, claimRole := tables(r.Role)
	rows, err := tx.Query(ctx, `SELECT DISTINCT mc.message_pk FROM message_claims mc JOIN message_observations o ON o.id=mc.observation_id JOIN chain_blocks b ON b.chain_id=o.chain_id AND b.block_hash=o.block_hash WHERE mc.role=$1 AND b.chain_id=$2::numeric AND b.canonical AND b.block_number>$3 ORDER BY mc.message_pk`, claimRole, r.ChainID.String(), int64(r.Ancestor))
	if err != nil {
		return p, err
	}
	ids := make([]int64, 0, p.AffectedMessages)
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return p, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return p, err
	}
	if int64(len(ids)) != p.AffectedMessages {
		return p, errors.New("affected message count changed")
	}
	_, err = tx.Exec(ctx, `UPDATE chain_blocks SET canonical=false WHERE chain_id=$1::numeric AND canonical AND block_number>$2`, r.ChainID.String(), int64(r.Ancestor))
	if err != nil {
		return p, err
	}
	_, err = tx.Exec(ctx, `UPDATE chain_checkpoints SET block_number=$2,block_hash=$3,health='DEGRADED',updated_at=$4 WHERE chain_id=$1::numeric`, r.ChainID.String(), int64(r.Ancestor), r.AncestorHash[:], s.clock.Now().UTC())
	if err != nil {
		return p, err
	}
	_, err = tx.Exec(ctx, `UPDATE `+stream+` SET status='DEGRADED',updated_at=$2 WHERE chain_id=$1::numeric`, r.ChainID.String(), s.clock.Now().UTC())
	if err != nil {
		return p, err
	}
	_, err = tx.Exec(ctx, `UPDATE `+reorg+` SET ancestor_hash=$2 WHERE id=$1`, criticalID, r.AncestorHash[:])
	if err != nil {
		return p, err
	}
	for _, id := range ids {
		cause := store.RecomputeCause{Reason: "manual canonical recovery"}
		if r.Role == "SOURCE" {
			cause.SourceReorgID = &criticalID
		} else {
			cause.DestinationReorgID = &criticalID
		}
		if _, err = s.projector.RecomputeCanonicalMessageTx(ctx, tx, id, cause); err != nil {
			return p, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return p, err
	}
	p.Executed = true
	return p, nil
}
