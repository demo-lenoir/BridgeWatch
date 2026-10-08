package source

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"bridgewatch/internal/clock"
	"bridgewatch/internal/domain"
	"bridgewatch/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool    *pgxpool.Pool
	claims  *store.Postgres
	metrics *Metrics
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool, claims: store.NewPostgres(pool)}
}
func (r *PostgresRepository) SetMetrics(metrics *Metrics) { r.metrics = metrics }
func (r *PostgresRepository) SetClock(value clock.Clock)  { r.claims.SetClock(value) }
func rawHash(h domain.Hash) []byte                        { return append([]byte(nil), h[:]...) }
func hashOf(raw []byte) domain.Hash                       { var h domain.Hash; copy(h[:], raw); return h }

func (r *PostgresRepository) Initialize(ctx context.Context, c Config, genesis, predecessor Header) (Checkpoint, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Checkpoint{}, err
	}
	defer tx.Rollback(ctx)
	var contract, anchor []byte
	var start, conf, maxDepth int64
	err = tx.QueryRow(ctx, `SELECT contract_address,start_block,genesis_hash,confirmations,max_reorg_depth FROM source_streams WHERE chain_id=$1::numeric FOR UPDATE`, c.SourceChainID.String()).Scan(&contract, &start, &anchor, &conf, &maxDepth)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(ctx, `INSERT INTO source_streams(chain_id,contract_address,start_block,genesis_hash,confirmations,max_reorg_depth,status) VALUES($1::numeric,$2,$3,$4,$5,$6,'HEALTHY')`, c.SourceChainID.String(), c.Contract.Bytes(), int64(c.StartBlock), rawHash(genesis.Hash), int64(c.Confirmations), int64(c.MaxReorgDepth))
		if err != nil {
			return Checkpoint{}, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO chain_blocks(chain_id,block_hash,block_number,parent_hash,canonical) VALUES($1::numeric,$2,$3,$4,true) ON CONFLICT(chain_id,block_hash) DO NOTHING`, c.SourceChainID.String(), rawHash(predecessor.Hash), int64(predecessor.Number), rawHash(predecessor.ParentHash))
		if err != nil {
			return Checkpoint{}, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO chain_checkpoints(chain_id,block_hash,block_number,health,finality_policy_version) VALUES($1::numeric,$2,$3,'HEALTHY',1)`, c.SourceChainID.String(), rawHash(predecessor.Hash), int64(predecessor.Number))
		if err != nil {
			return Checkpoint{}, err
		}
	} else if err != nil {
		return Checkpoint{}, err
	} else if !bytes.Equal(contract, c.Contract.Bytes()) || start != int64(c.StartBlock) || !bytes.Equal(anchor, genesis.Hash[:]) || conf != int64(c.Confirmations) || maxDepth != int64(c.MaxReorgDepth) {
		return Checkpoint{}, errors.New("source stream configuration changed; explicit rescan required")
	}
	var cp Checkpoint
	var cpHash []byte
	err = tx.QueryRow(ctx, `SELECT block_number,block_hash,health FROM chain_checkpoints WHERE chain_id=$1::numeric FOR UPDATE`, c.SourceChainID.String()).Scan(&cp.Number, &cpHash, &cp.Health)
	if err != nil {
		return cp, err
	}
	cp.Hash = hashOf(cpHash)
	if err = tx.Commit(ctx); err != nil {
		return cp, err
	}
	return cp, nil
}
func (r *PostgresRepository) Checkpoint(ctx context.Context, chain domain.ChainID) (Checkpoint, error) {
	var cp Checkpoint
	var raw []byte
	err := r.pool.QueryRow(ctx, `SELECT block_number,block_hash,health FROM chain_checkpoints WHERE chain_id=$1::numeric`, chain.String()).Scan(&cp.Number, &raw, &cp.Health)
	cp.Hash = hashOf(raw)
	return cp, err
}
func (r *PostgresRepository) CanonicalHashAt(ctx context.Context, chain domain.ChainID, number uint64) (domain.Hash, error) {
	var raw []byte
	err := r.pool.QueryRow(ctx, `SELECT block_hash FROM chain_blocks WHERE chain_id=$1::numeric AND block_number=$2 AND canonical`, chain.String(), int64(number)).Scan(&raw)
	return hashOf(raw), err
}
func (r *PostgresRepository) SetHealth(ctx context.Context, chain domain.ChainID, health string) error {
	if health != "HEALTHY" && health != "DEGRADED" {
		return errors.New("invalid source health")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `UPDATE source_streams SET status=$2,updated_at=now() WHERE chain_id=$1::numeric AND status<>'MANUAL_INTERVENTION'`, chain.String(), health); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE chain_checkpoints SET health=$2,updated_at=now() WHERE chain_id=$1::numeric AND health<>'MANUAL_INTERVENTION'`, chain.String(), health); err != nil {
		return err
	}
	if health == "HEALTHY" {
		if _, err = tx.Exec(ctx, `UPDATE chain_incidents SET resolved_at=now() WHERE chain_id=$1::numeric AND role='SOURCE' AND resolved_at IS NULL AND EXISTS (SELECT 1 FROM chain_checkpoints cp JOIN source_streams s USING(chain_id) WHERE cp.chain_id=$1::numeric AND cp.health='HEALTHY' AND s.status='HEALTHY')`, chain.String()); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (r *PostgresRepository) Ready(ctx context.Context, chain domain.ChainID) (bool, error) {
	if err := r.pool.Ping(ctx); err != nil {
		return false, err
	}
	var healthy bool
	err := r.pool.QueryRow(ctx, `SELECT cp.health='HEALTHY' AND s.status='HEALTHY' AND b.canonical AND b.block_number=cp.block_number AND NOT EXISTS (SELECT 1 FROM chain_incidents ci WHERE ci.chain_id=cp.chain_id AND ci.role='SOURCE' AND ci.resolved_at IS NULL) FROM chain_checkpoints cp JOIN source_streams s USING(chain_id) JOIN chain_blocks b ON b.chain_id=cp.chain_id AND b.block_hash=cp.block_hash WHERE cp.chain_id=$1::numeric`, chain.String()).Scan(&healthy)
	return healthy, err
}
func (r *PostgresRepository) MarkDeepReorg(ctx context.Context, c Config, cp Checkpoint, head Header, depth uint64) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var reorgID int64
	err = tx.QueryRow(ctx, `INSERT INTO source_reorgs(chain_id,old_head_hash,proposed_head_hash,depth,severity,disposition) VALUES($1::numeric,$2,$3,$4,'CRITICAL','MANUAL_INTERVENTION') RETURNING id`, c.SourceChainID.String(), rawHash(cp.Hash), rawHash(head.Hash), int64(depth)).Scan(&reorgID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE source_streams SET status='MANUAL_INTERVENTION',updated_at=now() WHERE chain_id=$1::numeric`, c.SourceChainID.String())
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE chain_checkpoints SET health='MANUAL_INTERVENTION',updated_at=now() WHERE chain_id=$1::numeric`, c.SourceChainID.String())
	if err != nil {
		return err
	}
	if err = r.claims.RecordManualIncidentTx(ctx, tx, c.SourceChainID, "SOURCE", &reorgID, nil); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	slog.Warn("manual intervention", "chain", c.SourceChainID.String(), "role", "SOURCE", "reorg_id", reorgID)
	return nil
}

func (r *PostgresRepository) Apply(ctx context.Context, c Config, ancestor Header, blocks []BlockEvidence, reorg bool) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		err := r.applyOnce(ctx, c, ancestor, blocks, reorg)
		if err == nil {
			return nil
		}
		last = err
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || (pgerr.Code != "40001" && pgerr.Code != "40P01") {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return fmt.Errorf("source apply retry exhausted: %w", last)
}

func (r *PostgresRepository) applyOnce(ctx context.Context, c Config, ancestor Header, blocks []BlockEvidence, reorg bool) error {
	if len(blocks) == 0 || len(blocks) > 64 {
		return errors.New("invalid source batch size")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var cpNumber int64
	var cpHash []byte
	var health string
	err = tx.QueryRow(ctx, `SELECT block_number,block_hash,health FROM chain_checkpoints WHERE chain_id=$1::numeric FOR UPDATE`, c.SourceChainID.String()).Scan(&cpNumber, &cpHash, &health)
	if err != nil {
		return err
	}
	if health == "MANUAL_INTERVENTION" {
		return ErrDeepReorg
	}
	if (!reorg && (uint64(cpNumber) != ancestor.Number || !bytes.Equal(cpHash, ancestor.Hash[:]))) || (reorg && ancestor.Number >= uint64(cpNumber)) {
		return errors.New("source checkpoint changed during fetch")
	}
	var storedAncestor []byte
	err = tx.QueryRow(ctx, `SELECT block_hash FROM chain_blocks WHERE chain_id=$1::numeric AND block_number=$2 AND canonical`, c.SourceChainID.String(), int64(ancestor.Number)).Scan(&storedAncestor)
	if err != nil || !bytes.Equal(storedAncestor, ancestor.Hash[:]) {
		return errors.New("source ancestor is not durable canonical evidence")
	}
	affected := map[int64]struct{}{}
	var reorgID *int64
	if reorg {
		rows, err := tx.Query(ctx, `SELECT DISTINCT mc.message_pk FROM message_claims mc JOIN message_observations o ON o.id=mc.observation_id JOIN chain_blocks b ON b.chain_id=o.chain_id AND b.block_hash=o.block_hash WHERE mc.role='SOURCE' AND b.chain_id=$1::numeric AND b.canonical AND b.block_number>$2`, c.SourceChainID.String(), int64(ancestor.Number))
		if err != nil {
			return err
		}
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			affected[id] = struct{}{}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE chain_blocks SET canonical=false WHERE chain_id=$1::numeric AND canonical AND block_number>$2`, c.SourceChainID.String(), int64(ancestor.Number))
		if err != nil {
			return err
		}
		var id int64
		err = tx.QueryRow(ctx, `INSERT INTO source_reorgs(chain_id,old_head_hash,proposed_head_hash,ancestor_hash,depth,severity,disposition) VALUES($1::numeric,$2,$3,$4,$5,'WARNING','RECONCILED') RETURNING id`, c.SourceChainID.String(), cpHash, rawHash(blocks[len(blocks)-1].Header.Hash), rawHash(ancestor.Hash), cpNumber-int64(ancestor.Number)).Scan(&id)
		if err != nil {
			return err
		}
		reorgID = &id
	}
	parent := ancestor.Hash
	number := ancestor.Number + 1
	for _, block := range blocks {
		h := block.Header
		if h.Number != number || h.ParentHash != parent || h.Hash == (domain.Hash{}) {
			return errors.New("invalid source batch ancestry")
		}
		_, err = tx.Exec(ctx, `INSERT INTO chain_blocks(chain_id,block_hash,block_number,parent_hash,canonical) VALUES($1::numeric,$2,$3,$4,false) ON CONFLICT(chain_id,block_hash) DO NOTHING`, c.SourceChainID.String(), rawHash(h.Hash), int64(h.Number), rawHash(h.ParentHash))
		if err != nil {
			return err
		}
		var storedNumber int64
		var storedParent []byte
		err = tx.QueryRow(ctx, `SELECT block_number,parent_hash FROM chain_blocks WHERE chain_id=$1::numeric AND block_hash=$2`, c.SourceChainID.String(), rawHash(h.Hash)).Scan(&storedNumber, &storedParent)
		if err != nil || storedNumber != int64(h.Number) || !bytes.Equal(storedParent, h.ParentHash[:]) {
			return errors.New("source block hash metadata conflict")
		}
		_, err = tx.Exec(ctx, `UPDATE chain_blocks SET canonical=true WHERE chain_id=$1::numeric AND block_hash=$2`, c.SourceChainID.String(), rawHash(h.Hash))
		if err != nil {
			return err
		}
		for _, claim := range block.Claims {
			if claim.Observation.Key.ChainID != c.SourceChainID || claim.Observation.Key.BlockHash != h.Hash || claim.Observation.BlockNumber != h.Number || claim.Observation.ParentHash != h.ParentHash {
				return errors.New("source claim does not belong to batch block")
			}
			pk, _, err := r.claims.PersistSourceClaimTx(ctx, tx, claim)
			if err != nil {
				return err
			}
			affected[pk] = struct{}{}
		}
		parent = h.Hash
		number++
	}
	head := blocks[len(blocks)-1].Header
	_, err = tx.Exec(ctx, `UPDATE chain_checkpoints SET block_hash=$2,block_number=$3,health='HEALTHY',updated_at=now() WHERE chain_id=$1::numeric`, c.SourceChainID.String(), rawHash(head.Hash), int64(head.Number))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE source_streams SET status='HEALTHY',updated_at=now() WHERE chain_id=$1::numeric AND status<>'MANUAL_INTERVENTION'`, c.SourceChainID.String())
	if err != nil {
		return err
	}
	if head.Number >= c.Confirmations-1 {
		threshold := head.Number - (c.Confirmations - 1)
		rows, err := tx.Query(ctx, `SELECT DISTINCT mc.message_pk FROM message_claims mc JOIN message_observations o ON o.id=mc.observation_id JOIN chain_blocks b ON b.chain_id=o.chain_id AND b.block_hash=o.block_hash JOIN cross_chain_messages m ON m.id=mc.message_pk WHERE mc.role='SOURCE' AND b.canonical AND b.chain_id=$1::numeric AND b.block_number<=$2 AND m.state='SOURCE_SEEN'`, c.SourceChainID.String(), int64(threshold))
		if err != nil {
			return err
		}
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			affected[id] = struct{}{}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
	}
	changes := make([]store.ProjectionChange, 0, len(affected))
	for id := range affected {
		change, recomputeErr := r.claims.RecomputeCanonicalMessageTx(ctx, tx, id, store.RecomputeCause{Reason: "canonical source recompute", SourceReorgID: reorgID})
		err = recomputeErr
		if err != nil {
			return err
		}
		changes = append(changes, change)
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	for _, change := range changes {
		if change.ResolvedStuck {
			slog.Info("stuck resolved", "message_pk", change.MessagePK, "state", change.To)
		}
		if change.Changed {
			if change.To == domain.Completed {
				slog.Info("message completed", "message_pk", change.MessagePK, "chain_pair", change.ChainPair)
			}
			r.metrics.ObserveState(change.Protocol, change.To, change.DeliverySeconds, change.ChainPair)
		}
	}
	return nil
}
