package operations

import (
	"context"
	"crypto/sha256"
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

type ReadyChecker interface {
	Ready(context.Context) (bool, error)
}

type Policy struct {
	Protocol           string
	SourceChainID      domain.ChainID
	DestinationChainID domain.ChainID
	Version            int
	SLA                time.Duration
}

func (p Policy) Validate() error {
	if p.Protocol == "" || len(p.Protocol) > 64 || !p.SourceChainID.Valid() || !p.DestinationChainID.Valid() || p.SourceChainID == p.DestinationChainID || p.Version < 1 || p.SLA < time.Millisecond || p.SLA > 30*24*time.Hour || p.SLA%time.Millisecond != 0 {
		return errors.New("invalid relay policy")
	}
	return nil
}

type Service struct {
	db          *pgxpool.Pool
	clock       clock.Clock
	source      ReadyChecker
	destination ReadyChecker
	projection  *store.Postgres
	logger      *slog.Logger
	metrics     *Metrics
}

func (s *Service) SetMetrics(metrics *Metrics) { s.metrics = metrics }

func NewService(db *pgxpool.Pool, businessClock clock.Clock, source, destination ReadyChecker, logger *slog.Logger) (*Service, error) {
	if db == nil || businessClock == nil || source == nil || destination == nil {
		return nil, errors.New("operations dependencies required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	projection := store.NewPostgres(db)
	projection.SetClock(businessClock)
	return &Service{db: db, clock: businessClock, source: source, destination: destination, projection: projection, logger: logger}, nil
}

func (s *Service) ConfigurePolicy(ctx context.Context, p Policy) error {
	if err := p.Validate(); err != nil {
		return err
	}
	var version int
	var milliseconds int64
	err := s.db.QueryRow(ctx, `SELECT version,sla_ms FROM relay_policies WHERE protocol=$1 AND source_chain_id=$2::numeric AND destination_chain_id=$3::numeric`, p.Protocol, p.SourceChainID.String(), p.DestinationChainID.String()).Scan(&version, &milliseconds)
	if err == nil {
		if version == p.Version && milliseconds == p.SLA.Milliseconds() {
			s.metrics.TrackPair(p.Protocol, p.SourceChainID.String()+"_"+p.DestinationChainID.String())
			return nil
		}
		if p.Version <= version {
			return errors.New("relay policy change requires a higher version")
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	command, err := s.db.Exec(ctx, `INSERT INTO relay_policies(protocol,source_chain_id,destination_chain_id,version,sla_ms) VALUES($1,$2::numeric,$3::numeric,$4,$5)
		ON CONFLICT(protocol,source_chain_id,destination_chain_id) DO UPDATE SET version=EXCLUDED.version,sla_ms=EXCLUDED.sla_ms,configured_at=now()
		WHERE relay_policies.version<EXCLUDED.version`, p.Protocol, p.SourceChainID.String(), p.DestinationChainID.String(), p.Version, p.SLA.Milliseconds())
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return errors.New("relay policy version changed concurrently")
	}
	s.metrics.TrackPair(p.Protocol, p.SourceChainID.String()+"_"+p.DestinationChainID.String())
	return nil
}

func (s *Service) ready(ctx context.Context) (bool, error) {
	sourceReady, err := s.source.Ready(ctx)
	if err != nil {
		return false, err
	}
	destReady, err := s.destination.Ready(ctx)
	if err != nil {
		return false, err
	}
	return sourceReady && destReady, nil
}

func validBatch(batch int) error {
	if batch < 1 || batch > 500 {
		return errors.New("batch must be between 1 and 500")
	}
	return nil
}

func retryable(err error) bool {
	var pgerr *pgconn.PgError
	return errors.As(err, &pgerr) && (pgerr.Code == "40001" || pgerr.Code == "40P01")
}

func (s *Service) ActivateEligible(ctx context.Context, batch int) (int, error) {
	if err := validBatch(batch); err != nil {
		return 0, err
	}
	ready, err := s.ready(ctx)
	if err != nil || !ready {
		return 0, err
	}
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		count, err := s.activateOnce(ctx, batch)
		if err == nil {
			return count, nil
		}
		if !retryable(err) {
			return 0, err
		}
		last = err
	}
	return 0, fmt.Errorf("activate relay expectation: %w", last)
}

func (s *Service) activateOnce(ctx context.Context, batch int) (int, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT m.id,m.source_final_at,p.sla_ms,p.version FROM cross_chain_messages m JOIN relay_policies p ON p.protocol=m.protocol AND p.source_chain_id=m.source_chain_id AND p.destination_chain_id=m.destination_chain_id
		WHERE m.state='SOURCE_FINAL' AND m.source_final_at IS NOT NULL AND m.relay_eligible_at IS NULL
		ORDER BY m.id LIMIT $1 FOR UPDATE OF m SKIP LOCKED`, batch)
	if err != nil {
		return 0, err
	}
	type candidate struct {
		id      int64
		anchor  time.Time
		sla     int64
		version int
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err = rows.Scan(&c.id, &c.anchor, &c.sla, &c.version); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	var transitions []string
	for _, c := range candidates {
		expected := c.anchor.Add(time.Duration(c.sla) * time.Millisecond)
		_, err = tx.Exec(ctx, `UPDATE cross_chain_messages SET relay_eligible_at=$2,relay_sla_ms=$3,relay_policy_version=$4,expected_by=$5 WHERE id=$1`, c.id, c.anchor, c.sla, c.version, expected)
		if err != nil {
			return 0, err
		}
		change, recomputeErr := s.projection.RecomputeCanonicalMessageTx(ctx, tx, c.id, store.RecomputeCause{Reason: "relay expectation activated"})
		if recomputeErr != nil {
			return 0, recomputeErr
		}
		if change.Changed && change.To == domain.RelayPending {
			transitions = append(transitions, change.Protocol)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	for _, protocol := range transitions {
		s.metrics.ObserveRelayPending(protocol)
	}
	for _, c := range candidates {
		s.logger.Info("relay eligible", "message_pk", c.id, "expected_by", c.anchor.Add(time.Duration(c.sla)*time.Millisecond), "policy_version", c.version)
	}
	return len(candidates), nil
}

func (s *Service) ClassifyDue(ctx context.Context, batch int) (int, error) {
	if err := validBatch(batch); err != nil {
		return 0, err
	}
	ready, err := s.ready(ctx)
	if err != nil || !ready {
		return 0, err
	}
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		count, err := s.classifyOnce(ctx, batch)
		if err == nil {
			return count, nil
		}
		if !retryable(err) {
			return 0, err
		}
		last = err
	}
	return 0, fmt.Errorf("classify due messages: %w", last)
}

func (s *Service) classifyOnce(ctx context.Context, batch int) (int, error) {
	now := s.clock.Now().UTC()
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id,relay_eligible_at,relay_sla_ms,relay_policy_version,source_chain_id::text,destination_chain_id::text FROM cross_chain_messages
		WHERE state='RELAY_PENDING' AND expected_by<=$1 AND relay_eligible_at IS NOT NULL
		AND NOT EXISTS(SELECT 1 FROM message_anomalies a WHERE a.message_pk=cross_chain_messages.id AND a.kind='STUCK' AND a.resolved_at IS NULL)
		ORDER BY expected_by,id LIMIT $2 FOR UPDATE SKIP LOCKED`, now, batch)
	if err != nil {
		return 0, err
	}
	type candidate struct {
		id      int64
		anchor  time.Time
		sla     int64
		version int
		source  string
		dest    string
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err = rows.Scan(&c.id, &c.anchor, &c.sla, &c.version, &c.source, &c.dest); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	opened := 0
	var openedAges []float64
	for _, c := range candidates {
		var healthy bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM source_streams s JOIN chain_checkpoints cp USING(chain_id) JOIN chain_blocks b ON b.chain_id=cp.chain_id AND b.block_hash=cp.block_hash AND b.block_number=cp.block_number WHERE s.chain_id=$1::numeric AND s.status='HEALTHY' AND cp.health='HEALTHY' AND b.canonical)
			AND EXISTS(SELECT 1 FROM destination_streams s JOIN chain_checkpoints cp USING(chain_id) JOIN chain_blocks b ON b.chain_id=cp.chain_id AND b.block_hash=cp.block_hash AND b.block_number=cp.block_number WHERE s.chain_id=$2::numeric AND s.status='HEALTHY' AND cp.health='HEALTHY' AND b.canonical)`, c.source, c.dest).Scan(&healthy)
		if err != nil {
			return 0, err
		}
		if !healthy {
			continue
		}
		if !Due(ClassificationInput{SourceFinal: true, RelayEligible: true, ObservationReady: true, ExpectedBy: c.anchor.Add(time.Duration(c.sla) * time.Millisecond), Now: now}) {
			continue
		}
		var blocked bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM message_anomalies WHERE message_pk=$1 AND kind IN ('CONFLICT','DUPLICATE_EXECUTION') AND resolved_at IS NULL)`, c.id).Scan(&blocked)
		if err != nil {
			return 0, err
		}
		if blocked {
			continue
		}
		var episodeID int64
		err = tx.QueryRow(ctx, `INSERT INTO message_anomalies(message_pk,kind,episode,reason,opened_at,sla_anchor,sla_threshold_ms,relay_policy_version)
			SELECT $1,'STUCK',COALESCE(MAX(episode),0)+1,'destination execution absent after configured relay interval',$2,$3,$4,$5
			FROM message_anomalies WHERE message_pk=$1 AND kind='STUCK'
			ON CONFLICT (message_pk,kind) WHERE resolved_at IS NULL DO NOTHING RETURNING id`, c.id, now, c.anchor, c.sla, c.version).Scan(&episodeID)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return 0, err
		}
		identity := sha256.Sum256([]byte(fmt.Sprintf("STUCK:%d", episodeID)))
		_, err = tx.Exec(ctx, `INSERT INTO message_alerts(anomaly_id,idempotency_key,status,created_at,next_attempt_at) VALUES($1,$2,'PENDING',$3,$3)`, episodeID, identity[:], now)
		if err != nil {
			return 0, err
		}
		opened++
		openedAges = append(openedAges, now.Sub(c.anchor).Seconds())
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	if opened > 0 {
		s.logger.Warn("stuck episodes opened", "count", opened)
		s.logger.Info("alerts queued", "kind", "STUCK", "count", opened)
		for _, age := range openedAges {
			s.metrics.ObserveStuck(age)
		}
	}
	return opened, nil
}

func (s *Service) Run(ctx context.Context, interval time.Duration, batch int) error {
	if interval <= 0 {
		return errors.New("classification interval must be positive")
	}
	if err := validBatch(batch); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if _, err := s.ActivateEligible(ctx, batch); err != nil && ctx.Err() == nil {
			s.logger.Error("relay activation failed", "error", err)
		}
		if _, err := s.ClassifyDue(ctx, batch); err != nil && ctx.Err() == nil {
			s.logger.Error("stuck classification failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
