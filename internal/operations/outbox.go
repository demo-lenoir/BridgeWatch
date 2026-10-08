package operations

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"bridgewatch/internal/clock"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Alert struct {
	ID                 int64  `json:"id"`
	Kind               string `json:"kind"`
	IdempotencyKey     string `json:"idempotency_key"`
	Protocol           string `json:"protocol,omitempty"`
	SourceChainID      string `json:"source_chain_id,omitempty"`
	DestinationChainID string `json:"destination_chain_id,omitempty"`
	MessageID          string `json:"message_id,omitempty"`
	IncidentChainID    string `json:"incident_chain_id,omitempty"`
	IncidentRole       string `json:"incident_role,omitempty"`
}

type AlertSink interface {
	Deliver(context.Context, Alert) error
}

type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

type DeliveryConfig struct {
	MaxAttempts int
	Lease       time.Duration
	BaseBackoff time.Duration
}

func (c DeliveryConfig) Validate() error {
	if c.MaxAttempts < 1 || c.MaxAttempts > 10 || c.Lease < time.Second || c.Lease > 10*time.Minute || c.BaseBackoff < time.Millisecond || c.BaseBackoff > time.Hour {
		return errors.New("invalid alert delivery configuration")
	}
	return nil
}

type AlertWorker struct {
	db      *pgxpool.Pool
	clock   clock.Clock
	sink    AlertSink
	config  DeliveryConfig
	logger  *slog.Logger
	metrics *Metrics
}

func NewAlertWorker(db *pgxpool.Pool, businessClock clock.Clock, sink AlertSink, config DeliveryConfig, logger *slog.Logger) (*AlertWorker, error) {
	if db == nil || businessClock == nil || sink == nil {
		return nil, errors.New("alert worker dependencies required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &AlertWorker{db: db, clock: businessClock, sink: sink, config: config, logger: logger}, nil
}
func (w *AlertWorker) SetMetrics(metrics *Metrics) { w.metrics = metrics }

type claimedAlert struct {
	Alert
	token    []byte
	attempts int
}

func (w *AlertWorker) claim(ctx context.Context, batch int) ([]claimedAlert, error) {
	now := w.clock.Now().UTC()
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT a.id,a.idempotency_key,a.attempt_count,COALESCE(ma.kind,ci.kind),COALESCE(m.protocol,''),COALESCE(m.source_chain_id::text,''),COALESCE(m.destination_chain_id::text,''),m.message_id,COALESCE(ci.chain_id::text,''),COALESCE(ci.role,'')
		FROM message_alerts a LEFT JOIN message_anomalies ma ON ma.id=a.anomaly_id LEFT JOIN cross_chain_messages m ON m.id=ma.message_pk
		LEFT JOIN chain_incidents ci ON ci.id=a.chain_incident_id
		WHERE a.status='PENDING' AND a.next_attempt_at<=$1 AND (a.lease_until IS NULL OR a.lease_until<=$1)
		ORDER BY a.next_attempt_at,a.id LIMIT $2 FOR UPDATE OF a SKIP LOCKED`, now, batch)
	if err != nil {
		return nil, err
	}
	var claimed []claimedAlert
	for rows.Next() {
		var item claimedAlert
		var key, message []byte
		if err = rows.Scan(&item.ID, &key, &item.attempts, &item.Kind, &item.Protocol, &item.SourceChainID, &item.DestinationChainID, &message, &item.IncidentChainID, &item.IncidentRole); err != nil {
			rows.Close()
			return nil, err
		}
		item.IdempotencyKey = hex.EncodeToString(key)
		if len(message) > 0 {
			item.MessageID = "0x" + hex.EncodeToString(message)
		}
		item.token = make([]byte, 16)
		if _, err = rand.Read(item.token); err != nil {
			rows.Close()
			return nil, err
		}
		claimed = append(claimed, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for _, item := range claimed {
		_, err = tx.Exec(ctx, `UPDATE message_alerts SET lease_token=$2,lease_until=$3 WHERE id=$1`, item.ID, item.token, now.Add(w.config.Lease))
		if err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (w *AlertWorker) DeliverBatch(ctx context.Context, batch int) (int, error) {
	if err := validBatch(batch); err != nil {
		return 0, err
	}
	items, err := w.claim(ctx, batch)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		deliveryErr := w.sink.Deliver(ctx, item.Alert)
		if ackErr := w.ack(ctx, item, deliveryErr); ackErr != nil {
			return processed, ackErr
		}
		processed++
	}
	return processed, nil
}

func (w *AlertWorker) ack(ctx context.Context, item claimedAlert, deliveryErr error) error {
	now := w.clock.Now().UTC()
	result := "DELIVERED"
	status := "DELIVERED"
	var failureKind any
	var next time.Time
	var lastError any
	if deliveryErr != nil {
		var permanent PermanentError
		if errors.As(deliveryErr, &permanent) {
			status = "CANCELLED"
			result = "PERMANENT"
			failureKind = "PERMANENT"
		} else if item.attempts+1 >= w.config.MaxAttempts {
			status = "CANCELLED"
			result = "EXHAUSTED"
			failureKind = "EXHAUSTED"
		} else {
			status = "PENDING"
			result = "RETRY"
			failureKind = "TRANSIENT"
			delay := w.config.BaseBackoff * time.Duration(1<<min(item.attempts, 9))
			if delay > time.Hour {
				delay = time.Hour
			}
			next = now.Add(delay)
		}
		lastError = deliveryErr.Error()
		if len(lastError.(string)) > 512 {
			lastError = lastError.(string)[:512]
		}
	}
	if status != "PENDING" {
		next = now
	}
	command, err := w.db.Exec(ctx, `UPDATE message_alerts SET status=$3,attempt_count=attempt_count+1,next_attempt_at=$4,lease_token=NULL,lease_until=NULL,last_error=$5,failure_kind=$6,delivered_at=CASE WHEN $3='DELIVERED' THEN $7 ELSE delivered_at END
		WHERE id=$1 AND lease_token=$2 AND status='PENDING'`, item.ID, item.token, status, next, lastError, failureKind, now)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return errors.New("alert lease was lost before acknowledgement")
	}
	w.metrics.ObserveAlert(item.Kind, result)
	if deliveryErr != nil {
		w.logger.Warn("alert delivery failed", "alert_id", item.ID, "kind", item.Kind, "result", result, "error", deliveryErr)
	}
	return nil
}

func (w *AlertWorker) Run(ctx context.Context, interval time.Duration, batch int) error {
	if interval <= 0 {
		return errors.New("delivery interval must be positive")
	}
	if err := validBatch(batch); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if _, err := w.DeliverBatch(ctx, batch); err != nil && ctx.Err() == nil {
			w.logger.Error("alert batch failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// MemorySink is a deterministic local sink. Repeated identities are retained once.
type MemorySink struct {
	mu     sync.Mutex
	alerts map[string]Alert
}

func NewMemorySink() *MemorySink { return &MemorySink{alerts: map[string]Alert{}} }
func (s *MemorySink) Deliver(ctx context.Context, a Alert) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.IdempotencyKey == "" {
		return fmt.Errorf("missing alert identity")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alerts[a.IdempotencyKey] = a
	return nil
}
func (s *MemorySink) Count() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.alerts) }
