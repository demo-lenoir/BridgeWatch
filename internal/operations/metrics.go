package operations

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	stuck     *prometheus.GaugeVec
	messages  *prometheus.CounterVec
	age       prometheus.Histogram
	conflicts *prometheus.GaugeVec
	alerts    *prometheus.CounterVec
	pending   prometheus.Gauge
}

func NewMetrics(registry prometheus.Registerer) (*Metrics, error) {
	if registry == nil {
		return nil, errors.New("metric registry required")
	}
	m := &Metrics{
		stuck:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "bridgewatch_stuck_messages", Help: "Open delayed-relay episodes by configured protocol and chain pair."}, []string{"protocol", "chain_pair"}),
		messages:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bridgewatch_messages_total", Help: "Durable message state transitions."}, []string{"protocol", "state"}),
		age:       prometheus.NewHistogram(prometheus.HistogramOpts{Name: "bridgewatch_stuck_age_seconds", Help: "Age of relay expectation when a delayed-relay episode opens.", Buckets: prometheus.DefBuckets}),
		conflicts: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "bridgewatch_conflicting_duplicates", Help: "Open conflict and duplicate-execution episodes by protocol."}, []string{"protocol"}),
		alerts:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bridgewatch_alerts_total", Help: "Alert delivery attempts by bounded kind and result."}, []string{"kind", "result"}),
		pending:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "bridgewatch_alert_outbox_pending", Help: "Pending durable alert outbox rows."}),
	}
	for _, collector := range []prometheus.Collector{m.stuck, m.age, m.conflicts, m.alerts, m.pending} {
		if err := registry.Register(collector); err != nil {
			return nil, err
		}
	}
	if err := registry.Register(m.messages); err != nil {
		already, ok := err.(prometheus.AlreadyRegisteredError)
		if !ok {
			return nil, err
		}
		existing, ok := already.ExistingCollector.(*prometheus.CounterVec)
		if !ok {
			return nil, err
		}
		m.messages = existing
	}
	return m, nil
}
func (m *Metrics) TrackPair(protocol, pair string) {
	if m != nil {
		m.stuck.WithLabelValues(protocol, pair).Set(0)
	}
}
func (m *Metrics) ObserveRelayPending(protocol string) {
	if m != nil {
		m.messages.WithLabelValues(protocol, "RELAY_PENDING").Inc()
	}
}
func (m *Metrics) ObserveAlert(kind, result string) {
	if m != nil {
		m.alerts.WithLabelValues(kind, result).Inc()
	}
}
func (m *Metrics) ObserveStuck(seconds float64) {
	if m != nil {
		m.age.Observe(seconds)
	}
}

// Refresh reads current durable counts. Scrape loss cannot affect classification.
func (m *Metrics) Refresh(ctx context.Context, db *pgxpool.Pool) error {
	if m == nil {
		return nil
	}
	rows, err := db.Query(ctx, `SELECT protocol,source_chain_id::text || '_' || destination_chain_id::text FROM relay_policies`)
	if err != nil {
		return err
	}
	stuck := map[[2]string]float64{}
	for rows.Next() {
		var protocol, pair string
		if err = rows.Scan(&protocol, &pair); err != nil {
			rows.Close()
			return err
		}
		stuck[[2]string{protocol, pair}] = 0
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = db.Query(ctx, `SELECT msg.protocol,msg.source_chain_id::text || '_' || msg.destination_chain_id::text,count(*) FROM message_anomalies a JOIN cross_chain_messages msg ON msg.id=a.message_pk WHERE a.kind='STUCK' AND a.resolved_at IS NULL GROUP BY msg.protocol,msg.source_chain_id,msg.destination_chain_id`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var protocol, pair string
		var count int64
		if err = rows.Scan(&protocol, &pair, &count); err != nil {
			rows.Close()
			return err
		}
		stuck[[2]string{protocol, pair}] = float64(count)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = db.Query(ctx, `SELECT msg.protocol,count(*) FROM message_anomalies a JOIN cross_chain_messages msg ON msg.id=a.message_pk WHERE a.kind IN ('CONFLICT','DUPLICATE_EXECUTION') AND a.resolved_at IS NULL GROUP BY msg.protocol`)
	if err != nil {
		return err
	}
	conflicts := map[string]float64{}
	for rows.Next() {
		var protocol string
		var count int64
		if err = rows.Scan(&protocol, &count); err != nil {
			rows.Close()
			return err
		}
		conflicts[protocol] = float64(count)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var pending int64
	if err = db.QueryRow(ctx, `SELECT count(*) FROM message_alerts WHERE status='PENDING'`).Scan(&pending); err != nil {
		return err
	}
	m.stuck.Reset()
	for labels, count := range stuck {
		m.stuck.WithLabelValues(labels[0], labels[1]).Set(count)
	}
	m.conflicts.Reset()
	for protocol, count := range conflicts {
		m.conflicts.WithLabelValues(protocol).Set(count)
	}
	m.pending.Set(float64(pending))
	return nil
}
