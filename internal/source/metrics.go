package source

import (
	"context"
	"errors"
	"regexp"
	"time"

	"bridgewatch/internal/domain"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/prometheus/client_golang/prometheus"
)

var metricAlias = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

type Metrics struct {
	chain     string
	endpoint  string
	head      prometheus.Gauge
	lag       prometheus.Gauge
	reorgs    prometheus.Counter
	requests  *prometheus.CounterVec
	latency   *prometheus.HistogramVec
	messages  *prometheus.CounterVec
	delivery  *prometheus.HistogramVec
	unmatched *prometheus.CounterVec
}

// Endpoint is a bounded configured alias, never a URL or request-derived value.
func NewMetrics(registry prometheus.Registerer, chain domain.ChainID, endpoint string) (*Metrics, error) {
	if registry == nil || !chain.Valid() || !metricAlias.MatchString(endpoint) {
		return nil, errors.New("invalid metric configuration")
	}
	chainLabel := chain.String()
	m := &Metrics{chain: chainLabel, endpoint: endpoint,
		head:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "bridgewatch_source_head_block", Help: "Latest validated source head.", ConstLabels: prometheus.Labels{"chain": chainLabel}}),
		lag:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "bridgewatch_lag_blocks", Help: "Validated chain head minus committed checkpoint.", ConstLabels: prometheus.Labels{"chain": chainLabel}}),
		reorgs:    prometheus.NewCounter(prometheus.CounterOpts{Name: "bridgewatch_reorgs_total", Help: "Reconciled chain reorgs.", ConstLabels: prometheus.Labels{"chain": chainLabel}}),
		requests:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bridgewatch_rpc_requests_total", Help: "Chain RPC calls by bounded method and result."}, []string{"chain", "endpoint", "method", "status"}),
		latency:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "bridgewatch_rpc_latency_seconds", Help: "Chain RPC call duration.", Buckets: prometheus.DefBuckets}, []string{"chain", "endpoint", "method"}),
		messages:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bridgewatch_messages_total", Help: "Durable message state transitions."}, []string{"protocol", "state"}),
		delivery:  prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "bridgewatch_delivery_seconds", Help: "Seconds from durable source finality to durable completion.", Buckets: prometheus.DefBuckets}, []string{"protocol", "chain_pair"}),
		unmatched: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bridgewatch_unmatched_destination_total", Help: "Transitions to unmatched destination evidence."}, []string{"protocol"}),
	}
	for _, collector := range []prometheus.Collector{m.head, m.lag, m.reorgs} {
		if err := registry.Register(collector); err != nil {
			return nil, err
		}
	}
	if err := registry.Register(m.requests); err != nil {
		already, ok := err.(prometheus.AlreadyRegisteredError)
		if !ok {
			return nil, err
		}
		existing, ok := already.ExistingCollector.(*prometheus.CounterVec)
		if !ok {
			return nil, err
		}
		m.requests = existing
	}
	if err := registry.Register(m.latency); err != nil {
		already, ok := err.(prometheus.AlreadyRegisteredError)
		if !ok {
			return nil, err
		}
		existing, ok := already.ExistingCollector.(*prometheus.HistogramVec)
		if !ok {
			return nil, err
		}
		m.latency = existing
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
	if err := registry.Register(m.delivery); err != nil {
		already, ok := err.(prometheus.AlreadyRegisteredError)
		if !ok {
			return nil, err
		}
		existing, ok := already.ExistingCollector.(*prometheus.HistogramVec)
		if !ok {
			return nil, err
		}
		m.delivery = existing
	}
	if err := registry.Register(m.unmatched); err != nil {
		already, ok := err.(prometheus.AlreadyRegisteredError)
		if !ok {
			return nil, err
		}
		existing, ok := already.ExistingCollector.(*prometheus.CounterVec)
		if !ok {
			return nil, err
		}
		m.unmatched = existing
	}
	return m, nil
}
func (m *Metrics) ForEndpoint(alias string) (*Metrics, error) {
	if m == nil || !metricAlias.MatchString(alias) {
		return nil, errors.New("invalid endpoint metric alias")
	}
	copy := *m
	copy.endpoint = alias
	return &copy, nil
}
func (m *Metrics) SetHead(number uint64) {
	if m != nil {
		m.head.Set(float64(number))
	}
}
func (m *Metrics) SetLag(number uint64) {
	if m != nil {
		m.lag.Set(float64(number))
	}
}
func (m *Metrics) IncReorg() {
	if m != nil {
		m.reorgs.Inc()
	}
}
func (m *Metrics) ObserveState(protocol string, state domain.State, deliverySeconds *float64, chainPair string) {
	if m == nil {
		return
	}
	m.messages.WithLabelValues(protocol, string(state)).Inc()
	if state == domain.UnmatchedDestination {
		m.unmatched.WithLabelValues(protocol).Inc()
	}
	if deliverySeconds != nil {
		m.delivery.WithLabelValues(protocol, chainPair).Observe(*deliverySeconds)
	}
}
func (m *Metrics) observe(method string, start time.Time, err error) {
	if m == nil {
		return
	}
	status := "ok"
	if err != nil {
		status = "error"
	}
	m.requests.WithLabelValues(m.chain, m.endpoint, method, status).Inc()
	m.latency.WithLabelValues(m.chain, m.endpoint, method).Observe(time.Since(start).Seconds())
}

type observedReader struct {
	Reader
	metrics *Metrics
}

func WithMetrics(reader Reader, metrics *Metrics) Reader {
	return &observedReader{Reader: reader, metrics: metrics}
}
func (r *observedReader) ChainID(ctx context.Context) (id domain.ChainID, err error) {
	start := time.Now()
	defer func() { r.metrics.observe("eth_chainId", start, err) }()
	return r.Reader.ChainID(ctx)
}
func (r *observedReader) BlockNumber(ctx context.Context) (number uint64, err error) {
	start := time.Now()
	defer func() { r.metrics.observe("eth_blockNumber", start, err) }()
	return r.Reader.BlockNumber(ctx)
}
func (r *observedReader) HeaderByNumber(ctx context.Context, n uint64) (h Header, err error) {
	start := time.Now()
	defer func() { r.metrics.observe("eth_getBlockByNumber", start, err) }()
	return r.Reader.HeaderByNumber(ctx, n)
}
func (r *observedReader) HeaderByHash(ctx context.Context, hash domain.Hash) (h Header, err error) {
	start := time.Now()
	defer func() { r.metrics.observe("eth_getBlockByHash", start, err) }()
	return r.Reader.HeaderByHash(ctx, hash)
}
func (r *observedReader) FilterLogs(ctx context.Context, from, to uint64, address common.Address) (logs []types.Log, err error) {
	start := time.Now()
	defer func() { r.metrics.observe("eth_getLogs", start, err) }()
	return r.Reader.FilterLogs(ctx, from, to, address)
}
