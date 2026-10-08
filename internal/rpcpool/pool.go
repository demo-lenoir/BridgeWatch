package rpcpool

import (
	"encoding/hex"
	"errors"
	"regexp"
	"sort"
	"sync"
	"time"

	"bridgewatch/internal/clock"
	"bridgewatch/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
)

var endpointID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

type Endpoint[R any] struct {
	ID     string
	Reader R
}

type Status struct {
	ID             string     `json:"id"`
	ChainID        string     `json:"chain_id"`
	ExpectedAnchor string     `json:"expected_anchor"`
	Eligible       bool       `json:"eligible"`
	ObservedHead   uint64     `json:"observed_head"`
	LatencyMS      int64      `json:"last_latency_ms"`
	Health         string     `json:"health"`
	Stale          bool       `json:"stale"`
	CooldownUntil  *time.Time `json:"cooldown_until"`
}

type record[R any] struct {
	endpoint Endpoint[R]
	status   Status
	cooldown time.Time
	hard     bool
}

// Pool is an availability selector. It never votes on canonical ancestry.
type Pool[R any] struct {
	mu       sync.Mutex
	clock    clock.Clock
	cooldown time.Duration
	anchor   domain.Hash
	records  []record[R]
	metrics  *Metrics
}

func New[R any](chain domain.ChainID, anchor domain.Hash, entries []Endpoint[R], businessClock clock.Clock, cooldown time.Duration) (*Pool[R], error) {
	if !chain.Valid() || anchor == (domain.Hash{}) || businessClock == nil || cooldown < time.Millisecond || cooldown > time.Hour || len(entries) < 1 || len(entries) > 8 {
		return nil, errors.New("invalid RPC pool configuration")
	}
	p := &Pool[R]{clock: businessClock, cooldown: cooldown, anchor: anchor}
	seen := map[string]bool{}
	for _, entry := range entries {
		if !endpointID.MatchString(entry.ID) || seen[entry.ID] {
			return nil, errors.New("invalid or duplicate endpoint identity")
		}
		seen[entry.ID] = true
		p.records = append(p.records, record[R]{endpoint: entry, status: Status{ID: entry.ID, ChainID: chain.String(), ExpectedAnchor: "0x" + hex.EncodeToString(anchor[:]), Health: "UNTESTED"}})
	}
	return p, nil
}

func (p *Pool[R]) ExpectedAnchor() domain.Hash { return p.anchor }

func (p *Pool[R]) SetMetrics(metrics *Metrics) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metrics = metrics
	for _, item := range p.records {
		p.metrics.SetEligible(item.status.ID, false)
	}
}

func (p *Pool[R]) Candidates() []Endpoint[R] {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.clock.Now()
	items := make([]record[R], 0, len(p.records))
	for _, item := range p.records {
		if !item.hard && !now.Before(item.cooldown) {
			items = append(items, item)
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].status.Health == "HEALTHY" && items[j].status.Health != "HEALTHY" {
			return true
		}
		if items[j].status.Health == "HEALTHY" && items[i].status.Health != "HEALTHY" {
			return false
		}
		return items[i].status.ObservedHead > items[j].status.ObservedHead
	})
	out := make([]Endpoint[R], len(items))
	for i, item := range items {
		out[i] = item.endpoint
	}
	return out
}

func (p *Pool[R]) Observe(id string, head uint64, duration time.Duration, result string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.records {
		item := &p.records[i]
		if item.endpoint.ID != id {
			continue
		}
		item.status.ObservedHead = head
		item.status.LatencyMS = duration.Milliseconds()
		item.status.Stale = result == "STALE"
		switch result {
		case "HEALTHY":
			item.status.Health = "HEALTHY"
			item.status.Eligible = true
			item.cooldown = time.Time{}
		case "WRONG_CHAIN", "WRONG_ANCHOR":
			item.status.Health = result
			item.status.Eligible = false
			item.hard = true
		default:
			item.status.Health = result
			item.status.Eligible = false
			item.cooldown = p.clock.Now().Add(p.cooldown)
		}
		if item.cooldown.IsZero() {
			item.status.CooldownUntil = nil
		} else {
			value := item.cooldown
			item.status.CooldownUntil = &value
		}
		p.metrics.SetEligible(id, item.status.Eligible)
		return
	}
}

func (p *Pool[R]) Status() []Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Status, len(p.records))
	for i, item := range p.records {
		out[i] = item.status
	}
	return out
}
func (p *Pool[R]) Failover(reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metrics.Failover(reason)
}

type Metrics struct {
	chain     string
	eligible  *prometheus.GaugeVec
	failovers *prometheus.CounterVec
}

func NewMetrics(registry prometheus.Registerer, chain domain.ChainID) (*Metrics, error) {
	if registry == nil || !chain.Valid() {
		return nil, errors.New("invalid RPC metric configuration")
	}
	m := &Metrics{chain: chain.String(), eligible: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "bridgewatch_rpc_eligible", Help: "Validated and currently eligible RPC endpoints."}, []string{"chain", "endpoint"}), failovers: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bridgewatch_rpc_failovers_total", Help: "Bounded provider failovers by chain and reason."}, []string{"chain", "reason"})}
	for _, collector := range []prometheus.Collector{m.eligible, m.failovers} {
		if err := registry.Register(collector); err != nil {
			already, ok := err.(prometheus.AlreadyRegisteredError)
			if !ok {
				return nil, err
			}
			switch collector {
			case m.eligible:
				v, ok := already.ExistingCollector.(*prometheus.GaugeVec)
				if !ok {
					return nil, err
				}
				m.eligible = v
			case m.failovers:
				v, ok := already.ExistingCollector.(*prometheus.CounterVec)
				if !ok {
					return nil, err
				}
				m.failovers = v
			}
		}
	}
	return m, nil
}
func (m *Metrics) SetEligible(id string, eligible bool) {
	if m == nil {
		return
	}
	value := 0.0
	if eligible {
		value = 1
	}
	m.eligible.WithLabelValues(m.chain, id).Set(value)
}
func (m *Metrics) Failover(reason string) {
	if m != nil {
		m.failovers.WithLabelValues(m.chain, reason).Inc()
	}
}
