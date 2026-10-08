package destination

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"bridgewatch/internal/operations"
)

type controlledClock struct {
	mu  sync.RWMutex
	now time.Time
}

func (c *controlledClock) Now() time.Time      { c.mu.RLock(); defer c.mu.RUnlock(); return c.now }
func (c *controlledClock) Set(value time.Time) { c.mu.Lock(); c.now = value; c.mu.Unlock() }

func operationalRig(t *testing.T) (*crossRig, *controlledClock, *operations.Service) {
	t.Helper()
	r := newCrossRig(t)
	c := &controlledClock{now: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)}
	r.sourceRepo.SetClock(c)
	r.destRepo.SetClock(c)
	r.syncDest(t)
	r.finalSource(t, 1)
	service, err := operations.NewService(r.db, c, r.sourceWatcher, r.destWatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = service.ConfigurePolicy(context.Background(), operations.Policy{Protocol: "mockbridge-v1", SourceChainID: r.sourceID, DestinationChainID: r.destID, Version: 1, SLA: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if count, err := service.ActivateEligible(context.Background(), 10); err != nil || count != 1 {
		t.Fatalf("relay eligibility: %d %v", count, err)
	}
	if state := r.state(t, 1); state != "RELAY_PENDING" {
		t.Fatal(state)
	}
	return r, c, service
}

func TestOperationalSLABoundaryLateDeliveryAndRestart(t *testing.T) {
	r, c, service := operationalRig(t)
	var anchor, expected time.Time
	var sla, version int64
	err := r.db.QueryRow(context.Background(), `SELECT relay_eligible_at,expected_by,relay_sla_ms,relay_policy_version FROM cross_chain_messages`).Scan(&anchor, &expected, &sla, &version)
	if err != nil || !anchor.Equal(c.Now()) || !expected.Equal(anchor.Add(30*time.Second)) || sla != 30000 || version != 1 {
		t.Fatalf("durable relay policy: %v", err)
	}
	c.Set(anchor.Add(30*time.Second - time.Millisecond))
	if n, err := service.ClassifyDue(context.Background(), 10); err != nil || n != 0 {
		t.Fatalf("before SLA: %d %v", n, err)
	}
	// Recreate the service before the deadline; it must use the stored anchor.
	restarted, err := operations.NewService(r.db, c, r.sourceWatcher, r.destWatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Set(expected)
	if n, err := restarted.ClassifyDue(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("at SLA: %d %v", n, err)
	}
	c.Set(expected.Add(time.Millisecond))
	if n, err := restarted.ClassifyDue(context.Background(), 10); err != nil || n != 0 {
		t.Fatalf("repeated classification: %d %v", n, err)
	}
	if r.count(t, "message_alerts") != 1 {
		t.Fatal("duplicate alert outbox row")
	}
	sink := operations.NewMemorySink()
	worker, err := operations.NewAlertWorker(r.db, c, sink, operations.DeliveryConfig{MaxAttempts: 3, Lease: time.Minute, BaseBackoff: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := worker.DeliverBatch(context.Background(), 10); err != nil || n != 1 || sink.Count() != 1 {
		t.Fatalf("initial alert: %d %d %v", n, sink.Count(), err)
	}
	a := extend(r.destChain.branch, 30, 2)
	r.addDestination(t, a[2], 1, 41, 7, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	if state := r.state(t, 1); state != "DEST_SEEN" {
		t.Fatal(state)
	}
	var resolved bool
	if err := r.db.QueryRow(context.Background(), `SELECT resolved_at IS NOT NULL FROM message_anomalies WHERE kind='STUCK'`).Scan(&resolved); err != nil || !resolved {
		t.Fatal("stuck episode not resolved", err)
	}
	a = extend(a, 30, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	if state := r.state(t, 1); state != "COMPLETED" {
		t.Fatal(state)
	}
	if r.count(t, "message_anomalies") < 1 || r.count(t, "message_alerts") != 1 {
		t.Fatal("historical episode lost")
	}
	if n, err := worker.DeliverBatch(context.Background(), 10); err != nil || n != 0 {
		t.Fatal("delivered alert sent again", n, err)
	}
}

func TestDestinationOutageSuppressesNewStuck(t *testing.T) {
	r, c, service := operationalRig(t)
	r.destChain.failedLogs = true
	if err := r.destWatcher.SyncOnce(context.Background()); err == nil {
		t.Fatal("destination outage not detected")
	}
	c.Set(c.Now().Add(31 * time.Second))
	if n, err := service.ClassifyDue(context.Background(), 10); err != nil || n != 0 {
		t.Fatalf("outage opened stuck: %d %v", n, err)
	}
	if r.count(t, "message_alerts") != 0 {
		t.Fatal("outage queued alert")
	}
	r.destChain.failedLogs = false
	r.syncDest(t)
	if n, err := service.ClassifyDue(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("recovery failed to classify: %d %v", n, err)
	}
}

func TestStuckSourceReorgAndDestinationReorgNewEpisode(t *testing.T) {
	r, c, service := operationalRig(t)
	c.Set(c.Now().Add(31 * time.Second))
	if n, err := service.ClassifyDue(context.Background(), 10); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	a := extend(r.destChain.branch, 30, 4)
	r.addDestination(t, a[2], 1, 41, 7, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	if r.state(t, 1) != "COMPLETED" {
		t.Fatal("late execution did not complete")
	}
	b := extend(a[:2], 40, 4)
	r.destChain.setBranch(b)
	r.syncDest(t)
	if state := r.state(t, 1); state != "RELAY_PENDING" {
		t.Fatalf("destination reorg: %s", state)
	}
	if n, err := service.ClassifyDue(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("second episode: %d %v", n, err)
	}
	var firstResolved, secondOpen bool
	if err := r.db.QueryRow(context.Background(), `SELECT resolved_at IS NOT NULL FROM message_anomalies WHERE kind='STUCK' AND episode=1`).Scan(&firstResolved); err != nil {
		t.Fatal(err)
	}
	if err := r.db.QueryRow(context.Background(), `SELECT resolved_at IS NULL FROM message_anomalies WHERE kind='STUCK' AND episode=2`).Scan(&secondOpen); err != nil {
		t.Fatal(err)
	}
	if !firstResolved || !secondOpen || r.count(t, "message_alerts") != 2 {
		t.Fatal("episode history did not advance")
	}
	r.sourceChain.setBranch(extend(r.sourceChain.branch[:2], 50, 4))
	r.syncSource(t)
	if state := r.state(t, 1); state != "NO_CANONICAL_EVIDENCE" {
		t.Fatalf("source reorg: %s", state)
	}
	var open int
	if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM message_anomalies WHERE kind='STUCK' AND resolved_at IS NULL`).Scan(&open); err != nil || open != 0 {
		t.Fatal("orphaned source left stuck open", err)
	}
	var expectation any
	if err := r.db.QueryRow(context.Background(), `SELECT relay_eligible_at FROM cross_chain_messages`).Scan(&expectation); err != nil || expectation != nil {
		t.Fatal("orphaned source kept SLA anchor", err)
	}
	var pending int
	if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM message_alerts WHERE status='PENDING'`).Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("invalidated STUCK left pending alerts: %d %v", pending, err)
	}
	sink := operations.NewMemorySink()
	worker, err := operations.NewAlertWorker(r.db, c, sink, operations.DeliveryConfig{MaxAttempts: 3, Lease: time.Minute, BaseBackoff: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := worker.DeliverBatch(context.Background(), 10); err != nil || count != 0 || sink.Count() != 0 {
		t.Fatalf("orphaned source delivered alert: %d %v", count, err)
	}
}

func TestStuckSourceReincludedOnNewCanonicalBranchResetsAnchor(t *testing.T) {
	r, c, service := operationalRig(t)
	c.Set(c.Now().Add(31 * time.Second))
	if n, err := service.ClassifyDue(context.Background(), 10); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	newBranch := extend(r.sourceChain.branch[:2], 70, 4)
	r.addSource(t, newBranch[2], 1, 99)
	r.sourceChain.setBranch(newBranch)
	r.syncSource(t)
	if state := r.state(t, 1); state != "SOURCE_FINAL" {
		t.Fatalf("replaced source evidence: %s", state)
	}
	var relay any
	var finalAt time.Time
	if err := r.db.QueryRow(context.Background(), `SELECT relay_eligible_at,source_final_at FROM cross_chain_messages`).Scan(&relay, &finalAt); err != nil || relay != nil || !finalAt.Equal(c.Now()) {
		t.Fatalf("replaced source anchor: %v %v %v", relay, finalAt, err)
	}
	var open int
	if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM message_anomalies WHERE kind='STUCK' AND resolved_at IS NULL`).Scan(&open); err != nil || open != 0 {
		t.Fatalf("old episode still open: %d %v", open, err)
	}
	if n, err := service.ActivateEligible(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("new expectation: %d %v", n, err)
	}
	if n, err := service.ClassifyDue(context.Background(), 10); err != nil || n != 0 {
		t.Fatalf("old deadline reused: %d %v", n, err)
	}
	c.Set(c.Now().Add(30 * time.Second))
	if n, err := service.ClassifyDue(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("new deadline: %d %v", n, err)
	}
}

func TestTwoStuckWorkersAndCompletionRace(t *testing.T) {
	r, c, service := operationalRig(t)
	c.Set(c.Now().Add(30 * time.Second))
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { <-start; _, err := service.ClassifyDue(context.Background(), 10); results <- err }()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if r.count(t, "message_alerts") != 1 {
		t.Fatal("concurrent scheduler duplicated alert")
	}
	a := extend(r.destChain.branch, 30, 4)
	r.addDestination(t, a[2], 1, 41, 7, 4)
	r.destChain.setBranch(a)
	start = make(chan struct{})
	results = make(chan error, 2)
	go func() { <-start; _, err := service.ClassifyDue(context.Background(), 10); results <- err }()
	go func() { <-start; results <- r.destWatcher.SyncOnce(context.Background()) }()
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if state := r.state(t, 1); state != "COMPLETED" {
		t.Fatalf("completion race: %s", state)
	}
	var open int
	if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM message_anomalies WHERE kind='STUCK' AND resolved_at IS NULL`).Scan(&open); err != nil || open != 0 {
		t.Fatal("completed message remains stuck", err)
	}
}

func TestCompletionBeforeSLANoEpisode(t *testing.T) {
	r, c, service := operationalRig(t)
	a := extend(r.destChain.branch, 30, 4)
	r.addDestination(t, a[2], 1, 41, 7, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	if state := r.state(t, 1); state != "COMPLETED" {
		t.Fatal(state)
	}
	c.Set(c.Now().Add(30 * time.Second))
	if n, err := service.ClassifyDue(context.Background(), 10); err != nil || n != 0 {
		t.Fatalf("completed message classified late: %d %v", n, err)
	}
	if r.count(t, "message_alerts") != 0 {
		t.Fatal("false alert")
	}
}

type responseLostSink struct {
	mu    sync.Mutex
	keys  []string
	first bool
}

func (s *responseLostSink) Deliver(_ context.Context, a operations.Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = append(s.keys, a.IdempotencyKey)
	if !s.first {
		s.first = true
		return errors.New("response lost")
	}
	return nil
}
func TestAlertResponseLostRetriesSameIdentity(t *testing.T) {
	r, c, service := operationalRig(t)
	c.Set(c.Now().Add(30 * time.Second))
	if _, err := service.ClassifyDue(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	sink := &responseLostSink{}
	worker, err := operations.NewAlertWorker(r.db, c, sink, operations.DeliveryConfig{MaxAttempts: 3, Lease: time.Minute, BaseBackoff: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := worker.DeliverBatch(context.Background(), 10); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	c.Set(c.Now().Add(time.Second))
	restarted, err := operations.NewAlertWorker(r.db, c, sink, operations.DeliveryConfig{MaxAttempts: 3, Lease: time.Minute, BaseBackoff: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := restarted.DeliverBatch(context.Background(), 10); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	sink.mu.Lock()
	same := len(sink.keys) == 2 && sink.keys[0] == sink.keys[1]
	sink.mu.Unlock()
	if !same || r.count(t, "message_alerts") != 1 {
		t.Fatal("response loss changed logical alert identity")
	}
	var status string
	var attempts int
	if err := r.db.QueryRow(context.Background(), `SELECT status,attempt_count FROM message_alerts`).Scan(&status, &attempts); err != nil || status != "DELIVERED" || attempts != 2 {
		t.Fatalf("delivery acknowledgement: %s %d %v", status, attempts, err)
	}
}

func TestManualIncidentAlertIsChainScoped(t *testing.T) {
	r := newCrossRig(t)
	r.finalSource(t, 1)
	a := extend(r.destChain.branch, 30, 10)
	r.destChain.setBranch(a)
	r.syncDest(t)
	b := extend(a[:2], 40, 10)
	r.destChain.setBranch(b)
	if err := r.destWatcher.SyncOnce(context.Background()); !errors.Is(err, ErrDeepReorg) {
		t.Fatal(err)
	}
	var role string
	var count int
	if err := r.db.QueryRow(context.Background(), `SELECT role FROM chain_incidents`).Scan(&role); err != nil || role != "DESTINATION" {
		t.Fatalf("incident: %s %v", role, err)
	}
	if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM message_alerts WHERE chain_incident_id IS NOT NULL`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("incident alert: %d %v", count, err)
	}
	if err := r.destWatcher.SyncOnce(context.Background()); !errors.Is(err, ErrDeepReorg) {
		t.Fatal(err)
	}
	if r.count(t, "chain_incidents") != 1 || r.count(t, "message_alerts") != 1 {
		t.Fatal("manual intervention duplicated")
	}
}

type scriptedSink struct {
	mu      sync.Mutex
	results []error
	keys    []string
}

func (s *scriptedSink) Deliver(_ context.Context, alert operations.Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = append(s.keys, alert.IdempotencyKey)
	if len(s.results) == 0 {
		return nil
	}
	err := s.results[0]
	s.results = s.results[1:]
	return err
}
func (s *scriptedSink) count() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.keys) }

func TestAlertDeliveryOutcomesAndRestart(t *testing.T) {
	for _, tc := range []struct {
		name     string
		results  []error
		status   string
		failure  string
		attempts int
	}{
		{name: "transient then success", results: []error{errors.New("temporary"), nil}, status: "DELIVERED", attempts: 2},
		{name: "timeout then success", results: []error{context.DeadlineExceeded, nil}, status: "DELIVERED", attempts: 2},
		{name: "permanent", results: []error{operations.PermanentError{Err: errors.New("bad configuration")}}, status: "CANCELLED", failure: "PERMANENT", attempts: 1},
		{name: "exhausted", results: []error{errors.New("down"), errors.New("still down"), errors.New("again")}, status: "CANCELLED", failure: "EXHAUSTED", attempts: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, c, service := operationalRig(t)
			c.Set(c.Now().Add(30 * time.Second))
			if _, err := service.ClassifyDue(context.Background(), 10); err != nil {
				t.Fatal(err)
			}
			sink := &scriptedSink{results: tc.results}
			for i := 0; i < tc.attempts; i++ {
				worker, err := operations.NewAlertWorker(r.db, c, sink, operations.DeliveryConfig{MaxAttempts: 3, Lease: time.Minute, BaseBackoff: time.Second}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if n, err := worker.DeliverBatch(context.Background(), 10); err != nil || n != 1 {
					t.Fatalf("attempt %d: %d %v", i, n, err)
				}
				c.Set(c.Now().Add(time.Duration(1<<i) * time.Second))
			}
			var status string
			var attempts int
			var failure *string
			if err := r.db.QueryRow(context.Background(), `SELECT status,attempt_count,failure_kind FROM message_alerts`).Scan(&status, &attempts, &failure); err != nil {
				t.Fatal(err)
			}
			if status != tc.status || attempts != tc.attempts || (tc.failure != "" && (failure == nil || *failure != tc.failure)) {
				t.Fatalf("outcome: %s %d %v", status, attempts, failure)
			}
			worker, err := operations.NewAlertWorker(r.db, c, sink, operations.DeliveryConfig{MaxAttempts: 3, Lease: time.Minute, BaseBackoff: time.Second}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := worker.DeliverBatch(context.Background(), 10); err != nil || n != 0 || sink.count() != tc.attempts {
				t.Fatalf("restart redelivered: %d %d %v", n, sink.count(), err)
			}
			if r.count(t, "message_alerts") != 1 {
				t.Fatal("duplicate outbox item")
			}
		})
	}
}

func TestConcurrentAlertWorkersClaimOnce(t *testing.T) {
	r, c, service := operationalRig(t)
	c.Set(c.Now().Add(30 * time.Second))
	if _, err := service.ClassifyDue(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	sink := &scriptedSink{}
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			worker, err := operations.NewAlertWorker(r.db, c, sink, operations.DeliveryConfig{MaxAttempts: 3, Lease: time.Minute, BaseBackoff: time.Second}, nil)
			if err != nil {
				results <- err
				return
			}
			<-start
			_, err = worker.DeliverBatch(context.Background(), 10)
			results <- err
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if sink.count() != 1 {
		t.Fatalf("concurrent duplicate delivery: %d", sink.count())
	}
}

func TestAlertLeaseExpiryAfterWorkerCrash(t *testing.T) {
	r, c, service := operationalRig(t)
	c.Set(c.Now().Add(30 * time.Second))
	if _, err := service.ClassifyDue(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	var identity []byte
	if err := r.db.QueryRow(context.Background(), `SELECT idempotency_key FROM message_alerts`).Scan(&identity); err != nil {
		t.Fatal(err)
	}
	_, err := r.db.Exec(context.Background(), `UPDATE message_alerts SET lease_token=$1,lease_until=$2`, make([]byte, 16), c.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	sink := operations.NewMemorySink()
	worker, err := operations.NewAlertWorker(r.db, c, sink, operations.DeliveryConfig{MaxAttempts: 3, Lease: time.Minute, BaseBackoff: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := worker.DeliverBatch(context.Background(), 10); err != nil || n != 0 {
		t.Fatalf("live lease stolen: %d %v", n, err)
	}
	c.Set(c.Now().Add(time.Minute))
	if n, err := worker.DeliverBatch(context.Background(), 10); err != nil || n != 1 || sink.Count() != 1 {
		t.Fatalf("expired lease not resumed: %d %d %v", n, sink.Count(), err)
	}
	var after []byte
	var attempts int
	if err := r.db.QueryRow(context.Background(), `SELECT idempotency_key,attempt_count FROM message_alerts`).Scan(&after, &attempts); err != nil || !bytes.Equal(identity, after) || attempts != 1 {
		t.Fatalf("alert identity changed: %d %v", attempts, err)
	}
}
