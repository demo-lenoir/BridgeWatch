package store_test

import (
	"context"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"bridgewatch/internal/domain"
	"bridgewatch/internal/protocol"
	"bridgewatch/internal/reconcile"
	"bridgewatch/internal/store"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgxpool"
)

type testRig struct {
	db               *pgxpool.Pool
	repo             *store.Postgres
	service          *reconcile.Service
	adapter          *protocol.MockBridgeAdapter
	sourceID, destID domain.ChainID
}

func newRig(t *testing.T) *testRig {
	t.Helper()
	url := os.Getenv("BRIDGEWATCH_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("PostgreSQL integration URL not configured")
	}
	ctx := context.Background()
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 40
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, "TRUNCATE message_alerts,message_transitions,message_anomalies,observation_conflicts,message_claims,message_observations,cross_chain_messages,chain_checkpoints,chain_blocks RESTART IDENTITY CASCADE")
	if err != nil {
		t.Fatal(err)
	}
	sourceID, _ := domain.NewChainID(big.NewInt(31337))
	destID, _ := domain.NewChainID(big.NewInt(31338))
	a, err := protocol.NewMockBridgeAdapter(protocol.MockConfig{SourceChainID: sourceID, DestinationChainID: destID, SourceContract: common.Address{1}, DestinationContract: common.Address{2}, Version: 1, RelaySLA: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	repo := store.NewPostgres(pool)
	return &testRig{db: pool, repo: repo, service: reconcile.New(a, repo), adapter: a, sourceID: sourceID, destID: destID}
}

func (r *testRig) source(t *testing.T, id, payload, tx byte) protocol.EventLog {
	t.Helper()
	data, err := r.adapter.SourceEvent().Inputs.Pack([32]byte{id}, big.NewInt(31338), common.Address{3}, common.Address{4}, [32]byte{payload}, common.Address{}, big.NewInt(0))
	if err != nil {
		t.Fatal(err)
	}
	return protocol.EventLog{ChainID: r.sourceID, ParentHash: common.Hash{10}, Log: types.Log{Address: common.Address{1}, Topics: []common.Hash{r.adapter.SourceEvent().ID}, Data: data, BlockNumber: 10, BlockHash: common.Hash{11}, TxHash: common.Hash{tx}, Index: 0}}
}
func (r *testRig) destination(t *testing.T, id, payload, tx byte) protocol.EventLog {
	t.Helper()
	data, err := r.adapter.DestinationEvent().Inputs.Pack([32]byte{id}, big.NewInt(31337), common.Address{4}, [32]byte{payload}, true)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.EventLog{ChainID: r.destID, ParentHash: common.Hash{20}, Log: types.Log{Address: common.Address{2}, Topics: []common.Hash{r.adapter.DestinationEvent().ID}, Data: data, BlockNumber: 20, BlockHash: common.Hash{21}, TxHash: common.Hash{tx}, Index: 0}}
}
func (r *testRig) key(id byte) domain.MessageKey {
	return domain.MessageKey{Protocol: "mockbridge-v1", SourceChainID: r.sourceID, DestinationChainID: r.destID, MessageID: domain.Hash{id}}
}
func (r *testRig) count(t *testing.T, table string) int {
	t.Helper()
	var n int
	err := r.db.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPostgresIdenticalDuplicateAndTransition(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	entry := r.source(t, 1, 2, 12)
	first, err := r.service.IngestSource(ctx, entry)
	if err != nil || first.Result != store.Inserted {
		t.Fatalf("first: %+v %v", first, err)
	}
	before, err := r.repo.LoadMessage(ctx, r.key(1))
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.service.IngestSource(ctx, entry)
	if err != nil || second.Result != store.IdenticalDuplicate || second.State != domain.SourceSeen {
		t.Fatalf("duplicate: %+v %v", second, err)
	}
	after, err := r.repo.LoadMessage(ctx, r.key(1))
	if err != nil {
		t.Fatal(err)
	}
	if before != after || after.Observations != 1 || after.Transitions != 1 || after.State != domain.SourceSeen || r.count(t, "message_observations") != 1 {
		t.Fatalf("duplicate mutated state: %+v %+v", before, after)
	}
}

func TestPostgresDestinationBeforeSource(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	_, err := r.service.IngestDestination(ctx, r.destination(t, 1, 2, 22))
	if err != nil {
		t.Fatal(err)
	}
	before, err := r.repo.LoadMessage(ctx, r.key(1))
	if err != nil || before.State != domain.UnmatchedDestination || before.DestinationClaims != 1 {
		t.Fatalf("unmatched: %+v %v", before, err)
	}
	queue, err := r.repo.LoadUnmatchedDestinations(ctx, 10)
	if err != nil || len(queue) != 1 || queue[0] != r.key(1) {
		t.Fatalf("queue: %+v %v", queue, err)
	}
	_, err = r.service.IngestSource(ctx, r.source(t, 1, 2, 12))
	if err != nil {
		t.Fatal(err)
	}
	after, err := r.repo.LoadMessage(ctx, r.key(1))
	if err != nil {
		t.Fatal(err)
	}
	if after.State != domain.SourceSeen || !after.Projection.Correlated || after.Observations != 2 || after.Transitions != 2 {
		t.Fatalf("did not reconcile: %+v", after)
	}
}

func TestPostgresRestartWithUnmatchedDestination(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	if _, err := r.service.IngestDestination(ctx, r.destination(t, 1, 2, 22)); err != nil {
		t.Fatal(err)
	}
	// A new repository/service uses only committed database evidence.
	restarted := store.NewPostgres(r.db)
	queue, err := restarted.LoadUnmatchedDestinations(ctx, 10)
	if err != nil || len(queue) != 1 {
		t.Fatalf("restart queue: %+v %v", queue, err)
	}
	service := reconcile.New(r.adapter, restarted)
	if _, err := service.IngestSource(ctx, r.source(t, 1, 2, 12)); err != nil {
		t.Fatal(err)
	}
	snap, err := restarted.LoadMessage(ctx, r.key(1))
	if err != nil || !snap.Projection.Correlated || snap.Observations != 2 {
		t.Fatalf("restart convergence: %+v %v", snap, err)
	}
}

func TestPostgresSameObservationKeyDifferentBytes(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	first := r.source(t, 1, 2, 12)
	second := r.source(t, 1, 9, 12)
	if _, err := r.service.IngestSource(ctx, first); err != nil {
		t.Fatal(err)
	}
	result, err := r.service.IngestSource(ctx, second)
	if err != nil || result.Result != store.ConflictingObservation {
		t.Fatalf("conflict: %+v %v", result, err)
	}
	if r.count(t, "message_observations") != 1 || r.count(t, "observation_conflicts") != 1 || r.count(t, "message_anomalies") != 1 || r.count(t, "message_transitions") != 1 {
		t.Fatal("conflicting bytes overwritten or duplicated")
	}
}

func TestPostgresCompetingBlocksAtSameHeight(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	first := r.source(t, 1, 2, 12)
	second := r.source(t, 2, 3, 13)
	second.Log.BlockHash = common.Hash{99}
	if _, err := r.service.IngestSource(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := r.service.IngestSource(ctx, second); err != nil {
		t.Fatal(err)
	}
	if r.count(t, "chain_blocks") != 2 || r.count(t, "message_observations") != 2 {
		t.Fatal("competing block history collided")
	}
}

func TestPostgresSameIdentityDifferentPayload(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	if _, err := r.service.IngestSource(ctx, r.source(t, 1, 2, 12)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.service.IngestSource(ctx, r.source(t, 1, 9, 13)); err != nil {
		t.Fatal(err)
	}
	snap, err := r.repo.LoadMessage(ctx, r.key(1))
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Projection.Conflict || snap.Conflicts != 1 || snap.SourceClaims != 2 || snap.Transitions != 1 {
		t.Fatalf("payload conflict lost: %+v", snap)
	}
	var persisted []byte
	id := domain.Hash{1}
	if err := r.db.QueryRow(ctx, "SELECT payload_hash FROM cross_chain_messages WHERE message_id=$1", id[:]).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted[0] != 2 {
		t.Fatal("conflicting payload overwrote first projection claim")
	}
}

func TestPostgresDistinctDestinationExecution(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	if _, err := r.service.IngestSource(ctx, r.source(t, 1, 2, 12)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.service.IngestDestination(ctx, r.destination(t, 1, 2, 22)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.service.IngestDestination(ctx, r.destination(t, 1, 2, 23)); err != nil {
		t.Fatal(err)
	}
	snap, err := r.repo.LoadMessage(ctx, r.key(1))
	if err != nil || !snap.Projection.DuplicateExecution || snap.DuplicateEpisodes != 1 || snap.DestinationClaims != 2 {
		t.Fatalf("execution duplicate: %+v %v", snap, err)
	}
}

func TestPostgresConcurrentIdentical100(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	entry := r.source(t, 1, 2, 12)
	var wg sync.WaitGroup
	errors := make(chan error, 100)
	results := make(chan store.Result, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := r.service.IngestSource(ctx, entry)
			if err != nil {
				errors <- err
				return
			}
			results <- result.Result
		}()
	}
	wg.Wait()
	close(errors)
	close(results)
	for err := range errors {
		t.Error(err)
	}
	inserted, duplicate := 0, 0
	for result := range results {
		if result == store.Inserted {
			inserted++
		} else if result == store.IdenticalDuplicate {
			duplicate++
		} else {
			t.Errorf("unexpected result %s", result)
		}
	}
	if inserted != 1 || duplicate != 99 || r.count(t, "message_observations") != 1 || r.count(t, "message_claims") != 1 || r.count(t, "cross_chain_messages") != 1 || r.count(t, "message_transitions") != 1 || r.count(t, "message_anomalies") != 0 {
		t.Fatalf("concurrent duplicates diverged: inserted=%d duplicate=%d", inserted, duplicate)
	}
}

func TestPostgresConcurrentSourceDestination(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	for id := byte(1); id <= 20; id++ {
		source, dest := r.source(t, id, 2, id), r.destination(t, id, 2, id)
		var wg sync.WaitGroup
		errors := make(chan error, 2)
		wg.Add(2)
		go func() { defer wg.Done(); _, err := r.service.IngestSource(ctx, source); errors <- err }()
		go func() { defer wg.Done(); _, err := r.service.IngestDestination(ctx, dest); errors <- err }()
		wg.Wait()
		close(errors)
		for err := range errors {
			if err != nil {
				t.Fatal(err)
			}
		}
		snap, err := r.repo.LoadMessage(ctx, r.key(id))
		if err != nil || snap.State != domain.SourceSeen || !snap.Projection.Correlated || snap.Observations != 2 || snap.Conflicts != 0 {
			t.Fatalf("race did not converge: %+v %v", snap, err)
		}
	}
	if r.count(t, "cross_chain_messages") != 20 || r.count(t, "message_observations") != 40 {
		t.Fatal("source/destination race lost evidence")
	}
}

func TestPostgresConcurrentConflict(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	one, two := r.source(t, 1, 2, 12), r.source(t, 1, 9, 13)
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); _, err := r.service.IngestSource(ctx, one); errors <- err }()
	go func() { defer wg.Done(); _, err := r.service.IngestSource(ctx, two); errors <- err }()
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	snap, err := r.repo.LoadMessage(ctx, r.key(1))
	if err != nil || !snap.Projection.Conflict || snap.Conflicts != 1 || snap.SourceClaims != 2 || r.count(t, "message_observations") != 2 {
		t.Fatalf("concurrent conflict lost: %+v %v", snap, err)
	}
}

func TestPostgresFinalityStatesUnreachable(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	_, err := r.service.IngestSource(ctx, r.source(t, 1, 2, 12))
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.service.IngestDestination(ctx, r.destination(t, 1, 2, 22))
	if err != nil {
		t.Fatal(err)
	}
	var count int
	err = r.db.QueryRow(ctx, `SELECT count(*) FROM cross_chain_messages WHERE state IN ('SOURCE_FINAL','COMPLETED')`).Scan(&count)
	if err != nil || count != 0 {
		t.Fatalf("finality state produced: %d %v", count, err)
	}
}
