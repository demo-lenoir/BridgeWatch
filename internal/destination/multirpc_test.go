package destination

import (
	"context"
	"errors"
	"testing"
	"time"

	"bridgewatch/internal/domain"
	"bridgewatch/internal/rpcpool"
	"bridgewatch/internal/source"
	"github.com/ethereum/go-ethereum/common"
)

type faultySourceReader struct {
	source.Reader
	failHead    bool
	staleHead   *uint64
	wrongChain  bool
	wrongAnchor bool
}

func (r *faultySourceReader) ChainID(ctx context.Context) (domain.ChainID, error) {
	if r.wrongChain {
		return domain.ChainID{}, nil
	}
	return r.Reader.ChainID(ctx)
}
func (r *faultySourceReader) BlockNumber(ctx context.Context) (uint64, error) {
	if r.failHead {
		return 0, errors.New("source endpoint unavailable")
	}
	if r.staleHead != nil {
		return *r.staleHead, nil
	}
	return r.Reader.BlockNumber(ctx)
}
func (r *faultySourceReader) HeaderByNumber(ctx context.Context, n uint64) (source.Header, error) {
	h, err := r.Reader.HeaderByNumber(ctx, n)
	if err == nil && n == 0 && r.wrongAnchor {
		h.Hash[0] ^= 0xff
	}
	return h, err
}
func (r *faultySourceReader) HeaderByHash(ctx context.Context, h domain.Hash) (source.Header, error) {
	if r.wrongAnchor {
		original, err := r.Reader.HeaderByNumber(ctx, 0)
		if err == nil {
			original.Hash[0] ^= 0xff
			if h == original.Hash {
				return original, nil
			}
		}
	}
	return r.Reader.HeaderByHash(ctx, h)
}

type faultyDestinationReader struct {
	Reader
	failHead  bool
	staleHead *uint64
}

func TestSourceRPCHeaderDisagreementFailsClosed(t *testing.T) {
	r := newCrossRig(t)
	r.finalSource(t, 1)
	before, err := r.sourceRepo.Checkpoint(context.Background(), r.sourceID)
	if err != nil {
		t.Fatal(err)
	}
	other := newForkChain(r.sourceID, 1)
	other.setBranch(extend(r.sourceChain.branch[:2], 77, 4))
	pool, err := rpcpool.New(r.sourceID, r.sourceChain.branch[0].Hash, []rpcpool.Endpoint[source.Reader]{{ID: "first", Reader: sourceReader{r.sourceChain}}, {ID: "second", Reader: sourceReader{other}}}, &controlledClock{now: time.Now()}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r.sourceWatcher.SetPool(pool)
	if err := r.sourceWatcher.SyncOnce(context.Background()); !errors.Is(err, source.ErrProviderDisagreement) {
		t.Fatalf("disagreement: %v", err)
	}
	after, err := r.sourceRepo.Checkpoint(context.Background(), r.sourceID)
	if err != nil || after.Number != before.Number || after.Hash != before.Hash {
		t.Fatal("checkpoint changed", after, err)
	}
	if ready, err := r.sourceWatcher.Ready(context.Background()); err != nil || ready {
		t.Fatal("readiness not degraded", err)
	}
	for _, status := range pool.Status() {
		if status.Health != "DISAGREEMENT" {
			t.Fatal(status)
		}
	}
}

func TestDestinationRPCHeaderDisagreementFailsClosed(t *testing.T) {
	r := newCrossRig(t)
	r.syncDest(t)
	a := extend(r.destChain.branch, 30, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	before, err := r.destRepo.Checkpoint(context.Background(), r.destID)
	if err != nil {
		t.Fatal(err)
	}
	other := newForkChain(r.destID, 2)
	other.setBranch(extend(a[:2], 70, 4))
	pool, err := rpcpool.New(r.destID, a[0].Hash, []rpcpool.Endpoint[Reader]{{ID: "first", Reader: r.destChain}, {ID: "second", Reader: other}}, &controlledClock{now: time.Now()}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r.destWatcher.SetPool(pool)
	if err := r.destWatcher.SyncOnce(context.Background()); !errors.Is(err, ErrProviderDisagreement) {
		t.Fatalf("disagreement: %v", err)
	}
	after, err := r.destRepo.Checkpoint(context.Background(), r.destID)
	if err != nil || after.Number != before.Number || after.Hash != before.Hash {
		t.Fatal("checkpoint changed", after, err)
	}
	if ready, err := r.destWatcher.Ready(context.Background()); err != nil || ready {
		t.Fatal("readiness not degraded", err)
	}
}

func (r *faultyDestinationReader) BlockNumber(ctx context.Context) (uint64, error) {
	if r.failHead {
		return 0, errors.New("destination endpoint unavailable")
	}
	if r.staleHead != nil {
		return *r.staleHead, nil
	}
	return r.Reader.BlockNumber(ctx)
}

func TestSourceRPCFailoverStaleOutageAndRecovery(t *testing.T) {
	r := newCrossRig(t)
	r.syncDest(t)
	r.finalSource(t, 1)
	clock := &controlledClock{now: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)}
	stale := uint64(1)
	primary := &faultySourceReader{Reader: sourceReader{r.sourceChain}, staleHead: &stale}
	secondary := &faultySourceReader{Reader: sourceReader{r.sourceChain}}
	anchor := r.sourceChain.branch[0].Hash
	pool, err := rpcpool.New(r.sourceID, anchor, []rpcpool.Endpoint[source.Reader]{{ID: "primary", Reader: primary}, {ID: "secondary", Reader: secondary}}, clock, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r.sourceWatcher.SetPool(pool)
	if err := r.sourceWatcher.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := r.sourceWatcher.Status(context.Background())
	if err != nil || !status.Ready || status.Endpoints[0].Health != "STALE" || status.Endpoints[1].Health != "HEALTHY" {
		t.Fatalf("failover status: %+v %v", status, err)
	}
	if state := r.state(t, 1); state != "SOURCE_FINAL" {
		t.Fatal(state)
	}
	secondary.failHead = true
	clock.Set(clock.Now().Add(5 * time.Second))
	primary.failHead = true
	primary.staleHead = nil
	if err := r.sourceWatcher.SyncOnce(context.Background()); err == nil {
		t.Fatal("all-provider outage accepted")
	}
	ready, err := r.sourceWatcher.Ready(context.Background())
	if err != nil || ready {
		t.Fatalf("outage readiness: %t %v", ready, err)
	}
	destReady, err := r.destWatcher.Ready(context.Background())
	if err != nil || !destReady {
		t.Fatalf("destination changed by source outage: %t %v", destReady, err)
	}
	secondary.failHead = false
	clock.Set(clock.Now().Add(5 * time.Second))
	if err := r.sourceWatcher.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	ready, err = r.sourceWatcher.Ready(context.Background())
	if err != nil || !ready || r.state(t, 1) != "SOURCE_FINAL" {
		t.Fatalf("recovery: %t %v", ready, err)
	}
}

func TestSourceRPCWrongChainAndAnchorQuarantined(t *testing.T) {
	r := newCrossRig(t)
	clock := &controlledClock{now: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)}
	anchor := r.sourceChain.branch[0].Hash
	badChain := &faultySourceReader{Reader: sourceReader{r.sourceChain}, wrongChain: true}
	badAnchor := &faultySourceReader{Reader: sourceReader{r.sourceChain}, wrongAnchor: true}
	good := &faultySourceReader{Reader: sourceReader{r.sourceChain}}
	watcher, err := source.NewWatcher(source.Config{SourceChainID: r.sourceID, Contract: common.Address{1}, StartBlock: 1, Confirmations: 3, MaxReorgDepth: 8, MaxBlocksPerRun: 8, ExpectedGenesis: &anchor}, good, r.adapter, r.sourceRepo)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := rpcpool.New(r.sourceID, anchor, []rpcpool.Endpoint[source.Reader]{{ID: "wrong_chain", Reader: badChain}, {ID: "wrong_anchor", Reader: badAnchor}, {ID: "good", Reader: good}}, clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	watcher.SetPool(pool)
	if err := watcher.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	statuses := pool.Status()
	if statuses[0].Health != "WRONG_CHAIN" || statuses[1].Health != "WRONG_ANCHOR" || statuses[2].Health != "HEALTHY" {
		t.Fatalf("endpoint eligibility: %+v", statuses)
	}
	clock.Set(clock.Now().Add(time.Hour))
	if len(pool.Candidates()) != 1 {
		t.Fatal("hard rejected endpoint returned")
	}
}

func TestDestinationRPCFailoverPreservesSource(t *testing.T) {
	r := newCrossRig(t)
	r.finalSource(t, 1)
	r.syncDest(t)
	clock := &controlledClock{now: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)}
	bad := &faultyDestinationReader{Reader: r.destChain, failHead: true}
	good := &faultyDestinationReader{Reader: r.destChain}
	anchor := r.destChain.branch[0].Hash
	pool, err := rpcpool.New(r.destID, anchor, []rpcpool.Endpoint[Reader]{{ID: "bad", Reader: bad}, {ID: "good", Reader: good}}, clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r.destWatcher.SetPool(pool)
	if err := r.destWatcher.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := r.destWatcher.Status(context.Background())
	if err != nil || !status.Ready || status.Endpoints[0].Health != "DEGRADED" || status.Endpoints[1].Health != "HEALTHY" {
		t.Fatalf("destination failover: %+v %v", status, err)
	}
	if state := r.state(t, 1); state != "SOURCE_FINAL" {
		t.Fatal(state)
	}
}
