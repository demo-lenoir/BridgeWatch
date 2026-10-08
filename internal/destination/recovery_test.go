package destination

import (
	"context"
	"errors"
	"testing"
	"time"

	"bridgewatch/internal/domain"
	"bridgewatch/internal/recovery"
	"bridgewatch/internal/source"
	"github.com/ethereum/go-ethereum/common"
)

type recoveryForkReader struct{ *forkChain }

func (r recoveryForkReader) HeaderByNumber(ctx context.Context, n uint64) (recovery.Header, error) {
	h, err := r.forkChain.HeaderByNumber(ctx, n)
	return recovery.Header{Number: h.Number, Hash: h.Hash, ParentHash: h.ParentHash}, err
}
func (r recoveryForkReader) HeaderByHash(ctx context.Context, hash domain.Hash) (recovery.Header, error) {
	h, err := r.forkChain.HeaderByHash(ctx, hash)
	return recovery.Header{Number: h.Number, Hash: h.Hash, ParentHash: h.ParentHash}, err
}
func (r recoveryForkReader) FilterLogs(ctx context.Context, from, to uint64, address common.Address) error {
	_, err := r.forkChain.FilterLogs(ctx, from, to, address)
	return err
}

func TestManualDestinationRecoveryWithdrawsCompletion(t *testing.T) {
	r := newCrossRig(t)
	r.finalSource(t, 1)
	a := extend(r.destChain.branch, 30, 12)
	r.addDestination(t, a[2], 1, 41, 7, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	if state := r.state(t, 1); state != "COMPLETED" {
		t.Fatal(state)
	}
	b := extend(a[:2], 40, 12)
	r.destChain.setBranch(b)
	ctx := context.Background()
	if err := r.destWatcher.SyncOnce(ctx); !errors.Is(err, ErrDeepReorg) {
		t.Fatalf("deep reorg: %v", err)
	}
	if ready, err := r.destWatcher.Ready(ctx); err != nil || ready {
		t.Fatal("manual state ready", err)
	}
	checkpoint, err := r.destRepo.Checkpoint(ctx, r.destID)
	if err != nil || checkpoint.Number != 12 || checkpoint.Health != "MANUAL_INTERVENTION" {
		t.Fatal(checkpoint, err)
	}
	service, err := recovery.New(r.db, recoveryForkReader{r.destChain}, &controlledClock{now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	request := recovery.Request{Role: "DESTINATION", ChainID: r.destID, Contract: common.Address{2}, Genesis: a[0].Hash, StartBlock: 1, Confirmations: 3, MaxReorgDepth: 8, ExpectedCheckpoint: checkpoint.Number, ExpectedCheckpointHash: checkpoint.Hash, Ancestor: 1, AncestorHash: a[1].Hash}
	before := r.count(t, "message_transitions")
	plan, err := service.Plan(ctx, request)
	if err != nil || plan.AffectedBlocks != 11 || plan.AffectedMessages != 1 || plan.Executed {
		t.Fatalf("plan: %+v %v", plan, err)
	}
	if r.count(t, "message_transitions") != before {
		t.Fatal("dry run mutated transitions")
	}
	checkpoint, err = r.destRepo.Checkpoint(ctx, r.destID)
	if err != nil || checkpoint.Number != 12 {
		t.Fatal(checkpoint, err)
	}
	plan, err = service.Execute(ctx, request)
	if err != nil || !plan.Executed {
		t.Fatalf("execute: %+v %v", plan, err)
	}
	if state := r.state(t, 1); state == "COMPLETED" {
		t.Fatal("completion retained after evidence invalidation")
	}
	if ready, err := r.destWatcher.Ready(ctx); err != nil || ready {
		t.Fatal("ready before replay", err)
	}
	r.syncDest(t)
	if state := r.state(t, 1); state != "SOURCE_FINAL" {
		t.Fatal(state)
	}
	if ready, err := r.destWatcher.Ready(ctx); err != nil || !ready {
		t.Fatal("readiness not restored", err)
	}
	var resolved bool
	if err := r.db.QueryRow(ctx, `SELECT resolved_at IS NOT NULL FROM chain_incidents WHERE role='DESTINATION'`).Scan(&resolved); err != nil || !resolved {
		t.Fatal("incident not resolved", err)
	}
	transitions := r.count(t, "message_transitions")
	observations := r.count(t, "message_observations")
	plan, err = service.Execute(ctx, request)
	if err != nil || !plan.AlreadyRecovered || plan.Executed {
		t.Fatalf("idempotent execute: %+v %v", plan, err)
	}
	if r.count(t, "message_transitions") != transitions || r.count(t, "message_observations") != observations || r.count(t, "chain_incidents") != 1 {
		t.Fatal("repeated recovery created records")
	}
}

func TestManualSourceRecoveryRecomputesStuck(t *testing.T) {
	r, businessClock, service := operationalRig(t)
	ctx := context.Background()
	businessClock.Set(businessClock.Now().Add(time.Minute))
	if n, err := service.ClassifyDue(ctx, 10); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if state := r.state(t, 1); state != "RELAY_PENDING" {
		t.Fatal(state)
	}
	var openStuck int
	if err := r.db.QueryRow(ctx, `SELECT count(*) FROM message_anomalies WHERE kind='STUCK' AND resolved_at IS NULL`).Scan(&openStuck); err != nil || openStuck != 1 {
		t.Fatal("missing stuck episode", err)
	}
	a := extend(r.sourceChain.branch, 10, 12)
	r.sourceChain.setBranch(a)
	r.syncSource(t)
	b := extend(a[:2], 50, 12)
	r.sourceChain.setBranch(b)
	if err := r.sourceWatcher.SyncOnce(ctx); !errors.Is(err, source.ErrDeepReorg) {
		t.Fatalf("deep source reorg: %v", err)
	}
	checkpoint, err := r.sourceRepo.Checkpoint(ctx, r.sourceID)
	if err != nil {
		t.Fatal(err)
	}
	recoveryService, err := recovery.New(r.db, recoveryForkReader{r.sourceChain}, businessClock)
	if err != nil {
		t.Fatal(err)
	}
	request := recovery.Request{Role: "SOURCE", ChainID: r.sourceID, Contract: common.Address{1}, Genesis: a[0].Hash, StartBlock: 1, Confirmations: 3, MaxReorgDepth: 8, ExpectedCheckpoint: checkpoint.Number, ExpectedCheckpointHash: checkpoint.Hash, Ancestor: 1, AncestorHash: a[1].Hash}
	if _, err := recoveryService.Execute(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := r.db.QueryRow(ctx, `SELECT count(*) FROM message_anomalies WHERE kind='STUCK' AND resolved_at IS NULL`).Scan(&openStuck); err != nil || openStuck != 0 {
		t.Fatal("stuck retained after source evidence withdrawal", err)
	}
	r.syncSource(t)
	if ready, err := r.sourceWatcher.Ready(ctx); err != nil || !ready {
		t.Fatal("source readiness", err)
	}
	var historical int
	if err := r.db.QueryRow(ctx, `SELECT count(*) FROM message_anomalies WHERE kind='STUCK'`).Scan(&historical); err != nil || historical != 1 {
		t.Fatal("historical stuck episode lost", historical, err)
	}
	if r.count(t, "message_alerts") != 2 {
		t.Fatal("unexpected alert count")
	}
}
