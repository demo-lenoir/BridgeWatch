package destination

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"bridgewatch/internal/domain"
	"bridgewatch/internal/protocol"
	"bridgewatch/internal/source"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type forkChain struct {
	mu                                sync.RWMutex
	id                                domain.ChainID
	branch                            []Header
	known                             map[domain.Hash]Header
	logs                              map[domain.Hash][]types.Log
	wrongChain, badHeader, failedLogs bool
	reportedHead                      *uint64
}

func newForkChain(id domain.ChainID, genesis byte) *forkChain {
	h := Header{Hash: domain.Hash{genesis}}
	return &forkChain{id: id, branch: []Header{h}, known: map[domain.Hash]Header{h.Hash: h}, logs: map[domain.Hash][]types.Log{}}
}
func (r *forkChain) setBranch(branch []Header) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.branch = branch
	for _, h := range branch {
		r.known[h.Hash] = h
	}
}
func extend(prefix []Header, tag byte, last int) []Header {
	out := append([]Header(nil), prefix...)
	for n := len(out); n <= last; n++ {
		out = append(out, Header{Number: uint64(n), Hash: domain.Hash{tag, byte(n)}, ParentHash: out[n-1].Hash})
	}
	return out
}
func (r *forkChain) ChainID(context.Context) (domain.ChainID, error) {
	if r.wrongChain {
		return domain.ChainID{99}, nil
	}
	return r.id, nil
}
func (r *forkChain) BlockNumber(context.Context) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.reportedHead != nil {
		return *r.reportedHead, nil
	}
	return uint64(len(r.branch) - 1), nil
}
func (r *forkChain) HeaderByNumber(_ context.Context, n uint64) (Header, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n >= uint64(len(r.branch)) {
		return Header{}, errors.New("missing header")
	}
	h := r.branch[n]
	if r.badHeader && n > 0 {
		h.Number++
	}
	return h, nil
}
func (r *forkChain) HeaderByHash(_ context.Context, hash domain.Hash) (Header, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.known[hash]
	if !ok {
		return Header{}, errors.New("missing hash")
	}
	return h, nil
}
func (r *forkChain) FilterLogs(_ context.Context, from, to uint64, _ common.Address) ([]types.Log, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.failedLogs {
		return nil, errors.New("RPC timeout")
	}
	if from != to || from >= uint64(len(r.branch)) {
		return nil, errors.New("invalid log range")
	}
	return append([]types.Log(nil), r.logs[r.branch[from].Hash]...), nil
}
func (r *forkChain) addLog(h Header, log types.Log) {
	r.mu.Lock()
	defer r.mu.Unlock()
	log.BlockNumber = h.Number
	log.BlockHash = common.Hash(h.Hash)
	log.Index = uint(len(r.logs[h.Hash]))
	r.logs[h.Hash] = append(r.logs[h.Hash], log)
}

type sourceReader struct{ *forkChain }

func (r sourceReader) HeaderByNumber(ctx context.Context, n uint64) (source.Header, error) {
	h, err := r.forkChain.HeaderByNumber(ctx, n)
	return source.Header{Number: h.Number, Hash: h.Hash, ParentHash: h.ParentHash}, err
}
func (r sourceReader) HeaderByHash(ctx context.Context, hash domain.Hash) (source.Header, error) {
	h, err := r.forkChain.HeaderByHash(ctx, hash)
	return source.Header{Number: h.Number, Hash: h.Hash, ParentHash: h.ParentHash}, err
}

type crossRig struct {
	db                     *pgxpool.Pool
	sourceChain, destChain *forkChain
	sourceWatcher          *source.Watcher
	destWatcher            *Watcher
	sourceRepo             *source.PostgresRepository
	destRepo               *PostgresRepository
	adapter                *protocol.MockBridgeAdapter
	sourceID, destID       domain.ChainID
}

func newCrossRig(t *testing.T) *crossRig {
	t.Helper()
	url := os.Getenv("BRIDGEWATCH_TEST_DESTINATION_DB_URL")
	if url == "" {
		t.Skip("destination PostgreSQL URL not configured")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	_, err = db.Exec(ctx, `TRUNCATE destination_reorgs,destination_streams,source_reorgs,source_streams,message_alerts,message_transitions,message_anomalies,observation_conflicts,message_claims,message_observations,cross_chain_messages,chain_checkpoints,chain_blocks RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	sourceID, _ := domain.NewChainID(big.NewInt(31337))
	destID, _ := domain.NewChainID(big.NewInt(31338))
	adapter, err := protocol.NewMockBridgeAdapter(protocol.MockConfig{SourceChainID: sourceID, DestinationChainID: destID, SourceContract: common.Address{1}, DestinationContract: common.Address{2}, Version: 1, RelaySLA: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	sc := newForkChain(sourceID, 1)
	dc := newForkChain(destID, 2)
	sr := source.NewPostgresRepository(db)
	dr := NewPostgresRepository(db)
	sw, err := source.NewWatcher(source.Config{SourceChainID: sourceID, Contract: common.Address{1}, StartBlock: 1, Confirmations: 3, MaxReorgDepth: 8, MaxBlocksPerRun: 8}, sourceReader{sc}, adapter, sr)
	if err != nil {
		t.Fatal(err)
	}
	dw, err := NewWatcher(Config{DestinationChainID: destID, Contract: common.Address{2}, StartBlock: 1, Confirmations: 3, MaxReorgDepth: 8, MaxBlocksPerRun: 8}, dc, adapter, dr)
	if err != nil {
		t.Fatal(err)
	}
	return &crossRig{db: db, sourceChain: sc, destChain: dc, sourceWatcher: sw, destWatcher: dw, sourceRepo: sr, destRepo: dr, adapter: adapter, sourceID: sourceID, destID: destID}
}
func (r *crossRig) addSource(t *testing.T, h Header, id, tx byte) {
	t.Helper()
	data, err := r.adapter.SourceEvent().Inputs.Pack([32]byte{id}, big.NewInt(31338), common.Address{3}, common.Address{4}, [32]byte{7}, common.Address{}, big.NewInt(0))
	if err != nil {
		t.Fatal(err)
	}
	r.sourceChain.addLog(h, types.Log{Address: common.Address{1}, Topics: []common.Hash{r.adapter.SourceEvent().ID}, Data: data, TxHash: common.Hash{tx}})
}
func (r *crossRig) addDestination(t *testing.T, h Header, id, tx, payload, recipient byte) {
	t.Helper()
	data, err := r.adapter.DestinationEvent().Inputs.Pack([32]byte{id}, big.NewInt(31337), common.Address{recipient}, [32]byte{payload}, true)
	if err != nil {
		t.Fatal(err)
	}
	r.destChain.addLog(h, types.Log{Address: common.Address{2}, Topics: []common.Hash{r.adapter.DestinationEvent().ID}, Data: data, TxHash: common.Hash{tx}})
}
func (r *crossRig) syncSource(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if err := r.sourceWatcher.SyncOnce(ctx); err != nil {
			t.Fatal(err)
		}
		cp, err := r.sourceRepo.Checkpoint(ctx, r.sourceID)
		if err != nil {
			t.Fatal(err)
		}
		head, _ := r.sourceChain.BlockNumber(ctx)
		if cp.Number == head {
			return
		}
	}
	t.Fatal("source did not catch up")
}
func (r *crossRig) syncDest(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if err := r.destWatcher.SyncOnce(ctx); err != nil {
			t.Fatal(err)
		}
		cp, err := r.destRepo.Checkpoint(ctx, r.destID)
		if err != nil {
			t.Fatal(err)
		}
		head, _ := r.destChain.BlockNumber(ctx)
		if cp.Number == head {
			return
		}
	}
	t.Fatal("destination did not catch up")
}
func (r *crossRig) state(t *testing.T, id byte) string {
	t.Helper()
	var s string
	key := domain.Hash{id}
	if err := r.db.QueryRow(context.Background(), `SELECT state FROM cross_chain_messages WHERE message_id=$1`, key[:]).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}
func (r *crossRig) count(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func (r *crossRig) finalSource(t *testing.T, id byte) {
	t.Helper()
	a := extend(r.sourceChain.branch, 10, 4)
	r.addSource(t, a[2], id, id+20)
	r.sourceChain.setBranch(a)
	r.syncSource(t)
	if s := r.state(t, id); s != "SOURCE_FINAL" && s != "COMPLETED" {
		t.Fatalf("source finality: %s", s)
	}
}

func TestDestinationFinalityBoundaryRestartAndEvidence(t *testing.T) {
	r := newCrossRig(t)
	r.finalSource(t, 1)
	a := extend(r.destChain.branch, 30, 3)
	r.addDestination(t, a[2], 1, 41, 7, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	if s := r.state(t, 1); s != "DEST_SEEN" {
		t.Fatalf("N-1 confirmations: %s", s)
	}
	a = extend(a, 30, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	if s := r.state(t, 1); s != "COMPLETED" {
		t.Fatalf("N confirmations: %s", s)
	}
	var sourceObserved, destObserved int
	var sourceEvidence, destEvidence int64
	err := r.db.QueryRow(context.Background(), `SELECT source_observed_confirmations,destination_observed_confirmations,source_evidence_observation_id,destination_evidence_observation_id FROM message_transitions WHERE to_state='COMPLETED'`).Scan(&sourceObserved, &destObserved, &sourceEvidence, &destEvidence)
	if err != nil || sourceObserved < 3 || destObserved != 3 || sourceEvidence == destEvidence {
		t.Fatalf("completion evidence: %d %d %d %d %v", sourceObserved, destObserved, sourceEvidence, destEvidence, err)
	}
	var sourceHead, destHead []byte
	var sourceRequired, destRequired, sourceVersion, destVersion int
	err = r.db.QueryRow(context.Background(), `SELECT justifying_head_hash,destination_head_hash,source_required_confirmations,destination_required_confirmations,source_policy_version,destination_policy_version FROM message_transitions WHERE to_state='COMPLETED'`).Scan(&sourceHead, &destHead, &sourceRequired, &destRequired, &sourceVersion, &destVersion)
	if err != nil || !bytes.Equal(sourceHead, r.sourceChain.branch[4].Hash[:]) || !bytes.Equal(destHead, a[4].Hash[:]) || sourceRequired != 3 || destRequired != 3 || sourceVersion != 1 || destVersion != 1 {
		t.Fatalf("completion policy/head evidence: %v", err)
	}
	a = extend(a, 30, 5)
	r.destChain.setBranch(a)
	r.syncDest(t)
	r.syncDest(t)
	if r.state(t, 1) != "COMPLETED" || r.count(t, "message_transitions") != 3 {
		t.Fatal("N+1 or duplicate catch-up changed state")
	}
	restarted, err := NewWatcher(r.destWatcher.config, r.destChain, r.adapter, r.destRepo)
	if err != nil {
		t.Fatal(err)
	}
	r.destWatcher = restarted
	r.syncDest(t)
	if r.count(t, "message_transitions") != 3 {
		t.Fatal("restart duplicated completion")
	}
}

func TestDestinationFirstConvergesWithoutReingestion(t *testing.T) {
	r := newCrossRig(t)
	registry := prometheus.NewRegistry()
	sourceMetrics, err := source.NewMetrics(registry, r.sourceID, "source_local")
	if err != nil {
		t.Fatal(err)
	}
	destMetrics, err := NewMetrics(registry, r.destID, "destination_local")
	if err != nil {
		t.Fatal(err)
	}
	r.sourceWatcher.SetMetrics(sourceMetrics)
	r.destWatcher.SetMetrics(destMetrics)
	a := extend(r.destChain.branch, 30, 4)
	r.addDestination(t, a[2], 1, 41, 7, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	if s := r.state(t, 1); s != "UNMATCHED_DESTINATION" {
		t.Fatal(s)
	}
	r.finalSource(t, 1)
	if s := r.state(t, 1); s != "COMPLETED" {
		t.Fatal(s)
	}
	if r.count(t, "cross_chain_messages") != 1 || r.count(t, "message_observations") != 2 || r.count(t, "message_transitions") != 2 {
		t.Fatal("destination-first evidence duplicated")
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var completed, unmatched float64
	for _, family := range families {
		if family.GetName() == "bridgewatch_messages_total" {
			for _, metric := range family.Metric {
				for _, label := range metric.Label {
					if label.GetName() == "state" && label.GetValue() == "COMPLETED" {
						completed = metric.GetCounter().GetValue()
					}
				}
			}
		}
		if family.GetName() == "bridgewatch_unmatched_destination_total" {
			for _, metric := range family.Metric {
				unmatched += metric.GetCounter().GetValue()
			}
		}
	}
	if completed != 1 || unmatched != 1 {
		t.Fatalf("shared transition metrics: completed=%v unmatched=%v", completed, unmatched)
	}
}

func TestDestinationReorgBeforeAndAfterCompletion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		head    int
		initial string
	}{{"before", 3, "DEST_SEEN"}, {"after", 4, "COMPLETED"}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newCrossRig(t)
			r.finalSource(t, 1)
			a := extend(r.destChain.branch, 30, tc.head)
			r.addDestination(t, a[2], 1, 41, 7, 4)
			r.destChain.setBranch(a)
			r.syncDest(t)
			if s := r.state(t, 1); s != tc.initial {
				t.Fatal(s)
			}
			b := extend(a[:2], 40, tc.head)
			r.destChain.setBranch(b)
			r.syncDest(t)
			if s := r.state(t, 1); s != "SOURCE_FINAL" {
				t.Fatalf("completion not withdrawn: %s", s)
			}
			var withdrawn, completed int
			if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM message_transitions WHERE to_state='SOURCE_FINAL' AND destination_reorg_id IS NOT NULL`).Scan(&withdrawn); err != nil {
				t.Fatal(err)
			}
			if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM message_transitions WHERE to_state='COMPLETED'`).Scan(&completed); err != nil {
				t.Fatal(err)
			}
			if withdrawn != 1 || (tc.name == "before" && completed != 0) || r.count(t, "message_observations") != 2 {
				t.Fatal("destination fork history missing")
			}
			var sourceCanonical int
			if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM chain_blocks WHERE chain_id=31337 AND canonical`).Scan(&sourceCanonical); err != nil || sourceCanonical != 5 {
				t.Fatalf("destination reorg altered source canonical branch: %d %v", sourceCanonical, err)
			}
		})
	}
}

func TestSourceReorgWithdrawsCompleted(t *testing.T) {
	r := newCrossRig(t)
	r.finalSource(t, 1)
	a := extend(r.destChain.branch, 30, 4)
	r.addDestination(t, a[2], 1, 41, 7, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	if r.state(t, 1) != "COMPLETED" {
		t.Fatal("precondition")
	}
	b := extend(r.sourceChain.branch[:2], 50, 4)
	r.sourceChain.setBranch(b)
	r.syncSource(t)
	if s := r.state(t, 1); s != "UNMATCHED_DESTINATION" {
		t.Fatalf("source reorg: %s", s)
	}
	var canonicalDest bool
	if err := r.db.QueryRow(context.Background(), `SELECT b.canonical FROM message_observations o JOIN chain_blocks b ON b.chain_id=o.chain_id AND b.block_hash=o.block_hash WHERE o.chain_id=31338`).Scan(&canonicalDest); err != nil || !canonicalDest {
		t.Fatal("destination evidence changed with source reorg", err)
	}
}

func TestDestinationForkDepthAndReturn(t *testing.T) {
	for _, depth := range []int{1, 2, 8} {
		t.Run(string(rune('0'+depth)), func(t *testing.T) {
			r := newCrossRig(t)
			r.finalSource(t, 1)
			a := extend(r.destChain.branch, 30, depth+1)
			r.addDestination(t, a[depth+1], 1, 41, 7, 4)
			r.destChain.setBranch(a)
			r.syncDest(t)
			b := extend(a[:2], 40, depth+1)
			r.destChain.setBranch(b)
			r.syncDest(t)
			if s := r.state(t, 1); s != "SOURCE_FINAL" {
				t.Fatal(s)
			}
			r.destChain.setBranch(a)
			r.syncDest(t)
			if s := r.state(t, 1); s != "DEST_SEEN" {
				t.Fatal(s)
			}
			if r.count(t, "cross_chain_messages") != 1 || r.count(t, "message_observations") != 2 || r.count(t, "destination_reorgs") != 2 {
				t.Fatal("A-B-A history lost or duplicated")
			}
		})
	}
}

func TestDestinationAlternateExecutionAndConflict(t *testing.T) {
	r := newCrossRig(t)
	r.finalSource(t, 1)
	a := extend(r.destChain.branch, 30, 4)
	r.addDestination(t, a[2], 1, 41, 7, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	b := extend(a[:2], 40, 4)
	r.addDestination(t, b[2], 1, 42, 7, 4)
	r.destChain.setBranch(b)
	r.syncDest(t)
	if r.state(t, 1) != "COMPLETED" || r.count(t, "cross_chain_messages") != 1 || r.count(t, "message_observations") != 3 {
		t.Fatal("alternate execution did not converge")
	}
	c := extend(b, 40, 5)
	r.addDestination(t, c[5], 1, 43, 8, 4)
	r.destChain.setBranch(c)
	r.syncDest(t)
	if r.state(t, 1) == "COMPLETED" {
		t.Fatal("conflicting execution completed")
	}
	var conflicts, duplicates int
	if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM message_anomalies WHERE kind='CONFLICT' AND resolved_at IS NULL`).Scan(&conflicts); err != nil {
		t.Fatal(err)
	}
	if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM message_anomalies WHERE kind='DUPLICATE_EXECUTION' AND resolved_at IS NULL`).Scan(&duplicates); err != nil {
		t.Fatal(err)
	}
	if conflicts != 1 || duplicates != 1 {
		t.Fatalf("anomalies conflict=%d duplicate=%d", conflicts, duplicates)
	}
}

func TestDestinationDeepReorgAndStaleProvider(t *testing.T) {
	r := newCrossRig(t)
	r.finalSource(t, 1)
	a := extend(r.destChain.branch, 30, 10)
	r.addDestination(t, a[2], 1, 41, 7, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	if r.state(t, 1) != "COMPLETED" {
		t.Fatal("precondition")
	}
	stale := uint64(9)
	r.destChain.reportedHead = &stale
	if err := r.destWatcher.SyncOnce(context.Background()); !errors.Is(err, ErrStaleHead) {
		t.Fatalf("stale endpoint: %v", err)
	}
	if r.state(t, 1) != "COMPLETED" {
		t.Fatal("stale endpoint revoked completion")
	}
	r.destChain.reportedHead = nil
	b := extend(a[:2], 40, 10)
	r.destChain.setBranch(b)
	if err := r.destWatcher.SyncOnce(context.Background()); !errors.Is(err, ErrDeepReorg) {
		t.Fatalf("deep reorg: %v", err)
	}
	cp, err := r.destRepo.Checkpoint(context.Background(), r.destID)
	if err != nil || cp.Number != 10 || cp.Health != "MANUAL_INTERVENTION" {
		t.Fatalf("checkpoint jumped: %+v %v", cp, err)
	}
	status, err := r.destWatcher.Status(context.Background())
	if err != nil || !status.ManualIntervention || status.Ready {
		t.Fatalf("destination status: %+v %v", status, err)
	}
	sourceStatus, err := r.sourceWatcher.Status(context.Background())
	if err != nil || !sourceStatus.Ready {
		t.Fatalf("source status: %+v %v", sourceStatus, err)
	}
	r.sourceChain.setBranch(extend(r.sourceChain.branch, 10, 5))
	r.syncSource(t)
	if r.state(t, 1) != "COMPLETED" {
		t.Fatal("manual destination status revoked previously justified completion")
	}
}

func TestDuplicateCanonicalDestinationExecutionBlocksCompletion(t *testing.T) {
	r := newCrossRig(t)
	r.finalSource(t, 1)
	a := extend(r.destChain.branch, 30, 4)
	r.addDestination(t, a[2], 1, 41, 7, 4)
	r.addDestination(t, a[3], 1, 42, 7, 4)
	r.destChain.setBranch(a)
	r.syncDest(t)
	if state := r.state(t, 1); state == "COMPLETED" {
		t.Fatal("duplicate execution completed")
	}
	var episodes int
	if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM message_anomalies WHERE kind='DUPLICATE_EXECUTION' AND resolved_at IS NULL`).Scan(&episodes); err != nil || episodes != 1 {
		t.Fatalf("duplicate anomaly: %d %v", episodes, err)
	}
	if r.count(t, "message_observations") != 3 {
		t.Fatal("distinct destination execution overwritten")
	}
	b := extend(a[:3], 40, 4)
	r.destChain.setBranch(b)
	r.syncDest(t)
	if state := r.state(t, 1); state != "COMPLETED" {
		t.Fatalf("remaining canonical execution did not regain completion: %s", state)
	}
	if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM message_anomalies WHERE kind='DUPLICATE_EXECUTION' AND resolved_at IS NULL`).Scan(&episodes); err != nil || episodes != 0 {
		t.Fatalf("duplicate anomaly did not resolve: %d %v", episodes, err)
	}
}

func TestDestinationApplyRollbackIsAtomic(t *testing.T) {
	r := newCrossRig(t)
	r.syncDest(t)
	a := extend(r.destChain.branch, 30, 2)
	first := BlockEvidence{Header: a[1]}
	second := BlockEvidence{Header: a[2], Claims: []domain.DestinationClaim{{}}}
	err := r.destRepo.Apply(context.Background(), r.destWatcher.config, a[0], []BlockEvidence{first, second}, false)
	if err == nil {
		t.Fatal("invalid claim accepted")
	}
	cp, err := r.destRepo.Checkpoint(context.Background(), r.destID)
	if err != nil || cp.Number != 0 || r.count(t, "chain_blocks") != 1 {
		t.Fatalf("partial destination unit committed: %+v %v", cp, err)
	}
	r.destChain.setBranch(a)
	r.syncDest(t)
	if cp, err = r.destRepo.Checkpoint(context.Background(), r.destID); err != nil || cp.Number != 2 {
		t.Fatalf("retry did not catch up: %+v %v", cp, err)
	}
}

func TestDestinationEndpointRejectsInvalidEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*crossRig)
	}{
		{"wrong chain", func(r *crossRig) { r.destChain.wrongChain = true }},
		{"wrong anchor", func(r *crossRig) { bad := domain.Hash{9}; r.destWatcher.config.ExpectedGenesis = &bad }},
		{"bad header", func(r *crossRig) { r.destChain.badHeader = true }},
		{"log timeout", func(r *crossRig) { r.destChain.failedLogs = true }},
		{"wrong contract", func(r *crossRig) { r.destChain.logs[r.destChain.branch[1].Hash][0].Address = common.Address{9} }},
		{"wrong block hash", func(r *crossRig) { r.destChain.logs[r.destChain.branch[1].Hash][0].BlockHash = common.Hash{9} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newCrossRig(t)
			a := extend(r.destChain.branch, 30, 1)
			r.addDestination(t, a[1], 1, 41, 7, 4)
			r.destChain.setBranch(a)
			tc.change(r)
			if err := r.destWatcher.SyncOnce(context.Background()); err == nil {
				t.Fatal("invalid evidence accepted")
			}
			if r.count(t, "message_observations") != 0 {
				t.Fatal("invalid evidence persisted")
			}
		})
	}
}

func TestDestinationGapCatchupAndRestartDuringReorg(t *testing.T) {
	r := newCrossRig(t)
	r.finalSource(t, 1)
	a := extend(r.destChain.branch, 30, 12)
	r.addDestination(t, a[2], 1, 41, 7, 4)
	r.addDestination(t, a[11], 2, 42, 7, 4)
	r.destChain.setBranch(a)
	if err := r.destWatcher.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	cp, err := r.destRepo.Checkpoint(context.Background(), r.destID)
	if err != nil || cp.Number != 8 {
		t.Fatalf("bounded first batch: %+v %v", cp, err)
	}
	restarted, err := NewWatcher(r.destWatcher.config, r.destChain, r.adapter, r.destRepo)
	if err != nil {
		t.Fatal(err)
	}
	r.destWatcher = restarted
	r.syncDest(t)
	if cp, err = r.destRepo.Checkpoint(context.Background(), r.destID); err != nil || cp.Number != 12 || r.count(t, "message_observations") != 3 {
		t.Fatalf("gap recovery: %+v %v", cp, err)
	}
	b := extend(a[:11], 40, 12)
	r.destChain.setBranch(b)
	restarted, err = NewWatcher(r.destWatcher.config, r.destChain, r.adapter, r.destRepo)
	if err != nil {
		t.Fatal(err)
	}
	r.destWatcher = restarted
	r.syncDest(t)
	if r.state(t, 2) != "NO_CANONICAL_EVIDENCE" || r.state(t, 1) != "COMPLETED" {
		t.Fatal("restart reorg projection")
	}
	if r.count(t, "message_observations") != 3 || r.count(t, "destination_reorgs") != 1 {
		t.Fatal("reorg evidence not retained")
	}
}

func TestConcurrentSourceReorgAndDestinationFinality(t *testing.T) {
	for round := 0; round < 8; round++ {
		r := newCrossRig(t)
		r.finalSource(t, 1)
		a := extend(r.destChain.branch, 30, 3)
		r.addDestination(t, a[2], 1, 41, 7, 4)
		r.destChain.setBranch(a)
		r.syncDest(t)
		if r.state(t, 1) != "DEST_SEEN" {
			t.Fatal("precondition")
		}
		r.destChain.setBranch(extend(a, 30, 4))
		r.sourceChain.setBranch(extend(r.sourceChain.branch[:2], 50, 4))
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() { <-start; results <- r.destWatcher.SyncOnce(context.Background()) }()
		go func() { <-start; results <- r.sourceWatcher.SyncOnce(context.Background()) }()
		close(start)
		for i := 0; i < 2; i++ {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if state := r.state(t, 1); state != "UNMATCHED_DESTINATION" {
			t.Fatalf("round %d: %s", round, state)
		}
		var sourceCanonical int
		if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM message_claims mc JOIN message_observations o ON o.id=mc.observation_id JOIN chain_blocks b ON b.chain_id=o.chain_id AND b.block_hash=o.block_hash WHERE mc.role='SOURCE' AND b.canonical`).Scan(&sourceCanonical); err != nil || sourceCanonical != 0 {
			t.Fatalf("canonical source after race: %d %v", sourceCanonical, err)
		}
	}
}

func TestConcurrentCompletionAndDestinationReorg(t *testing.T) {
	for round := 0; round < 8; round++ {
		r := newCrossRig(t)
		r.finalSource(t, 1)
		a := extend(r.destChain.branch, 30, 3)
		r.addDestination(t, a[2], 1, 41, 7, 4)
		r.destChain.setBranch(a)
		r.syncDest(t)
		finalChain := newForkChain(r.destID, 2)
		finalChain.setBranch(extend(a, 30, 4))
		finalChain.logs = r.destChain.logs
		b := extend(a[:2], 40, 4)
		r.destChain.setBranch(b)
		finalWatcher, err := NewWatcher(r.destWatcher.config, finalChain, r.adapter, r.destRepo)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() { <-start; results <- finalWatcher.SyncOnce(context.Background()) }()
		go func() { <-start; results <- r.destWatcher.SyncOnce(context.Background()) }()
		close(start)
		for i := 0; i < 2; i++ {
			<-results
		}
		// A later authoritative B read must reconcile any A commit that won first.
		r.syncDest(t)
		if state := r.state(t, 1); state != "SOURCE_FINAL" {
			t.Fatalf("round %d: %s", round, state)
		}
		var invalidCompletion int
		if err := r.db.QueryRow(context.Background(), `SELECT count(*) FROM cross_chain_messages m WHERE m.state='COMPLETED' AND NOT EXISTS (SELECT 1 FROM message_claims mc JOIN message_observations o ON o.id=mc.observation_id JOIN chain_blocks b ON b.chain_id=o.chain_id AND b.block_hash=o.block_hash WHERE mc.message_pk=m.id AND mc.role='DESTINATION' AND b.canonical)`).Scan(&invalidCompletion); err != nil || invalidCompletion != 0 {
			t.Fatalf("false completion: %d %v", invalidCompletion, err)
		}
	}
}
