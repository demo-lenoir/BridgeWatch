package source

import (
	"context"
	"errors"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"bridgewatch/internal/domain"
	"bridgewatch/internal/protocol"
	"bridgewatch/internal/store"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgxpool"
)

type forkReader struct {
	mu           sync.RWMutex
	chain        domain.ChainID
	branch       []Header
	known        map[domain.Hash]Header
	logs         map[domain.Hash][]types.Log
	badChain     bool
	badHeader    bool
	failedLogs   bool
	reportedHead *uint64
}

func newForkReader(chain domain.ChainID) *forkReader {
	genesis := Header{Number: 0, Hash: domain.Hash{1}}
	return &forkReader{chain: chain, branch: []Header{genesis}, known: map[domain.Hash]Header{genesis.Hash: genesis}, logs: map[domain.Hash][]types.Log{}}
}
func (r *forkReader) setBranch(branch []Header) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.branch = branch
	for _, h := range branch {
		r.known[h.Hash] = h
	}
}
func (r *forkReader) ChainID(context.Context) (domain.ChainID, error) {
	if r.badChain {
		return domain.ChainID{99}, nil
	}
	return r.chain, nil
}
func (r *forkReader) BlockNumber(context.Context) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.reportedHead != nil {
		return *r.reportedHead, nil
	}
	return uint64(len(r.branch) - 1), nil
}
func (r *forkReader) HeaderByNumber(_ context.Context, n uint64) (Header, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n >= uint64(len(r.branch)) {
		return Header{}, errors.New("header unavailable")
	}
	h := r.branch[n]
	if r.badHeader && n > 0 {
		h.Number++
	}
	return h, nil
}
func (r *forkReader) HeaderByHash(_ context.Context, h domain.Hash) (Header, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.known[h]
	if !ok {
		return Header{}, errors.New("hash unavailable")
	}
	return v, nil
}
func (r *forkReader) FilterLogs(_ context.Context, from, to uint64, _ common.Address) ([]types.Log, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.failedLogs {
		return nil, errors.New("RPC timeout")
	}
	if from != to || from >= uint64(len(r.branch)) {
		return nil, errors.New("invalid range")
	}
	return append([]types.Log(nil), r.logs[r.branch[from].Hash]...), nil
}
func branch(prefix []Header, tag byte, last int) []Header {
	out := append([]Header(nil), prefix...)
	for n := len(out); n <= last; n++ {
		parent := out[n-1].Hash
		h := Header{Number: uint64(n), Hash: domain.Hash{tag, byte(n)}, ParentHash: parent}
		out = append(out, h)
	}
	return out
}

type forkRig struct {
	db               *pgxpool.Pool
	repo             *PostgresRepository
	reader           *forkReader
	adapter          *protocol.MockBridgeAdapter
	watcher          *Watcher
	config           Config
	sourceID, destID domain.ChainID
}

func newForkRig(t *testing.T, start uint64) *forkRig {
	t.Helper()
	url := os.Getenv("BRIDGEWATCH_TEST_SOURCE_DB_URL")
	if url == "" {
		t.Skip("source PostgreSQL URL not configured")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, `TRUNCATE source_reorgs,source_streams,message_alerts,message_transitions,message_anomalies,observation_conflicts,message_claims,message_observations,cross_chain_messages,chain_checkpoints,chain_blocks RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	sourceID, _ := domain.NewChainID(big.NewInt(31337))
	destID, _ := domain.NewChainID(big.NewInt(31338))
	adapter, err := protocol.NewMockBridgeAdapter(protocol.MockConfig{SourceChainID: sourceID, DestinationChainID: destID, SourceContract: common.Address{1}, DestinationContract: common.Address{2}, Version: 1, RelaySLA: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	config := Config{SourceChainID: sourceID, Contract: common.Address{1}, StartBlock: start, Confirmations: 3, MaxReorgDepth: 8, MaxBlocksPerRun: 16}
	reader := newForkReader(sourceID)
	repo := NewPostgresRepository(pool)
	watcher, err := NewWatcher(config, reader, adapter, repo)
	if err != nil {
		t.Fatal(err)
	}
	return &forkRig{db: pool, repo: repo, reader: reader, adapter: adapter, watcher: watcher, config: config, sourceID: sourceID, destID: destID}
}
func (r *forkRig) addLog(t *testing.T, h Header, id, tx byte) {
	t.Helper()
	data, err := r.adapter.SourceEvent().Inputs.Pack([32]byte{id}, big.NewInt(31338), common.Address{3}, common.Address{4}, [32]byte{7}, common.Address{}, big.NewInt(0))
	if err != nil {
		t.Fatal(err)
	}
	r.reader.logs[h.Hash] = append(r.reader.logs[h.Hash], types.Log{Address: common.Address{1}, Topics: []common.Hash{r.adapter.SourceEvent().ID}, Data: data, BlockNumber: h.Number, BlockHash: common.Hash(h.Hash), TxHash: common.Hash{tx}, Index: uint(len(r.reader.logs[h.Hash]))})
}
func (r *forkRig) sync(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 30; i++ {
		if err := r.watcher.SyncOnce(ctx); err != nil {
			t.Fatal(err)
		}
		cp, err := r.repo.Checkpoint(ctx, r.sourceID)
		if err != nil {
			t.Fatal(err)
		}
		head, _ := r.reader.BlockNumber(ctx)
		if cp.Number == head {
			return
		}
	}
	t.Fatal("watcher did not catch up")
}
func (r *forkRig) state(t *testing.T, id byte) string {
	t.Helper()
	var state string
	key := domain.Hash{id}
	err := r.db.QueryRow(context.Background(), "SELECT state FROM cross_chain_messages WHERE message_id=$1", key[:]).Scan(&state)
	if err != nil {
		t.Fatal(err)
	}
	return state
}
func (r *forkRig) count(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := r.db.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSourceBackfillFinalityBoundary(t *testing.T) {
	r := newForkRig(t, 1)
	a := branch(r.reader.branch, 10, 2)
	r.addLog(t, a[2], 1, 12)
	r.reader.setBranch(a)
	r.sync(t)
	if got := r.state(t, 1); got != "SOURCE_SEEN" {
		t.Fatal(got)
	}
	a = branch(a, 10, 3)
	r.reader.setBranch(a)
	r.sync(t)
	if got := r.state(t, 1); got != "SOURCE_SEEN" {
		t.Fatal("two confirmations: " + got)
	}
	a = branch(a, 10, 4)
	r.reader.setBranch(a)
	r.sync(t)
	if got := r.state(t, 1); got != "SOURCE_FINAL" {
		t.Fatal("three confirmations: " + got)
	}
	a = branch(a, 10, 5)
	r.reader.setBranch(a)
	r.sync(t)
	if r.state(t, 1) != "SOURCE_FINAL" || r.count(t, "message_transitions") != 2 {
		t.Fatal("four confirmations changed transition")
	}
	var number int64
	var hash []byte
	err := r.db.QueryRow(context.Background(), "SELECT justifying_head_number,justifying_head_hash FROM message_transitions WHERE to_state='SOURCE_FINAL'").Scan(&number, &hash)
	if err != nil || number != 4 || !bytesEqual(hash, a[4].Hash[:]) {
		t.Fatal("finality head evidence missing")
	}
}

func TestSourceEmptyAndMultipleEventBlocksAreIdempotent(t *testing.T) {
	r := newForkRig(t, 1)
	a := branch(r.reader.branch, 10, 4)
	r.addLog(t, a[2], 1, 12)
	r.addLog(t, a[2], 2, 13)
	r.reader.setBranch(a)
	r.sync(t)
	if r.count(t, "chain_blocks") != 5 || r.count(t, "message_observations") != 2 || r.count(t, "cross_chain_messages") != 2 {
		t.Fatal("empty or multi-event block was not represented exactly once")
	}
	r.sync(t)
	if r.count(t, "message_observations") != 2 || r.count(t, "message_transitions") != 2 {
		t.Fatal("duplicate backfill changed business evidence")
	}
}
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSourceReorgBeforeAndAfterFinality(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		head, logBlock, forkAt int
		before                 string
	}{{"before", 3, 3, 2, "SOURCE_SEEN"}, {"after", 5, 2, 1, "SOURCE_FINAL"}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newForkRig(t, 1)
			a := branch(r.reader.branch, 10, tc.head)
			r.addLog(t, a[tc.logBlock], 1, 12)
			r.reader.setBranch(a)
			r.sync(t)
			if r.state(t, 1) != tc.before {
				t.Fatal("initial state")
			}
			b := branch(a[:tc.forkAt+1], 20, tc.head)
			r.reader.setBranch(b)
			r.sync(t)
			if r.state(t, 1) != "NO_CANONICAL_EVIDENCE" || r.count(t, "message_observations") != 1 || r.count(t, "source_reorgs") != 1 {
				t.Fatal("orphan evidence or rollback lost")
			}
			var finalTransitions, linkedWithdrawals int
			if err := r.db.QueryRow(context.Background(), "SELECT count(*) FROM message_transitions WHERE to_state='SOURCE_FINAL'").Scan(&finalTransitions); err != nil {
				t.Fatal(err)
			}
			if err := r.db.QueryRow(context.Background(), "SELECT count(*) FROM message_transitions WHERE to_state='NO_CANONICAL_EVIDENCE' AND source_reorg_id IS NOT NULL").Scan(&linkedWithdrawals); err != nil {
				t.Fatal(err)
			}
			wantFinal := 0
			if tc.before == "SOURCE_FINAL" {
				wantFinal = 1
			}
			if finalTransitions != wantFinal || linkedWithdrawals != 1 {
				t.Fatalf("final transitions=%d want=%d linked withdrawals=%d", finalTransitions, wantFinal, linkedWithdrawals)
			}
			var canonical bool
			err := r.db.QueryRow(context.Background(), "SELECT canonical FROM chain_blocks WHERE block_hash=$1", a[tc.logBlock].Hash[:]).Scan(&canonical)
			if err != nil || canonical {
				t.Fatal("old source block still canonical")
			}
		})
	}
}

func TestSourceReorgDepthsAndBranchReturn(t *testing.T) {
	for _, depth := range []int{1, 2, 8} {
		t.Run(string(rune('0'+depth)), func(t *testing.T) {
			r := newForkRig(t, 1)
			a := branch(r.reader.branch, 10, 10)
			r.addLog(t, a[10], 1, 12)
			r.reader.setBranch(a)
			r.sync(t)
			b := branch(a[:11-depth], 20, 10)
			r.reader.setBranch(b)
			r.sync(t)
			var recorded int64
			err := r.db.QueryRow(context.Background(), "SELECT depth FROM source_reorgs").Scan(&recorded)
			if err != nil || recorded != int64(depth) {
				t.Fatalf("depth=%d recorded=%d %v", depth, recorded, err)
			}
			if r.state(t, 1) != "NO_CANONICAL_EVIDENCE" {
				t.Fatal("orphan retained as active")
			}
			if depth == 1 {
				r.reader.setBranch(a)
				r.sync(t)
				if r.state(t, 1) != "SOURCE_SEEN" || r.count(t, "message_observations") != 1 || r.count(t, "source_reorgs") != 2 || r.count(t, "cross_chain_messages") != 1 {
					t.Fatal("A-B-A did not converge")
				}
			}
		})
	}
}

func TestSourceDeepReorgFailsClosed(t *testing.T) {
	r := newForkRig(t, 1)
	a := branch(r.reader.branch, 10, 10)
	r.reader.setBranch(a)
	r.sync(t)
	b := branch(a[:2], 20, 10)
	r.reader.setBranch(b)
	err := r.watcher.SyncOnce(context.Background())
	if !errors.Is(err, ErrDeepReorg) {
		t.Fatalf("deep reorg accepted: %v", err)
	}
	cp, err := r.repo.Checkpoint(context.Background(), r.sourceID)
	if err != nil || cp.Number != 10 || cp.Hash != a[10].Hash {
		t.Fatal("checkpoint moved on deep reorg")
	}
	ready, err := r.watcher.Ready(context.Background())
	if err != nil || ready {
		t.Fatal("deep reorg left ready")
	}
	var severity, disposition string
	if err = r.db.QueryRow(context.Background(), "SELECT severity,disposition FROM source_reorgs").Scan(&severity, &disposition); err != nil || severity != "CRITICAL" || disposition != "MANUAL_INTERVENTION" {
		t.Fatalf("deep reorg evidence=%s/%s err=%v", severity, disposition, err)
	}
}

func TestSourceCheckpointCorruptionFailsClosed(t *testing.T) {
	r := newForkRig(t, 1)
	a := branch(r.reader.branch, 10, 2)
	r.reader.setBranch(a)
	r.sync(t)
	ctx := context.Background()
	bad := domain.Hash{99}
	_, err := r.db.Exec(ctx, `INSERT INTO chain_blocks(chain_id,block_hash,block_number,parent_hash,canonical) VALUES(31337,$1,2,$2,false)`, bad[:], a[1].Hash[:])
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.db.Exec(ctx, `UPDATE chain_checkpoints SET block_hash=$1 WHERE chain_id=31337`, bad[:])
	if err != nil {
		t.Fatal(err)
	}
	if err := r.watcher.SyncOnce(ctx); err == nil {
		t.Fatal("corrupt checkpoint accepted")
	}
	ready, err := r.watcher.Ready(ctx)
	if err != nil || ready {
		t.Fatal("corrupt checkpoint left ready")
	}
}

func TestSourceRestartGapAndFinality(t *testing.T) {
	r := newForkRig(t, 100)
	a := branch(r.reader.branch, 10, 104)
	r.addLog(t, a[104], 1, 12)
	r.reader.setBranch(a)
	r.sync(t)
	if r.state(t, 1) != "SOURCE_SEEN" {
		t.Fatal("source not seen")
	}
	a = branch(a, 10, 115)
	r.reader.setBranch(a)
	restarted, err := NewWatcher(r.config, r.reader, r.adapter, NewPostgresRepository(r.db))
	if err != nil {
		t.Fatal(err)
	}
	r.watcher = restarted
	r.sync(t)
	if r.state(t, 1) != "SOURCE_FINAL" || r.count(t, "message_observations") != 1 {
		t.Fatal("restart failed to catch up and finalize")
	}
	cp, err := r.repo.Checkpoint(context.Background(), r.sourceID)
	if err != nil || cp.Number != 115 {
		t.Fatal("checkpoint gap")
	}
}

func TestSourceRejectsWrongEndpointAndMalformedEvidence(t *testing.T) {
	t.Run("chain", func(t *testing.T) {
		r := newForkRig(t, 1)
		r.reader.badChain = true
		if err := r.watcher.SyncOnce(context.Background()); err == nil {
			t.Fatal("wrong chain accepted")
		}
	})
	t.Run("anchor", func(t *testing.T) {
		r := newForkRig(t, 1)
		wrong := domain.Hash{99}
		r.config.ExpectedGenesis = &wrong
		watcher, err := NewWatcher(r.config, r.reader, r.adapter, r.repo)
		if err != nil {
			t.Fatal(err)
		}
		if err = watcher.SyncOnce(context.Background()); !errors.Is(err, ErrAnchorMismatch) {
			t.Fatalf("wrong anchor accepted: %v", err)
		}
	})
	t.Run("header", func(t *testing.T) {
		r := newForkRig(t, 1)
		a := branch(r.reader.branch, 10, 1)
		r.reader.setBranch(a)
		r.reader.badHeader = true
		if err := r.watcher.SyncOnce(context.Background()); err == nil {
			t.Fatal("bad header accepted")
		}
	})
	t.Run("log hash", func(t *testing.T) {
		r := newForkRig(t, 1)
		a := branch(r.reader.branch, 10, 1)
		r.addLog(t, a[1], 1, 12)
		r.reader.logs[a[1].Hash][0].BlockHash = common.Hash{99}
		r.reader.setBranch(a)
		if err := r.watcher.SyncOnce(context.Background()); err == nil {
			t.Fatal("wrong log block accepted")
		}
		cp, _ := r.repo.Checkpoint(context.Background(), r.sourceID)
		if cp.Number != 0 {
			t.Fatal("checkpoint advanced through invalid log")
		}
	})
	t.Run("log contract", func(t *testing.T) {
		r := newForkRig(t, 1)
		a := branch(r.reader.branch, 10, 1)
		r.addLog(t, a[1], 1, 12)
		r.reader.logs[a[1].Hash][0].Address = common.Address{99}
		r.reader.setBranch(a)
		if err := r.watcher.SyncOnce(context.Background()); err == nil {
			t.Fatal("wrong source contract accepted")
		}
	})
	t.Run("timeout", func(t *testing.T) {
		r := newForkRig(t, 1)
		a := branch(r.reader.branch, 10, 1)
		r.reader.setBranch(a)
		r.reader.failedLogs = true
		if err := r.watcher.SyncOnce(context.Background()); err == nil {
			t.Fatal("timeout ignored")
		}
		cp, _ := r.repo.Checkpoint(context.Background(), r.sourceID)
		if cp.Number != 0 {
			t.Fatal("checkpoint skipped failed range")
		}
	})
	t.Run("stale head", func(t *testing.T) {
		r := newForkRig(t, 1)
		a := branch(r.reader.branch, 10, 2)
		r.reader.setBranch(a)
		r.sync(t)
		stale := uint64(1)
		r.reader.reportedHead = &stale
		if err := r.watcher.SyncOnce(context.Background()); !errors.Is(err, ErrStaleHead) {
			t.Fatalf("stale endpoint accepted: %v", err)
		}
		ready, err := r.watcher.Ready(context.Background())
		if err != nil || ready {
			t.Fatal("stale endpoint left ready")
		}
	})
}

func TestSourceStreamConfigurationCannotChangeOnRestart(t *testing.T) {
	r := newForkRig(t, 1)
	a := branch(r.reader.branch, 10, 1)
	r.reader.setBranch(a)
	r.sync(t)
	changed := r.config
	changed.Confirmations = 4
	restarted, err := NewWatcher(changed, r.reader, r.adapter, NewPostgresRepository(r.db))
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.SyncOnce(context.Background()); err == nil {
		t.Fatal("unsafe stream configuration change accepted")
	}
}

func TestSourceBatchRollbackAndRestart(t *testing.T) {
	r := newForkRig(t, 1)
	ctx := context.Background()
	if err := r.watcher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	genesis := r.reader.branch[0]
	valid := Header{Number: 1, Hash: domain.Hash{10, 1}, ParentHash: genesis.Hash}
	invalid := Header{Number: 2, Hash: domain.Hash{10, 2}, ParentHash: domain.Hash{99}}
	if err := r.repo.Apply(ctx, r.config, genesis, []BlockEvidence{{Header: valid}, {Header: invalid}}, false); err == nil {
		t.Fatal("invalid batch committed")
	}
	cp, err := r.repo.Checkpoint(ctx, r.sourceID)
	if err != nil || cp.Number != 0 || r.count(t, "chain_blocks") != 1 {
		t.Fatal("failed batch left partial evidence")
	}
	a := branch(r.reader.branch, 10, 3)
	r.addLog(t, a[1], 1, 12)
	r.reader.setBranch(a)
	restarted, err := NewWatcher(r.config, r.reader, r.adapter, NewPostgresRepository(r.db))
	if err != nil {
		t.Fatal(err)
	}
	r.watcher = restarted
	r.sync(t)
	if r.state(t, 1) != "SOURCE_FINAL" {
		t.Fatal("restart did not recover rolled-back range")
	}
}

func TestSourceRestartBetweenBoundedCommitsAndAfterReorg(t *testing.T) {
	r := newForkRig(t, 1)
	r.config.MaxBlocksPerRun = 2
	a := branch(r.reader.branch, 10, 6)
	r.addLog(t, a[2], 1, 12)
	r.reader.setBranch(a)
	ctx := context.Background()
	for expected := uint64(2); expected <= 6; expected += 2 {
		watcher, err := NewWatcher(r.config, r.reader, r.adapter, NewPostgresRepository(r.db))
		if err != nil {
			t.Fatal(err)
		}
		r.watcher = watcher
		if err = watcher.SyncOnce(ctx); err != nil {
			t.Fatal(err)
		}
		cp, err := r.repo.Checkpoint(ctx, r.sourceID)
		if err != nil || cp.Number != expected {
			t.Fatalf("checkpoint after restart=%d want=%d err=%v", cp.Number, expected, err)
		}
	}
	if r.state(t, 1) != "SOURCE_FINAL" {
		t.Fatal("finality did not progress across restarts")
	}
	b := branch(a[:5], 20, 6)
	r.reader.setBranch(b)
	r.sync(t)
	watcher, err := NewWatcher(r.config, r.reader, r.adapter, NewPostgresRepository(r.db))
	if err != nil {
		t.Fatal(err)
	}
	r.watcher = watcher
	r.sync(t)
	if r.state(t, 1) != "SOURCE_FINAL" || r.count(t, "source_reorgs") != 1 {
		t.Fatal("restart after reorg reconciliation did not converge")
	}
}

func TestSourceConcurrentSyncAndShutdown(t *testing.T) {
	r := newForkRig(t, 1)
	a := branch(r.reader.branch, 10, 3)
	r.addLog(t, a[1], 1, 12)
	r.reader.setBranch(a)
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- r.watcher.SyncOnce(context.Background()) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if r.state(t, 1) != "SOURCE_FINAL" || r.count(t, "message_observations") != 1 || r.count(t, "message_transitions") != 1 {
		t.Fatal("concurrent sync duplicated work")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.watcher.Run(ctx, 10*time.Millisecond) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("watcher did not stop")
	}
}

func TestSourceDestinationFirstRemainsIncomplete(t *testing.T) {
	r := newForkRig(t, 1)
	ctx := context.Background()
	data, err := r.adapter.DestinationEvent().Inputs.Pack([32]byte{1}, big.NewInt(31337), common.Address{4}, [32]byte{7}, true)
	if err != nil {
		t.Fatal(err)
	}
	dest := protocol.EventLog{ChainID: r.destID, ParentHash: common.Hash{20}, Log: types.Log{Address: common.Address{2}, Topics: []common.Hash{r.adapter.DestinationEvent().ID}, Data: data, BlockNumber: 1, BlockHash: common.Hash{21}, TxHash: common.Hash{22}}}
	claim, err := r.adapter.DecodeDestination(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.NewPostgres(r.db).IngestDestination(ctx, claim); err != nil {
		t.Fatal(err)
	}
	a := branch(r.reader.branch, 10, 3)
	r.addLog(t, a[1], 1, 12)
	r.reader.setBranch(a)
	r.sync(t)
	if r.state(t, 1) != "SOURCE_FINAL" {
		t.Fatal("source did not finalize")
	}
	var count int
	err = r.db.QueryRow(ctx, "SELECT count(*) FROM cross_chain_messages WHERE state='COMPLETED'").Scan(&count)
	if err != nil || count != 0 {
		t.Fatal("destination evidence completed message")
	}
}
