package destination

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"bridgewatch/internal/domain"
	"bridgewatch/internal/protocol"
	"bridgewatch/internal/rpcpool"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
)

var ErrDeepReorg = errors.New("destination reorg exceeds automatic depth")
var ErrStaleHead = errors.New("destination endpoint head is behind checkpoint")
var ErrWrongChain = errors.New("wrong destination chain endpoint")
var ErrAnchorMismatch = errors.New("destination genesis anchor mismatch")
var ErrNoProvider = errors.New("no eligible destination RPC endpoint")
var ErrProviderDisagreement = errors.New("destination RPC endpoints disagree beyond automatic reorg depth")

type deepCandidate struct {
	checkpoint Checkpoint
	head       Header
	depth      uint64
}

func (e deepCandidate) Error() string { return "destination candidate exceeds automatic reorg depth" }

type Config struct {
	DestinationChainID domain.ChainID
	Contract           common.Address
	StartBlock         uint64
	Confirmations      uint64
	MaxReorgDepth      uint64
	MaxBlocksPerRun    uint64
	ExpectedGenesis    *domain.Hash
}

func (c Config) Validate() error {
	if !c.DestinationChainID.Valid() || c.Contract == (common.Address{}) || c.StartBlock < 1 || c.StartBlock > math.MaxInt64 || c.Confirmations < 1 || c.Confirmations > math.MaxInt64 || c.MaxReorgDepth < 1 || c.MaxReorgDepth > 64 || c.MaxBlocksPerRun < 1 || c.MaxBlocksPerRun > 64 {
		return errors.New("invalid destination watcher configuration")
	}
	return nil
}

type Checkpoint struct {
	Number uint64
	Hash   domain.Hash
	Health string
}

type Status struct {
	ChainID            domain.ChainID
	Head               uint64
	Checkpoint         uint64
	Lag                uint64
	Health             string
	ManualIntervention bool
	Ready              bool
	Confirmations      uint64
	LastSuccessfulAt   *time.Time
	Endpoints          []rpcpool.Status
}

type BlockEvidence struct {
	Header Header
	Claims []domain.DestinationClaim
}

type Repository interface {
	Initialize(context.Context, Config, Header, Header) (Checkpoint, error)
	Checkpoint(context.Context, domain.ChainID) (Checkpoint, error)
	CanonicalHashAt(context.Context, domain.ChainID, uint64) (domain.Hash, error)
	Apply(context.Context, Config, Header, []BlockEvidence, bool) error
	MarkDeepReorg(context.Context, Config, Checkpoint, Header, uint64) error
	SetHealth(context.Context, domain.ChainID, string) error
	Ready(context.Context, domain.ChainID) (bool, error)
}

type DestinationDecoder interface {
	DecodeDestination(context.Context, protocol.EventLog) (domain.DestinationClaim, error)
}

type Watcher struct {
	mu          sync.Mutex
	metrics     *Metrics
	config      Config
	reader      Reader
	pool        *rpcpool.Pool[Reader]
	decoder     DestinationDecoder
	repository  Repository
	ready       atomic.Bool
	head        atomic.Uint64
	checkpoint  atomic.Uint64
	lastSuccess atomic.Int64
}

func NewWatcher(config Config, reader Reader, decoder DestinationDecoder, repository Repository) (*Watcher, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if reader == nil || decoder == nil || repository == nil {
		return nil, errors.New("destination watcher dependencies required")
	}
	return &Watcher{config: config, reader: reader, decoder: decoder, repository: repository}, nil
}

func (w *Watcher) SetMetrics(metrics *Metrics) {
	w.metrics = metrics
	if setter, ok := w.repository.(interface{ SetMetrics(*Metrics) }); ok {
		setter.SetMetrics(metrics)
	}
}

func (w *Watcher) SetPool(pool *rpcpool.Pool[Reader]) { w.mu.Lock(); w.pool = pool; w.mu.Unlock() }

func (w *Watcher) SyncOnce(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pool == nil {
		return w.syncOnce(ctx)
	}
	original := w.reader
	defer func() { w.reader = original }()
	candidates := w.pool.Candidates()
	if len(candidates) == 0 {
		w.ready.Store(false)
		_ = w.repository.SetHealth(ctx, w.config.DestinationChainID, "DEGRADED")
		return ErrNoProvider
	}
	if len(candidates) > 1 {
		var err error
		candidates, err = w.preflightProviders(ctx, candidates)
		if err != nil {
			w.ready.Store(false)
			_ = w.repository.SetHealth(ctx, w.config.DestinationChainID, "DEGRADED")
			return err
		}
	}
	var firstDeep *deepCandidate
	deepCount := 0
	disagree := false
	var last error
	var previousReason string
	for index, candidate := range candidates {
		if index > 0 {
			w.pool.Failover(previousReason)
		}
		w.reader = candidate.Reader
		w.head.Store(0)
		started := time.Now()
		err := w.syncOnce(ctx)
		if err == nil {
			w.pool.Observe(candidate.ID, w.head.Load(), time.Since(started), "HEALTHY")
			return nil
		}
		last = err
		reason := "DEGRADED"
		switch {
		case errors.Is(err, ErrWrongChain):
			reason = "WRONG_CHAIN"
		case errors.Is(err, ErrAnchorMismatch):
			reason = "WRONG_ANCHOR"
		case errors.Is(err, ErrStaleHead):
			reason = "STALE"
		}
		var deep deepCandidate
		if errors.As(err, &deep) {
			reason = "DISAGREEMENT"
			deepCount++
			if firstDeep == nil {
				firstDeep = &deep
			} else if firstDeep.head.Hash != deep.head.Hash {
				disagree = true
			}
		}
		w.pool.Observe(candidate.ID, w.head.Load(), time.Since(started), reason)
		previousReason = reason
	}
	if deepCount == len(candidates) {
		if disagree {
			return ErrProviderDisagreement
		}
		if err := w.repository.MarkDeepReorg(ctx, w.config, firstDeep.checkpoint, firstDeep.head, firstDeep.depth); err != nil {
			return err
		}
		return ErrDeepReorg
	}
	return last
}

// preflightProviders compares validated headers before a provider can mutate
// canonical state. Disagreement blocks the sync without selecting a majority.
func (w *Watcher) preflightProviders(ctx context.Context, candidates []rpcpool.Endpoint[Reader]) ([]rpcpool.Endpoint[Reader], error) {
	checkpoint, checkpointErr := w.repository.Checkpoint(ctx, w.config.DestinationChainID)
	if checkpointErr != nil && !errors.Is(checkpointErr, pgx.ErrNoRows) {
		return nil, checkpointErr
	}
	type valid struct {
		endpoint rpcpool.Endpoint[Reader]
		head     uint64
	}
	validProviders := make([]valid, 0, len(candidates))
	var last error
	for _, candidate := range candidates {
		w.reader = candidate.Reader
		started := time.Now()
		result := "DEGRADED"
		chain, err := w.reader.ChainID(ctx)
		if err == nil && chain != w.config.DestinationChainID {
			err = ErrWrongChain
			result = "WRONG_CHAIN"
		}
		var genesis Header
		if err == nil {
			genesis, err = w.checkedHeader(ctx, 0)
		}
		if err == nil && genesis.Hash != w.pool.ExpectedAnchor() {
			err = ErrAnchorMismatch
			result = "WRONG_ANCHOR"
		}
		if err == nil {
			var logs []types.Log
			logs, err = w.reader.FilterLogs(ctx, 0, 0, w.config.Contract)
			if err == nil && len(logs) != 0 {
				err = errors.New("genesis returned unexpected logs")
			}
		}
		var head uint64
		if err == nil {
			head, err = w.reader.BlockNumber(ctx)
		}
		if err == nil && checkpointErr == nil && head < checkpoint.Number {
			err = ErrStaleHead
			result = "STALE"
		}
		if err == nil {
			_, err = w.checkedHeader(ctx, head)
		}
		if err != nil {
			last = err
			w.pool.Observe(candidate.ID, head, time.Since(started), result)
			continue
		}
		w.pool.Observe(candidate.ID, head, time.Since(started), "HEALTHY")
		validProviders = append(validProviders, valid{candidate, head})
	}
	if len(validProviders) == 0 {
		if last != nil {
			return nil, last
		}
		return nil, ErrNoProvider
	}
	if len(validProviders) > 1 {
		shared := validProviders[0].head
		for _, provider := range validProviders[1:] {
			if provider.head < shared {
				shared = provider.head
			}
		}
		var first Header
		for i, provider := range validProviders {
			w.reader = provider.endpoint.Reader
			header, err := w.checkedHeader(ctx, shared)
			if err != nil {
				w.pool.Observe(provider.endpoint.ID, provider.head, 0, "DEGRADED")
				return nil, err
			}
			if i == 0 {
				first = header
			} else if header != first {
				for _, item := range validProviders {
					w.pool.Observe(item.endpoint.ID, item.head, 0, "DISAGREEMENT")
				}
				return nil, ErrProviderDisagreement
			}
		}
	}
	out := make([]rpcpool.Endpoint[Reader], len(validProviders))
	for i, item := range validProviders {
		out[i] = item.endpoint
	}
	return out, nil
}

func (w *Watcher) syncOnce(ctx context.Context) (retErr error) {
	w.ready.Store(false)
	defer func() {
		if retErr != nil && !errors.Is(retErr, ErrDeepReorg) && ctx.Err() == nil {
			_ = w.repository.SetHealth(ctx, w.config.DestinationChainID, "DEGRADED")
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	chain, err := w.reader.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("destination chain ID: %w", err)
	}
	if chain != w.config.DestinationChainID {
		_ = w.repository.SetHealth(ctx, w.config.DestinationChainID, "DEGRADED")
		return ErrWrongChain
	}
	genesis, err := w.checkedHeader(ctx, 0)
	if err != nil {
		return fmt.Errorf("destination genesis: %w", err)
	}
	if w.config.ExpectedGenesis != nil && genesis.Hash != *w.config.ExpectedGenesis {
		_ = w.repository.SetHealth(ctx, w.config.DestinationChainID, "DEGRADED")
		return ErrAnchorMismatch
	}
	probe, err := w.reader.FilterLogs(ctx, 0, 0, w.config.Contract)
	if err != nil {
		return fmt.Errorf("destination log method unavailable: %w", err)
	}
	if len(probe) != 0 {
		return errors.New("destination genesis returned unexpected logs")
	}
	predecessor, err := w.checkedHeader(ctx, w.config.StartBlock-1)
	if err != nil {
		return fmt.Errorf("destination start predecessor: %w", err)
	}
	cp, err := w.repository.Initialize(ctx, w.config, genesis, predecessor)
	if err != nil {
		return err
	}
	w.checkpoint.Store(cp.Number)
	if cp.Health == "MANUAL_INTERVENTION" {
		return ErrDeepReorg
	}
	localHead, err := w.repository.CanonicalHashAt(ctx, w.config.DestinationChainID, cp.Number)
	if err != nil || localHead != cp.Hash {
		return errors.New("destination checkpoint lacks canonical block evidence")
	}
	headNumber, err := w.reader.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("destination head: %w", err)
	}
	if headNumber > math.MaxInt64 {
		return errors.New("destination head exceeds database block range")
	}
	w.head.Store(headNumber)
	w.metrics.SetHead(headNumber)
	if headNumber >= cp.Number {
		w.metrics.SetLag(headNumber - cp.Number)
	}
	if headNumber < cp.Number {
		return ErrStaleHead
	}
	latest, err := w.checkedHeader(ctx, headNumber)
	if err != nil {
		return err
	}
	current, err := w.checkedHeader(ctx, cp.Number)
	if err != nil {
		return err
	}
	ancestor := current
	reorg := current.Hash != cp.Hash
	if reorg {
		var remoteAncestor Header
		ancestorNumber, found, searchErr := boundedCommonAncestor(cp.Number, w.config.StartBlock-1, w.config.MaxReorgDepth, func(n uint64) (bool, error) {
			remote, err := w.checkedHeader(ctx, n)
			if err != nil {
				return false, err
			}
			local, err := w.repository.CanonicalHashAt(ctx, w.config.DestinationChainID, n)
			if err != nil {
				return false, err
			}
			if remote.Hash == local {
				remoteAncestor = remote
				return true, nil
			}
			return false, nil
		})
		if searchErr != nil {
			return searchErr
		}
		if !found {
			if w.pool != nil {
				return deepCandidate{checkpoint: cp, head: latest, depth: w.config.MaxReorgDepth + 1}
			}
			if err := w.repository.MarkDeepReorg(ctx, w.config, cp, latest, w.config.MaxReorgDepth+1); err != nil {
				return err
			}
			return ErrDeepReorg
		}
		if remoteAncestor.Number != ancestorNumber {
			return errors.New("destination common ancestor search was inconsistent")
		}
		ancestor = remoteAncestor
	}
	from := cp.Number + 1
	if reorg {
		from = ancestor.Number + 1
	}
	if from > headNumber {
		if err := w.repository.SetHealth(ctx, w.config.DestinationChainID, "HEALTHY"); err != nil {
			return err
		}
		w.ready.Store(true)
		w.lastSuccess.Store(time.Now().UTC().UnixNano())
		return nil
	}
	to, err := boundedEnd(from, headNumber, w.config.MaxBlocksPerRun)
	if err != nil {
		return err
	}
	blocks := make([]BlockEvidence, 0, to-from+1)
	parent := ancestor.Hash
	for number := from; number <= to; number++ {
		block, err := w.fetchBlock(ctx, number)
		if err != nil {
			return err
		}
		if block.Header.ParentHash != parent {
			return fmt.Errorf("destination ancestry mismatch at block %d", number)
		}
		parent = block.Header.Hash
		blocks = append(blocks, block)
	}
	if err := w.repository.Apply(ctx, w.config, ancestor, blocks, reorg); err != nil {
		return err
	}
	committed := blocks[len(blocks)-1].Header.Number
	w.checkpoint.Store(committed)
	w.metrics.SetLag(headNumber - committed)
	if reorg {
		w.metrics.IncReorg()
	}
	w.ready.Store(committed == headNumber)
	if committed == headNumber {
		if err := w.repository.SetHealth(ctx, w.config.DestinationChainID, "HEALTHY"); err != nil {
			w.ready.Store(false)
			return err
		}
		w.lastSuccess.Store(time.Now().UTC().UnixNano())
	}
	return nil
}

func boundedEnd(from, head, maximum uint64) (uint64, error) {
	if maximum == 0 || from > head {
		return 0, errors.New("invalid destination range")
	}
	if maximum-1 <= head-from {
		return from + maximum - 1, nil
	}
	return head, nil
}

func boundedCommonAncestor(tip, floor, maximum uint64, same func(uint64) (bool, error)) (uint64, bool, error) {
	if maximum == 0 || tip < floor || same == nil {
		return 0, false, errors.New("invalid destination common ancestor search")
	}
	limit := maximum
	if distance := tip - floor; distance < limit {
		limit = distance
	}
	for depth := uint64(1); depth <= limit; depth++ {
		number := tip - depth
		equal, err := same(number)
		if err != nil {
			return 0, false, err
		}
		if equal {
			return number, true, nil
		}
	}
	return 0, false, nil
}

func (w *Watcher) checkedHeader(ctx context.Context, number uint64) (Header, error) {
	header, err := w.reader.HeaderByNumber(ctx, number)
	if err != nil {
		return Header{}, err
	}
	if header.Number != number || header.Hash == (domain.Hash{}) || (number > 0 && header.ParentHash == (domain.Hash{})) {
		return Header{}, errors.New("malformed or mismatched destination header")
	}
	byHash, err := w.reader.HeaderByHash(ctx, header.Hash)
	if err != nil {
		return Header{}, err
	}
	if byHash != header {
		return Header{}, errors.New("inconsistent destination header by hash")
	}
	return header, nil
}

func (w *Watcher) fetchBlock(ctx context.Context, number uint64) (BlockEvidence, error) {
	header, err := w.checkedHeader(ctx, number)
	if err != nil {
		return BlockEvidence{}, err
	}
	logs, err := w.reader.FilterLogs(ctx, number, number, w.config.Contract)
	if err != nil {
		return BlockEvidence{}, err
	}
	claims := make([]domain.DestinationClaim, 0, len(logs))
	for _, log := range logs {
		if log.BlockNumber != number || domain.Hash(log.BlockHash) != header.Hash || log.TxHash == (common.Hash{}) || log.Address != w.config.Contract || log.Removed {
			return BlockEvidence{}, errors.New("destination log disagrees with fetched block")
		}
		claim, err := w.decoder.DecodeDestination(ctx, protocol.EventLog{ChainID: w.config.DestinationChainID, ParentHash: common.Hash(header.ParentHash), Log: log})
		if err != nil {
			return BlockEvidence{}, fmt.Errorf("decode destination block %d: %w", number, err)
		}
		claims = append(claims, claim)
	}
	return BlockEvidence{Header: header, Claims: claims}, nil
}

func (w *Watcher) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("poll interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if err := w.SyncOnce(ctx); errors.Is(err, ErrWrongChain) || errors.Is(err, ErrAnchorMismatch) || errors.Is(err, ErrDeepReorg) {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (w *Watcher) Ready(ctx context.Context) (bool, error) {
	if !w.ready.Load() {
		return false, nil
	}
	return w.repository.Ready(ctx, w.config.DestinationChainID)
}

func (w *Watcher) Status(ctx context.Context) (Status, error) {
	cp, err := w.repository.Checkpoint(ctx, w.config.DestinationChainID)
	if err != nil {
		return Status{ChainID: w.config.DestinationChainID}, err
	}
	head := w.head.Load()
	lag := uint64(0)
	if head >= cp.Number {
		lag = head - cp.Number
	}
	ready, err := w.Ready(ctx)
	if err != nil {
		return Status{ChainID: w.config.DestinationChainID}, err
	}
	status := Status{ChainID: w.config.DestinationChainID, Head: head, Checkpoint: cp.Number, Lag: lag, Health: cp.Health, ManualIntervention: cp.Health == "MANUAL_INTERVENTION", Ready: ready, Confirmations: w.config.Confirmations}
	if w.pool != nil {
		status.Endpoints = w.pool.Status()
	}
	if stamp := w.lastSuccess.Load(); stamp > 0 {
		instant := time.Unix(0, stamp).UTC()
		status.LastSuccessfulAt = &instant
	}
	return status, nil
}
