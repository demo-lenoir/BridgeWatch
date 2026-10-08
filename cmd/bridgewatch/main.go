package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"bridgewatch/internal/api"
	"bridgewatch/internal/clock"
	"bridgewatch/internal/destination"
	"bridgewatch/internal/domain"
	"bridgewatch/internal/operations"
	"bridgewatch/internal/protocol"
	"bridgewatch/internal/rpcpool"
	"bridgewatch/internal/source"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type config struct {
	database, address, webhook                                                               string
	sourceRPCs, destinationRPCs                                                              []endpointConfig
	sourceID, destinationID                                                                  domain.ChainID
	sourceContract, destinationContract                                                      common.Address
	sourceGenesis, destinationGenesis                                                        domain.Hash
	sourceStart, destinationStart, sourceConfirmations, destinationConfirmations, reorgDepth uint64
	policyVersion                                                                            int
	sla                                                                                      time.Duration
}
type endpointConfig struct{ id, url string }

func endpoints(multiple, single, alias string) ([]endpointConfig, error) {
	raw := os.Getenv(multiple)
	if raw == "" {
		value, err := required(single)
		if err != nil {
			return nil, err
		}
		return []endpointConfig{{id: alias, url: value}}, nil
	}
	parts := strings.Split(raw, ",")
	if len(parts) < 1 || len(parts) > 8 {
		return nil, fmt.Errorf("invalid %s endpoint count", multiple)
	}
	seen := map[string]bool{}
	out := make([]endpointConfig, 0, len(parts))
	for _, part := range parts {
		pair := strings.SplitN(part, "=", 2)
		if len(pair) != 2 || pair[0] == "" || pair[1] == "" || seen[pair[0]] {
			return nil, fmt.Errorf("invalid %s endpoint identity", multiple)
		}
		seen[pair[0]] = true
		out = append(out, endpointConfig{id: pair[0], url: pair[1]})
	}
	return out, nil
}

func required(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}
func unsigned(name string) (uint64, error) {
	value, err := required(name)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(value, 10, 64)
}
func chainID(name string) (domain.ChainID, error) {
	value, err := required(name)
	if err != nil {
		return domain.ChainID{}, err
	}
	return domain.ChainIDFromDecimal(value)
}
func address(name string) (common.Address, error) {
	value, err := required(name)
	if err != nil {
		return common.Address{}, err
	}
	if !common.IsHexAddress(value) {
		return common.Address{}, fmt.Errorf("invalid %s", name)
	}
	return common.HexToAddress(value), nil
}
func genesis(name string) (domain.Hash, error) {
	value, err := required(name)
	if err != nil {
		return domain.Hash{}, err
	}
	if len(value) != 66 || value[:2] != "0x" {
		return domain.Hash{}, fmt.Errorf("invalid %s", name)
	}
	if _, err := hex.DecodeString(value[2:]); err != nil {
		return domain.Hash{}, fmt.Errorf("invalid %s", name)
	}
	parsed := common.HexToHash(value)
	if parsed == (common.Hash{}) {
		return domain.Hash{}, fmt.Errorf("invalid %s", name)
	}
	return domain.Hash(parsed), nil
}

func readConfig() (config, error) {
	var c config
	var err error
	if c.database, err = required("BRIDGEWATCH_DATABASE_URL"); err != nil {
		return c, err
	}
	if c.sourceRPCs, err = endpoints("BRIDGEWATCH_SOURCE_RPC_ENDPOINTS", "BRIDGEWATCH_SOURCE_RPC_URL", "source_rpc"); err != nil {
		return c, err
	}
	if c.destinationRPCs, err = endpoints("BRIDGEWATCH_DESTINATION_RPC_ENDPOINTS", "BRIDGEWATCH_DESTINATION_RPC_URL", "destination_rpc"); err != nil {
		return c, err
	}
	if c.sourceID, err = chainID("BRIDGEWATCH_SOURCE_CHAIN_ID"); err != nil {
		return c, err
	}
	if c.destinationID, err = chainID("BRIDGEWATCH_DESTINATION_CHAIN_ID"); err != nil {
		return c, err
	}
	if c.sourceContract, err = address("BRIDGEWATCH_SOURCE_CONTRACT"); err != nil {
		return c, err
	}
	if c.destinationContract, err = address("BRIDGEWATCH_DESTINATION_CONTRACT"); err != nil {
		return c, err
	}
	if c.sourceGenesis, err = genesis("BRIDGEWATCH_SOURCE_GENESIS"); err != nil {
		return c, err
	}
	if c.destinationGenesis, err = genesis("BRIDGEWATCH_DESTINATION_GENESIS"); err != nil {
		return c, err
	}
	if c.sourceStart, err = unsigned("BRIDGEWATCH_SOURCE_START_BLOCK"); err != nil {
		return c, err
	}
	if c.destinationStart, err = unsigned("BRIDGEWATCH_DESTINATION_START_BLOCK"); err != nil {
		return c, err
	}
	if c.sourceConfirmations, err = unsigned("BRIDGEWATCH_SOURCE_CONFIRMATIONS"); err != nil {
		return c, err
	}
	if c.destinationConfirmations, err = unsigned("BRIDGEWATCH_DESTINATION_CONFIRMATIONS"); err != nil {
		return c, err
	}
	if c.reorgDepth, err = unsigned("BRIDGEWATCH_MAX_REORG_DEPTH"); err != nil {
		return c, err
	}
	seconds, err := unsigned("BRIDGEWATCH_RELAY_SLA_SECONDS")
	if err != nil || seconds == 0 || seconds > 30*24*3600 {
		return c, errors.New("invalid BRIDGEWATCH_RELAY_SLA_SECONDS")
	}
	c.sla = time.Duration(seconds) * time.Second
	version, err := unsigned("BRIDGEWATCH_RELAY_POLICY_VERSION")
	if err != nil || version == 0 || version > 1<<31-1 {
		return c, errors.New("invalid BRIDGEWATCH_RELAY_POLICY_VERSION")
	}
	c.policyVersion = int(version)
	c.address = os.Getenv("BRIDGEWATCH_LISTEN_ADDR")
	if c.address == "" {
		c.address = "127.0.0.1:8080"
	}
	c.webhook = os.Getenv("BRIDGEWATCH_ALERT_WEBHOOK_URL")
	return c, nil
}

func run(ctx context.Context, c config, logger *slog.Logger) error {
	db, err := pgxpool.New(ctx, c.database)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.Ping(ctx); err != nil {
		return err
	}
	var sourceReaders []*source.HTTPReader
	var destinationReaders []*destination.HTTPReader
	defer func() {
		for _, reader := range sourceReaders {
			reader.Close()
		}
		for _, reader := range destinationReaders {
			reader.Close()
		}
	}()
	for _, endpoint := range c.sourceRPCs {
		reader, dialErr := source.DialHTTP(ctx, endpoint.url)
		if dialErr != nil {
			return fmt.Errorf("source endpoint %s: %w", endpoint.id, dialErr)
		}
		sourceReaders = append(sourceReaders, reader)
	}
	for _, endpoint := range c.destinationRPCs {
		reader, dialErr := destination.DialHTTP(ctx, endpoint.url)
		if dialErr != nil {
			return fmt.Errorf("destination endpoint %s: %w", endpoint.id, dialErr)
		}
		destinationReaders = append(destinationReaders, reader)
	}
	adapter, err := protocol.NewMockBridgeAdapter(protocol.MockConfig{SourceChainID: c.sourceID, DestinationChainID: c.destinationID, SourceContract: c.sourceContract, DestinationContract: c.destinationContract, Version: 1, RelaySLA: c.sla})
	if err != nil {
		return err
	}
	sourceRepo := source.NewPostgresRepository(db)
	destinationRepo := destination.NewPostgresRepository(db)
	businessClock := clock.Real{}
	sourceRepo.SetClock(businessClock)
	destinationRepo.SetClock(businessClock)
	sourceWatcher, err := source.NewWatcher(source.Config{SourceChainID: c.sourceID, Contract: c.sourceContract, StartBlock: c.sourceStart, Confirmations: c.sourceConfirmations, MaxReorgDepth: c.reorgDepth, MaxBlocksPerRun: 8, ExpectedGenesis: &c.sourceGenesis}, sourceReaders[0], adapter, sourceRepo)
	if err != nil {
		return err
	}
	destinationWatcher, err := destination.NewWatcher(destination.Config{DestinationChainID: c.destinationID, Contract: c.destinationContract, StartBlock: c.destinationStart, Confirmations: c.destinationConfirmations, MaxReorgDepth: c.reorgDepth, MaxBlocksPerRun: 8, ExpectedGenesis: &c.destinationGenesis}, destinationReaders[0], adapter, destinationRepo)
	if err != nil {
		return err
	}
	registry := prometheus.NewRegistry()
	sourceMetrics, err := source.NewMetrics(registry, c.sourceID, "source_rpc")
	if err != nil {
		return err
	}
	destinationMetrics, err := destination.NewMetrics(registry, c.destinationID, "destination_rpc")
	if err != nil {
		return err
	}
	operationalMetrics, err := operations.NewMetrics(registry)
	if err != nil {
		return err
	}
	sourceWatcher.SetMetrics(sourceMetrics)
	destinationWatcher.SetMetrics(destinationMetrics)
	sourcePoolMetrics, err := rpcpool.NewMetrics(registry, c.sourceID)
	if err != nil {
		return err
	}
	destinationPoolMetrics, err := rpcpool.NewMetrics(registry, c.destinationID)
	if err != nil {
		return err
	}
	sourceEntries := make([]rpcpool.Endpoint[source.Reader], 0, len(sourceReaders))
	for i, reader := range sourceReaders {
		metricView, viewErr := sourceMetrics.ForEndpoint(c.sourceRPCs[i].id)
		if viewErr != nil {
			return viewErr
		}
		sourceEntries = append(sourceEntries, rpcpool.Endpoint[source.Reader]{ID: c.sourceRPCs[i].id, Reader: source.WithMetrics(reader, metricView)})
	}
	destinationEntries := make([]rpcpool.Endpoint[destination.Reader], 0, len(destinationReaders))
	for i, reader := range destinationReaders {
		metricView, viewErr := destinationMetrics.ForEndpoint(c.destinationRPCs[i].id)
		if viewErr != nil {
			return viewErr
		}
		destinationEntries = append(destinationEntries, rpcpool.Endpoint[destination.Reader]{ID: c.destinationRPCs[i].id, Reader: destination.WithMetrics(reader, metricView)})
	}
	sourcePool, err := rpcpool.New(c.sourceID, c.sourceGenesis, sourceEntries, businessClock, 5*time.Second)
	if err != nil {
		return err
	}
	destinationPool, err := rpcpool.New(c.destinationID, c.destinationGenesis, destinationEntries, businessClock, 5*time.Second)
	if err != nil {
		return err
	}
	sourcePool.SetMetrics(sourcePoolMetrics)
	destinationPool.SetMetrics(destinationPoolMetrics)
	sourceWatcher.SetPool(sourcePool)
	destinationWatcher.SetPool(destinationPool)
	service, err := operations.NewService(db, businessClock, sourceWatcher, destinationWatcher, logger)
	if err != nil {
		return err
	}
	service.SetMetrics(operationalMetrics)
	if err = service.ConfigurePolicy(ctx, operations.Policy{Protocol: "mockbridge-v1", SourceChainID: c.sourceID, DestinationChainID: c.destinationID, Version: c.policyVersion, SLA: c.sla}); err != nil {
		return err
	}
	server, err := api.New(db, businessClock, sourceWatcher, destinationWatcher, registry, operationalMetrics)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", c.address)
	if err != nil {
		return err
	}
	defer listener.Close()
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, 5)
	start := func(name string, work func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if runErr := work(); runErr != nil && child.Err() == nil {
				errCh <- fmt.Errorf("%s: %w", name, runErr)
			}
		}()
	}
	start("source watcher", func() error { return sourceWatcher.Run(child, time.Second) })
	start("destination watcher", func() error { return destinationWatcher.Run(child, time.Second) })
	start("relay classifier", func() error { return service.Run(child, time.Second, 100) })
	if c.webhook != "" {
		sink, sinkErr := operations.NewWebhookSink(c.webhook)
		if sinkErr != nil {
			cancel()
			wg.Wait()
			return sinkErr
		}
		worker, workerErr := operations.NewAlertWorker(db, businessClock, sink, operations.DeliveryConfig{MaxAttempts: 5, Lease: 30 * time.Second, BaseBackoff: time.Second}, logger)
		if workerErr != nil {
			cancel()
			wg.Wait()
			return workerErr
		}
		worker.SetMetrics(operationalMetrics)
		start("alert delivery", func() error { return worker.Run(child, time.Second, 25) })
	} else {
		logger.Warn("alert webhook is not configured; outbox remains pending")
	}
	start("HTTP API", func() error { return server.Serve(child, listener) })
	logger.Info("BridgeWatch started", "listen", listener.Addr().String(), "source_chain", c.sourceID.String(), "destination_chain", c.destinationID.String())
	var result error
	select {
	case <-ctx.Done():
	case result = <-errCh:
	}
	cancel()
	wg.Wait()
	return result
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if len(os.Args) > 1 && os.Args[1] == "reconcile-chain" {
		if err := reconcileChain(context.Background(), os.Args[2:]); err != nil {
			logger.Error("reconciliation failed", "error", err)
			os.Exit(1)
		}
		return
	}
	c, err := readConfig()
	if err == nil {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		err = run(ctx, c, logger)
	}
	if err != nil {
		logger.Error("BridgeWatch stopped", "error", err)
		os.Exit(1)
	}
}
