package destination_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"bridgewatch/internal/api"
	"bridgewatch/internal/destination"
	"bridgewatch/internal/domain"
	"bridgewatch/internal/operations"
	"bridgewatch/internal/protocol"
	"bridgewatch/internal/rpcpool"
	"bridgewatch/internal/source"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type anvilArtifact struct {
	ABI      json.RawMessage `json:"abi"`
	Bytecode struct {
		Object string `json:"object"`
	} `json:"bytecode"`
}
type localChain struct {
	url     string
	rpc     *rpc.Client
	eth     *ethclient.Client
	account common.Address
	genesis common.Hash
}
type interruptedSourceReader struct {
	source.Reader
	unavailable bool
}

func (r *interruptedSourceReader) BlockNumber(ctx context.Context) (uint64, error) {
	if r.unavailable {
		return 0, errors.New("injected source RPC outage")
	}
	return r.Reader.BlockNumber(ctx)
}

type controlledClock struct {
	mu  sync.RWMutex
	now time.Time
}

func (c *controlledClock) Now() time.Time      { c.mu.RLock(); defer c.mu.RUnlock(); return c.now }
func (c *controlledClock) Set(value time.Time) { c.mu.Lock(); c.now = value; c.mu.Unlock() }

func startLocalChain(t *testing.T, bin string, id, timestamp int) *localChain {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, bin, "--chain-id", strconv.Itoa(id), "--host", "127.0.0.1", "--port", strconv.Itoa(port), "--timestamp", strconv.Itoa(timestamp), "--silent")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err = cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); _ = cmd.Wait() })
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	var client *rpc.Client
	for i := 0; i < 100; i++ {
		candidate, dialErr := rpc.DialContext(ctx, url)
		if dialErr == nil {
			var chainID string
			dialErr = candidate.CallContext(ctx, &chainID, "eth_chainId")
			if dialErr == nil {
				client = candidate
				break
			}
			candidate.Close()
		}
		time.Sleep(25 * time.Millisecond)
	}
	if client == nil {
		t.Fatal("Anvil not ready")
	}
	t.Cleanup(client.Close)
	eth := ethclient.NewClient(client)
	var accounts []common.Address
	if err = client.CallContext(ctx, &accounts, "eth_accounts"); err != nil || len(accounts) == 0 {
		t.Fatalf("Anvil account: %v", err)
	}
	genesis, err := eth.HeaderByNumber(ctx, big.NewInt(0))
	if err != nil {
		t.Fatal(err)
	}
	return &localChain{url: url, rpc: client, eth: eth, account: accounts[0], genesis: genesis.Hash()}
}
func loadArtifact(t *testing.T, name string) (anvilArtifact, abi.ABI) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "out", name+".sol", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var artifact anvilArtifact
	if err = json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	parsed, err := abi.JSON(bytes.NewReader(artifact.ABI))
	if err != nil {
		t.Fatal(err)
	}
	return artifact, parsed
}
func localTx(t *testing.T, chain *localChain, to *common.Address, data []byte) *types.Receipt {
	t.Helper()
	ctx := context.Background()
	args := map[string]any{"from": chain.account, "data": "0x" + common.Bytes2Hex(data), "gas": "0x300000"}
	if to != nil {
		args["to"] = *to
	}
	var hash common.Hash
	if err := chain.rpc.CallContext(ctx, &hash, "eth_sendTransaction", args); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		receipt, err := chain.eth.TransactionReceipt(ctx, hash)
		if err == nil {
			if receipt.Status != 1 {
				t.Fatal("local transaction reverted")
			}
			return receipt
		}
		if !errors.Is(err, ethereum.NotFound) {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("receipt timeout")
	return nil
}
func mine(t *testing.T, chain *localChain, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		var result any
		if err := chain.rpc.CallContext(context.Background(), &result, "evm_mine"); err != nil {
			t.Fatal(err)
		}
	}
}
func assertMessage(t *testing.T, db *pgxpool.Pool, id domain.Hash, want string) {
	t.Helper()
	var got string
	if err := db.QueryRow(context.Background(), `SELECT state FROM cross_chain_messages WHERE message_id=$1`, id[:]).Scan(&got); err != nil || got != want {
		t.Fatalf("message %x: got %q want %q: %v", id[:4], got, want, err)
	}
}
func predictedMessageID(t *testing.T, from common.Address, recipient common.Address, payload [32]byte, sequence int64) domain.Hash {
	t.Helper()
	uintType, err := abi.NewType("uint256", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	addressType, err := abi.NewType("address", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	bytesType, err := abi.NewType("bytes32", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	args := abi.Arguments{{Type: uintType}, {Type: uintType}, {Type: addressType}, {Type: addressType}, {Type: bytesType}, {Type: uintType}}
	packed, err := args.Pack(big.NewInt(31337), big.NewInt(31338), from, recipient, payload, big.NewInt(sequence))
	if err != nil {
		t.Fatal(err)
	}
	return domain.Hash(crypto.Keccak256Hash(packed))
}

func TestRealTwoAnvilCompletionAndDestinationFirst(t *testing.T) { runTwoAnvilScenario(t, false) }
func TestFinalDemo(t *testing.T)                                 { runTwoAnvilScenario(t, true) }

func snapshot(t *testing.T, chain *localChain) string {
	t.Helper()
	var id string
	if err := chain.rpc.CallContext(context.Background(), &id, "evm_snapshot"); err != nil {
		t.Fatal(err)
	}
	return id
}
func revert(t *testing.T, chain *localChain, id string) {
	t.Helper()
	var restored bool
	if err := chain.rpc.CallContext(context.Background(), &restored, "evm_revert", id); err != nil || !restored {
		t.Fatal("snapshot revert", err)
	}
}
func completedSnapshotHead(t *testing.T, chain *localChain) uint64 {
	t.Helper()
	head, err := chain.eth.BlockNumber(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return head
}
func messageState(t *testing.T, db *pgxpool.Pool, id domain.Hash) string {
	t.Helper()
	var state string
	if err := db.QueryRow(context.Background(), `SELECT state FROM cross_chain_messages WHERE message_id=$1`, id[:]).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func runTwoAnvilScenario(t *testing.T, final bool) {
	started := time.Now()
	bin := os.Getenv("BRIDGEWATCH_ANVIL_BIN")
	url := os.Getenv("BRIDGEWATCH_TEST_DESTINATION_DB_URL")
	if bin == "" || url == "" {
		t.Skip("Anvil or destination database not configured")
	}
	ctx := context.Background()
	sourceChain := startLocalChain(t, bin, 31337, 1)
	destChain := startLocalChain(t, bin, 31338, 2)
	sourceArtifact, sourceABI := loadArtifact(t, "MockBridgeSource")
	destArtifact, destABI := loadArtifact(t, "MockBridgeDestination")
	sourceDeployment := localTx(t, sourceChain, nil, common.FromHex(sourceArtifact.Bytecode.Object))
	destDeployment := localTx(t, destChain, nil, common.FromHex(destArtifact.Bytecode.Object))
	sourceContract, destContract := sourceDeployment.ContractAddress, destDeployment.ContractAddress
	var sourceSnapshot, destSnapshot string
	if final {
		sourceSnapshot = snapshot(t, sourceChain)
		destSnapshot = snapshot(t, destChain)
	}
	if sourceContract == (common.Address{}) || destContract == (common.Address{}) {
		t.Fatal("missing fixture contract")
	}
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(ctx, `TRUNCATE destination_reorgs,destination_streams,source_reorgs,source_streams,message_alerts,message_transitions,message_anomalies,observation_conflicts,message_claims,message_observations,cross_chain_messages,chain_checkpoints,chain_blocks RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	sourceID, _ := domain.NewChainID(big.NewInt(31337))
	destID, _ := domain.NewChainID(big.NewInt(31338))
	adapter, err := protocol.NewMockBridgeAdapter(protocol.MockConfig{SourceChainID: sourceID, DestinationChainID: destID, SourceContract: sourceContract, DestinationContract: destContract, Version: 1, RelaySLA: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	sourceReader, err := source.DialHTTP(ctx, sourceChain.url)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceReader.Close()
	destReader, err := destination.DialHTTP(ctx, destChain.url)
	if err != nil {
		t.Fatal(err)
	}
	defer destReader.Close()
	sourceAnchor, destAnchor := domain.Hash(sourceChain.genesis), domain.Hash(destChain.genesis)
	businessClock := &controlledClock{now: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)}
	sourceRepo := source.NewPostgresRepository(db)
	destRepo := destination.NewPostgresRepository(db)
	sourceRepo.SetClock(businessClock)
	destRepo.SetClock(businessClock)
	sourceWatcher, err := source.NewWatcher(source.Config{SourceChainID: sourceID, Contract: sourceContract, StartBlock: sourceDeployment.BlockNumber.Uint64(), Confirmations: 3, MaxReorgDepth: 8, MaxBlocksPerRun: 8, ExpectedGenesis: &sourceAnchor}, sourceReader, adapter, sourceRepo)
	if err != nil {
		t.Fatal(err)
	}
	destWatcher, err := destination.NewWatcher(destination.Config{DestinationChainID: destID, Contract: destContract, StartBlock: destDeployment.BlockNumber.Uint64(), Confirmations: 3, MaxReorgDepth: 8, MaxBlocksPerRun: 8, ExpectedGenesis: &destAnchor}, destReader, adapter, destRepo)
	if err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	operationalMetrics, err := operations.NewMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	sourceMetrics, err := source.NewMetrics(registry, sourceID, "source_local")
	if err != nil {
		t.Fatal(err)
	}
	destMetrics, err := destination.NewMetrics(registry, destID, "destination_local")
	if err != nil {
		t.Fatal(err)
	}
	sourceWatcher.SetMetrics(sourceMetrics)
	destWatcher.SetMetrics(destMetrics)
	apiServer, err := api.New(db, businessClock, sourceWatcher, destWatcher, registry, operationalMetrics)
	if err != nil {
		t.Fatal(err)
	}
	read := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRecorder()
		apiServer.Handler().ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
		return r
	}
	service, err := operations.NewService(db, businessClock, sourceWatcher, destWatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	service.SetMetrics(operationalMetrics)
	if err = service.ConfigurePolicy(ctx, operations.Policy{Protocol: "mockbridge-v1", SourceChainID: sourceID, DestinationChainID: destID, Version: 1, SLA: 30 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if err = destWatcher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	firstPayload := [32]byte{7}
	firstRecipient := common.Address{4}
	call, err := sourceABI.Pack("send", big.NewInt(31338), firstRecipient, firstPayload)
	if err != nil {
		t.Fatal(err)
	}
	sendReceipt := localTx(t, sourceChain, &sourceContract, call)
	if len(sendReceipt.Logs) != 1 {
		t.Fatal("source event missing")
	}
	firstID := domain.Hash(sendReceipt.Logs[0].Data[:32])
	if err = sourceWatcher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertMessage(t, db, firstID, "SOURCE_SEEN")
	mine(t, sourceChain, 2)
	if err = sourceWatcher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertMessage(t, db, firstID, "SOURCE_FINAL")
	if count, err := service.ActivateEligible(ctx, 10); err != nil || count != 1 {
		t.Fatalf("relay pending: %d %v", count, err)
	}
	assertMessage(t, db, firstID, "RELAY_PENDING")
	businessClock.Set(businessClock.Now().Add(30 * time.Second))
	if count, err := service.ClassifyDue(ctx, 10); err != nil || count != 1 {
		t.Fatalf("stuck: %d %v", count, err)
	}
	messageKey := domain.MessageKey{Protocol: "mockbridge-v1", SourceChainID: sourceID, DestinationChainID: destID, MessageID: firstID}
	messagePath := "/v1/messages/" + api.EncodeMessageID(messageKey)
	if response := read(messagePath); response.Code != 200 || !bytes.Contains(response.Body.Bytes(), []byte(`"diagnosis":"STUCK"`)) {
		t.Fatalf("stuck API: %d %s", response.Code, response.Body.String())
	}
	sink := operations.NewMemorySink()
	alertWorker, err := operations.NewAlertWorker(db, businessClock, sink, operations.DeliveryConfig{MaxAttempts: 3, Lease: time.Minute, BaseBackoff: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	alertWorker.SetMetrics(operationalMetrics)
	if count, err := alertWorker.DeliverBatch(ctx, 10); err != nil || count != 1 || sink.Count() != 1 {
		t.Fatalf("alert: %d %d %v", count, sink.Count(), err)
	}
	call, err = destABI.Pack("execute", [32]byte(firstID), big.NewInt(31337), firstRecipient, firstPayload, true)
	if err != nil {
		t.Fatal(err)
	}
	localTx(t, destChain, &destContract, call)
	if err = destWatcher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertMessage(t, db, firstID, "DEST_SEEN")
	mine(t, destChain, 2)
	if err = destWatcher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertMessage(t, db, firstID, "COMPLETED")
	if response := read(messagePath); response.Code != 200 || !bytes.Contains(response.Body.Bytes(), []byte(`"state":"COMPLETED"`)) || !bytes.Contains(response.Body.Bytes(), []byte(`"resolved_at":`)) {
		t.Fatalf("completed API: %d %s", response.Code, response.Body.String())
	}
	if response := read("/metrics"); response.Code != 200 || !bytes.Contains(response.Body.Bytes(), []byte(`bridgewatch_stuck_messages{chain_pair="31337_31338",protocol="mockbridge-v1"} 0`)) || !bytes.Contains(response.Body.Bytes(), []byte(`bridgewatch_messages_total{protocol="mockbridge-v1",state="RELAY_PENDING"} 1`)) || !bytes.Contains(response.Body.Bytes(), []byte(`bridgewatch_alerts_total{kind="STUCK",result="DELIVERED"} 1`)) || !bytes.Contains(response.Body.Bytes(), []byte("bridgewatch_delivery_seconds")) {
		t.Fatalf("operational metrics: %d %s", response.Code, response.Body.String())
	}
	var transitions int
	if err = db.QueryRow(ctx, `SELECT count(*) FROM message_transitions m JOIN cross_chain_messages c ON c.id=m.message_pk WHERE c.message_id=$1`, firstID[:]).Scan(&transitions); err != nil || transitions != 5 {
		t.Fatalf("normal transitions: %d %v", transitions, err)
	}
	var stuckCount, resolvedCount, alertCount int
	if err = db.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE resolved_at IS NOT NULL) FROM message_anomalies WHERE kind='STUCK'`).Scan(&stuckCount, &resolvedCount); err != nil || stuckCount != 1 || resolvedCount != 1 {
		t.Fatalf("stuck recovery: %d %d %v", stuckCount, resolvedCount, err)
	}
	if err = db.QueryRow(ctx, `SELECT count(*) FROM message_alerts WHERE status='DELIVERED'`).Scan(&alertCount); err != nil || alertCount != 1 {
		t.Fatalf("alert delivery: %d %v", alertCount, err)
	}
	if families, err := registry.Gather(); err != nil || len(families) == 0 {
		t.Fatalf("operational metrics: %d %v", len(families), err)
	}
	if final {
		// Replaying an identical execution and then a conflicting one must not
		// leave the message completed. Both old observations remain durable.
		completedSnapshot := snapshot(t, destChain)
		call, err = destABI.Pack("execute", [32]byte(firstID), big.NewInt(31337), firstRecipient, firstPayload, true)
		if err != nil {
			t.Fatal(err)
		}
		localTx(t, destChain, &destContract, call)
		if err := destWatcher.SyncOnce(ctx); err != nil {
			t.Fatal(err)
		}
		var duplicateCount int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM message_anomalies WHERE kind='DUPLICATE_EXECUTION' AND resolved_at IS NULL`).Scan(&duplicateCount); err != nil || duplicateCount != 1 {
			t.Fatal("duplicate execution not classified", duplicateCount, err)
		}
		badPayload := [32]byte{99}
		call, err = destABI.Pack("execute", [32]byte(firstID), big.NewInt(31337), firstRecipient, badPayload, true)
		if err != nil {
			t.Fatal(err)
		}
		localTx(t, destChain, &destContract, call)
		if err := destWatcher.SyncOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(ctx, `SELECT count(*) FROM message_anomalies WHERE kind='CONFLICT' AND resolved_at IS NULL`).Scan(&duplicateCount); err != nil || duplicateCount != 1 {
			t.Fatal("conflict not classified", duplicateCount, err)
		}
		if state := messageState(t, db, firstID); state == "COMPLETED" {
			t.Fatal("conflicting execution retained completion")
		}
		var destOldHead uint64
		if destOldHead, err = destReader.BlockNumber(ctx); err != nil {
			t.Fatal(err)
		}
		revert(t, destChain, completedSnapshot)
		mine(t, destChain, int(destOldHead-completedSnapshotHead(t, destChain)+1))
		if err := destWatcher.SyncOnce(ctx); err != nil {
			t.Fatal(err)
		}
		assertMessage(t, db, firstID, "COMPLETED")

		// Source rollback removes the original send; re-inclusion restores the
		// same deterministic message ID before the destination rollback.
		var sourceOldHead uint64
		if sourceOldHead, err = sourceReader.BlockNumber(ctx); err != nil {
			t.Fatal(err)
		}
		revert(t, sourceChain, sourceSnapshot)
		mine(t, sourceChain, int(sourceOldHead-completedSnapshotHead(t, sourceChain)+1))
		if err := sourceWatcher.SyncOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if state := messageState(t, db, firstID); state == "COMPLETED" {
			t.Fatal("source rollback retained completion")
		}
		call, err = sourceABI.Pack("send", big.NewInt(31338), firstRecipient, firstPayload)
		if err != nil {
			t.Fatal(err)
		}
		receipt := localTx(t, sourceChain, &sourceContract, call)
		if len(receipt.Logs) != 1 || !bytes.Equal(receipt.Logs[0].Data[:32], firstID[:]) {
			t.Fatal("source re-inclusion changed message ID")
		}
		mine(t, sourceChain, 2)
		if err := sourceWatcher.SyncOnce(ctx); err != nil {
			t.Fatal(err)
		}
		assertMessage(t, db, firstID, "COMPLETED")

		if destOldHead, err = destReader.BlockNumber(ctx); err != nil {
			t.Fatal(err)
		}
		revert(t, destChain, destSnapshot)
		mine(t, destChain, int(destOldHead-completedSnapshotHead(t, destChain)+1))
		if err := destWatcher.SyncOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if state := messageState(t, db, firstID); state == "COMPLETED" {
			t.Fatal("destination rollback retained completion")
		}
		call, err = destABI.Pack("execute", [32]byte(firstID), big.NewInt(31337), firstRecipient, firstPayload, true)
		if err != nil {
			t.Fatal(err)
		}
		localTx(t, destChain, &destContract, call)
		mine(t, destChain, 2)
		if err := destWatcher.SyncOnce(ctx); err != nil {
			t.Fatal(err)
		}
		assertMessage(t, db, firstID, "COMPLETED")

		// A fresh watcher uses the durable checkpoint and the second endpoint
		// when the first endpoint fails its bounded head request.
		primary := &interruptedSourceReader{Reader: sourceReader, unavailable: true}
		pool, err := rpcpool.New(sourceID, sourceAnchor, []rpcpool.Endpoint[source.Reader]{{ID: "primary", Reader: primary}, {ID: "secondary", Reader: sourceReader}}, businessClock, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		restarted, err := source.NewWatcher(source.Config{SourceChainID: sourceID, Contract: sourceContract, StartBlock: sourceDeployment.BlockNumber.Uint64(), Confirmations: 3, MaxReorgDepth: 8, MaxBlocksPerRun: 8, ExpectedGenesis: &sourceAnchor}, sourceReader, adapter, sourceRepo)
		if err != nil {
			t.Fatal(err)
		}
		restarted.SetMetrics(sourceMetrics)
		restarted.SetPool(pool)
		if err := restarted.SyncOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if status, err := restarted.Status(ctx); err != nil || !status.Ready || status.Endpoints[0].Eligible || !status.Endpoints[1].Eligible {
			t.Fatal("failover status", status, err)
		}
		sourceWatcher = restarted
		apiServer, err = api.New(db, businessClock, sourceWatcher, destWatcher, registry, operationalMetrics)
		if err != nil {
			t.Fatal(err)
		}
		if response := read("/health/ready"); response.Code != 200 {
			t.Fatal("readiness", response.Code, response.Body.String())
		}
		if response := read("/metrics"); response.Code != 200 {
			t.Fatal("metrics", response.Code)
		}
	}

	secondPayload := [32]byte{8}
	secondRecipient := common.Address{5}
	secondID := predictedMessageID(t, sourceChain.account, secondRecipient, secondPayload, 2)
	call, err = destABI.Pack("execute", [32]byte(secondID), big.NewInt(31337), secondRecipient, secondPayload, true)
	if err != nil {
		t.Fatal(err)
	}
	localTx(t, destChain, &destContract, call)
	if err = destWatcher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertMessage(t, db, secondID, "UNMATCHED_DESTINATION")
	call, err = sourceABI.Pack("send", big.NewInt(31338), secondRecipient, secondPayload)
	if err != nil {
		t.Fatal(err)
	}
	sendReceipt = localTx(t, sourceChain, &sourceContract, call)
	if len(sendReceipt.Logs) != 1 || !bytes.Equal(sendReceipt.Logs[0].Data[:32], secondID[:]) {
		t.Fatal("predicted source identity differs")
	}
	if err = sourceWatcher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertMessage(t, db, secondID, "SOURCE_SEEN")
	mine(t, sourceChain, 2)
	if err = sourceWatcher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertMessage(t, db, secondID, "DEST_SEEN")
	mine(t, destChain, 2)
	if err = destWatcher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertMessage(t, db, secondID, "COMPLETED")
	var messages, observations int
	if err = db.QueryRow(ctx, `SELECT count(*) FROM cross_chain_messages`).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(ctx, `SELECT count(*) FROM message_observations`).Scan(&observations); err != nil {
		t.Fatal(err)
	}
	expectedObservations := 4
	if final {
		expectedObservations = 8
	}
	if messages != 2 || observations != expectedObservations {
		t.Fatalf("two-chain evidence: messages=%d observations=%d", messages, observations)
	}
	checksum := sha256.Sum256([]byte(fmt.Sprintf("messages=%d observations=%d transitions=%d stuck=%d resolved=%d alerts=%d", messages, observations, transitions, stuckCount, resolvedCount, alertCount)))
	t.Logf("operational state counts: messages=%d observations=%d first_transitions=%d stuck=%d resolved=%d delivered_alerts=%d checksum=%x", messages, observations, transitions, stuckCount, resolvedCount, alertCount, checksum)
	t.Logf("source anchor=%s contract=%s deployment_block=%d", sourceChain.genesis.Hex(), sourceContract.Hex(), sourceDeployment.BlockNumber.Uint64())
	t.Logf("destination anchor=%s contract=%s deployment_block=%d", destChain.genesis.Hex(), destContract.Hex(), destDeployment.BlockNumber.Uint64())
	if final {
		writeFinalDemoEvidence(t, db, started, sourceAnchor, destAnchor)
	}
}

type finalDemoEvidence struct {
	ToolVersions     map[string]string `json:"tool_versions"`
	Topology         map[string]any    `json:"topology"`
	Policy           map[string]any    `json:"policy"`
	Scenario         []string          `json:"scenario"`
	Counts           map[string]int    `json:"counts"`
	StateHistogram   map[string]int    `json:"state_histogram"`
	AnomalyHistogram map[string]int    `json:"anomaly_histogram"`
	Checksum         string            `json:"checksum"`
	DurationMS       int64             `json:"duration_ms"`
}

func histogram(t *testing.T, db *pgxpool.Pool, query string) map[string]int {
	t.Helper()
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	values := map[string]int{}
	for rows.Next() {
		var label string
		var count int
		if err := rows.Scan(&label, &count); err != nil {
			t.Fatal(err)
		}
		values[label] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}
func commandVersion(t *testing.T, command string, args ...string) string {
	t.Helper()
	raw, err := exec.Command(command, args...).Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(raw))
}
func writeFinalDemoEvidence(t *testing.T, db *pgxpool.Pool, started time.Time, sourceAnchor, destAnchor domain.Hash) {
	t.Helper()
	ctx := context.Background()
	counts := map[string]int{}
	queries := map[string]string{
		"messages":                       `SELECT count(*) FROM cross_chain_messages`,
		"transitions":                    `SELECT count(*) FROM message_transitions`,
		"observations":                   `SELECT count(*) FROM message_observations`,
		"anomalies":                      `SELECT count(*) FROM message_anomalies`,
		"open_stuck":                     `SELECT count(*) FROM message_anomalies WHERE kind='STUCK' AND resolved_at IS NULL`,
		"completed":                      `SELECT count(*) FROM cross_chain_messages WHERE state='COMPLETED'`,
		"alert_episodes":                 `SELECT count(*) FROM message_alerts`,
		"source_reorgs":                  `SELECT count(*) FROM source_reorgs`,
		"destination_reorgs":             `SELECT count(*) FROM destination_reorgs`,
		"duplicate_business_transitions": `SELECT count(*) FROM (SELECT to_state,lag(to_state) OVER (PARTITION BY message_pk ORDER BY revision) AS preceding FROM message_transitions) x WHERE to_state=preceding`,
	}
	for name, query := range queries {
		var count int
		if err := db.QueryRow(ctx, query).Scan(&count); err != nil {
			t.Fatal(name, err)
		}
		counts[name] = count
	}
	states := histogram(t, db, `SELECT state,count(*) FROM cross_chain_messages GROUP BY state`)
	anomalies := histogram(t, db, `SELECT kind,count(*) FROM message_anomalies GROUP BY kind`)
	parts := make([]string, 0, len(counts)+len(states)+len(anomalies))
	for k, v := range counts {
		parts = append(parts, fmt.Sprintf("count.%s=%d", k, v))
	}
	for k, v := range states {
		parts = append(parts, fmt.Sprintf("state.%s=%d", k, v))
	}
	for k, v := range anomalies {
		parts = append(parts, fmt.Sprintf("anomaly.%s=%d", k, v))
	}
	sort.Strings(parts)
	checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(parts, "\n"))))
	t.Logf("final demo counts=%v states=%v anomalies=%v checksum=%s", counts, states, anomalies, checksum)
	if counts["messages"] != 2 || counts["transitions"] != 15 || counts["observations"] != 8 || counts["anomalies"] != 4 || counts["open_stuck"] != 0 || counts["completed"] != 2 || counts["alert_episodes"] != 1 || counts["source_reorgs"] != 1 || counts["destination_reorgs"] != 2 || counts["duplicate_business_transitions"] != 0 || len(states) != 1 || states["COMPLETED"] != 2 || len(anomalies) != 4 || anomalies["STUCK"] != 1 || anomalies["DUPLICATE_EXECUTION"] != 1 || anomalies["CONFLICT"] != 1 || anomalies["REORGED"] != 1 {
		t.Fatal("final demo count invariant failed")
	}
	const expectedChecksum = "79c87f304c3a1eb24633b7c4f31d9e3d8ecd07c2b886f6e1b74a9efa22aa2ec4"
	if checksum != expectedChecksum {
		t.Fatalf("final demo checksum: got %s expected %s", checksum, expectedChecksum)
	}
	if path := os.Getenv("BRIDGEWATCH_DEMO_EVIDENCE"); path != "" {
		var postgresVersion string
		if err := db.QueryRow(ctx, `SHOW server_version`).Scan(&postgresVersion); err != nil {
			t.Fatal(err)
		}
		evidence := finalDemoEvidence{ToolVersions: map[string]string{"go": runtime.Version(), "anvil": commandVersion(t, os.Getenv("BRIDGEWATCH_ANVIL_BIN"), "--version"), "postgresql": postgresVersion}, Topology: map[string]any{"source_chain_id": 31337, "destination_chain_id": 31338, "source_genesis": fmt.Sprintf("0x%x", sourceAnchor), "destination_genesis": fmt.Sprintf("0x%x", destAnchor), "database": "local PostgreSQL", "alert_sink": "in-memory deterministic"}, Policy: map[string]any{"source_confirmations": 3, "destination_confirmations": 3, "relay_sla_seconds": 30, "maximum_automatic_reorg_depth": 8}, Scenario: []string{"source seen", "source final", "relay pending", "stuck and one alert", "destination seen", "completed", "duplicate execution", "conflicting execution", "destination conflict reorg", "source reorg", "source re-inclusion", "destination reorg", "destination re-execution", "watcher restart", "RPC failover", "API readiness and metrics", "destination-first second message"}, Counts: counts, StateHistogram: states, AnomalyHistogram: anomalies, Checksum: checksum, DurationMS: time.Since(started).Milliseconds()}
		raw, err := json.MarshalIndent(evidence, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(raw, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
}
