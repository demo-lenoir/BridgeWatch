package source

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"bridgewatch/internal/domain"
	"bridgewatch/internal/protocol"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type contractArtifact struct {
	ABI      json.RawMessage `json:"abi"`
	Bytecode struct {
		Object string `json:"object"`
	} `json:"bytecode"`
}

func TestRealAnvilSourceCatchup(t *testing.T) {
	bin := os.Getenv("BRIDGEWATCH_ANVIL_BIN")
	dbURL := os.Getenv("BRIDGEWATCH_TEST_SOURCE_DB_URL")
	if bin == "" || dbURL == "" {
		t.Skip("Anvil or source database not configured")
	}
	artifactBytes, err := os.ReadFile(filepath.Join("..", "..", "out", "MockBridgeSource.sol", "MockBridgeSource.json"))
	if err != nil {
		t.Fatal(err)
	}
	var artifact contractArtifact
	if err = json.Unmarshal(artifactBytes, &artifact); err != nil {
		t.Fatal(err)
	}
	contractABI, err := abi.JSON(bytes.NewReader(artifact.ABI))
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--chain-id", "31337", "--host", "127.0.0.1", "--port", strconv.Itoa(port), "--timestamp", "1", "--silent")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); cmd.Wait() }()
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	var client *rpc.Client
	for attempt := 0; attempt < 100; attempt++ {
		try, err := rpc.DialContext(ctx, url)
		if err == nil {
			var id string
			err = try.CallContext(ctx, &id, "eth_chainId")
			if err == nil {
				client = try
				break
			}
			try.Close()
		}
		time.Sleep(25 * time.Millisecond)
	}
	if client == nil {
		t.Fatal("Anvil did not become ready")
	}
	defer client.Close()
	eth := ethclient.NewClient(client)
	var accounts []common.Address
	if err = client.CallContext(ctx, &accounts, "eth_accounts"); err != nil || len(accounts) == 0 {
		t.Fatalf("unlocked local account: %v", err)
	}
	genesis, err := eth.HeaderByNumber(ctx, big.NewInt(0))
	if err != nil {
		t.Fatal(err)
	}
	deployHash := sendLocalTx(t, ctx, client, accounts[0], nil, artifact.Bytecode.Object)
	deployReceipt := waitReceipt(t, ctx, eth, deployHash)
	contract := deployReceipt.ContractAddress
	if contract == (common.Address{}) {
		t.Fatal("contract deployment missing address")
	}
	payload := [32]byte{7}
	call, err := contractABI.Pack("send", big.NewInt(31338), common.Address{4}, payload)
	if err != nil {
		t.Fatal(err)
	}
	sendHash := sendLocalTx(t, ctx, client, accounts[0], &contract, common.Bytes2Hex(call))
	sendReceipt := waitReceipt(t, ctx, eth, sendHash)
	if len(sendReceipt.Logs) != 1 {
		t.Fatalf("expected one source event, got %d", len(sendReceipt.Logs))
	}
	db, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(ctx, `TRUNCATE source_reorgs,source_streams,message_alerts,message_transitions,message_anomalies,observation_conflicts,message_claims,message_observations,cross_chain_messages,chain_checkpoints,chain_blocks RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	sourceID, _ := domain.NewChainID(big.NewInt(31337))
	destID, _ := domain.NewChainID(big.NewInt(31338))
	adapter, err := protocol.NewMockBridgeAdapter(protocol.MockConfig{SourceChainID: sourceID, DestinationChainID: destID, SourceContract: contract, DestinationContract: common.Address{2}, Version: 1, RelaySLA: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := DialHTTP(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	anchor := domain.Hash(genesis.Hash())
	config := Config{SourceChainID: sourceID, Contract: contract, StartBlock: deployReceipt.BlockNumber.Uint64(), Confirmations: 3, MaxReorgDepth: 8, MaxBlocksPerRun: 8, ExpectedGenesis: &anchor}
	metrics, err := NewMetrics(prometheus.NewRegistry(), sourceID, "source_local")
	if err != nil {
		t.Fatal(err)
	}
	watcher, err := NewWatcher(config, WithMetrics(reader, metrics), adapter, NewPostgresRepository(db))
	if err != nil {
		t.Fatal(err)
	}
	watcher.SetMetrics(metrics)
	if err = watcher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	messageID := domain.Hash(sendReceipt.Logs[0].Data[:32])
	var state string
	err = db.QueryRow(ctx, "SELECT state FROM cross_chain_messages WHERE message_id=$1", messageID[:]).Scan(&state)
	if err != nil || state != "SOURCE_SEEN" {
		t.Fatalf("source observation: %s %v", state, err)
	}
	for i := 0; i < 2; i++ {
		var mined any
		if err = client.CallContext(ctx, &mined, "evm_mine"); err != nil {
			t.Fatal(err)
		}
	}
	if err = watcher.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	err = db.QueryRow(ctx, "SELECT state FROM cross_chain_messages WHERE message_id=$1", messageID[:]).Scan(&state)
	if err != nil || state != "SOURCE_FINAL" {
		t.Fatalf("source finality: %s %v", state, err)
	}
	var persistedAnchor []byte
	err = db.QueryRow(ctx, "SELECT genesis_hash FROM source_streams WHERE chain_id=31337").Scan(&persistedAnchor)
	if err != nil || !bytes.Equal(persistedAnchor, anchor[:]) {
		t.Fatal("actual genesis anchor not persisted")
	}
	t.Logf("local source anchor=%s contract=%s deployment_block=%d", genesis.Hash().Hex(), contract.Hex(), deployReceipt.BlockNumber.Uint64())
}

func sendLocalTx(t *testing.T, ctx context.Context, client *rpc.Client, from common.Address, to *common.Address, data string) common.Hash {
	t.Helper()
	if len(data) < 2 || data[:2] != "0x" {
		data = "0x" + data
	}
	args := map[string]any{"from": from, "data": data, "gas": "0x300000"}
	if to != nil {
		args["to"] = to
	}
	var hash common.Hash
	if err := client.CallContext(ctx, &hash, "eth_sendTransaction", args); err != nil {
		t.Fatal(err)
	}
	return hash
}
func waitReceipt(t *testing.T, ctx context.Context, eth *ethclient.Client, hash common.Hash) *types.Receipt {
	t.Helper()
	for i := 0; i < 100; i++ {
		receipt, err := eth.TransactionReceipt(ctx, hash)
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
	t.Fatal("local transaction receipt timeout")
	return nil
}
