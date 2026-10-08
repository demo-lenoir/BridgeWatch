package protocol

import (
	"context"
	"math/big"
	"testing"
	"time"

	"bridgewatch/internal/domain"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func fixture(t *testing.T) (*MockBridgeAdapter, EventLog, EventLog) {
	t.Helper()
	sourceID, _ := domain.NewChainID(big.NewInt(31337))
	destID, _ := domain.NewChainID(big.NewInt(31338))
	sourceContract := common.HexToAddress("0x1111111111111111111111111111111111111111")
	destContract := common.HexToAddress("0x2222222222222222222222222222222222222222")
	adapter, err := NewMockBridgeAdapter(MockConfig{SourceChainID: sourceID, DestinationChainID: destID, SourceContract: sourceContract, DestinationContract: destContract, Version: 1, RelaySLA: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	id := [32]byte{1}
	payload := [32]byte{2}
	sender := common.HexToAddress("0x3333333333333333333333333333333333333333")
	recipient := common.HexToAddress("0x4444444444444444444444444444444444444444")
	sourceData, err := adapter.SourceEvent().Inputs.Pack(id, big.NewInt(31338), sender, recipient, payload, common.Address{}, big.NewInt(0))
	if err != nil {
		t.Fatal(err)
	}
	destData, err := adapter.DestinationEvent().Inputs.Pack(id, big.NewInt(31337), recipient, payload, true)
	if err != nil {
		t.Fatal(err)
	}
	source := EventLog{ChainID: sourceID, ParentHash: common.HexToHash("0x10"), Log: types.Log{Address: sourceContract, Topics: []common.Hash{adapter.SourceEvent().ID}, Data: sourceData, BlockNumber: 10, BlockHash: common.HexToHash("0x11"), TxHash: common.HexToHash("0x12"), Index: 0}}
	dest := EventLog{ChainID: destID, ParentHash: common.HexToHash("0x20"), Log: types.Log{Address: destContract, Topics: []common.Hash{adapter.DestinationEvent().ID}, Data: destData, BlockNumber: 20, BlockHash: common.HexToHash("0x21"), TxHash: common.HexToHash("0x22"), Index: 0}}
	return adapter, source, dest
}

func TestMockAdapterGolden(t *testing.T) {
	a, source, dest := fixture(t)
	s, err := a.DecodeSource(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	d, err := a.DecodeDestination(context.Background(), dest)
	if err != nil {
		t.Fatal(err)
	}
	if s.Key != d.Key || s.PayloadHash != d.PayloadHash || a.Correlate(s, d) != domain.Match || s.ProtocolVersion != 1 || d.ProtocolVersion != 1 {
		t.Fatalf("unexpected claims: %+v %+v", s, d)
	}
	if a.RelayExpectation(s).SLA != 30*time.Second {
		t.Fatal("wrong SLA")
	}
	dest.Log.Data = append([]byte(nil), dest.Log.Data...)
	dest.Log.Data[96+3] ^= 1
	changed, err := a.DecodeDestination(context.Background(), dest)
	if err != nil {
		t.Fatal(err)
	}
	if a.Correlate(s, changed) != domain.Conflict {
		t.Fatal("payload mismatch accepted")
	}
}

func TestMockAdapterRejectsUnsupportedLogs(t *testing.T) {
	a, source, dest := fixture(t)
	cases := []struct {
		name   string
		source bool
		entry  EventLog
	}{
		{"wrong source address", true, func() EventLog {
			x := source
			x.Log.Address = common.HexToAddress("0x5555555555555555555555555555555555555555")
			return x
		}()},
		{"wrong source topic", true, func() EventLog { x := source; x.Log.Topics = []common.Hash{common.HexToHash("0x99")}; return x }()},
		{"extra source topic", true, func() EventLog {
			x := source
			x.Log.Topics = append(append([]common.Hash(nil), x.Log.Topics...), common.Hash{1})
			return x
		}()},
		{"truncated source", true, func() EventLog { x := source; x.Log.Data = x.Log.Data[:len(x.Log.Data)-1]; return x }()},
		{"noncanonical address padding", true, func() EventLog {
			x := source
			x.Log.Data = append([]byte(nil), x.Log.Data...)
			x.Log.Data[64] = 1
			return x
		}()},
		{"wrong source chain", true, func() EventLog { x := source; x.ChainID = dest.ChainID; return x }()},
		{"wrong destination address", false, func() EventLog { x := dest; x.Log.Address = source.Log.Address; return x }()},
		{"wrong destination topic", false, func() EventLog { x := dest; x.Log.Topics = []common.Hash{source.Log.Topics[0]}; return x }()},
		{"truncated destination", false, func() EventLog { x := dest; x.Log.Data = x.Log.Data[:len(x.Log.Data)-1]; return x }()},
		{"wrong destination chain", false, func() EventLog { x := dest; x.ChainID = source.ChainID; return x }()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.source {
				_, err = a.DecodeSource(context.Background(), tc.entry)
			} else {
				_, err = a.DecodeDestination(context.Background(), tc.entry)
			}
			if err == nil {
				t.Fatal("unsupported log decoded")
			}
		})
	}
}

func TestMockAdapterRejectsUnsupportedVersion(t *testing.T) {
	a, _, _ := fixture(t)
	config := a.config
	config.Version = 2
	if _, err := NewMockBridgeAdapter(config); err == nil {
		t.Fatal("unsupported protocol version accepted")
	}
}

func FuzzDecodeSource(f *testing.F) {
	a, source, _ := fixtureForFuzz(f)
	f.Add(source.Log.Data)
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		entry := source
		entry.Log.Data = data
		claim, err := a.DecodeSource(context.Background(), entry)
		if err == nil {
			if claim.Validate() != nil || len(data) != 7*32 {
				t.Fatal("invalid accepted source claim")
			}
		}
		entry.Log.Topics = []common.Hash{common.Hash{7}}
		if _, err := a.DecodeSource(context.Background(), entry); err == nil {
			t.Fatal("wrong topic accepted")
		}
	})
}
func FuzzDecodeDestination(f *testing.F) {
	a, _, dest := fixtureForFuzz(f)
	f.Add(dest.Log.Data)
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		entry := dest
		entry.Log.Data = data
		claim, err := a.DecodeDestination(context.Background(), entry)
		if err == nil {
			if claim.Validate() != nil || len(data) != 5*32 {
				t.Fatal("invalid accepted destination claim")
			}
		}
		entry.Log.Address = common.Address{9}
		if _, err := a.DecodeDestination(context.Background(), entry); err == nil {
			t.Fatal("wrong address accepted")
		}
	})
}

func fixtureForFuzz(f *testing.F) (*MockBridgeAdapter, EventLog, EventLog) {
	f.Helper()
	sourceID, _ := domain.NewChainID(big.NewInt(31337))
	destID, _ := domain.NewChainID(big.NewInt(31338))
	a, _ := NewMockBridgeAdapter(MockConfig{SourceChainID: sourceID, DestinationChainID: destID, SourceContract: common.Address{1}, DestinationContract: common.Address{2}, Version: 1, RelaySLA: 30 * time.Second})
	id := [32]byte{1}
	payload := [32]byte{2}
	sender := common.Address{3}
	recipient := common.Address{4}
	sdata, _ := a.SourceEvent().Inputs.Pack(id, big.NewInt(31338), sender, recipient, payload, common.Address{}, big.NewInt(0))
	ddata, _ := a.DestinationEvent().Inputs.Pack(id, big.NewInt(31337), recipient, payload, true)
	s := EventLog{ChainID: sourceID, ParentHash: common.Hash{5}, Log: types.Log{Address: common.Address{1}, Topics: []common.Hash{a.SourceEvent().ID}, Data: sdata, BlockHash: common.Hash{6}, TxHash: common.Hash{7}}}
	d := EventLog{ChainID: destID, ParentHash: common.Hash{8}, Log: types.Log{Address: common.Address{2}, Topics: []common.Hash{a.DestinationEvent().ID}, Data: ddata, BlockHash: common.Hash{9}, TxHash: common.Hash{10}}}
	return a, s, d
}
