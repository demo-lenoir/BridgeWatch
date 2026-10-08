package protocol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"bridgewatch/internal/domain"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const mockABI = `[{"anonymous":false,"inputs":[{"indexed":false,"name":"messageId","type":"bytes32"},{"indexed":false,"name":"destinationChainId","type":"uint256"},{"indexed":false,"name":"sender","type":"address"},{"indexed":false,"name":"recipient","type":"address"},{"indexed":false,"name":"payloadHash","type":"bytes32"},{"indexed":false,"name":"token","type":"address"},{"indexed":false,"name":"amount","type":"uint256"}],"name":"MessageSent","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"messageId","type":"bytes32"},{"indexed":false,"name":"sourceChainId","type":"uint256"},{"indexed":false,"name":"recipient","type":"address"},{"indexed":false,"name":"payloadHash","type":"bytes32"},{"indexed":false,"name":"success","type":"bool"}],"name":"MessageExecuted","type":"event"}]`

type EventLog struct {
	ChainID    domain.ChainID
	ParentHash common.Hash
	Log        types.Log
}

type RelayPolicy struct{ SLA time.Duration }

type Adapter interface {
	DecodeSource(context.Context, EventLog) (domain.SourceClaim, error)
	DecodeDestination(context.Context, EventLog) (domain.DestinationClaim, error)
	Correlate(domain.SourceClaim, domain.DestinationClaim) domain.MatchResult
	RelayExpectation(domain.SourceClaim) RelayPolicy
}

type MockConfig struct {
	SourceChainID       domain.ChainID
	DestinationChainID  domain.ChainID
	SourceContract      common.Address
	DestinationContract common.Address
	Version             uint32
	RelaySLA            time.Duration
}

type MockBridgeAdapter struct {
	config MockConfig
	abi    abi.ABI
}

func NewMockBridgeAdapter(config MockConfig) (*MockBridgeAdapter, error) {
	if !config.SourceChainID.Valid() || !config.DestinationChainID.Valid() || config.SourceChainID == config.DestinationChainID || config.SourceContract == (common.Address{}) || config.DestinationContract == (common.Address{}) || config.Version != 1 || config.RelaySLA <= 0 {
		return nil, errors.New("invalid MockBridge v1 configuration")
	}
	parsed, err := abi.JSON(strings.NewReader(mockABI))
	if err != nil {
		return nil, fmt.Errorf("parse MockBridge ABI: %w", err)
	}
	return &MockBridgeAdapter{config: config, abi: parsed}, nil
}

func (a *MockBridgeAdapter) SourceEvent() abi.Event      { return a.abi.Events["MessageSent"] }
func (a *MockBridgeAdapter) DestinationEvent() abi.Event { return a.abi.Events["MessageExecuted"] }

func (a *MockBridgeAdapter) DecodeSource(ctx context.Context, entry EventLog) (domain.SourceClaim, error) {
	if err := ctx.Err(); err != nil {
		return domain.SourceClaim{}, err
	}
	event := a.SourceEvent()
	if err := a.validateLog(entry, a.config.SourceChainID, a.config.SourceContract, event.ID, 7*32); err != nil {
		return domain.SourceClaim{}, err
	}
	values, err := event.Inputs.Unpack(entry.Log.Data)
	if err != nil || len(values) != 7 {
		return domain.SourceClaim{}, errors.New("malformed source ABI data")
	}
	canonical, err := event.Inputs.Pack(values...)
	if err != nil || !bytes.Equal(canonical, entry.Log.Data) {
		return domain.SourceClaim{}, errors.New("noncanonical source ABI data")
	}
	id, ok0 := values[0].([32]byte)
	destID, ok1 := values[1].(*big.Int)
	sender, ok2 := values[2].(common.Address)
	recipient, ok3 := values[3].(common.Address)
	payload, ok4 := values[4].([32]byte)
	token, ok5 := values[5].(common.Address)
	amount, ok6 := values[6].(*big.Int)
	if !(ok0 && ok1 && ok2 && ok3 && ok4 && ok5 && ok6) {
		return domain.SourceClaim{}, errors.New("unexpected source ABI types")
	}
	chain, err := domain.NewChainID(destID)
	if err != nil || chain != a.config.DestinationChainID {
		return domain.SourceClaim{}, errors.New("wrong destination chain")
	}
	if amount.Sign() < 0 || amount.BitLen() > 256 || (token == (common.Address{}) && amount.Sign() != 0) || (token != (common.Address{}) && amount.Sign() == 0) {
		return domain.SourceClaim{}, errors.New("invalid token amount")
	}
	claim := domain.SourceClaim{Key: domain.MessageKey{Protocol: "mockbridge-v1", SourceChainID: a.config.SourceChainID, DestinationChainID: chain, MessageID: domain.Hash(id)}, Observation: observation(entry), Sender: domain.Address(sender), Recipient: domain.Address(recipient), PayloadHash: domain.Hash(payload), ProtocolVersion: 1}
	if token != (common.Address{}) {
		t := domain.Address(token)
		claim.Token = &t
		claim.Amount = new(big.Int).Set(amount)
	}
	if err := claim.Validate(); err != nil {
		return domain.SourceClaim{}, err
	}
	return claim, nil
}

func (a *MockBridgeAdapter) DecodeDestination(ctx context.Context, entry EventLog) (domain.DestinationClaim, error) {
	if err := ctx.Err(); err != nil {
		return domain.DestinationClaim{}, err
	}
	event := a.DestinationEvent()
	if err := a.validateLog(entry, a.config.DestinationChainID, a.config.DestinationContract, event.ID, 5*32); err != nil {
		return domain.DestinationClaim{}, err
	}
	values, err := event.Inputs.Unpack(entry.Log.Data)
	if err != nil || len(values) != 5 {
		return domain.DestinationClaim{}, errors.New("malformed destination ABI data")
	}
	canonical, err := event.Inputs.Pack(values...)
	if err != nil || !bytes.Equal(canonical, entry.Log.Data) {
		return domain.DestinationClaim{}, errors.New("noncanonical destination ABI data")
	}
	id, ok0 := values[0].([32]byte)
	sourceID, ok1 := values[1].(*big.Int)
	recipient, ok2 := values[2].(common.Address)
	payload, ok3 := values[3].([32]byte)
	success, ok4 := values[4].(bool)
	if !(ok0 && ok1 && ok2 && ok3 && ok4) {
		return domain.DestinationClaim{}, errors.New("unexpected destination ABI types")
	}
	chain, err := domain.NewChainID(sourceID)
	if err != nil || chain != a.config.SourceChainID {
		return domain.DestinationClaim{}, errors.New("wrong source chain")
	}
	claim := domain.DestinationClaim{Key: domain.MessageKey{Protocol: "mockbridge-v1", SourceChainID: chain, DestinationChainID: a.config.DestinationChainID, MessageID: domain.Hash(id)}, Observation: observation(entry), Recipient: domain.Address(recipient), PayloadHash: domain.Hash(payload), Success: success, ProtocolVersion: 1}
	if err := claim.Validate(); err != nil {
		return domain.DestinationClaim{}, err
	}
	return claim, nil
}

func (a *MockBridgeAdapter) validateLog(entry EventLog, chain domain.ChainID, contract common.Address, signature common.Hash, size int) error {
	log := entry.Log
	if entry.ChainID != chain || log.Address != contract || log.Removed || len(log.Topics) != 1 || log.Topics[0] != signature || len(log.Data) != size {
		return errors.New("unsupported event origin, signature, or shape")
	}
	return nil
}

func observation(entry EventLog) domain.Observation {
	log := entry.Log
	return domain.Observation{Key: domain.ObservationKey{ChainID: entry.ChainID, BlockHash: domain.Hash(log.BlockHash), TxHash: domain.Hash(log.TxHash), LogIndex: uint64(log.Index)}, BlockNumber: log.BlockNumber, ParentHash: domain.Hash(entry.ParentHash), Emitter: domain.Address(log.Address), Topics: []domain.Hash{domain.Hash(log.Topics[0])}, Data: append([]byte(nil), log.Data...)}
}

func (a *MockBridgeAdapter) Correlate(source domain.SourceClaim, destination domain.DestinationClaim) domain.MatchResult {
	return domain.Correlate(source, destination)
}
func (a *MockBridgeAdapter) RelayExpectation(_ domain.SourceClaim) RelayPolicy {
	return RelayPolicy{SLA: a.config.RelaySLA}
}
