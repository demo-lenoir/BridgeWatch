package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
)

type Hash [32]byte
type Address [20]byte
type ChainID [32]byte

func NewChainID(value *big.Int) (ChainID, error) {
	var id ChainID
	if value == nil || value.Sign() <= 0 || value.BitLen() > 256 {
		return id, errors.New("chain ID must be a positive uint256")
	}
	value.FillBytes(id[:])
	return id, nil
}

func ChainIDFromDecimal(value string) (ChainID, error) {
	n, ok := new(big.Int).SetString(value, 10)
	if !ok {
		return ChainID{}, errors.New("invalid decimal chain ID")
	}
	return NewChainID(n)
}

func (id ChainID) String() string { return new(big.Int).SetBytes(id[:]).String() }
func (id ChainID) Valid() bool    { return id != ChainID{} }

type MessageKey struct {
	Protocol           string
	SourceChainID      ChainID
	DestinationChainID ChainID
	MessageID          Hash
}

func (key MessageKey) Validate() error {
	if key.Protocol == "" || len(key.Protocol) > 64 || !key.SourceChainID.Valid() || !key.DestinationChainID.Valid() || key.SourceChainID == key.DestinationChainID || key.MessageID == (Hash{}) {
		return errors.New("invalid message key")
	}
	return nil
}

// LockID serializes all ingestion for one message without exposing a user-supplied key.
func (key MessageKey) LockID() int64 {
	h := sha256.New()
	h.Write([]byte(key.Protocol))
	h.Write([]byte{0})
	h.Write(key.SourceChainID[:])
	h.Write(key.DestinationChainID[:])
	h.Write(key.MessageID[:])
	sum := h.Sum(nil)
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

type ObservationKey struct {
	ChainID   ChainID
	BlockHash Hash
	TxHash    Hash
	LogIndex  uint64
}

func (key ObservationKey) Validate() error {
	if !key.ChainID.Valid() || key.BlockHash == (Hash{}) || key.TxHash == (Hash{}) {
		return errors.New("invalid observation key")
	}
	if key.LogIndex > uint64(^uint64(0)>>1) {
		return errors.New("log index exceeds PostgreSQL bigint")
	}
	return nil
}

type Observation struct {
	Key         ObservationKey
	BlockNumber uint64
	ParentHash  Hash
	Emitter     Address
	Topics      []Hash
	Data        []byte
}

func (o Observation) Validate() error {
	if err := o.Key.Validate(); err != nil {
		return err
	}
	if o.BlockNumber > uint64(^uint64(0)>>1) || o.ParentHash == (Hash{}) || o.Emitter == (Address{}) || len(o.Topics) != 1 || len(o.Data) > 65536 {
		return errors.New("invalid observation metadata or event shape")
	}
	return nil
}

func (o Observation) Digest() Hash {
	h := sha256.New()
	h.Write(o.Emitter[:])
	for _, topic := range o.Topics {
		h.Write(topic[:])
	}
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(o.Data)))
	h.Write(length[:])
	h.Write(o.Data)
	var result Hash
	copy(result[:], h.Sum(nil))
	return result
}

type SourceClaim struct {
	Key             MessageKey
	Observation     Observation
	Sender          Address
	Recipient       Address
	PayloadHash     Hash
	Token           *Address
	Amount          *big.Int
	ProtocolVersion uint32
}

func (claim SourceClaim) Validate() error {
	if err := claim.Key.Validate(); err != nil {
		return err
	}
	if err := claim.Observation.Validate(); err != nil {
		return err
	}
	if claim.Observation.Key.ChainID != claim.Key.SourceChainID || claim.Sender == (Address{}) || claim.Recipient == (Address{}) || claim.PayloadHash == (Hash{}) || claim.ProtocolVersion == 0 {
		return errors.New("invalid source claim")
	}
	if claim.Amount != nil && (claim.Amount.Sign() < 0 || claim.Amount.BitLen() > 256) {
		return errors.New("amount exceeds uint256")
	}
	if (claim.Token == nil) != (claim.Amount == nil) || (claim.Token != nil && (*claim.Token == (Address{}) || claim.Amount.Sign() == 0)) {
		return errors.New("token and positive amount must be present together")
	}
	return nil
}

type DestinationClaim struct {
	Key             MessageKey
	Observation     Observation
	Recipient       Address
	PayloadHash     Hash
	Success         bool
	ProtocolVersion uint32
}

func (claim DestinationClaim) Validate() error {
	if err := claim.Key.Validate(); err != nil {
		return err
	}
	if err := claim.Observation.Validate(); err != nil {
		return err
	}
	if claim.Observation.Key.ChainID != claim.Key.DestinationChainID || claim.Recipient == (Address{}) || claim.PayloadHash == (Hash{}) || claim.ProtocolVersion == 0 {
		return errors.New("invalid destination claim")
	}
	return nil
}

type MatchResult string

const (
	Match       MatchResult = "MATCH"
	NoMatch     MatchResult = "NO_MATCH"
	Conflict    MatchResult = "CONFLICT"
	Unsupported MatchResult = "UNSUPPORTED"
)

func Correlate(source SourceClaim, dest DestinationClaim) MatchResult {
	if source.ProtocolVersion != dest.ProtocolVersion {
		return Unsupported
	}
	if source.Key != dest.Key {
		return NoMatch
	}
	if source.PayloadHash != dest.PayloadHash || source.Recipient != dest.Recipient {
		return Conflict
	}
	return Match
}

type State string

const (
	NoCanonicalEvidence  State = "NO_CANONICAL_EVIDENCE"
	UnmatchedDestination State = "UNMATCHED_DESTINATION"
	SourceSeen           State = "SOURCE_SEEN"
	SourceFinal          State = "SOURCE_FINAL"
	RelayPending         State = "RELAY_PENDING"
	DestinationSeen      State = "DEST_SEEN"
	Completed            State = "COMPLETED"
)

type Projection struct {
	State               State
	Correlated          bool
	SuccessfulExecution bool
	ObservedFailure     bool
	Conflict            bool
	DuplicateExecution  bool
}

// Project derives correlation from claims and deliberately has no head inputs.
func Project(sources []SourceClaim, destinations []DestinationClaim) Projection {
	result := Projection{State: NoCanonicalEvidence}
	if len(destinations) > 0 {
		result.State = UnmatchedDestination
	}
	if len(sources) > 0 {
		result.State = SourceSeen
	}
	if len(destinations) > 1 {
		result.DuplicateExecution = true
	}
	for i := 1; i < len(sources); i++ {
		if sources[i].PayloadHash != sources[0].PayloadHash || sources[i].Recipient != sources[0].Recipient || sources[i].Sender != sources[0].Sender || sources[i].ProtocolVersion != sources[0].ProtocolVersion || !sameOptionalAddress(sources[i].Token, sources[0].Token) || !sameAmount(sources[i].Amount, sources[0].Amount) {
			result.Conflict = true
		}
	}
	for i := range destinations {
		for j := range sources {
			switch Correlate(sources[j], destinations[i]) {
			case Match:
				result.Correlated = true
				if destinations[i].Success {
					result.SuccessfulExecution = true
				} else {
					result.ObservedFailure = true
				}
			case Conflict, Unsupported:
				result.Conflict = true
			}
		}
	}
	for i := 1; i < len(destinations); i++ {
		if destinations[i].PayloadHash != destinations[0].PayloadHash || destinations[i].Recipient != destinations[0].Recipient || destinations[i].ProtocolVersion != destinations[0].ProtocolVersion || destinations[i].Success != destinations[0].Success {
			result.Conflict = true
		}
	}
	if result.Conflict {
		result.Correlated = false
		result.SuccessfulExecution = false
	}
	return result
}

type LifecycleEvidence struct {
	SourceFinal      bool
	RelayEligible    bool
	DestinationSeen  bool
	DestinationFinal bool
	Blocked          bool
}

// ProjectLifecycle adds independently established canonical/finality evidence
// to the claim projection. It contains no transition history or inverse rules.
func ProjectLifecycle(base Projection, evidence LifecycleEvidence) Projection {
	result := base
	if result.State != SourceSeen || result.Conflict || evidence.Blocked || !evidence.SourceFinal {
		return result
	}
	result.State = SourceFinal
	if evidence.RelayEligible {
		result.State = RelayPending
	}
	if result.Correlated && result.SuccessfulExecution && evidence.DestinationSeen {
		result.State = DestinationSeen
		if evidence.DestinationFinal {
			result.State = Completed
		}
	}
	return result
}

func sameOptionalAddress(a, b *Address) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
func sameAmount(a, b *big.Int) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Cmp(b) == 0
}

func (p Projection) Validate() error {
	if p.State != NoCanonicalEvidence && p.State != UnmatchedDestination && p.State != SourceSeen && p.State != SourceFinal && p.State != RelayPending && p.State != DestinationSeen && p.State != Completed {
		return fmt.Errorf("unsupported projection state: %s", p.State)
	}
	return nil
}
