package domain

import (
	"math/big"
	"testing"
)

func TestIdentityAndProjectionBoundary(t *testing.T) {
	sourceID, _ := NewChainID(big.NewInt(31337))
	destID, _ := NewChainID(big.NewInt(31338))
	key := MessageKey{Protocol: "mockbridge-v1", SourceChainID: sourceID, DestinationChainID: destID, MessageID: Hash{1}}
	if err := key.Validate(); err != nil {
		t.Fatal(err)
	}
	if key.LockID() != key.LockID() {
		t.Fatal("unstable lock key")
	}
	observationA := ObservationKey{ChainID: sourceID, BlockHash: Hash{1}, TxHash: Hash{2}, LogIndex: 0}
	observationB := observationA
	observationB.BlockHash = Hash{3}
	if observationA == observationB {
		t.Fatal("competing branches collided")
	}
	source := SourceClaim{Key: key, Sender: Address{1}, Recipient: Address{2}, PayloadHash: Hash{3}, ProtocolVersion: 1}
	dest := DestinationClaim{Key: key, Recipient: Address{2}, PayloadHash: Hash{3}, Success: true, ProtocolVersion: 1}
	cases := []struct {
		sources      []SourceClaim
		destinations []DestinationClaim
		want         State
	}{
		{nil, nil, NoCanonicalEvidence},
		{nil, []DestinationClaim{dest}, UnmatchedDestination},
		{[]SourceClaim{source}, nil, SourceSeen},
		{[]SourceClaim{source}, []DestinationClaim{dest}, SourceSeen},
	}
	for _, tc := range cases {
		actual := Project(tc.sources, tc.destinations)
		if actual.State != tc.want || actual.Validate() != nil {
			t.Fatalf("projection %+v", actual)
		}
		if actual.State == "SOURCE_FINAL" || actual.State == "COMPLETED" {
			t.Fatal("finality boundary crossed")
		}
	}
	if !Project([]SourceClaim{source}, []DestinationClaim{dest}).Correlated {
		t.Fatal("matching evidence not correlated")
	}
	dest.Success = false
	failed := Project([]SourceClaim{source}, []DestinationClaim{dest})
	if !failed.ObservedFailure || failed.SuccessfulExecution {
		t.Fatal("failed execution treated as success")
	}
	dest.Success = true
	dest.PayloadHash = Hash{4}
	if !Project([]SourceClaim{source}, []DestinationClaim{dest}).Conflict {
		t.Fatal("payload conflict missed")
	}
	other := source
	other.PayloadHash = Hash{9}
	forward := Project([]SourceClaim{source, other}, []DestinationClaim{dest})
	reverse := Project([]SourceClaim{other, source}, []DestinationClaim{dest})
	if forward != reverse {
		t.Fatalf("projection depends on claim order: %+v %+v", forward, reverse)
	}
}

func TestLifecycleProjectionRequiresIndependentFinality(t *testing.T) {
	sourceID, _ := NewChainID(big.NewInt(31337))
	destinationID, _ := NewChainID(big.NewInt(31338))
	key := MessageKey{Protocol: "mockbridge-v1", SourceChainID: sourceID, DestinationChainID: destinationID, MessageID: Hash{1}}
	source := SourceClaim{Key: key, Sender: Address{1}, Recipient: Address{2}, PayloadHash: Hash{3}, ProtocolVersion: 1}
	destination := DestinationClaim{Key: key, Recipient: Address{2}, PayloadHash: Hash{3}, Success: true, ProtocolVersion: 1}
	base := Project([]SourceClaim{source}, []DestinationClaim{destination})
	cases := []struct {
		name     string
		evidence LifecycleEvidence
		want     State
	}{
		{"no finality", LifecycleEvidence{}, SourceSeen},
		{"source only", LifecycleEvidence{SourceFinal: true}, SourceFinal},
		{"destination observed", LifecycleEvidence{SourceFinal: true, DestinationSeen: true}, DestinationSeen},
		{"both final", LifecycleEvidence{SourceFinal: true, DestinationSeen: true, DestinationFinal: true}, Completed},
		{"destination final without source", LifecycleEvidence{DestinationSeen: true, DestinationFinal: true}, SourceSeen},
		{"blocked", LifecycleEvidence{SourceFinal: true, DestinationSeen: true, DestinationFinal: true, Blocked: true}, SourceSeen},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			projection := ProjectLifecycle(base, tc.evidence)
			if projection.State != tc.want || projection.Validate() != nil {
				t.Fatalf("projection=%+v want=%s", projection, tc.want)
			}
		})
	}
	destination.PayloadHash = Hash{9}
	conflict := Project([]SourceClaim{source}, []DestinationClaim{destination})
	if got := ProjectLifecycle(conflict, LifecycleEvidence{SourceFinal: true, DestinationSeen: true, DestinationFinal: true}); got.State != SourceSeen || !got.Conflict {
		t.Fatal("conflicting destination reached completion")
	}
}

func TestChainIDValidation(t *testing.T) {
	for _, value := range []*big.Int{nil, big.NewInt(0), big.NewInt(-1), new(big.Int).Lsh(big.NewInt(1), 256)} {
		if _, err := NewChainID(value); err == nil {
			t.Fatal("invalid ID accepted")
		}
	}
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	id, err := NewChainID(max)
	if err != nil || id.String() != max.String() {
		t.Fatal("uint256 round trip failed")
	}
}

func TestSourceClaimTokenAmountPair(t *testing.T) {
	sourceID, _ := NewChainID(big.NewInt(31337))
	destID, _ := NewChainID(big.NewInt(31338))
	claim := SourceClaim{Key: MessageKey{Protocol: "mockbridge-v1", SourceChainID: sourceID, DestinationChainID: destID, MessageID: Hash{1}}, Observation: Observation{Key: ObservationKey{ChainID: sourceID, BlockHash: Hash{2}, TxHash: Hash{3}}, ParentHash: Hash{4}, Emitter: Address{5}, Topics: []Hash{{6}}}, Sender: Address{7}, Recipient: Address{8}, PayloadHash: Hash{9}, ProtocolVersion: 1}
	claim.Amount = big.NewInt(1)
	if claim.Validate() == nil {
		t.Fatal("amount without token accepted")
	}
	token := Address{10}
	claim.Token = &token
	if err := claim.Validate(); err != nil {
		t.Fatal(err)
	}
}
