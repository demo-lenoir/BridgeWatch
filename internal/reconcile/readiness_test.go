package reconcile

import (
	"context"
	"errors"
	"testing"
)

type readinessStub struct {
	ready bool
	err   error
}

func (s readinessStub) Ready(context.Context) (bool, error) { return s.ready, s.err }

func TestBothReadyRequiresIndependentChains(t *testing.T) {
	for _, test := range []struct {
		source, destination bool
		expected            bool
	}{{true, true, true}, {true, false, false}, {false, true, false}, {false, false, false}} {
		got, err := BothReady(context.Background(), readinessStub{ready: test.source}, readinessStub{ready: test.destination})
		if err != nil || got != test.expected {
			t.Fatalf("source=%v destination=%v: %v %v", test.source, test.destination, got, err)
		}
	}
	failure := errors.New("destination unavailable")
	if _, err := BothReady(context.Background(), readinessStub{ready: true}, readinessStub{err: failure}); !errors.Is(err, failure) {
		t.Fatalf("missing destination error: %v", err)
	}
}
