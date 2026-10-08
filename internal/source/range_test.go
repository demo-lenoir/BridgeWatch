package source

import (
	"math"
	"testing"
)

func FuzzBoundRange(f *testing.F) {
	f.Add(uint64(1), uint64(10), uint64(3))
	f.Add(uint64(math.MaxUint64-1), uint64(math.MaxUint64), uint64(2))
	f.Add(uint64(10), uint64(9), uint64(1))
	f.Fuzz(func(t *testing.T, from, head, maximum uint64) {
		end, err := boundedEnd(from, head, maximum)
		if maximum == 0 || from > head {
			if err == nil {
				t.Fatal("invalid range accepted")
			}
			return
		}
		if err != nil || end < from || end > head || end-from+1 > maximum {
			t.Fatalf("bad bound %d %d %d => %d %v", from, head, maximum, end, err)
		}
	})
}

func FuzzCommonAncestor(f *testing.F) {
	f.Add(uint8(10), uint8(8), uint8(8))
	f.Add(uint8(10), uint8(1), uint8(8))
	f.Add(uint8(0), uint8(0), uint8(1))
	f.Fuzz(func(t *testing.T, tipByte, ancestorByte, maximumByte uint8) {
		tip := uint64(tipByte)
		ancestor := uint64(ancestorByte)
		maximum := uint64(maximumByte)
		if ancestor > tip {
			ancestor = tip
		}
		if tip > 0 && ancestor == tip {
			ancestor--
		}
		got, found, err := boundedCommonAncestor(tip, 0, maximum, func(number uint64) (bool, error) {
			return number <= ancestor, nil
		})
		if maximum == 0 {
			if err == nil {
				t.Fatal("zero bound accepted")
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		withinBound := tip-ancestor <= maximum && tip > 0
		if found != withinBound {
			t.Fatalf("found=%v want=%v tip=%d ancestor=%d maximum=%d", found, withinBound, tip, ancestor, maximum)
		}
		if found {
			if got > ancestor || got > tip || tip-got > maximum {
				t.Fatalf("invalid ancestor %d", got)
			}
			if got != ancestor {
				t.Fatalf("did not choose nearest common ancestor: got=%d want=%d", got, ancestor)
			}
		}
	})
}
