package pubsub

import "testing"

func deliver(tr *offsetTracker, partition int32, offsets ...int64) {
	for _, o := range offsets {
		tr.Deliver("t", partition, o)
	}
}

func TestTrackerAdvancesInOrder(t *testing.T) {
	tr := newOffsetTracker()
	deliver(tr, 0, 0, 1, 2)

	for i, want := range []int64{1, 2, 3} {
		next, ok := tr.Done("t", 0, int64(i))
		if !ok || next != want {
			t.Errorf("Done(%d) = (%d, %v), want (%d, true)", i, next, ok, want)
		}
	}
}

// A later message finishing first must not commit past an earlier unfinished one.
func TestTrackerNeverCommitsPastAnUnfinishedMessage(t *testing.T) {
	tr := newOffsetTracker()
	deliver(tr, 0, 0, 1, 2)

	if next, ok := tr.Done("t", 0, 2); ok {
		t.Fatalf("offset 2 finished first but offsets 0 and 1 are still in flight; got advance to %d", next)
	}

	if next, ok := tr.Done("t", 0, 0); !ok || next != 1 {
		t.Errorf("after offset 0: (%d, %v), want (1, true)", next, ok)
	}

	if next, ok := tr.Done("t", 0, 1); !ok || next != 3 {
		t.Errorf("after offset 1 the finished offset 2 must be released too: (%d, %v), want (3, true)", next, ok)
	}
}

// Compacted topics and transaction markers leave gaps in the offsets.
func TestTrackerHandlesOffsetGaps(t *testing.T) {
	tr := newOffsetTracker()
	deliver(tr, 0, 5, 9)

	if next, ok := tr.Done("t", 0, 5); !ok || next != 9 {
		t.Errorf("Done(5) = (%d, %v), want (9, true): nothing between 5 and 9 will ever be delivered", next, ok)
	}

	if next, ok := tr.Done("t", 0, 9); !ok || next != 10 {
		t.Errorf("Done(9) = (%d, %v), want (10, true)", next, ok)
	}
}

func TestTrackerIgnoresUnknownAndRepeatedOffsets(t *testing.T) {
	tr := newOffsetTracker()
	deliver(tr, 0, 0, 1)

	if _, ok := tr.Done("t", 0, 100); ok {
		t.Error("an offset that was never delivered must be ignored")
	}

	if _, ok := tr.Done("other", 0, 0); ok {
		t.Error("an unknown topic must be ignored")
	}

	if _, ok := tr.Done("t", 0, 0); !ok {
		t.Fatal("first Done(0) should advance")
	}

	if _, ok := tr.Done("t", 0, 0); ok {
		t.Error("a repeated Done must not advance again")
	}
}

func TestTrackerKeepsPartitionsIndependent(t *testing.T) {
	tr := newOffsetTracker()
	deliver(tr, 0, 0, 1)
	deliver(tr, 1, 0)

	if next, ok := tr.Done("t", 1, 0); !ok || next != 1 {
		t.Errorf("partition 1: (%d, %v), want (1, true)", next, ok)
	}

	if _, ok := tr.Done("t", 0, 1); ok {
		t.Error("partition 0 still has offset 0 in flight; finishing 1 must not advance it")
	}
}

func TestTrackerFirstDeliveryDefinesTheStartingPoint(t *testing.T) {
	tr := newOffsetTracker()
	deliver(tr, 0, 40, 41)

	// Finishing 41 first leaves 40 in flight: the committed position must stay at 40,
	// which is where this consumer started, so there is nothing to advance.
	if _, ok := tr.Done("t", 0, 41); ok {
		t.Error("must not advance while the first delivered offset is unfinished")
	}

	if next, ok := tr.Done("t", 0, 40); !ok || next != 42 {
		t.Errorf("Done(40) = (%d, %v), want (42, true)", next, ok)
	}
}
