package pubsub

import "sync"

type partitionKey struct {
	topic     string
	partition int32
}

// partitionState tracks the messages of one partition that are in flight. Kafka
// offsets are positions in a log, not a per-message acknowledgement, so committing
// offset N tells the broker that everything before N is done. Messages are processed
// concurrently and can finish out of order, so the position may only move past an
// offset once every earlier delivered offset has finished.
type partitionState struct {
	pending  []int64        // delivered offsets, in delivery order (ascending)
	finished map[int64]bool // pending offsets that are already done
}

// offsetTracker computes, per partition, the highest offset that is safe to commit.
type offsetTracker struct {
	mu         sync.Mutex
	partitions map[partitionKey]*partitionState
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{partitions: make(map[partitionKey]*partitionState)}
}

// Deliver records that a message was handed to the pipeline. Offsets within a
// partition must be delivered in ascending order, which Kafka guarantees.
func (t *offsetTracker) Deliver(topic string, partition int32, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := partitionKey{topic, partition}

	st, ok := t.partitions[key]
	if !ok {
		st = &partitionState{finished: make(map[int64]bool)}
		t.partitions[key] = st
	}

	st.pending = append(st.pending, offset)
}

// Done records that the message at offset has been fully handled. When that lets the
// committed position move forward it returns the next offset to read, which is the
// value to pass to MarkOffset, and true. Unknown or repeated offsets are ignored.
func (t *offsetTracker) Done(topic string, partition int32, offset int64) (next int64, advanced bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	st, ok := t.partitions[partitionKey{topic, partition}]
	if !ok || !st.markFinished(offset) {
		return 0, false
	}

	released := 0

	for released < len(st.pending) && st.finished[st.pending[released]] {
		delete(st.finished, st.pending[released])

		released++
	}

	if released == 0 {
		return 0, false
	}

	// The next offset to read is the first one still in flight. Offsets between two
	// delivered ones were never delivered (compaction, transaction markers), so they
	// need no processing. With nothing left in flight it is one past the last finished.
	if released < len(st.pending) {
		next = st.pending[released]
	} else {
		next = st.pending[released-1] + 1
	}

	st.pending = st.pending[released:]

	return next, true
}

// markFinished flags a delivered, not yet finished offset and reports whether it did.
func (st *partitionState) markFinished(offset int64) bool {
	if st.finished[offset] {
		return false
	}

	for _, p := range st.pending {
		if p == offset {
			st.finished[offset] = true

			return true
		}
	}

	return false
}

// Reset forgets everything in flight, used when the consumer group session ends and
// the partitions are handed to another consumer.
func (t *offsetTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.partitions = make(map[partitionKey]*partitionState)
}
