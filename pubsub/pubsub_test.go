package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

const testTimeout = 2 * time.Second

type mark struct {
	topic     string
	partition int32
	offset    int64
}

type fakeSession struct {
	ctx     context.Context
	mu      sync.Mutex
	marks   []mark
	commits int
}

func (s *fakeSession) Claims() map[string][]int32 { return nil }
func (s *fakeSession) MemberID() string           { return "member" }
func (s *fakeSession) GenerationID() int32        { return 1 }
func (s *fakeSession) Context() context.Context   { return s.ctx }

func (s *fakeSession) MarkOffset(topic string, partition int32, offset int64, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.marks = append(s.marks, mark{topic, partition, offset})
}

func (s *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	s.MarkOffset(msg.Topic, msg.Partition, msg.Offset+1, "")
}

func (s *fakeSession) ResetOffset(string, int32, int64, string) {}

func (s *fakeSession) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.commits++
}

func (s *fakeSession) snapshot() (marks []mark, commits int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]mark(nil), s.marks...), s.commits
}

type fakeClaim struct {
	msgs chan *sarama.ConsumerMessage
}

func (c *fakeClaim) Topic() string                            { return "t" }
func (c *fakeClaim) Partition() int32                         { return 0 }
func (c *fakeClaim) InitialOffset() int64                     { return 0 }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.msgs }

func claimWith(offsets ...int64) *fakeClaim {
	c := &fakeClaim{msgs: make(chan *sarama.ConsumerMessage, len(offsets))}
	for _, o := range offsets {
		c.msgs <- &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: o}
	}

	return c
}

type fakeGroup struct {
	closed chan struct{}
	once   sync.Once
}

func (g *fakeGroup) Consume(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
	select {
	case <-ctx.Done():
	case <-g.closed:
	}

	return sarama.ErrClosedConsumerGroup
}

func (g *fakeGroup) Errors() <-chan error           { return nil }
func (g *fakeGroup) Pause(map[string][]int32)       {}
func (g *fakeGroup) Resume(map[string][]int32)      {}
func (g *fakeGroup) PauseAll()                      {}
func (g *fakeGroup) ResumeAll()                     {}
func (g *fakeGroup) Close() error                   { g.once.Do(func() { close(g.closed) }); return nil }
func newFakeGroup() *fakeGroup                      { return &fakeGroup{closed: make(chan struct{})} }
func producerMock(t *testing.T) sarama.SyncProducer { return mocks.NewSyncProducer(t, nil) }
func testClient(t *testing.T, buffer int) *KafkaClient {
	t.Helper()

	return newKafkaClient(newFakeGroup(), producerMock(t), []string{"t"}, []string{"localhost:9092"}, sarama.NewConfig(), buffer)
}

func recv(t *testing.T, c *KafkaClient) *sarama.ConsumerMessage {
	t.Helper()

	select {
	case m := <-c.msgChan:
		return m
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for a message to be delivered")

		return nil
	}
}

func runClaim(t *testing.T, c *KafkaClient, sess *fakeSession, claim *fakeClaim) <-chan error {
	t.Helper()

	h := &consumerGroupHandler{client: c}
	if err := h.Setup(sess); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)

	go func() { done <- h.ConsumeClaim(sess, claim) }()

	return done
}

// The old handler marked every message as soon as it was queued in memory, so a crash
// lost anything that had not been processed yet. Now an offset is committed only
// after the message is acknowledged.
func TestOffsetIsNotMarkedUntilTheMessageIsAcknowledged(t *testing.T) {
	c := testClient(t, 8)
	sess := &fakeSession{ctx: context.Background()}
	claim := claimWith(0, 1, 2)

	runClaim(t, c, sess, claim)

	m0, m1, m2 := recv(t, c), recv(t, c), recv(t, c)

	if marks, _ := sess.snapshot(); len(marks) != 0 {
		t.Fatalf("offsets were marked before any message was acknowledged: %+v", marks)
	}

	c.Ack(m1)

	if marks, _ := sess.snapshot(); len(marks) != 0 {
		t.Fatalf("offset 1 finished but offset 0 is still in flight, got marks %+v", marks)
	}

	c.Ack(m0)

	if marks, _ := sess.snapshot(); len(marks) != 1 || marks[0] != (mark{"t", 0, 2}) {
		t.Fatalf("after 0 and 1 are done the next offset to read is 2, got %+v", marks)
	}

	c.Ack(m2)

	if marks, _ := sess.snapshot(); len(marks) != 2 || marks[1] != (mark{"t", 0, 3}) {
		t.Fatalf("after all three the next offset to read is 3, got %+v", marks)
	}
}

func TestAckWithoutASessionOrWithNilIsHarmless(t *testing.T) {
	c := testClient(t, 1)

	c.Ack(nil)
	c.Ack(&sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 5})
}

func TestCleanupCommitsAndDetachesTheSession(t *testing.T) {
	c := testClient(t, 4)
	sess := &fakeSession{ctx: context.Background()}
	h := &consumerGroupHandler{client: c}

	if err := h.Setup(sess); err != nil {
		t.Fatal(err)
	}

	if err := h.Cleanup(sess); err != nil {
		t.Fatal(err)
	}

	if _, commits := sess.snapshot(); commits != 1 {
		t.Errorf("Cleanup must flush marked offsets with Commit, commits = %d", commits)
	}

	c.Ack(&sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 0})

	if marks, _ := sess.snapshot(); len(marks) != 0 {
		t.Errorf("an ack after the session ended must not mark anything: %+v", marks)
	}
}

func TestConsumeClaimStopsWhenTheSessionEndsWhileBlocked(t *testing.T) {
	c := testClient(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	sess := &fakeSession{ctx: ctx}

	done := runClaim(t, c, sess, claimWith(0, 1, 2, 3)) // nobody reads msgChan, so it blocks

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("ConsumeClaim kept blocking after the session context was cancelled")
	}
}

// Closing the client used to close msgChan under a producer that might still be
// sending on it, which panics.
func TestCloseWhileAClaimIsBlockedDoesNotPanic(t *testing.T) {
	c := testClient(t, 1)
	sess := &fakeSession{ctx: context.Background()}

	done := runClaim(t, c, sess, claimWith(0, 1, 2, 3))

	time.Sleep(20 * time.Millisecond)

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("ConsumeClaim did not return after Close")
	}

	if err := c.Close(); err != nil {
		t.Errorf("a second Close must be a no-op, got %v", err)
	}
}

func TestSubscribeReturnsAnErrorAfterClose(t *testing.T) {
	c := testClient(t, 1)

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)

	go func() {
		_, err := c.Subscribe(context.Background())
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Error("Subscribe must fail once the client is closed")
		}
	case <-time.After(testTimeout):
		t.Fatal("Subscribe blocked on a closed client")
	}
}

func TestSubscribeDeliversQueuedMessagesAndHonoursContext(t *testing.T) {
	c := testClient(t, 2)

	want := &sarama.ConsumerMessage{Topic: "t", Offset: 7}
	c.msgChan <- want

	got, err := c.Subscribe(context.Background())
	if err != nil || got != want {
		t.Fatalf("Subscribe = (%v, %v), want the queued message", got, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := c.Subscribe(ctx); err == nil {
		t.Error("Subscribe must return the context error when cancelled")
	}

	_ = c.Close()
}

// Subscribe must not panic or hang when Close races with a claim that is delivering
// messages. This is the scenario the old implementation broke by closing msgChan.
func TestCloseRacingWithDeliveryNeverPanics(t *testing.T) {
	for i := 0; i < 200; i++ {
		c := testClient(t, 1)
		sess := &fakeSession{ctx: context.Background()}

		offsets := make([]int64, 50)
		for j := range offsets {
			offsets[j] = int64(j)
		}

		done := runClaim(t, c, sess, claimWith(offsets...))

		// Drain a little so the claim keeps sending while Close lands.
		go func() {
			for k := 0; k < 3; k++ {
				select {
				case <-c.msgChan:
				case <-c.done:
					return
				}
			}
		}()

		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		select {
		case <-done:
		case <-time.After(testTimeout):
			t.Fatal("ConsumeClaim did not return after Close")
		}
	}
}
