package push

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"koudmain-worker/internal/model"
	"koudmain-worker/internal/repository"
)

// fakeHandler enregistre les jobs reçus ; `fail` décide si l'appel n°n
// (à partir de 1) échoue.
type fakeHandler struct {
	mu    sync.Mutex
	jobs  []model.PushJob
	fail  func(call int) bool
	err   error // erreur retournée quand fail(call) est vrai
	calls chan struct{}
}

func newFakeHandler(fail func(call int) bool) *fakeHandler {
	return &fakeHandler{fail: fail, err: errors.New("invalid request"), calls: make(chan struct{}, 100)}
}

func (h *fakeHandler) Handle(_ context.Context, job model.PushJob) error {
	h.mu.Lock()
	h.jobs = append(h.jobs, job)
	call := len(h.jobs)
	h.mu.Unlock()
	h.calls <- struct{}{}
	if h.fail != nil && h.fail(call) {
		return h.err
	}
	return nil
}

func (h *fakeHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.jobs)
}

func testOptions() ConsumerOptions {
	opts := DefaultConsumerOptions()
	opts.MaxAttempts = 3
	opts.Block = 20 * time.Millisecond
	opts.RetryDelay = 5 * time.Millisecond
	opts.RetryAfterIdle = 30 * time.Millisecond
	return opts
}

func newTestRepo(t *testing.T) *repository.RedisRepository {
	t.Helper()
	s := miniredis.RunT(t)
	host, port, _ := strings.Cut(s.Addr(), ":")
	repo, err := repository.NewRedisRepository(host, port, "")
	if err != nil {
		t.Fatalf("redis repository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Client.Close() })
	return repo
}

func addEntry(t *testing.T, repo *repository.RedisRepository, values map[string]any) string {
	t.Helper()
	id, err := repo.Client.XAdd(context.Background(), &redis.XAddArgs{Stream: StreamKey, Values: values}).Result()
	if err != nil {
		t.Fatalf("XAdd failed: %v", err)
	}
	return id
}

// startConsumer lance Run en arrière-plan et retourne une fonction d'arrêt
// qui attend la fin de Run.
func startConsumer(t *testing.T, c *Consumer) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop after context cancellation")
		}
	}
	t.Cleanup(stop)
	return stop
}

func waitCalls(t *testing.T, h *fakeHandler, n int) {
	t.Helper()
	for range n {
		select {
		case <-h.calls:
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for handler call (got %d, want %d)", h.count(), n)
		}
	}
}

func waitNoPending(t *testing.T, repo *repository.RedisRepository, opts ConsumerOptions) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pending, err := repo.Client.XPending(context.Background(), opts.Stream, opts.Group).Result()
		if err == nil && pending.Count == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("entries are still pending (not acked)")
}

const validJob = `{"recipientUserIds":[1,2],"payload":{"title":"t","body":"b","collapseId":"c","data":{"k":1}},` +
	`"outboxEventId":7,"applicationId":42}`

func TestConsumerProcessesAndAcksNewEntries(t *testing.T) {
	repo := newTestRepo(t)
	handler := newFakeHandler(nil)
	opts := testOptions()
	startConsumer(t, NewConsumer(repo, handler, opts))

	addEntry(t, repo, map[string]any{"payload": validJob})
	waitCalls(t, handler, 1)
	waitNoPending(t, repo, opts)

	job := handler.jobs[0]
	if len(job.RecipientUserIDs) != 2 || job.OutboxEventID != 7 || job.ApplicationID == nil || *job.ApplicationID != 42 ||
		job.Payload.Title != "t" || job.Payload.CollapseID != "c" || job.Payload.Data["k"] != float64(1) {
		t.Fatalf("unexpected decoded job %+v", job)
	}
}

func TestConsumerAcksPoisonMessages(t *testing.T) {
	repo := newTestRepo(t)
	handler := newFakeHandler(nil)
	opts := testOptions()
	startConsumer(t, NewConsumer(repo, handler, opts))

	addEntry(t, repo, map[string]any{"payload": "{not json"})
	addEntry(t, repo, map[string]any{"other": "field"})
	addEntry(t, repo, map[string]any{"payload": validJob})

	waitCalls(t, handler, 1)
	waitNoPending(t, repo, opts)
	if handler.count() != 1 {
		t.Fatalf("poison messages must not reach the handler, got %d calls", handler.count())
	}
}

// cancellingHandler cancelle le contexte au premier appel, pour vérifier que
// processAll n'entame pas les entrées suivantes du batch après l'annulation.
type cancellingHandler struct {
	cancel context.CancelFunc
	calls  int
}

func (h *cancellingHandler) Handle(_ context.Context, _ model.PushJob) error {
	h.calls++
	h.cancel()
	return nil
}

func TestConsumerStopsProcessingBatchOnContextCancel(t *testing.T) {
	repo := newTestRepo(t)
	opts := testOptions()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := repo.EnsureConsumerGroup(context.Background(), opts.Stream, opts.Group); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	addEntry(t, repo, map[string]any{"payload": validJob})
	addEntry(t, repo, map[string]any{"payload": validJob})

	handler := &cancellingHandler{cancel: cancel}
	if err := NewConsumer(repo, handler, opts).poll(ctx); err != nil {
		t.Fatalf("poll: %v", err)
	}

	if handler.calls != 1 {
		t.Fatalf("expected exactly 1 handler call before the cancelled context stopped the batch, got %d", handler.calls)
	}

	pending, err := repo.Client.XPending(context.Background(), opts.Stream, opts.Group).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 1 {
		t.Fatalf("expected the second entry to remain pending and untouched, got %d pending", pending.Count)
	}
}

func TestConsumerRetriesFailedEntries(t *testing.T) {
	repo := newTestRepo(t)
	handler := newFakeHandler(func(call int) bool { return call < 3 })
	opts := testOptions()
	opts.MaxAttempts = 5
	c := NewConsumer(repo, handler, opts)
	startConsumer(t, c)

	addEntry(t, repo, map[string]any{"payload": validJob})
	waitCalls(t, handler, 3)
	waitNoPending(t, repo, opts)

	time.Sleep(50 * time.Millisecond)
	if handler.count() != 3 {
		t.Fatalf("expected 2 failures then 1 success, got %d calls", handler.count())
	}
}

func TestConsumerGivesUpAfterMaxAttempts(t *testing.T) {
	repo := newTestRepo(t)
	handler := newFakeHandler(func(int) bool { return true })
	opts := testOptions()
	c := NewConsumer(repo, handler, opts)
	stop := startConsumer(t, c)

	addEntry(t, repo, map[string]any{"payload": validJob})
	waitCalls(t, handler, opts.MaxAttempts)
	waitNoPending(t, repo, opts)

	time.Sleep(50 * time.Millisecond)
	stop()
	if handler.count() != opts.MaxAttempts {
		t.Fatalf("expected exactly %d attempts, got %d", opts.MaxAttempts, handler.count())
	}
}

func TestConsumerTakesOverEntriesOfAStoppedReplica(t *testing.T) {
	repo := newTestRepo(t)
	opts := testOptions()
	ctx := context.Background()

	// Un autre réplica a lu l'entrée puis s'est arrêté sans l'acker.
	if err := repo.EnsureConsumerGroup(ctx, opts.Stream, opts.Group); err != nil {
		t.Fatalf("EnsureConsumerGroup: %v", err)
	}
	addEntry(t, repo, map[string]any{"payload": validJob})
	if entries, err := repo.ReadGroup(ctx, opts.Stream, opts.Group, "stopped-replica", ">", 10, -1); err != nil || len(entries) != 1 {
		t.Fatalf("pre-delivery failed: %v (err=%v)", entries, err)
	}

	handler := newFakeHandler(nil)
	startConsumer(t, NewConsumer(repo, handler, opts))

	waitCalls(t, handler, 1)
	waitNoPending(t, repo, opts)
}

func TestConsumerDoesNotTakeAnEntryAnotherReplicaIsStillHandling(t *testing.T) {
	repo := newTestRepo(t)
	opts := testOptions()
	opts.RetryAfterIdle = time.Hour
	ctx := context.Background()

	if err := repo.EnsureConsumerGroup(ctx, opts.Stream, opts.Group); err != nil {
		t.Fatalf("EnsureConsumerGroup: %v", err)
	}
	addEntry(t, repo, map[string]any{"payload": validJob})
	if _, err := repo.ReadGroup(ctx, opts.Stream, opts.Group, "busy-replica", ">", 10, -1); err != nil {
		t.Fatalf("pre-delivery failed: %v", err)
	}

	handler := newFakeHandler(nil)
	stop := startConsumer(t, NewConsumer(repo, handler, opts))
	time.Sleep(100 * time.Millisecond)
	stop()

	if handler.count() != 0 {
		t.Fatalf("the entry is in flight on another replica and must not be handled twice, got %d calls", handler.count())
	}
}

func TestConsumerNeverGivesUpWhileTheBackendIsUnavailable(t *testing.T) {
	repo := newTestRepo(t)
	handler := newFakeHandler(func(call int) bool { return call <= 4 })
	handler.err = fmt.Errorf("récupération des device tokens: %w",
		&url.Error{Op: "Get", URL: "http://backend:3000/internal/device-tokens", Err: errors.New("connection refused")})
	opts := testOptions()
	opts.MaxAttempts = 2
	startConsumer(t, NewConsumer(repo, handler, opts))

	addEntry(t, repo, map[string]any{"payload": validJob})
	// 4 échecs transitoires (au-delà de MaxAttempts) puis le backend revient.
	waitCalls(t, handler, 5)
	waitNoPending(t, repo, opts)
}

func TestIsTransient(t *testing.T) {
	transport := &url.Error{Op: "Get", URL: "http://backend:3000", Err: errors.New("connection refused")}
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"connection refused":         {fmt.Errorf("récupération des device tokens: %w", transport), true},
		"backend 503":                {&repository.StatusError{Status: 503}, true},
		"backend 503, wrapped":       {fmt.Errorf("récupération des device tokens: %w", &repository.StatusError{Status: 503}), true},
		"backend 400":                {&repository.StatusError{Status: 400}, false},
		"backend 401 (wrong secret)": {&repository.StatusError{Status: 401}, false},
		"invalid response body":      {errors.New("réponse device-tokens invalide"), false},
	} {
		if got := isTransient(tc.err); got != tc.want {
			t.Errorf("%s: isTransient = %v, want %v", name, got, tc.want)
		}
	}
}

// scriptedStream est un StreamClient dont les erreurs sont pilotées par le test.
type scriptedStream struct {
	mu          sync.Mutex
	ensureErrs  []error
	readErrs    []error
	ensureCalls int
}

func (s *scriptedStream) EnsureConsumerGroup(context.Context, string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureCalls++
	if len(s.ensureErrs) > 0 {
		err := s.ensureErrs[0]
		s.ensureErrs = s.ensureErrs[1:]
		return err
	}
	return nil
}

func (s *scriptedStream) ReadGroup(context.Context, string, string, string, string, int64, time.Duration) ([]redis.XMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.readErrs) > 0 {
		err := s.readErrs[0]
		s.readErrs = s.readErrs[1:]
		return nil, err
	}
	return nil, nil
}

func (s *scriptedStream) StalePending(context.Context, string, string, time.Duration, int64) ([]redis.XPendingExt, error) {
	return nil, nil
}

func (s *scriptedStream) Claim(context.Context, string, string, string, time.Duration, []string) ([]redis.XMessage, error) {
	return nil, nil
}

func (s *scriptedStream) Ack(context.Context, string, string, string) error { return nil }

func (s *scriptedStream) ensures() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ensureCalls
}

func TestConsumerRetriesGroupCreationAndRecreatesMissingGroup(t *testing.T) {
	stream := &scriptedStream{
		ensureErrs: []error{errors.New("redis down")},
		readErrs:   []error{errors.New("NOGROUP No such key 'push-jobs'"), errors.New("i/o timeout")},
	}
	stop := startConsumer(t, NewConsumer(stream, newFakeHandler(nil), testOptions()))

	deadline := time.Now().Add(2 * time.Second)
	for stream.ensures() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	stop()
	// 1 échec + 1 succès au démarrage, puis 1 recréation après NOGROUP.
	if got := stream.ensures(); got != 3 {
		t.Fatalf("expected 3 EnsureConsumerGroup calls, got %d", got)
	}
}

func TestConsumerRunReturnsImmediatelyWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		NewConsumer(&scriptedStream{}, newFakeHandler(nil), testOptions()).Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run should return when the context is already cancelled")
	}
}
