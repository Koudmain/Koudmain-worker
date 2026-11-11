package repository

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRepo(t *testing.T) *RedisRepository {
	t.Helper()
	s := miniredis.RunT(t)
	host, port, _ := strings.Cut(s.Addr(), ":")
	repo, err := NewRedisRepository(host, port, "")
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Client.Close() })
	return repo
}

func TestEnsureConsumerGroupIsIdempotent(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	if err := repo.EnsureConsumerGroup(ctx, "jobs", "group"); err != nil {
		t.Fatalf("first EnsureConsumerGroup failed: %v", err)
	}
	if err := repo.EnsureConsumerGroup(ctx, "jobs", "group"); err != nil {
		t.Fatalf("BUSYGROUP should be ignored, got: %v", err)
	}

	groups, err := repo.Client.XInfoGroups(ctx, "jobs").Result()
	if err != nil || len(groups) != 1 || groups[0].Name != "group" {
		t.Fatalf("expected one group named \"group\", got %v (err=%v)", groups, err)
	}
}

func TestEnsureConsumerGroupPropagatesOtherErrors(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	// Une clé d'un autre type fait échouer XGROUP CREATE (WRONGTYPE).
	if err := repo.Client.Set(ctx, "jobs", "not-a-stream", 0).Err(); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := repo.EnsureConsumerGroup(ctx, "jobs", "group"); err == nil {
		t.Fatal("expected an error on a non-stream key")
	}
}

func TestReadGroupNewPendingAndAck(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	if err := repo.EnsureConsumerGroup(ctx, "jobs", "group"); err != nil {
		t.Fatalf("EnsureConsumerGroup failed: %v", err)
	}
	id, err := repo.Client.XAdd(ctx, &redis.XAddArgs{Stream: "jobs", Values: map[string]any{"payload": "{}"}}).Result()
	if err != nil {
		t.Fatalf("XAdd failed: %v", err)
	}

	entries, err := repo.ReadGroup(ctx, "jobs", "group", "c1", ">", 10, 50*time.Millisecond)
	if err != nil || len(entries) != 1 || entries[0].ID != id || entries[0].Values["payload"] != "{}" {
		t.Fatalf("unexpected new entries %v (err=%v)", entries, err)
	}

	pending, err := repo.ReadGroup(ctx, "jobs", "group", "c1", "0", 10, -1)
	if err != nil || len(pending) != 1 || pending[0].ID != id {
		t.Fatalf("expected the unacked entry in pending, got %v (err=%v)", pending, err)
	}

	if ackErr := repo.Ack(ctx, "jobs", "group", id); ackErr != nil {
		t.Fatalf("Ack failed: %v", ackErr)
	}

	pending, err = repo.ReadGroup(ctx, "jobs", "group", "c1", "0", 10, -1)
	if err != nil || len(pending) != 0 {
		t.Fatalf("expected no pending entry after ack, got %v (err=%v)", pending, err)
	}
}

func TestReadGroupReturnsEmptyOnTimeout(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	if err := repo.EnsureConsumerGroup(ctx, "jobs", "group"); err != nil {
		t.Fatalf("EnsureConsumerGroup failed: %v", err)
	}

	entries, err := repo.ReadGroup(ctx, "jobs", "group", "c1", ">", 10, 20*time.Millisecond)
	if err != nil || len(entries) != 0 {
		t.Fatalf("expected no entry and no error, got %v (err=%v)", entries, err)
	}
}

func TestReadGroupWithoutGroupFails(t *testing.T) {
	repo := newTestRepo(t)

	_, err := repo.ReadGroup(context.Background(), "jobs", "missing", "c1", "0", 10, -1)
	if err == nil || !strings.Contains(err.Error(), "NOGROUP") {
		t.Fatalf("expected a NOGROUP error, got %v", err)
	}
}
