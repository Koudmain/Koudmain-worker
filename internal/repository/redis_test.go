package repository

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestNewRedisRepository(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()

	host, port, ok := strings.Cut(s.Addr(), ":")
	if !ok {
		t.Fatalf("unexpected redis address format: %q", s.Addr())
	}

	repo, err := NewRedisRepository(host, port, "")
	if err != nil {
		t.Fatalf("expected repository to be created, got error: %v", err)
	}

	if repo == nil {
		t.Fatal("expected repository to be non-nil")
	}

	if repo.Client == nil {
		t.Fatal("expected redis client to be initialized")
	}

	if _, err := repo.Client.Ping(context.Background()).Result(); err != nil {
		t.Fatalf("expected ping to succeed, got error: %v", err)
	}
}

func TestSubscribe(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()

	host, port, ok := strings.Cut(s.Addr(), ":")
	if !ok {
		t.Fatalf("unexpected redis address format: %q", s.Addr())
	}

	repo, err := NewRedisRepository(host, port, "")
	if err != nil {
		t.Fatalf("expected repository to be created, got error: %v", err)
	}

	channel := "chat:test"
	messages := repo.Subscribe(context.Background(), channel)

	_ = s.Publish(channel, "hello")

	select {
	case msg := <-messages:
		if msg == nil {
			t.Fatal("expected a message, got nil")
		}
		if msg.Channel != channel {
			t.Fatalf("unexpected channel: got %q want %q", msg.Channel, channel)
		}
		if msg.Payload != "hello" {
			t.Fatalf("unexpected payload: got %q want %q", msg.Payload, "hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for redis message")
	}
}
