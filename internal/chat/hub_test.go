package chat

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type mockConn struct {
	mu       sync.Mutex
	closed   bool
	messages [][]byte
	writeErr error
	// block, when set, makes WriteMessage hang until it is closed (a client that stopped reading).
	block chan struct{}
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockConn) SetWriteDeadline(time.Time) error { return nil }

func (m *mockConn) WriteMessage(_ int, data []byte) error {
	if m.block != nil {
		<-m.block
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	b := make([]byte, len(data))
	copy(b, data)
	m.messages = append(m.messages, b)
	return nil
}

func (m *mockConn) received() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][]byte(nil), m.messages...)
}

func (m *mockConn) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// waitFor polls cond: messages are written by the per-connection goroutine.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRegisterUnregister(t *testing.T) {
	h := NewHub()
	mc := &mockConn{}

	h.Register(42, mc)
	if h.ConnectionCount(42) != 1 {
		t.Fatalf("expected client registered")
	}

	h.Unregister(42, mc)
	if _, ok := h.clients[42]; ok {
		t.Fatalf("expected client map entry removed when no connections remain")
	}
	if !mc.isClosed() {
		t.Fatalf("expected connection closed on unregister")
	}

	// A second unregister (handler defer + write error) must be a no-op.
	h.Unregister(42, mc)
}

func TestSendToUser(t *testing.T) {
	h := NewHub()
	mc := &mockConn{}
	h.Register(7, mc)

	payload := []byte("hello")
	h.SendToUser(7, payload)

	waitFor(t, "the message", func() bool { return len(mc.received()) == 1 })
	if !reflect.DeepEqual(mc.received()[0], payload) {
		t.Fatalf("message mismatch: got %v want %v", mc.received()[0], payload)
	}

	// sending to non-existent user should be a no-op
	h.SendToUser(99, []byte("nop"))
}

func TestMultiConnectionPerUser(t *testing.T) {
	h := NewHub()
	pcConn := &mockConn{}
	mobileConn := &mockConn{}
	userID := 10

	h.Register(userID, pcConn)
	h.Register(userID, mobileConn)
	if h.ConnectionCount(userID) != 2 {
		t.Fatalf("expected 2 active connections, got %d", h.ConnectionCount(userID))
	}

	h.SendToUser(userID, []byte("broadcast to all my devices"))
	waitFor(t, "both devices", func() bool {
		return len(pcConn.received()) == 1 && len(mobileConn.received()) == 1
	})

	h.Unregister(userID, pcConn)
	if !pcConn.isClosed() || mobileConn.isClosed() {
		t.Fatalf("expected only the PC connection to be closed")
	}
	if h.ConnectionCount(userID) != 1 {
		t.Fatalf("expected 1 remaining connection for user, got %d", h.ConnectionCount(userID))
	}

	h.Unregister(userID, mobileConn)
	if _, ok := h.clients[userID]; ok {
		t.Fatalf("expected user to be completely removed from map")
	}
}

func TestWriteErrorUnregistersTheConnection(t *testing.T) {
	h := NewHub()
	dead := &mockConn{writeErr: errors.New("broken pipe")}
	h.Register(5, dead)

	h.SendToUser(5, []byte("hello"))

	waitFor(t, "the dead connection to be removed", func() bool { return h.ConnectionCount(5) == 0 })
	if !dead.isClosed() {
		t.Fatalf("expected the dead connection to be closed")
	}
}

func TestStuckClientDoesNotBlockOthers(t *testing.T) {
	h := NewHub()
	stuck := &mockConn{block: make(chan struct{})}
	defer close(stuck.block)
	healthy := &mockConn{}
	h.Register(1, stuck)
	h.Register(2, healthy)

	// More messages than the stuck client can buffer: SendToUser must never wait on it.
	done := make(chan struct{})
	go func() {
		for i := 0; i < sendBuffer+5; i++ {
			h.SendToUser(1, []byte("for the stuck client"))
		}
		h.SendToUser(2, []byte("for the healthy client"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("SendToUser blocked on a client that stopped reading")
	}

	waitFor(t, "the healthy client", func() bool { return len(healthy.received()) == 1 })
	if h.ConnectionCount(1) != 0 || !stuck.isClosed() {
		t.Fatalf("expected the stuck client to be unregistered once its queue was full")
	}
}
