package chat

import (
	"reflect"
	"testing"
)

type mockConn struct {
	closed       bool
	messages     [][]byte
	messageTypes []int
}

func (m *mockConn) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	return nil
}

func (m *mockConn) WriteMessage(messageType int, data []byte) error {
	// copy bytes to avoid aliasing
	b := make([]byte, len(data))
	copy(b, data)
	m.messages = append(m.messages, b)
	m.messageTypes = append(m.messageTypes, messageType)
	return nil
}

func TestRegisterUnregister(t *testing.T) {
	h := NewHub()
	mc := &mockConn{}

	h.Register(42, mc)

	if _, ok := h.Clients[42]; !ok {
		t.Fatalf("expected client registered")
	}

	h.Unregister(42)

	if _, ok := h.Clients[42]; ok {
		t.Fatalf("expected client unregistered")
	}

	if !mc.closed {
		t.Fatalf("expected connection closed on unregister")
	}
}

func TestSendToUser(t *testing.T) {
	h := NewHub()
	mc := &mockConn{}
	h.Register(7, mc)

	payload := []byte("hello")
	h.SendToUser(7, payload)

	if len(mc.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(mc.messages))
	}

	if !reflect.DeepEqual(mc.messages[0], payload) {
		t.Fatalf("message mismatch: got %v want %v", mc.messages[0], payload)
	}

	// sending to non-existent user should be a no-op
	h.SendToUser(99, []byte("nop"))
}
