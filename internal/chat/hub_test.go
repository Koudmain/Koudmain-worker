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

	conns, ok := h.clients[42]
	if !ok || len(conns) != 1 || conns[0] != mc {
		t.Fatalf("expected client registered in the slice")
	}

	h.Unregister(42, mc)

	if _, ok := h.clients[42]; ok {
		t.Fatalf("expected client map entry removed when no connections remain")
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

func TestMultiConnectionPerUser(t *testing.T) {
	h := NewHub()

	pcConn := &mockConn{}
	mobileConn := &mockConn{}

	userID := 10

	h.Register(userID, pcConn)
	h.Register(userID, mobileConn)

	if len(h.clients[userID]) != 2 {
		t.Fatalf("expected 2 active connections, got %d", len(h.clients[userID]))
	}

	payload := []byte("broadcast to all my devices")
	h.SendToUser(userID, payload)

	if len(pcConn.messages) != 1 || len(mobileConn.messages) != 1 {
		t.Fatalf("expected both connections to receive the message")
	}

	h.Unregister(userID, pcConn)
	if !pcConn.closed {
		t.Fatalf("expected PC connection to be closed")
	}
	if mobileConn.closed {
		t.Fatalf("expected Mobile connection to remain open")
	}

	if len(h.clients[userID]) != 1 {
		t.Fatalf("expected 1 remaining connection for user, got %d", len(h.clients[userID]))
	}

	h.Unregister(userID, mobileConn)

	if _, ok := h.clients[userID]; ok {
		t.Fatalf("expected user to be completely removed from map")
	}
}
