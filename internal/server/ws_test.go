package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"koudmain-worker/internal/chat"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
)

func makeTestToken(t *testing.T, secret string, sub int) string {
	t.Helper()

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": float64(sub),
	})

	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	return signed
}

func TestServeWS_MissingToken(t *testing.T) {
	hub := chat.NewHub()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws", nil)
	rec := httptest.NewRecorder()

	ServeWS(hub, rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestServeWS_ValidTokenAndWebSocketConnection(t *testing.T) {
	const secret = "test-secret"
	originalSecret := os.Getenv("JWT_ACCESS_SECRET")
	if err := os.Setenv("JWT_ACCESS_SECRET", secret); err != nil {
		t.Fatalf("failed to set JWT_ACCESS_SECRET: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Setenv("JWT_ACCESS_SECRET", originalSecret)
	})

	hub := chat.NewHub()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWS(hub, w, r)
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	token := makeTestToken(t, secret, 42)

	conn, resp, err := websocket.DefaultDialer.Dial(wsURL+"?token="+token, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

	connCloseErr := conn.Close()
	if connCloseErr != nil {
		t.Fatalf("failed to close websocket connection: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	hub.SendToUser(42, []byte("hello websocket"))

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	messageType, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read websocket message: %v", err)
	}

	if messageType != websocket.TextMessage {
		t.Fatalf("unexpected message type: got %d want %d", messageType, websocket.TextMessage)
	}

	if string(payload) != "hello websocket" {
		t.Fatalf("unexpected payload: got %q want %q", string(payload), "hello websocket")
	}
}
