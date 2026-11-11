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

// dialTestServer starts ServeWS behind a test server and connects user 42 to it.
func dialTestServer(t *testing.T, hub *chat.Hub) *websocket.Conn {
	t.Helper()
	const secret = "test-secret"
	t.Setenv("JWT_ACCESS_SECRET", secret)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWS(hub, w, r)
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?token=" + makeTestToken(t, secret, 42)
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func shortenHeartbeat(t *testing.T) {
	t.Helper()
	previousPongWait, previousPingPeriod := pongWait, pingPeriod
	pongWait, pingPeriod = 300*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { pongWait, pingPeriod = previousPongWait, previousPingPeriod })
}

func waitForConnections(t *testing.T, hub *chat.Hub, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for hub.ConnectionCount(42) != want {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d connection(s), got %d", want, hub.ConnectionCount(42))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServeWS_RemovesAClientThatStopsAnsweringPings(t *testing.T) {
	shortenHeartbeat(t)
	hub := chat.NewHub()

	// The client never reads, so it never answers the server pings (a phone gone offline).
	dialTestServer(t, hub)
	waitForConnections(t, hub, 1)

	waitForConnections(t, hub, 0)
}

func TestServeWS_KeepsAClientThatAnswersPings(t *testing.T) {
	shortenHeartbeat(t)
	hub := chat.NewHub()

	conn := dialTestServer(t, hub)
	go func() {
		// Reading lets gorilla answer pings with pongs.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	waitForConnections(t, hub, 1)

	time.Sleep(3 * pongWait)

	if hub.ConnectionCount(42) != 1 {
		t.Fatalf("expected the responsive client to stay connected")
	}
}
