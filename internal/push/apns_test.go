package push

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/token"

	"koudmain-worker/internal/model"
)

func newP8Key(t *testing.T) (*ecdsa.PrivateKey, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return key, string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

// newFakeAPNs démarre un faux serveur APNs et retourne un APNsSender dont le
// client apns2 réel pointe dessus.
func newFakeAPNs(t *testing.T, handler http.HandlerFunc) *APNsSender {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	key, _ := newP8Key(t)
	client := &apns2.Client{
		Host:       srv.URL,
		HTTPClient: srv.Client(),
		Token:      &token.Token{AuthKey: key, KeyID: "KEYID", TeamID: "TEAMID"},
	}
	return &APNsSender{client: client, topic: "com.koudmain.app"}
}

func TestAPNsSenderSendSuccess(t *testing.T) {
	var body map[string]any
	sender := newFakeAPNs(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/3/device/ios-token" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if r.Header.Get("apns-topic") != "com.koudmain.app" || r.Header.Get("apns-collapse-id") != "application:1" {
			t.Errorf("unexpected headers %v", r.Header)
		}
		if !strings.HasPrefix(r.Header.Get("authorization"), "bearer ") {
			t.Errorf("missing provider token, got %q", r.Header.Get("authorization"))
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("apns-id", "id-1")
		w.WriteHeader(http.StatusOK)
	})

	result := sender.Send(context.Background(), "ios-token", model.PushPayload{
		Title: "t", Body: "b", CollapseID: "application:1",
		Data: map[string]any{"applicationId": 42, "aps": "ignored"},
	})

	if !result.Success || result.InvalidToken || result.Error != "" {
		t.Fatalf("unexpected result %+v", result)
	}
	alert, _ := body["aps"].(map[string]any)["alert"].(map[string]any)
	if alert["title"] != "t" || alert["body"] != "b" {
		t.Fatalf("unexpected aps payload: %v", body["aps"])
	}
	if body["applicationId"] != float64(42) {
		t.Fatalf("custom data missing from payload: %v", body)
	}
}

func TestAPNsSenderErrorMapping(t *testing.T) {
	tests := []struct {
		status      int
		body        string
		wantInvalid bool
		wantError   string
	}{
		{http.StatusBadRequest, `{"reason":"BadDeviceToken"}`, true, "BadDeviceToken"},
		{http.StatusGone, `{"reason":"Unregistered","timestamp":1700000000000}`, true, "Unregistered"},
		{http.StatusBadRequest, `{"reason":"DeviceTokenNotForTopic"}`, true, "DeviceTokenNotForTopic"},
		{http.StatusRequestEntityTooLarge, `{"reason":"PayloadTooLarge"}`, false, "PayloadTooLarge"},
		{http.StatusInternalServerError, ``, false, "status 500"},
	}

	for _, tt := range tests {
		t.Run(tt.wantError, func(t *testing.T) {
			sender := newFakeAPNs(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			})

			result := sender.Send(context.Background(), "tok", model.PushPayload{Title: "t", Body: "b"})
			if result.Success || result.InvalidToken != tt.wantInvalid || result.Error != tt.wantError {
				t.Fatalf("unexpected result %+v", result)
			}
		})
	}
}

type failingAPNsClient struct{}

func (failingAPNsClient) PushWithContext(apns2.Context, *apns2.Notification) (*apns2.Response, error) {
	return nil, errors.New("connection refused")
}

func TestAPNsSenderTransportError(t *testing.T) {
	sender := &APNsSender{client: failingAPNsClient{}, topic: "com.koudmain.app"}

	result := sender.Send(context.Background(), "tok", model.PushPayload{Title: "t", Body: "b"})
	if result.Success || result.InvalidToken || result.Error != "connection refused" {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestNewAPNsSender(t *testing.T) {
	_, key := newP8Key(t)
	escaped := strings.ReplaceAll(strings.TrimSpace(key), "\n", `\n`)

	for _, production := range []bool{false, true} {
		sender, err := NewAPNsSender(APNsConfig{
			Key: escaped, KeyID: "KEYID", TeamID: "TEAMID", BundleID: "com.koudmain.app", Production: production,
		})
		if err != nil {
			t.Fatalf("NewAPNsSender failed: %v", err)
		}
		wantHost := apns2.HostDevelopment
		if production {
			wantHost = apns2.HostProduction
		}
		if got := sender.client.(*apns2.Client).Host; got != wantHost {
			t.Fatalf("production=%t: host %q, want %q", production, got, wantHost)
		}
		if sender.topic != "com.koudmain.app" {
			t.Fatalf("unexpected topic %q", sender.topic)
		}
	}
}

func TestNewAPNsSenderErrors(t *testing.T) {
	_, err := NewAPNsSender(APNsConfig{KeyID: "KEYID"})
	if err == nil || err.Error() != "variables manquantes: APNS_BUNDLE_ID, APNS_KEY, APNS_TEAM_ID" {
		t.Fatalf("unexpected error for missing variables: %v", err)
	}

	_, err = NewAPNsSender(APNsConfig{Key: "not a pem", KeyID: "K", TeamID: "T", BundleID: "B"})
	if err == nil || !strings.Contains(err.Error(), "APNS_KEY invalide") {
		t.Fatalf("unexpected error for an invalid key: %v", err)
	}
}
