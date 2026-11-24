package push

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/api/option"

	"koudmain-worker/internal/model"
)

const fcmErrorTemplate = `{"error":{"code":%CODE%,"message":"%MSG%","status":"%STATUS%",` +
	`"details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"%FCM%"}]}}`

func fcmError(code, msg, status, fcmCode string) string {
	return strings.NewReplacer("%CODE%", code, "%MSG%", msg, "%STATUS%", status, "%FCM%", fcmCode).
		Replace(fcmErrorTemplate)
}

// newFakeFCM démarre un faux endpoint FCM HTTP v1 et retourne un FCMSender
// (SDK Firebase réel) pointant dessus.
func newFakeFCM(t *testing.T, handler http.HandlerFunc) *FCMSender {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	sender, err := newFCMSender(context.Background(), "test-project",
		option.WithEndpoint(srv.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("newFCMSender failed: %v", err)
	}
	return sender
}

func TestFCMSenderSendSuccess(t *testing.T) {
	var body struct {
		Message struct {
			Token        string            `json:"token"`
			Notification map[string]string `json:"notification"`
			Data         map[string]string `json:"data"`
			Android      struct {
				CollapseKey string `json:"collapse_key"`
			} `json:"android"`
		} `json:"message"`
	}
	sender := newFakeFCM(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/projects/test-project/messages:send" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("invalid body: %v", err)
		}
		_, _ = io.WriteString(w, `{"name":"projects/test-project/messages/1"}`)
	})

	result := sender.Send(context.Background(), "android-token", model.PushPayload{
		Title: "t", Body: "b", CollapseID: "application:1",
		Data: map[string]any{"type": "application", "id": 12},
	})

	if !result.Success || result.InvalidToken || result.Error != "" {
		t.Fatalf("unexpected result %+v", result)
	}
	if body.Message.Token != "android-token" ||
		body.Message.Notification["title"] != "t" || body.Message.Notification["body"] != "b" ||
		body.Message.Android.CollapseKey != "application:1" ||
		body.Message.Data["type"] != "application" || body.Message.Data["id"] != "12" {
		t.Fatalf("unexpected FCM message: %+v", body.Message)
	}
}

func TestFCMSenderOmitsAndroidConfigWithoutCollapseID(t *testing.T) {
	var raw map[string]map[string]json.RawMessage
	sender := newFakeFCM(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&raw)
		_, _ = io.WriteString(w, `{"name":"x"}`)
	})

	if result := sender.Send(context.Background(), "tok", model.PushPayload{Title: "t", Body: "b"}); !result.Success {
		t.Fatalf("unexpected result %+v", result)
	}
	if _, ok := raw["message"]["android"]; ok {
		t.Fatal("android config should be omitted without collapseId")
	}
}

func TestFCMSenderErrorMapping(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		wantInvalid bool
	}{
		{"unregistered", http.StatusNotFound,
			fcmError("404", "Requested entity was not found.", "NOT_FOUND", "UNREGISTERED"), true},
		{"invalid registration token", http.StatusBadRequest,
			fcmError("400", "The registration token is not a valid FCM registration token", "INVALID_ARGUMENT", "INVALID_ARGUMENT"), true},
		{"invalid payload", http.StatusBadRequest,
			fcmError("400", "Invalid value at 'message.data'", "INVALID_ARGUMENT", "INVALID_ARGUMENT"), false},
		{"sender id mismatch", http.StatusForbidden,
			fcmError("403", "SenderId mismatch", "PERMISSION_DENIED", "SENDER_ID_MISMATCH"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender := newFakeFCM(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			})

			result := sender.Send(context.Background(), "tok", model.PushPayload{Title: "t", Body: "b"})
			if result.Success || result.Error == "" || result.InvalidToken != tt.wantInvalid {
				t.Fatalf("unexpected result %+v (wantInvalid=%t)", result, tt.wantInvalid)
			}
		})
	}
}

func TestToFCMData(t *testing.T) {
	if toFCMData(nil) != nil {
		t.Fatal("expected nil for empty data")
	}

	got := toFCMData(map[string]any{
		"s":    "text",
		"n":    3.5,
		"b":    true,
		"obj":  map[string]any{"a": 1},
		"list": []any{1, "x"},
		"nil":  nil,
		"bad":  func() {},
	})
	want := map[string]string{
		"s": "text", "n": "3.5", "b": "true", "obj": `{"a":1}`, "list": `[1,"x"]`, "nil": "null",
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("data[%q] = %q, want %q", key, got[key], value)
		}
	}
	if got["bad"] == "" {
		t.Error("non-JSON values should fall back to fmt.Sprint")
	}
}

func TestNewFCMSenderValidation(t *testing.T) {
	for name, input := range map[string]string{
		"empty":           "  ",
		"invalid json":    "{not json",
		"not a sa":        `{"type":"authorized_user"}`,
		"missing project": `{"type":"service_account"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewFCMSender(context.Background(), input); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestNewFCMSenderWithServiceAccount(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	// Clé RSA générée à la volée : aucun secret réel.
	sa, _ := json.Marshal(map[string]string{ //nolint:gosec // G101: faux positif, credentials de test
		"type":           "service_account",
		"project_id":     "koudmain-test",
		"private_key_id": "kid",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   "worker@koudmain-test.iam.gserviceaccount.com",
		"client_id":      "1",
		"token_uri":      "https://oauth2.googleapis.com/token",
	})

	sender, err := NewFCMSender(context.Background(), string(sa))
	if err != nil || sender == nil {
		t.Fatalf("expected a sender, got %v (err=%v)", sender, err)
	}
}
