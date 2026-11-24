package repository

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"koudmain-worker/internal/model"
)

const testSecret = "s3cr3t"

// newBackendServer démarre un faux NestJS qui vérifie le header
// X-Internal-Secret avant de déléguer à `handler`.
func newBackendServer(t *testing.T, handler http.HandlerFunc) *BackendClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(InternalSecretHeader); got != testSecret {
			t.Errorf("unexpected %s header: %q", InternalSecretHeader, got)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	return NewBackendClient(srv.URL+"/", testSecret, time.Second)
}

func TestGetDeviceTokens(t *testing.T) {
	client := newBackendServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/internal/device-tokens" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.URL.RawQuery != "userIds=1,2,3" {
			t.Errorf("unexpected query %q", r.URL.RawQuery)
		}
		_, _ = io.WriteString(w, `[{"userId":1,"token":"a","platform":"IOS"},{"userId":2,"token":"b","platform":"ANDROID"}]`)
	})

	tokens, err := client.GetDeviceTokens(context.Background(), []int{1, 2, 3})
	if err != nil {
		t.Fatalf("GetDeviceTokens failed: %v", err)
	}
	want := []model.DeviceToken{
		{UserID: 1, Token: "a", Platform: model.PlatformIOS},
		{UserID: 2, Token: "b", Platform: model.PlatformAndroid},
	}
	if len(tokens) != len(want) || tokens[0] != want[0] || tokens[1] != want[1] {
		t.Fatalf("unexpected tokens: %+v", tokens)
	}
}

func TestGetDeviceTokensErrors(t *testing.T) {
	t.Run("unexpected status", func(t *testing.T) {
		client := newBackendServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "boom")
		})
		_, err := client.GetDeviceTokens(context.Background(), []int{1})
		if err == nil || !strings.Contains(err.Error(), "500") || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("expected a 500 error with body, got %v", err)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		client := newBackendServer(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "not json")
		})
		if _, err := client.GetDeviceTokens(context.Background(), []int{1}); err == nil {
			t.Fatal("expected a decoding error")
		}
	})

	t.Run("backend unreachable", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		srv.Close()
		client := NewBackendClient(srv.URL, testSecret, time.Second)
		if _, err := client.GetDeviceTokens(context.Background(), []int{1}); err == nil {
			t.Fatal("expected a network error")
		}
	})

	t.Run("timeout", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		t.Cleanup(srv.Close)
		client := NewBackendClient(srv.URL, testSecret, 50*time.Millisecond)
		if _, err := client.GetDeviceTokens(context.Background(), []int{1}); err == nil {
			t.Fatal("expected a timeout error")
		}
	})

	t.Run("invalid base url", func(t *testing.T) {
		client := NewBackendClient("http://bad host", testSecret, time.Second)
		if _, err := client.GetDeviceTokens(context.Background(), []int{1}); err == nil {
			t.Fatal("expected a request build error")
		}
	})
}

func TestRecordDelivery(t *testing.T) {
	appID := 42
	errMsg := "BadDeviceToken"

	var got map[string]any
	client := newBackendServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/notification-deliveries" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("unexpected content type %q", ct)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("invalid body: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
	})

	err := client.RecordDelivery(context.Background(), model.NotificationDelivery{
		OutboxEventID: 7,
		ApplicationID: &appID,
		UserID:        3,
		Platform:      model.PlatformIOS,
		Channel:       model.DeliveryChannelPush,
		Status:        model.DeliveryStatusFailed,
		ErrorMessage:  &errMsg,
	})
	if err != nil {
		t.Fatalf("RecordDelivery failed: %v", err)
	}

	want := map[string]any{
		"outboxEventId": float64(7),
		"applicationId": float64(42),
		"userId":        float64(3),
		"platform":      "IOS",
		"channel":       "Push",
		"status":        "Failed",
		"errorMessage":  "BadDeviceToken",
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("body[%q] = %v, want %v", key, got[key], value)
		}
	}
}

func TestRecordDeliverySendsNulls(t *testing.T) {
	var raw map[string]json.RawMessage
	client := newBackendServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&raw)
		w.WriteHeader(http.StatusCreated)
	})

	err := client.RecordDelivery(context.Background(), model.NotificationDelivery{
		OutboxEventID: 1, UserID: 1, Platform: model.PlatformAndroid,
		Channel: model.DeliveryChannelPush, Status: model.DeliveryStatusSent,
	})
	if err != nil {
		t.Fatalf("RecordDelivery failed: %v", err)
	}
	if string(raw["applicationId"]) != "null" || string(raw["errorMessage"]) != "null" {
		t.Fatalf("expected explicit nulls, got applicationId=%s errorMessage=%s", raw["applicationId"], raw["errorMessage"])
	}
}

func TestRecordDeliveryUnexpectedStatus(t *testing.T) {
	client := newBackendServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	if err := client.RecordDelivery(context.Background(), model.NotificationDelivery{}); err == nil {
		t.Fatal("expected an error on 400")
	}
}

func TestDeleteDeviceToken(t *testing.T) {
	const token = "abc/def+ghi:jkl"
	client := newBackendServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("unexpected method %s", r.Method)
		}
		if r.URL.EscapedPath() != "/internal/device-tokens/abc%2Fdef+ghi:jkl" {
			t.Errorf("token not url-encoded in path: %q", r.URL.EscapedPath())
		}
		w.WriteHeader(http.StatusNoContent)
	})

	if err := client.DeleteDeviceToken(context.Background(), token); err != nil {
		t.Fatalf("DeleteDeviceToken failed: %v", err)
	}
}

func TestDeleteDeviceTokenStatuses(t *testing.T) {
	for status, wantErr := range map[int]bool{
		http.StatusNotFound:            false,
		http.StatusInternalServerError: true,
	} {
		client := newBackendServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		})
		err := client.DeleteDeviceToken(context.Background(), "tok")
		if (err != nil) != wantErr {
			t.Errorf("status %d: got err=%v, wantErr=%t", status, err, wantErr)
		}
	}
}
