package push

import (
	"context"
	"errors"
	"testing"

	"koudmain-worker/internal/model"
)

func intPtr(v int) *int { return &v }

func TestDispatcherSkipsJobWithoutRecipients(t *testing.T) {
	backend := &fakeBackend{}
	d := NewDispatcher(backend, nil)

	if err := d.Handle(context.Background(), model.PushJob{OutboxEventID: 1}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(backend.getCalls) != 0 {
		t.Fatal("device tokens should not be fetched without recipients")
	}
}

func TestDispatcherFailsJobWhenTokensCannotBeFetched(t *testing.T) {
	backend := &fakeBackend{getErr: errors.New("connection refused")}
	sender := &fakeSender{}
	d := NewDispatcher(backend, map[model.Platform]Sender{model.PlatformAndroid: sender})

	err := d.Handle(context.Background(), model.PushJob{RecipientUserIDs: []int{1}})
	if err == nil {
		t.Fatal("expected the job to fail so it is retried")
	}
	if len(sender.sent) != 0 || len(backend.deliveries) != 0 {
		t.Fatal("nothing should be sent nor audited")
	}
}

func TestDispatcherWithoutDeviceTokens(t *testing.T) {
	backend := &fakeBackend{}
	d := NewDispatcher(backend, nil)

	if err := d.Handle(context.Background(), model.PushJob{RecipientUserIDs: []int{1, 2}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(backend.getCalls) != 1 || len(backend.getCalls[0]) != 2 || len(backend.deliveries) != 0 {
		t.Fatalf("unexpected backend calls: %+v", backend)
	}
}

func TestDispatcherSendsAuditsAndRemovesInvalidTokens(t *testing.T) {
	backend := &fakeBackend{tokens: []model.DeviceToken{
		{UserID: 1, Token: "android-ok", Platform: model.PlatformAndroid},
		{UserID: 2, Token: "ios-invalid", Platform: model.PlatformIOS},
		{UserID: 3, Token: "web", Platform: "WEB"},
	}}
	android := &fakeSender{result: Result{Success: true}}
	ios := &fakeSender{result: Result{InvalidToken: true, Error: "BadDeviceToken"}}
	d := NewDispatcher(backend, map[model.Platform]Sender{
		model.PlatformAndroid: android,
		model.PlatformIOS:     ios,
	})

	job := model.PushJob{
		RecipientUserIDs: []int{1, 2, 3},
		Payload:          model.PushPayload{Title: "t", Body: "b"},
		OutboxEventID:    9,
		ApplicationID:    intPtr(42),
	}
	if err := d.Handle(context.Background(), job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(android.sent) != 1 || android.sent[0] != "android-ok" || len(ios.sent) != 1 || ios.sent[0] != "ios-invalid" {
		t.Fatalf("unexpected sends android=%v ios=%v", android.sent, ios.sent)
	}

	if len(backend.deliveries) != 2 {
		t.Fatalf("expected 2 deliveries (unknown platform skipped), got %+v", backend.deliveries)
	}
	sent, failed := backend.deliveries[0], backend.deliveries[1]
	if sent.Status != model.DeliveryStatusSent || sent.ErrorMessage != nil || sent.UserID != 1 ||
		sent.Platform != model.PlatformAndroid || sent.Channel != "Push" ||
		sent.OutboxEventID != 9 || sent.ApplicationID == nil || *sent.ApplicationID != 42 {
		t.Fatalf("unexpected sent delivery %+v", sent)
	}
	if failed.Status != model.DeliveryStatusFailed || failed.ErrorMessage == nil ||
		*failed.ErrorMessage != "BadDeviceToken" || failed.UserID != 2 || failed.Platform != model.PlatformIOS {
		t.Fatalf("unexpected failed delivery %+v", failed)
	}

	if len(backend.deletedKeys) != 1 || backend.deletedKeys[0] != "ios-invalid" {
		t.Fatalf("expected only the invalid token to be deleted, got %v", backend.deletedKeys)
	}
}

func TestDispatcherDoesNotFailJobOnAuditOrDeleteErrors(t *testing.T) {
	backend := &fakeBackend{
		tokens:    []model.DeviceToken{{UserID: 1, Token: "tok", Platform: model.PlatformAndroid}},
		recordErr: errors.New("audit down"),
		deleteErr: errors.New("delete down"),
	}
	sender := &fakeSender{result: Result{InvalidToken: true, Error: "UNREGISTERED"}}
	d := NewDispatcher(backend, map[model.Platform]Sender{model.PlatformAndroid: sender})

	if err := d.Handle(context.Background(), model.PushJob{RecipientUserIDs: []int{1}}); err != nil {
		t.Fatalf("audit/delete errors must not fail the job (would resend the push): %v", err)
	}
	if len(backend.deliveries) != 1 || len(backend.deletedKeys) != 1 {
		t.Fatalf("expected audit and delete to be attempted: %+v", backend)
	}
}

func TestDispatcherAuditsUnavailableProviderAsFailed(t *testing.T) {
	backend := &fakeBackend{tokens: []model.DeviceToken{{UserID: 1, Token: "tok", Platform: model.PlatformIOS}}}
	d := NewDispatcher(backend, map[model.Platform]Sender{
		model.PlatformIOS: UnavailableSender{Provider: "APNs", Reason: "APNS_KEY absent"},
	})

	if err := d.Handle(context.Background(), model.PushJob{RecipientUserIDs: []int{1}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(backend.deliveries) != 1 || backend.deliveries[0].Status != model.DeliveryStatusFailed ||
		*backend.deliveries[0].ErrorMessage != "APNs non configuré: APNS_KEY absent" {
		t.Fatalf("unexpected deliveries %+v", backend.deliveries)
	}
	if len(backend.deletedKeys) != 0 {
		t.Fatal("token must not be deleted when the provider is not configured")
	}
}
