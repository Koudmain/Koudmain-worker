package push

import (
	"context"
	"errors"
	"strings"
	"testing"

	"koudmain-worker/internal/model"
)

func TestNewSendersFallsBackWhenCredentialsAreMissing(t *testing.T) {
	senders := NewSenders(context.Background(), Config{})

	for platform, provider := range map[model.Platform]string{
		model.PlatformAndroid: "FCM",
		model.PlatformIOS:     "APNs",
	} {
		sender, ok := senders[platform].(UnavailableSender)
		if !ok {
			t.Fatalf("%s: expected an UnavailableSender, got %T", platform, senders[platform])
		}
		result := sender.Send(context.Background(), "tok", model.PushPayload{})
		if result.Success || result.InvalidToken || !strings.HasPrefix(result.Error, provider+" non configuré: ") {
			t.Fatalf("%s: unexpected result %+v", platform, result)
		}
	}
}

func TestNewSendersUsesConfiguredProviders(t *testing.T) {
	origFCM, origAPNs := fcmFactory, apnsFactory
	t.Cleanup(func() { fcmFactory, apnsFactory = origFCM, origAPNs })

	fcm, apns := &fakeSender{}, &fakeSender{}
	fcmFactory = func(context.Context, Config) (Sender, error) { return fcm, nil }
	apnsFactory = func(Config) (Sender, error) { return apns, nil }

	senders := NewSenders(context.Background(), Config{})
	if senders[model.PlatformAndroid] != fcm || senders[model.PlatformIOS] != apns {
		t.Fatalf("unexpected senders %v", senders)
	}

	apnsFactory = func(Config) (Sender, error) { return nil, errors.New("bad key") }
	senders = NewSenders(context.Background(), Config{})
	if senders[model.PlatformAndroid] != fcm {
		t.Fatal("FCM should stay enabled when only APNs fails")
	}
	if _, ok := senders[model.PlatformIOS].(UnavailableSender); !ok {
		t.Fatal("APNs should fall back to UnavailableSender")
	}
}

func TestValidateInternalSecret(t *testing.T) {
	cases := []struct {
		name      string
		secret    string
		appEnv    string
		wantError bool
	}{
		{"secret present, prod", "s3cr3t", "production", false},
		{"secret present, dev", "s3cr3t", "development", false},
		{"secret present, APP_ENV unset", "s3cr3t", "", false},
		{"secret missing, APP_ENV unset (dev by default)", "", "", false},
		{"secret missing, dev", "", "development", false},
		{"secret missing, prod", "", "production", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateInternalSecret(tc.secret, tc.appEnv)
			if tc.wantError && err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !tc.wantError && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
