package push

import (
	"context"
	"log"

	"koudmain-worker/internal/model"
)

// Config regroupe les credentials des providers push, lus depuis
// l'environnement par main.go.
type Config struct {
	FirebaseServiceAccountJSON string
	APNs                       APNsConfig
}

// fcmFactory et apnsFactory sont surchargeables dans les tests.
var (
	fcmFactory = func(ctx context.Context, cfg Config) (Sender, error) {
		return NewFCMSender(ctx, cfg.FirebaseServiceAccountJSON)
	}
	apnsFactory = func(cfg Config) (Sender, error) {
		return NewAPNsSender(cfg.APNs)
	}
)

// NewSenders construit un Sender par plateforme. Si les credentials d'un
// provider sont absents ou invalides, un warning est loggé et un
// UnavailableSender est utilisé à la place : le worker démarre quand même et
// les envois vers cette plateforme sont audités en Failed.
func NewSenders(ctx context.Context, cfg Config) map[model.Platform]Sender {
	senders := make(map[model.Platform]Sender, 2)

	if fcm, err := fcmFactory(ctx, cfg); err != nil {
		log.Printf("WARNING: push FCM désactivé (ANDROID): %v", err)
		senders[model.PlatformAndroid] = UnavailableSender{Provider: "FCM", Reason: err.Error()}
	} else {
		senders[model.PlatformAndroid] = fcm
	}

	if apns, err := apnsFactory(cfg); err != nil {
		log.Printf("WARNING: push APNs désactivé (IOS): %v", err)
		senders[model.PlatformIOS] = UnavailableSender{Provider: "APNs", Reason: err.Error()}
	} else {
		senders[model.PlatformIOS] = apns
	}

	return senders
}
