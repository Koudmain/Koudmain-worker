package push

import (
	"context"
	"sync"

	"koudmain-worker/internal/model"
)

// fakeSender enregistre les envois et retourne `result`.
type fakeSender struct {
	mu     sync.Mutex
	result Result
	sent   []string
}

func (s *fakeSender) Send(_ context.Context, token string, _ model.PushPayload) Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, token)
	return s.result
}

// fakeBackend simule les endpoints internes NestJS.
type fakeBackend struct {
	tokens      []model.DeviceToken
	getErr      error
	recordErr   error
	deleteErr   error
	getCalls    [][]int
	deliveries  []model.NotificationDelivery
	deletedKeys []string
}

func (b *fakeBackend) GetDeviceTokens(_ context.Context, userIDs []int) ([]model.DeviceToken, error) {
	b.getCalls = append(b.getCalls, userIDs)
	return b.tokens, b.getErr
}

func (b *fakeBackend) RecordDelivery(_ context.Context, delivery model.NotificationDelivery) error {
	b.deliveries = append(b.deliveries, delivery)
	return b.recordErr
}

func (b *fakeBackend) DeleteDeviceToken(_ context.Context, token string) error {
	b.deletedKeys = append(b.deletedKeys, token)
	return b.deleteErr
}
