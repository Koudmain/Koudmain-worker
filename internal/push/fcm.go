package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"

	"koudmain-worker/internal/model"
)

// fcmClient est le sous-ensemble de *messaging.Client utilisé par FCMSender.
type fcmClient interface {
	Send(ctx context.Context, message *messaging.Message) (string, error)
}

// FCMSender envoie les notifications Android via Firebase Cloud Messaging.
type FCMSender struct {
	client fcmClient
}

type serviceAccount struct {
	Type      string `json:"type"`
	ProjectID string `json:"project_id"`
}

// NewFCMSender crée un FCMSender à partir du contenu JSON brut d'un service
// account Firebase (FIREBASE_SERVICE_ACCOUNT_JSON).
func NewFCMSender(ctx context.Context, serviceAccountJSON string) (*FCMSender, error) {
	if strings.TrimSpace(serviceAccountJSON) == "" {
		return nil, errors.New("FIREBASE_SERVICE_ACCOUNT_JSON absent")
	}

	var sa serviceAccount
	if err := json.Unmarshal([]byte(serviceAccountJSON), &sa); err != nil {
		return nil, fmt.Errorf("FIREBASE_SERVICE_ACCOUNT_JSON n'est pas un JSON valide: %w", err)
	}
	if sa.Type != "service_account" || sa.ProjectID == "" {
		return nil, errors.New("FIREBASE_SERVICE_ACCOUNT_JSON n'est pas un service account (type/project_id manquants)")
	}

	return newFCMSender(ctx, sa.ProjectID,
		option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(serviceAccountJSON)))
}

func newFCMSender(ctx context.Context, projectID string, opts ...option.ClientOption) (*FCMSender, error) {
	app, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: projectID}, opts...)
	if err != nil {
		return nil, fmt.Errorf("initialisation Firebase: %w", err)
	}
	client, err := app.Messaging(ctx)
	if err != nil {
		return nil, fmt.Errorf("initialisation du client FCM: %w", err)
	}
	return &FCMSender{client: client}, nil
}

// Send envoie `payload` au token FCM `token`.
func (s *FCMSender) Send(ctx context.Context, token string, payload model.PushPayload) Result {
	message := &messaging.Message{
		Token: token,
		Notification: &messaging.Notification{
			Title: payload.Title,
			Body:  payload.Body,
		},
		Data: toFCMData(payload.Data),
	}
	if payload.CollapseID != "" {
		message.Android = &messaging.AndroidConfig{CollapseKey: payload.CollapseID}
	}

	if _, err := s.client.Send(ctx, message); err != nil {
		return Result{InvalidToken: isInvalidFCMToken(err), Error: err.Error()}
	}
	return Result{Success: true}
}

// isInvalidFCMToken reprend le mapping de l'ancien push.service.ts NestJS :
// le token est supprimé s'il n'est plus enregistré (UNREGISTERED) ou s'il
// est rejeté comme token mal formé (INVALID_ARGUMENT portant sur le
// registration token). Un INVALID_ARGUMENT lié au payload ne supprime pas
// le token.
func isInvalidFCMToken(err error) bool {
	if messaging.IsUnregistered(err) {
		return true
	}
	return messaging.IsInvalidArgument(err) &&
		strings.Contains(strings.ToLower(err.Error()), "registration token")
}

// toFCMData convertit les data arbitraires du job en map[string]string
// (seul format accepté par FCM) : les strings sont gardées telles quelles,
// les autres valeurs sont sérialisées en JSON.
func toFCMData(data map[string]any) map[string]string {
	if len(data) == 0 {
		return nil
	}
	out := make(map[string]string, len(data))
	for key, value := range data {
		if str, ok := value.(string); ok {
			out[key] = str
			continue
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			out[key] = fmt.Sprint(value)
			continue
		}
		out[key] = string(encoded)
	}
	return out
}
