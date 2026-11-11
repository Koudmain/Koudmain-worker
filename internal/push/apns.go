package push

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sideshow/apns2/token"

	"koudmain-worker/internal/model"
)

// APNsConfig regroupe les paramètres d'authentification APNs par token (.p8).
type APNsConfig struct {
	// Key est le contenu de la clé .p8 (les "\n" échappés sont acceptés).
	Key        string
	KeyID      string
	TeamID     string
	BundleID   string
	Production bool
}

// apnsInvalidTokenReasons sont les raisons APNs pour lesquelles le token est
// considéré comme invalide (mapping repris de l'ancien push.constants.ts).
var apnsInvalidTokenReasons = map[string]bool{
	apns2.ReasonBadDeviceToken:         true,
	apns2.ReasonUnregistered:           true,
	apns2.ReasonDeviceTokenNotForTopic: true,
}

// apnsClient est le sous-ensemble de *apns2.Client utilisé par APNsSender.
type apnsClient interface {
	PushWithContext(ctx apns2.Context, n *apns2.Notification) (*apns2.Response, error)
}

// APNsSender envoie les notifications iOS via APNs (HTTP/2, auth par token).
type APNsSender struct {
	client apnsClient
	topic  string
}

// NewAPNsSender crée un APNsSender à partir de la configuration .p8.
func NewAPNsSender(cfg APNsConfig) (*APNsSender, error) {
	var missing []string
	for name, value := range map[string]string{
		"APNS_KEY":       cfg.Key,
		"APNS_KEY_ID":    cfg.KeyID,
		"APNS_TEAM_ID":   cfg.TeamID,
		"APNS_BUNDLE_ID": cfg.BundleID,
	} {
		if strings.TrimSpace(value) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return nil, fmt.Errorf("variables manquantes: %s", strings.Join(missing, ", "))
	}

	authKey, err := token.AuthKeyFromBytes([]byte(normalizePEM(cfg.Key)))
	if err != nil {
		return nil, fmt.Errorf("APNS_KEY invalide: %w", err)
	}

	client := apns2.NewTokenClient(&token.Token{
		AuthKey: authKey,
		KeyID:   cfg.KeyID,
		TeamID:  cfg.TeamID,
	})
	if cfg.Production {
		client = client.Production()
	} else {
		client = client.Development()
	}

	return &APNsSender{client: client, topic: cfg.BundleID}, nil
}

// Send envoie `p` au device token APNs `deviceToken`.
func (s *APNsSender) Send(ctx context.Context, deviceToken string, p model.PushPayload) Result {
	body := payload.NewPayload().AlertTitle(p.Title).AlertBody(p.Body)
	for key, value := range p.Data {
		// "aps" est réservé par Apple : on ne laisse pas les data l'écraser.
		if key == "aps" {
			continue
		}
		body.Custom(key, value)
	}

	notification := &apns2.Notification{
		DeviceToken: deviceToken,
		Topic:       s.topic,
		CollapseID:  p.CollapseID,
		Payload:     body,
	}

	res, err := s.client.PushWithContext(ctx, notification)
	if err != nil {
		return Result{Error: err.Error()}
	}
	if res.Sent() {
		return Result{Success: true}
	}

	reason := res.Reason
	if reason == "" {
		reason = fmt.Sprintf("status %d", res.StatusCode)
	}
	return Result{InvalidToken: apnsInvalidTokenReasons[res.Reason], Error: reason}
}

// normalizePEM remplace les "\n" échappés (clé passée sur une seule ligne
// dans une variable d'environnement) par de vrais retours à la ligne.
func normalizePEM(key string) string {
	return strings.ReplaceAll(strings.TrimSpace(key), `\n`, "\n")
}
