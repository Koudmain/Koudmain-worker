package push

import (
	"context"
	"fmt"
	"log"
	"time"

	"koudmain-worker/internal/model"
)

// DefaultSendTimeout borne la durée d'un envoi vers un provider.
const DefaultSendTimeout = 10 * time.Second

// BackendAPI est le sous-ensemble des endpoints internes NestJS utilisé par
// le Dispatcher (implémenté par repository.BackendClient).
type BackendAPI interface {
	GetDeviceTokens(ctx context.Context, userIDs []int) ([]model.DeviceToken, error)
	RecordDelivery(ctx context.Context, delivery model.NotificationDelivery) error
	DeleteDeviceToken(ctx context.Context, token string) error
}

// Dispatcher traite un PushJob : résolution des device tokens, envoi via le
// Sender de chaque plateforme, audit, puis suppression des tokens invalides.
type Dispatcher struct {
	backend     BackendAPI
	senders     map[model.Platform]Sender
	sendTimeout time.Duration
}

// NewDispatcher crée un Dispatcher. `senders` associe chaque plateforme à
// son Sender (voir NewSenders).
func NewDispatcher(backend BackendAPI, senders map[model.Platform]Sender) *Dispatcher {
	return &Dispatcher{backend: backend, senders: senders, sendTimeout: DefaultSendTimeout}
}

// Handle traite un job.
//
// Seule l'impossibilité de récupérer les device tokens fait échouer le job
// (erreur retournée => le consumer le rejouera) : rien n'a encore été envoyé,
// rejouer est donc sans risque. Une fois les envois commencés, les échecs
// (provider, audit, suppression de token) sont loggés et audités mais ne
// font pas échouer le job, pour ne jamais renvoyer deux fois la même
// notification aux devices déjà servis.
func (d *Dispatcher) Handle(ctx context.Context, job model.PushJob) error {
	if len(job.RecipientUserIDs) == 0 {
		log.Printf("push: aucun destinataire pour l'outbox event %d, push ignoré", job.OutboxEventID)
		return nil
	}

	tokens, err := d.backend.GetDeviceTokens(ctx, job.RecipientUserIDs)
	if err != nil {
		return fmt.Errorf("récupération des device tokens: %w", err)
	}
	if len(tokens) == 0 {
		log.Printf("push: aucun device token pour l'outbox event %d, push ignoré", job.OutboxEventID)
		return nil
	}

	for _, deviceToken := range tokens {
		d.sendToDevice(ctx, job, deviceToken)
	}
	return nil
}

func (d *Dispatcher) sendToDevice(ctx context.Context, job model.PushJob, deviceToken model.DeviceToken) {
	sender, ok := d.senders[deviceToken.Platform]
	if !ok {
		log.Printf("push: plateforme %q inconnue pour l'utilisateur %d, device ignoré",
			deviceToken.Platform, deviceToken.UserID)
		return
	}

	sendCtx, cancel := context.WithTimeout(ctx, d.sendTimeout)
	result := sender.Send(sendCtx, deviceToken.Token, job.Payload)
	cancel()

	delivery := model.NotificationDelivery{
		OutboxEventID: job.OutboxEventID,
		ApplicationID: job.ApplicationID,
		UserID:        deviceToken.UserID,
		Platform:      deviceToken.Platform,
		Channel:       model.DeliveryChannelPush,
		Status:        model.DeliveryStatusSent,
	}
	if !result.Success {
		errMsg := result.Error
		delivery.Status = model.DeliveryStatusFailed
		delivery.ErrorMessage = &errMsg
		log.Printf("push: échec %s pour l'utilisateur %d (invalidToken=%t): %s",
			deviceToken.Platform, deviceToken.UserID, result.InvalidToken, result.Error)
	}

	if err := d.backend.RecordDelivery(ctx, delivery); err != nil {
		log.Printf("push: audit impossible pour l'utilisateur %d (outbox event %d): %v",
			deviceToken.UserID, job.OutboxEventID, err)
	}

	if result.InvalidToken {
		if err := d.backend.DeleteDeviceToken(ctx, deviceToken.Token); err != nil {
			log.Printf("push: suppression du token invalide de l'utilisateur %d impossible: %v",
				deviceToken.UserID, err)
		}
	}
}
