package push

import (
	"context"
	"fmt"

	"koudmain-worker/internal/model"
)

// Result est l'issue d'un envoi push vers un device.
type Result struct {
	// Success vaut true si le provider a accepté la notification.
	Success bool
	// InvalidToken vaut true si le provider indique que le token n'est plus
	// valide (application désinstallée, token mal formé...) : il doit être
	// supprimé côté NestJS.
	InvalidToken bool
	// Error décrit l'échec (vide en cas de succès). Il est recopié dans
	// l'errorMessage de l'audit.
	Error string
}

// Sender envoie une notification à un device token d'une plateforme donnée.
// Les erreurs du provider sont converties en Result (jamais de panic ni
// d'erreur Go) : un envoi en échec est audité, il ne fait pas échouer le job.
type Sender interface {
	Send(ctx context.Context, token string, payload model.PushPayload) Result
}

// UnavailableSender est utilisé quand les credentials d'un provider sont
// absents ou invalides : le worker démarre quand même et chaque envoi vers
// cette plateforme est audité en Failed avec un message explicite.
type UnavailableSender struct {
	Provider string
	Reason   string
}

// Send retourne systématiquement un échec (token non invalidé).
func (s UnavailableSender) Send(_ context.Context, _ string, _ model.PushPayload) Result {
	return Result{Error: fmt.Sprintf("%s non configuré: %s", s.Provider, s.Reason)}
}
