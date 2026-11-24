package push

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"koudmain-worker/internal/model"
)

const (
	// StreamKey est la clé du Redis Stream alimenté par NestJS (XADD).
	StreamKey = "push-jobs"
	// payloadField est l'unique champ d'une entrée : le PushJob en JSON.
	payloadField = "payload"
)

// StreamClient est le sous-ensemble des opérations Redis Stream utilisé par
// le Consumer (implémenté par repository.RedisRepository).
type StreamClient interface {
	EnsureConsumerGroup(ctx context.Context, stream, group string) error
	ReadGroup(ctx context.Context, stream, group, consumer, startID string, count int64, block time.Duration) ([]redis.XMessage, error)
	Ack(ctx context.Context, stream, group, id string) error
}

// JobHandler traite un PushJob. Une erreur retournée déclenche un retry.
type JobHandler interface {
	Handle(ctx context.Context, job model.PushJob) error
}

// ConsumerOptions paramètre le Consumer.
type ConsumerOptions struct {
	Stream   string
	Group    string
	Consumer string
	// MaxAttempts est le nombre de tentatives avant d'abandonner (et d'acker)
	// une entrée pour ne pas bloquer le stream.
	MaxAttempts int
	// Block est la durée d'attente de XREADGROUP sur les nouvelles entrées.
	Block time.Duration
	// RetryDelay est la pause après une erreur Redis, et la pause initiale
	// avant de rejouer un job en échec (doublée à chaque tentative).
	RetryDelay time.Duration
	// MaxRetryDelay plafonne le backoff exponentiel entre deux tentatives.
	MaxRetryDelay time.Duration
	// BatchSize est le nombre maximum d'entrées lues par XREADGROUP.
	BatchSize int64
}

// DefaultConsumerOptions retourne la configuration de production, alignée
// sur l'ancien OutboxStreamConsumerService NestJS.
func DefaultConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		Stream:        StreamKey,
		Group:         "push-workers",
		Consumer:      "push-worker",
		MaxAttempts:   5,
		Block:         5 * time.Second,
		RetryDelay:    2 * time.Second,
		MaxRetryDelay: 30 * time.Second,
		BatchSize:     10,
	}
}

// Consumer lit le stream `push-jobs` via un groupe de consumers et confie
// chaque job au JobHandler.
//
// À chaque tour, il relit d'abord ses entrées pending (ID "0" : délivrées
// mais pas encore ackées, ex: job en échec ou crash du worker), puis attend
// les nouvelles (">"). Une entrée est ackée quand elle est traitée, quand son
// JSON est invalide (poison message) ou après MaxAttempts échecs. Entre deux
// tentatives, le consumer attend RetryDelay * 2^(tentative-1) (plafonné à
// MaxRetryDelay) : avec la config par défaut, un job est rejoué pendant ~30s,
// le temps que le backend redémarre.
//
// Le compteur de tentatives est gardé en mémoire (comme côté NestJS) : il
// n'est utilisé que par la boucle de Run, donc sans accès concurrent. Après
// un redémarrage, une entrée pending repart de zéro tentative.
type Consumer struct {
	stream   StreamClient
	handler  JobHandler
	opts     ConsumerOptions
	attempts map[string]int
}

// NewConsumer crée un Consumer.
func NewConsumer(stream StreamClient, handler JobHandler, opts ConsumerOptions) *Consumer {
	return &Consumer{
		stream:   stream,
		handler:  handler,
		opts:     opts,
		attempts: make(map[string]int),
	}
}

// Run consomme le stream jusqu'à l'annulation de `ctx`. Le job en cours est
// terminé avant de rendre la main (il n'est pas interrompu à mi-envoi, pour
// éviter de renvoyer la notification aux devices déjà servis au redémarrage).
func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := c.stream.EnsureConsumerGroup(ctx, c.opts.Stream, c.opts.Group); err != nil {
			log.Printf("push: création du groupe %q sur %q impossible: %v", c.opts.Group, c.opts.Stream, err)
			c.sleep(ctx, c.opts.RetryDelay)
			continue
		}
		break
	}
	log.Printf("push: consumer %q démarré sur le stream %q", c.opts.Consumer, c.opts.Stream)

	for ctx.Err() == nil {
		if err := c.poll(ctx); err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("push: erreur de lecture du stream %q: %v", c.opts.Stream, err)
			if strings.Contains(err.Error(), "NOGROUP") {
				// Stream ou groupe supprimé (ex: FLUSHALL) : on le recrée.
				if err := c.stream.EnsureConsumerGroup(ctx, c.opts.Stream, c.opts.Group); err != nil {
					log.Printf("push: recréation du groupe impossible: %v", err)
				}
			}
			c.sleep(ctx, c.opts.RetryDelay)
		}
	}
	log.Printf("push: consumer %q arrêté", c.opts.Consumer)
}

// poll effectue un tour : pending d'abord, nouvelles entrées sinon.
func (c *Consumer) poll(ctx context.Context) error {
	pending, err := c.stream.ReadGroup(ctx, c.opts.Stream, c.opts.Group, c.opts.Consumer, "0", c.opts.BatchSize, -1)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		pending, err = c.stream.ReadGroup(ctx, c.opts.Stream, c.opts.Group, c.opts.Consumer, ">", c.opts.BatchSize, c.opts.Block)
		if err != nil {
			return err
		}
	}

	if attempts := c.processAll(ctx, pending); attempts > 0 {
		// Laisse au backend le temps de revenir avant de rejouer l'entrée.
		c.sleep(ctx, c.backoff(attempts))
	}
	return nil
}

// processAll traite les entrées et retourne le plus grand nombre de
// tentatives parmi celles restées en échec (0 si aucune n'est à rejouer).
//
// À l'arrêt (ctx annulé), l'entrée en cours de traitement va jusqu'au bout
// (handleEntry utilise context.WithoutCancel pour l'envoi et l'ack), mais les
// entrées suivantes du batch ne sont pas démarrées : ça borne le temps entre
// un SIGTERM et l'arrêt réel au traitement d'une seule entrée au lieu d'un
// batch entier (jusqu'à BatchSize), pour rester sous le délai de grâce du
// orchestrateur (ex: 10s par défaut pour `docker stop`).
func (c *Consumer) processAll(ctx context.Context, entries []redis.XMessage) int {
	maxAttempts := 0
	for _, entry := range entries {
		maxAttempts = max(maxAttempts, c.handleEntry(ctx, entry))
		if ctx.Err() != nil {
			break
		}
	}
	return maxAttempts
}

// backoff retourne la pause avant la tentative suivante d'une entrée déjà
// tentée `attempts` fois.
func (c *Consumer) backoff(attempts int) time.Duration {
	delay := c.opts.RetryDelay
	for i := 1; i < attempts && delay < c.opts.MaxRetryDelay; i++ {
		delay *= 2
	}
	if c.opts.MaxRetryDelay > 0 && delay > c.opts.MaxRetryDelay {
		return c.opts.MaxRetryDelay
	}
	return delay
}

// handleEntry traite une entrée. Elle retourne 0 si l'entrée a été ackée
// (succès, poison message ou abandon), sinon son nombre de tentatives (elle
// reste pending et sera rejouée).
func (c *Consumer) handleEntry(ctx context.Context, entry redis.XMessage) int {
	job, err := decodeJob(entry)
	if err != nil {
		log.Printf("push: entrée %s invalide (poison message), ack sans traitement: %v", entry.ID, err)
		c.ack(ctx, entry.ID)
		return 0
	}

	// Le job en cours n'est pas annulé par l'arrêt du worker : les appels
	// restent bornés par les timeouts HTTP et d'envoi.
	if err := c.handler.Handle(context.WithoutCancel(ctx), job); err != nil {
		attempts := c.attempts[entry.ID] + 1
		if attempts >= c.opts.MaxAttempts {
			log.Printf("push: abandon de l'entrée %s après %d tentatives, ack pour débloquer le stream: %v",
				entry.ID, attempts, err)
			c.ack(ctx, entry.ID)
			delete(c.attempts, entry.ID)
			return 0
		}
		c.attempts[entry.ID] = attempts
		log.Printf("push: échec de l'entrée %s (tentative %d/%d): %v", entry.ID, attempts, c.opts.MaxAttempts, err)
		return attempts
	}

	c.ack(ctx, entry.ID)
	delete(c.attempts, entry.ID)
	return 0
}

func (c *Consumer) ack(ctx context.Context, id string) {
	if err := c.stream.Ack(context.WithoutCancel(ctx), c.opts.Stream, c.opts.Group, id); err != nil {
		log.Printf("push: ack de l'entrée %s impossible: %v", id, err)
	}
}

func (c *Consumer) sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func decodeJob(entry redis.XMessage) (model.PushJob, error) {
	var job model.PushJob
	raw, ok := entry.Values[payloadField].(string)
	if !ok {
		return job, errors.New("champ \"payload\" absent")
	}
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		return job, err
	}
	return job, nil
}
