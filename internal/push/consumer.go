package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
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
	StalePending(ctx context.Context, stream, group string, minIdle time.Duration, count int64) ([]redis.XPendingExt, error)
	Claim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, ids []string) ([]redis.XMessage, error)
	Ack(ctx context.Context, stream, group, id string) error
}

// JobHandler traite un PushJob. Une erreur retournée déclenche un retry.
type JobHandler interface {
	Handle(ctx context.Context, job model.PushJob) error
}

// ConsumerOptions paramètre le Consumer.
type ConsumerOptions struct {
	Stream string
	Group  string
	// Consumer doit être unique par réplica (voir defaultConsumerName).
	Consumer string
	// MaxAttempts est le nombre de tentatives avant d'abandonner (et d'acker)
	// une entrée pour ne pas bloquer le stream. Les échecs transitoires du
	// backend (voir isTransient) ne mènent jamais à l'abandon.
	MaxAttempts int
	// Block est la durée d'attente de XREADGROUP sur les nouvelles entrées.
	Block time.Duration
	// RetryDelay est la pause après une erreur Redis.
	RetryDelay time.Duration
	// RetryAfterIdle : une entrée non ackée depuis ce délai (job en échec, ou
	// réplica arrêté en plein travail) est reprise par n'importe quel réplica.
	// C'est aussi le délai entre deux tentatives d'un job en échec. Il doit
	// dépasser la durée du plus long job, sinon un réplica peut reprendre un
	// job encore en cours chez un autre.
	RetryAfterIdle time.Duration
	// BatchSize est le nombre maximum d'entrées lues ou reprises par tour.
	BatchSize int64
}

// DefaultConsumerOptions retourne la configuration de production.
func DefaultConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		Stream:      StreamKey,
		Group:       "push-workers",
		Consumer:    defaultConsumerName(),
		MaxAttempts: 5,
		Block:       5 * time.Second,
		RetryDelay:  2 * time.Second,
		// shortcut: un job de plus de 2 min (beaucoup d'appareils et des
		// providers qui atteignent tous leur timeout) peut être repris par un
		// autre réplica et renvoyé ; augmenter ce délai si ça arrive.
		RetryAfterIdle: 2 * time.Minute,
		BatchSize:      10,
	}
}

// defaultConsumerName est unique par réplica : Redis tient la liste des
// entrées en cours (PEL) par nom de consumer, et un nom partagé faisait
// traiter la même entrée par deux réplicas (scaling, rolling deploy). Dans
// Docker, le hostname est l'id du conteneur ; le PID distingue deux process
// lancés sur la même machine.
func defaultConsumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "push-worker"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// Consumer lit le stream `push-jobs` via un groupe de consumers et confie
// chaque job au JobHandler.
//
// À chaque tour, il reprend d'abord les entrées non ackées depuis
// RetryAfterIdle, quel que soit le réplica qui les avait lues (job en échec,
// ou réplica arrêté en plein travail), puis attend les nouvelles (">"). Une
// entrée est ackée quand elle est traitée, quand son JSON est invalide
// (poison message) ou après MaxAttempts échecs définitifs.
//
// Le nombre de tentatives est celui que Redis tient pour chaque entrée
// pending : il survit aux redémarrages et reste juste quand une entrée passe
// d'un réplica à l'autre.
type Consumer struct {
	stream  StreamClient
	handler JobHandler
	opts    ConsumerOptions
}

// NewConsumer crée un Consumer.
func NewConsumer(stream StreamClient, handler JobHandler, opts ConsumerOptions) *Consumer {
	return &Consumer{stream: stream, handler: handler, opts: opts}
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

// poll effectue un tour : entrées à reprendre d'abord, nouvelles entrées sinon.
func (c *Consumer) poll(ctx context.Context) error {
	retried, err := c.retryStaleEntries(ctx)
	if err != nil || retried {
		return err
	}

	entries, err := c.stream.ReadGroup(ctx, c.opts.Stream, c.opts.Group, c.opts.Consumer, ">", c.opts.BatchSize, c.opts.Block)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		c.handleEntry(ctx, entry, 1)
		if ctx.Err() != nil {
			break
		}
	}
	return nil
}

// retryStaleEntries reprend (XCLAIM) les entrées non ackées depuis
// RetryAfterIdle et les retraite. Elle retourne true s'il y en avait.
//
// À l'arrêt (ctx annulé), l'entrée en cours va jusqu'au bout (handleEntry
// utilise context.WithoutCancel), mais les suivantes ne sont pas démarrées :
// le temps entre un SIGTERM et l'arrêt réel reste borné à une seule entrée,
// sous le délai de grâce de l'orchestrateur (10s par défaut pour docker stop).
func (c *Consumer) retryStaleEntries(ctx context.Context) (bool, error) {
	stale, err := c.stream.StalePending(ctx, c.opts.Stream, c.opts.Group, c.opts.RetryAfterIdle, c.opts.BatchSize)
	if err != nil || len(stale) == 0 {
		return false, err
	}

	ids := make([]string, len(stale))
	deliveries := make(map[string]int64, len(stale))
	for i, p := range stale {
		ids[i] = p.ID
		deliveries[p.ID] = p.RetryCount
	}

	entries, err := c.stream.Claim(ctx, c.opts.Stream, c.opts.Group, c.opts.Consumer, c.opts.RetryAfterIdle, ids)
	if err != nil {
		return true, err
	}
	for _, entry := range entries {
		// XCLAIM vient d'ajouter une livraison au compteur Redis.
		c.handleEntry(ctx, entry, int(deliveries[entry.ID])+1)
		if ctx.Err() != nil {
			break
		}
	}
	return true, nil
}

// handleEntry traite une entrée délivrée pour la `attempt`-ième fois. Elle
// l'ack (succès, poison message ou abandon) ou la laisse pending : elle sera
// reprise par retryStaleEntries après RetryAfterIdle.
func (c *Consumer) handleEntry(ctx context.Context, entry redis.XMessage, attempt int) {
	job, err := decodeJob(entry)
	if err != nil {
		log.Printf("push: entrée %s invalide (poison message), ack sans traitement: %v", entry.ID, err)
		c.ack(ctx, entry.ID)
		return
	}

	// Le job en cours n'est pas annulé par l'arrêt du worker : les appels
	// restent bornés par les timeouts HTTP et d'envoi.
	err = c.handler.Handle(context.WithoutCancel(ctx), job)
	switch {
	case err == nil:
		c.ack(ctx, entry.ID)
	case isTransient(err):
		// Backend arrêté ou en redémarrage : on rejoue jusqu'à son retour.
		log.Printf("push: backend indisponible pour l'entrée %s, nouvel essai dans %s: %v",
			entry.ID, c.opts.RetryAfterIdle, err)
	case attempt >= c.opts.MaxAttempts:
		// Le compteur Redis inclut aussi les essais transitoires : un job qui a
		// déjà attendu le backend est abandonné dès son premier échec définitif,
		// ce qui est sans conséquence puisqu'un 4xx ne guérit pas en rejouant.
		log.Printf("push: abandon de l'entrée %s après %d tentatives, ack pour débloquer le stream: %v",
			entry.ID, attempt, err)
		c.ack(ctx, entry.ID)
	default:
		log.Printf("push: échec de l'entrée %s (tentative %d/%d), nouvel essai dans %s: %v",
			entry.ID, attempt, c.opts.MaxAttempts, c.opts.RetryAfterIdle, err)
	}
}

// isTransient indique un échec qui peut réussir plus tard sans rien changer :
// le backend n'a pas répondu (connexion refusée, timeout, DNS pendant son
// démarrage) ou a répondu 5xx. Ces échecs ne comptent pas dans MaxAttempts.
func isTransient(err error) bool {
	var transportErr *url.Error
	if errors.As(err, &transportErr) {
		return true
	}
	var statusErr interface{ StatusCode() int }
	return errors.As(err, &statusErr) && statusErr.StatusCode() >= 500
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
