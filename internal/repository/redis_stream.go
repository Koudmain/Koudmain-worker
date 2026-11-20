package repository

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// EnsureConsumerGroup crée le groupe de consumers `group` sur le stream
// `stream` (en créant le stream s'il n'existe pas, MKSTREAM). Le groupe
// démarre à l'ID "0" pour ne perdre aucune entrée publiée avant sa création.
// L'erreur BUSYGROUP (groupe déjà existant) est ignorée.
func (r *RedisRepository) EnsureConsumerGroup(ctx context.Context, stream, group string) error {
	err := r.Client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// ReadGroup lit jusqu'à `count` entrées du stream pour le consumer donné
// (XREADGROUP).
//
// Arguments:
//   - startID: ">" pour les nouvelles entrées, "0" pour relire les entrées
//     déjà délivrées à ce consumer mais pas encore ackées (pending).
//   - block: durée maximale d'attente; une valeur négative désactive BLOCK
//     (lecture immédiate, utilisée pour les pending).
//
// Retour: les entrées lues (slice vide si rien n'est disponible).
func (r *RedisRepository) ReadGroup(
	ctx context.Context,
	stream, group, consumer, startID string,
	count int64,
	block time.Duration,
) ([]redis.XMessage, error) {
	res, err := r.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, startID},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	return res[0].Messages, nil
}

// Ack acquitte l'entrée `id` du stream pour le groupe (XACK) afin qu'elle
// sorte de la liste des pending.
func (r *RedisRepository) Ack(ctx context.Context, stream, group, id string) error {
	return r.Client.XAck(ctx, stream, group, id).Err()
}
