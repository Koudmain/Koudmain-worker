package repository

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

type RedisRepository struct {
	Client *redis.Client
}

func (r *RedisRepository) Subscribe(ctx context.Context, channel string) <-chan *redis.Message {
	pubsub := r.Client.Subscribe(ctx, channel)
	return pubsub.Channel()
}

func NewRedisRepository(host, port, password string) (*RedisRepository, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		Password: password,
		DB:       0,
	})

	ctx := context.Background()
	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, err
	}

	return &RedisRepository{Client: client}, nil
}