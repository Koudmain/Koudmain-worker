package repository

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisRepository est un wrapper léger autour du client Redis.
// Il expose des méthodes utilitaires (ex : Subscribe) qui simplifient
// l'interaction avec Redis pour le reste de l'application.
type RedisRepository struct {
	Client *redis.Client
}

// Subscribe s'abonne au channel Redis spécifié et retourne le canal
// fournissant les messages publiés sur ce channel.
//
// Arguments:
//   - ctx: contexte permettant d'annuler l'abonnement (timeout, cancel).
//   - channel: nom du channel Redis auquel s'abonner.
//
// Retour:
//   - <-chan *redis.Message: canal recevant les messages publiés.
//   - func() error: fonction pour fermer proprement l'abonnement et libérer les ressources.
func (r *RedisRepository) Subscribe(ctx context.Context, channel string) (<-chan *redis.Message, func() error) {
	pubsub := r.Client.Subscribe(ctx, channel)
	return pubsub.Channel(), pubsub.Close
}

// NewRedisRepository crée et configure un client Redis pointant sur
// `host:port` avec le mot de passe `password` (peut être vide).
//
// Arguments:
//   - host: adresse IP ou nom d'hôte du serveur Redis.
//   - port: port TCP du serveur Redis.
//   - password: mot de passe Redis (vide si non requis).
//
// Retour:
//   - *RedisRepository: instance initialisée du repository.
//   - error: erreur si la connexion (Ping) échoue.
//
// La fonction effectue un `Ping` de vérification lors de l'initialisation
// et retourne une erreur si la connexion n'est pas possible.
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
