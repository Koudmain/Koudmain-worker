package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"koudmain-worker/internal/chat"
	"koudmain-worker/internal/model"
	"koudmain-worker/internal/push"
	"koudmain-worker/internal/repository"
	"koudmain-worker/internal/server"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// backendTimeout borne chaque appel aux endpoints internes de NestJS.
const backendTimeout = 10 * time.Second

func main() {
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "redis"
	}
	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		redisPort = "6379"
	}
	redisPassword := os.Getenv("REDIS_PASSWORD")

	redisRepo, err := repository.NewRedisRepository(redisHost, redisPort, redisPassword)
	if err != nil {
		log.Fatalf("Erreur Redis : %v", err)
	}
	fmt.Println("Connecté à Redis")

	// ctx est annulé sur SIGINT/SIGTERM : il pilote l'arrêt propre du serveur
	// HTTP et du consumer push.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	myHub := chat.NewHub()

	go func() {
		msgs, closeSubscription := redisRepo.Subscribe(ctx, "chat:messages")

		defer func() {
			if err := closeSubscription(); err != nil {
				log.Printf("Erreur lors de la fermeture de l'abonnement Redis: %v", err)
			}
		}()

		for msg := range msgs {
			var chatMsg model.ChatMessage
			if err := json.Unmarshal([]byte(msg.Payload), &chatMsg); err != nil {
				log.Printf("Erreur JSON: %v", err)
				continue
			}

			log.Printf("Routing message to User %d", chatMsg.ReceiverID)
			myHub.SendToUser(chatMsg.ReceiverID, []byte(msg.Payload))
			myHub.SendToUser(chatMsg.SenderID, []byte(msg.Payload))
		}
	}()

	var workers sync.WaitGroup
	pushConsumer := newPushConsumer(redisRepo)
	workers.Go(func() {
		pushConsumer.Run(ctx)
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		server.ServeWS(myHub, w, r)
	})

	fmt.Println("Worker démarré sur le port :4000")
	httpServer := &http.Server{
		Addr:         ":4000",
		Handler:      nil,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Erreur lors de l'arrêt du serveur HTTP: %v", err)
		}
	}()

	serveErr := httpServer.ListenAndServe()

	// Arrêt du serveur (signal ou erreur) : on arrête aussi le consumer push
	// et on attend la fin du job en cours.
	stop()
	log.Println("Arrêt du worker : attente de la fin du job push en cours")
	workers.Wait()

	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		log.Fatal(serveErr)
	}
}

// newPushConsumer construit le consumer du stream `push-jobs` à partir des
// variables d'environnement. Les credentials FCM/APNs absents ou invalides ne
// bloquent pas le démarrage (voir push.NewSenders).
func newPushConsumer(redisRepo *repository.RedisRepository) *push.Consumer {
	backendURL := os.Getenv("BACKEND_INTERNAL_URL")
	if backendURL == "" {
		backendURL = "http://backend:3000"
	}
	internalSecret := os.Getenv("INTERNAL_SERVICE_SECRET")
	if internalSecret == "" {
		log.Println("WARNING: INTERNAL_SERVICE_SECRET absent, les appels internes au backend seront refusés")
	}

	// context.Background() : les clients des providers (dont le refresh du
	// token OAuth Firebase) doivent rester utilisables pendant l'arrêt, le
	// temps de finir le job en cours.
	senders := push.NewSenders(context.Background(), push.Config{
		FirebaseServiceAccountJSON: os.Getenv("FIREBASE_SERVICE_ACCOUNT_JSON"),
		APNs: push.APNsConfig{
			Key:        os.Getenv("APNS_KEY"),
			KeyID:      os.Getenv("APNS_KEY_ID"),
			TeamID:     os.Getenv("APNS_TEAM_ID"),
			BundleID:   os.Getenv("APNS_BUNDLE_ID"),
			Production: os.Getenv("APNS_PRODUCTION") == "true",
		},
	})

	backend := repository.NewBackendClient(backendURL, internalSecret, backendTimeout)
	dispatcher := push.NewDispatcher(backend, senders)
	return push.NewConsumer(redisRepo, dispatcher, push.DefaultConsumerOptions())
}
