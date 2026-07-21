package main

import (
	"context"
	"encoding/json"
	"fmt"
	"koudmain-worker/internal/chat"
	"koudmain-worker/internal/model"
	"koudmain-worker/internal/repository"
	"koudmain-worker/internal/server"
	"log"
	"net/http"
	"os"
	"time"
)

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

	myHub := chat.NewHub()

	go func() {
		ctx := context.Background()

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
	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
