package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"koudmain-worker/internal/repository"
)

func main() {
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisPassword := os.Getenv("REDIS_PASSWORD")

	if redisHost == "" { redisHost = "redis" }
	if redisPort == "" { redisPort = "6379" }

	redisRepo, err := repository.NewRedisRepository(redisHost, redisPort, redisPassword)
	if err != nil {
		log.Fatalf("Erreur de connexion Redis : %v", err)
	}

	fmt.Println("Worker Go connecté à Redis avec succès !")

	ctx := context.Background()

	go func() {
		msgs := redisRepo.Subscribe(ctx, "chat:messages")

		for msg := range msgs {
			fmt.Printf("Nouveau message reçu sur [%s]: %s\n", msg.Channel, msg.Payload)
		}
	}()

    select {}
}