package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "strconv"

    "koudmain-worker/internal/chat"
    "koudmain-worker/internal/model"
    "koudmain-worker/internal/repository"
    "github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
    CheckOrigin: func(r *http.Request) bool { return true }, // Equivalent CORS *
}

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

    myHub := chat.NewHub()

    go func() {
        ctx := context.Background()
        msgs := redisRepo.Subscribe(ctx, "chat:messages")
        for msg := range msgs {
            var chatMsg model.ChatMessage
            json.Unmarshal([]byte(msg.Payload), &chatMsg)

            myHub.SendToUser(chatMsg.SenderID, []byte(msg.Payload))
        }
    }()

    http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
        conn, err := upgrader.Upgrade(w, r, nil)
        if err != nil { return }

        userID, _ := strconv.Atoi(r.URL.Query().Get("userId"))

        myHub.Register(userID, conn)
        fmt.Printf("Utilisateur %d connecté !\n", userID)
    })

    fmt.Println("Worker démarré sur :4000")
    log.Fatal(http.ListenAndServe(":4000", nil))
}