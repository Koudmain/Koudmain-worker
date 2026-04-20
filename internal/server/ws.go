package server

import (
	"net/http"
	"github.com/gorilla/websocket"
	"log"
)

// Upgrader configure la transition HTTP -> WebSocket
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func ServeWS(hub *chat.Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Erreur upgrade: %v", err)
		return
	}

	// 2. Récupérer l'ID utilisateur (pour le test, on le passe en query param ?userId=1)
	// Plus tard, on décodera le JWT ici.
	userIDStr := r.URL.Query().Get("userId")

	hub.Register(userID, conn)

	log.Printf("Utilisateur %d connecté via WebSocket", userID)
}