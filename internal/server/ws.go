package server

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"koudmain-worker/internal/chat"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func ServeWS(hub *chat.Hub, w http.ResponseWriter, r *http.Request) {
	tokenString := r.URL.Query().Get("token")
	if tokenString == "" {
		http.Error(w, "Token manquant", http.StatusUnauthorized)
		return
	}

	secretKey := []byte(os.Getenv("JWT_ACCESS_SECRET"))

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("méthode de signature inattendue: %v", token.Header["alg"])
		}
		return secretKey, nil
	})

	if err != nil || !token.Valid {
		log.Printf("JWT invalide: %v", err)
		http.Error(w, "Non autorisé", http.StatusUnauthorized)
		return
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		http.Error(w, "Claims invalides", http.StatusUnauthorized)
		return
	}

	userIDFloat, ok := claims["sub"].(string)
	if !ok {
		log.Printf("Champ 'sub' manquant ou n'est pas une string dans le JWT")
		return
	}
	userID := int(userIDFloat)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Erreur upgrade: %v", err)
		return
	}

	hub.Register(userID, conn)
	log.Printf("Utilisateur %s authentifié et connecté", userID)

	defer func() {
		hub.Unregister(userID)
		log.Printf("Utilisateur %d déconnecté", userID)
	}()

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
}