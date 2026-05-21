package server

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"koudmain-worker/internal/chat"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
)

// Package server fournit les handlers HTTP/WS utilisés par l'application.
// Il contient notamment la logique d'upgrade WebSocket et d'authentification JWT.

// upgrader est utilisé pour promouvoir une requête HTTP vers une connexion WebSocket.
// La fonction CheckOrigin est permissive ici (retourne toujours true) —
// adapter cette vérification en production si nécessaire pour restreindre les origines.
var upgrader = websocket.Upgrader{
    // CheckOrigin: func(r *http.Request) bool {
    //     origin := r.Header.Get("Origin")
	// 	// En production, on autorise uniquement notre frontend
    //     if origin == "https://monfrontend.com" {
    //         return true
    //     }
    //     return false
    // },
	// #nosec G402 -- Autorisé temporairement en dev local, à restreindre en production
    CheckOrigin: func(_ *http.Request) bool { return true },
}
// ServeWS upgrade la requête HTTP en WebSocket après vérification du JWT.
//
// Arguments:
//   - hub: instance du Hub où enregistrer/désenregistrer la connexion.
//   - w: ResponseWriter HTTP utilisé pour l'upgrade et les réponses d'erreur.
//   - r: requête HTTP entrante contenant la query `token`.
//
// Comportement :
// - lit le paramètre `token` depuis la query string
// - valide le JWT avec la clé `JWT_ACCESS_SECRET` (variable d'environnement)
// - extrait la claim `sub` comme identifiant d'utilisateur (attendu en nombre)
// - effectue l'upgrade WebSocket et enregistre la connexion dans le `hub`
// - la connexion est conservée tant que `ReadMessage` renvoie des messages;
//   en cas d'erreur la boucle se termine et la connexion est désenregistrée.
//
// Retour:
//   - aucun retour direct ; la fonction écrit des réponses HTTP en cas d'erreur
//     (ex. 401 Unauthorized) et gère le cycle de vie de la connexion via le hub.
//
// Effets secondaires : enregistre/annule l'enregistrement du client via `hub.Register` / `hub.Unregister`.
// Note : la fonction renvoie des réponses HTTP 401 pour les cas d'authentification invalides.
func ServeWS(hub *chat.Hub, w http.ResponseWriter, r *http.Request) {
	tokenString := r.URL.Query().Get("token")
	if tokenString == "" {
		http.Error(w, "Token manquant", http.StatusUnauthorized)
		return
	}

	secretStr := os.Getenv("JWT_ACCESS_SECRET")
	if secretStr == "" {
    log.Println("Erreur critique: JWT_ACCESS_SECRET n'est pas configuré")

		http.Error(w, "Internal Server Error", http.StatusInternalServerError)

		return
	}

	secretKey := []byte(secretStr)

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

	var userID int

	switch v := claims["sub"].(type) {
	case float64:
		userID = int(v)
	case string:
		id, err := strconv.Atoi(v)
		if err != nil {
			log.Printf("Impossible de parser le 'sub' string en entier: %v", err)
			http.Error(w, "Unauthorized: invalid user ID format", http.StatusUnauthorized)
			return
		}
		userID = id
	default:
		log.Printf("Champ 'sub' manquant ou type invalide dans le JWT")
		http.Error(w, "Unauthorized: missing or invalid subject", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Erreur upgrade: %v", err)
		return
	}

	hub.Register(userID, conn)
	log.Printf("Utilisateur %d authentifié et connecté", userID)

	defer func() {
		hub.Unregister(userID, conn)
		log.Printf("Utilisateur %d déconnecté", userID)
	}()

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
}