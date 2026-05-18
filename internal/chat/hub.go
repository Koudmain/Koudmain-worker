package chat

import (
	"sync"

	"github.com/gorilla/websocket"
)

// Conn est l'interface minimale attendue par le Hub pour représenter
// une connexion WebSocket. Elle permet de découpler le Hub de
// l'implémentation concrète `*websocket.Conn` et facilite les tests.
type Conn interface {
	Close() error
	WriteMessage(messageType int, data []byte) error
}

// Hub gère les connexions clients identifiées par un userID.
// Il protège l'accès concurrent à la map `Clients` via un mutex RW.
type Hub struct {
	Clients map[int]Conn
	mu      sync.RWMutex
}

// NewHub crée et initialise un Hub prêt à enregistrer des connexions.
//
// Retour:
//   - *Hub: instance initialisée du Hub avec la map `Clients` prête.
func NewHub() *Hub {
	return &Hub{
		Clients: make(map[int]Conn),
	}
}

// Register enregistre la connexion `conn` pour l'utilisateur `userID`.
//
// Arguments:
//   - userID: identifiant unique de l'utilisateur.
//   - conn: connexion implémentant l'interface `Conn`.
//
// Effets de bord:
//   - modifie la map interne `Clients` (protégée par mutex).
func (h *Hub) Register(userID int, conn Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Clients[userID] = conn
}

// Unregister supprime la connexion associée à `userID` et ferme la connexion
// si elle existe.
//
// Arguments:
//   - userID: identifiant unique de l'utilisateur à désenregistrer.
//
// Effets de bord:
//   - ferme la connexion via `Close()` puis supprime l'entrée dans `Clients`.
func (h *Hub) Unregister(userID int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if conn, ok := h.Clients[userID]; ok {
		conn.Close()
		delete(h.Clients, userID)
	}
}

// SendToUser envoie `data` au client identifié par `userID` si celui-ci est
// enregistré.
//
// Arguments:
//   - userID: identifiant du destinataire.
//   - data: payload à envoyer (bytes).
//
// Retour:
//   - aucune valeur retournée; l'envoi est effectué de façon synchrone.
//
// Remarque : l'erreur renvoyée par `WriteMessage` n'est pas remontée
// actuellement — on peut envisager de gérer l'erreur et de désenregistrer
// le client si l'envoi échoue.
func (h *Hub) SendToUser(userID int, data []byte) {
	h.mu.RLock()
	client, ok := h.Clients[userID]
	h.mu.RUnlock()

	if ok {
		client.WriteMessage(websocket.TextMessage, data)
	}
}
