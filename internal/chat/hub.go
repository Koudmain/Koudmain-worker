package chat

import (
	"github.com/gorilla/websocket"
	"log"
	"sync"
)

// Conn est l'interface minimale attendue par le Hub pour représenter
// une connexion WebSocket. Elle permet de découpler le Hub de
// l'implémentation concrète `*websocket.Conn` et facilite les tests.
type Conn interface {
	Close() error
	WriteMessage(messageType int, data []byte) error
}

// Hub gère les connexions clients identifiées par un userID.
// Il protège l'accès concurrent à la map `clients` via un mutex RW.
type Hub struct {
	clients map[int][]Conn
	mu      sync.RWMutex
}

// NewHub crée et initialise un Hub prêt à enregistrer des connexions.
//
// Retour:
//   - *Hub: instance initialisée du Hub avec la map `clients` prête.
func NewHub() *Hub {
	return &Hub{
		clients: make(map[int][]Conn),
	}
}

// Register enregistre la connexion `conn` pour l'utilisateur `userID`.
//
// Arguments:
//   - userID: identifiant unique de l'utilisateur.
//   - conn: connexion implémentant l'interface `Conn`.
//
// Effets de bord:
//   - modifie la map interne `clients` (protégée par mutex).
func (h *Hub) Register(userID int, conn Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[userID] = append(h.clients[userID], conn)
}

// Unregister supprime une connexion SPÉCIFIQUE associée à `userID` et la ferme.
// Cela évite qu'un onglet qui se ferme ne détruise les connexions des autres onglets ouverts.
//
// Arguments:
//   - userID: identifiant unique de l'utilisateur à désenregistrer.
//   - connToRemove: la connexion précise à fermer et retirer.
func (h *Hub) Unregister(userID int, connToRemove Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	conns, ok := h.clients[userID]
	if !ok {
		return
	}

	for i, conn := range conns {
		if conn == connToRemove {
			_ = conn.Close()

			h.clients[userID] = append(conns[:i], conns[i+1:]...)
			break
		}
	}

	// 3. Si l'utilisateur n'a plus aucun onglet/appareil connecté, on nettoie la map
	if len(h.clients[userID]) == 0 {
		delete(h.clients, userID)
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
func (h *Hub) SendToUser(userID int, data []byte) {
	h.mu.RLock()
	conns, ok := h.clients[userID]
	if !ok || len(conns) == 0 {
		h.mu.RUnlock()
		return
	}

	connsCopy := append([]Conn(nil), conns...)
	h.mu.RUnlock()

	for _, conn := range connsCopy {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Printf("Impossible d'envoyer le message au client %d sur une de ses connexions : %v", userID, err)
		}
	}
}
