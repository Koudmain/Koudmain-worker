package chat

import (
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// WriteWait est le temps maximal accordé à l'écriture d'un message.
	WriteWait = 10 * time.Second
	// sendBuffer est le nombre de messages en attente par connexion avant de la considérer bloquée.
	sendBuffer = 64
)

// Conn est l'interface minimale attendue par le Hub pour représenter
// une connexion WebSocket. Elle permet de découpler le Hub de
// l'implémentation concrète `*websocket.Conn` et facilite les tests.
type Conn interface {
	Close() error
	WriteMessage(messageType int, data []byte) error
	SetWriteDeadline(t time.Time) error
}

// client associe une connexion à sa file d'envoi : un seul goroutine écrit sur la
// connexion, et un client lent ne bloque jamais l'envoi aux autres.
type client struct {
	conn Conn
	send chan []byte
}

// Hub gère les connexions clients identifiées par un userID.
// Il protège l'accès concurrent à la map `clients` via un mutex RW.
type Hub struct {
	clients map[int][]*client
	mu      sync.RWMutex
}

// NewHub crée et initialise un Hub prêt à enregistrer des connexions.
func NewHub() *Hub {
	return &Hub{
		clients: make(map[int][]*client),
	}
}

// Register enregistre la connexion `conn` pour l'utilisateur `userID` et démarre
// le goroutine qui écrit ses messages.
func (h *Hub) Register(userID int, conn Conn) {
	c := &client{conn: conn, send: make(chan []byte, sendBuffer)}

	h.mu.Lock()
	h.clients[userID] = append(h.clients[userID], c)
	h.mu.Unlock()

	go h.writePump(userID, c)
}

// writePump écrit les messages en file avec un délai maximal : une connexion morte
// ou trop lente est désenregistrée au lieu de rester dans la map.
func (h *Hub) writePump(userID int, c *client) {
	for data := range c.send {
		if err := c.conn.SetWriteDeadline(time.Now().Add(WriteWait)); err == nil {
			err = c.conn.WriteMessage(websocket.TextMessage, data)
			if err == nil {
				continue
			}
			log.Printf("Impossible d'envoyer le message au client %d : %v", userID, err)
		}
		h.Unregister(userID, c.conn)
	}
}

// Unregister supprime une connexion SPÉCIFIQUE associée à `userID` et la ferme.
// Cela évite qu'un onglet qui se ferme ne détruise les connexions des autres onglets ouverts.
// Appeler Unregister plusieurs fois pour la même connexion est sans effet.
func (h *Hub) Unregister(userID int, connToRemove Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	clients := h.clients[userID]
	for i, c := range clients {
		if c.conn == connToRemove {
			close(c.send)
			_ = c.conn.Close()
			h.clients[userID] = append(clients[:i], clients[i+1:]...)
			break
		}
	}

	if len(h.clients[userID]) == 0 {
		delete(h.clients, userID)
	}
}

// SendToUser met `data` dans la file de chaque connexion de `userID`, sans jamais
// bloquer : une connexion dont la file est pleine est désenregistrée.
func (h *Hub) SendToUser(userID int, data []byte) {
	var stuck []Conn

	h.mu.RLock()
	for _, c := range h.clients[userID] {
		select {
		case c.send <- data:
		default:
			stuck = append(stuck, c.conn)
		}
	}
	h.mu.RUnlock()

	for _, conn := range stuck {
		log.Printf("File d'envoi pleine pour le client %d : connexion fermée", userID)
		h.Unregister(userID, conn)
	}
}

// ConnectionCount renvoie le nombre de connexions ouvertes pour `userID`.
func (h *Hub) ConnectionCount(userID int) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients[userID])
}
