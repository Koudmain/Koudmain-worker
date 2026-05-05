package chat

import (
	"sync"

	"github.com/gorilla/websocket"
)

type Hub struct {
	Clients map[string]*websocket.Conn
	mu      sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		Clients: make(map[string]*websocket.Conn),
	}
}

func (h *Hub) Register(userID string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Clients[userID] = conn
}

func (h *Hub) Unregister(userID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if conn, ok := h.Clients[userID]; ok {
		conn.Close()
		delete(h.Clients, userID)
	}
}

func (h *Hub) SendToUser(userID string, data []byte) {
	h.mu.RLock()
	client, ok := h.Clients[userID]
	h.mu.RUnlock()

	if ok {
		client.WriteMessage(websocket.TextMessage, data)
	}
}
