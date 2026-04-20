package chat

import (
	"sync"
	"github.com/gorilla/websocket"
)

// Hub est une structure qui va centraliser les connexions.
// Comme en C++, on définit les types des champs.
type Hub struct {
	// Clients est une map (table de hachage).
	// Clé : ID de l'utilisateur (int), Valeur : pointeur vers la connexion WebSocket.
	Clients map[int]*websocket.Conn

	// Lock est un Mutex (standard) pour éviter les "Race Conditions".
	// Go est très strict sur l'accès concurrent aux maps.
	Lock sync.RWMutex
}

/* CONSTRUCTEUR */
func NewHub() *Hub {
	return &Hub{
		Clients: make(map[int]*websocket.Conn),
	}
}

func (h *Hub) Register(userID int, conn *websocket.Conn) {
	h.Lock.Lock()
	defer h.Lock.Unlock() // defer = s'exécute à la sortie de la fonction (comme un destructeur automatique)
	h.Clients[userID] = conn
}

func (h *Hub) Unregister(userID int) {
	h.Lock.Lock()
	defer h.Lock.Unlock()
	if conn, ok := h.Clients[userID]; ok {
		conn.Close()
		delete(h.Clients, userID)
	}
}

func (h *Hub) SendToUser(userID int, data []byte) {
    h.Mu.RLock()
    client, ok := h.Clients[userID]
    h.Mu.RUnlock()

    if ok {
        client.WriteMessage(websocket.TextMessage, data)
    }
}