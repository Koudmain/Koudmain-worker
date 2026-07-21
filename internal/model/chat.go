package model

// ChatMessage représente un message échangé dans une conversation entre deux utilisateurs.
type ChatMessage struct {
	ID             int    `json:"id"`
	ConversationID int    `json:"conversation_id"`
	SenderID       int    `json:"sender_id"`
	ReceiverID     int    `json:"receiver_id"`
	ContentText    string `json:"content_text"`
	MessageType    string `json:"message_type"`
	CreatedAt      string `json:"created_at"`
}
