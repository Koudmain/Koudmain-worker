package model

import "time"

type WSMessage struct {
    ID             int       `json:"id"`
    ConversationID int       `json:"conversation_id"`
    SenderID       int       `json:"sender_id"`
    ContentText    string    `json:"content_text"`
    MessageType    string    `json:"message_type"`
    CreatedAt      time.Time `json:"created_at"`
}