package model

type ChatMessage struct {
    ID             int    `json:"id"`
    ConversationID int    `json:"conversation_id"`
    SenderID       string    `json:"sender_id"`
    ReceiverID     string    `json:"receiver_id"`
    ContentText    string `json:"content_text"`
    MessageType    string `json:"message_type"`
    CreatedAt      string `json:"created_at"`
}