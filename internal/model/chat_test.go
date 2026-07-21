package model

import (
	"encoding/json"
	"testing"
)

func TestChatMessageJSON(t *testing.T) {
	msg := ChatMessage{
		ID:             1,
		ConversationID: 2,
		SenderID:       3,
		ReceiverID:     4,
		ContentText:    "hello",
		MessageType:    "text",
		CreatedAt:      "2026-05-18T12:00:00Z",
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var got ChatMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if got.ID != msg.ID || got.ContentText != msg.ContentText {
		t.Fatal("json round-trip mismatch")
	}
}
