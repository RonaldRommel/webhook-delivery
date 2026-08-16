package model

import "time"

type Event struct {
	EventId string `json:"event_id"`
	EventType string `json:"event_type"`
	Payload map[string]interface{} `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}