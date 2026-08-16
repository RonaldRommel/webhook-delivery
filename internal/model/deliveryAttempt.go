package model

import "time"

type DeliveryAttempt struct {
	AttemptId     int64      `json:"attempt_id"`
	EventId       string     `json:"event_id"`
	AppId         string     `json:"app_id"`
	AttemptNumber int        `json:"attempt_number"`
	State         string     `json:"state"`
	Error         *string    `json:"error,omitempty"`
	SentAt        time.Time  `json:"sent_at"`
	ReceivedAt    *time.Time `json:"received_at,omitempty"`
}