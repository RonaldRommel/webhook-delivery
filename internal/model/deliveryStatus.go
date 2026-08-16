package model

import "time"

type DeliveryStatus struct {
	EventId       string     `json:"event_id"`
	AppId         string     `json:"app_id"`
	State         string     `json:"state"`
	Error         *string    `json:"error"`
	AttemptNumber int        `json:"attempt_count"`
	NextRetryAt   *time.Time `json:"next_retry_at"`
	CreatedAt     time.Time  `json:"sent_at"`
	ReceivedAt    *time.Time `json:"received_at"`
}
