package event

import (
	"context"
	"webhook-delivery/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Event struct{
	pool *pgxpool.Pool
}

func NewEvent(pl *pgxpool.Pool) *Event {
	return &Event{pool:pl}
}

func (e *Event) InsertEvent(ctx context.Context, event model.Event) error {
	_, err := e.pool.Exec(ctx,
		`INSERT INTO events (event_id, event_type, payload, created_at)
		 VALUES ($1, $2, $3, $4)`,
		event.EventId, event.EventType, event.Payload, event.CreatedAt)
	if err != nil {
		return err
	}
	return nil
}