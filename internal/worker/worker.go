package worker

import (
	"context"
	"encoding/json"
	"log"
	"time"
	"webhook-delivery/internal/delivery"
	"webhook-delivery/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Worker struct {
	pool     *pgxpool.Pool
	delivery *delivery.Delivery
}

func NewWorker(pool *pgxpool.Pool, d *delivery.Delivery) *Worker {
	return &Worker{pool: pool, delivery: d}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.processDueRetries(ctx)
		case <-ctx.Done():
			return
		}
	}
}

type dueRow struct {
	event        model.Event
	app          model.App
	attemptCount int
}

func (w *Worker) processDueRetries(ctx context.Context) {
	rows, err := w.pool.Query(ctx, `
		SELECT e.event_id, e.event_type, e.payload, a.app_id, a.url, ds.attempt_count
		FROM delivery_status ds
		JOIN events e ON ds.event_id = e.event_id
		JOIN app_registrations a ON ds.app_id = a.app_id
		WHERE ds.state in ('retry_later','pending') AND ds.next_retry_at <= now()`)
	if err != nil {
		log.Println("failed to query due retries:", err)
		return
	}

	var due []dueRow
	for rows.Next() {
		var r dueRow
		if err := rows.Scan(
			&r.event.EventId, &r.event.EventType, &r.event.Payload,
			&r.app.AppId, &r.app.Url,
			&r.attemptCount,
		); err != nil {
			log.Println("failed to scan due retry row:", err)
			continue
		}
		due = append(due, r)
	}
	if err := rows.Err(); err != nil {
		log.Println("error iterating due retries:", err)
	}
	rows.Close() // release the connection/cursor before making HTTP calls

	for _, r := range due {
		body, err := json.Marshal(r.event)
		if err != nil {
			log.Println("failed to marshal event for retry:", err)
			continue
		}
		if err := w.delivery.DeliverToApp(ctx, r.event, r.app, r.attemptCount+1, body); err != nil {
			log.Println("retry attempt failed:", err)
		}
	}
}
