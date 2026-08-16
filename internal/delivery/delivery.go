package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"
	"webhook-delivery/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Delivery struct {
	client *http.Client
	pool   *pgxpool.Pool
}

func NewDelivery(dbpool *pgxpool.Pool) *Delivery {
	return &Delivery{
		client: &http.Client{
			Timeout: 3 * time.Second,
		},
		pool: dbpool,
	}
}

func (d *Delivery) DeliverToApp(ctx context.Context, event model.Event, app model.App, attemptNumber int, body []byte) error {
	resp, err := d.client.Post(app.Url, "application/json", bytes.NewBuffer(body))
	rtime := time.Now()
	nextRetryAt := rtime.Add(time.Duration(60*math.Pow(2, float64(attemptNumber-1))) * time.Second)
	deliveryStatus := model.DeliveryStatus{
		EventId:       event.EventId,
		AppId:         app.AppId,
		State:         "retry_later",
		Error:         nil,
		AttemptNumber: attemptNumber,
		NextRetryAt:   nil,
		CreatedAt:     time.Now(),
		ReceivedAt:    nil,
	}
	deliveryAttempt := model.DeliveryAttempt{
		EventId:       event.EventId,
		AppId:         app.AppId,
		AttemptNumber: attemptNumber,
		State:         "failed",
		Error:         nil,
		SentAt:        time.Now(),
		ReceivedAt:    nil,
	}
	// no response received
	if err != nil {
		if attemptNumber >= 5 {
			deliveryStatus.State = "dead"
			deliveryAttempt.State = "failed"
		} else {
			deliveryStatus.NextRetryAt = &nextRetryAt
		}
		errMsg := err.Error()
		deliveryStatus.Error = &errMsg
		deliveryAttempt.Error = &errMsg
		if err := d.InsertDeliveryStatus(ctx, deliveryStatus); err != nil {
			fmt.Println("failed to insert delivery status:", err) // or proper logging
		}
		if err := d.InsertDeliveryAttempt(ctx, deliveryAttempt); err != nil {
			fmt.Println("failed to insert delivery attempt:", err)
		}
		return err
	}
	defer resp.Body.Close()
	// Check for non-200 status codes
	if resp.StatusCode != http.StatusOK {
		// Handle 4xx and 5xx responses - no retry for 4xx, retry for 5xx
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			deliveryStatus.State = "failed"
			errMsg := fmt.Sprintf("received 4xx response: %d", resp.StatusCode)
			deliveryStatus.Error = &errMsg
			deliveryAttempt.Error = &errMsg
		} else {
			if attemptNumber >= 5 {
				deliveryStatus.State = "dead"
				deliveryAttempt.State = "failed"
			} else {
				deliveryStatus.NextRetryAt = &nextRetryAt
			}
			errMsg := fmt.Sprintf("received non-200 response: %d", resp.StatusCode)
			deliveryStatus.Error = &errMsg
			deliveryStatus.ReceivedAt = &rtime
			deliveryAttempt.Error = &errMsg
			deliveryAttempt.ReceivedAt = &rtime
		}
		if err := d.InsertDeliveryStatus(ctx, deliveryStatus); err != nil {
			fmt.Println("failed to insert delivery status:", err) // or proper logging
		}
		if err := d.InsertDeliveryAttempt(ctx, deliveryAttempt); err != nil {
			fmt.Println("failed to insert delivery attempt:", err)
		}
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	// successful delivery
	deliveryStatus.State = "success"
	deliveryStatus.ReceivedAt = &rtime
	deliveryAttempt.State = "success"
	deliveryAttempt.ReceivedAt = &rtime
	if err := d.InsertDeliveryStatus(ctx, deliveryStatus); err != nil {
		fmt.Println("failed to insert delivery status:", err) // or proper logging
	}
	if err := d.InsertDeliveryAttempt(ctx, deliveryAttempt); err != nil {
		fmt.Println("failed to insert delivery attempt:", err)
	}
	return nil
}

func (d *Delivery) DeliverEvent(event model.Event, apps []model.App) error {
	ctx := context.Background()
	body, err := json.Marshal(event)

	if err != nil {
		fmt.Println("Error marshalling payload:", err)
		return err
	}

	var failures []string
	for _, app := range apps {

		err := d.DeliverToApp(ctx, event, app, 1, body)
		if err != nil {
			fmt.Println("Error delivering event to app:", err)
			failures = append(failures, fmt.Sprintf("Failed to deliver event to app %s: %v", app.AppId, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("delivery failures: %v", failures)
	}
	return nil
}

func (d *Delivery) GetDeliveryStatus(ctx context.Context, eventId string) ([]model.DeliveryStatus, error) {
	rows, err := d.pool.Query(ctx,
		`SELECT event_id, app_id, state, error, attempt_count, next_retry_at, sent_at, received_at
		 FROM delivery_status WHERE event_id = $1`, eventId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []model.DeliveryStatus
	for rows.Next() {
		var s model.DeliveryStatus
		if err := rows.Scan(&s.EventId, &s.AppId, &s.State, &s.Error, &s.AttemptNumber, &s.NextRetryAt, &s.CreatedAt, &s.ReceivedAt); err != nil {
			return nil, err
		}
		results = append(results, s)
	}
	return results, rows.Err()
}

func (d *Delivery) InsertDeliveryStatus(ctx context.Context, dstatus model.DeliveryStatus) error {
	_, err := d.pool.Exec(ctx,
		`INSERT INTO delivery_status (event_id, app_id, state, error, attempt_count, next_retry_at, sent_at, received_at)
	 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	 ON CONFLICT (event_id, app_id) DO UPDATE SET
	     state         = EXCLUDED.state,
	     error         = EXCLUDED.error,
	     attempt_count = EXCLUDED.attempt_count,
	     next_retry_at = EXCLUDED.next_retry_at,
	     sent_at       = EXCLUDED.sent_at,
	     received_at   = EXCLUDED.received_at`,
		dstatus.EventId, dstatus.AppId, dstatus.State, dstatus.Error, dstatus.AttemptNumber, dstatus.NextRetryAt, dstatus.CreatedAt, dstatus.ReceivedAt)
	if err != nil {
		return err
	}
	return nil
}

func (d *Delivery) InsertDeliveryAttempt(ctx context.Context, dattempt model.DeliveryAttempt) error {
	_, err := d.pool.Exec(ctx,
		`INSERT INTO delivery_attempts (event_id, app_id, attempt_number, state, error, sent_at, received_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		dattempt.EventId, dattempt.AppId, dattempt.AttemptNumber, dattempt.State,
		dattempt.Error, dattempt.SentAt, dattempt.ReceivedAt)
	if err != nil {
		return err
	}
	return nil
}
