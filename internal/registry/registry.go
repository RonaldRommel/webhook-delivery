package registry

import (
	"errors"
	"context"
	"webhook-delivery/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrDuplicate = errors.New("registration already exists")

type Registry struct {
	pool *pgxpool.Pool
}

func NewRegistry(pool *pgxpool.Pool) *Registry {
	return &Registry{pool: pool}
}

func (r *Registry) RegisterApp(ctx context.Context, app model.App) error {
	tag, err := r.pool.Exec(ctx,
		`INSERT INTO app_registrations (app_id, event_type, url)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (event_type, url) DO NOTHING`,
		app.AppId, app.EventType, app.Url,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrDuplicate
	}
	return nil
}

func (r *Registry) GetAppByEventType(ctx context.Context, eventType string) ([]model.App, error) {
	rows, err := r.pool.Query(ctx,
		"SELECT app_id, event_type, url FROM app_registrations WHERE event_type = $1",
		eventType,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.App
	for rows.Next() {
		var app model.App
		if err := rows.Scan(&app.AppId, &app.EventType, &app.Url); err != nil {
			return nil, err
		}
		result = append(result, app)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}