package api

import (
	"webhook-delivery/internal/delivery"
	"webhook-delivery/internal/event"
	"webhook-delivery/internal/registry"

	"github.com/jackc/pgx/v5/pgxpool"
)

// API holds shared dependencies for the HTTP handlers.
type API struct {
	registry *registry.Registry
	delivery *delivery.Delivery
	event    *event.Event
	pool     *pgxpool.Pool
}

// New returns an API with the given dependencies.
func New(reg *registry.Registry, del *delivery.Delivery, evt *event.Event, pool *pgxpool.Pool) *API {
	return &API{registry: reg, delivery: del, event: evt, pool: pool}
}
