package api

import (
	"webhook-delivery/internal/delivery"
	"webhook-delivery/internal/event"
	"webhook-delivery/internal/registry"
)

// API holds shared dependencies for the HTTP handlers.
type API struct {
	registry *registry.Registry
	delivery *delivery.Delivery
	event    *event.Event
}

// New returns an API with the given dependencies.
func New(reg *registry.Registry, del *delivery.Delivery, evt *event.Event) *API {
	return &API{registry: reg, delivery: del, event: evt}
}
