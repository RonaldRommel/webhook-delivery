package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// Routes builds the chi router with all middleware and routes mounted.
func (a *API) Routes() http.Handler {
	r := chi.NewRouter()

	r.Get("/health", a.handleHealth)
	r.Post("/register",a.handleRegister)
	r.Post("/event",a.handleEvent)
	r.Get("/event/{eventId}/status",a.handleGetEventStatus)


	return r
}
