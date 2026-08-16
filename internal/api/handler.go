package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
	"webhook-delivery/internal/model"
	"webhook-delivery/internal/registry"

	"github.com/google/uuid"
)

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func (a *API) handleRegister(w http.ResponseWriter, r *http.Request) {
	var app model.App
	err := json.NewDecoder(r.Body).Decode(&app)
	if err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	app.AppId = uuid.New().String()
	err = a.registry.RegisterApp(r.Context(), app)
	if err != nil {
		if errors.Is(err, registry.ErrDuplicate) {
			http.Error(w, "registration already exists", http.StatusConflict)
		} else {
			http.Error(w, "failed to register app", http.StatusInternalServerError)
		}
		return
	}

	appResponse := model.AppResponse{AppId: app.AppId}

	appBytes, err := json.Marshal(appResponse)
	if err != nil {
		http.Error(w, "failed to marshal response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(appBytes)
}

func (a *API) handleEvent(w http.ResponseWriter, r *http.Request) {
	var event model.Event
	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	apps, err := a.registry.GetAppByEventType(r.Context(), event.EventType)
	if err != nil {
		http.Error(w, "failed to get registered apps", http.StatusInternalServerError)
		return
	}
	event.EventId = uuid.New().String()
	event.CreatedAt = time.Now()
	err = a.event.InsertEvent(r.Context(), event)
	if err != nil {
		http.Error(w, "failed to insert event", http.StatusInternalServerError)
		return
	}
	if len(apps) == 0 {
		http.Error(w, "no registered apps for this event type", http.StatusNotFound)
		return
	}

	go a.delivery.DeliverEvent(event, apps)
	eventResponse := model.EventResponse{EventId: event.EventId}
	respBytes, err := json.Marshal(eventResponse)
	if err != nil {
		http.Error(w, "failed to marshal response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write(respBytes)
}

func (a *API) handleGetEventStatus(w http.ResponseWriter, r *http.Request) {
	eventId := r.PathValue("eventId")
	deliveryStatus, err := a.delivery.GetDeliveryStatus(r.Context(), eventId)
	if err != nil {
		http.Error(w, "failed to fetch delivery status", http.StatusInternalServerError)
		return
	}
	if len(deliveryStatus) == 0 {
		http.Error(w, "event not found", http.StatusNotFound)
		return
	}

	respBytes, err := json.Marshal(deliveryStatus)
	if err != nil {
		http.Error(w, "failed to marshal response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respBytes)
}
