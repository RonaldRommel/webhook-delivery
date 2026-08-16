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
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	apps, err := a.registry.GetAppByEventType(r.Context(), event.EventType)
	if err != nil {
		http.Error(w, "failed to get registered apps", http.StatusInternalServerError)
		return
	}
	if len(apps) == 0 {
		http.Error(w, "no registered apps for this event type", http.StatusNotFound)
		return
	}

	event.EventId = uuid.New().String()
	event.CreatedAt = time.Now()

	tx, err := a.pool.Begin(r.Context())
	if err != nil {
		http.Error(w, "failed to start transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context()) // no-op if already committed

	if err := a.event.InsertEvent(r.Context(), tx, event); err != nil {
		http.Error(w, "failed to insert event", http.StatusInternalServerError)
		return
	}

	nxtRetry := time.Now()
	for _, app := range apps {
		status := model.DeliveryStatus{
			EventId:       event.EventId,
			AppId:         app.AppId,
			State:         "pending",
			AttemptNumber: 0,
			NextRetryAt:   &nxtRetry,
			CreatedAt:     event.CreatedAt,
		}
		if err := a.delivery.InsertDeliveryStatus(r.Context(), tx, status); err != nil {
			http.Error(w, "failed to schedule delivery", http.StatusInternalServerError)
			return
		}
	}

	if err := tx.Commit(r.Context()); err != nil {
		http.Error(w, "failed to commit transaction", http.StatusInternalServerError)
		return
	}

	eventResponse := model.EventResponse{EventId: event.EventId}
	respBytes, _ := json.Marshal(eventResponse)
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
