package model

type App struct {
    AppId     string `json:"app_id"`
    Url       string `json:"url"`
    EventType string `json:"event_type"`
}
