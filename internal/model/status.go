package model;
import "time"

type Status struct {
    State     string     `json:"state"`
    Error     string 	 `json:"error"`
    Timestamp time.Time  `json:"timestamp"`
}