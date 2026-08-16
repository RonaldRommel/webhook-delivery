package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"webhook-delivery/internal/api"
	"webhook-delivery/internal/db"
	"webhook-delivery/internal/delivery"
	"webhook-delivery/internal/event"
	"webhook-delivery/internal/registry"
	"webhook-delivery/internal/worker"

	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, relying on environment variables")
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	ctx := context.Background()
	pool, err := db.NewPostgres(ctx, dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	registry := registry.NewRegistry(pool)
	delivery := delivery.NewDelivery(pool)
	event := event.NewEvent(pool)
	worker:=worker.NewWorker(pool,delivery)
	a := api.New(registry, delivery, event)
	go worker.Run(ctx)

	log.Println("listening on :8080")
	if err := http.ListenAndServe(":8080", a.Routes()); err != nil {
		log.Fatal(err)
	}
}
