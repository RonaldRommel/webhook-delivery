# Webhook Delivery Service

## Project Context

Early-stage portfolio project built incrementally. Currently at Stage 1 (synchronous delivery).

## Review Instructions

When asked to review code:

- Bullet points only
- Flag bugs, compilation errors, incorrect Go idioms, broken wiring between layers
- Do not suggest features or improvements beyond the current stage
- Be concise

## Structure

- /internal/model - data models
- /internal/registry - in-memory storage
- /internal/delivery - HTTP fan-out
- /internal/api - handlers and routes
- /cmd/main.go - entrypoint

## Current Stage

Stage 1 — synchronous delivery, in-memory storage, no retries, no auth
