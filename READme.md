# Webhook Delivery Service

A standalone webhook delivery infrastructure service, built from scratch in Go — modeled after
products like [Svix](https://www.svix.com/) and [Hookdeck](https://hookdeck.com/). Subscribers
register an endpoint and an event type; the service durably accepts events, delivers them via a
background worker, tracks delivery outcomes, and automatically retries failed deliveries with
exponential backoff.

This project was built incrementally, stage by stage, with each stage triggered by a concrete
limitation of the previous one — the same way a real system evolves under real constraints. The
sections below describe not just what was built, but *why*, including decisions that were
reconsidered and reversed along the way.

## Table of Contents

- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Data model](#data-model)
- [Key design decisions](#key-design-decisions)
- [Known limitations / deliberately deferred work](#known-limitations--deliberately-deferred-work)
- [Running it locally](#running-it-locally)

---

## What it does

1. **Register a subscriber** — `POST /register` with a `url` and `event_type`. The service stores
   this mapping durably.
2. **Send an event** — `POST /event` with an `event_type` and a `payload`. The service persists the
   event and schedules a delivery for each matching subscriber — all in one transaction — then
   returns immediately. A background worker picks up scheduled deliveries (both brand-new events
   and due retries) and executes them.
3. **Check delivery status** — `GET /event/{eventId}/status` returns the current delivery state for
   every subscriber the event fanned out to.
4. **Automatic retries** — if a delivery attempt fails in a way that looks transient (network error,
   5xx response), the service retries with exponential backoff, up to 5 attempts, before giving up.

---

## Architecture

```
                          ┌─────────────┐
   POST /register     ──▶ │             │
   POST /event        ──▶ │   HTTP API   │
   GET /event/:id/status ▶│  (chi router)│
                          └──────┬──────┘
                            │
              ┌─────────────┼─────────────────┐
              ▼             ▼                 ▼
        ┌──────────┐  ┌──────────┐      ┌───────────┐
        │ registry │  │  event   │      │ delivery  │
        │(subscribers)│(event log)│     │(HTTP POST │
        └────┬─────┘  └────┬─────┘      │ + status) │
             │             │            └─────┬─────┘
             ▼             ▼                  ▼
        ┌────────────────────────────────────────┐
        │              PostgreSQL                 │
        │  app_registrations · events ·            │
        │  delivery_status · delivery_attempts     │
        └────────────────────────────────────────┘
                            ▲
                            │  polls every 30s for
                            │  rows due now — both
                            │  never-attempted (state=
                            │  pending) and scheduled
                            │  retries (state=retry_later)
                     ┌──────┴──────┐
                     │   worker    │
                     │ (background │
                     │  goroutine) │
                     └─────────────┘
```

`POST /event`'s only write path is a single Postgres transaction: insert the event, insert one
`delivery_status` row per subscriber (`state = 'pending'`, due immediately). No delivery is fired
directly from the HTTP handler — the worker is the *only* thing that ever executes a delivery
attempt, whether it's a brand-new event or a scheduled retry. This means first attempts and
retries share one code path end to end, with no special-casing between them.

**Packages:**

| Package    | Responsibility |
|------------|-----------------|
| `api`      | HTTP handlers, request/response shaping. No business logic of its own. |
| `registry` | Subscriber (app) registration and lookup. Owns `app_registrations`. |
| `event`    | Persisting the event itself, independent of who's subscribed. Owns `events`. |
| `delivery` | Executing a single delivery attempt (HTTP POST), classifying the outcome, and writing status/history. Owns `delivery_status` and `delivery_attempts`. |
| `worker`   | Background polling loop that finds due deliveries — both never-attempted events and scheduled retries — and executes them via `delivery`. |

Dependency injection is used throughout (each package exposes a struct constructed with its
Postgres pool, rather than package-level globals) — this was a deliberate refactor away from an
earlier in-memory, global-singleton design, specifically to make persistence swappable and the
code testable without a live database.

---

## Data model

Four tables, each with a distinct role:

### `app_registrations`
```sql
app_id      uuid PRIMARY KEY
url         text NOT NULL
event_type  text NOT NULL
created_at  timestamptz DEFAULT now()
UNIQUE (url, event_type)
```
The subscriber list. Source of truth for "who gets what." The `(url, event_type)` uniqueness
constraint replaced a hand-rolled duplicate-check loop from the original in-memory version —
letting Postgres enforce the invariant atomically instead of a mutex-guarded loop.

### `events`
```sql
event_id    uuid PRIMARY KEY
event_type  text NOT NULL
payload     jsonb NOT NULL
created_at  timestamptz NOT NULL
```
Every accepted event, persisted **synchronously and transactionally** — alongside its initial
`delivery_status` rows — before the HTTP handler returns. This means a crash immediately after
`202 Accepted` never loses the event or the fact that it needs delivering (see
[Key design decisions](#key-design-decisions) for how this evolved from an earlier,
non-durable goroutine-based dispatch). Persisted regardless of whether any subscriber currently
exists for its event type (a deliberate choice — an event is a fact that occurred, independent of
who happened to be listening).

### `delivery_status`
```sql
event_id       uuid
app_id         uuid
state          delivery_state  -- pending | success | failed | retry_later | dead
error          text
attempt_count  int
next_retry_at  timestamptz
sent_at        timestamptz
received_at    timestamptz
PRIMARY KEY (event_id, app_id)
```
**Current, authoritative state** per (event, subscriber) pair — updated in place. This is the
table the retry worker and the status API both read from; it must always be correct.

### `delivery_attempts`
```sql
attempt_id      bigserial PRIMARY KEY
event_id        uuid
app_id          uuid
attempt_number  int
state           text  -- success | failed  (never "dead" or "retry_later" — see below)
error           text
sent_at         timestamptz NOT NULL
received_at     timestamptz          -- NULL if no response was ever received (timeout, DNS, etc.)
```
**Append-only history** — one row per delivery attempt, never updated. Best-effort: if a write
here occasionally fails, nothing downstream depends on it, so it's simply logged and skipped.

**Why two tables instead of one:** `delivery_status` answers "what's the current situation, and
does my retry logic need to act on it" — it has to be correct, because the worker's own decisions
depend on it. `delivery_attempts` answers "what happened, historically" — a debugging/audit trail
that nothing in the system reads back to make decisions, so it can tolerate an occasional gap.
Collapsing them into one mutable row would lose the full attempt history; collapsing `dead` into
`failed` would hide the difference between "permanently rejected" (4xx) and "genuinely exhausted
after real effort" (5xx/timeout, 5 attempts).

---

## Key design decisions

A selection of decisions that involved real tradeoffs — the reasoning here is deliberately kept,
since it's the part of this project most worth discussing in an interview.

**Async delivery, `202 Accepted`.** Delivering to N subscribers synchronously within one HTTP
request means one slow/hung subscriber blocks the response — and with no timeout on the HTTP
client, could hang forever. Delivery is fired via `go`, and the API responds `202 Accepted`
immediately with the event ID, before any delivery is attempted.

**HTTP client with a 3-second timeout.** Without one, `http.DefaultClient`'s zero timeout means a
single unresponsive subscriber can hang a goroutine (and leak it) indefinitely, and — before
async delivery — starve every other subscriber queued behind it in the same fan-out loop.

**Postgres for durability; Redis considered and explicitly deferred.** Registrations must survive
a restart (they're config, not observational data) — Postgres. Delivery status also needs to be
durable, since retry logic depends on reading it correctly. Redis was seriously considered as a
**cache-aside layer in front of Postgres** for delivery-status reads (to demonstrate a measurable
latency improvement) — but was deprioritized in favor of finishing retries within the project
timeline. Using Redis as the *sole* store for delivery status was considered and rejected: it
would mean losing all delivery history on every Redis restart, which contradicts the reliability
promise of a webhook delivery service.

**Retryable vs. terminal failure classification.** Follows the same convention widely used in
production systems (Stripe, Svix): **4xx responses are treated as permanent, non-retryable
failures** (the request itself was rejected — retrying the identical request produces the
identical result), while **network errors and 5xx responses are treated as transient and
retryable** (the failure is plausibly temporary). This is a simplification — a fully precise
implementation would special-case `429 Too Many Requests` to respect a `Retry-After` header —
which is a deliberate, acknowledged corner cut here.

**Exponential backoff, 5 attempts, ~15 minute total window.** `delay = 60s * 2^(attempt-1)`,
giving retries at roughly 60s, 120s, 240s, 480s after the previous failure — summing to 15
minutes from first failure to giving up. This was chosen (and verified against real test runs) as
a reasonable window for a portfolio project; production systems handling higher-stakes traffic
(e.g., payments) often retry over hours or days instead.

**Two-table split for delivery status/history**, and the corresponding distinction between
`failed` (per-attempt: this one attempt failed) and `dead` (aggregate: all retries exhausted) —
see [Data model](#data-model) above.

**Background worker as a goroutine within the same process, not a separate service.** A
`time.Ticker`-driven loop polls for due work every 30 seconds, running inside the same binary
as the HTTP server. This keeps the deployment story simple (one process) while still cleanly
separating "orchestrate delivery" (worker) from "execute one delivery attempt" (delivery) as
distinct responsibilities — a natural seam to split into a separate worker service later if
horizontal scaling required it.

**Making the initial delivery dispatch durable, without a message queue.** Retries were already
durable — a retry is just a `delivery_status` row with a future `next_retry_at`, and the worker
will always find it, even across a restart. The *first* delivery attempt was not: it was fired as
a bare goroutine (`go delivery.DeliverEvent(...)`) directly from the HTTP handler, so a process
crash in the narrow window between accepting the request and the goroutine completing meant that
delivery was silently, permanently lost — even though the event itself was already safely
persisted.

The instinctive fix reached for here is a message queue (Redis Streams, RabbitMQ, etc.) — push the
event onto a queue instead of firing a goroutine, let a consumer pick it up. That was evaluated
seriously: a real queue would give near-instant pickup instead of polling latency, zero idle cost,
and built-in multi-consumer coordination via consumer groups. But it also introduces a new piece
of infrastructure to run and reason about, its own persistence/durability configuration to get
right, and non-trivial crash-recovery semantics (an unacknowledged message needs to be reclaimed
via something like `XCLAIM`/`XAUTOCLAIM`, or it's stuck in limbo). None of that was needed to solve
the *actual* problem — durability of the dispatch decision — as opposed to problems this project
doesn't yet have, like high-throughput fan-out across many consumer processes or true pub/sub to
independent services.

The solution actually implemented is smaller: introduce a `pending` state — distinct from
`retry_later` — meaning "scheduled for its first attempt, not yet tried" (`attempt_count = 0`,
`next_retry_at = now()`). `POST /event` now inserts the event **and** one `pending`
`delivery_status` row per subscriber inside a single Postgres transaction, so the two either
commit together or not at all — no window where the event exists but its scheduled deliveries
don't (or vice versa). The worker's query was then simply widened from
`WHERE state = 'retry_later'` to `WHERE state IN ('pending', 'retry_later')`. The result: a
brand-new event's first attempt and a retried attempt are now indistinguishable to the worker —
same query, same code path, same `DeliverToApp` call, differing only by which state got them
picked up. No special-casing was needed anywhere.

This meant `Registry`- and `Delivery`-owned insert methods needed to run inside a caller-controlled
transaction — solved by having them accept a small `Executor` interface (`Exec`/`Query`/`QueryRow`)
rather than always using their own stored connection pool. Both `*pgxpool.Pool` and `pgx.Tx`
satisfy this interface, so the same method works standalone (pass the pool) or as part of an atomic
multi-table transaction (pass a `tx` from `pool.Begin(ctx)`) — the caller decides, per call site,
whether atomicity is required.

**Sequential (not concurrent) processing, for both the initial fan-out and the worker's batch of
due rows.** A concurrent, goroutine-per-row approach was considered and explicitly rejected: it
introduces unbounded outbound connections with no backpressure or pool-size limit, and solves a
scaling problem this project doesn't currently have. The concrete trigger condition for revisiting
this: if a single poll cycle's processing time (rows found × up to 3s per attempt) starts to
regularly exceed the 30-second poll interval, the worker falls permanently behind and
`next_retry_at` timestamps drift further from "now" over time — that's the signal concurrency (a
bounded worker pool) would become necessary, not just nice to have.

---

## Known limitations / deliberately deferred work

These aren't oversights — each was identified, discussed, and consciously scoped out to keep the
project achievable within its timeline. Listed here rather than hidden, since a well-reasoned scope
cut is a stronger signal than an undocumented gap.

- **No message queue (Redis Streams, etc.).** Evaluated specifically for the initial-dispatch
  durability problem and deliberately not used — solved instead with a transactional insert and a
  unified `pending`/`retry_later` polling model (see [Key design decisions](#key-design-decisions)).
  A real queue would still be worth revisiting for different reasons: near-instant (rather than
  up-to-30s) pickup latency, and built-in multi-consumer coordination if this were ever split
  across multiple worker instances.
- **No concurrent delivery processing.** Both the initial dispatch and retries are now processed
  by one worker, one row at a time. Trigger condition for revisiting: if a poll cycle's total
  processing time (rows found × up to 3s per attempt) starts regularly exceeding the 30s poll
  interval, `next_retry_at` timestamps will drift further behind "now" over time — that's the
  concrete signal a bounded worker pool becomes necessary.
- **No Redis caching layer.** Considered and designed (cache-aside, caching only fully-resolved
  statuses to sidestep invalidation issues on in-flight/mutable data) but not implemented, in favor
  of prioritizing retry logic.
- **No authentication / multi-tenancy.** Any caller can register subscribers or query any event's
  status. A real system would scope all of this per API key/tenant.
- **No signature verification (HMAC).** Subscribers currently have no way to cryptographically
  verify a payload genuinely came from this service — a defining feature of Svix/Hookdeck.
- **No graceful shutdown.** `SIGTERM`/`SIGINT` currently kill the process immediately; no
  in-flight HTTP requests, deliveries, or worker cycles are given a chance to finish, and the
  Postgres connection pool isn't closed cleanly.
- **No rate limiting** on outbound deliveries per subscriber.
- **Single worker, no distributed coordination.** If scaled to multiple worker instances, they
  would need a mechanism (row-level locking, a `processing`/claim state) to avoid double-processing
  the same due row — not needed with a single worker goroutine, as is currently the case. This is
  also the main problem a real message queue's consumer groups would solve for free.

---

## Running it locally

**Requirements:** Go, Docker (for Postgres).

```bash
# start Postgres
docker compose up -d

# set required env var (or use a .env file)
export DATABASE_URL="postgres://user:pass@localhost:5432/webhook?sslmode=disable"

# run migrations (schema in /migrations, or apply manually — see Data model section)

# run the service
go run ./cmd/server
```

**Example usage:**

```bash
# register a subscriber
curl -X POST localhost:8080/register \
  -d '{"url":"https://example.com/webhook","event_type":"order.created"}'

# send an event
curl -X POST localhost:8080/event \
  -d '{"event_type":"order.created","payload":{"order_id":123}}'

# check delivery status
curl localhost:8080/event/<event_id>/status
```