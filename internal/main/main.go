// Load test for the webhook delivery service.
//
// Spins up N fake subscriber HTTP servers with configurable behavior
// (fast success, slow, 5xx, 4xx), registers them against the real
// service, fires a burst of events, measures API response latency
// (time to 202), and polls delivery status until everything settles
// to measure end-to-end fan-out + retry time.
//
// Usage:
//
//	go run main.go -service http://localhost:8080 -subscribers 50 -events 20
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type subscriberKind int

const (
	kindFast subscriberKind = iota // 200 immediately
	kindSlow                      // 200 after a delay (tests timeout handling)
	kind5xx                       // always 500 (tests retry path)
	kind4xx                       // always 400 (tests terminal-failure path)
)

// Mix of subscriber behaviors. Adjust to taste.
var mix = []struct {
	kind  subscriberKind
	ratio float64
}{
	{kindFast, 0.70},
	{kindSlow, 0.15}, // slow enough to test the 3s client timeout
	{kind5xx, 0.10},
	{kind4xx, 0.05},
}

func main() {
	serviceURL := flag.String("service", "http://localhost:8080", "base URL of the webhook service")
	numSubs := flag.Int("subscribers", 50, "number of fake subscribers to spin up")
	numEvents := flag.Int("events", 20, "number of events to fire")
	eventType := flag.String("event-type", "loadtest.event", "event_type used for registration + events")
	settleTimeout := flag.Duration("settle-timeout", 30*time.Minute, "max time to wait for all deliveries to resolve")
	pollEvery := flag.Duration("poll-interval", 3*time.Second, "how often to poll /events/{id} while settling")
	basePort := flag.Int("base-port", 9200, "starting local port for fake subscriber servers")
	flag.Parse()

	log.Printf("starting %d fake subscribers on ports %d-%d", *numSubs, *basePort, *basePort+*numSubs-1)

	subs := make([]*fakeSubscriber, *numSubs)
	for i := 0; i < *numSubs; i++ {
		k := pickKind(i, *numSubs)
		s := newFakeSubscriber(*basePort+i, k)
		s.start()
		subs[i] = s
	}
	defer func() {
		for _, s := range subs {
			s.stop()
		}
	}()

	client := &http.Client{Timeout: 10 * time.Second}

	log.Printf("registering %d subscribers with the service...", len(subs))
	for _, s := range subs {
		if err := register(client, *serviceURL, s.url, *eventType); err != nil {
			log.Fatalf("register failed for %s: %v", s.url, err)
		}
	}
	log.Printf("registration complete")

	// Fire events and measure API-response latency (time to 202).
	apiLatencies := make([]time.Duration, *numEvents)
	eventIDs := make([]string, *numEvents)
	var wg sync.WaitGroup
	var mu sync.Mutex
	log.Printf("firing %d events concurrently...", *numEvents)
	overallStart := time.Now()
	for i := 0; i < *numEvents; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			start := time.Now()
			id, err := sendEvent(client, *serviceURL, *eventType, idx)
			elapsed := time.Since(start)
			if err != nil {
				log.Printf("event %d failed: %v", idx, err)
				return
			}
			mu.Lock()
			apiLatencies[idx] = elapsed
			eventIDs[idx] = id
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	fanoutWallClock := time.Since(overallStart)

	printLatencyStats("API response time (time to 202)", apiLatencies)
	fmt.Printf("Wall-clock time to fire all %d events concurrently: %s\n\n", *numEvents, fanoutWallClock)

	// Poll until every event's deliveries have settled (no more pending/retry_later).
	log.Printf("polling delivery status every %s (timeout %s)...", *pollEvery, *settleTimeout)
	settleStart := time.Now()
	deadline := time.Now().Add(*settleTimeout)
	var lastSummary map[string]int
	round := 0
	consecutiveErrs := 0
	expectedPerEvent := *numSubs
	expectedTotal := *numSubs * len(eventIDs)

	for time.Now().Before(deadline) {
		round++
		allDone := true
		summary := map[string]int{}
		errCount := 0
		seenTotal := 0
		for _, id := range eventIDs {
			if id == "" {
				continue
			}
			statuses, err := getEventStatus(client, *serviceURL, id)
			if err != nil {
				errCount++
				allDone = false
				continue
			}
			seenTotal += len(statuses)
			if len(statuses) < expectedPerEvent {
				// Not all delivery_status rows have been created yet for
				// this event (e.g. first attempt hasn't fired for every
				// subscriber). Don't count this as settled.
				allDone = false
			}
			for _, st := range statuses {
				summary[st]++
				if st != "success" && st != "dead" && st != "failed" {
					allDone = false
				}
			}
		}
		lastSummary = summary
		log.Printf("poll #%d: %v (seen %d/%d rows, errors: %d)", round, summary, seenTotal, expectedTotal, errCount)

		if errCount > 0 {
			consecutiveErrs++
			if consecutiveErrs == 1 {
				// Surface the first real error once, loudly, instead of
				// silently retrying for the full timeout window.
				for _, id := range eventIDs {
					if id == "" {
						continue
					}
					if _, err := getEventStatus(client, *serviceURL, id); err != nil {
						log.Printf("SAMPLE ERROR (event %s): %v", id, err)
						break
					}
				}
			}
		} else {
			consecutiveErrs = 0
		}

		if allDone {
			break
		}
		time.Sleep(*pollEvery)
	}
	settleElapsed := time.Since(settleStart)

	fmt.Println("=== Final delivery status summary ===")
	total := 0
	for state, count := range lastSummary {
		fmt.Printf("  %-12s %d\n", state, count)
		total += count
	}
	fmt.Printf("  %-12s %d\n", "TOTAL", total)
	fmt.Printf("\nTime from first event sent to full settlement: %s\n", settleElapsed)
	fmt.Printf("(includes retry backoff windows for 5xx/network-error subscribers)\n")
}

func pickKind(i, total int) subscriberKind {
	pos := float64(i) / float64(total)
	acc := 0.0
	for _, m := range mix {
		acc += m.ratio
		if pos < acc {
			return m.kind
		}
	}
	return kindFast
}

// ---- fake subscriber server ----

type fakeSubscriber struct {
	port   int
	url    string
	kind   subscriberKind
	srv    *http.Server
	hits   int64
}

func newFakeSubscriber(port int, kind subscriberKind) *fakeSubscriber {
	return &fakeSubscriber{
		port: port,
		url:  fmt.Sprintf("http://localhost:%d/webhook", port),
		kind: kind,
	}
}

func (s *fakeSubscriber) start() {
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&s.hits, 1)
		io.Copy(io.Discard, r.Body)
		switch s.kind {
		case kindFast:
			w.WriteHeader(http.StatusOK)
		case kindSlow:
			time.Sleep(5 * time.Second) // longer than a typical 3s client timeout
			w.WriteHeader(http.StatusOK)
		case kind5xx:
			w.WriteHeader(http.StatusInternalServerError)
		case kind4xx:
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	s.srv = &http.Server{Addr: fmt.Sprintf(":%d", s.port), Handler: mux}
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("subscriber %d server error: %v", s.port, err)
		}
	}()
}

func (s *fakeSubscriber) stop() {
	if s.srv != nil {
		s.srv.Close()
	}
}

// ---- HTTP helpers against the real service ----

func register(client *http.Client, base, url, eventType string) error {
	body, _ := json.Marshal(map[string]string{"url": url, "event_type": eventType})
	resp, err := client.Post(base+"/register", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func sendEvent(client *http.Client, base, eventType string, idx int) (string, error) {
	payload := map[string]interface{}{
		"event_type": eventType,
		"payload":    map[string]interface{}{"idx": idx, "ts": time.Now().UnixNano()},
	}
	body, _ := json.Marshal(payload)
	resp, err := client.Post(base+"/event", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, string(b))
	}
	var out struct {
		EventID string `json:"event_id"`
		ID      string `json:"id"` // fallback key name, in case the service uses "id"
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return "", fmt.Errorf("could not parse response %q: %w", string(b), err)
	}
	if out.EventID != "" {
		return out.EventID, nil
	}
	if out.ID != "" {
		return out.ID, nil
	}
	return "", fmt.Errorf("no event id found in response: %s", string(b))
}

// getEventStatus hits GET /events/{id} and returns the list of per-subscriber
// delivery states. Adjust the JSON shape below if your API's response differs.
func getEventStatus(client *http.Client, base, eventID string) ([]string, error) {
	resp, err := client.Get(fmt.Sprintf("%s/event/%s/status", base, eventID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(b))
	}

	// Try a couple of plausible shapes since the exact response format
	// isn't pinned down here: either a top-level array, or {"deliveries": [...]}.
	var asArray []struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(b, &asArray); err == nil && len(asArray) > 0 {
		states := make([]string, len(asArray))
		for i, d := range asArray {
			states[i] = d.State
		}
		return states, nil
	}

	var asWrapped struct {
		Deliveries []struct {
			State string `json:"state"`
		} `json:"deliveries"`
	}
	if err := json.Unmarshal(b, &asWrapped); err == nil && len(asWrapped.Deliveries) > 0 {
		states := make([]string, len(asWrapped.Deliveries))
		for i, d := range asWrapped.Deliveries {
			states[i] = d.State
		}
		return states, nil
	}

	return nil, fmt.Errorf("could not parse status response, adjust getEventStatus() to match your API shape: %s", string(b))
}

// ---- stats ----

func printLatencyStats(label string, durs []time.Duration) {
	var valid []time.Duration
	for _, d := range durs {
		if d > 0 {
			valid = append(valid, d)
		}
	}
	if len(valid) == 0 {
		fmt.Printf("%s: no successful samples\n\n", label)
		return
	}
	sort.Slice(valid, func(i, j int) bool { return valid[i] < valid[j] })

	p := func(pct float64) time.Duration {
		idx := int(math.Ceil(pct/100*float64(len(valid)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(valid) {
			idx = len(valid) - 1
		}
		return valid[idx]
	}

	var sum time.Duration
	for _, d := range valid {
		sum += d
	}
	avg := sum / time.Duration(len(valid))

	fmt.Printf("=== %s (n=%d) ===\n", label, len(valid))
	fmt.Printf("  min: %s\n", valid[0])
	fmt.Printf("  avg: %s\n", avg)
	fmt.Printf("  p50: %s\n", p(50))
	fmt.Printf("  p95: %s\n", p(95))
	fmt.Printf("  p99: %s\n", p(99))
	fmt.Printf("  max: %s\n", valid[len(valid)-1])
	fmt.Println()
}