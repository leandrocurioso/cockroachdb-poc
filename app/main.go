// Command bench drives a steady transaction rate against CockroachDB,
// inserting one client (client + client_status + client_address) per
// transaction, to benchmark cluster write performance.
//
// A single sequential loop cannot sustain thousands of TPS — each
// transaction round-trips in milliseconds, so throughput needs concurrency.
// This runs a pool of workers paced by a shared rate limiter targeting
// TARGET_TPS overall (default 4000), with enough workers in flight
// (Little's law: workers >= targetTPS * avgLatency) to actually hit it.
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"
)

const (
	defaultTargetTPS = 4000
	defaultWorkers   = 256
	reportInterval   = time.Second

	// Every worker connecting through a single node pins all SQL-gateway and
	// admission-control work onto that one node — with enough concurrency to
	// hit a few thousand TPS, that node throttles hard and throughput
	// oscillates (bursts of a few thousand, then multi-second stalls while
	// it drains). Spreading connections round-robin across all nodes lets
	// each one admit load independently, which is what actually sustains a
	// high steady-state rate.
	defaultDatabaseURLs = "postgresql://root@localhost:26257/bench?sslmode=disable," +
		"postgresql://root@localhost:26258/bench?sslmode=disable," +
		"postgresql://root@localhost:26259/bench?sslmode=disable"
)

var (
	statuses  = []string{"active", "inactive", "pending", "suspended"}
	cities    = []string{"Lima", "Bogota", "Santiago", "Buenos Aires", "Mexico City"}
	countries = []string{"PE", "CO", "CL", "AR", "MX"}
	letters   = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

type result struct {
	dur time.Duration
	err error
}

func main() {
	dsnList := os.Getenv("DATABASE_URL")
	if dsnList == "" {
		dsnList = defaultDatabaseURLs
	}
	dsns := strings.Split(dsnList, ",")
	targetTPS := envInt("TARGET_TPS", defaultTargetTPS)
	workers := envInt("WORKERS", defaultWorkers)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	connsPerNode := max(int32(workers)/int32(len(dsns)), 4)
	pools := make([]*pgxpool.Pool, len(dsns))
	for i, dsn := range dsns {
		cfg, err := pgxpool.ParseConfig(strings.TrimSpace(dsn))
		if err != nil {
			log.Fatalf("parse dsn %q: %v", dsn, err)
		}
		cfg.MaxConns = connsPerNode

		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			log.Fatalf("connect to %q: %v", dsn, err)
		}
		defer pool.Close()

		if err := pool.Ping(ctx); err != nil {
			log.Fatalf("ping %q: %v (did you run `make schema`?)", dsn, err)
		}
		pools[i] = pool
	}
	log.Printf("connected to %d node(s) — target %d tps across %d workers (Ctrl+C to stop)", len(pools), targetTPS, workers)

	limiter := rate.NewLimiter(rate.Limit(targetTPS), burstFor(targetTPS))
	resultCh := make(chan result, workers*4)
	start := time.Now()

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		pool := pools[i%len(pools)]
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				if err := limiter.Wait(ctx); err != nil {
					return // context cancelled
				}
				t0 := time.Now()
				err := insertClient(ctx, pool, rng)
				select {
				case resultCh <- result{dur: time.Since(t0), err: err}:
				case <-ctx.Done():
					return
				}
			}
		}(time.Now().UnixNano() + int64(i))
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	total, failed := aggregate(resultCh, start)
	log.Printf("shutting down — total inserted: %d, failed: %d, elapsed: %s, avg rate: %.1f tps",
		total, failed, time.Since(start), float64(total)/time.Since(start).Seconds())
}

// aggregate consumes results as they arrive, printing a tps + latency
// report once per second, until resultCh is closed (all workers stopped).
func aggregate(resultCh <-chan result, start time.Time) (total, failed int64) {
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	var window []time.Duration
	var windowFailed int64

	for {
		select {
		case r, ok := <-resultCh:
			if !ok {
				return total, failed
			}
			total++
			if r.err != nil {
				failed++
				windowFailed++
				continue
			}
			window = append(window, r.dur)

		case <-ticker.C:
			report(total, failed, time.Since(start), window, windowFailed)
			window = window[:0]
			windowFailed = 0
		}
	}
}

// insertClientSQL writes one client plus its status and address rows as a
// single statement built from data-modifying CTEs. CockroachDB runs this as
// one implicit transaction, which lets it take the single-round-trip fast
// commit path — an explicit BEGIN/INSERT/INSERT/INSERT/COMMIT sequence pays
// for a second Raft round trip per transaction and measurably more latency.
const insertClientSQL = `
WITH c AS (
	INSERT INTO client (name) VALUES ($1) RETURNING id
), s AS (
	INSERT INTO client_status (client_id, status) SELECT id, $2 FROM c RETURNING client_id
)
INSERT INTO client_address (client_id, street, city, country)
SELECT id, $3, $4, $5 FROM c`

func insertClient(ctx context.Context, pool *pgxpool.Pool, rng *rand.Rand) error {
	idx := rng.Intn(len(cities))
	_, err := pool.Exec(ctx, insertClientSQL,
		randomName(rng),
		statuses[rng.Intn(len(statuses))],
		randomStreet(rng), cities[idx], countries[idx],
	)
	return err
}

// report prints achieved tps for the last window plus its latency
// distribution — the window resets every reportInterval.
func report(total, failed int64, elapsed time.Duration, window []time.Duration, windowFailed int64) {
	if len(window) == 0 {
		log.Printf("total=%d failed=%d | no successful inserts in last window (failed=%d)", total, failed, windowFailed)
		return
	}

	sorted := append([]time.Duration(nil), window...)
	slices.Sort(sorted)

	var sum time.Duration
	for _, l := range sorted {
		sum += l
	}
	avg := sum / time.Duration(len(sorted))
	p95 := sorted[int(float64(len(sorted))*0.95)]
	min, max := sorted[0], sorted[len(sorted)-1]
	instantTPS := float64(len(sorted)) / reportInterval.Seconds()
	overallRate := float64(total) / elapsed.Seconds()

	log.Printf("tps=%.0f (overall avg=%.0f) total=%d failed=%d | latency min=%s avg=%s p95=%s max=%s",
		instantTPS, overallRate, total, failed, min, avg, p95, max)
}

func burstFor(targetTPS int) int {
	return max(targetTPS/10, 1)
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func randomString(rng *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func randomName(rng *rand.Rand) string {
	return "client-" + randomString(rng, 8)
}

func randomStreet(rng *rand.Rand) string {
	return fmt.Sprintf("%d %s St", rng.Intn(9999), randomString(rng, 6))
}
