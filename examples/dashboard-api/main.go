// Command dashboard-api is a read-only JSON API over a flywheel database — the
// shape an external dashboard service takes when it imports flywheel and reads
// the runtime's tables from its own database. It serves:
//
//	GET /running            the jobs running now, slow ones flagged
//	GET /queues             per-queue depth and lag
//	GET /finished           the most recently finished jobs (?state=discarded&before=<finalized_at>,<id>)
//	GET /stats              per-kind stats (?since=24h&kind=…&queue=…)
//	GET /series             a trend series (?since=720h&interval=hour|day&kind=…&tz=America/New_York)
//	GET /anomalies          the latest rolled hour's anomalies
//	GET /jobs/{id}/runs     one job's attempts (?before_attempt=<attempt>)
//
// A /finished cursor is the previous page's last job: its finalized_at exactly
// as the JSON carried it, a comma, and its id. URL-encode it — an RFC3339 offset
// such as +02:00 carries a '+', which a query string otherwise reads as a space
// (the handler restores one left unencoded, but a client should not rely on it).
//
// It writes nothing and runs no runtime: the database's own flywheel deployment
// (a Node with a Scheduler whose StatsRollupInterval is set, or `flywheel serve`)
// keeps the rollups the history endpoints read. It opens the database read-only
// in spirit — nothing here calls a write API — so it can point at a replica.
//
//	go run ./examples/dashboard-api -db 'file:flywheel.db?_pragma=busy_timeout(5000)'
//	go run ./examples/dashboard-api -db "$DATABASE_URL" -dialect postgres
//
// See docs/INTEGRATING.md for what each endpoint costs and how often to poll it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/glebarez/sqlite"
	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "listen address")
	dsn := flag.String("db", "file:flywheel.db?_pragma=busy_timeout(5000)", "database DSN")
	dialect := flag.String("dialect", "sqlite", "sqlite or postgres")
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	db, err := open(*dialect, *dsn)
	if err != nil {
		logger.Error("open database", "error", err)
		os.Exit(1)
	}

	srv := &http.Server{Addr: *addr, Handler: NewHandler(db), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	logger.Info("dashboard api listening", "addr", *addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		logger.Error("serve", "error", err)
		os.Exit(1)
	}
}

// open connects to the database a flywheel deployment writes.
func open(dialect, dsn string) (*gorm.DB, error) {
	switch dialect {
	case "sqlite":
		return gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	case "postgres":
		return gorm.Open(postgres.Open(dsn), &gorm.Config{})
	default:
		return nil, fmt.Errorf("unknown dialect %q", dialect)
	}
}

// NewHandler returns the dashboard's routes over db. It is the embeddable part:
// a host mounts it under its own mux, behind its own authentication — the
// endpoints expose job metadata and error messages, so they are not for the
// open internet.
func NewHandler(db *gorm.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /running", func(w http.ResponseWriter, r *http.Request) {
		respond(w, r, func(ctx context.Context) (any, error) {
			return flywheel.ListRunning(ctx, db, flywheel.ListRunningParams{
				Kind: r.URL.Query().Get("kind"), Queue: r.URL.Query().Get("queue"), WithBaseline: true,
			})
		})
	})
	mux.HandleFunc("GET /queues", func(w http.ResponseWriter, r *http.Request) {
		respond(w, r, func(ctx context.Context) (any, error) { return flywheel.QueueDepths(ctx, db) })
	})
	mux.HandleFunc("GET /finished", func(w http.ResponseWriter, r *http.Request) {
		respond(w, r, func(ctx context.Context) (any, error) {
			p := flywheel.ListFinishedParams{Kind: r.URL.Query().Get("kind"), Queue: r.URL.Query().Get("queue")}
			if s := r.URL.Query().Get("state"); s != "" {
				p.States = []flywheel.JobState{flywheel.JobState(s)}
			}
			if before := r.URL.Query().Get("before"); before != "" {
				cursor, err := finishedCursor(before)
				if err != nil {
					return nil, err
				}
				p.Before = cursor
			}
			return flywheel.ListFinished(ctx, db, p)
		})
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		respond(w, r, func(ctx context.Context) (any, error) {
			since, err := sinceParam(r, 24*time.Hour)
			if err != nil {
				return nil, err
			}
			now := time.Now()
			return flywheel.Stats(ctx, db, flywheel.StatsParams{
				From: now.Add(-since), To: now, Kind: r.URL.Query().Get("kind"), Queue: r.URL.Query().Get("queue"),
			})
		})
	})
	mux.HandleFunc("GET /series", func(w http.ResponseWriter, r *http.Request) {
		respond(w, r, func(ctx context.Context) (any, error) {
			since, err := sinceParam(r, 7*24*time.Hour)
			if err != nil {
				return nil, err
			}
			loc := time.UTC
			if tz := r.URL.Query().Get("tz"); tz != "" {
				if loc, err = time.LoadLocation(tz); err != nil {
					return nil, badRequest("tz: " + err.Error())
				}
			}
			now := time.Now()
			return flywheel.StatsSeries(ctx, db, flywheel.SeriesParams{
				From: now.Add(-since), To: now, Kind: r.URL.Query().Get("kind"), Queue: r.URL.Query().Get("queue"),
				Interval: flywheel.SeriesInterval(r.URL.Query().Get("interval")), Location: loc,
			})
		})
	})
	mux.HandleFunc("GET /anomalies", func(w http.ResponseWriter, r *http.Request) {
		respond(w, r, func(ctx context.Context) (any, error) {
			return flywheel.Anomalies(ctx, db, flywheel.AnomalyParams{})
		})
	})
	mux.HandleFunc("GET /jobs/{id}/runs", func(w http.ResponseWriter, r *http.Request) {
		respond(w, r, func(ctx context.Context) (any, error) {
			p := flywheel.ListRunsParams{Limit: 50}
			if raw := r.URL.Query().Get("before_attempt"); raw != "" {
				attempt, err := strconv.Atoi(raw)
				if err != nil || attempt <= 0 {
					return nil, badRequest("before_attempt must be a positive attempt number")
				}
				p.BeforeAttempt = attempt
			}
			return flywheel.ListRuns(ctx, db, r.PathValue("id"), p)
		})
	})
	return mux
}

// finishedCursor parses a /finished cursor, <RFC3339 finalized_at>,<id>.
//
// A '+' a client left unencoded reaches the handler as a space, and RFC3339
// never contains one, so turning spaces back into '+' is lossless — and it keeps
// paging working for every database whose timestamps carry a positive offset.
func finishedCursor(raw string) (*flywheel.FinishedCursor, error) {
	at, id, ok := strings.Cut(raw, ",")
	if !ok || id == "" {
		return nil, badRequest("before must be <RFC3339 finalized_at>,<id>")
	}
	t, err := time.Parse(time.RFC3339Nano, strings.ReplaceAll(at, " ", "+"))
	if err != nil {
		return nil, badRequest("before must be <RFC3339 finalized_at>,<id>: " + err.Error())
	}
	return &flywheel.FinishedCursor{FinalizedAt: t, ID: id}, nil
}

// errBadRequest marks a caller error, answered with 400 rather than 500.
var errBadRequest = errors.New("bad request")

// badRequest wraps msg as a 400.
func badRequest(msg string) error { return fmt.Errorf("%w: %s", errBadRequest, msg) }

// sinceParam reads ?since as a Go duration, defaulting to def.
func sinceParam(r *http.Request, def time.Duration) (time.Duration, error) {
	raw := r.URL.Query().Get("since")
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, badRequest("since must be a positive duration like 24h")
	}
	return d, nil
}

// respond runs one read with a deadline and writes its result as JSON. A
// validation error or a window the rollups do not cover is the caller's to fix
// (400); anything else is the server's (500).
func respond(w http.ResponseWriter, r *http.Request, read func(context.Context) (any, error)) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	out, err := read(ctx)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errBadRequest) || errors.Is(err, flywheel.ErrValidation) || errors.Is(err, flywheel.ErrStatsNotRolledUp) {
			status = http.StatusBadRequest
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(out)
}
