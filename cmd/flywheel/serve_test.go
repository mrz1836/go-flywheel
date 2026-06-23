package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// freeMetricsAddr reserves and releases an ephemeral loopback port, returning its
// address for the serve metrics server to bind. The brief reserve/release window
// is tolerated by the test's connect-retry loop.
func freeMetricsAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// writeMetricsConfig writes a SQLite-backed flywheel.yaml whose runtime exposes
// the metrics server at addr.
func writeMetricsConfig(t *testing.T, dir, addr string) string {
	t.Helper()
	body := fmt.Sprintf(`db:
  sqlite: %s/flywheel.db
runtime:
  queues: [default, periodic]
  concurrency: 1
  poll_interval: 30ms
  metrics_addr: %q
log:
  level: error
`, dir, addr)
	p := filepath.Join(dir, "flywheel.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

// getMetrics polls url until it returns 200 (or the deadline elapses) and returns
// the body, so the test observes serve's infinite Run loop making the endpoint
// live without a fixed sleep.
func getMetrics(t *testing.T, url string, timeout time.Duration) string {
	t.Helper()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return string(body)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("GET %s did not return 200 within %s", url, timeout)
	return ""
}

func TestCLIServeExposesMetricsEndpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	addr := freeMetricsAddr(t)
	cfg := writeMetricsConfig(t, dir, addr)

	_, err := runRoot(context.Background(), "--config", cfg, "migrate")
	require.NoError(t, err)
	// Enqueue a job so the queue gauges have something to report.
	_, err = runRoot(context.Background(), "--config", cfg, "enqueue", "exec", `{"command":"true"}`)
	require.NoError(t, err)

	serveCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = runRoot(serveCtx, "--config", cfg, "serve")
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	base := "http://" + addr
	body := getMetrics(t, base+"/metrics", 5*time.Second)
	assert.Contains(t, body, "# TYPE flywheel_queue_jobs gauge", "the queue gauges are exposed")
	assert.Contains(t, body, "flywheel_queue_ready", "the lag/ready gauges are sampled per scrape")

	// /healthz and /readyz still serve alongside /metrics.
	client := &http.Client{Timeout: 500 * time.Millisecond}
	health, err := client.Get(base + "/healthz")
	require.NoError(t, err)
	_ = health.Body.Close()
	assert.Equal(t, http.StatusOK, health.StatusCode)
}
