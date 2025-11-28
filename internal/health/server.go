package health

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync/atomic"

	"go.uber.org/zap"
)

// MetricsProvider is a function that returns current metrics as key-value pairs.
type MetricsProvider func() map[string]interface{}

var globalMetricsProvider atomic.Value

// SetMetricsProvider sets the global metrics provider for the /metrics endpoint.
func SetMetricsProvider(provider MetricsProvider) {
	globalMetricsProvider.Store(provider)
}

// Start launches a simple health endpoint at the given address.
func Start(ctx context.Context, addr string, logger *zap.Logger) {
	if addr == "" {
		return
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// pprof endpoints for profiling
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))

	// Metrics endpoint
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		provider := globalMetricsProvider.Load()
		if provider == nil {
			fmt.Fprintln(w, "# No metrics provider configured")
			return
		}

		metricsFunc, ok := provider.(MetricsProvider)
		if !ok {
			fmt.Fprintln(w, "# Invalid metrics provider")
			return
		}

		metrics := metricsFunc()
		fmt.Fprintln(w, "# CDC Handler Metrics")
		for key, value := range metrics {
			switch v := value.(type) {
			case float64:
				fmt.Fprintf(w, "%s %.6f\n", key, v)
			case uint64:
				fmt.Fprintf(w, "%s %d\n", key, v)
			case int64:
				fmt.Fprintf(w, "%s %d\n", key, v)
			default:
				fmt.Fprintf(w, "%s %v\n", key, v)
			}
		}
	})

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Warn("health server error", zap.Error(err))
		}
	}()
}
