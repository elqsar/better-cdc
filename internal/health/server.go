package health

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type Check struct {
	Name string
	Func func(context.Context) error
}

type Options struct {
	Addr         string
	EnablePprof  bool
	CheckTimeout time.Duration
	Readiness    []Check
	Logger       *zap.Logger
}

// NewHandler builds the health HTTP handler.
func NewHandler(opts Options) http.Handler {
	mux := http.NewServeMux()
	timeout := opts.CheckTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if len(opts.Readiness) == 0 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}

		var failures []string
		for _, check := range opts.Readiness {
			if check.Func == nil {
				continue
			}
			checkCtx, cancel := context.WithTimeout(context.Background(), timeout)
			err := check.Func(checkCtx)
			cancel()
			if err != nil {
				name := check.Name
				if name == "" {
					name = "unnamed"
				}
				failures = append(failures, fmt.Sprintf("%s: %v", name, err))
			}
		}

		if len(failures) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(strings.Join(failures, "; ")))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	if opts.EnablePprof {
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
	}

	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

// Start launches the health server.
func Start(ctx context.Context, opts Options) {
	if opts.Addr == "" {
		return
	}
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	srv := &http.Server{
		Addr:    opts.Addr,
		Handler: NewHandler(opts),
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
