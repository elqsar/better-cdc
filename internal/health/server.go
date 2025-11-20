package health

import (
	"context"
	"net/http"

	"go.uber.org/zap"
)

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
