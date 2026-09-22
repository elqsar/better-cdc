// Example embedding a CDC producer in an application that owns its HTTP server
// and process signals. Set DATABASE_URL, NATS_URL, and CDC_SOURCE_ID to run it.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	cdc "github.com/elqsar/better-cdc"
)

func main() {
	cfg := cdc.DefaultConfig()
	cfg.Source.SourceID = os.Getenv("CDC_SOURCE_ID")
	if value := os.Getenv("DATABASE_URL"); value != "" {
		cfg.Source.DatabaseURL = value
	}
	if value := os.Getenv("NATS_URL"); value != "" {
		cfg.JetStream.NATSURLs = []string{value}
	}
	runner, err := cdc.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	mux := http.NewServeMux()
	mux.Handle("/metrics", runner.MetricsHandler())
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := runner.Ready(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Print(err)
			cancel()
		}
	}()
	runErr := runner.Run(ctx)
	shutdownCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Print(err)
		_ = server.Close()
	}
	<-stopped
	if runErr != nil {
		log.Fatal(runErr)
	}
}
