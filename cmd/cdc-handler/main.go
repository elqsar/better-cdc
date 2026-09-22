package main

import (
	"context"
	"fmt"
	cdc "github.com/elqsar/better-cdc"
	"github.com/elqsar/better-cdc/internal/app"
	"github.com/elqsar/better-cdc/internal/config"
	"github.com/elqsar/better-cdc/internal/health"
	"github.com/elqsar/better-cdc/internal/logging"
	"github.com/elqsar/better-cdc/internal/publisher"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(1)
	}

	if cfg.EnableProfiling {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
	}
	logger, err := logging.New(cfg.Debug)
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if len(os.Args) > 1 {
		if os.Args[1] != "dlq" {
			logger.Error("usage: cdc-handler [dlq list|inspect|redrive]")
			os.Exit(1)
		}
		if err := runDLQ(ctx, cfg, logger, os.Args[2:], os.Stdout); err != nil {
			logger.Error("dlq command failed", zap.Error(err))
			os.Exit(1)
		}
		return
	}

	runner, err := cdc.New(config.Group(cfg), cdc.WithLogger(logger))
	if err != nil {
		logger.Error("invalid producer configuration", zap.Error(err))
		os.Exit(1)
	}
	if err := health.Start(ctx, health.Options{Addr: cfg.HealthAddr, EnablePprof: cfg.EnablePprof, Logger: logger,
		MetricsHandler: runner.MetricsHandler(), Readiness: []health.Check{{Name: "producer", Func: runner.Ready}},
	}); err != nil {
		logger.Error("health server failed", zap.Error(err))
		os.Exit(1)
	}
	if err := runner.Run(ctx); err != nil {
		logger.Error("cdc engine stopped", zap.Error(err))
		os.Exit(1)
	}
}

func buildPublisher(cfg config.Config, logger *zap.Logger) (publisher.Publisher, error) {
	return app.BuildPublisher(cfg, logger, nil)
}
