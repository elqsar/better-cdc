// Package cdc streams PostgreSQL logical replication changes to NATS JetStream.
// It owns no signals or HTTP listeners. Each Runner owns one existing slot and
// must have a stable source identity. Start configuration with DefaultConfig.
package cdc

import (
	"context"
	"net/http"

	"github.com/elqsar/better-cdc/internal/app"
	"github.com/elqsar/better-cdc/internal/config"
	"github.com/elqsar/better-cdc/internal/model"

	"go.uber.org/zap"
)

// Config holds settings for one producer. Start with DefaultConfig.
type Config = config.LibraryConfig
type PostgresConfig = config.PostgresConfig
type JetStreamConfig = config.JetStreamConfig
type PipelineConfig = config.PipelineConfig
type RecoveryConfig = config.RecoveryConfig

// Event is the published envelope. Decode JSON using json.Decoder.UseNumber
// to retain exact integer and decimal values in row images.
type Event = model.CDCEvent

// DefaultConfig supplies local settings; Source.SourceID must be set explicitly.
func DefaultConfig() Config {
	cfg := config.Group(config.DefaultConfig())
	// Derive recovery names from the final stream name in New.
	cfg.Recovery.DLQStream, cfg.Recovery.DLQBucket = "", ""
	return cfg
}

type options struct{ logger *zap.Logger }

// Option configures runtime integrations without changing capture settings.
type Option func(*options)

// WithLogger supplies structured logging; nil selects a silent logger.
func WithLogger(logger *zap.Logger) Option { return func(o *options) { o.logger = logger } }

// Runner is a single-use producer. Independent instances have isolated metrics
// and spill storage. Run releases its resources before returning.
type Runner struct{ impl *app.Runner }

// New validates and snapshots configuration without network or filesystem I/O.
func New(cfg Config, opts ...Option) (*Runner, error) {
	o := options{}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	impl, err := app.New(config.Flatten(cfg), o.logger)
	if err != nil {
		return nil, err
	}
	return &Runner{impl: impl}, nil
}

// Run blocks until cancellation or failure. A successful requested shutdown returns nil.
func (r *Runner) Run(ctx context.Context) error { return r.impl.Run(ctx) }

// Ready reports whether this instance is actively capturing and publishing.
func (r *Runner) Ready(ctx context.Context) error { return r.impl.Ready(ctx) }

// MetricsHandler exposes only this runner's Prometheus registry.
func (r *Runner) MetricsHandler() http.Handler { return r.impl.MetricsHandler() }
