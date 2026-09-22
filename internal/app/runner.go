// Package app owns the producer lifecycle shared by the library and executable.
package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/elqsar/better-cdc/internal/checkpoint"
	"github.com/elqsar/better-cdc/internal/config"
	"github.com/elqsar/better-cdc/internal/engine"
	"github.com/elqsar/better-cdc/internal/metrics"
	"github.com/elqsar/better-cdc/internal/model"
	"github.com/elqsar/better-cdc/internal/parser"
	"github.com/elqsar/better-cdc/internal/publisher"
	"github.com/elqsar/better-cdc/internal/transformer"
	"github.com/elqsar/better-cdc/internal/wal"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type Runner struct {
	cfg       config.Config
	logger    *zap.Logger
	metrics   *metrics.Metrics
	reader    *wal.PGReader
	publisher publisher.Publisher
	monitor   *wal.Monitor
	used      atomic.Bool
	running   atomic.Bool
}

func New(cfg config.Config, logger *zap.Logger) (*Runner, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.Plugin == "" {
		cfg.Plugin = "pgoutput"
	}
	if cfg.PublishFailurePolicy == "" {
		cfg.PublishFailurePolicy = "dlq"
	}
	if cfg.StreamName == "" {
		cfg.StreamName = "CDC"
	}
	if cfg.DLQStream == "" {
		cfg.DLQStream = cfg.StreamName + "_DLQ"
	}
	if cfg.DLQBucket == "" {
		cfg.DLQBucket = cfg.StreamName + "_RECOVERY"
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.NATSURLs = slices.Clone(cfg.NATSURLs)
	cfg.StreamSubjects = slices.Clone(cfg.StreamSubjects)
	cfg.Publications = slices.Clone(cfg.Publications)
	cfg.TableFilters = slices.Clone(cfg.TableFilters)
	m := metrics.NewMetrics()
	pub, err := BuildPublisher(cfg, logger, m)
	if err != nil {
		return nil, err
	}
	reader := wal.NewPGReader(wal.SlotConfig{Metrics: m, SlotName: cfg.SlotName, Plugin: cfg.Plugin, DatabaseURL: cfg.DatabaseURL,
		Publications: cfg.Publications, TableFilter: buildTableFilter(cfg.TableFilters), FeedbackInterval: cfg.CheckpointFreq, MaxBufferBytes: cfg.RawBufferBytes,
	}, cfg.RawMessageBufferSize, logger)
	return &Runner{cfg: cfg, logger: logger, metrics: m, reader: reader, publisher: pub,
		monitor: wal.NewMonitor(cfg.DatabaseURL, cfg.SlotName, reader)}, nil
}

func (r *Runner) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(r.metrics.Registry, promhttp.HandlerOpts{})
}
func (r *Runner) Ready(ctx context.Context) error {
	if !r.running.Load() {
		return fmt.Errorf("producer is not running")
	}
	if err := r.reader.Ready(ctx); err != nil {
		return err
	}
	if err := r.monitor.Ready(ctx); err != nil {
		return err
	}
	if ready, ok := r.publisher.(interface{ Ready(context.Context) error }); ok {
		return ready.Ready(ctx)
	}
	return nil
}

func (r *Runner) Run(ctx context.Context) error {
	if !r.used.CompareAndSwap(false, true) {
		return fmt.Errorf("producer Run may only be called once")
	}
	if ctx.Err() != nil {
		return nil
	}
	cfg := r.cfg
	if err := wal.Preflight(ctx, wal.SlotConfig{SlotName: cfg.SlotName, Plugin: cfg.Plugin, DatabaseURL: cfg.DatabaseURL, Publications: cfg.Publications}); err != nil {
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			return nil
		}
		return err
	}
	spillDir, release, err := parser.PrepareSpillDir(cfg.SpillDir, cfg.SourceID, cfg.SlotName)
	if err != nil {
		return err
	}
	defer release()
	var decoder parser.Parser
	filter := buildTableFilter(cfg.TableFilters)
	if cfg.Plugin == "pgoutput" {
		decoder = parser.NewPGOutputParser(parser.PGOutputConfig{Metrics: r.metrics, TableFilter: filter, Logger: r.logger,
			BufferSize: cfg.ParsedEventBufferSize, MaxTxBufferSize: cfg.MaxTxBufferSize, MaxBufferBytes: cfg.ParsedBufferBytes,
			MaxTxBytes: cfg.MaxTxBytes, MaxSpillBytes: cfg.MaxSpillBytes, SpillDir: spillDir})
	} else {
		decoder = parser.NewWal2JSONParser(parser.Wal2JSONConfig{Metrics: r.metrics, TableFilter: filter, Logger: r.logger,
			BufferSize: cfg.ParsedEventBufferSize, MaxBufferBytes: cfg.ParsedBufferBytes})
	}
	store := checkpoint.NewSlotStore(cfg.DatabaseURL, cfg.SlotName)
	pos, err := store.Load(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			return nil
		}
		return err
	}
	ckpt := checkpoint.NewManager(store, cfg.CheckpointFreq, r.logger)
	ckpt.Init(pos, time.Now())
	monitorCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); r.monitor.Run(monitorCtx) }()
	defer func() { cancel(); <-done }()
	r.running.Store(true)
	defer r.running.Store(false)
	identity := model.Identity{SourceID: cfg.SourceID, Slot: cfg.SlotName, Decoder: cfg.Plugin}
	eng := engine.NewEngine(engine.Options{Metrics: r.metrics, Identity: identity, Reader: r.reader, Parser: decoder,
		Transformer: transformer.NewSimpleTransformer(cfg.Database, identity), Publisher: r.publisher, Checkpointer: ckpt,
		Database: cfg.Database, BatchSize: cfg.BatchSize, BatchTimeout: cfg.BatchTimeout, MaxPublishRetries: cfg.MaxPublishRetries,
		UnsafeUnorderedAsyncPublish: cfg.UnsafeUnorderedAsyncPublish, FailurePolicy: engine.FailurePolicy(cfg.PublishFailurePolicy),
		DLQSubjectPrefix: cfg.DLQSubjectPrefix, Logger: r.logger})
	return eng.Run(ctx, pos)
}
