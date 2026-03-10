package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"better-cdc/internal/checkpoint"
	"better-cdc/internal/config"
	"better-cdc/internal/engine"
	"better-cdc/internal/health"
	"better-cdc/internal/logging"
	"better-cdc/internal/parser"
	"better-cdc/internal/publisher"
	"better-cdc/internal/transformer"
	"better-cdc/internal/wal"
	"go.uber.org/zap"
)

func main() {
	cfg := config.Load()
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

	tableFilter := buildTableFilter(cfg.TableFilters)

	reader := wal.NewPGReader(wal.SlotConfig{
		SlotName:     cfg.SlotName,
		Plugin:       cfg.Plugin,
		DatabaseURL:  cfg.DatabaseURL,
		Publications: cfg.Publications,
		TableFilter:  tableFilter,
	}, cfg.RawMessageBufferSize, logger)

	var parse parser.Parser
	switch cfg.Plugin {
	case "pgoutput":
		parse = parser.NewPGOutputParser(parser.PGOutputConfig{
			TableFilter:     tableFilter,
			Logger:          logger,
			BufferSize:      cfg.ParsedEventBufferSize,
			MaxTxBufferSize: cfg.MaxTxBufferSize,
		})
	default:
		parse = parser.NewWal2JSONParser(parser.Wal2JSONConfig{
			TableFilter: tableFilter,
			Logger:      logger,
			BufferSize:  cfg.ParsedEventBufferSize,
		})
	}
	trans := transformer.NewSimpleTransformer(cfg.Database)
	pub, err := buildPublisher(cfg, logger)
	if err != nil {
		logger.Error("invalid publisher configuration", zap.Error(err))
		os.Exit(1)
	}
	store := checkpoint.NewSlotStore(cfg.DatabaseURL, cfg.SlotName)
	ckpt := checkpoint.NewManager(store, cfg.CheckpointFreq, logger)
	health.Start(ctx, health.Options{
		Addr:        cfg.HealthAddr,
		EnablePprof: cfg.EnablePprof,
		Logger:      logger,
		Readiness: []health.Check{
			{
				Name: "postgres",
				Func: func(ctx context.Context) error {
					_, err := store.Load(ctx)
					return err
				},
			},
			{
				Name: "publisher",
				Func: func(ctx context.Context) error {
					ready, ok := pub.(interface {
						Ready(context.Context) error
					})
					if !ok {
						return nil
					}
					return ready.Ready(ctx)
				},
			},
		},
	})
	logger.Info("health server configured",
		zap.String("health_endpoint", cfg.HealthAddr+"/health"),
		zap.String("ready_endpoint", cfg.HealthAddr+"/ready"),
		zap.String("metrics_endpoint", cfg.HealthAddr+"/metrics"),
		zap.Bool("pprof_enabled", cfg.EnablePprof))

	logger.Info("starting better-cdc",
		zap.Bool("debug", cfg.Debug),
		zap.Bool("profiling", cfg.EnableProfiling),
		zap.Bool("allow_noop_publisher", cfg.AllowNoopPublisher),
		zap.String("slot", cfg.SlotName),
		zap.Strings("publications", cfg.Publications),
		zap.String("db", cfg.Database),
		zap.String("plugin", cfg.Plugin),
		zap.Int("raw_buffer", cfg.RawMessageBufferSize),
		zap.Int("parsed_buffer", cfg.ParsedEventBufferSize),
		zap.Int("max_tx_buffer", cfg.MaxTxBufferSize))

	eng := engine.NewEngine(reader, parse, trans, pub, ckpt, cfg.Database, cfg.BatchSize, cfg.BatchTimeout, logger)

	startPos, err := store.Load(ctx)
	if err != nil {
		logger.Warn("failed to load checkpoint, starting from earliest", zap.Error(err))
	}
	ckpt.Init(startPos, time.Now())

	if err := eng.Run(ctx, startPos); err != nil {
		logger.Error("cdc engine stopped", zap.Error(err))
		os.Exit(1)
	}
}

func buildPublisher(cfg config.Config, logger *zap.Logger) (publisher.Publisher, error) {
	urls := compactStrings(cfg.NATSURLs)
	if len(urls) == 0 {
		if !cfg.AllowNoopPublisher {
			return nil, fmt.Errorf("NATS_URL is required unless ALLOW_NOOP_PUBLISHER=true")
		}
		logger.Warn("NATS URLs missing, using noop publisher because ALLOW_NOOP_PUBLISHER is enabled")
		return publisher.NewNoopPublisher(), nil
	}
	return publisher.NewJetStreamPublisher(publisher.JetStreamOptions{
		URLs:            urls,
		Username:        cfg.NATSUsername,
		Password:        cfg.NATSPassword,
		ConnectTimeout:  cfg.NATSTimeout,
		PublishTimeout:  cfg.NATSTimeout,
		StreamName:      cfg.StreamName,
		StreamSubjects:  cfg.StreamSubjects,
		StreamStorage:   cfg.StreamStorage,
		StreamReplicas:  cfg.StreamReplicas,
		StreamMaxAge:    cfg.StreamMaxAge,
		DuplicateWindow: cfg.DuplicateWindow,
	}, logger), nil
}

func buildTableFilter(filters []string) map[string]struct{} {
	if len(filters) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(filters))
	for _, f := range filters {
		out[f] = struct{}{}
	}
	return out
}

func compactStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
