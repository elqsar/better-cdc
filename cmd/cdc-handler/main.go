package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"better-cdc/internal/checkpoint"
	"better-cdc/internal/config"
	"better-cdc/internal/engine"
	"better-cdc/internal/health"
	"better-cdc/internal/logging"
	"better-cdc/internal/parser"
	"better-cdc/internal/publisher"
	"better-cdc/internal/transformer"
	"better-cdc/internal/wal"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func main() {
	// Enable block and mutex profiling for contention analysis
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)

	cfg := config.Load()
	logger, err := logging.New(cfg.Debug)
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	health.Start(ctx, cfg.HealthAddr, logger)
	logger.Info("prometheus metrics available", zap.String("endpoint", cfg.HealthAddr+"/metrics"))

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
			TableFilter: tableFilter,
			Logger:      logger,
			BufferSize:  cfg.ParsedEventBufferSize,
		})
	default:
		parse = parser.NewWal2JSONParser(parser.Wal2JSONConfig{
			TableFilter: tableFilter,
			Logger:      logger,
			BufferSize:  cfg.ParsedEventBufferSize,
		})
	}
	trans := transformer.NewSimpleTransformer(cfg.Database)
	pub := buildPublisher(cfg, logger)
	store, cleanup := newCheckpointStore(cfg, logger)
	defer cleanup()
	ckpt := checkpoint.NewManager(store, cfg.CheckpointFreq, logger)

	logger.Info("starting better-cdc",
		zap.Bool("debug", cfg.Debug),
		zap.String("slot", cfg.SlotName),
		zap.Strings("publications", cfg.Publications),
		zap.String("db", cfg.Database),
		zap.String("plugin", cfg.Plugin),
		zap.Int("raw_buffer", cfg.RawMessageBufferSize),
		zap.Int("parsed_buffer", cfg.ParsedEventBufferSize))

	eng := engine.NewEngine(reader, parse, trans, pub, ckpt, cfg.Database, cfg.BatchSize, cfg.BatchTimeout, logger)

	startPos, err := store.Load(ctx)
	if err != nil {
		logger.Warn("failed to load checkpoint, starting from earliest", zap.Error(err))
	}

	if err := eng.Run(ctx, startPos); err != nil {
		logger.Error("cdc engine stopped", zap.Error(err))
		os.Exit(1)
	}
}

func buildPublisher(cfg config.Config, logger *zap.Logger) publisher.Publisher {
	if len(cfg.NATSURLs) == 0 {
		logger.Warn("NATS URLs missing, using noop publisher")
		return publisher.NewNoopPublisher()
	}
	return publisher.NewJetStreamPublisher(publisher.JetStreamOptions{
		URLs:           cfg.NATSURLs,
		Username:       cfg.NATSUsername,
		Password:       cfg.NATSPassword,
		ConnectTimeout: cfg.NATSTimeout,
		PublishTimeout: cfg.NATSTimeout,
	}, logger)
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

// newCheckpointStore builds Redis-backed checkpoint store, falling back to in-memory if unavailable.
func newCheckpointStore(cfg config.Config, logger *zap.Logger) (checkpoint.Store, func()) {
	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		logger.Warn("invalid redis url, using memory store", zap.String("url", cfg.RedisURL), zap.Error(err))
		return checkpoint.NewMemoryStore(), func() {}
	}
	client := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.CheckpointFreq)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		logger.Warn("redis unavailable, using memory store", zap.Error(err))
		_ = client.Close()
		return checkpoint.NewMemoryStore(), func() {}
	}
	store := checkpoint.NewRedisStore(client, cfg.CheckpointKey, cfg.CheckpointTTL)
	return store, func() { _ = client.Close() }
}
