package main

import (
	"context"
	"io"

	"github.com/elqsar/better-cdc/internal/app"
	"github.com/elqsar/better-cdc/internal/config"

	"go.uber.org/zap"
)

func runDLQ(ctx context.Context, cfg config.Config, logger *zap.Logger, args []string, out io.Writer) error {
	return app.RunDLQ(ctx, cfg, logger, args, out)
}
