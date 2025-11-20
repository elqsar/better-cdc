package logging

import (
	"go.uber.org/zap"
)

// New builds a zap logger; debug enables development config and debug level.
func New(debug bool) (*zap.Logger, error) {
	if debug {
		cfg := zap.NewDevelopmentConfig()
		return cfg.Build()
	}
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "json"
	return cfg.Build()
}
