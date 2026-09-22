package app

import (
	"fmt"
	"strings"

	"github.com/elqsar/better-cdc/internal/config"
	"github.com/elqsar/better-cdc/internal/metrics"
	"github.com/elqsar/better-cdc/internal/publisher"

	"go.uber.org/zap"
)

func BuildPublisher(cfg config.Config, logger *zap.Logger, registry *metrics.Metrics) (publisher.Publisher, error) {
	urls := compactStrings(cfg.NATSURLs)
	if len(urls) == 0 {
		if !cfg.AllowNoopPublisher {
			return nil, fmt.Errorf("NATS_URL is required unless ALLOW_NOOP_PUBLISHER=true")
		}
		logger.Warn("NATS URLs missing, using noop publisher because ALLOW_NOOP_PUBLISHER is enabled; all publishes will be dropped and readiness will fail")
		return publisher.NewNoopPublisher(), nil
	}
	return publisher.NewJetStreamPublisher(publisher.JetStreamOptions{
		Metrics:   registry,
		URLs:      urls,
		EnableDLQ: cfg.PublishFailurePolicy == "dlq", DLQStream: cfg.DLQStream, DLQBucket: cfg.DLQBucket, DLQSubjectPrefix: cfg.DLQSubjectPrefix,
		DLQMaxBytes: cfg.DLQMaxBytes, DLQIndexMaxBytes: cfg.DLQIndexMaxBytes,
		CredentialsFile: cfg.NATSCredentialsFile, TLSCA: cfg.NATSTLSCA, TLSCert: cfg.NATSTLSCert, TLSKey: cfg.NATSTLSKey,
		Username:               cfg.NATSUsername,
		Password:               cfg.NATSPassword,
		ConnectTimeout:         cfg.NATSTimeout,
		PublishTimeout:         cfg.NATSTimeout,
		PublishAsyncMaxPending: cfg.EffectivePublishAsyncMaxPending(),
		StreamName:             cfg.StreamName,
		StreamSubjects:         cfg.StreamSubjects,
		StreamStorage:          cfg.StreamStorage,
		StreamReplicas:         cfg.StreamReplicas,
		StreamMaxAge:           cfg.StreamMaxAge,
		DuplicateWindow:        cfg.DuplicateWindow,
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
