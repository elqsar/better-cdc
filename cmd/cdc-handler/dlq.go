package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"better-cdc/internal/config"
	"better-cdc/internal/model"
	"better-cdc/internal/parser"
	"better-cdc/internal/publisher"
	"better-cdc/internal/transformer"
	"go.uber.org/zap"
)

func runDLQ(ctx context.Context, cfg config.Config, logger *zap.Logger, args []string, out io.Writer) error {
	if len(args) == 0 || (args[0] != "list" && args[0] != "inspect" && args[0] != "redrive") {
		return fmt.Errorf("usage: cdc-handler dlq list | inspect <event-id> | redrive [--skip <event-id>]...")
	}
	cfg.PublishFailurePolicy = "dlq"
	pub, err := buildPublisher(cfg, logger)
	if err != nil {
		return err
	}
	p, ok := pub.(*publisher.JetStreamPublisher)
	if !ok {
		return fmt.Errorf("DLQ requires JetStream")
	}
	if err = p.Connect(); err != nil {
		return err
	}
	defer func() { _ = p.Close() }()
	encode := json.NewEncoder(out)
	switch args[0] {
	case "list":
		if len(args) != 1 {
			return fmt.Errorf("usage: dlq list")
		}
		return p.WalkDLQ(ctx, func(seq uint64, rec publisher.DeadLetterRecord) error {
			return encode.Encode(struct {
				Sequence uint64                     `json:"sequence"`
				Record   publisher.DeadLetterRecord `json:"record"`
			}{seq, rec})
		})
	case "inspect":
		if len(args) != 2 {
			return fmt.Errorf("usage: dlq inspect <event-id>")
		}
		found := false
		err = p.WalkDLQ(ctx, func(_ uint64, rec publisher.DeadLetterRecord) error {
			if rec.EventID != args[1] || found {
				return nil
			}
			found = true
			obj, err := p.LoadRecovery(ctx, rec)
			if err != nil {
				return err
			}
			return encode.Encode(obj)
		})
		if err == nil && !found {
			return fmt.Errorf("event not found")
		}
		return err
	case "redrive":
		skip, err := parseRedriveSkips(args[1:])
		if err != nil {
			return err
		}
		skipped, err := p.RedriveDLQ(ctx, skip, func(ctx context.Context, obj *publisher.RecoveryObject) error {
			data, subject := obj.Payload, obj.Subject
			if len(data) == 0 {
				evt, err := parser.RestoreChange(obj.Recovery)
				if err != nil {
					return err
				}
				defer model.ReleaseWALEvent(evt)
				normalized, err := transformer.NewSimpleTransformer(obj.Database).Transform(ctx, evt)
				if err != nil {
					return err
				}
				defer model.ReleaseCDCEvent(normalized)
				if normalized.EventID != obj.EventID {
					return fmt.Errorf("reconstructed event identity differs")
				}
				subject, err = publisher.SubjectForEvent(obj.Database, normalized)
				if err != nil {
					return err
				}
				data, err = json.Marshal(normalized)
				if err != nil {
					return err
				}
			}
			if subject == "" {
				return fmt.Errorf("recovery subject missing")
			}
			return p.PublishWithRetries(ctx, subject, data, cfg.MaxPublishRetries, obj.EventID)
		})
		for _, id := range skipped {
			delete(skip, id)
			if encErr := encode.Encode(struct {
				Skipped string `json:"skipped"`
			}{id}); encErr != nil && err == nil {
				err = encErr
			}
		}
		if err == nil && len(skip) > 0 {
			missing := make([]string, 0, len(skip))
			for id := range skip {
				missing = append(missing, id)
			}
			sort.Strings(missing)
			return fmt.Errorf("--skip event IDs not found among pending records: %s", strings.Join(missing, ", "))
		}
		return err
	}
	return nil
}

// parseRedriveSkips reads repeated "--skip <event-id>" arguments.
func parseRedriveSkips(args []string) (map[string]bool, error) {
	skip := make(map[string]bool)
	for i := 0; i < len(args); i++ {
		if args[i] != "--skip" || i+1 >= len(args) || args[i+1] == "" {
			return nil, fmt.Errorf("usage: dlq redrive [--skip <event-id>]...")
		}
		skip[args[i+1]] = true
		i++
	}
	return skip, nil
}
