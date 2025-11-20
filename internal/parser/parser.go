package parser

import (
	"context"

	"better-cdc/internal/model"
)

// Parser converts WAL decoding output into structured WALEvents.
type Parser interface {
	Parse(ctx context.Context, stream <-chan *model.WALEvent) (<-chan *model.WALEvent, error)
}

// NoopParser passes events through; placeholder while real decoding is added.
type NoopParser struct{}

func NewNoopParser() *NoopParser {
	return &NoopParser{}
}

func (p *NoopParser) Parse(ctx context.Context, stream <-chan *model.WALEvent) (<-chan *model.WALEvent, error) {
	_ = ctx
	return stream, nil
}
