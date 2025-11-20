package parser

import (
	"context"

	"github.com/jackc/pglogrepl"

	"better-cdc/internal/model"
)

// Plugin enumerates supported logical decoding plugins.
type Plugin string

const (
	PluginWal2JSON Plugin = "wal2json"
	PluginPGOutput Plugin = "pgoutput"
)

// RawMessage is a single logical decoding message emitted by the WAL reader.
// It carries the plugin identifier, WAL start LSN, and the raw payload bytes.
type RawMessage struct {
	Plugin   Plugin
	WALStart pglogrepl.LSN
	Data     []byte
}

// Parser converts raw logical decoding messages into structured WALEvents.
type Parser interface {
	Parse(ctx context.Context, stream <-chan *RawMessage) (<-chan *model.WALEvent, error)
}
