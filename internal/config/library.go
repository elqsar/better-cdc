package config

import "time"

// LibraryConfig configures one Postgres-to-JetStream producer. Start with defaults.
type LibraryConfig struct {
	Source    PostgresConfig
	JetStream JetStreamConfig
	Pipeline  PipelineConfig
	Recovery  RecoveryConfig
}

// PostgresConfig holds source settings.
type PostgresConfig struct {
	SourceID     string
	Database     string
	SlotName     string
	Plugin       string
	DatabaseURL  string
	Publications []string
	TableFilters []string
}

// JetStreamConfig holds jetstream settings.
type JetStreamConfig struct {
	NATSURLs            []string
	NATSUsername        string
	NATSPassword        string
	NATSCredentialsFile string
	NATSTLSCA           string
	NATSTLSCert         string
	NATSTLSKey          string
	NATSTimeout         time.Duration
	StreamName          string
	StreamSubjects      []string
	StreamStorage       string
	StreamReplicas      int
	StreamMaxAge        time.Duration
	DuplicateWindow     time.Duration
}

// PipelineConfig holds pipeline settings.
type PipelineConfig struct {
	BatchSize                   int
	BatchTimeout                time.Duration
	CheckpointFreq              time.Duration
	RawMessageBufferSize        int
	ParsedEventBufferSize       int
	RawBufferBytes              int64
	ParsedBufferBytes           int64
	MaxTxBufferSize             int
	MaxTxBytes                  int64
	MaxSpillBytes               int64
	SpillDir                    string
	PublishAsyncMaxPending      int
	MaxPublishRetries           int
	UnsafeUnorderedAsyncPublish bool
	AllowNoopPublisher          bool
}

// RecoveryConfig holds recovery settings.
type RecoveryConfig struct {
	PublishFailurePolicy string
	DLQSubjectPrefix     string
	DLQStream            string // Empty derives <StreamName>_DLQ.
	DLQBucket            string // Empty derives <StreamName>_RECOVERY.
	DLQMaxBytes          int64
	DLQIndexMaxBytes     int64
}

func Group(c Config) LibraryConfig {
	return LibraryConfig{
		Source: PostgresConfig{
			SourceID:     c.SourceID,
			Database:     c.Database,
			SlotName:     c.SlotName,
			Plugin:       c.Plugin,
			DatabaseURL:  c.DatabaseURL,
			Publications: c.Publications,
			TableFilters: c.TableFilters,
		},
		JetStream: JetStreamConfig{
			NATSURLs:            c.NATSURLs,
			NATSUsername:        c.NATSUsername,
			NATSPassword:        c.NATSPassword,
			NATSCredentialsFile: c.NATSCredentialsFile,
			NATSTLSCA:           c.NATSTLSCA,
			NATSTLSCert:         c.NATSTLSCert,
			NATSTLSKey:          c.NATSTLSKey,
			NATSTimeout:         c.NATSTimeout,
			StreamName:          c.StreamName,
			StreamSubjects:      c.StreamSubjects,
			StreamStorage:       c.StreamStorage,
			StreamReplicas:      c.StreamReplicas,
			StreamMaxAge:        c.StreamMaxAge,
			DuplicateWindow:     c.DuplicateWindow,
		},
		Pipeline: PipelineConfig{
			BatchSize:                   c.BatchSize,
			BatchTimeout:                c.BatchTimeout,
			CheckpointFreq:              c.CheckpointFreq,
			RawMessageBufferSize:        c.RawMessageBufferSize,
			ParsedEventBufferSize:       c.ParsedEventBufferSize,
			RawBufferBytes:              c.RawBufferBytes,
			ParsedBufferBytes:           c.ParsedBufferBytes,
			MaxTxBufferSize:             c.MaxTxBufferSize,
			MaxTxBytes:                  c.MaxTxBytes,
			MaxSpillBytes:               c.MaxSpillBytes,
			SpillDir:                    c.SpillDir,
			PublishAsyncMaxPending:      c.PublishAsyncMaxPending,
			MaxPublishRetries:           c.MaxPublishRetries,
			UnsafeUnorderedAsyncPublish: c.UnsafeUnorderedAsyncPublish,
			AllowNoopPublisher:          c.AllowNoopPublisher,
		},
		Recovery: RecoveryConfig{
			PublishFailurePolicy: c.PublishFailurePolicy,
			DLQSubjectPrefix:     c.DLQSubjectPrefix,
			DLQStream:            c.DLQStream,
			DLQBucket:            c.DLQBucket,
			DLQMaxBytes:          c.DLQMaxBytes,
			DLQIndexMaxBytes:     c.DLQIndexMaxBytes,
		},
	}
}

func Flatten(c LibraryConfig) Config {
	return Config{
		SourceID:                    c.Source.SourceID,
		Database:                    c.Source.Database,
		SlotName:                    c.Source.SlotName,
		Plugin:                      c.Source.Plugin,
		DatabaseURL:                 c.Source.DatabaseURL,
		Publications:                c.Source.Publications,
		TableFilters:                c.Source.TableFilters,
		NATSURLs:                    c.JetStream.NATSURLs,
		NATSUsername:                c.JetStream.NATSUsername,
		NATSPassword:                c.JetStream.NATSPassword,
		NATSCredentialsFile:         c.JetStream.NATSCredentialsFile,
		NATSTLSCA:                   c.JetStream.NATSTLSCA,
		NATSTLSCert:                 c.JetStream.NATSTLSCert,
		NATSTLSKey:                  c.JetStream.NATSTLSKey,
		NATSTimeout:                 c.JetStream.NATSTimeout,
		StreamName:                  c.JetStream.StreamName,
		StreamSubjects:              c.JetStream.StreamSubjects,
		StreamStorage:               c.JetStream.StreamStorage,
		StreamReplicas:              c.JetStream.StreamReplicas,
		StreamMaxAge:                c.JetStream.StreamMaxAge,
		DuplicateWindow:             c.JetStream.DuplicateWindow,
		BatchSize:                   c.Pipeline.BatchSize,
		BatchTimeout:                c.Pipeline.BatchTimeout,
		CheckpointFreq:              c.Pipeline.CheckpointFreq,
		RawMessageBufferSize:        c.Pipeline.RawMessageBufferSize,
		ParsedEventBufferSize:       c.Pipeline.ParsedEventBufferSize,
		RawBufferBytes:              c.Pipeline.RawBufferBytes,
		ParsedBufferBytes:           c.Pipeline.ParsedBufferBytes,
		MaxTxBufferSize:             c.Pipeline.MaxTxBufferSize,
		MaxTxBytes:                  c.Pipeline.MaxTxBytes,
		MaxSpillBytes:               c.Pipeline.MaxSpillBytes,
		SpillDir:                    c.Pipeline.SpillDir,
		PublishAsyncMaxPending:      c.Pipeline.PublishAsyncMaxPending,
		MaxPublishRetries:           c.Pipeline.MaxPublishRetries,
		UnsafeUnorderedAsyncPublish: c.Pipeline.UnsafeUnorderedAsyncPublish,
		AllowNoopPublisher:          c.Pipeline.AllowNoopPublisher,
		PublishFailurePolicy:        c.Recovery.PublishFailurePolicy,
		DLQSubjectPrefix:            c.Recovery.DLQSubjectPrefix,
		DLQStream:                   c.Recovery.DLQStream,
		DLQBucket:                   c.Recovery.DLQBucket,
		DLQMaxBytes:                 c.Recovery.DLQMaxBytes,
		DLQIndexMaxBytes:            c.Recovery.DLQIndexMaxBytes,
	}
}
