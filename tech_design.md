# CDC Handler Technical Design

## Overview

This document describes the technical design of a Change Data Capture (CDC) handler in Go that listens to AWS RDS WAL (Write-Ahead Log) and publishes events to NATS Jetstream.

## Architecture

### High-Level Architecture

```
┌─────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   AWS RDS   │───▶│   CDC Handler   │───▶│ NATS Jetstream  │
│   WAL Log   │    │      (Go)       │    │   Event Broker  │
└─────────────┘    └─────────────────┘    └─────────────────┘
                           │
                           ▼
                    ┌─────────────────┐
                    │   Monitoring    │
                    │     System      │
                    └─────────────────┘
```

### Core Components

1. **WAL Reader**: Monitors AWS RDS WAL logs
2. **CDC Parser**: Parses WAL entries into structured events
3. **Event Transformer**: Transforms database events into standardized format
4. **NATS Publisher**: Publishes events to NATS Jetstream
5. **Error Handler**: Manages failures and retries
6. **Checkpoint Manager**: Tracks processed WAL positions

## Component Design

### 1. WAL Reader

**Purpose**: Monitor and read AWS RDS WAL logs

**Implementation**:
- Use AWS RDS native replication API or logical decoding
- Support PostgreSQL and MySQL RDS instances
- Handle WAL file rotation and continuous streaming

**Key Features**:
- Connection management to AWS RDS
- WAL position tracking (`LSN` for PostgreSQL, `BinlogPosition` for MySQL)
- Backpressure handling
- Connection pooling

**Interfaces**:
```go
type WALReader interface {
    Start(ctx context.Context) error
    ReadWAL(ctx context.Context, position WALPosition) (<-chan *WALEvent, error)
    GetCurrentPosition(ctx context.Context) (WALPosition, error)
    Stop(ctx context.Context) error
}
```

### 2. CDC Parser

**Purpose**: Parse WAL entries into structured events

**Features**:
- Parse DDL and DML operations
- Extract schema information
- Handle transaction boundaries
- Support table filtering

**Data Models**:
```go
type WALEvent struct {
    Position     WALPosition
    Timestamp    time.Time
    Operation    OperationType  // INSERT, UPDATE, DELETE, DDL
    Schema       string
    Table        string
    OldValues    map[string]interface{}
    NewValues    map[string]interface{}
    TransactionID string
}
```

### 3. Event Transformer

**Purpose**: Transform database events into standardized NATS message format

**Features**:
- Schema evolution handling
- Event enrichment
- Data type mapping
- Sensitive data filtering

**Output Format**:
```go
type CDCEvent struct {
    EventID       string    `json:"event_id"`
    EventType     string    `json:"event_type"`    // cdc.insert, cdc.update, cdc.delete, cdc.ddl
    Source        string    `json:"source"`        // database identifier
    Timestamp     time.Time `json:"timestamp"`
    Schema        string    `json:"schema"`
    Table         string    `json:"table"`
    Operation     string    `json:"operation"`
    Before        map[string]interface{} `json:"before,omitempty"`
    After         map[string]interface{} `json:"after,omitempty"`
    Metadata      map[string]interface{} `json:"metadata,omitempty"`
}
```

### 4. NATS Publisher

**Purpose**: Publish CDC events to NATS Jetstream

**Features**:
- Connection management
- Message serialization (JSON)
- Retry logic with exponential backoff
- Load balancing across NATS cluster
- Message acknowledgment handling

**Interface**:
```go
type NATSPublisher interface {
    Connect() error
    Publish(ctx context.Context, subject string, data []byte) error
    PublishWithRetries(ctx context.Context, subject string, data []byte, maxRetries int) error
    Close() error
}
```

### 5. Checkpoint Manager

**Purpose**: Track processed WAL positions for restart recovery

**Features**:
- Persistent storage of checkpoints
- Dual-write safety
- Periodic checkpointing
- Recovery mechanisms

**Storage Options**:
- File-based (local filesystem)
- Redis (distributed checkpoints)
- AWS S3 (durable storage)

## Data Flow

```
1. WAL Reader connects to AWS RDS and starts streaming
2. WAL entries are parsed into structured events by CDC Parser
3. Events are transformed into standardized CDC format
4. Transformed events are published to NATS Jetstream subjects:
   - cdc.{database}.{schema}.{table}
5. Checkpoint is updated after successful publication
6. Errors trigger retry logic and checkpoint rollback if needed
```

## Configuration

### Environment Variables

```bash
# AWS RDS Configuration
AWS_RDS_ENDPOINT=rds-instance.xxxxx.region.rds.amazonaws.com
AWS_RDS_USERNAME=replicator
AWS_RDS_PASSWORD=secure_password
AWS_RDS_DATABASE=production_db
AWS_RDS_REGION=us-east-1

# NATS Configuration  
NATS_URL=nats://nats-cluster:4222
NATS_USERNAME=nats_user
NATS_PASSWORD=nats_password
NATS_CLUSTER_ID=prod-cluster

# CDC Handler Configuration
CHECKPOINT_STORAGE=redis://localhost:6379
CHECKPOINT_INTERVAL=10s
MAX_RETRY_ATTEMPTS=3
BATCH_SIZE=1000
BATCH_TIMEOUT=5s

# Monitoring
PROMETHEUS_PORT=8080
LOG_LEVEL=info
```

### Configuration File (config.yaml)

```yaml
server:
  port: 8080
  grace_period: 30s

rds:
  endpoint: ${AWS_RDS_ENDPOINT}
  username: ${AWS_RDS_USERNAME}
  password: ${AWS_RDS_PASSWORD}
  database: ${AWS_RDS_DATABASE}
  region: ${AWS_RDS_REGION}
  ssl_mode: require
  max_connections: 10

nats:
  urls:
    - ${NATS_URL}
  username: ${NATS_USERNAME}
  password: ${NATS_PASSWORD}
  cluster_id: ${NATS_CLUSTER_ID}
  request_timeout: 10s

cdc:
  checkpoint_storage: ${CHECKPOINT_STORAGE}
  checkpoint_interval: ${CHECKPOINT_INTERVAL}
  max_retry_attempts: ${MAX_RETRY_ATTEMPTS}
  batch_size: ${BATCH_SIZE}
  batch_timeout: ${BATCH_TIMEOUT}
  table_filters:
    - "schema1.table1"
    - "schema2.table2"
  
  # Schema evolution handling
  schema_evolution:
    enabled: true
    strict_mode: false

  # Security
  sensitive_fields:
    - "password"
    - "credit_card"
    - "ssn"

monitoring:
  prometheus:
    enabled: true
    port: ${PROMETHEUS_PORT}
  
  logging:
    level: ${LOG_LEVEL}
    format: json
```

## Security

### Data Protection

1. **Encryption in Transit**:
   - TLS 1.2+ for AWS RDS connections
   - TLS for NATS connections
   - Certificate validation

2. **Encryption at Rest**:
   - AWS RDS encryption
   - Encrypted checkpoint storage
   - Secure credential storage

3. **Access Control**:
   - Minimal AWS IAM permissions for RDS replication
   - NATS authentication and authorization
   - Principle of least privilege

4. **Data Filtering**:
   - Field-level filtering for sensitive data
   - Schema-based access control
   - PII detection and redaction

### AWS IAM Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "rds:DescribeDBInstances",
                "rds:DescribeDBClusters",
                "rds:DescribeDBClusterSnapshots",
                "rds:DescribeDBLogFiles",
                "rds:DownloadDBLogFilePortion"
            ],
            "Resource": "*"
        }
    ]
}
```

## Error Handling & Resilience

### Retry Strategy

1. **Exponential Backoff**: 1s, 2s, 4s, 8s intervals
2. **Circuit Breaker**: Stop retries after threshold
3. **Dead Letter Queue**: Move failed events to DLQ
4. **Compensation**: Rollback checkpoints on critical failures

### Fault Tolerance

- **Checkpoint Atomicity**: Dual-write with transactional boundaries
- **Graceful Shutdown**: Clean connection closure and checkpoint flush
- **Connection Resilience**: Automatic reconnection with jitter
- **Memory Management**: Bounded buffers and backpressure handling

### Monitoring & Alerting

**Key Metrics**:
- Events processed per second
- Event processing latency
- Error rates by type
- Checkpoint lag
- Connection status
- Memory and CPU usage

**Health Checks**:
- RDS connectivity
- NATS connectivity
- Checkpoint storage availability
- Memory usage thresholds

## Performance Considerations

### Optimization Strategies

1. **Batching**:
   - Batch multiple events before NATS publication
   - Configurable batch size and timeout
   - Adaptive batching based on load

2. **Connection Pooling**:
   - Reuse connections to NATS
   - Connection health monitoring
   - Load balancing across NATS cluster

3. **Memory Management**:
   - Stream processing without materialization
   - Object pooling for frequent allocations
   - GC pressure minimization

4. **Parallel Processing**:
   - Concurrent parsing and publishing
   - Separate goroutines for I/O operations
   - Backpressure coordination

### Scalability

**Horizontal Scaling**:
- Multiple CDC handler instances
- Sharding by database/table
- Load distribution across NATS cluster

**Vertical Scaling**:
- CPU optimization for parsing
- Memory tuning for buffering
- Network optimization

## Deployment

### Docker Configuration

```dockerfile
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main cmd/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/main .
COPY --from=builder /app/configs/ ./configs/

CMD ["./main"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cdc-handler
spec:
  replicas: 3
  selector:
    matchLabels:
      app: cdc-handler
  template:
    metadata:
      labels:
        app: cdc-handler
    spec:
      containers:
      - name: cdc-handler
        image: cdc-handler:latest
        env:
        - name: AWS_RDS_ENDPOINT
          valueFrom:
            secretKeyRef:
              name: rds-credentials
              key: endpoint
        - name: AWS_RDS_USERNAME
          valueFrom:
            secretKeyRef:
              name: rds-credentials
              key: username
        - name: AWS_RDS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: rds-credentials
              key: password
        ports:
        - containerPort: 8080
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
```

## Testing Strategy

### Unit Testing

- WAL parsing logic
- Event transformation
- NATS publishing
- Error handling scenarios

### Integration Testing

- AWS RDS connection
- End-to-end event flow
- Checkpoint persistence
- Connection recovery

### Load Testing

- High-volume event processing
- Connection failure scenarios
- Memory leak detection
- Performance benchmarking

## Future Enhancements

1. **Schema Registry Integration**: Dynamic schema evolution
2. **Event Versioning**: Support for multiple event versions
3. **Data Quality Monitoring**: Event validation and anomaly detection
4. **Multi-Database Support**: Oracle, SQL Server support
5. **Cloud-Native Scaling**: Kubernetes operators
6. **Advanced Analytics**: Event processing metrics and insights