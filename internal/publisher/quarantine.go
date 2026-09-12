package publisher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"better-cdc/internal/model"
	"github.com/nats-io/nats.go"
)

// quarantineStage is overridden only in integration builds for deterministic crash tests.
var quarantineStage = func(string) {}

type RecoveryObject struct {
	Version  int                   `json:"version"`
	EventID  string                `json:"event_id"`
	Database string                `json:"database"`
	Subject  string                `json:"subject"`
	Payload  []byte                `json:"payload,omitempty"`
	Recovery *model.RecoveryChange `json:"recovery,omitempty"`
}

func digest(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }

func (p *JetStreamPublisher) ensureQuarantine() error {
	if !p.opts.EnableDLQ {
		return nil
	}
	expected := &nats.StreamConfig{Name: p.opts.DLQStream, Subjects: []string{p.opts.DLQSubjectPrefix + ".>"}, Storage: nats.FileStorage, Replicas: p.opts.StreamReplicas, Retention: nats.LimitsPolicy, Discard: nats.DiscardNew, MaxBytes: p.opts.DLQIndexMaxBytes, Duplicates: p.opts.DuplicateWindow}
	info, err := p.js.StreamInfo(expected.Name)
	if errors.Is(err, nats.ErrStreamNotFound) {
		info, err = p.js.AddStream(expected)
	}
	if err != nil {
		return fmt.Errorf("DLQ index: %w", err)
	}
	if err = validateQuarantineStream(&info.Config, expected); err != nil {
		return err
	}
	obj, err := p.js.ObjectStore(p.opts.DLQBucket)
	if errors.Is(err, nats.ErrBucketNotFound) || errors.Is(err, nats.ErrStreamNotFound) {
		obj, err = p.js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: p.opts.DLQBucket, Storage: nats.FileStorage, Replicas: p.opts.StreamReplicas, MaxBytes: p.opts.DLQMaxBytes})
	}
	if err != nil {
		return fmt.Errorf("DLQ object store: %w", err)
	}
	p.objects = obj
	bucketInfo, err := p.js.StreamInfo("OBJ_" + p.opts.DLQBucket)
	if err != nil {
		return err
	}
	if bucketInfo.Config.Storage != nats.FileStorage || bucketInfo.Config.MaxAge != 0 || bucketInfo.Config.Discard != nats.DiscardNew || bucketInfo.Config.MaxBytes != p.opts.DLQMaxBytes || bucketInfo.Config.Replicas != p.opts.StreamReplicas {
		return fmt.Errorf("DLQ bucket must use file storage, matching replicas/capacity, no expiry and discard-new")
	}
	return nil
}
func validateQuarantineStream(actual, expected *nats.StreamConfig) error {
	if !sameSubjects(actual.Subjects, expected.Subjects) || actual.Storage != nats.FileStorage || actual.MaxAge != 0 || actual.MaxMsgs > 0 || actual.MaxMsgsPerSubject > 0 || actual.Retention != nats.LimitsPolicy || actual.Discard != nats.DiscardNew || actual.MaxBytes != expected.MaxBytes || actual.Replicas != expected.Replicas {
		return fmt.Errorf("DLQ index must retain all records without expiry or eviction, using configured capacity and replicas")
	}
	return nil
}

func (p *JetStreamPublisher) Quarantine(ctx context.Context, prefix string, rec *DeadLetterRecord) error {
	ctx, cancel := context.WithTimeout(ctx, p.publishTimeout())
	defer cancel()
	if p.objects == nil || prefix != p.opts.DLQSubjectPrefix {
		return fmt.Errorf("durable quarantine is not configured")
	}
	if rec.EventID == "" || (len(rec.Payload) == 0 && rec.Recovery == nil) {
		return fmt.Errorf("missing complete recovery data")
	}
	object := RecoveryObject{Version: 1, EventID: rec.EventID, Database: rec.Database, Subject: rec.Subject, Payload: rec.Payload, Recovery: rec.Recovery}
	data, err := json.Marshal(object)
	if err != nil {
		return err
	}
	name := digest([]byte(rec.Database + "\x00" + rec.EventID))
	checksum := digest(data)
	// Reuse an existing immutable object across retries and restarts. Never
	// overwrite recovery bytes when a software/configuration change alters output.
	quarantineStage("before_object")
	existing, err := p.readObject(ctx, name)
	if errors.Is(err, nats.ErrObjectNotFound) {
		chunk := int(p.nc.MaxPayload() / 2)
		if chunk > 128<<10 {
			chunk = 128 << 10
		}
		if chunk < 1 {
			return fmt.Errorf("NATS payload limit too small")
		}
		_, err = p.objects.Put(&nats.ObjectMeta{Name: name, Opts: &nats.ObjectMetaOptions{ChunkSize: uint32(chunk)}}, bytes.NewReader(data), nats.Context(ctx))
		if err == nil {
			existing, err = p.readObject(ctx, name)
		}
	}
	if err != nil {
		return fmt.Errorf("store recovery object: %w", err)
	}
	if digest(existing) != checksum {
		return fmt.Errorf("recovery object checksum conflict for %s", name)
	}
	quarantineStage("object_verified")
	index := *rec
	index.Payload = nil
	index.Recovery = nil
	index.Object = name
	index.SHA256 = checksum
	body, err := json.Marshal(index)
	if err != nil {
		return err
	}
	_, err = p.js.Publish(DeadLetterSubject(prefix, rec.Database, rec.Schema, rec.Table), body, nats.MsgId("dlq-"+name), nats.ExpectStream(p.opts.DLQStream), nats.Context(ctx))
	if err == nil {
		quarantineStage("index_acked")
	}
	return err
}
func (p *JetStreamPublisher) readObject(ctx context.Context, name string) ([]byte, error) {
	obj, err := p.objects.Get(name, nats.Context(ctx))
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	return io.ReadAll(io.LimitReader(obj, p.opts.DLQMaxBytes+1))
}
func (p *JetStreamPublisher) LoadRecovery(ctx context.Context, index DeadLetterRecord) (*RecoveryObject, error) {
	data, err := p.readObject(ctx, index.Object)
	if err != nil {
		return nil, err
	}
	if digest(data) != index.SHA256 {
		return nil, fmt.Errorf("recovery checksum mismatch")
	}
	var obj RecoveryObject
	if err = json.Unmarshal(data, &obj); err != nil {
		return nil, err
	}
	if obj.Version != 1 || obj.EventID != index.EventID || obj.Database != index.Database {
		return nil, fmt.Errorf("recovery identity/version mismatch")
	}
	return &obj, nil
}

// WalkDLQ inspects retained index records without creating a consumer or acknowledging work.
func (p *JetStreamPublisher) WalkDLQ(ctx context.Context, visit func(uint64, DeadLetterRecord) error) error {
	info, err := p.js.StreamInfo(p.opts.DLQStream, nats.Context(ctx))
	if err != nil {
		return err
	}
	for seq := info.State.FirstSeq; seq > 0 && seq <= info.State.LastSeq; seq++ {
		msg, err := p.js.GetMsg(p.opts.DLQStream, seq, nats.Context(ctx))
		if errors.Is(err, nats.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		var rec DeadLetterRecord
		if err = json.Unmarshal(msg.Data, &rec); err != nil {
			return err
		}
		if err = visit(seq, rec); err != nil {
			return err
		}
	}
	return nil
}

// RedriveDLQ uses one persistent consumer; a crash after publish but before its
// acknowledgement can replay the original EventID, so downstream dedup remains required.
func (p *JetStreamPublisher) RedriveDLQ(ctx context.Context, publish func(context.Context, *RecoveryObject) error) error {
	_, err := p.js.ConsumerInfo(p.opts.DLQStream, "redrive", nats.Context(ctx))
	if errors.Is(err, nats.ErrConsumerNotFound) {
		_, err = p.js.AddConsumer(p.opts.DLQStream, &nats.ConsumerConfig{Durable: "redrive", FilterSubject: p.opts.DLQSubjectPrefix + ".>", AckPolicy: nats.AckExplicitPolicy, MaxAckPending: 1, AckWait: 5 * time.Minute}, nats.Context(ctx))
	}
	if err != nil {
		return err
	}
	sub, err := p.js.PullSubscribe(p.opts.DLQSubjectPrefix+".>", "redrive", nats.Bind(p.opts.DLQStream, "redrive"))
	if err != nil {
		return err
	}
	defer func() { _ = sub.Unsubscribe() }()
	for {
		msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
		if errors.Is(err, nats.ErrTimeout) {
			return nil
		}
		if err != nil {
			return err
		}
		msg := msgs[0]
		var rec DeadLetterRecord
		if err = json.Unmarshal(msg.Data, &rec); err != nil {
			return err
		}
		object, err := p.LoadRecovery(ctx, rec)
		if err == nil {
			err = publish(ctx, object)
		}
		if err != nil {
			_ = msg.Nak()
			return err
		}
		if err = msg.AckSync(nats.Context(ctx)); err != nil {
			return err
		}
	}
}
