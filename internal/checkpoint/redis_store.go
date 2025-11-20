package checkpoint

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"better-cdc/internal/model"
)

// RedisStore persists checkpoints into Redis with TTL to avoid stale entries.
type RedisStore struct {
	client *redis.Client
	key    string
	ttl    time.Duration
}

func NewRedisStore(client *redis.Client, key string, ttl time.Duration) *RedisStore {
	return &RedisStore{
		client: client,
		key:    key,
		ttl:    ttl,
	}
}

func (s *RedisStore) Save(ctx context.Context, pos model.WALPosition) error {
	if pos.LSN == "" {
		return fmt.Errorf("empty LSN")
	}
	if err := s.client.Set(ctx, s.key, pos.LSN, s.ttl).Err(); err != nil {
		return fmt.Errorf("redis set checkpoint: %w", err)
	}
	return nil
}

func (s *RedisStore) Load(ctx context.Context) (model.WALPosition, error) {
	lsn, err := s.client.Get(ctx, s.key).Result()
	if err != nil {
		if err == redis.Nil {
			return model.WALPosition{}, nil
		}
		return model.WALPosition{}, fmt.Errorf("redis get checkpoint: %w", err)
	}
	return model.WALPosition{LSN: lsn}, nil
}
