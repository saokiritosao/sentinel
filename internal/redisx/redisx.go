package redisx

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"sentinel/internal/config"
)

func Connect(ctx context.Context, c config.Redis, log *slog.Logger) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         c.Addr,
		Password:     c.Password,
		DB:           c.DB,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := rdb.Ping(ctx).Err()
		if err == nil {
			return rdb, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			_ = rdb.Close()
			return nil, fmt.Errorf("redis not reachable: %w", err)
		}
		log.Warn("waiting for redis", "err", err)
		time.Sleep(time.Second)
	}
}
