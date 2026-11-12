package ingest

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// fixed window на Lua: проверка и инкремент атомарно,
// иначе две реплики ingest могут одновременно пройти лимит
var limiterScript = redis.NewScript(`
local n      = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])
local window = tonumber(ARGV[3])
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current + n > limit then
  local ttl = redis.call('PTTL', KEYS[1])
  if ttl < 0 then ttl = window end
  return {0, ttl}
end
local v = redis.call('INCRBY', KEYS[1], n)
if v == n then
  redis.call('PEXPIRE', KEYS[1], window)
end
return {1, 0}
`)

type RedisLimiter struct {
	rdb    redis.Scripter
	limit  int
	window time.Duration
}

func NewRedisLimiter(rdb redis.Scripter, perSecond int) *RedisLimiter {
	return &RedisLimiter{rdb: rdb, limit: perSecond, window: time.Second}
}

func (l *RedisLimiter) Allow(ctx context.Context, key string, n int) (bool, time.Duration, error) {
	window := time.Now().UnixMilli() / l.window.Milliseconds()
	redisKey := "ratelimit:ingest:" + key + ":" + strconv.FormatInt(window, 10)

	res, err := limiterScript.Run(ctx, l.rdb, []string{redisKey}, n, l.limit, l.window.Milliseconds()).Int64Slice()
	if err != nil {
		return false, 0, err
	}
	return res[0] == 1, time.Duration(res[1]) * time.Millisecond, nil
}
