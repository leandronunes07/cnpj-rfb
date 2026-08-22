package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter enforces a fixed-window per-client request budget backed by
// Redis, so the limit holds across every instance of the app sharing the
// same Redis rather than resetting per process. Nil-safe by construction:
// callers check for a nil *RateLimiter and skip limiting entirely, which is
// what happens whenever REDIS_ADDR isn't configured.
type RateLimiter struct {
	client *redis.Client
	limit  int
	window time.Duration
}

func NewRateLimiter(client *redis.Client, limitPerWindow int) *RateLimiter {
	return &RateLimiter{client: client, limit: limitPerWindow, window: time.Minute}
}

// Allow reports whether the request identified by key may proceed. When it
// can't, retryAfter is how long until the current window resets.
func (rl *RateLimiter) Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error) {
	windowSeconds := int64(rl.window.Seconds())
	now := time.Now().Unix()
	windowID := now / windowSeconds
	redisKey := fmt.Sprintf("cnpjrbf:ratelimit:%s:%d", key, windowID)

	count, err := rl.client.Incr(ctx, redisKey).Result()
	if err != nil {
		return false, 0, err
	}
	if count == 1 {
		// Only the request that created the counter needs to set its
		// expiry; every subsequent INCR in the same window just bumps it.
		rl.client.Expire(ctx, redisKey, rl.window)
	}

	if count > int64(rl.limit) {
		elapsedInWindow := now % windowSeconds
		return false, time.Duration(windowSeconds-elapsedInWindow) * time.Second, nil
	}
	return true, 0, nil
}

// clientIP extracts the caller's address for rate-limit keying. It trusts
// X-Forwarded-For when present, which only makes sense because this app is
// documented as always running behind a reverse proxy in production (see
// docs/TROUBLESHOOTING.md) — a proxy that sets/overwrites the header itself.
// Deployed with the API port directly internet-facing (against that
// guidance), a client could spoof this to dodge its own rate limit.
func clientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		if first, _, ok := strings.Cut(fwd, ","); ok {
			return strings.TrimSpace(first)
		}
		return strings.TrimSpace(fwd)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
