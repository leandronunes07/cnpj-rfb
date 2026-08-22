// Package lock coordinates single-flight execution of the ETL pipeline.
//
// In a single-instance deployment an in-process flag is enough — that's
// what this project used until now. Once multiple instances of the app run
// against the same database (e.g. behind a load balancer, or several
// Portainer/Kubernetes replicas), an in-process flag can't see what the
// other instances are doing: each one thinks it's the only one, and two
// instances can end up importing the same files at the same time. Only a
// lock shared outside the process (Redis) actually prevents that.
package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// PipelineLock coordinates single-flight execution of the ETL pipeline.
type PipelineLock interface {
	// TryAcquire attempts to take the lock. Returns false (no error) if
	// another holder already has it — that is the expected, non-error way
	// of losing the race, not a failure.
	TryAcquire(ctx context.Context) (bool, error)
	// Release gives up the lock. Safe to call even if TryAcquire returned
	// false or was never called.
	Release(ctx context.Context)
}

// Local is an in-process lock: correct for a single running instance, but
// invisible to any other instance of the app. This is the default when no
// Redis connection is configured.
type Local struct {
	held atomic.Bool
}

func NewLocal() *Local {
	return &Local{}
}

func (l *Local) TryAcquire(_ context.Context) (bool, error) {
	return l.held.CompareAndSwap(false, true), nil
}

func (l *Local) Release(_ context.Context) {
	l.held.Store(false)
}

// releaseScript deletes the lock key only if it still holds this instance's
// token — a plain DEL would risk deleting a lock some other instance
// legitimately acquired after this one's key expired (e.g. this instance
// stalled longer than the TTL). The check-and-delete has to be atomic on
// the Redis side, hence the Lua script instead of a GET-then-DEL from Go.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`)

// Redis is a distributed lock backed by a Redis key with a TTL, renewed by
// a background heartbeat while held. The TTL exists so that if an instance
// crashes or is killed while holding the lock, it self-clears instead of
// blocking every other instance forever; the heartbeat exists so a run that
// legitimately takes longer than the TTL doesn't lose the lock mid-flight.
type Redis struct {
	client *redis.Client
	key    string
	ttl    time.Duration
	token  string

	stopHeartbeat chan struct{}
}

const defaultTTL = 5 * time.Minute

func NewRedis(client *redis.Client, key string) *Redis {
	return &Redis{
		client: client,
		key:    key,
		ttl:    defaultTTL,
		token:  randomToken(),
	}
}

func (l *Redis) TryAcquire(ctx context.Context) (bool, error) {
	ok, err := l.client.SetNX(ctx, l.key, l.token, l.ttl).Result()
	if err != nil {
		return false, err
	}
	if ok {
		l.startHeartbeat()
	}
	return ok, nil
}

func (l *Redis) Release(ctx context.Context) {
	if l.stopHeartbeat != nil {
		close(l.stopHeartbeat)
		l.stopHeartbeat = nil
	}
	// Best-effort: if this fails the key still expires on its own via TTL.
	_ = releaseScript.Run(ctx, l.client, []string{l.key}, l.token).Err()
}

func (l *Redis) startHeartbeat() {
	l.stopHeartbeat = make(chan struct{})
	stop := l.stopHeartbeat
	go func() {
		ticker := time.NewTicker(l.ttl / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				l.client.Expire(context.Background(), l.key, l.ttl)
			case <-stop:
				return
			}
		}
	}()
}

func randomToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing is effectively unrecoverable on any real
		// platform; fall back to a fixed value rather than panicking —
		// worst case this instance's lock token collides with another's,
		// which only matters if both happen to crash at the exact same
		// moment while holding the lock, an already-rare double failure.
		return "cnpjrbf-fallback-token"
	}
	return hex.EncodeToString(b)
}
