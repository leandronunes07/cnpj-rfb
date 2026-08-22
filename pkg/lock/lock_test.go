package lock

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedisClient(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed starting miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return client, mr
}

func TestLocalLockPreventsConcurrentAcquire(t *testing.T) {
	l := NewLocal()
	ctx := context.Background()

	ok, err := l.TryAcquire(ctx)
	if err != nil || !ok {
		t.Fatalf("expected first TryAcquire to succeed, got ok=%v err=%v", ok, err)
	}

	ok, err = l.TryAcquire(ctx)
	if err != nil || ok {
		t.Fatalf("expected second TryAcquire to fail while held, got ok=%v err=%v", ok, err)
	}

	l.Release(ctx)

	ok, err = l.TryAcquire(ctx)
	if err != nil || !ok {
		t.Fatalf("expected TryAcquire to succeed after Release, got ok=%v err=%v", ok, err)
	}
}

func TestRedisLockPreventsConcurrentAcquireAcrossInstances(t *testing.T) {
	client, _ := newTestRedisClient(t)
	ctx := context.Background()

	// Two independent lock instances sharing the same Redis key simulate
	// two separate app instances/replicas racing for the same pipeline run.
	instanceA := NewRedis(client, "cnpjrbf:test:lock")
	instanceB := NewRedis(client, "cnpjrbf:test:lock")

	ok, err := instanceA.TryAcquire(ctx)
	if err != nil || !ok {
		t.Fatalf("expected instance A to acquire the lock, got ok=%v err=%v", ok, err)
	}
	defer instanceA.Release(ctx)

	ok, err = instanceB.TryAcquire(ctx)
	if err != nil || ok {
		t.Fatalf("expected instance B to be blocked while A holds the lock, got ok=%v err=%v", ok, err)
	}
}

func TestRedisLockReleaseAllowsReacquire(t *testing.T) {
	client, _ := newTestRedisClient(t)
	ctx := context.Background()

	instanceA := NewRedis(client, "cnpjrbf:test:lock")
	instanceB := NewRedis(client, "cnpjrbf:test:lock")

	if ok, err := instanceA.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("instance A failed to acquire: ok=%v err=%v", ok, err)
	}
	instanceA.Release(ctx)

	if ok, err := instanceB.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("expected instance B to acquire after A released, got ok=%v err=%v", ok, err)
	}
	instanceB.Release(ctx)
}

// Release must only delete the key if it still holds this instance's own
// token — otherwise instance A releasing after its key already expired
// (and got legitimately re-acquired by instance B) would delete B's live
// lock out from under it.
func TestRedisLockReleaseDoesNotStealAnotherHoldersLock(t *testing.T) {
	client, mr := newTestRedisClient(t)
	ctx := context.Background()

	instanceA := NewRedis(client, "cnpjrbf:test:lock")
	if ok, err := instanceA.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("instance A failed to acquire: ok=%v err=%v", ok, err)
	}

	// Simulate A's key expiring (e.g. it crashed and stopped heartbeating)
	// and B legitimately acquiring the now-free lock.
	mr.FastForward(defaultTTL + time.Second)

	instanceB := NewRedis(client, "cnpjrbf:test:lock")
	if ok, err := instanceB.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("instance B failed to acquire after A's key expired: ok=%v err=%v", ok, err)
	}

	// A (unaware it already lost the lock) tries to release its stale token.
	instanceA.Release(ctx)

	// B's lock must still be in place.
	val, err := client.Get(ctx, "cnpjrbf:test:lock").Result()
	if err != nil {
		t.Fatalf("expected B's lock key to still exist, got error: %v", err)
	}
	if val != instanceB.token {
		t.Fatalf("lock key was overwritten/removed by A's stale release; got %q, want instance B's token", val)
	}
}
