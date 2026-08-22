package api

import (
	"context"
	"net/http"
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

func TestRateLimiterAllowsUpToLimit(t *testing.T) {
	client, _ := newTestRedisClient(t)
	rl := NewRateLimiter(client, 3)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		allowed, _, err := rl.Allow(ctx, "client-a")
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i, err)
		}
		if !allowed {
			t.Fatalf("request %d: expected allowed, got denied", i)
		}
	}

	allowed, retryAfter, err := rl.Allow(ctx, "client-a")
	if err != nil {
		t.Fatalf("4th request: unexpected error: %v", err)
	}
	if allowed {
		t.Fatalf("4th request: expected denied after exceeding limit of 3, got allowed")
	}
	if retryAfter <= 0 {
		t.Errorf("expected a positive retryAfter when denied, got %v", retryAfter)
	}
}

func TestRateLimiterKeysAreIndependent(t *testing.T) {
	client, _ := newTestRedisClient(t)
	rl := NewRateLimiter(client, 1)
	ctx := context.Background()

	if allowed, _, err := rl.Allow(ctx, "client-a"); err != nil || !allowed {
		t.Fatalf("client-a first request should be allowed, got allowed=%v err=%v", allowed, err)
	}
	if allowed, _, err := rl.Allow(ctx, "client-a"); err != nil || allowed {
		t.Fatalf("client-a second request should be denied, got allowed=%v err=%v", allowed, err)
	}

	// A different key must have its own independent budget.
	if allowed, _, err := rl.Allow(ctx, "client-b"); err != nil || !allowed {
		t.Fatalf("client-b first request should be allowed regardless of client-a's state, got allowed=%v err=%v", allowed, err)
	}
}

func TestRateLimiterResetsAfterWindow(t *testing.T) {
	client, mr := newTestRedisClient(t)
	rl := NewRateLimiter(client, 1)
	ctx := context.Background()

	if allowed, _, err := rl.Allow(ctx, "client-a"); err != nil || !allowed {
		t.Fatalf("first request should be allowed, got allowed=%v err=%v", allowed, err)
	}
	if allowed, _, err := rl.Allow(ctx, "client-a"); err != nil || allowed {
		t.Fatalf("second request in the same window should be denied, got allowed=%v err=%v", allowed, err)
	}

	mr.FastForward(time.Minute + time.Second)

	if allowed, _, err := rl.Allow(ctx, "client-a"); err != nil || !allowed {
		t.Fatalf("request in a new window should be allowed again, got allowed=%v err=%v", allowed, err)
	}
}

func TestClientIPPrefersXForwardedFor(t *testing.T) {
	req, _ := http.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:54321"
	req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.2")

	if got := clientIP(req); got != "203.0.113.7" {
		t.Errorf("clientIP() = %q, want %q (first hop of X-Forwarded-For)", got, "203.0.113.7")
	}
}

func TestClientIPFallsBackToRemoteAddr(t *testing.T) {
	req, _ := http.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:54321"

	if got := clientIP(req); got != "10.0.0.1" {
		t.Errorf("clientIP() = %q, want %q (host portion of RemoteAddr)", got, "10.0.0.1")
	}
}
