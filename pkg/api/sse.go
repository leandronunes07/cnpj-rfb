package api

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// sseHistoryLimit bounds how many recent log lines are replayed to a client
// that connects after the fact — without it, opening the dashboard after the
// pipeline already started (which is the common case: the ETL kicks off on
// boot, before anyone has had a chance to open a browser) shows a blank
// console with no way to tell whether anything happened.
const sseHistoryLimit = 200

// ssePingInterval sends a periodic SSE comment so reverse proxies in front
// of the app (nginx/Traefik in front of Portainer/Easypanel deployments,
// which is the typical setup here) don't treat a quiet connection as idle
// and kill it, and so the client can tell the stream is still alive.
const ssePingInterval = 15 * time.Second

type SSEBroadcaster struct {
	mu      sync.Mutex
	clients map[chan string]bool
	history []string
}

var GlobalBroadcaster = &SSEBroadcaster{
	clients: make(map[chan string]bool),
}

func (b *SSEBroadcaster) Broadcast(message string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.history = append(b.history, message)
	if len(b.history) > sseHistoryLimit {
		b.history = b.history[len(b.history)-sseHistoryLimit:]
	}

	for client := range b.clients {
		select {
		case client <- message:
		default:
		}
	}
}

func (b *SSEBroadcaster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	// Nginx (a common reverse proxy in front of Portainer/Easypanel setups)
	// buffers proxied responses by default, which silently breaks SSE: the
	// browser sees nothing until the buffer fills or the connection ends.
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, canFlush := w.(http.Flusher)

	clientChan := make(chan string, 100)

	b.mu.Lock()
	backlog := make([]string, len(b.history))
	copy(backlog, b.history)
	b.clients[clientChan] = true
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.clients, clientChan)
		b.mu.Unlock()
		close(clientChan)
	}()

	for _, msg := range backlog {
		fmt.Fprintf(w, "data: %s\n\n", msg)
	}
	if canFlush {
		flusher.Flush()
	}

	ticker := time.NewTicker(ssePingInterval)
	defer ticker.Stop()

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case <-ticker.C:
			// Comment line per the SSE spec: EventSource ignores it, but it
			// keeps the connection alive through idle-timeout proxies.
			fmt.Fprint(w, ": ping\n\n")
			if canFlush {
				flusher.Flush()
			}
		case msg := <-clientChan:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			if canFlush {
				flusher.Flush()
			}
		}
	}
}

type LogBroadcasterWriter struct {
	stdOut io.Writer
}

func NewLogBroadcasterWriter() io.Writer {
	return &LogBroadcasterWriter{
		stdOut: os.Stderr,
	}
}

func (w *LogBroadcasterWriter) Write(p []byte) (n int, err error) {
	msg := strings.TrimSpace(string(p))
	if msg != "" {
		GlobalBroadcaster.Broadcast(msg)
	}
	return w.stdOut.Write(p)
}
