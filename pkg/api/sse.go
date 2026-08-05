package api

import (
	"fmt"
	"net/http"
	"sync"
)

type SSEBroadcaster struct {
	mu      sync.Mutex
	clients map[chan string]bool
}

var GlobalBroadcaster = &SSEBroadcaster{
	clients: make(map[chan string]bool),
}

func (b *SSEBroadcaster) Broadcast(message string) {
	b.mu.Lock()
	defer b.mu.Unlock()

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

	clientChan := make(chan string, 10)

	b.mu.Lock()
	b.clients[clientChan] = true
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.clients, clientChan)
		b.mu.Unlock()
		close(clientChan)
	}()

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case msg := <-clientChan:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}
}
