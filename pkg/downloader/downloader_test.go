package downloader

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestDownloadStreamToChannel(t *testing.T) {
	var requestCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.Header().Set("Content-Length", "11")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello world"))
	}))
	defer server.Close()

	tmpDir, err := os.MkdirTemp("", "downloader_test_*")
	if err != nil {
		t.Fatalf("failed creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	d := NewDownloader(2, "")

	tasks := []DownloadTask{
		{
			URL:      server.URL + "/file1.zip",
			DestPath: filepath.Join(tmpDir, "file1.zip"),
			Filename: "file1.zip",
		},
		{
			URL:      server.URL + "/file2.zip",
			DestPath: filepath.Join(tmpDir, "file2.zip"),
			Filename: "file2.zip",
		},
		{
			URL:      server.URL + "/file3.zip",
			DestPath: filepath.Join(tmpDir, "file3.zip"),
			Filename: "file3.zip",
		},
	}

	outChan := make(chan DownloadTask, len(tasks))

	err = d.DownloadStreamToChannel(tasks, outChan)
	if err != nil {
		t.Fatalf("DownloadStreamToChannel returned unexpected error: %v", err)
	}

	var received []DownloadTask
	for task := range outChan {
		received = append(received, task)
		info, statErr := os.Stat(task.DestPath)
		if statErr != nil {
			t.Errorf("expected file %s to exist, err: %v", task.DestPath, statErr)
		} else if info.Size() != 11 {
			t.Errorf("expected file %s size to be 11, got %d", task.DestPath, info.Size())
		}
	}

	if len(received) != len(tasks) {
		t.Errorf("expected %d tasks received from outChan, got %d", len(tasks), len(received))
	}
}

func TestDownloadStreamToChannel_Empty(t *testing.T) {
	d := NewDownloader(2, "")
	outChan := make(chan DownloadTask, 5)

	err := d.DownloadStreamToChannel(nil, outChan)
	if err != nil {
		t.Fatalf("expected nil error for empty tasks, got: %v", err)
	}

	count := 0
	for range outChan {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 tasks, got %d", count)
	}
}
