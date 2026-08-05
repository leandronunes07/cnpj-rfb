package downloader

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Downloader struct {
	Workers    int
	HTTPClient *http.Client
}

func NewDownloader(workers int) *Downloader {
	return &Downloader{
		Workers: workers,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Minute, // Long timeout for large zip downloads
		},
	}
}

type DownloadTask struct {
	URL      string
	DestPath string
	Filename string
}

func (d *Downloader) DownloadAll(tasks []DownloadTask) error {
	if len(tasks) == 0 {
		log.Println("[Downloader] Nenhum arquivo para baixar.")
		return nil
	}

	log.Printf("[Downloader] Iniciando download de %d arquivo(s) com %d worker(s)...", len(tasks), d.Workers)

	taskChan := make(chan DownloadTask, len(tasks))
	errChan := make(chan error, len(tasks))
	var wg sync.WaitGroup

	// Launch worker pool
	for i := 1; i <= d.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range taskChan {
				log.Printf("[Worker %d] Baixando %s...", workerID, task.Filename)
				if err := d.downloadFile(task); err != nil {
					log.Printf("[Worker %d] ERRO ao baixar %s: %v", workerID, task.Filename, err)
					errChan <- fmt.Errorf("failed downloading %s: %w", task.Filename, err)
					return
				}
				log.Printf("[Worker %d] Concluído: %s", workerID, task.Filename)
			}
		}(i)
	}

	for _, task := range tasks {
		taskChan <- task
	}
	close(taskChan)

	wg.Wait()
	close(errChan)

	// Check if any worker returned error
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Downloader) downloadFile(task DownloadTask) error {
	// Check if already downloaded and matches remote size
	if d.needsDownload(task.URL, task.DestPath) == false {
		log.Printf("[Downloader] Arquivo já existe e está atualizado (skip): %s", task.Filename)
		return nil
	}

	resp, err := d.HTTPClient.Get(task.URL)
	if err != nil {
		return fmt.Errorf("HTTP GET request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP status %d", resp.StatusCode)
	}

	tmpPath := task.DestPath + ".tmp"
	out, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	buf := make([]byte, 1024*1024) // 1MB buffer
	_, err = io.CopyBuffer(out, resp.Body, buf)
	out.Close()

	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed writing file body: %w", err)
	}

	// Rename temp to final destination
	if err := os.Rename(tmpPath, task.DestPath); err != nil {
		return fmt.Errorf("failed renaming temp file: %w", err)
	}

	return nil
}

func (d *Downloader) needsDownload(url string, destPath string) bool {
	info, err := os.Stat(destPath)
	if os.IsNotExist(err) {
		return true
	}
	if err != nil {
		return true
	}

	// Check HEAD remote Content-Length
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return true
	}
	resp, err := d.HTTPClient.Do(req)
	if err != nil || resp.StatusCode >= 400 {
		return true
	}
	defer resp.Body.Close()

	contentLengthStr := resp.Header.Get("Content-Length")
	if contentLengthStr == "" {
		return true
	}

	remoteSize, err := strconv.ParseInt(contentLengthStr, 10, 64)
	if err != nil {
		return true
	}

	if info.Size() != remoteSize {
		_ = os.Remove(destPath)
		return true
	}

	return false
}

func EnsureDir(path string) error {
	dir := filepath.Dir(path)
	return os.MkdirAll(dir, 0755)
}
