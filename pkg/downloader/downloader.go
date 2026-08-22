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
	AuthToken  string
	HTTPClient *http.Client
}

func NewDownloader(workers int, authToken string) *Downloader {
	return &Downloader{
		Workers:   workers,
		AuthToken: authToken,
		HTTPClient: &http.Client{
			Timeout: 2 * time.Hour, // Aumentado para 2 horas para permitir o download completo de arquivos grandes (1.5GB+) em conexões mais lentas
			Transport: &http.Transport{
				ResponseHeaderTimeout: 2 * time.Minute,
				IdleConnTimeout:       3 * time.Minute,
				TLSHandshakeTimeout:   30 * time.Second,
			},
		},
	}
}

type DownloadTask struct {
	URL      string
	DestPath string
	Filename string
}

func (d *Downloader) DownloadAll(tasks []DownloadTask) error {
	return d.DownloadStream(tasks, nil)
}

func (d *Downloader) DownloadStream(tasks []DownloadTask, onComplete func(task DownloadTask) error) error {
	if len(tasks) == 0 {
		log.Println("[Downloader] Nenhum arquivo para baixar.")
		return nil
	}

	log.Printf("[Downloader] Iniciando download em stream de %d arquivo(s) com %d worker(s)...", len(tasks), d.Workers)

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
				log.Printf("[Worker %d] Concluído download: %s", workerID, task.Filename)
				if onComplete != nil {
					if err := onComplete(task); err != nil {
						log.Printf("[Worker %d] ERRO ao processar %s: %v", workerID, task.Filename, err)
						errChan <- fmt.Errorf("failed processing %s: %w", task.Filename, err)
						return
					}
				}
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
	if !d.needsDownload(task.URL, task.DestPath) {
		log.Printf("[Downloader] Arquivo já existe e está atualizado (skip): %s", task.Filename)
		return nil
	}

	maxRetries := 3
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			log.Printf("[Downloader] Tentativa %d/%d para baixar %s...", attempt, maxRetries, task.Filename)
			time.Sleep(time.Duration(attempt*3) * time.Second)
		}

		err := d.doDownload(task)
		if err == nil {
			return nil
		}
		lastErr = err
		log.Printf("[Downloader] Falha na tentativa %d ao baixar %s: %v", attempt, task.Filename, err)
	}

	return lastErr
}

func (d *Downloader) doDownload(task DownloadTask) error {
	req, err := http.NewRequest("GET", task.URL, nil)
	if err != nil {
		return fmt.Errorf("failed to create GET request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	if d.AuthToken != "" {
		req.SetBasicAuth(d.AuthToken, "")
	}

	resp, err := d.HTTPClient.Do(req)
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

func (d *Downloader) needsDownload(targetURL string, destPath string) bool {
	info, err := os.Stat(destPath)
	if os.IsNotExist(err) {
		return true
	}
	if err != nil {
		return true
	}

	// Check HEAD remote Content-Length
	req, err := http.NewRequest("HEAD", targetURL, nil)
	if err != nil {
		return true
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	if d.AuthToken != "" {
		req.SetBasicAuth(d.AuthToken, "")
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
