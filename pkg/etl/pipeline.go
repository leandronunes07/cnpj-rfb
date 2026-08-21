package etl

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/crawler"
	"github.com/leandronunes07/cnpj-rfb/pkg/database"
	"github.com/leandronunes07/cnpj-rfb/pkg/downloader"
	"github.com/leandronunes07/cnpj-rfb/pkg/extractor"
	"github.com/leandronunes07/cnpj-rfb/pkg/schema"
)

type Pipeline struct {
	cfg        *config.Config
	db         database.DBDriver
	crawler    *crawler.ReceitaCrawler
	downloader *downloader.Downloader
	extractor  *extractor.Extractor
	running    atomic.Bool
}

func NewPipeline(cfg *config.Config, db database.DBDriver) *Pipeline {
	rc := crawler.NewReceitaCrawler(cfg.BaseURL)
	return &Pipeline{
		cfg:        cfg,
		db:         db,
		crawler:    rc,
		downloader: downloader.NewDownloader(cfg.DownloadWorkers, rc.Token),
		extractor:  extractor.NewExtractor(),
	}
}

// Run executes the ETL pipeline. It refuses to run concurrently with itself:
// only one of the boot goroutine, the cron scheduler, and manual API triggers
// can be importing data at any given time.
func (p *Pipeline) Run() error {
	if !p.running.CompareAndSwap(false, true) {
		log.Println("[ETL Pipeline] Execução já em andamento, ignorando novo disparo concorrente.")
		return fmt.Errorf("pipeline já está em execução")
	}
	defer p.running.Store(false)

	log.Println("==================================================================")
	log.Println("[ETL Pipeline] Iniciando rotina de verificação e carga de dados CNPJ")
	log.Println("==================================================================")

	targetMonth := p.cfg.DataMonth
	if targetMonth == "" {
		latest, err := p.crawler.GetLatestMonth()
		if err != nil {
			return fmt.Errorf("erro ao verificar último mês na Receita: %w", err)
		}
		targetMonth = latest
	}

	log.Printf("[ETL Pipeline] Competência alvo identificada: %s", targetMonth)

	dataURL, zipFiles, err := p.crawler.ListZipFiles(targetMonth)
	if err != nil {
		return fmt.Errorf("erro ao listar arquivos zip para a competência %s: %w", targetMonth, err)
	}

	log.Printf("[ETL Pipeline] Encontrados %d arquivos .zip no servidor da Receita para %s.", len(zipFiles), targetMonth)

	// Filter files already processed in DB
	var pendingZipFiles []string
	for _, zipName := range zipFiles {
		processed, err := p.db.IsFileProcessed(targetMonth, zipName)
		if err != nil {
			log.Printf("[ETL Pipeline] Warning ao verificar histórico do arquivo %s: %v", zipName, err)
		}
		if processed {
			log.Printf("[ETL Pipeline] Arquivo %s já foi inserido no banco (ignorado).", zipName)
			continue
		}
		pendingZipFiles = append(pendingZipFiles, zipName)
	}

	if len(pendingZipFiles) == 0 {
		log.Printf("[ETL Pipeline] Todos os %d arquivos da competência %s já foram processados. Nenhuma ação necessária.", len(zipFiles), targetMonth)
		_ = p.db.SaveProcessedMonth(targetMonth)
		return nil
	}

	log.Printf("[ETL Pipeline] Encontrados %d arquivos pendentes para download e carga.", len(pendingZipFiles))

	// Build download tasks for pending files only
	var tasks []downloader.DownloadTask
	for _, zipName := range pendingZipFiles {
		fileURL, err := p.crawler.BuildFileURL(dataURL, zipName)
		if err != nil {
			log.Printf("[ETL Pipeline] Warning ao resolver URL para %s: %v", zipName, err)
			continue
		}
		destPath := filepath.Join(p.cfg.OutputDir, zipName)
		tasks = append(tasks, downloader.DownloadTask{
			URL:      fileURL,
			DestPath: destPath,
			Filename: zipName,
		})
	}

	// Download & Stream Load Phase (Intercalado)
	startTime := time.Now()
	var importMu sync.Mutex

	err = p.downloader.DownloadStream(tasks, func(task downloader.DownloadTask) error {
		importMu.Lock()
		defer importMu.Unlock()

		log.Printf("[ETL Pipeline] Processando imediatamente arquivo baixado: %s", task.Filename)

		extractedFiles, err := p.extractor.ExtractZip(task.DestPath, p.cfg.ExtractedDir)
		if err != nil {
			log.Printf("[ETL Pipeline] ERRO ao descompactar %s: %v", task.Filename, err)
			return nil
		}

		matchedCount := 0
		fileImportSuccess := true
		for _, extFile := range extractedFiles {
			baseName := filepath.Base(extFile)
			tableSpec := matchTableSpec(baseName)
			if tableSpec == nil {
				log.Printf("[ETL Pipeline] Nenhum schema correspondente para arquivo %s (ignorado).", baseName)
				_ = os.Remove(extFile)
				continue
			}

			matchedCount++
			log.Printf("[ETL Pipeline] Importando dados de %s para a tabela `%s`...", baseName, tableSpec.Name)
			if err := p.importFileToTable(extFile, *tableSpec); err != nil {
				log.Printf("[ETL Pipeline] ERRO ao importar %s para `%s`: %v", baseName, tableSpec.Name, err)
				fileImportSuccess = false
			}

			// Clean up extracted file immediately
			if p.cfg.AutoCleanup {
				_ = os.Remove(extFile)
				log.Printf("[Auto-Cleanup] Arquivo extraído removido: %s", baseName)
			}
		}

		// Clean up zip file immediately
		if p.cfg.AutoCleanup {
			_ = os.Remove(task.DestPath)
			log.Printf("[Auto-Cleanup] Arquivo ZIP removido: %s", task.Filename)
		}

		if matchedCount > 0 && fileImportSuccess {
			if err := p.db.SaveProcessedFile(targetMonth, task.Filename, "SUCCESS"); err != nil {
				log.Printf("[ETL Pipeline] ERRO ao registrar arquivo %s no banco: %v", task.Filename, err)
			} else {
				log.Printf("[ETL Pipeline] Arquivo %s importado no banco e registrado com sucesso!", task.Filename)
			}
		} else if matchedCount == 0 {
			log.Printf("[ETL Pipeline] ATENÇÃO: NENHUM SCHEMA CORRESPONDIDO para %s. O arquivo NÃO foi marcado como concluído no banco.", task.Filename)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("falha no pipeline de download e carga: %w", err)
	}

	// Record success in etl_metadata
	if err := p.db.SaveProcessedMonth(targetMonth); err != nil {
		log.Printf("[ETL Pipeline] ERRO ao salvar metadados da competência %s: %v", targetMonth, err)
	} else {
		log.Printf("[ETL Pipeline] Competência %s registrada com sucesso!", targetMonth)
	}

	elapsed := time.Since(startTime)
	log.Printf("==================================================================")
	log.Printf("[ETL Pipeline] Processamento concluído com sucesso em %v!", elapsed)
	log.Printf("==================================================================")

	return nil
}

func (p *Pipeline) importFileToTable(filePath string, table schema.TableSpec) error {
	var batch [][]string
	totalRows := 0

	// Dinamiza o tamanho do batch para evitar limite de placeholders (65535 no MySQL/Postgres)
	maxPlaceholders := 65000
	if p.cfg.DBDriver == "sqlite" {
		maxPlaceholders = 32700 // SQLite modern limit
	}

	batchLimit := maxPlaceholders / len(table.Columns)
	if batchLimit == 0 {
		batchLimit = 1
	}
	if batchLimit > p.cfg.BatchSize {
		batchLimit = p.cfg.BatchSize
	}

	err := p.extractor.StreamCSVRows(filePath, func(row []string) error {
		batch = append(batch, row)
		if len(batch) >= batchLimit {
			if err := p.db.InsertBatch(table, batch); err != nil {
				return err
			}
			totalRows += len(batch)
			batch = batch[:0]
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Insert remaining rows in final batch
	if len(batch) > 0 {
		if err := p.db.InsertBatch(table, batch); err != nil {
			return err
		}
		totalRows += len(batch)
	}

	log.Printf("[ETL Pipeline] Tabela `%s` atualizada com +%d registros.", table.Name, totalRows)
	return nil
}

func matchTableSpec(filename string) *schema.TableSpec {
	upperName := strings.ToUpper(filename)
	for _, t := range schema.Tables {
		for _, prefix := range t.Prefixes {
			if strings.Contains(upperName, prefix) {
				return &t
			}
		}
	}
	return nil
}
