package etl

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
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
}

func NewPipeline(cfg *config.Config, db database.DBDriver) *Pipeline {
	return &Pipeline{
		cfg:        cfg,
		db:         db,
		crawler:    crawler.NewReceitaCrawler(cfg.BaseURL),
		downloader: downloader.NewDownloader(cfg.DownloadWorkers),
		extractor:  extractor.NewExtractor(),
	}
}

func (p *Pipeline) Run() error {
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

	// Check if already processed
	lastProcessed, err := p.db.GetLatestProcessedMonth()
	if err != nil {
		log.Printf("[ETL Pipeline] Warning ao verificar histórico de metadados: %v", err)
	}

	if lastProcessed != "" && lastProcessed == targetMonth {
		log.Printf("[ETL Pipeline] A competência %s já foi processada anteriormente. Nenhuma ação necessária.", targetMonth)
		return nil
	}

	log.Printf("[ETL Pipeline] Nova competência %s detectada! (Última processada: %s)", targetMonth, lastProcessed)

	dataURL, zipFiles, err := p.crawler.ListZipFiles(targetMonth)
	if err != nil {
		return fmt.Errorf("erro ao listar arquivos zip para a competência %s: %w", targetMonth, err)
	}

	log.Printf("[ETL Pipeline] Encontrados %d arquivos .zip no servidor da Receita.", len(zipFiles))

	// Build download tasks
	var tasks []downloader.DownloadTask
	for _, zipName := range zipFiles {
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

	// Download phase
	startTime := time.Now()
	if err := p.downloader.DownloadAll(tasks); err != nil {
		return fmt.Errorf("falha no download dos arquivos: %w", err)
	}

	// Extraction and Load Phase
	for _, task := range tasks {
		log.Printf("[ETL Pipeline] Processando arquivo: %s", task.Filename)

		extractedFiles, err := p.extractor.ExtractZip(task.DestPath, p.cfg.ExtractedDir)
		if err != nil {
			log.Printf("[ETL Pipeline] ERRO ao descompactar %s: %v", task.Filename, err)
			continue
		}

		for _, extFile := range extractedFiles {
			baseName := filepath.Base(extFile)
			tableSpec := matchTableSpec(baseName)
			if tableSpec == nil {
				log.Printf("[ETL Pipeline] Nenhum schema correspondente para arquivo %s (ignorado).", baseName)
				_ = os.Remove(extFile)
				continue
			}

			log.Printf("[ETL Pipeline] Importando dados de %s para a tabela `%s`...", baseName, tableSpec.Name)
			if err := p.importFileToTable(extFile, *tableSpec); err != nil {
				log.Printf("[ETL Pipeline] ERRO ao importar %s para `%s`: %v", baseName, tableSpec.Name, err)
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

	err := p.extractor.StreamCSVRows(filePath, func(row []string) error {
		batch = append(batch, row)
		if len(batch) >= p.cfg.BatchSize {
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
			if strings.HasPrefix(upperName, prefix) {
				return &t
			}
		}
	}
	return nil
}
