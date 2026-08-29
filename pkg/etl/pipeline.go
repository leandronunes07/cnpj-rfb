package etl

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/crawler"
	"github.com/leandronunes07/cnpj-rfb/pkg/database"
	"github.com/leandronunes07/cnpj-rfb/pkg/downloader"
	"github.com/leandronunes07/cnpj-rfb/pkg/extractor"
	"github.com/leandronunes07/cnpj-rfb/pkg/lock"
	"github.com/leandronunes07/cnpj-rfb/pkg/schema"
	"github.com/leandronunes07/cnpj-rfb/pkg/search"
)

type Pipeline struct {
	cfg          *config.Config
	db           database.DBDriver
	crawler      *crawler.ReceitaCrawler
	downloader   *downloader.Downloader
	extractor    *extractor.Extractor
	runLock      lock.PipelineLock
	searchClient *search.Client // nil when Meilisearch isn't configured
}

// NewPipeline wires up the pipeline.
//
// redisClient may be nil — in that case the "only one execution at a time"
// guard is an in-process lock, which is all a single-instance deployment
// needs. Pass a real client when the app runs as multiple instances/
// replicas against the same database, so the guard actually holds across
// processes instead of just within one.
//
// searchClient may be nil — in that case GET /api/v1/busca stays on the
// SQL-based search path (see pkg/database) unchanged. Pass a real client to
// have the pipeline keep a Meilisearch index in sync after every successful
// run, which the API then prefers when available.
func NewPipeline(cfg *config.Config, db database.DBDriver, redisClient *redis.Client, searchClient *search.Client) *Pipeline {
	rc := crawler.NewReceitaCrawler(cfg.BaseURL)

	var runLock lock.PipelineLock
	if redisClient != nil {
		runLock = lock.NewRedis(redisClient, "cnpjrbf:pipeline:lock")
	} else {
		runLock = lock.NewLocal()
	}

	return &Pipeline{
		cfg:          cfg,
		db:           db,
		crawler:      rc,
		downloader:   downloader.NewDownloader(cfg.DownloadWorkers, rc.Token),
		extractor:    extractor.NewExtractor(),
		runLock:      runLock,
		searchClient: searchClient,
	}
}

// Run executes the ETL pipeline. It refuses to run concurrently with itself:
// only one of the boot goroutine, the cron scheduler, and manual API triggers
// — across every instance of the app sharing the same lock — can be
// importing data at any given time.
func (p *Pipeline) Run() error {
	ctx := context.Background()
	acquired, err := p.runLock.TryAcquire(ctx)
	if err != nil {
		return fmt.Errorf("erro ao adquirir lock de execução do pipeline: %w", err)
	}
	if !acquired {
		log.Println("[ETL Pipeline] Execução já em andamento (nesta instância ou em outra), ignorando novo disparo concorrente.")
		return fmt.Errorf("pipeline já está em execução")
	}
	defer p.runLock.Release(ctx)

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
		if err := p.db.EnsureIndexes(); err != nil {
			log.Printf("[ETL Pipeline] Warning ao garantir índices: %v", err)
		}
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

	// Download & Stream Load Phase (Intercalado).
	// Cada worker do downloader processa (descompacta + importa) o arquivo que
	// ele mesmo baixou, sem lock global: como cada arquivo vai para sua própria
	// subpasta de extração e cada InsertBatch abre sua própria transação, até
	// DOWNLOAD_WORKERS arquivos podem ser importados em paralelo com segurança
	// (bancos com múltiplas conexões escalam; SQLite, limitado a 1 conexão,
	// naturalmente serializa via o pool sem precisar de lock explícito aqui).
	startTime := time.Now()

	err = p.downloader.DownloadStream(tasks, func(task downloader.DownloadTask) error {
		log.Printf("[ETL Pipeline] Processando imediatamente arquivo baixado: %s", task.Filename)

		taskExtractDir := filepath.Join(p.cfg.ExtractedDir, strings.TrimSuffix(task.Filename, filepath.Ext(task.Filename)))
		extractedFiles, err := p.extractor.ExtractZip(task.DestPath, taskExtractDir)
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
				continue
			}

			matchedCount++
			log.Printf("[ETL Pipeline] Importando dados de %s para a tabela `%s`...", baseName, tableSpec.Name)
			if err := p.importFileToTable(extFile, *tableSpec, targetMonth); err != nil {
				log.Printf("[ETL Pipeline] ERRO ao importar %s para `%s`: %v", baseName, tableSpec.Name, err)
				fileImportSuccess = false
			}
		}

		// Clean up extracted dir + zip file immediately
		if p.cfg.AutoCleanup {
			_ = os.RemoveAll(taskExtractDir)
			_ = os.Remove(task.DestPath)
			log.Printf("[Auto-Cleanup] Arquivos extraídos e ZIP removidos: %s", task.Filename)
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

	// Build secondary indexes now that the bulk load is done, instead of
	// maintaining them on every single insert during the load above.
	if err := p.db.EnsureIndexes(); err != nil {
		log.Printf("[ETL Pipeline] Warning ao garantir índices: %v", err)
	}

	// Keep the Meilisearch index in sync now that there's new data. This is
	// a full reindex, not incremental — simple and correct, and cheap
	// enough given the Receita Federal data only changes monthly. Only
	// triggered here (after real work happened this run), not on the
	// "nothing pending" early return above, so an unchanged day doesn't
	// pay for re-reading and re-pushing tens of millions of rows for
	// nothing.
	p.SyncSearchIndex()

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

const searchSyncPageSize = 5000

// SyncSearchIndex pushes every estabelecimento+empresa record into
// Meilisearch, paginated. It's a no-op (returns immediately) when no search
// client is configured. Safe to call on its own — main.go also calls this
// once at boot in the background, independent of Run(), so enabling
// Meilisearch on a deployment that already has fully-loaded data populates
// the index without waiting for the next month's new files to trigger it
// via Run() itself.
func (p *Pipeline) SyncSearchIndex() {
	if p.searchClient == nil {
		return
	}

	if err := p.searchClient.EnsureIndex(); err != nil {
		log.Printf("[Search] Warning ao configurar índice do Meilisearch: %v", err)
		return
	}

	log.Println("[Search] Sincronizando índice de busca (Meilisearch)...")
	start := time.Now()
	total := 0

	for offset := 0; ; offset += searchSyncPageSize {
		docs, hasMore, err := p.db.GetSearchDocuments(offset, searchSyncPageSize)
		if err != nil {
			log.Printf("[Search] ERRO ao ler página de documentos (offset %d) do banco: %v", offset, err)
			return
		}
		if len(docs) == 0 {
			break
		}

		searchDocs := make([]search.Document, len(docs))
		for i, d := range docs {
			searchDocs[i] = search.Document{
				CNPJ:         d.CNPJ,
				RazaoSocial:  d.RazaoSocial,
				NomeFantasia: d.NomeFantasia,
				UF:           d.UF,
				CNAE:         d.CNAE,
			}
		}

		if err := p.searchClient.IndexDocuments(searchDocs); err != nil {
			log.Printf("[Search] ERRO ao indexar página (offset %d) no Meilisearch: %v", offset, err)
			return
		}

		total += len(docs)
		if !hasMore {
			break
		}
	}

	log.Printf("[Search] Índice de busca sincronizado: %d registros em %v.", total, time.Since(start))
}

func (p *Pipeline) importFileToTable(filePath string, table schema.TableSpec, competencia string) error {
	var batch [][]string
	totalRows := 0

	// Cada driver conhece seus próprios limites (placeholders para
	// Postgres/SQLite, ausência desse limite para LOAD DATA no MySQL, etc.)
	batchLimit := p.db.BatchLimit(len(table.Columns))

	// UpsertBatchTracked, not InsertBatch: for tables with a stable natural
	// key (empresa/estabelecimento/simples, see mysql.go) this detects rows
	// that changed since a previous competência and records what changed,
	// instead of silently discarding them like a plain re-import would.
	// Other tables/drivers fall back to the old InsertBatch behavior
	// transparently — see each driver's UpsertBatchTracked.
	err := p.extractor.StreamCSVRows(filePath, func(row []string) error {
		batch = append(batch, row)
		if len(batch) >= batchLimit {
			if err := p.db.UpsertBatchTracked(table, batch, competencia); err != nil {
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
		if err := p.db.UpsertBatchTracked(table, batch, competencia); err != nil {
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
