package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/leandronunes07/cnpj-rfb/pkg/api"
	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/database"
	"github.com/leandronunes07/cnpj-rfb/pkg/etl"
	"github.com/leandronunes07/cnpj-rfb/pkg/scheduler"
	"github.com/leandronunes07/cnpj-rfb/pkg/search"
)

// connectRedis returns nil (not an error) when Redis isn't configured or
// isn't reachable — Redis is an optional accelerator here (distributed
// pipeline lock, API rate limiting), never a hard requirement, so a bad
// connection degrades the app back to single-instance/no-rate-limit
// behavior instead of refusing to start.
func connectRedis(cfg *config.Config) *redis.Client {
	if cfg.RedisAddr == "" {
		return nil
	}

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("[Redis] AVISO: não foi possível conectar em %s (%v). Seguindo sem Redis: lock de execução volta a ser local (não coordena múltiplas instâncias) e rate limiting da API fica desativado.", cfg.RedisAddr, err)
		_ = client.Close()
		return nil
	}

	log.Printf("[Redis] Conectado com sucesso em %s.", cfg.RedisAddr)
	return client
}

// connectSearch returns nil when Meilisearch isn't configured or isn't
// reachable — same reasoning as connectRedis: it's an accelerator for
// GET /api/v1/busca, never a hard requirement. The SQL-based search in
// pkg/database keeps working either way.
func connectSearch(cfg *config.Config) *search.Client {
	if cfg.MeiliHost == "" {
		return nil
	}

	client := search.New(cfg.MeiliHost, cfg.MeiliAPIKey, cfg.MeiliIndex)
	if !client.Healthy() {
		log.Printf("[Search] AVISO: não foi possível conectar no Meilisearch em %s. Seguindo sem ele: busca por nome continua funcionando via SQL, sem a aceleração/ranking do Meilisearch.", cfg.MeiliHost)
		return nil
	}

	log.Printf("[Search] Conectado ao Meilisearch em %s (índice %q).", cfg.MeiliHost, cfg.MeiliIndex)
	return client
}

func main() {
	log.SetOutput(api.NewLogBroadcasterWriter())

	onceFlag := flag.Bool("once", false, "Executa o pipeline uma única vez e encerra, sem iniciar o servidor daemon.")
	flag.Parse()

	log.Println("==================================================================")
	log.Println("  Agência Taruga - www.agenciataruga.com")
	log.Println("  CNPJ Receita Federal ETL Engine & Microservice API em Go")
	log.Println("==================================================================")

	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("[FATAL] Erro ao carregar configurações: %v", err)
	}

	if *onceFlag {
		cfg.RunOnce = true
	}

	// Initialize database driver
	dbDriver, err := database.NewDBDriver(cfg)
	if err != nil {
		log.Fatalf("[FATAL] Erro ao instanciar driver de banco: %v", err)
	}

	if err := dbDriver.Connect(); err != nil {
		log.Fatalf("[FATAL] Erro ao conectar ao banco de dados: %v", err)
	}
	defer dbDriver.Close()

	if err := dbDriver.InitSchema(); err != nil {
		log.Fatalf("[FATAL] Erro ao inicializar esquema do banco: %v", err)
	}

	redisClient := connectRedis(cfg)
	if redisClient != nil {
		defer redisClient.Close()
	}

	searchClient := connectSearch(cfg)

	pipeline := etl.NewPipeline(cfg, dbDriver, redisClient, searchClient)

	if cfg.RunOnce {
		log.Println("[INFO] Modo --once ativado. Executando o pipeline uma única vez...")
		if err := pipeline.Run(); err != nil {
			log.Printf("[WARNING] Ocorreu uma falha no pipeline: %v", err)
		}
		log.Println("[INFO] Pipeline concluído. Encerrando aplicação.")
		os.Exit(0)
	}

	// Launch HTTP API Server + Web Dashboard in background Goroutine (Imediato)
	apiServer := api.NewServer(cfg, dbDriver, pipeline, redisClient, searchClient)
	go func() {
		if err := apiServer.Start(); err != nil {
			log.Fatalf("[FATAL] Erro no servidor HTTP API: %v", err)
		}
	}()

	// Execute initial check/pipeline run on startup in background
	go func() {
		if err := pipeline.Run(); err != nil {
			log.Printf("[WARNING] Ocorreu uma falha na verificação inicial do pipeline: %v", err)
		}
	}()

	// If Meilisearch is configured, sync it once at boot too — not just
	// after Run() loads new data — so enabling it on a deployment that
	// already has fully-loaded data populates the index immediately instead
	// of waiting for next month's new files.
	if searchClient != nil {
		go pipeline.SyncSearchIndex()
	}

	// Start Cron scheduler daemon (blocks main thread until interrupt signal)
	cronScheduler := scheduler.NewScheduler(cfg, pipeline)
	if err := cronScheduler.Start(); err != nil {
		log.Fatalf("[FATAL] Erro ao iniciar agendador: %v", err)
	}
}
