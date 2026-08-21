package main

import (
	"flag"
	"log"
	"os"

	"github.com/leandronunes07/cnpj-rfb/pkg/api"
	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/database"
	"github.com/leandronunes07/cnpj-rfb/pkg/etl"
	"github.com/leandronunes07/cnpj-rfb/pkg/scheduler"
)

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

	pipeline := etl.NewPipeline(cfg, dbDriver)

	if cfg.RunOnce {
		log.Println("[INFO] Modo --once ativado. Executando o pipeline uma única vez...")
		if err := pipeline.Run(); err != nil {
			log.Printf("[WARNING] Ocorreu uma falha no pipeline: %v", err)
		}
		log.Println("[INFO] Pipeline concluído. Encerrando aplicação.")
		os.Exit(0)
	}

	// Launch HTTP API Server + Web Dashboard in background Goroutine (Imediato)
	apiServer := api.NewServer(cfg, dbDriver, pipeline)
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

	// Start Cron scheduler daemon (blocks main thread until interrupt signal)
	cronScheduler := scheduler.NewScheduler(cfg, pipeline)
	if err := cronScheduler.Start(); err != nil {
		log.Fatalf("[FATAL] Erro ao iniciar agendador: %v", err)
	}
}
