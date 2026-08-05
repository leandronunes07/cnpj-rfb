package scheduler

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/etl"
	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	cfg      *config.Config
	pipeline *etl.Pipeline
	cron     *cron.Cron
}

func NewScheduler(cfg *config.Config, pipeline *etl.Pipeline) *Scheduler {
	return &Scheduler{
		cfg:      cfg,
		pipeline: pipeline,
		cron:     cron.New(),
	}
}

func (s *Scheduler) Start() error {
	log.Printf("[Scheduler] Configurando agendador mensal com expressão cron: '%s'", s.cfg.CronSchedule)

	_, err := s.cron.AddFunc(s.cfg.CronSchedule, func() {
		log.Println("[Scheduler] Executando rotina mensal agendada da Receita Federal...")
		if err := s.pipeline.Run(); err != nil {
			log.Printf("[Scheduler] ERRO na rotina mensal: %v", err)
		}
	})
	if err != nil {
		return err
	}

	s.cron.Start()
	log.Println("[Scheduler] Agendador iniciado com sucesso. Aguardando próximas execuções...")

	// Listen for interrupt signals to shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("[Scheduler] Encerrando agendador gracefully...")
	s.cron.Stop()
	return nil
}
