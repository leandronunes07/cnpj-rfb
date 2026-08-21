package api

import (
	"fmt"
	"log"
	"net/http"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/database"
	"github.com/leandronunes07/cnpj-rfb/pkg/etl"
	"github.com/leandronunes07/cnpj-rfb/pkg/web"
)

type Server struct {
	cfg      *config.Config
	db       database.DBDriver
	pipeline *etl.Pipeline
}

func NewServer(cfg *config.Config, db database.DBDriver, pipeline *etl.Pipeline) *Server {
	return &Server{
		cfg:      cfg,
		db:       db,
		pipeline: pipeline,
	}
}

func (s *Server) Start() error {
	handler := NewAPIHandler(s.cfg, s.db, s.pipeline)
	mux := http.NewServeMux()

	// API REST Endpoints (Autenticados por API_TOKEN)
	mux.HandleFunc("/api/v1/cnpj/", handler.AuthMiddleware(handler.HandleGetCNPJ))
	mux.HandleFunc("/api/v1/busca", handler.AuthMiddleware(handler.HandleSearch))
	mux.HandleFunc("/api/v1/status", handler.AuthMiddleware(handler.HandleStatus))
	mux.HandleFunc("/api/v1/trigger-etl", handler.AuthMiddleware(handler.HandleTriggerETL))
	mux.HandleFunc("/api/v1/events", handler.AuthMiddleware(GlobalBroadcaster.ServeHTTP))

	// Web Dashboard Static UI (Embedded HTML/CSS/JS)
	mux.Handle("/", web.StaticHandler())

	addr := fmt.Sprintf(":%d", s.cfg.APIPort)
	log.Printf("[API Server] Servidor HTTP Microservice + Dashboard Web iniciado na porta %s", addr)
	log.Printf("[API Server] Dashboard Web acessível em: http://localhost:%d", s.cfg.APIPort)

	return http.ListenAndServe(addr, mux)
}
