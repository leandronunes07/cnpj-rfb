package api

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/database"
	"github.com/leandronunes07/cnpj-rfb/pkg/etl"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9]`)

type APIHandler struct {
	cfg      *config.Config
	db       database.DBDriver
	pipeline *etl.Pipeline
}

func NewAPIHandler(cfg *config.Config, db database.DBDriver, pipeline *etl.Pipeline) *APIHandler {
	return &APIHandler{
		cfg:      cfg,
		db:       db,
		pipeline: pipeline,
	}
}

func (h *APIHandler) AuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Token")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		token := r.Header.Get("X-API-Token")
		if token == "" {
			authHeader := r.Header.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				token = strings.TrimPrefix(authHeader, "Bearer ")
			}
		}
		if token == "" {
			token = r.URL.Query().Get("token")
		}

		if token != h.cfg.APIToken {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Não autorizado. Token de API inválido ou ausente.",
			})
			return
		}

		next(w, r)
	}
}

func (h *APIHandler) HandleGetCNPJ(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 4 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "CNPJ não informado na URL"})
		return
	}

	rawCNPJ := parts[3]
	cleanCNPJ := nonAlphanumericRegex.ReplaceAllString(rawCNPJ, "")

	if len(cleanCNPJ) != 14 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "CNPJ inválido. Deve conter exatamente 14 caracteres alfanuméricos."})
		return
	}

	cnpjData, err := h.db.GetCNPJ(cleanCNPJ)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	json.NewEncoder(w).Encode(cnpjData)
}

func (h *APIHandler) HandleSearch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	query := r.URL.Query().Get("q")
	if query == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Parâmetro 'q' (busca) é obrigatório."})
		return
	}

	uf := r.URL.Query().Get("uf")
	limitStr := r.URL.Query().Get("limit")
	limit := 20
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}

	results, err := h.db.SearchCNPJ(query, uf, limit)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"query":        query,
		"uf":           uf,
		"total_count": len(results),
		"results":      results,
	})
}

func (h *APIHandler) HandleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	stats, err := h.db.GetStats()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "online",
		"engine": "Go ETL Engine v2.0",
		"stats":  stats,
	})
}

func (h *APIHandler) HandleTriggerETL(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	go func() {
		GlobalBroadcaster.Broadcast("Iniciando verificação manual do ETL disparada via Dashboard/API...")
		if err := h.pipeline.Run(); err != nil {
			GlobalBroadcaster.Broadcast("ERRO no ETL: " + err.Error())
		} else {
			GlobalBroadcaster.Broadcast("ETL finalizado com sucesso!")
		}
	}()

	json.NewEncoder(w).Encode(map[string]string{
		"message": "Rotina de verificação e carga do ETL iniciada em background.",
	})
}
