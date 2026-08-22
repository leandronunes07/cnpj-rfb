package api

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/database"
	"github.com/leandronunes07/cnpj-rfb/pkg/etl"
	"github.com/leandronunes07/cnpj-rfb/pkg/search"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9]`)

type APIHandler struct {
	cfg          *config.Config
	db           database.DBDriver
	pipeline     *etl.Pipeline
	rateLimiter  *RateLimiter   // nil when Redis isn't configured — rate limiting is then simply skipped
	searchClient *search.Client // nil when Meilisearch isn't configured — HandleSearch then always uses SQL
}

func NewAPIHandler(cfg *config.Config, db database.DBDriver, pipeline *etl.Pipeline, rateLimiter *RateLimiter, searchClient *search.Client) *APIHandler {
	return &APIHandler{
		cfg:          cfg,
		db:           db,
		pipeline:     pipeline,
		rateLimiter:  rateLimiter,
		searchClient: searchClient,
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

		if subtle.ConstantTimeCompare([]byte(token), []byte(h.cfg.APIToken)) != 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Não autorizado. Token de API inválido ou ausente.",
			})
			return
		}

		// Rate limiting is keyed by client, not by token: every caller
		// shares the single API_TOKEN, so limiting by token would just be
		// a global limit for the whole API rather than a per-client one.
		// Checked only after auth succeeds, so failed-auth attempts don't
		// burn through a legitimate client's budget.
		if h.rateLimiter != nil {
			allowed, retryAfter, err := h.rateLimiter.Allow(r.Context(), clientIP(r))
			if err != nil {
				log.Printf("[RateLimiter] Warning: %v — permitindo a requisição (fail-open).", err)
			} else if !allowed {
				w.Header().Set("Retry-After", fmt.Sprintf("%.0f", retryAfter.Seconds()))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				json.NewEncoder(w).Encode(map[string]string{
					"error": "Limite de requisições excedido. Tente novamente em instantes.",
				})
				return
			}
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
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	results, source, err := h.search(query, uf, limit)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"query":       query,
		"uf":          uf,
		"total_count": len(results),
		"results":     results,
		"source":      source, // "meilisearch" or "sql" — which engine actually served this request
	})
}

// search tries Meilisearch first when configured, falling back to the
// SQL-based search (pkg/database — LIKE, or the driver's own acceleration,
// see docs/API.md) whenever Meilisearch isn't configured or the query
// against it fails. Response shape is identical either way, so API
// consumers don't need to care which one answered.
func (h *APIHandler) search(query, uf string, limit int) ([]map[string]interface{}, string, error) {
	if h.searchClient != nil {
		docs, err := h.searchClient.Search(query, uf, limit)
		if err == nil {
			results := make([]map[string]interface{}, len(docs))
			for i, d := range docs {
				results[i] = map[string]interface{}{
					"cnpj":          d.CNPJ,
					"razao_social":  d.RazaoSocial,
					"nome_fantasia": d.NomeFantasia,
					"uf":            d.UF,
					"cnae":          d.CNAE,
				}
			}
			return results, "meilisearch", nil
		}
		log.Printf("[Search] Warning: consulta ao Meilisearch falhou (%v), usando busca SQL como fallback.", err)
	}

	results, err := h.db.SearchCNPJ(query, uf, limit)
	if err != nil {
		return nil, "", err
	}
	return results, "sql", nil
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
