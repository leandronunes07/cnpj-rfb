package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	DBDriver        string
	DBHost          string
	DBPort          int
	DBUser          string
	DBPassword      string
	DBName          string
	DBSSLMode       string
	DBFile          string // For SQLite / DuckDB
	APIPort         int
	APIToken        string
	OutputDir       string
	ExtractedDir    string
	BaseURL         string
	DataMonth       string
	DownloadWorkers int
	BatchSize       int
	AutoCleanup     bool
	CronSchedule    string
	RunOnce         bool

	// Redis is optional. Empty RedisAddr means "no Redis" — the app falls
	// back to an in-process pipeline lock and skips API rate limiting
	// entirely rather than failing to start. Set it once you need either
	// multi-instance coordination or rate limiting (see docs/API.md).
	RedisAddr          string
	RedisPassword      string
	RedisDB            int
	RateLimitPerMinute int

	// Meilisearch is optional. Empty MeiliHost means "no search engine" —
	// GET /api/v1/busca falls back to the SQL-based search (LIKE, or the
	// driver-specific acceleration described in docs/API.md) unchanged.
	MeiliHost   string
	MeiliAPIKey string
	MeiliIndex  string
}

func LoadConfig() (*Config, error) {
	_ = godotenv.Load()

	dbDriver := strings.ToLower(getEnv("DB_DRIVER", "postgres"))
	validDrivers := map[string]bool{
		"postgres":   true,
		"mysql":      true,
		"sqlite":     true,
		"turso":      true,
		"duckdb":     true,
		"clickhouse": true,
	}

	if !validDrivers[dbDriver] {
		return nil, fmt.Errorf("unsupported DB_DRIVER: %s (must be postgres, mysql, sqlite, turso, duckdb, or clickhouse)", dbDriver)
	}

	defaultPort := 5432
	switch dbDriver {
	case "mysql":
		defaultPort = 3306
	case "clickhouse":
		defaultPort = 9000
	}

	outputDir := getEnv("OUTPUT_FILES_PATH", "./data/zip")
	extractedDir := getEnv("EXTRACTED_FILES_PATH", "./data/extracted")

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output dir: %w", err)
	}
	if err := os.MkdirAll(extractedDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create extracted dir: %w", err)
	}

	dbFile := getEnv("DB_FILE", "./data/cnpj.sqlite")
	if err := os.MkdirAll(filepath.Dir(dbFile), 0755); err != nil {
		return nil, fmt.Errorf("failed creating db file directory: %w", err)
	}

	baseURL := getEnv("DATA_BASE_URL", "https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9")
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}

	cfg := &Config{
		DBDriver:        dbDriver,
		DBHost:          getEnv("DB_HOST", "localhost"),
		DBPort:          getEnvAsInt("DB_PORT", defaultPort),
		DBUser:          getEnv("DB_USER", "postgres"),
		DBPassword:      getEnv("DB_PASSWORD", ""),
		DBName:          getEnv("DB_NAME", "cnpj"),
		DBSSLMode:       getEnv("DB_SSLMODE", "disable"),
		DBFile:          filepath.Clean(dbFile),
		APIPort:         getEnvAsInt("API_PORT", 8080),
		APIToken:        getEnv("API_TOKEN", ""),
		OutputDir:       filepath.Clean(outputDir),
		ExtractedDir:    filepath.Clean(extractedDir),
		BaseURL:         baseURL,
		DataMonth:       getEnv("DATA_MONTH", ""),
		DownloadWorkers: getEnvAsInt("DOWNLOAD_WORKERS", 4),
		BatchSize:       getEnvAsInt("BATCH_SIZE", 10000),
		AutoCleanup:     getEnvAsBool("AUTO_CLEANUP", true),
		CronSchedule:    getEnv("CRON_SCHEDULE", "0 3 * * *"),
		RunOnce:         getEnvAsBool("RUN_ONCE", false),

		RedisAddr:          getEnv("REDIS_ADDR", ""),
		RedisPassword:      getEnv("REDIS_PASSWORD", ""),
		RedisDB:            getEnvAsInt("REDIS_DB", 0),
		RateLimitPerMinute: getEnvAsInt("RATE_LIMIT_PER_MINUTE", 120),

		MeiliHost:   getEnv("MEILISEARCH_HOST", ""),
		MeiliAPIKey: getEnv("MEILISEARCH_API_KEY", ""),
		MeiliIndex:  getEnv("MEILISEARCH_INDEX", "estabelecimentos"),
	}

	if cfg.APIToken == "" {
		return nil, fmt.Errorf("API_TOKEN não configurado: defina uma variável de ambiente API_TOKEN com um valor forte e secreto antes de iniciar a aplicação")
	}

	networkedDrivers := map[string]bool{"postgres": true, "mysql": true, "clickhouse": true}
	if networkedDrivers[dbDriver] && cfg.DBPassword == "" {
		return nil, fmt.Errorf("DB_PASSWORD não configurado: obrigatório para o driver %q", dbDriver)
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		return strings.TrimSpace(val)
	}
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	strVal := getEnv(key, "")
	if strVal == "" {
		return fallback
	}
	val, err := strconv.Atoi(strVal)
	if err != nil {
		return fallback
	}
	return val
}

func getEnvAsBool(key string, fallback bool) bool {
	strVal := getEnv(key, "")
	if strVal == "" {
		return fallback
	}
	val, err := strconv.ParseBool(strVal)
	if err != nil {
		return fallback
	}
	return val
}
