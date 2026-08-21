package database

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/schema"
)

type ClickHouseDriver struct {
	cfg *config.Config
	db  *sql.DB
}

func NewClickHouseDriver(cfg *config.Config) *ClickHouseDriver {
	return &ClickHouseDriver{cfg: cfg}
}

func (c *ClickHouseDriver) Connect() error {
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?dial_timeout=10s",
		c.cfg.DBUser, c.cfg.DBPassword, c.cfg.DBHost, c.cfg.DBPort, c.cfg.DBName)

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("failed opening clickhouse connection: %w", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(10 * time.Minute)

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed connecting to clickhouse: %w", err)
	}

	c.db = db
	log.Printf("[ClickHouse] Conectado com sucesso em %s:%d/%s", c.cfg.DBHost, c.cfg.DBPort, c.cfg.DBName)
	return nil
}

func (c *ClickHouseDriver) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *ClickHouseDriver) InitSchema() error {
	createMetaTable := `
	CREATE TABLE IF NOT EXISTS etl_metadata (
		id UInt64,
		data_month String,
		processed_at DateTime DEFAULT now()
	) ENGINE = MergeTree() ORDER BY id;
	CREATE TABLE IF NOT EXISTS etl_processed_files (
		id UInt64,
		data_month String,
		filename String,
		status String,
		processed_at DateTime DEFAULT now()
	) ENGINE = MergeTree() ORDER BY (data_month, filename);`
	if _, err := c.db.Exec(createMetaTable); err != nil {
		return fmt.Errorf("failed creating etl_metadata/etl_processed_files tables: %w", err)
	}

	for _, t := range schema.Tables {
		var colDefs []string
		primaryKey := "tuple()"

		for _, col := range t.Columns {
			chType := "String"
			if col.Type == "INTEGER" {
				chType = "Int64"
			} else if col.Type == "NUMERIC" {
				chType = "Float64"
			}
			colDefs = append(colDefs, fmt.Sprintf("%s %s", col.Name, chType))
		}

		if t.Name == "empresa" {
			primaryKey = "cnpj_basico"
		} else if t.Name == "estabelecimento" {
			primaryKey = "(cnpj_basico, cnpj_ordem, cnpj_dv)"
		} else if t.Name == "simples" {
			primaryKey = "cnpj_basico"
		} else if len(t.Columns) > 0 {
			primaryKey = t.Columns[0].Name
		}

		// ReplacingMergeTree dedupes rows sharing the same ORDER BY key on merge/FINAL,
		// since ClickHouse has no unique constraint and reprocessing a file would
		// otherwise duplicate every row (plain MergeTree has no dedupe semantics).
		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = ReplacingMergeTree() ORDER BY %s;",
			t.Name, strings.Join(colDefs, ", "), primaryKey)

		if _, err := c.db.Exec(ddl); err != nil {
			return fmt.Errorf("failed creating table %s in clickhouse: %w", t.Name, err)
		}
	}

	log.Println("[ClickHouse] DDL e Schemas inicializados com sucesso.")
	return nil
}

// EnsureIndexes is a no-op for ClickHouse: MergeTree's sort key (ORDER BY) is
// its primary index and is fixed at table creation time, there are no
// separate secondary indexes to defer here.
func (c *ClickHouseDriver) EnsureIndexes() error {
	return nil
}

func (c *ClickHouseDriver) GetLatestProcessedMonth() (string, error) {
	var month string
	err := c.db.QueryRow("SELECT data_month FROM etl_metadata ORDER BY processed_at DESC LIMIT 1").Scan(&month)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return month, nil
}

func (c *ClickHouseDriver) SaveProcessedMonth(month string) error {
	_, err := c.db.Exec("INSERT INTO etl_metadata (id, data_month) VALUES (?, ?)", time.Now().UnixNano(), month)
	return err
}

func (c *ClickHouseDriver) IsFileProcessed(dataMonth string, filename string) (bool, error) {
	var count uint64
	err := c.db.QueryRow("SELECT count() FROM etl_processed_files WHERE data_month = ? AND filename = ? AND status = 'SUCCESS'", dataMonth, filename).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (c *ClickHouseDriver) SaveProcessedFile(dataMonth string, filename string, status string) error {
	_, err := c.db.Exec("INSERT INTO etl_processed_files (id, data_month, filename, status) VALUES (?, ?, ?, ?)", time.Now().UnixNano(), dataMonth, filename, status)
	return err
}

func (c *ClickHouseDriver) InsertBatch(table schema.TableSpec, rows [][]string) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed starting clickhouse transaction: %w", err)
	}
	defer tx.Rollback()

	cols := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		cols[i] = col.Name
	}

	stmtStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table.Name, strings.Join(cols, ", "), strings.Repeat("?, ", len(cols)-1)+"?")

	stmt, err := tx.Prepare(stmtStr)
	if err != nil {
		return fmt.Errorf("failed preparing clickhouse insert statement: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		args := make([]interface{}, len(table.Columns))
		for cIdx := 0; cIdx < len(table.Columns); cIdx++ {
			var val string
			if cIdx < len(row) {
				val = row[cIdx]
			}
			args[cIdx] = sanitizeValueClickHouse(val, table.Columns[cIdx].Type)
		}
		if _, err := stmt.Exec(args...); err != nil {
			return fmt.Errorf("error executing batch row insert in clickhouse: %w", err)
		}
	}

	return tx.Commit()
}

func (c *ClickHouseDriver) GetCNPJ(cleanCNPJ string) (map[string]interface{}, error) {
	if len(cleanCNPJ) < 14 {
		return nil, fmt.Errorf("CNPJ deve ter 14 caracteres")
	}

	cnpjBasico := cleanCNPJ[:8]
	cnpjOrdem := cleanCNPJ[8:12]
	cnpjDV := cleanCNPJ[12:14]

	query := `
	SELECT 
		concat(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS cnpj,
		emp.razao_social,
		e.nome_fantasia,
		e.situacao_cadastral,
		e.data_inicio_atividade,
		e.uf,
		e.municipio,
		e.tipo_logradouro,
		e.logradouro,
		e.numero,
		e.complemento,
		e.bairro,
		e.cep,
		e.ddd_1,
		e.telefone_1,
		e.correio_eletronico,
		s.opcao_pelo_simples,
		s.opcao_mei
	FROM estabelecimento e FINAL
	LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
	LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
	WHERE e.cnpj_basico = ? AND e.cnpj_ordem = ? AND e.cnpj_dv = ?
	LIMIT 1;`

	row := c.db.QueryRow(query, cnpjBasico, cnpjOrdem, cnpjDV)

	var cnpjOut, razao, fantasia, uf, tpLog, lograd, num, comp, bairro, cep, ddd1, tel1, email, simplesOpt, meiOpt sql.NullString
	var sitCad, munic, dtInicio sql.NullInt64

	err := row.Scan(
		&cnpjOut, &razao, &fantasia, &sitCad, &dtInicio, &uf, &munic,
		&tpLog, &lograd, &num, &comp, &bairro, &cep, &ddd1, &tel1, &email,
		&simplesOpt, &meiOpt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("CNPJ não encontrado")
	}
	if err != nil {
		return nil, err
	}

	result := map[string]interface{}{
		"cnpj":                   cnpjOut.String,
		"razao_social":           razao.String,
		"nome_fantasia":          fantasia.String,
		"situacao_cadastral":     sitCad.Int64,
		"data_inicio_atividade": dtInicio.Int64,
		"uf":                     uf.String,
		"municipio":              munic.Int64,
		"logradouro":             fmt.Sprintf("%s %s, %s %s", tpLog.String, lograd.String, num.String, comp.String),
		"bairro":                 bairro.String,
		"cep":                    cep.String,
		"telefone":               fmt.Sprintf("(%s) %s", ddd1.String, tel1.String),
		"email":                  email.String,
		"opcao_simples":          simplesOpt.String,
		"opcao_mei":              meiOpt.String,
	}

	return result, nil
}

func (c *ClickHouseDriver) SearchCNPJ(query string, uf string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	whereClause := "WHERE (lower(emp.razao_social) LIKE ? OR lower(e.nome_fantasia) LIKE ?)"
	args := []interface{}{"%" + strings.ToLower(query) + "%", "%" + strings.ToLower(query) + "%"}

	if uf != "" {
		whereClause += " AND e.uf = ?"
		args = append(args, strings.ToUpper(uf))
	}

	sqlStr := fmt.Sprintf(`
	SELECT 
		concat(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS cnpj,
		emp.razao_social,
		e.nome_fantasia,
		e.uf,
		e.cnae_fiscal_principal
	FROM estabelecimento e FINAL
	LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
	%s
	LIMIT %d;`, whereClause, limit)

	rows, err := c.db.Query(sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var cnpj, razao, fantasia, ufVal sql.NullString
		var cnae sql.NullInt64
		if err := rows.Scan(&cnpj, &razao, &fantasia, &ufVal, &cnae); err == nil {
			results = append(results, map[string]interface{}{
				"cnpj":          cnpj.String,
				"razao_social":  razao.String,
				"nome_fantasia": fantasia.String,
				"uf":            ufVal.String,
				"cnae":          cnae.Int64,
			})
		}
	}

	return results, nil
}

func (c *ClickHouseDriver) GetStats() (map[string]interface{}, error) {
	var totalEmpresas, totalEstab, totalSocios int64

	_ = c.db.QueryRow("SELECT count() FROM empresa FINAL").Scan(&totalEmpresas)
	_ = c.db.QueryRow("SELECT count() FROM estabelecimento FINAL").Scan(&totalEstab)
	_ = c.db.QueryRow("SELECT count() FROM socios FINAL").Scan(&totalSocios)

	latestMonth, _ := c.GetLatestProcessedMonth()

	return map[string]interface{}{
		"total_empresas":        totalEmpresas,
		"total_estabelecimentos": totalEstab,
		"total_socios":          totalSocios,
		"ultima_competencia":    latestMonth,
		"driver":               "ClickHouse",
	}, nil
}

func sanitizeValueClickHouse(val string, colType string) interface{} {
	val = strings.TrimSpace(val)
	if val == "" {
		if colType == "INTEGER" || colType == "NUMERIC" {
			return 0
		}
		return ""
	}
	if colType == "NUMERIC" {
		val = strings.ReplaceAll(val, ",", ".")
		num, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return float64(0)
		}
		return num
	}
	if colType == "INTEGER" {
		num, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return int64(0)
		}
		return num
	}
	return val
}
