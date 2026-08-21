package database

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/schema"
)

type MySQLDriver struct {
	cfg *config.Config
	db  *sql.DB

	// loadDataUnavailable latches to true the first time the server rejects
	// LOAD DATA LOCAL INFILE (e.g. local_infile=0). Once set, subsequent
	// batches skip straight to the slower INSERT IGNORE fallback instead of
	// repeatedly retrying and failing against a server that will never allow it.
	loadDataUnavailable atomic.Bool
}

var loadDataHandlerSeq int64

func NewMySQLDriver(cfg *config.Config) *MySQLDriver {
	return &MySQLDriver{cfg: cfg}
}

func (m *MySQLDriver) Connect() error {
	adminDSN := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/?parseTime=true&multiStatements=true",
		m.cfg.DBUser, m.cfg.DBPassword, m.cfg.DBHost, m.cfg.DBPort,
	)

	adminDB, err := sql.Open("mysql", adminDSN)
	if err == nil {
		defer adminDB.Close()
		log.Printf("[MySQL] Verificando existência da base '%s'...", m.cfg.DBName)
		_, _ = adminDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;", m.cfg.DBName))
	}

	dsn := fmt.Sprintf(
		// interpolateParams evita o round-trip extra de prepared statement
		// (COM_STMT_PREPARE + COM_STMT_EXECUTE) em cada INSERT em lote,
		// enviando a query já com os valores interpolados client-side.
		"%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&interpolateParams=true",
		m.cfg.DBUser, m.cfg.DBPassword, m.cfg.DBHost, m.cfg.DBPort, m.cfg.DBName,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed opening mysql connection: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed connecting to mysql: %w", err)
	}

	m.db = db
	log.Printf("[MySQL] Conectado com sucesso em %s:%d/%s", m.cfg.DBHost, m.cfg.DBPort, m.cfg.DBName)
	return nil
}

func (m *MySQLDriver) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func (m *MySQLDriver) InitSchema() error {
	createMetaTable := `
	CREATE TABLE IF NOT EXISTS etl_metadata (
		id INT AUTO_INCREMENT PRIMARY KEY,
		data_month VARCHAR(7) NOT NULL,
		processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	CREATE TABLE IF NOT EXISTS etl_processed_files (
		id INT AUTO_INCREMENT PRIMARY KEY,
		data_month VARCHAR(7) NOT NULL,
		filename VARCHAR(255) NOT NULL,
		status VARCHAR(32) NOT NULL,
		processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`
	if _, err := m.db.Exec(createMetaTable); err != nil {
		return fmt.Errorf("failed creating etl_metadata/etl_processed_files tables: %w", err)
	}

	for _, t := range schema.Tables {
		var colDefs []string
		for _, col := range t.Columns {
			mySQLType := "TEXT"
			if col.Type == "INTEGER" {
				mySQLType = "INT"
			} else if col.Type == "NUMERIC" {
				mySQLType = "DECIMAL(15,2)"
			} else if col.Name == "cnpj_basico" {
				mySQLType = "VARCHAR(8)"
			} else if col.Name == "cnpj_ordem" {
				mySQLType = "VARCHAR(4)"
			} else if col.Name == "cnpj_dv" {
				mySQLType = "VARCHAR(2)"
			} else if col.Name == "codigo" {
				mySQLType = "INT"
			} else if col.Name == "uf" {
				mySQLType = "VARCHAR(2)"
			}
			colDefs = append(colDefs, fmt.Sprintf("`%s` %s", col.Name, mySQLType))
		}

		var primaryKeyClause string
		if t.Name == "empresa" {
			primaryKeyClause = ", PRIMARY KEY (`cnpj_basico`)"
		} else if t.Name == "estabelecimento" {
			primaryKeyClause = ", PRIMARY KEY (`cnpj_basico`, `cnpj_ordem`, `cnpj_dv`)"
		} else if t.Name == "simples" {
			primaryKeyClause = ", PRIMARY KEY (`cnpj_basico`)"
		} else if len(t.Columns) > 0 && t.Columns[0].Name == "codigo" {
			primaryKeyClause = ", PRIMARY KEY (`codigo`)"
		}

		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", t.Name, strings.Join(colDefs, ", "), primaryKeyClause)
		if _, err := m.db.Exec(ddl); err != nil {
			return fmt.Errorf("failed creating table %s: %w", t.Name, err)
		}
	}

	createView := `
	CREATE OR REPLACE VIEW vw_cnpj_completo AS
	SELECT 
		CONCAT(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS cnpj,
		emp.razao_social,
		e.nome_fantasia,
		e.situacao_cadastral,
		moti.descricao AS motivo_situacao_cadastral,
		e.uf,
		mun.descricao AS municipio,
		cnae.descricao AS cnae_fiscal_principal_descricao,
		e.cnae_fiscal_principal,
		nat.descricao AS natureza_juridica,
		e.correio_eletronico,
		e.telefone_1,
		s.opcao_pelo_simples,
		s.opcao_mei
	FROM estabelecimento e
	LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
	LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
	LEFT JOIN cnae cnae ON e.cnae_fiscal_principal = cnae.codigo
	LEFT JOIN municipio mun ON e.municipio = mun.codigo
	LEFT JOIN motivo_situacao_cadastral moti ON e.motivo_situacao_cadastral = moti.codigo
	LEFT JOIN natureza_juridica nat ON emp.natureza_juridica = nat.codigo;`
	if _, err := m.db.Exec(createView); err != nil {
		log.Printf("[MySQL] Warning ao criar view vw_cnpj_completo: %v", err)
	}

	log.Println("[MySQL] DDL, Schemas e Views inicializados com sucesso.")
	return nil
}

// EnsureIndexes creates secondary indexes if missing. MySQL's CREATE INDEX has
// no IF NOT EXISTS clause, so a "duplicate key name" error on a re-run is
// expected and logged at debug level rather than treated as a failure.
func (m *MySQLDriver) EnsureIndexes() error {
	indexes := []map[string]string{
		{"table": "empresa", "name": "idx_empresa_cnpj", "col": "cnpj_basico"},
		{"table": "empresa", "name": "idx_empresa_razao", "col": "razao_social(100)"},
		{"table": "estabelecimento", "name": "idx_estabelecimento_cnpj", "col": "cnpj_basico"},
		{"table": "estabelecimento", "name": "idx_estabelecimento_uf", "col": "uf"},
		{"table": "estabelecimento", "name": "idx_estabelecimento_municipio", "col": "municipio"},
		{"table": "estabelecimento", "name": "idx_estabelecimento_cnae", "col": "cnae_fiscal_principal"},
		{"table": "estabelecimento", "name": "idx_estabelecimento_situacao", "col": "situacao_cadastral"},
		{"table": "estabelecimento", "name": "idx_estabelecimento_fantasia", "col": "nome_fantasia(100)"},
		{"table": "socios", "name": "idx_socios_cnpj", "col": "cnpj_basico"},
		{"table": "simples", "name": "idx_simples_cnpj", "col": "cnpj_basico"},
		{"table": "etl_processed_files", "name": "idx_etl_files_month_file", "col": "data_month, filename"},
	}

	log.Println("[MySQL] Garantindo índices secundários (pode demorar na primeira carga completa)...")
	for _, idx := range indexes {
		sqlStr := fmt.Sprintf("CREATE INDEX %s ON %s(%s);", idx["name"], idx["table"], idx["col"])
		if _, err := m.db.Exec(sqlStr); err != nil {
			if !strings.Contains(err.Error(), "Duplicate key name") {
				log.Printf("[MySQL] Warning ao criar índice %s: %v", idx["name"], err)
			}
		}
	}
	log.Println("[MySQL] Índices secundários prontos.")
	return nil
}

func (m *MySQLDriver) GetLatestProcessedMonth() (string, error) {
	var month string
	err := m.db.QueryRow("SELECT data_month FROM etl_metadata ORDER BY id DESC LIMIT 1").Scan(&month)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return month, nil
}

func (m *MySQLDriver) SaveProcessedMonth(month string) error {
	_, err := m.db.Exec("INSERT INTO etl_metadata (data_month) VALUES (?)", month)
	return err
}

func (m *MySQLDriver) IsFileProcessed(dataMonth string, filename string) (bool, error) {
	var count int
	err := m.db.QueryRow("SELECT COUNT(*) FROM etl_processed_files WHERE data_month = ? AND filename = ? AND status = 'SUCCESS'", dataMonth, filename).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (m *MySQLDriver) SaveProcessedFile(dataMonth string, filename string, status string) error {
	_, err := m.db.Exec("INSERT INTO etl_processed_files (data_month, filename, status) VALUES (?, ?, ?)", dataMonth, filename, status)
	return err
}

// BatchLimit: LOAD DATA LOCAL INFILE has no placeholder ceiling (it streams
// text, not bound `?` params), so while it's available we let BatchSize
// control the row count directly instead of dividing it down by column
// count like the placeholder-bound drivers. If LOAD DATA has been disabled
// by the server, fall back to the same placeholder-safe limit Postgres/
// SQLite use, since insertBatchInsertIgnore takes over at that point.
func (m *MySQLDriver) BatchLimit(numCols int) int {
	if m.loadDataUnavailable.Load() {
		return placeholderBatchLimit(numCols, m.cfg.BatchSize, 65000)
	}
	if m.cfg.BatchSize <= 0 {
		return 10000
	}
	return m.cfg.BatchSize
}

func (m *MySQLDriver) InsertBatch(table schema.TableSpec, rows [][]string) error {
	if len(rows) == 0 {
		return nil
	}

	if !m.loadDataUnavailable.Load() {
		err := m.insertBatchLoadData(table, rows)
		if err == nil {
			return nil
		}
		if !isLocalInfileDisabledErr(err) {
			return err
		}
		m.loadDataUnavailable.Store(true)
		log.Printf("[MySQL] AVISO: LOAD DATA LOCAL INFILE indisponível no servidor (%v). Caindo para o modo INSERT IGNORE em lote (mais lento) pelo restante da execução. Para acelerar, habilite `local_infile=1` no servidor MySQL.", err)
	}

	return m.insertBatchInsertIgnore(table, rows)
}

// insertBatchLoadData bulk-loads rows via MySQL's native LOAD DATA LOCAL
// INFILE using a driver-registered in-memory Reader (no temp file touches
// disk). This is dramatically faster than a batched multi-row INSERT because
// it uses MySQL's dedicated bulk-load path instead of parsing/executing a
// large INSERT statement. "IGNORE" preserves the same idempotency semantics
// as the INSERT IGNORE fallback: rows colliding with an existing primary key
// are silently skipped.
func (m *MySQLDriver) insertBatchLoadData(table schema.TableSpec, rows [][]string) error {
	var buf bytes.Buffer
	for _, row := range rows {
		for cIdx, col := range table.Columns {
			if cIdx > 0 {
				buf.WriteByte('\t')
			}
			var val string
			if cIdx < len(row) {
				val = row[cIdx]
			}
			buf.WriteString(formatValueForLoadData(val, col.Type))
		}
		buf.WriteByte('\n')
	}
	data := buf.Bytes()

	handlerName := fmt.Sprintf("cnpjrbf_%s_%d_%d", table.Name, time.Now().UnixNano(), atomic.AddInt64(&loadDataHandlerSeq, 1))
	mysqldriver.RegisterReaderHandler(handlerName, func() io.Reader {
		return bytes.NewReader(data)
	})
	defer mysqldriver.DeregisterReaderHandler(handlerName)

	cols := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		cols[i] = fmt.Sprintf("`%s`", c.Name)
	}

	query := fmt.Sprintf(
		"LOAD DATA LOCAL INFILE 'Reader::%s' IGNORE INTO TABLE `%s` CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' (%s)",
		handlerName, table.Name, strings.Join(cols, ","),
	)

	if _, err := m.db.Exec(query); err != nil {
		return fmt.Errorf("load data infile error in table %s: %w", table.Name, err)
	}
	return nil
}

// insertBatchInsertIgnore is the original batched multi-row INSERT IGNORE
// path, kept as a fallback for MySQL servers with local_infile disabled.
//
// It re-chunks internally to a placeholder-safe sub-batch size regardless of
// how many rows it's handed: the caller sizes batches assuming LOAD DATA (no
// placeholder ceiling) up front, so if the LOAD DATA fallback triggers mid-
// file, the batch already in flight can be far bigger than a single INSERT
// statement can safely hold.
func (m *MySQLDriver) insertBatchInsertIgnore(table schema.TableSpec, rows [][]string) error {
	safeLimit := placeholderBatchLimit(len(table.Columns), m.cfg.BatchSize, 65000)
	if len(rows) > safeLimit {
		for start := 0; start < len(rows); start += safeLimit {
			end := start + safeLimit
			if end > len(rows) {
				end = len(rows)
			}
			if err := m.insertBatchInsertIgnoreChunk(table, rows[start:end]); err != nil {
				return err
			}
		}
		return nil
	}
	return m.insertBatchInsertIgnoreChunk(table, rows)
}

func (m *MySQLDriver) insertBatchInsertIgnoreChunk(table schema.TableSpec, rows [][]string) error {
	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("failed starting mysql transaction: %w", err)
	}
	defer tx.Rollback()

	cols := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		cols[i] = fmt.Sprintf("`%s`", c.Name)
	}

	numCols := len(table.Columns)
	valueStrings := make([]string, 0, len(rows))
	valueArgs := make([]interface{}, 0, len(rows)*numCols)

	for _, row := range rows {
		placeholders := make([]string, numCols)
		for cIdx := 0; cIdx < numCols; cIdx++ {
			placeholders[cIdx] = "?"

			var val string
			if cIdx < len(row) {
				val = row[cIdx]
			}
			valueArgs = append(valueArgs, sanitizeValueMySQL(val, table.Columns[cIdx].Type))
		}
		valueStrings = append(valueStrings, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	stmtStr := fmt.Sprintf("INSERT IGNORE INTO `%s` (%s) VALUES %s", table.Name, strings.Join(cols, ", "), strings.Join(valueStrings, ", "))

	if _, err := tx.Exec(stmtStr, valueArgs...); err != nil {
		return fmt.Errorf("bulk insert error in table %s: %w", table.Name, err)
	}

	return tx.Commit()
}

// formatValueForLoadData renders a raw CSV field as a LOAD DATA text-format
// value, mirroring sanitizeValueMySQL's semantics: an empty/unparsable value
// becomes SQL NULL ("\N" in LOAD DATA's text format), everything else is
// escaped so literal tabs/newlines/backslashes in the data can't be mistaken
// for field/line terminators.
func formatValueForLoadData(val string, colType string) string {
	val = strings.TrimSpace(val)
	if val == "" {
		return `\N`
	}
	if colType == "NUMERIC" {
		val = strings.ReplaceAll(val, ",", ".")
		if _, err := strconv.ParseFloat(val, 64); err != nil {
			return `\N`
		}
		return escapeForLoadData(val)
	}
	if colType == "INTEGER" {
		if _, err := strconv.Atoi(val); err != nil {
			return `\N`
		}
		return escapeForLoadData(val)
	}
	return escapeForLoadData(val)
}

func escapeForLoadData(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case 0:
			b.WriteString(`\0`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// isLocalInfileDisabledErr detects the server-side rejection of LOAD DATA
// LOCAL INFILE (MySQL error 1148), which happens when the server's
// local_infile system variable is off. Any other error is treated as a real
// failure and propagated instead of triggering the fallback, so genuine bugs
// aren't silently masked.
func isLocalInfileDisabledErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "1148") ||
		strings.Contains(msg, "local_infile") ||
		strings.Contains(msg, "local infile") ||
		strings.Contains(msg, "not allowed with this mysql")
}

func (m *MySQLDriver) GetCNPJ(cleanCNPJ string) (map[string]interface{}, error) {
	if len(cleanCNPJ) < 14 {
		return nil, fmt.Errorf("CNPJ deve ter 14 caracteres")
	}

	cnpjBasico := cleanCNPJ[:8]
	cnpjOrdem := cleanCNPJ[8:12]
	cnpjDV := cleanCNPJ[12:14]

	query := `
	SELECT 
		CONCAT(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS cnpj,
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
		COALESCE(s.opcao_pelo_simples, 'N') AS opcao_pelo_simples,
		COALESCE(s.opcao_mei, 'N') AS opcao_mei
	FROM estabelecimento e
	LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
	LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
	WHERE e.cnpj_basico = ? AND e.cnpj_ordem = ? AND e.cnpj_dv = ?
	LIMIT 1;`

	row := m.db.QueryRow(query, cnpjBasico, cnpjOrdem, cnpjDV)

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

func (m *MySQLDriver) SearchCNPJ(query string, uf string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	whereClause := "WHERE (LOWER(emp.razao_social) LIKE ? OR LOWER(e.nome_fantasia) LIKE ?)"
	args := []interface{}{"%" + strings.ToLower(query) + "%", "%" + strings.ToLower(query) + "%"}

	if uf != "" {
		whereClause += " AND e.uf = ?"
		args = append(args, strings.ToUpper(uf))
	}

	sqlStr := fmt.Sprintf(`
	SELECT 
		CONCAT(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS cnpj,
		emp.razao_social,
		e.nome_fantasia,
		e.uf,
		e.cnae_fiscal_principal
	FROM estabelecimento e
	LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
	%s
	LIMIT %d;`, whereClause, limit)

	rows, err := m.db.Query(sqlStr, args...)
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

func (m *MySQLDriver) GetStats() (map[string]interface{}, error) {
	var totalEmpresas, totalEstab, totalSocios int64

	_ = m.db.QueryRow("SELECT COUNT(*) FROM empresa").Scan(&totalEmpresas)
	_ = m.db.QueryRow("SELECT COUNT(*) FROM estabelecimento").Scan(&totalEstab)
	_ = m.db.QueryRow("SELECT COUNT(*) FROM socios").Scan(&totalSocios)

	latestMonth, _ := m.GetLatestProcessedMonth()

	return map[string]interface{}{
		"total_empresas":        totalEmpresas,
		"total_estabelecimentos": totalEstab,
		"total_socios":          totalSocios,
		"ultima_competencia":    latestMonth,
		"driver":               "MySQL",
	}, nil
}

func sanitizeValueMySQL(val string, colType string) interface{} {
	val = strings.TrimSpace(val)
	if val == "" {
		return nil
	}
	if colType == "NUMERIC" {
		val = strings.ReplaceAll(val, ",", ".")
		num, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil
		}
		return num
	}
	if colType == "INTEGER" {
		num, err := strconv.Atoi(val)
		if err != nil {
			return nil
		}
		return num
	}
	return val
}
