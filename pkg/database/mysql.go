package database

import (
	"bytes"
	"context"
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

	// fulltextSearchReady latches to true once EnsureIndexes confirms the
	// FULLTEXT indexes used by SearchCNPJ's fast path exist. Until then (e.g.
	// a fresh deployment before the first pipeline run), SearchCNPJ uses the
	// original LIKE-based query instead of erroring out.
	fulltextSearchReady atomic.Bool
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

// mysqlColumnTypeOverrides sizes columns that are semantically short,
// fixed-format fields as VARCHAR(n) instead of the generic TEXT the schema
// package declares them as. TEXT stores off-page in InnoDB and can't be
// indexed without a prefix length, so this shrinks row size and makes plain
// (non-FULLTEXT) indexes on these columns actually useful. Widths follow the
// Receita Federal layout spec with headroom; columns not listed here that
// still map to TEXT (only cnae_fiscal_secundaria in practice) are genuinely
// variable-length lists that don't fit a fixed width safely.
//
// NOTE: this only affects `CREATE TABLE IF NOT EXISTS` on a fresh database —
// it does NOT retroactively migrate an existing table already running in
// production. See docs/TROUBLESHOOTING.md for how to apply it manually if
// you want it on a database that already has data.
var mysqlColumnTypeOverrides = map[string]string{
	"cnpj_basico":                 "VARCHAR(8)",
	"cnpj_ordem":                  "VARCHAR(4)",
	"cnpj_dv":                     "VARCHAR(2)",
	"uf":                          "VARCHAR(2)",
	"razao_social":                "VARCHAR(200)",
	"nome_fantasia":               "VARCHAR(60)",
	"ente_federativo_responsavel": "VARCHAR(60)",
	"nome_cidade_exterior":        "VARCHAR(60)",
	"pais":                        "VARCHAR(20)",
	"tipo_logradouro":             "VARCHAR(20)",
	"logradouro":                  "VARCHAR(100)",
	"numero":                      "VARCHAR(10)",
	"complemento":                 "VARCHAR(120)",
	"bairro":                      "VARCHAR(60)",
	"cep":                         "VARCHAR(8)",
	"ddd_1":                       "VARCHAR(4)",
	"telefone_1":                  "VARCHAR(12)",
	"ddd_2":                       "VARCHAR(4)",
	"telefone_2":                  "VARCHAR(12)",
	"ddd_fax":                     "VARCHAR(4)",
	"fax":                         "VARCHAR(12)",
	"correio_eletronico":          "VARCHAR(160)",
	"situacao_especial":           "VARCHAR(60)",
	"nome_socio_razao_social":     "VARCHAR(200)",
	"cpf_cnpj_socio":              "VARCHAR(14)",
	"representante_legal":         "VARCHAR(14)",
	"nome_do_representante":       "VARCHAR(200)",
	"opcao_pelo_simples":          "VARCHAR(1)",
	"opcao_mei":                   "VARCHAR(1)",
	"descricao":                   "VARCHAR(200)",
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
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	CREATE TABLE IF NOT EXISTS etl_change_log (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		cnpj_basico VARCHAR(8) NOT NULL,
		tabela VARCHAR(32) NOT NULL,
		campo VARCHAR(64) NOT NULL,
		valor_antigo TEXT,
		valor_novo TEXT,
		competencia VARCHAR(7) NOT NULL,
		detectado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
		INDEX idx_change_log_cnpj (cnpj_basico)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`
	if _, err := m.db.Exec(createMetaTable); err != nil {
		return fmt.Errorf("failed creating etl_metadata/etl_processed_files tables: %w", err)
	}

	for _, t := range schema.Tables {
		var colDefs []string
		for _, col := range t.Columns {
			mySQLType := "TEXT" // safe default for anything not covered below (e.g. cnae_fiscal_secundaria, a variable-length list of codes)
			if col.Type == "INTEGER" {
				mySQLType = "INT"
			} else if col.Type == "NUMERIC" {
				mySQLType = "DECIMAL(15,2)"
			} else if override, ok := mysqlColumnTypeOverrides[col.Name]; ok {
				mySQLType = override
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
		// nome_fantasia is VARCHAR(60) (see mysqlColumnTypeOverrides) — no
		// prefix length needed/possible beyond the column's own width.
		{"table": "estabelecimento", "name": "idx_estabelecimento_fantasia", "col": "nome_fantasia"},
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

	// SearchCNPJ's old query is `LOWER(col) LIKE '%termo%'` — a leading
	// wildcard InnoDB can never use a regular index for, so every search is
	// a full table scan. A FULLTEXT index lets SearchCNPJ switch to
	// MATCH ... AGAINST (boolean mode) instead, which is genuinely indexed.
	// fulltextSearchReady only flips to true if both indexes end up in
	// place (created now or already existing from a previous run) — until
	// then SearchCNPJ keeps using the original LIKE path unchanged.
	fulltextIndexes := []map[string]string{
		{"table": "empresa", "name": "ft_empresa_razao", "col": "razao_social"},
		{"table": "estabelecimento", "name": "ft_estabelecimento_fantasia", "col": "nome_fantasia"},
	}
	fulltextOK := true
	for _, idx := range fulltextIndexes {
		sqlStr := fmt.Sprintf("ALTER TABLE %s ADD FULLTEXT INDEX %s (%s);", idx["table"], idx["name"], idx["col"])
		if _, err := m.db.Exec(sqlStr); err != nil {
			if !strings.Contains(err.Error(), "Duplicate key name") {
				log.Printf("[MySQL] Warning ao criar índice FULLTEXT %s: %v", idx["name"], err)
				fulltextOK = false
			}
		}
	}
	m.fulltextSearchReady.Store(fulltextOK)

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

// mysqlTrackedTables maps table name to its primary-key column(s), for the
// subset of tables UpsertBatchTracked actually compares field-by-field
// against what's already stored, instead of silently skipping duplicates
// the way plain InsertBatch does.
//
// socios is deliberately absent: the RFB layout gives it no single natural
// unique key today (InitSchema doesn't define a PRIMARY KEY for it either —
// every existing import already treats it as append-only). Tracking it
// needs a schema decision — a composite key, plus a dedup pass for whatever
// duplicates a prior untracked import may already have inserted — that's a
// separate piece of work from this one.
var mysqlTrackedTables = map[string][]string{
	"empresa":         {"cnpj_basico"},
	"estabelecimento": {"cnpj_basico", "cnpj_ordem", "cnpj_dv"},
	"simples":         {"cnpj_basico"},
}

// mysqlNormalizeForCompare mirrors sanitizeValueMySQL's parsing but returns
// the canonical string form MySQL storage would produce, so a freshly
// parsed CSV value can be compared directly against a value scanned back
// from the database (see UpsertBatchTracked). Kept separate from
// sanitizeValueMySQL — which returns a typed interface{} for driver args —
// since the two solve different problems even though the parsing is the
// same.
func mysqlNormalizeForCompare(val string, colType string) string {
	val = strings.TrimSpace(val)
	if val == "" {
		return ""
	}
	switch colType {
	case "NUMERIC":
		val = strings.ReplaceAll(val, ",", ".")
		num, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return ""
		}
		return strconv.FormatFloat(num, 'f', 2, 64) // matches DECIMAL(15,2) storage
	case "INTEGER":
		num, err := strconv.Atoi(val)
		if err != nil {
			return ""
		}
		return strconv.Itoa(num)
	default:
		return val
	}
}

type mysqlChange struct {
	cnpjBasico  string
	tabela      string
	campo       string
	valorAntigo string
	valorNovo   string
}

// UpsertBatchTracked behaves like InsertBatch for the tables in
// mysqlTrackedTables: a genuinely new row (primary key not seen before) is
// inserted exactly as InsertBatch would; a row whose primary key already
// exists is compared column-by-column against what's stored, and only
// UPDATEd — with each changed field recorded in etl_change_log — if at
// least one column actually differs. Rows that already match are skipped
// entirely: no UPDATE, no log entry, no write at all. That's what keeps a
// monthly reimport cheap — most companies don't change from one
// competência to the next, so most of the batch costs one indexed lookup
// (mysqlFetchExisting) and nothing else.
func (m *MySQLDriver) UpsertBatchTracked(table schema.TableSpec, rows [][]string, competencia string) error {
	pkCols, tracked := mysqlTrackedTables[table.Name]
	if !tracked || len(rows) == 0 {
		return m.InsertBatch(table, rows)
	}

	pkIdx := make([]int, len(pkCols))
	for i, pkCol := range pkCols {
		pkIdx[i] = -1
		for ci, c := range table.Columns {
			if c.Name == pkCol {
				pkIdx[i] = ci
				break
			}
		}
		if pkIdx[i] == -1 {
			// Defensive: schema and mysqlTrackedTables disagree. Fall back
			// to the untracked path rather than panic on a bad index.
			return m.InsertBatch(table, rows)
		}
	}

	rowValue := func(row []string, idx int) string {
		if idx < len(row) {
			return row[idx]
		}
		return ""
	}
	keyOf := func(row []string) string {
		parts := make([]string, len(pkIdx))
		for i, idx := range pkIdx {
			parts[i] = rowValue(row, idx)
		}
		return strings.Join(parts, "\x1f")
	}

	existing, err := m.mysqlFetchExisting(table, pkCols, pkIdx, rows)
	if err != nil {
		return fmt.Errorf("erro ao buscar linhas existentes de `%s` para comparação: %w", table.Name, err)
	}

	var newRows [][]string
	var changedRows [][]string
	var changes []mysqlChange

	for _, row := range rows {
		old, found := existing[keyOf(row)]
		if !found {
			newRows = append(newRows, row)
			continue
		}

		rowChanged := false
		for ci, col := range table.Columns {
			newVal := mysqlNormalizeForCompare(rowValue(row, ci), col.Type)
			if newVal != old[ci] {
				rowChanged = true
				changes = append(changes, mysqlChange{
					cnpjBasico:  rowValue(row, pkIdx[0]), // cnpj_basico is always the first tracked column
					tabela:      table.Name,
					campo:       col.Name,
					valorAntigo: old[ci],
					valorNovo:   newVal,
				})
			}
		}
		if rowChanged {
			changedRows = append(changedRows, row)
		}
	}

	if len(newRows) > 0 {
		if err := m.InsertBatch(table, newRows); err != nil {
			return fmt.Errorf("erro ao inserir linhas novas de `%s`: %w", table.Name, err)
		}
	}
	if len(changedRows) > 0 {
		// The UPDATE and its etl_change_log entries must land together: a
		// row that changed with no matching log entry (or a log entry for a
		// change that never actually got applied) defeats the entire point
		// of this feature. Both happen in mysqlApplyChangesTx's single
		// transaction, not two separate ones.
		if err := m.mysqlApplyChangesTx(table, pkCols, pkIdx, changedRows, changes, competencia); err != nil {
			return fmt.Errorf("erro ao aplicar alterações de `%s`: %w", table.Name, err)
		}
	}

	return nil
}

// mysqlFetchExisting looks up which of the batch's rows already exist,
// returning each found row's columns as canonical comparison strings
// (mysqlNormalizeForCompare's format), keyed the same way UpsertBatchTracked
// builds keys. Chunked to stay under MySQL's placeholder ceiling — this is
// an indexed point lookup (IN on the primary key), not a table scan, so it
// stays fast regardless of how big the table already is.
func (m *MySQLDriver) mysqlFetchExisting(table schema.TableSpec, pkCols []string, pkIdx []int, rows [][]string) (map[string][]string, error) {
	result := make(map[string][]string, len(rows))

	cols := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		cols[i] = fmt.Sprintf("`%s`", c.Name)
	}
	quotedPK := make([]string, len(pkCols))
	for i, c := range pkCols {
		quotedPK[i] = fmt.Sprintf("`%s`", c)
	}

	chunkSize := placeholderBatchLimit(len(pkCols), m.cfg.BatchSize, 65000)

	for start := 0; start < len(rows); start += chunkSize {
		end := start + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[start:end]

		var whereClause string
		args := make([]interface{}, 0, len(chunk)*len(pkCols))
		if len(pkCols) == 1 {
			placeholders := make([]string, len(chunk))
			for i, row := range chunk {
				placeholders[i] = "?"
				args = append(args, row[pkIdx[0]])
			}
			whereClause = fmt.Sprintf("%s IN (%s)", quotedPK[0], strings.Join(placeholders, ","))
		} else {
			tuplePlaceholder := "(" + strings.Repeat("?,", len(pkCols)-1) + "?)"
			tuples := make([]string, len(chunk))
			for i, row := range chunk {
				tuples[i] = tuplePlaceholder
				for _, idx := range pkIdx {
					args = append(args, row[idx])
				}
			}
			whereClause = fmt.Sprintf("(%s) IN (%s)", strings.Join(quotedPK, ","), strings.Join(tuples, ","))
		}

		querySQL := fmt.Sprintf("SELECT %s FROM `%s` WHERE %s", strings.Join(cols, ","), table.Name, whereClause)

		if err := func() error {
			queryRows, err := m.db.Query(querySQL, args...)
			if err != nil {
				return err
			}
			defer queryRows.Close()

			scanTargets := make([]sql.NullString, len(table.Columns))
			scanArgs := make([]interface{}, len(table.Columns))
			for i := range scanTargets {
				scanArgs[i] = &scanTargets[i]
			}

			for queryRows.Next() {
				if err := queryRows.Scan(scanArgs...); err != nil {
					return err
				}
				values := make([]string, len(table.Columns))
				for i, ns := range scanTargets {
					values[i] = ns.String
				}
				keyParts := make([]string, len(pkIdx))
				for pi, idx := range pkIdx {
					keyParts[pi] = values[idx]
				}
				result[strings.Join(keyParts, "\x1f")] = values
			}
			return queryRows.Err()
		}(); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// mysqlApplyChangesTx applies every UPDATE for rows UpsertBatchTracked
// determined actually changed, together with all of their etl_change_log
// entries, inside a single transaction. This must not be two separate
// transactions: a row updated with no matching log entry (say, because the
// log insert failed after the update already committed) silently defeats
// the entire point of this feature — the data changes but nothing explains
// when or why. Verified live: an early version of this code did split them,
// and a table-not-found error on the log insert left a real production row
// changed with no record of it (see git history / commit message for this
// change).
func (m *MySQLDriver) mysqlApplyChangesTx(table schema.TableSpec, pkCols []string, pkIdx []int, changedRows [][]string, changes []mysqlChange, competencia string) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	setClauses := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		setClauses[i] = fmt.Sprintf("`%s` = ?", c.Name)
	}
	whereClauses := make([]string, len(pkCols))
	for i, c := range pkCols {
		whereClauses[i] = fmt.Sprintf("`%s` = ?", c)
	}
	updateStmtStr := fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", table.Name, strings.Join(setClauses, ", "), strings.Join(whereClauses, " AND "))

	updateStmt, err := tx.Prepare(updateStmtStr)
	if err != nil {
		return err
	}
	defer updateStmt.Close()

	for _, row := range changedRows {
		args := make([]interface{}, 0, len(table.Columns)+len(pkCols))
		for ci, c := range table.Columns {
			var val string
			if ci < len(row) {
				val = row[ci]
			}
			args = append(args, sanitizeValueMySQL(val, c.Type))
		}
		for _, idx := range pkIdx {
			args = append(args, row[idx])
		}
		if _, err := updateStmt.Exec(args...); err != nil {
			return err
		}
	}

	const chunkSize = 5000 // 6 placeholders/row
	for start := 0; start < len(changes); start += chunkSize {
		end := start + chunkSize
		if end > len(changes) {
			end = len(changes)
		}
		chunk := changes[start:end]

		valueStrings := make([]string, len(chunk))
		args := make([]interface{}, 0, len(chunk)*6)
		for i, c := range chunk {
			valueStrings[i] = "(?, ?, ?, ?, ?, ?)"
			args = append(args, c.cnpjBasico, c.tabela, c.campo, c.valorAntigo, c.valorNovo, competencia)
		}

		logSQL := fmt.Sprintf(
			"INSERT INTO etl_change_log (cnpj_basico, tabela, campo, valor_antigo, valor_novo, competencia) VALUES %s",
			strings.Join(valueStrings, ","),
		)
		if _, err := tx.Exec(logSQL, args...); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetChangeHistory returns every recorded change for a company's
// cnpj_basico across all tracked tables, newest first.
func (m *MySQLDriver) GetChangeHistory(cnpjBasico string) ([]ChangeLogEntry, error) {
	rows, err := m.db.Query(
		"SELECT tabela, campo, valor_antigo, valor_novo, competencia, detectado_em FROM etl_change_log WHERE cnpj_basico = ? ORDER BY detectado_em DESC, id DESC",
		cnpjBasico,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := []ChangeLogEntry{}
	for rows.Next() {
		var e ChangeLogEntry
		var antigo, novo sql.NullString
		if err := rows.Scan(&e.Tabela, &e.Campo, &antigo, &novo, &e.Competencia, &e.DetectadoEm); err != nil {
			return nil, err
		}
		e.ValorAntigo = antigo.String
		e.ValorNovo = novo.String
		entries = append(entries, e)
	}
	return entries, rows.Err()
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
// LOCAL INFILE, which happens when the server's local_infile system
// variable is off. The exact error code/wording differs across MySQL
// versions — confirmed against a real MySQL 8.4 server, which returns
// error 3948 ("Loading local data is disabled; this must be enabled on
// both the client and server sides"), not the older 1148 ("the used
// command is not allowed with this MySQL version") this originally only
// checked for. Any other error is treated as a real failure and propagated
// instead of triggering the fallback, so genuine bugs aren't silently
// masked.
func isLocalInfileDisabledErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "1148") ||
		strings.Contains(msg, "3948") ||
		strings.Contains(msg, "local_infile") ||
		strings.Contains(msg, "local infile") ||
		strings.Contains(msg, "loading local data is disabled") ||
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
		"cnpj":                  cnpjOut.String,
		"razao_social":          razao.String,
		"nome_fantasia":         fantasia.String,
		"situacao_cadastral":    sitCad.Int64,
		"data_inicio_atividade": dtInicio.Int64,
		"uf":                    uf.String,
		"municipio":             munic.Int64,
		"logradouro":            fmt.Sprintf("%s %s, %s %s", tpLog.String, lograd.String, num.String, comp.String),
		"bairro":                bairro.String,
		"cep":                   cep.String,
		"telefone":              fmt.Sprintf("(%s) %s", ddd1.String, tel1.String),
		"email":                 email.String,
		"opcao_simples":         simplesOpt.String,
		"opcao_mei":             meiOpt.String,
	}

	return result, nil
}

// SearchCNPJ uses MySQL FULLTEXT (MATCH ... AGAINST, boolean mode) when the
// index is ready and the query is suitable for it — this is a real index
// scan instead of a full table scan, which is what the plain
// `LOWER(col) LIKE '%termo%'` query below always was (a leading wildcard
// InnoDB can never use a normal index for). Falls back to that original
// LIKE query whenever FULLTEXT isn't available or isn't a safe fit (short
// search terms below MySQL's default 3-character minimum token size would
// silently match nothing via FULLTEXT even though LIKE would still find
// them), so recall never gets worse than before, only faster in the common
// case.
// searchQueryTimeout bounds how long a single search query is allowed to
// run. Verified live: a trailing-wildcard FULLTEXT search for an extremely
// common prefix (e.g. "SILVA", 200k+ matches) against razao_social can take
// MySQL 400s+ to even finish its internal FULLTEXT initialization step —
// and MySQL didn't honor a plain KILL QUERY promptly either. This timeout
// is passed to MySQL as a MAX_EXECUTION_TIME hint (so the server itself
// aborts the statement) and also bounds the Go-side context, so a
// pathological search term degrades the caller's response time instead of
// hanging indefinitely; SearchCNPJ's existing FULLTEXT-then-LIKE fallback
// treats a timeout like any other query error.
const searchQueryTimeout = 8 * time.Second

func (m *MySQLDriver) SearchCNPJ(query string, uf string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	if m.fulltextSearchReady.Load() {
		if boolQuery, ok := buildBooleanFulltextQuery(query); ok {
			results, err := m.searchCNPJFulltext(boolQuery, uf, limit)
			if err == nil {
				return results, nil
			}
			log.Printf("[MySQL] Warning: busca FULLTEXT falhou (%v), usando LIKE como fallback.", err)
		}
	}

	return m.searchCNPJLike(query, uf, limit)
}

// searchCNPJFulltext runs two single-table FULLTEXT lookups UNION'd together
// instead of one query with `MATCH(a) OR MATCH(b)` across the JOIN. That
// simpler form is what a first pass would write, but MySQL's optimizer
// cannot use either FULLTEXT index once the two MATCH clauses (one per side
// of the join) are combined with OR — verified live via EXPLAIN, which showed
// a full table scan (type=ALL) over all 72M+ estabelecimento rows despite
// both indexes existing. Splitting into two subqueries, each with a single
// MATCH against its own table, lets MySQL use each FULLTEXT index
// independently; UNION also dedupes the case where both sides match the same
// establishment.
func (m *MySQLDriver) searchCNPJFulltext(boolQuery string, uf string, limit int) ([]map[string]interface{}, error) {
	ufClause := ""
	args := []interface{}{boolQuery}
	if uf != "" {
		ufClause = " AND e.uf = ?"
	}
	if uf != "" {
		args = append(args, strings.ToUpper(uf))
	}
	args = append(args, boolQuery)
	if uf != "" {
		args = append(args, strings.ToUpper(uf))
	}

	maxExecMS := searchQueryTimeout.Milliseconds()
	sqlStr := fmt.Sprintf(`
	(SELECT /*+ MAX_EXECUTION_TIME(%d) */
		CONCAT(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS cnpj,
		emp.razao_social,
		e.nome_fantasia,
		e.uf,
		e.cnae_fiscal_principal
	FROM estabelecimento e
	LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
	WHERE MATCH(e.nome_fantasia) AGAINST (? IN BOOLEAN MODE)%s
	LIMIT %d)
	UNION
	(SELECT /*+ MAX_EXECUTION_TIME(%d) */
		CONCAT(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS cnpj,
		emp.razao_social,
		e.nome_fantasia,
		e.uf,
		e.cnae_fiscal_principal
	FROM empresa emp
	JOIN estabelecimento e ON e.cnpj_basico = emp.cnpj_basico
	WHERE MATCH(emp.razao_social) AGAINST (? IN BOOLEAN MODE)%s
	LIMIT %d)
	LIMIT %d;`, maxExecMS, ufClause, limit, maxExecMS, ufClause, limit, limit)

	ctx, cancel := context.WithTimeout(context.Background(), searchQueryTimeout)
	defer cancel()

	rows, err := m.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanSearchRows(rows), nil
}

func (m *MySQLDriver) searchCNPJLike(query string, uf string, limit int) ([]map[string]interface{}, error) {
	whereClause := "WHERE (LOWER(emp.razao_social) LIKE ? OR LOWER(e.nome_fantasia) LIKE ?)"
	args := []interface{}{"%" + strings.ToLower(query) + "%", "%" + strings.ToLower(query) + "%"}

	if uf != "" {
		whereClause += " AND e.uf = ?"
		args = append(args, strings.ToUpper(uf))
	}

	sqlStr := fmt.Sprintf(`
	SELECT /*+ MAX_EXECUTION_TIME(%d) */
		CONCAT(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS cnpj,
		emp.razao_social,
		e.nome_fantasia,
		e.uf,
		e.cnae_fiscal_principal
	FROM estabelecimento e
	LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
	%s
	LIMIT %d;`, searchQueryTimeout.Milliseconds(), whereClause, limit)

	ctx, cancel := context.WithTimeout(context.Background(), searchQueryTimeout)
	defer cancel()

	rows, err := m.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanSearchRows(rows), nil
}

func scanSearchRows(rows *sql.Rows) []map[string]interface{} {
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
	return results
}

var fulltextSpecialChars = strings.NewReplacer(
	"+", "", "-", "", "<", "", ">", "", "(", "", ")", "", "~", "", "*", "", `"`, "", "@", "",
)

// buildBooleanFulltextQuery converts a free-text search query into MySQL
// FULLTEXT boolean-mode syntax: every word required (+), prefix-matched (*).
// Returns ok=false when the query isn't a safe fit for FULLTEXT — after
// stripping boolean-mode operator characters (which would otherwise risk a
// syntax error from the FULLTEXT parser), any word shorter than 3 runes
// falls below MySQL's default innodb_ft_min_token_size and wouldn't match
// via FULLTEXT even though the LIKE fallback still would.
func buildBooleanFulltextQuery(query string) (string, bool) {
	words := strings.Fields(query)
	parts := make([]string, 0, len(words))
	for _, w := range words {
		w = fulltextSpecialChars.Replace(w)
		if len([]rune(w)) < 3 {
			return "", false
		}
		parts = append(parts, "+"+w+"*")
	}
	if len(parts) == 0 {
		return "", false
	}
	return strings.Join(parts, " "), true
}

// mysqlTableRowEstimate reads InnoDB's own row-count estimate from
// information_schema instead of running COUNT(*). TABLE_ROWS can be off by
// roughly 10-15% until the next ANALYZE TABLE, but it's a catalog lookup —
// no table scan — which matters because GetStats backs GET /api/v1/status,
// something the dashboard polls every 15s. Verified live against MySQL 8.4
// with 70M+ rows: the exact COUNT(*) this replaced took 30-60s+ per call,
// and concurrent polling piled up queries faster than any of them could
// finish, leaving the endpoint permanently unresponsive. An estimate that
// returns instantly is the right trade-off for a dashboard stat.
func (m *MySQLDriver) mysqlTableRowEstimate(table string) int64 {
	var n int64
	_ = m.db.QueryRow(
		"SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		table,
	).Scan(&n)
	return n
}

func (m *MySQLDriver) GetStats() (map[string]interface{}, error) {
	totalEmpresas := m.mysqlTableRowEstimate("empresa")
	totalEstab := m.mysqlTableRowEstimate("estabelecimento")
	totalSocios := m.mysqlTableRowEstimate("socios")

	latestMonth, _ := m.GetLatestProcessedMonth()

	return map[string]interface{}{
		"total_empresas":         totalEmpresas,
		"total_estabelecimentos": totalEstab,
		"total_socios":           totalSocios,
		"ultima_competencia":     latestMonth,
		"driver":                 "MySQL",
	}, nil
}

func (m *MySQLDriver) GetSearchDocuments(offset, limit int) ([]SearchDocument, bool, error) {
	query := `
	SELECT
		CONCAT(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS cnpj,
		emp.razao_social,
		e.nome_fantasia,
		e.uf,
		e.cnae_fiscal_principal
	FROM estabelecimento e
	LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
	ORDER BY e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv
	LIMIT ? OFFSET ?;`

	rows, err := m.db.Query(query, limit+1, offset)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	return scanSearchDocuments(rows, limit)
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
