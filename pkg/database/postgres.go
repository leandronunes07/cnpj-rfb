package database

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/schema"
	"github.com/lib/pq"
)

type PostgresDriver struct {
	cfg *config.Config
	db  *sql.DB
}

func NewPostgresDriver(cfg *config.Config) *PostgresDriver {
	return &PostgresDriver{cfg: cfg}
}

func (p *PostgresDriver) Connect() error {
	adminDSN := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=postgres sslmode=%s",
		p.cfg.DBHost, p.cfg.DBPort, p.cfg.DBUser, p.cfg.DBPassword, p.cfg.DBSSLMode,
	)

	adminDB, err := sql.Open("postgres", adminDSN)
	if err == nil {
		defer adminDB.Close()
		var exists bool
		err := adminDB.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", p.cfg.DBName).Scan(&exists)
		if err == nil && !exists {
			log.Printf("[PostgreSQL] Criando banco de dados '%s' automaticamente...", p.cfg.DBName)
			if _, err := adminDB.Exec(fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(p.cfg.DBName))); err != nil {
				log.Printf("[PostgreSQL] Warning: falha ao criar banco '%s' automaticamente: %v", p.cfg.DBName, err)
			}
		}
	}

	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		p.cfg.DBHost, p.cfg.DBPort, p.cfg.DBUser, p.cfg.DBPassword, p.cfg.DBName, p.cfg.DBSSLMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed opening postgres db connection: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed connecting to postgres: %w", err)
	}

	p.db = db
	log.Printf("[PostgreSQL] Conectado com sucesso em %s:%d/%s", p.cfg.DBHost, p.cfg.DBPort, p.cfg.DBName)
	return nil
}

func (p *PostgresDriver) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func (p *PostgresDriver) InitSchema() error {
	createMetaTable := `
	CREATE TABLE IF NOT EXISTS etl_metadata (
		id SERIAL PRIMARY KEY,
		data_month VARCHAR(7) NOT NULL,
		processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	CREATE TABLE IF NOT EXISTS etl_processed_files (
		id SERIAL PRIMARY KEY,
		data_month VARCHAR(7) NOT NULL,
		filename VARCHAR(255) NOT NULL,
		status VARCHAR(32) NOT NULL,
		processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	CREATE TABLE IF NOT EXISTS etl_change_log (
		id BIGSERIAL PRIMARY KEY,
		cnpj_basico VARCHAR(8) NOT NULL,
		tabela VARCHAR(32) NOT NULL,
		campo VARCHAR(64) NOT NULL,
		valor_antigo TEXT,
		valor_novo TEXT,
		competencia VARCHAR(7) NOT NULL,
		detectado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_change_log_cnpj ON etl_change_log(cnpj_basico);`
	if _, err := p.db.Exec(createMetaTable); err != nil {
		return fmt.Errorf("failed creating etl_metadata/etl_processed_files tables: %w", err)
	}

	for _, t := range schema.Tables {
		var colDefs []string
		for _, col := range t.Columns {
			pgType := col.Type
			if pgType == "NUMERIC" {
				pgType = "NUMERIC(15,2)"
			}
			colDefs = append(colDefs, fmt.Sprintf("%s %s", col.Name, pgType))
		}

		var primaryKeyClause string
		if t.Name == "empresa" {
			primaryKeyClause = ", PRIMARY KEY (cnpj_basico)"
		} else if t.Name == "estabelecimento" {
			primaryKeyClause = ", PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)"
		} else if t.Name == "simples" {
			primaryKeyClause = ", PRIMARY KEY (cnpj_basico)"
		} else if len(t.Columns) > 0 && t.Columns[0].Name == "codigo" {
			primaryKeyClause = ", PRIMARY KEY (codigo)"
		}

		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s%s);", t.Name, strings.Join(colDefs, ", "), primaryKeyClause)
		if _, err := p.db.Exec(ddl); err != nil {
			return fmt.Errorf("failed creating table %s: %w", t.Name, err)
		}
	}

	createView := `
	CREATE OR REPLACE VIEW vw_cnpj_completo AS
	SELECT 
		e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj,
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
	if _, err := p.db.Exec(createView); err != nil {
		log.Printf("[PostgreSQL] Warning ao criar view vw_cnpj_completo: %v", err)
	}

	log.Println("[PostgreSQL] DDL, Schemas e Views inicializados com sucesso.")
	return nil
}

func (p *PostgresDriver) EnsureIndexes() error {
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_empresa_cnpj ON empresa(cnpj_basico);",
		"CREATE INDEX IF NOT EXISTS idx_empresa_razao ON empresa(razao_social);",
		"CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnpj ON estabelecimento(cnpj_basico);",
		"CREATE INDEX IF NOT EXISTS idx_estabelecimento_uf ON estabelecimento(uf);",
		"CREATE INDEX IF NOT EXISTS idx_estabelecimento_municipio ON estabelecimento(municipio);",
		"CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnae ON estabelecimento(cnae_fiscal_principal);",
		"CREATE INDEX IF NOT EXISTS idx_estabelecimento_situacao ON estabelecimento(situacao_cadastral);",
		"CREATE INDEX IF NOT EXISTS idx_estabelecimento_fantasia ON estabelecimento(nome_fantasia);",
		"CREATE INDEX IF NOT EXISTS idx_socios_cnpj ON socios(cnpj_basico);",
		"CREATE INDEX IF NOT EXISTS idx_simples_cnpj ON simples(cnpj_basico);",
		"CREATE INDEX IF NOT EXISTS idx_etl_files_month_file ON etl_processed_files(data_month, filename);",
	}

	log.Println("[PostgreSQL] Garantindo índices secundários...")
	for _, idx := range indexes {
		if _, err := p.db.Exec(idx); err != nil {
			log.Printf("[PostgreSQL] Warning ao criar índice: %v", err)
		}
	}

	// SearchCNPJ runs LOWER(col) LIKE '%termo%' — a leading wildcard that a
	// plain B-tree index can never use, so without this it's a full table
	// scan over `estabelecimento` on every search. pg_trgm lets the planner
	// use a GIN trigram index for that exact same LIKE query automatically,
	// no query rewrite needed. Requires the pg_trgm contrib extension
	// (bundled with essentially every Postgres install/managed service); if
	// the connection lacks privilege to create it, this is logged and
	// skipped rather than failing the whole pipeline — search just stays
	// on the slower sequential-scan path.
	if _, err := p.db.Exec("CREATE EXTENSION IF NOT EXISTS pg_trgm;"); err != nil {
		log.Printf("[PostgreSQL] Warning: não foi possível habilitar a extensão pg_trgm (%v) — busca por nome continuará sem aceleração por índice.", err)
	} else {
		trigramIndexes := []string{
			"CREATE INDEX IF NOT EXISTS idx_empresa_razao_trgm ON empresa USING GIN (LOWER(razao_social) gin_trgm_ops);",
			"CREATE INDEX IF NOT EXISTS idx_estabelecimento_fantasia_trgm ON estabelecimento USING GIN (LOWER(nome_fantasia) gin_trgm_ops);",
		}
		for _, idx := range trigramIndexes {
			if _, err := p.db.Exec(idx); err != nil {
				log.Printf("[PostgreSQL] Warning ao criar índice trigram: %v", err)
			}
		}
	}

	log.Println("[PostgreSQL] Índices secundários prontos.")
	return nil
}

func (p *PostgresDriver) BatchLimit(numCols int) int {
	return placeholderBatchLimit(numCols, p.cfg.BatchSize, 65000)
}

func (p *PostgresDriver) GetLatestProcessedMonth() (string, error) {
	var month string
	err := p.db.QueryRow("SELECT data_month FROM etl_metadata ORDER BY id DESC LIMIT 1").Scan(&month)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return month, nil
}

func (p *PostgresDriver) SaveProcessedMonth(month string) error {
	_, err := p.db.Exec("INSERT INTO etl_metadata (data_month) VALUES ($1)", month)
	return err
}

func (p *PostgresDriver) IsFileProcessed(dataMonth string, filename string) (bool, error) {
	var count int
	err := p.db.QueryRow("SELECT COUNT(*) FROM etl_processed_files WHERE data_month = $1 AND filename = $2 AND status = 'SUCCESS'", dataMonth, filename).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (p *PostgresDriver) SaveProcessedFile(dataMonth string, filename string, status string) error {
	_, err := p.db.Exec("INSERT INTO etl_processed_files (data_month, filename, status) VALUES ($1, $2, $3)", dataMonth, filename, status)
	return err
}

func (p *PostgresDriver) InsertBatch(table schema.TableSpec, rows [][]string) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed starting postgres transaction: %w", err)
	}
	defer tx.Rollback()

	cols := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		cols[i] = c.Name
	}

	numCols := len(table.Columns)
	valueStrings := make([]string, 0, len(rows))
	valueArgs := make([]interface{}, 0, len(rows)*numCols)

	for rIdx, row := range rows {
		placeholders := make([]string, numCols)
		for cIdx := 0; cIdx < numCols; cIdx++ {
			paramPos := rIdx*numCols + cIdx + 1
			placeholders[cIdx] = fmt.Sprintf("$%d", paramPos)

			var val string
			if cIdx < len(row) {
				val = row[cIdx]
			}
			valueArgs = append(valueArgs, sanitizeValuePostgres(val, table.Columns[cIdx].Type))
		}
		valueStrings = append(valueStrings, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	stmtStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING", table.Name, strings.Join(cols, ", "), strings.Join(valueStrings, ", "))

	if _, err := tx.Exec(stmtStr, valueArgs...); err != nil {
		return fmt.Errorf("bulk insert error in table %s: %w", table.Name, err)
	}

	return tx.Commit()
}

// postgresTrackedTables mirrors mysqlTrackedTables — see that comment in
// mysql.go for why socios is deliberately absent.
var postgresTrackedTables = map[string][]string{
	"empresa":         {"cnpj_basico"},
	"estabelecimento": {"cnpj_basico", "cnpj_ordem", "cnpj_dv"},
	"simples":         {"cnpj_basico"},
}

// postgresNormalizeForCompare mirrors mysqlNormalizeForCompare — same
// parsing as sanitizeValuePostgres, but returns the canonical string form
// NUMERIC(15,2)/INTEGER storage would produce, comparable directly against
// a value scanned back from the database.
func postgresNormalizeForCompare(val string, colType string) string {
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
		return strconv.FormatFloat(num, 'f', 2, 64) // matches NUMERIC(15,2) storage
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

type postgresChange struct {
	cnpjBasico  string
	tabela      string
	campo       string
	valorAntigo string
	valorNovo   string
}

// UpsertBatchTracked mirrors MySQLDriver.UpsertBatchTracked: a genuinely
// new row (primary key not seen before) is inserted exactly as InsertBatch
// would; a row whose primary key already exists is compared column-by-
// column against what's stored, and only UPDATEd — with each changed field
// recorded in etl_change_log — if at least one column actually differs.
// See mysql.go's UpsertBatchTracked for the full design rationale (same
// architecture, only the SQL dialect differs).
func (p *PostgresDriver) UpsertBatchTracked(table schema.TableSpec, rows [][]string, competencia string) error {
	pkCols, tracked := postgresTrackedTables[table.Name]
	if !tracked || len(rows) == 0 {
		return p.InsertBatch(table, rows)
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
			return p.InsertBatch(table, rows)
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

	existing, err := p.postgresFetchExisting(table, pkCols, pkIdx, rows)
	if err != nil {
		return fmt.Errorf("erro ao buscar linhas existentes de %s para comparação: %w", table.Name, err)
	}

	var newRows [][]string
	var changedRows [][]string
	var changes []postgresChange

	for _, row := range rows {
		old, found := existing[keyOf(row)]
		if !found {
			newRows = append(newRows, row)
			continue
		}

		rowChanged := false
		for ci, col := range table.Columns {
			newVal := postgresNormalizeForCompare(rowValue(row, ci), col.Type)
			if newVal != old[ci] {
				rowChanged = true
				changes = append(changes, postgresChange{
					cnpjBasico:  rowValue(row, pkIdx[0]),
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
		if err := p.InsertBatch(table, newRows); err != nil {
			return fmt.Errorf("erro ao inserir linhas novas de %s: %w", table.Name, err)
		}
	}
	if len(changedRows) > 0 {
		// UPDATE and its etl_change_log entries land in one transaction —
		// see mysql.go's mysqlApplyChangesTx for why splitting them is
		// unsafe (verified live, corrupted real data before being caught).
		if err := p.postgresApplyChangesTx(table, pkCols, pkIdx, changedRows, changes, competencia); err != nil {
			return fmt.Errorf("erro ao aplicar alterações de %s: %w", table.Name, err)
		}
	}

	return nil
}

// postgresFetchExisting mirrors mysqlFetchExisting — an indexed lookup by
// primary key, chunked to stay under Postgres's placeholder ceiling.
func (p *PostgresDriver) postgresFetchExisting(table schema.TableSpec, pkCols []string, pkIdx []int, rows [][]string) (map[string][]string, error) {
	result := make(map[string][]string, len(rows))

	cols := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		cols[i] = c.Name
	}

	chunkSize := placeholderBatchLimit(len(pkCols), p.cfg.BatchSize, 65000)

	for start := 0; start < len(rows); start += chunkSize {
		end := start + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[start:end]

		var whereClause string
		args := make([]interface{}, 0, len(chunk)*len(pkCols))
		paramPos := 1
		if len(pkCols) == 1 {
			placeholders := make([]string, len(chunk))
			for i, row := range chunk {
				placeholders[i] = fmt.Sprintf("$%d", paramPos)
				paramPos++
				args = append(args, row[pkIdx[0]])
			}
			whereClause = fmt.Sprintf("%s IN (%s)", pkCols[0], strings.Join(placeholders, ","))
		} else {
			tuples := make([]string, len(chunk))
			for i, row := range chunk {
				placeholders := make([]string, len(pkCols))
				for j, idx := range pkIdx {
					placeholders[j] = fmt.Sprintf("$%d", paramPos)
					paramPos++
					args = append(args, row[idx])
				}
				tuples[i] = "(" + strings.Join(placeholders, ",") + ")"
			}
			whereClause = fmt.Sprintf("(%s) IN (%s)", strings.Join(pkCols, ","), strings.Join(tuples, ","))
		}

		querySQL := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(cols, ","), table.Name, whereClause)

		if err := func() error {
			queryRows, err := p.db.Query(querySQL, args...)
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

// postgresApplyChangesTx mirrors mysqlApplyChangesTx — the UPDATE for every
// changed row and all of its etl_change_log entries in a single
// transaction, so the two can never land inconsistently with each other.
func (p *PostgresDriver) postgresApplyChangesTx(table schema.TableSpec, pkCols []string, pkIdx []int, changedRows [][]string, changes []postgresChange, competencia string) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	setClauses := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		setClauses[i] = fmt.Sprintf("%s = $%d", c.Name, i+1)
	}
	whereClauses := make([]string, len(pkCols))
	for i := range pkCols {
		whereClauses[i] = fmt.Sprintf("%s = $%d", pkCols[i], len(table.Columns)+i+1)
	}
	updateStmtStr := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table.Name, strings.Join(setClauses, ", "), strings.Join(whereClauses, " AND "))

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
			args = append(args, sanitizeValuePostgres(val, c.Type))
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
			base := i * 6
			valueStrings[i] = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", base+1, base+2, base+3, base+4, base+5, base+6)
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
func (p *PostgresDriver) GetChangeHistory(cnpjBasico string) ([]ChangeLogEntry, error) {
	rows, err := p.db.Query(
		"SELECT tabela, campo, valor_antigo, valor_novo, competencia, detectado_em FROM etl_change_log WHERE cnpj_basico = $1 ORDER BY detectado_em DESC, id DESC",
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

func (p *PostgresDriver) GetCNPJ(cleanCNPJ string) (map[string]interface{}, error) {
	if len(cleanCNPJ) < 14 {
		return nil, fmt.Errorf("CNPJ deve ter 14 caracteres")
	}

	cnpjBasico := cleanCNPJ[:8]

	query := `
	SELECT 
		e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj,
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
	WHERE e.cnpj_basico = $1 AND e.cnpj_ordem = $2 AND e.cnpj_dv = $3
	LIMIT 1;`

	cnpjOrdem := cleanCNPJ[8:12]
	cnpjDV := cleanCNPJ[12:14]

	row := p.db.QueryRow(query, cnpjBasico, cnpjOrdem, cnpjDV)

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

func (p *PostgresDriver) SearchCNPJ(query string, uf string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	whereClause := "WHERE (LOWER(emp.razao_social) LIKE $1 OR LOWER(e.nome_fantasia) LIKE $1)"
	args := []interface{}{"%" + strings.ToLower(query) + "%"}

	if uf != "" {
		whereClause += " AND e.uf = $2"
		args = append(args, strings.ToUpper(uf))
	}

	sqlStr := fmt.Sprintf(`
	SELECT 
		e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj,
		emp.razao_social,
		e.nome_fantasia,
		e.uf,
		e.cnae_fiscal_principal
	FROM estabelecimento e
	LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
	%s
	LIMIT %d;`, whereClause, limit)

	rows, err := p.db.Query(sqlStr, args...)
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

// postgresTableRowEstimate reads Postgres's own planner estimate
// (pg_class.reltuples, refreshed by autovacuum/ANALYZE) instead of running
// COUNT(*), which is always a full sequential/index scan in Postgres — there
// is no shortcut for an exact count. See the identical trade-off explained
// on MySQLDriver.mysqlTableRowEstimate: GetStats backs GET /api/v1/status,
// polled every 15s by the dashboard, so an instant estimate is worth more
// here than exactness.
func (p *PostgresDriver) postgresTableRowEstimate(table string) int64 {
	var n int64
	_ = p.db.QueryRow(
		"SELECT reltuples::bigint FROM pg_class WHERE relname = $1",
		table,
	).Scan(&n)
	if n < 0 {
		return 0
	}
	return n
}

func (p *PostgresDriver) GetStats() (map[string]interface{}, error) {
	totalEmpresas := p.postgresTableRowEstimate("empresa")
	totalEstab := p.postgresTableRowEstimate("estabelecimento")
	totalSocios := p.postgresTableRowEstimate("socios")

	latestMonth, _ := p.GetLatestProcessedMonth()

	return map[string]interface{}{
		"total_empresas":         totalEmpresas,
		"total_estabelecimentos": totalEstab,
		"total_socios":           totalSocios,
		"ultima_competencia":     latestMonth,
		"driver":                 "PostgreSQL",
	}, nil
}

func (p *PostgresDriver) GetSearchDocuments(offset, limit int) ([]SearchDocument, bool, error) {
	// Fetches one extra row to detect "is there another page" without a
	// separate COUNT(*) query, which would be a full scan on a table this
	// size for no benefit beyond a boolean.
	query := `
	SELECT
		e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj,
		emp.razao_social,
		e.nome_fantasia,
		e.uf,
		e.cnae_fiscal_principal
	FROM estabelecimento e
	LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
	ORDER BY e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv
	LIMIT $1 OFFSET $2;`

	rows, err := p.db.Query(query, limit+1, offset)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	return scanSearchDocuments(rows, limit)
}

func sanitizeValuePostgres(val string, colType string) interface{} {
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
