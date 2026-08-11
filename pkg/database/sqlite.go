package database

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/leandronunes07/cnpj-rfb/pkg/config"
	"github.com/leandronunes07/cnpj-rfb/pkg/schema"
	_ "modernc.org/sqlite"
)

type SQLiteDriver struct {
	cfg *config.Config
	db  *sql.DB
}

func NewSQLiteDriver(cfg *config.Config) *SQLiteDriver {
	return &SQLiteDriver{cfg: cfg}
}

func (s *SQLiteDriver) Connect() error {
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=temp_store(MEMORY)", s.cfg.DBFile)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("failed opening sqlite database at %s: %w", s.cfg.DBFile, err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed connecting to sqlite db: %w", err)
	}

	s.db = db
	log.Printf("[SQLite] Conectado com sucesso em %s (WAL Mode ativado)", s.cfg.DBFile)
	return nil
}

func (s *SQLiteDriver) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SQLiteDriver) InitSchema() error {
	createMetaTable := `
	CREATE TABLE IF NOT EXISTS etl_metadata (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		data_month TEXT NOT NULL,
		processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	CREATE TABLE IF NOT EXISTS etl_processed_files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		data_month TEXT NOT NULL,
		filename TEXT NOT NULL,
		status TEXT NOT NULL,
		processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
	if _, err := s.db.Exec(createMetaTable); err != nil {
		return fmt.Errorf("failed creating etl_metadata/etl_processed_files tables: %w", err)
	}

	for _, t := range schema.Tables {
		var colDefs []string
		for _, col := range t.Columns {
			sqType := "TEXT"
			if col.Type == "INTEGER" {
				sqType = "INTEGER"
			} else if col.Type == "NUMERIC" {
				sqType = "REAL"
			}
			colDefs = append(colDefs, fmt.Sprintf("%s %s", col.Name, sqType))
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
		if _, err := s.db.Exec(ddl); err != nil {
			return fmt.Errorf("failed creating table %s: %w", t.Name, err)
		}
	}

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

	for _, idx := range indexes {
		if _, err := s.db.Exec(idx); err != nil {
			log.Printf("[SQLite] Warning ao criar índice: %v", err)
		}
	}

	createView := `
	CREATE VIEW IF NOT EXISTS vw_cnpj_completo AS
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
	if _, err := s.db.Exec(createView); err != nil {
		log.Printf("[SQLite] Warning ao criar view vw_cnpj_completo: %v", err)
	}

	log.Println("[SQLite] DDL, Schemas e Views inicializados com sucesso.")
	return nil
}

func (s *SQLiteDriver) GetLatestProcessedMonth() (string, error) {
	var month string
	err := s.db.QueryRow("SELECT data_month FROM etl_metadata ORDER BY id DESC LIMIT 1").Scan(&month)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return month, nil
}

func (s *SQLiteDriver) SaveProcessedMonth(month string) error {
	_, err := s.db.Exec("INSERT INTO etl_metadata (data_month) VALUES (?)", month)
	return err
}

func (s *SQLiteDriver) IsFileProcessed(dataMonth string, filename string) (bool, error) {
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM etl_processed_files WHERE data_month = ? AND filename = ? AND status = 'SUCCESS'", dataMonth, filename).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *SQLiteDriver) SaveProcessedFile(dataMonth string, filename string, status string) error {
	_, err := s.db.Exec("INSERT INTO etl_processed_files (data_month, filename, status) VALUES (?, ?, ?)", dataMonth, filename, status)
	return err
}

func (s *SQLiteDriver) InsertBatch(table schema.TableSpec, rows [][]string) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed starting sqlite transaction: %w", err)
	}
	defer tx.Rollback()

	cols := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		cols[i] = c.Name
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
			valueArgs = append(valueArgs, sanitizeValueSQLite(val, table.Columns[cIdx].Type))
		}
		valueStrings = append(valueStrings, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	stmtStr := fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES %s", table.Name, strings.Join(cols, ", "), strings.Join(valueStrings, ", "))

	if _, err := tx.Exec(stmtStr, valueArgs...); err != nil {
		return fmt.Errorf("bulk insert error in table %s: %w", table.Name, err)
	}

	return tx.Commit()
}

func (s *SQLiteDriver) GetCNPJ(cleanCNPJ string) (map[string]interface{}, error) {
	if len(cleanCNPJ) < 14 {
		return nil, fmt.Errorf("CNPJ deve ter 14 caracteres")
	}

	cnpjBasico := cleanCNPJ[:8]
	cnpjOrdem := cleanCNPJ[8:12]
	cnpjDV := cleanCNPJ[12:14]

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
	WHERE e.cnpj_basico = ? AND e.cnpj_ordem = ? AND e.cnpj_dv = ?
	LIMIT 1;`

	row := s.db.QueryRow(query, cnpjBasico, cnpjOrdem, cnpjDV)

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

func (s *SQLiteDriver) SearchCNPJ(query string, uf string, limit int) ([]map[string]interface{}, error) {
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
		e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj,
		emp.razao_social,
		e.nome_fantasia,
		e.uf,
		e.cnae_fiscal_principal
	FROM estabelecimento e
	LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
	%s
	LIMIT %d;`, whereClause, limit)

	rows, err := s.db.Query(sqlStr, args...)
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

func (s *SQLiteDriver) GetStats() (map[string]interface{}, error) {
	var totalEmpresas, totalEstab, totalSocios int64

	_ = s.db.QueryRow("SELECT COUNT(*) FROM empresa").Scan(&totalEmpresas)
	_ = s.db.QueryRow("SELECT COUNT(*) FROM estabelecimento").Scan(&totalEstab)
	_ = s.db.QueryRow("SELECT COUNT(*) FROM socios").Scan(&totalSocios)

	latestMonth, _ := s.GetLatestProcessedMonth()

	return map[string]interface{}{
		"total_empresas":        totalEmpresas,
		"total_estabelecimentos": totalEstab,
		"total_socios":          totalSocios,
		"ultima_competencia":    latestMonth,
		"driver":               "SQLite / Turso",
	}, nil
}

func sanitizeValueSQLite(val string, colType string) interface{} {
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
