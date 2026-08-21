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
	_ "github.com/go-sql-driver/mysql"
)

type MySQLDriver struct {
	cfg *config.Config
	db  *sql.DB
}

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

func (m *MySQLDriver) InsertBatch(table schema.TableSpec, rows [][]string) error {
	if len(rows) == 0 {
		return nil
	}

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
