package schema

type Column struct {
	Name string
	Type string // generic standard types: TEXT, INTEGER, NUMERIC
}

type TableSpec struct {
	Name     string
	Prefixes []string
	Columns  []Column
}

var Tables = []TableSpec{
	{
		Name:     "empresa",
		Prefixes: []string{"EMPRE"},
		Columns: []Column{
			{Name: "cnpj_basico", Type: "TEXT"},
			{Name: "razao_social", Type: "TEXT"},
			{Name: "natureza_juridica", Type: "INTEGER"},
			{Name: "qualificacao_responsavel", Type: "INTEGER"},
			{Name: "capital_social", Type: "NUMERIC"},
			{Name: "porte_empresa", Type: "INTEGER"},
			{Name: "ente_federativo_responsavel", Type: "TEXT"},
		},
	},
	{
		Name:     "estabelecimento",
		Prefixes: []string{"ESTABELE"},
		Columns: []Column{
			{Name: "cnpj_basico", Type: "TEXT"},
			{Name: "cnpj_ordem", Type: "TEXT"},
			{Name: "cnpj_dv", Type: "TEXT"},
			{Name: "identificador_matriz_filial", Type: "INTEGER"},
			{Name: "nome_fantasia", Type: "TEXT"},
			{Name: "situacao_cadastral", Type: "INTEGER"},
			{Name: "data_situacao_cadastral", Type: "INTEGER"},
			{Name: "motivo_situacao_cadastral", Type: "INTEGER"},
			{Name: "nome_cidade_exterior", Type: "TEXT"},
			{Name: "pais", Type: "TEXT"},
			{Name: "data_inicio_atividade", Type: "INTEGER"},
			{Name: "cnae_fiscal_principal", Type: "INTEGER"},
			{Name: "cnae_fiscal_secundaria", Type: "TEXT"},
			{Name: "tipo_logradouro", Type: "TEXT"},
			{Name: "logradouro", Type: "TEXT"},
			{Name: "numero", Type: "TEXT"},
			{Name: "complemento", Type: "TEXT"},
			{Name: "bairro", Type: "TEXT"},
			{Name: "cep", Type: "TEXT"},
			{Name: "uf", Type: "TEXT"},
			{Name: "municipio", Type: "INTEGER"},
			{Name: "ddd_1", Type: "TEXT"},
			{Name: "telefone_1", Type: "TEXT"},
			{Name: "ddd_2", Type: "TEXT"},
			{Name: "telefone_2", Type: "TEXT"},
			{Name: "ddd_fax", Type: "TEXT"},
			{Name: "fax", Type: "TEXT"},
			{Name: "correio_eletronico", Type: "TEXT"},
			{Name: "situacao_especial", Type: "TEXT"},
			{Name: "data_situacao_especial", Type: "INTEGER"},
		},
	},
	{
		Name:     "socios",
		Prefixes: []string{"SOCIO"},
		Columns: []Column{
			{Name: "cnpj_basico", Type: "TEXT"},
			{Name: "identificador_socio", Type: "INTEGER"},
			{Name: "nome_socio_razao_social", Type: "TEXT"},
			{Name: "cpf_cnpj_socio", Type: "TEXT"},
			{Name: "qualificacao_socio", Type: "INTEGER"},
			{Name: "data_entrada_sociedade", Type: "INTEGER"},
			{Name: "pais", Type: "INTEGER"},
			{Name: "representante_legal", Type: "TEXT"},
			{Name: "nome_do_representante", Type: "TEXT"},
			{Name: "qualificacao_representante_legal", Type: "INTEGER"},
			{Name: "faixa_etaria", Type: "INTEGER"},
		},
	},
	{
		Name:     "simples",
		Prefixes: []string{"SIMPLES"},
		Columns: []Column{
			{Name: "cnpj_basico", Type: "TEXT"},
			{Name: "opcao_pelo_simples", Type: "TEXT"},
			{Name: "data_opcao_simples", Type: "INTEGER"},
			{Name: "data_exclusao_simples", Type: "INTEGER"},
			{Name: "opcao_mei", Type: "TEXT"},
			{Name: "data_opcao_mei", Type: "INTEGER"},
			{Name: "data_exclusao_mei", Type: "INTEGER"},
		},
	},
	{
		Name:     "cnae",
		Prefixes: []string{"CNAE"},
		Columns: []Column{
			{Name: "codigo", Type: "INTEGER"},
			{Name: "descricao", Type: "TEXT"},
		},
	},
	{
		Name:     "moti",
		Prefixes: []string{"MOTI"},
		Columns: []Column{
			{Name: "codigo", Type: "INTEGER"},
			{Name: "descricao", Type: "TEXT"},
		},
	},
	{
		Name:     "munic",
		Prefixes: []string{"MUNIC"},
		Columns: []Column{
			{Name: "codigo", Type: "INTEGER"},
			{Name: "descricao", Type: "TEXT"},
		},
	},
	{
		Name:     "natju",
		Prefixes: []string{"NATJU"},
		Columns: []Column{
			{Name: "codigo", Type: "INTEGER"},
			{Name: "descricao", Type: "TEXT"},
		},
	},
	{
		Name:     "pais",
		Prefixes: []string{"PAIS"},
		Columns: []Column{
			{Name: "codigo", Type: "INTEGER"},
			{Name: "descricao", Type: "TEXT"},
		},
	},
	{
		Name:     "quals",
		Prefixes: []string{"QUALS"},
		Columns: []Column{
			{Name: "codigo", Type: "INTEGER"},
			{Name: "descricao", Type: "TEXT"},
		},
	},
}
