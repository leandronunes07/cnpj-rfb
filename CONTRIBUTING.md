# Contribuindo

Obrigado pelo interesse em contribuir com o **CNPJ Receita Federal ETL Engine**! Este guia cobre como rodar o projeto localmente, convenções de código e o passo a passo para adicionar suporte a um novo banco de dados — o ponto de extensão mais comum.

## Ambiente de desenvolvimento

Requisitos: Go 1.22+.

```bash
git clone https://github.com/leandronunes07/cnpj-rfb.git
cd cnpj-rfb
go mod tidy
cp .env.example .env
```

Para desenvolver sem precisar subir Postgres/MySQL/ClickHouse, use `DB_DRIVER=sqlite` no `.env` — é o caminho mais rápido para iterar, e é o que a suíte de testes automatizados usa.

```bash
go run ./cmd/cnpj-etl --once   # roda o pipeline uma vez, contra o SQLite local
go run ./cmd/cnpj-etl          # roda como serviço (API + dashboard + cron)
```

## Antes de abrir um PR

```bash
go build ./...
go vet ./...
go test ./...
gofmt -l .   # não deve listar os arquivos que você tocou
```

O [CI](.github/workflows/ci.yml) roda exatamente esses mesmos passos (build, vet, gofmt, `go mod tidy`, testes) em todo push e pull request para `main` — rodá-los localmente antes de abrir o PR só antecipa o feedback, não substitui a checagem automática.

## Convenções de código

- **Sem comentários óbvios.** Só comente o que não é dedutível lendo o código (uma decisão não-óbvia, uma limitação de uma API externa, um workaround para um bug específico). Nomes de função/variável bem escolhidos já substituem a maioria dos comentários.
- **Erros tratados, não engolidos silenciosamente.** Se um erro realmente não pode/deve interromper o fluxo, logue por que (`log.Printf("... Warning: %v", err)`), não use `_ = algumaChamada()` sem explicação.
- **Sem abstração prematura.** Se três drivers de banco fazem a mesma coisa de forma ligeiramente diferente, isso é aceitável hoje — não crie uma camada de abstração genérica "pra não repetir" a menos que já exista um caso concreto pedindo por ela.
- **Testes não exigem infraestrutura externa.** Novos testes devem rodar com `go test ./...` sem precisar de Docker ou de um banco de verdade — use o driver SQLite (arquivo temporário) como já é feito em `pkg/database/*_test.go`.

## Adicionando um novo driver de banco de dados

O ponto de extensão mais provável para quem for contribuir é adicionar suporte a um novo banco (implementações reais de DuckDB e Turso/libSQL, por exemplo, seriam bem-vindas — hoje ambos são só um alias do SQLite, veja [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)).

1. Crie `pkg/database/<seubanco>.go` implementando a interface `DBDriver` (definida em `pkg/database/driver.go`):
   ```go
   type DBDriver interface {
       Connect() error
       Close() error
       InitSchema() error       // cria tabelas + a view vw_cnpj_completo, SEM índices secundários
       EnsureIndexes() error    // cria índices secundários — chamado só depois da carga completa
       BatchLimit(numCols int) int
       GetLatestProcessedMonth() (string, error)
       SaveProcessedMonth(month string) error
       IsFileProcessed(dataMonth string, filename string) (bool, error)
       SaveProcessedFile(dataMonth string, filename string, status string) error
       InsertBatch(table schema.TableSpec, rows [][]string) error
       GetCNPJ(cnpj string) (map[string]interface{}, error)
       SearchCNPJ(query string, uf string, limit int) ([]map[string]interface{}, error)
       GetStats() (map[string]interface{}, error)
   }
   ```
2. Use `pkg/database/postgres.go` ou `pkg/database/sqlite.go` como referência — a estrutura é praticamente a mesma entre os drivers existentes (é a duplicação mencionada nas [Limitações conhecidas](README.md#limitações-conhecidas) do README).
3. Pense no `BatchLimit`: se seu driver faz `INSERT` multi-linha com placeholders (`?`/`$1`), limite pelo teto de placeholders da conexão dividido pelo número de colunas (veja `placeholderBatchLimit` em `driver.go`). Se seu driver tem um mecanismo de bulk-load nativo sem esse limite (como o `LOAD DATA` do MySQL), pode usar `BatchSize` diretamente.
4. `InsertBatch` deve ser **idempotente** — use o equivalente do seu banco a `ON CONFLICT DO NOTHING`/`INSERT IGNORE`, já que o pipeline pode reprocessar um arquivo em caso de falha parcial.
5. Registre o novo driver em `NewDBDriver` (`pkg/database/driver.go`) e na validação de `DB_DRIVER` em `pkg/config/config.go`.
6. Adicione testes em `pkg/database/<seubanco>_test.go` — se o driver puder rodar embarcado/local (como SQLite), teste de verdade; se exigir um servidor externo, pelo menos cubra a lógica pura (formatação de valores, cálculo de batch limit, detecção de erros) sem precisar de conexão real, como é feito em `pkg/database/mysql_load_data_test.go`.
7. Atualize a tabela de "Bancos de dados suportados" no [README](README.md#bancos-de-dados-suportados).

## Reportando bugs e vulnerabilidades

- **Bugs**: abra uma issue com passos para reproduzir, driver de banco usado, e o trecho relevante do log.
- **Vulnerabilidades de segurança**: não abra uma issue pública — envie um e-mail para [leandro@agenciataruga.com](mailto:leandro@agenciataruga.com) descrevendo o problema antes de divulgar publicamente.

## Assinatura

Contribuições e este guia são mantidos por:

**Leandro Oliveira Nunes** — [leandro@agenciataruga.com](mailto:leandro@agenciataruga.com)
[@leandronunes07](https://instagram.com/leandronunes07) no Instagram, Facebook, TikTok e LinkedIn

**Agência Taruga** — [www.agenciataruga.com](https://www.agenciataruga.com)
[@agenciataruga](https://instagram.com/agenciataruga) no Instagram, Facebook, TikTok e LinkedIn
