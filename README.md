# CNPJ Receita Federal ETL Engine em Go 🚀

Engine em **Go (Golang)** para baixar, extrair, transformar e carregar — de forma incremental e paralela — os dados públicos de **CNPJ e Simples Nacional** disponibilizados pela Receita Federal do Brasil em um banco de dados relacional, com **API REST** e **dashboard web** prontos para consulta.

Baixa apenas o que ainda não foi processado, transforma os CSVs (ISO-8859-1 → UTF-8) em stream sem carregar o dataset inteiro em memória, e escreve no banco em paralelo.

---

## Índice

- [Visão geral](#visão-geral)
- [Funcionalidades](#funcionalidades)
- [Arquitetura](#arquitetura)
- [Requisitos](#requisitos)
- [Instalação rápida](#instalação-rápida)
- [Configuração (`.env`)](#configuração-env)
- [Bancos de dados suportados](#bancos-de-dados-suportados)
- [Tabelas geradas](#tabelas-geradas)
- [Uso](#uso)
- [Performance](#performance)
- [Solução de problemas](#solução-de-problemas)
- [Segurança](#segurança)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Testes](#testes)
- [Contribuindo](#contribuindo)
- [Limitações conhecidas](#limitações-conhecidas)
- [Licença](#licença)
- [Autor](#autor)

---

## Visão geral

A Receita Federal publica mensalmente, em `https://arquivos.receitafederal.gov.br/...`, um conjunto de arquivos `.zip` com os dados públicos de todas as empresas registradas no Brasil (CNPJ, sócios, situação cadastral, endereço, opção pelo Simples/MEI, etc). São dezenas de arquivos grandes (o mês inteiro soma vários GB compactados).

Este projeto automatiza o ciclo completo:

1. Descobre a competência (`YYYY-MM`) mais recente disponível.
2. Lista e baixa os arquivos `.zip` que ainda não foram importados (idempotente — reprocessar não duplica dados).
3. Descompacta e faz streaming de cada CSV, convertendo `ISO-8859-1` → `UTF-8`.
4. Carrega os dados no banco de dados escolhido, em lotes, com paralelismo real entre arquivos.
5. Expõe os dados via API REST e um dashboard web com console de logs em tempo real.
6. Repete automaticamente todo dia (cron configurável), baixando só o que for novo.

## Funcionalidades

- **Download + importação paralelos**: cada worker baixa um arquivo e imediatamente o descompacta/importa, sem lock global — várias tabelas carregam ao mesmo tempo.
- **Streaming, não carrega tudo em memória**: extração de `.zip` e parsing de `.csv` são feitos linha a linha.
- **Multi-banco**: PostgreSQL, MySQL (com `LOAD DATA LOCAL INFILE` nativo), SQLite e ClickHouse — troca via uma variável de ambiente.
- **Índices adiados**: os índices secundários só são criados depois da carga completa, evitando manutenção de índice a cada `INSERT` durante o backfill inicial.
- **Idempotente e incremental**: cada arquivo processado com sucesso fica registrado no próprio banco; reexecuções pulam o que já foi feito.
- **Auto-cleanup**: apaga `.zip`/`.csv` imediatamente após confirmar a inserção, importante em VPS com pouco disco.
- **Dashboard web embutido**: estatísticas ao vivo, console de logs via Server-Sent Events, testador de API/busca de CNPJ — tudo compilado dentro do binário (`go:embed`), sem dependências externas.
- **API REST autenticada**: consulta de CNPJ, busca por razão social/UF, status/estatísticas, disparo manual da rotina.
- **Pronto para Docker / Portainer / Easypanel**: binário estático, container único.

## Arquitetura

Fluxo resumido (detalhes completos, com diagrama, em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)):

```
crawler → downloader (worker pool) → extractor (stream) → etl.Pipeline → database.DBDriver
                                                                              ↓
                                                              api.Server (REST + SSE) + web dashboard
```

Cada worker de download processa (descompacta + importa) o próprio arquivo que baixou — até `DOWNLOAD_WORKERS` arquivos são importados ao mesmo tempo, cada um em sua própria transação.

## Requisitos

- **Go 1.22+** (só se for compilar/rodar localmente sem Docker)
- **Docker + Docker Compose** (recomendado para produção)
- Um banco de dados: PostgreSQL, MySQL, ClickHouse, **ou** nada além do disco local (SQLite)
- Acesso de rede de saída para `arquivos.receitafederal.gov.br`

## Instalação rápida

### Docker Compose (recomendado)

```bash
git clone https://github.com/<seu-usuario>/cnpj-rbf.git
cd cnpj-rbf
cp .env.example .env
# edite o .env: pelo menos API_TOKEN e DB_PASSWORD são obrigatórios
docker-compose up -d --build
```

O painel fica disponível em `http://localhost:8080` (ou a porta que você definir em `API_PORT`).

### Execução local via Go

```bash
go mod tidy
cp .env.example .env
# edite o .env

go run ./cmd/cnpj-etl            # roda como serviço (API + cron)
go run ./cmd/cnpj-etl --once     # roda o pipeline uma vez e encerra (ideal para cron externo)
```

### Easypanel / Portainer

1. Crie uma aplicação a partir deste repositório Git.
2. Build type: **Dockerfile** (usa o `Dockerfile` do repositório, gera um binário estático em `alpine`).
3. Configure as variáveis de ambiente (seção abaixo) — `API_TOKEN` e `DB_PASSWORD` são obrigatórias, a aplicação recusa iniciar sem elas.
4. Deploy. O container fica de pé, roda a carga inicial no boot e depois só verifica novidades conforme `CRON_SCHEDULE`.

## Configuração (`.env`)

```bash
cp .env.example .env
```

| Variável | Descrição | Padrão |
|---|---|---|
| `DB_DRIVER` | `postgres`, `mysql`, `sqlite`, `clickhouse` (`turso`/`duckdb` = alias de `sqlite`, ver [Limitações](#limitações-conhecidas)) | `postgres` |
| `DB_HOST` | Host do banco | `localhost` |
| `DB_PORT` | Porta do banco | `5432` / `3306` / `9000` |
| `DB_USER` | Usuário do banco | `postgres` / `root` |
| `DB_PASSWORD` | Senha do banco. **Obrigatória** para `postgres`, `mysql` e `clickhouse` | — |
| `DB_NAME` | Nome do banco/schema | `cnpj` |
| `DB_SSLMODE` | Modo SSL (Postgres) | `disable` |
| `DB_FILE` | Caminho do arquivo, só para `sqlite`/`turso`/`duckdb` | `./data/cnpj.sqlite` |
| `API_PORT` | Porta do servidor HTTP | `8080` |
| `API_TOKEN` | Token de autenticação da API/dashboard. **Obrigatório** — gere com `openssl rand -hex 32` | — |
| `OUTPUT_FILES_PATH` | Diretório temporário dos `.zip` baixados | `./data/zip` |
| `EXTRACTED_FILES_PATH` | Diretório temporário dos `.csv` extraídos | `./data/extracted` |
| `DATA_BASE_URL` | URL base do compartilhamento da Receita Federal | ver `.env.example` |
| `DATA_MONTH` | Competência específica (`YYYY-MM`). Vazio = descobre a mais recente automaticamente | — |
| `DOWNLOAD_WORKERS` | Goroutines paralelas para download **e** importação | `4` |
| `BATCH_SIZE` | Teto de registros por lote. Valor efetivo por tabela depende do driver — ver [Performance](#performance) | `10000` |
| `AUTO_CLEANUP` | Apaga `.zip`/`.csv` logo após importar (`true`/`false`) | `true` |
| `CRON_SCHEDULE` | Expressão cron para checagem diária | `0 3 * * *` |
| `RUN_ONCE` | Se `true`, roda uma vez e encerra (equivalente a `--once`) | `false` |

## Bancos de dados suportados

| Driver | Status | Observação |
|---|---|---|
| `postgres` | ✅ Nativo | `INSERT ... ON CONFLICT DO NOTHING` em lote |
| `mysql` | ✅ Nativo | Usa `LOAD DATA LOCAL INFILE` (bulk-load nativo do MySQL); cai automaticamente para `INSERT IGNORE` em lote se o servidor tiver `local_infile` desabilitado |
| `sqlite` | ✅ Nativo | Arquivo local, WAL mode, ótimo para testar sem infraestrutura |
| `clickhouse` | ✅ Nativo | Tabelas `ReplacingMergeTree` (dedupe em merge/`FINAL`) |
| `turso` / `duckdb` | ⚠️ Alias de `sqlite` | Ainda **não** há cliente nativo Turso/libSQL nem DuckDB — essas opções só existem para não quebrar quem já as configurou; funcionam como um SQLite local comum. Ver [Limitações](#limitações-conhecidas) |

## Tabelas geradas

| Tabela | Conteúdo |
|---|---|
| `empresa` | Razão social, porte, capital social, natureza jurídica |
| `estabelecimento` | CNPJ completo, nome fantasia, situação cadastral, endereço, contatos |
| `socios` | Sócios, qualificação, CPF/CNPJ do sócio, faixa etária |
| `simples` | Opção pelo Simples Nacional e MEI, datas de entrada/exclusão |
| `cnae`, `motivo_situacao_cadastral`, `municipio`, `natureza_juridica`, `pais`, `qualificacao_socio` | Tabelas de domínio (código → descrição) |
| `etl_metadata` | Controle da última competência processada |
| `etl_processed_files` | Controle individual de cada arquivo já importado (garante idempotência) |
| `vw_cnpj_completo` | View com join pronto de estabelecimento + empresa + simples + domínios |

## Uso

### Dashboard web

Acesse `http://<host>:<API_PORT>`, informe o `API_TOKEN`. O painel mostra:
- Estatísticas ao vivo (total de empresas/estabelecimentos/sócios, atualiza sozinho a cada 15s).
- Console de logs em tempo real via SSE (com histórico das últimas mensagens, não só o que acontece depois de você abrir a página).
- Busca de CNPJ e busca por razão social/UF.
- Botão para disparar uma verificação manual da Receita Federal.

### API REST

Referência completa com todos os endpoints e exemplos de `curl` em [`docs/API.md`](docs/API.md). Resumo:

```bash
# Autenticação: header X-API-Token, Authorization: Bearer <token>, ou ?token=
curl -H "X-API-Token: $API_TOKEN" http://localhost:8080/api/v1/status
curl -H "X-API-Token: $API_TOKEN" http://localhost:8080/api/v1/cnpj/00000000000191
curl -H "X-API-Token: $API_TOKEN" "http://localhost:8080/api/v1/busca?q=agencia&uf=MG"
curl -X POST -H "X-API-Token: $API_TOKEN" http://localhost:8080/api/v1/trigger-etl
```

### CLI

```bash
go run ./cmd/cnpj-etl           # serviço: API + cron diário
go run ./cmd/cnpj-etl --once    # roda o pipeline uma vez e encerra
```

## Performance

- A carga inicial completa (todos os arquivos do zero) é a etapa mais pesada — dezenas de milhões de linhas em `estabelecimento`/`empresa`/`socios`/`simples`. O tempo real depende muito do hardware do banco e da velocidade da rede até a Receita Federal; não há um número universal.
- **MySQL usa `LOAD DATA LOCAL INFILE`** por padrão (muito mais rápido que `INSERT` em lote). Se o log mostrar `AVISO: LOAD DATA LOCAL INFILE indisponível`, veja [Solução de problemas](#solução-de-problemas) para habilitar `local_infile` no servidor.
- Índices secundários são criados **depois** da carga completa, não durante — não tente acelerar desabilitando isso, já está otimizado dessa forma.
- `BATCH_SIZE` controla o teto de linhas por lote; para drivers que usam `INSERT` multi-linha (Postgres/SQLite, e o fallback do MySQL) o valor efetivo por tabela é `min(BATCH_SIZE, limite_de_placeholders / nº colunas)` — tabelas largas como `estabelecimento` (~30 colunas) ficam abaixo do teto configurado mesmo se você aumentá-lo bastante.
- `DOWNLOAD_WORKERS` controla tanto o paralelismo de download quanto de importação (cada worker processa o arquivo que baixou). Aumentar demais pode esbarrar em limites do servidor da Receita Federal ou do seu banco de conexões.

## Solução de problemas

Guia completo em [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md). Os mais comuns:

- **"API_TOKEN não configurado"** ao iniciar → defina `API_TOKEN` no `.env` (não há valor padrão, de propósito).
- **"DB_PASSWORD não configurado"** → obrigatório para `postgres`/`mysql`/`clickhouse`.
- **`AVISO: LOAD DATA LOCAL INFILE indisponível`** no log do MySQL → habilite `local_infile=1` no servidor MySQL (a aplicação continua funcionando, só mais devagar, via fallback automático).
- **Dashboard "trava" e não atualiza** → geralmente é proxy reverso (nginx/Traefik) bufferizando a conexão SSE; a aplicação já envia os headers corretos (`X-Accel-Buffering: no` + ping periódico), mas confira a configuração do seu proxy se persistir.

## Segurança

- `API_TOKEN` e `DB_PASSWORD` são obrigatórios — a aplicação recusa iniciar sem eles, não existe fallback fraco.
- Todos os endpoints da API (inclusive o stream de logs SSE) exigem autenticação.
- Nunca exponha a porta da API diretamente na internet sem HTTPS na frente (nginx/Traefik/Caddy) — o `API_TOKEN` viaja em texto puro no header/query string.
- Se encontrar uma vulnerabilidade, reporte de forma responsável — veja [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Estrutura do projeto

```
cmd/cnpj-etl/       ponto de entrada (main.go)
pkg/config/         carregamento e validação de variáveis de ambiente
pkg/crawler/        descoberta da competência e listagem de arquivos na Receita Federal
pkg/downloader/     worker pool de download com retry
pkg/extractor/      extração de zip + streaming de CSV (ISO-8859-1 → UTF-8)
pkg/schema/         definição das tabelas/colunas (usada para gerar DDL e mapear arquivo→tabela)
pkg/database/       interface DBDriver + drivers (postgres, mysql, sqlite, clickhouse)
pkg/etl/            orquestração do pipeline completo
pkg/api/            servidor HTTP, autenticação, endpoints REST, broadcaster SSE
pkg/web/            dashboard estático embutido no binário (go:embed)
pkg/scheduler/      agendador cron
```

Detalhes de cada pacote e diagrama de fluxo em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Testes

```bash
go build ./...
go vet ./...
go test ./...
```

Os testes cobrem: mapeamento de arquivo→tabela, inserção em lote (inclusive concorrente e idempotência via `INSERT OR IGNORE`), e a formatação/detecção de fallback do `LOAD DATA` no MySQL. Não exigem banco externo (usam SQLite em arquivo temporário).

## Contribuindo

Veja [`CONTRIBUTING.md`](CONTRIBUTING.md) — inclui como rodar o ambiente localmente, convenções de código e como adicionar suporte a um novo banco de dados.

## Limitações conhecidas

Sendo transparente sobre o estado atual do projeto:

- `turso` e `duckdb` são hoje apenas um alias do driver SQLite local — não há cliente nativo Turso/libSQL nem DuckDB implementado.
- Há duplicação de código relevante entre os 4 drivers de banco (`GetCNPJ`, `SearchCNPJ`, `GetStats` são quase idênticos entre eles) — funcional, mas um ponto de atenção para quem for mexer em uma query e esquecer de replicar nos outros 3 arquivos.
- O CORS da API está aberto (`Access-Control-Allow-Origin: *`) em todos os endpoints — avaliado como baixo risco dado o esquema de autenticação por token, mas vale revisar se o seu caso de uso exigir mais restrição.
- Sem suíte de testes de integração contra um banco real (Postgres/MySQL/ClickHouse) — os testes automatizados usam SQLite.

Contribuições bem-vindas em qualquer um desses pontos.

## Licença

Licenciado sob a [Licença MIT](LICENSE).

## Autor

**Leandro Oliveira Nunes** — [leandro@agenciataruga.com](mailto:leandro@agenciataruga.com)
[@leandronunes07](https://instagram.com/leandronunes07) no Instagram, Facebook, TikTok e LinkedIn

**Agência Taruga** — [www.agenciataruga.com](https://www.agenciataruga.com)
[@agenciataruga](https://instagram.com/agenciataruga) no Instagram, Facebook, TikTok e LinkedIn
