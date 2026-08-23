<div align="center">

# ⚡ CNPJ Receita Federal ETL Engine

### Engine em Go de altíssima performance para ETL dos dados públicos de CNPJ e Simples Nacional da Receita Federal do Brasil

Download paralelo → stream de extração → carga paralela no banco → API REST + Dashboard em tempo real.<br/>
Tudo em um único binário compilado, sem runtime, sem dependências externas.

[![CI](https://img.shields.io/github/actions/workflow/status/leandronunes07/cnpj-rfb/ci.yml?branch=main&style=for-the-badge&label=CI)](https://github.com/leandronunes07/cnpj-rfb/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev)
[![License: MIT](https://img.shields.io/github/license/leandronunes07/cnpj-rfb?style=for-the-badge&color=blue)](LICENSE)
[![Docker Ready](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white)](Dockerfile)
[![Last Commit](https://img.shields.io/github/last-commit/leandronunes07/cnpj-rfb?style=for-the-badge&color=orange)](https://github.com/leandronunes07/cnpj-rfb/commits/main)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen?style=for-the-badge)](CONTRIBUTING.md)

[![Go](https://img.shields.io/badge/Go-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![MySQL](https://img.shields.io/badge/MySQL-4479A1?logo=mysql&logoColor=white)](https://www.mysql.com)
[![SQLite](https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white)](https://www.sqlite.org)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?logo=clickhouse&logoColor=black)](https://clickhouse.com)
[![Redis](https://img.shields.io/badge/Redis-optional-DC382D?logo=redis&logoColor=white)](https://redis.io)
[![Meilisearch](https://img.shields.io/badge/Meilisearch-optional-FF5CAA?logo=meilisearch&logoColor=white)](https://www.meilisearch.com)
[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)](https://www.docker.com)

[Instalação rápida](#-instalação-rápida) •
[Por que Go?](#-por-que-go) •
[API](docs/API.md) •
[Arquitetura](docs/ARCHITECTURE.md) •
[Contribuindo](CONTRIBUTING.md)

</div>

---

## 🚀 Visão geral

A Receita Federal publica mensalmente, em formato `.zip`, os dados públicos de **todas as empresas registradas no Brasil** — CNPJ, sócios, situação cadastral, endereço, CNAE, opção pelo Simples Nacional/MEI. É um dos datasets públicos mais pesados do governo brasileiro: dezenas de arquivos, muitos GB compactados, dezenas de milhões de linhas.

Este projeto é uma engine de ETL construída do zero em **Go** especificamente para esse problema: baixar só o que mudou, processar em stream sem estourar RAM, carregar em paralelo com o mínimo overhead possível, e servir os dados prontos via API REST — tudo automatizado, todo dia, sem intervenção manual.

1. Descobre a competência (`YYYY-MM`) mais recente disponível na Receita Federal.
2. Baixa, em paralelo, só os arquivos ainda não processados (idempotente).
3. Descompacta e faz streaming de cada CSV (`ISO-8859-1` → `UTF-8`), sem carregar o arquivo inteiro em memória.
4. Carrega no banco em lotes, com paralelismo real entre arquivos — não é "baixa tudo, depois importa tudo".
5. Expõe os dados via API REST autenticada e um dashboard web com console de logs ao vivo.
6. Repete automaticamente todo dia, baixando só o que for novo.

## ⚡ Por que Go?

Este não é um script de ETL genérico portado para Go — o design inteiro explora o que a linguagem faz de melhor para exatamente este tipo de carga de trabalho:

- **Binário único, sem runtime.** `go build` gera um executável estático — sem instalar interpretador, sem `node_modules`, sem JVM, sem "funciona na minha máquina". O `Dockerfile` deste projeto compila e empacota tudo em uma imagem Alpine mínima.
- **Goroutines, não threads pesadas.** Uma goroutine custa poucos KB de memória contra os MBs de uma thread de SO. É isso que permite baixar **e** importar dezenas de arquivos em paralelo (`DOWNLOAD_WORKERS`) sem explodir o consumo de RAM — em Python isso normalmente esbarra no GIL ou exige `multiprocessing` pesado; em Node, quer dizer lidar com callbacks/promises para tentar imitar paralelismo real de I/O+CPU.
- **Streaming de verdade, do zip ao banco.** Extração de `.zip` e parsing de `.csv` são feitos linha a linha via `io.Reader` — o dataset completo (dezenas de GB descompactados) nunca precisa caber em memória. Processar um CSV de milhões de linhas em Python/Node sem reescrever manualmente cada etapa como stream costuma significar carregar o arquivo inteiro na RAM.
- **Tipagem estática pega erro em tempo de compilação**, não depois de 40 milhões de linhas já inseridas em produção.
- **Startup instantâneo.** Sem cold start de interpretador — importante para um processo que roda em cron, reinicia em containers pequenos, e precisa responder à API imediatamente após o boot.
- **`database/sql` nativo e maduro.** Trocar de banco (Postgres, MySQL, SQLite, ClickHouse) é uma variável de ambiente, não uma reescrita — a mesma interface `DBDriver` funciona para todos.

O resultado prático: uma engine pensada para ser **rápida, enxuta em memória e simples de operar** — um único binário, sem dependências de runtime, capaz de processar um dos maiores datasets públicos do Brasil em uma fração do tempo que uma abordagem ingênua de INSERT sequencial levaria. Detalhes técnicos de cada otimização (paralelismo de importação, índices adiados, `LOAD DATA LOCAL INFILE` no MySQL) estão na seção [Performance](#-performance).

## ✨ Funcionalidades

- **Download + importação paralelos** — cada worker baixa um arquivo e imediatamente o descompacta/importa, sem lock global.
- **Streaming de ponta a ponta** — zip e CSV processados linha a linha, memória sob controle mesmo em datasets enormes.
- **Multi-banco** — PostgreSQL, MySQL (com `LOAD DATA LOCAL INFILE` nativo), SQLite e ClickHouse, trocados por uma variável de ambiente.
- **Índices adiados** — índices secundários só são criados depois da carga completa, evitando manutenção de índice a cada `INSERT`.
- **Idempotente e incremental** — cada arquivo processado com sucesso fica registrado; reexecuções pulam o que já foi feito, sem duplicar dados.
- **Auto-cleanup** — apaga `.zip`/`.csv` imediatamente após confirmar a inserção, essencial em VPS com disco limitado.
- **Dashboard web embutido** — estatísticas ao vivo, console de logs via Server-Sent Events, testador de API — tudo compilado dentro do binário (`go:embed`).
- **API REST autenticada** — consulta de CNPJ, busca por razão social/UF, status/estatísticas, disparo manual.
- **Lock distribuído opcional (Redis)** — se você rodar múltiplas instâncias do app, o guard de "só uma execução do pipeline por vez" passa a ser coordenado via Redis em vez de só em memória.
- **Rate limiting opcional (Redis)** — limite de requisições por cliente na API, configurável, ativado automaticamente quando o Redis está configurado.
- **Busca com Meilisearch opcional** — ranking de relevância e tolerância a erro de digitação na busca por nome, com fallback automático para SQL se não estiver configurado ou indisponível.
- **Pronto para produção** — Docker, Docker Compose, Portainer, Easypanel.

## 📐 Arquitetura

```
crawler → downloader (worker pool) → extractor (stream) → etl.Pipeline → database.DBDriver
                                                                              ↓
                                                              api.Server (REST + SSE) + web dashboard
```

Cada worker de download processa (descompacta + importa) o próprio arquivo que baixou — até `DOWNLOAD_WORKERS` arquivos são importados ao mesmo tempo, cada um em sua própria transação. Diagrama completo, explicação do paralelismo e responsabilidade de cada pacote em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## 📋 Requisitos

- **Go 1.24+** (só se for compilar/rodar localmente sem Docker)
- **Docker + Docker Compose** (recomendado para produção)
- Um banco de dados: PostgreSQL, MySQL, ClickHouse — **ou** nada além do disco local (SQLite)
- Acesso de rede de saída para `arquivos.receitafederal.gov.br`
- Redis e Meilisearch são **opcionais** — nada quebra sem eles, veja [Configuração](#-configuração-env)

## 🏁 Instalação rápida

### Docker Compose (recomendado)

```bash
git clone https://github.com/leandronunes07/cnpj-rfb.git
cd cnpj-rfb
cp .env.example .env
# edite o .env: pelo menos API_TOKEN e DB_PASSWORD são obrigatórios
docker-compose up -d --build
```

O painel fica disponível em `http://localhost:8080` (ou a porta que você definir em `API_PORT`).

Para subir Redis e Meilisearch junto (opcional — veja [Configuração](#-configuração-env)):

```bash
# defina REDIS_ADDR=redis:6379 e MEILISEARCH_HOST=http://meilisearch:7700 no .env antes
docker-compose --profile extras up -d --build
```

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
2. Build type: **Dockerfile** (usa o `Dockerfile` do repositório, gera um binário estático em Alpine).
3. Configure as variáveis de ambiente (seção abaixo) — `API_TOKEN` e `DB_PASSWORD` são obrigatórias, a aplicação recusa iniciar sem elas.
4. Deploy. O container fica de pé, roda a carga inicial no boot e depois só verifica novidades conforme `CRON_SCHEDULE`.

## ⚙️ Configuração (`.env`)

```bash
cp .env.example .env
```

| Variável | Descrição | Padrão |
|---|---|---|
| `DB_DRIVER` | `postgres`, `mysql`, `sqlite`, `clickhouse` (`turso`/`duckdb` = alias de `sqlite`, ver [Limitações](#-roadmap--limitações-conhecidas)) | `postgres` |
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
| `BATCH_SIZE` | Teto de registros por lote. Valor efetivo por tabela depende do driver — ver [Performance](#-performance) | `10000` |
| `AUTO_CLEANUP` | Apaga `.zip`/`.csv` logo após importar (`true`/`false`) | `true` |
| `CRON_SCHEDULE` | Expressão cron para checagem diária | `0 3 * * *` |
| `RUN_ONCE` | Se `true`, roda uma vez e encerra (equivalente a `--once`) | `false` |
| `REDIS_ADDR` | Endereço `host:porta` do Redis. **Opcional** — vazio = lock do pipeline fica local e rate limiting desativado | — |
| `REDIS_PASSWORD` | Senha do Redis, se houver | — |
| `REDIS_DB` | Número do banco lógico do Redis | `0` |
| `RATE_LIMIT_PER_MINUTE` | Requisições por minuto, por cliente (IP), quando `REDIS_ADDR` está configurado | `120` |
| `MEILISEARCH_HOST` | URL do Meilisearch (ex.: `http://localhost:7700`). **Opcional** — vazio = busca por nome fica só no SQL | — |
| `MEILISEARCH_API_KEY` | Chave de API do Meilisearch | — |
| `MEILISEARCH_INDEX` | Nome do índice usado para a busca | `estabelecimentos` |

## 🗄️ Bancos de dados suportados

| Driver | Status | Observação |
|---|---|---|
| `postgres` | ✅ Nativo | `INSERT ... ON CONFLICT DO NOTHING` em lote |
| `mysql` | ✅ Nativo | Usa `LOAD DATA LOCAL INFILE` (bulk-load nativo do MySQL); cai automaticamente para `INSERT IGNORE` em lote se o servidor tiver `local_infile` desabilitado |
| `sqlite` | ✅ Nativo | Arquivo local, WAL mode, ótimo para testar sem infraestrutura |
| `clickhouse` | ✅ Nativo | Tabelas `ReplacingMergeTree` (dedupe em merge/`FINAL`) |
| `turso` / `duckdb` | ⚠️ Alias de `sqlite` | Ainda **não** há cliente nativo Turso/libSQL nem DuckDB — essas opções só existem para não quebrar quem já as configurou; funcionam como um SQLite local comum |

## 📊 Tabelas geradas

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

## 🖥️ Uso

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

## 🔥 Performance

O que faz essa engine ser rápida não é um único truque, é a soma de várias decisões de arquitetura:

**Carga (escrita):**

- **Importação paralela, não sequencial** — até `DOWNLOAD_WORKERS` arquivos são descompactados e carregados no banco ao mesmo tempo, sem lock global entre eles.
- **`LOAD DATA LOCAL INFILE` no MySQL** — usa o mecanismo nativo de bulk-load do MySQL em vez de `INSERT` em lote, com fallback automático e transparente se o servidor não permitir.
- **Índices secundários adiados** — criados só depois da carga completa, não a cada `INSERT`, que é o maior fator de lentidão em uma carga em massa do zero.
- **`ReplacingMergeTree` no ClickHouse** — dedupe em merge, sem overhead de checagem de unicidade por linha.
- Tempo real de uma carga completa depende muito do hardware do banco e da rede até a Receita Federal — não existe número universal.

Se o log do MySQL mostrar `AVISO: LOAD DATA LOCAL INFILE indisponível`, veja [Solução de problemas](#-solução-de-problemas) para destravar o modo mais rápido.

**Busca (leitura):**

Uma busca "contém" (`%termo%`) nunca usa índice B-tree comum — todo banco relacional esbarra nisso. Cada driver compensa à sua forma, e o **Meilisearch (opcional)** substitui todos eles quando configurado — é a única opção com ranking de relevância e tolerância a erro de digitação de verdade. Detalhes e trade-offs de cada um em [`docs/API.md`](docs/API.md#como-a-busca-é-acelerada-varia-por-driver):

- **Meilisearch** (se `MEILISEARCH_HOST` configurado): usado primeiro, sempre — ranking, typo tolerance, muito mais rápido que qualquer aceleração SQL. Reindexação completa (não incremental) roda em background depois de cada carga com dados novos.
- **Postgres**: índice GIN trigram (`pg_trgm`) — acelera sem mudar nenhuma semântica de busca.
- **MySQL**: índice `FULLTEXT` (`MATCH ... AGAINST`) — muda a semântica para busca por palavra/prefixo em vez de substring literal, com fallback automático para o `LIKE` original em termos curtos (< 3 caracteres) ou se a query estourar 8s (ver caixa abaixo).
- **ClickHouse**: índice de skip `ngrambf_v1` — puramente aditivo, zero mudança de semântica.
- **SQLite/turso/duckdb**: sem aceleração nativa (scan completo) — habilitar o Meilisearch é a forma recomendada de acelerar a busca nesses drivers.

Toda resposta de `GET /api/v1/busca` inclui um campo `"source"` (`"meilisearch"` ou `"sql"`) indicando qual motor respondeu.

**Schema MySQL**: colunas curtas de formato fixo (`nome_fantasia`, `logradouro`, `cep`, etc.) usam `VARCHAR(n)` em vez de `TEXT` genérico — menor I/O, indexável de verdade. Isso só vale para instalações novas (`CREATE TABLE IF NOT EXISTS`); um banco MySQL que já está em produção não é migrado automaticamente — veja [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md#aplicar-os-tipos-de-coluna-otimizados-varchar-em-um-banco-mysql-que-já-está-em-produção) se quiser aplicar manualmente.

> ✅ **MySQL** (`LOAD DATA LOCAL INFILE`, fallback pra `INSERT IGNORE`, índice `FULLTEXT`, schema `VARCHAR`) e **Postgres** (`pg_trgm`, índice trigram) foram validados ao vivo contra MySQL 8.4 e PostgreSQL 15 reais — não só revisão de código. Essa validação real encontrou e corrigiu **três bugs de verdade** que a revisão manual não tinha pego: um índice `nome_fantasia(100)` que quebrava contra a coluna `VARCHAR(60)` recém-criada; a detecção de fallback do `LOAD DATA` que não reconhecia a mensagem de erro real do MySQL 8.4 (`Error 3948`, diferente do `1148` mais antigo que era o único caso coberto); e a query de busca do MySQL usando `MATCH(tabela_a) OR MATCH(tabela_b)` entre duas tabelas num `JOIN` — um padrão que parece razoável mas faz o otimizador do MySQL **ignorar os dois índices `FULLTEXT`** e cair num scan completo (confirmado com `EXPLAIN`: `type: ALL` nas 72M+ linhas). **ClickHouse** (`ngrambf_v1`) segue sem validação ao vivo — nenhuma instância disponível para testar. Todas as otimizações têm fallback automático e defensivo — se algo não aplicar, a aplicação loga um aviso e a busca continua funcionando do jeito antigo.
>
> **Importação real, do zero, contra MySQL 8.4** (72,8M estabelecimentos + 69,5M empresas + 49,7M registros do Simples + 28M sócios): **19h04min** completo (carga + os 2 índices `FULLTEXT`), ou **~9h35min** só a carga + índices normais (os `FULLTEXT` sozinhos comeram quase metade do tempo total). Números de busca depois de corrigido o bug do `JOIN`, medidos direto no banco (sem contenção de outras queries concorrentes): termo raro/específico ("TARUGA") **412s → 0,1-0,8s**; termo composto ("AGENCIA TARUGA") **~0,3s**; termos genéricos de uma palavra só e altíssima frequência ("SILVA", "SANTOS", 200k+ ocorrências) esbarram numa característica conhecida do FULLTEXT do MySQL (expansão de prefixo curinga contra um posting list enorme) e estouram o teto de 8s, caindo pro `LIKE` — ainda corretos, só não instantâneos. `KILL QUERY` e `MAX_EXECUTION_TIME` no MySQL não interrompem essa fase ("FULLTEXT initialization") de forma imediata — o app já devolveu resposta ao cliente pelo timeout, mas a query pode continuar consumindo um thread no servidor por mais alguns segundos/minutos até o MySQL efetivamente encerrá-la. Isso é comportamento do MySQL, não algo que dê pra corrigir na aplicação.

## 🩹 Solução de problemas

Guia completo em [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md). Os mais comuns:

- **"API_TOKEN não configurado"** ao iniciar → defina `API_TOKEN` no `.env` (não há valor padrão, de propósito).
- **"DB_PASSWORD não configurado"** → obrigatório para `postgres`/`mysql`/`clickhouse`.
- **`AVISO: LOAD DATA LOCAL INFILE indisponível`** no log do MySQL → habilite `local_infile=1` no servidor MySQL (a aplicação continua funcionando, só mais devagar, via fallback automático).
- **Dashboard "trava" e não atualiza** → geralmente é proxy reverso (nginx/Traefik) bufferizando a conexão SSE; a aplicação já envia os headers corretos (`X-Accel-Buffering: no` + ping periódico), mas confira a configuração do seu proxy se persistir.
- **`[Redis] AVISO: não foi possível conectar`** ou **`[Search] AVISO: não foi possível conectar no Meilisearch`** → ambos opcionais, a aplicação segue funcionando normalmente sem eles (lock local, sem rate limiting, busca só via SQL); confira o endereço/porta configurado.
- **`429 Too Many Requests`** na API → rate limiting ativo (Redis configurado); veja o header `Retry-After` da resposta, ou ajuste `RATE_LIMIT_PER_MINUTE`.

## 🔒 Segurança

- `API_TOKEN` e `DB_PASSWORD` são obrigatórios — a aplicação recusa iniciar sem eles, não existe fallback fraco.
- Todos os endpoints da API (inclusive o stream de logs SSE) exigem autenticação, comparada em tempo constante.
- Nunca exponha a porta da API diretamente na internet sem HTTPS na frente (nginx/Traefik/Caddy) — o `API_TOKEN` viaja em texto puro no header/query string.
- Se encontrar uma vulnerabilidade, reporte de forma responsável — veja [`CONTRIBUTING.md`](CONTRIBUTING.md).

## 🧩 Estrutura do projeto

```
cmd/cnpj-etl/       ponto de entrada (main.go)
pkg/config/         carregamento e validação de variáveis de ambiente
pkg/crawler/        descoberta da competência e listagem de arquivos na Receita Federal
pkg/downloader/     worker pool de download com retry
pkg/extractor/      extração de zip + streaming de CSV (ISO-8859-1 → UTF-8)
pkg/schema/         definição das tabelas/colunas (usada para gerar DDL e mapear arquivo→tabela)
pkg/database/       interface DBDriver + drivers (postgres, mysql, sqlite, clickhouse)
pkg/etl/            orquestração do pipeline completo
pkg/api/            servidor HTTP, autenticação, rate limiting, endpoints REST, broadcaster SSE
pkg/web/            dashboard estático embutido no binário (go:embed)
pkg/scheduler/      agendador cron
pkg/lock/           lock de execução do pipeline (local ou distribuído via Redis)
pkg/search/         cliente Meilisearch para busca por nome (opcional)
```

Detalhes de cada pacote e diagrama de fluxo em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## ✅ Testes

```bash
go build ./...
go vet ./...
go test ./...
```

Os testes cobrem: mapeamento de arquivo→tabela, inserção em lote (inclusive concorrente e idempotência via `INSERT OR IGNORE`), paginação de documentos para busca, a formatação/detecção de fallback do `LOAD DATA` no MySQL, o lock distribuído e o rate limiter (contra um Redis real em memória via [miniredis](https://github.com/alicebob/miniredis)), e o cliente Meilisearch (contra um servidor HTTP fake simulando a API REST real). Não exigem infraestrutura externa — SQLite em arquivo temporário, Redis em memória, Meilisearch mockado.

## 🤝 Contribuindo

Contribuições são bem-vindas! Veja [`CONTRIBUTING.md`](CONTRIBUTING.md) — inclui como rodar o ambiente localmente, convenções de código e o passo a passo para adicionar suporte a um novo banco de dados.

## 🗺️ Roadmap / Limitações conhecidas

Transparência sobre o estado atual do projeto — bom pra quem quiser contribuir:

- `turso` e `duckdb` são hoje apenas um alias do driver SQLite local — implementar clientes nativos de verdade é a contribuição mais valiosa que falta.
- Há duplicação de código entre os 4 drivers de banco (`GetCNPJ`, `SearchCNPJ`, `GetStats` são quase idênticos entre eles) — funcional, mas um ponto de atenção para quem for mexer em uma query.
- O CORS da API está aberto (`Access-Control-Allow-Origin: *`) em todos os endpoints — avaliado como baixo risco dado o esquema de autenticação por token, mas vale revisar se o seu caso de uso exigir mais restrição.
- Sem suíte de testes de integração **automatizada** contra um banco real no CI (Postgres/MySQL/ClickHouse) — os testes automatizados no CI usam SQLite. MySQL e Postgres já foram validados manualmente ao vivo pelo menos uma vez (ver caixa na seção [Performance](#-performance)); ClickHouse (`ngrambf_v1`) ainda não. Automatizar isso no CI (via `services:` do GitHub Actions, por exemplo) é a contribuição de infraestrutura mais valiosa que falta.
- `SQLite`/`turso`/`duckdb` não têm nenhuma aceleração nativa de busca por nome (`GET /api/v1/busca` faz scan completo nesses drivers sem Meilisearch); habilitar o Meilisearch é a forma recomendada de acelerar a busca nesses drivers.
- ClickHouse usa `FINAL` nas leituras de `GetCNPJ`/`SearchCNPJ`/`GetStats`/`GetSearchDocuments` para garantir dedupe correto (ver [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)) — corrige um bug real, mas tem custo de performance na leitura em tabelas muito grandes; não foi otimizado além disso.
- A sincronização com o Meilisearch é uma reindexação completa a cada carga com dados novos, não incremental — simples e correto, mas reprocessa a tabela inteira mesmo que só um arquivo pequeno tenha mudado. Aceitável dado que a Receita Federal só atualiza mensalmente; passaria a valer a pena otimizar se a cadência de atualização mudasse.
- Nunca foi medido um tempo real de ponta a ponta para uma carga completa em produção — as estimativas de performance no README são por ordem de grandeza, não medição.

## 📄 Licença

Licenciado sob a [Licença MIT](LICENSE).

---

<div align="center">

## 👨‍💻 Autor

**Leandro Oliveira Nunes**

[![Email](https://img.shields.io/badge/Email-leandro%40agenciataruga.com-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:leandro@agenciataruga.com)
[![GitHub](https://img.shields.io/badge/GitHub-leandronunes07-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/leandronunes07)
[![Instagram](https://img.shields.io/badge/Instagram-%40leandronunes07-E4405F?style=for-the-badge&logo=instagram&logoColor=white)](https://instagram.com/leandronunes07)
[![Facebook](https://img.shields.io/badge/Facebook-%40leandronunes07-1877F2?style=for-the-badge&logo=facebook&logoColor=white)](https://facebook.com/leandronunes07)
[![TikTok](https://img.shields.io/badge/TikTok-%40leandronunes07-000000?style=for-the-badge&logo=tiktok&logoColor=white)](https://tiktok.com/@leandronunes07)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-leandronunes07-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/leandronunes07)

### Agência Taruga

[![Website](https://img.shields.io/badge/Website-agenciataruga.com-000000?style=for-the-badge&logo=googlechrome&logoColor=white)](https://www.agenciataruga.com)
[![Instagram](https://img.shields.io/badge/Instagram-%40agenciataruga-E4405F?style=for-the-badge&logo=instagram&logoColor=white)](https://instagram.com/agenciataruga)
[![Facebook](https://img.shields.io/badge/Facebook-%40agenciataruga-1877F2?style=for-the-badge&logo=facebook&logoColor=white)](https://facebook.com/agenciataruga)
[![TikTok](https://img.shields.io/badge/TikTok-%40agenciataruga-000000?style=for-the-badge&logo=tiktok&logoColor=white)](https://tiktok.com/@agenciataruga)

<sub>Se este projeto te ajudou, considere deixar uma ⭐ no repositório.</sub>

</div>
