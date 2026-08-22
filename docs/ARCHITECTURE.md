# Arquitetura

Este documento descreve como as peças do projeto se encaixam. Para configuração e uso, veja o [README](../README.md).

## Visão geral do pipeline

```mermaid
flowchart TD
    A["pkg/crawler<br/>descobre a competência (YYYY-MM)<br/>e lista os .zip disponíveis"] --> B["pkg/downloader<br/>worker pool (DOWNLOAD_WORKERS)<br/>baixa + retry + skip se já atualizado"]
    B -->|"cada worker processa<br/>o arquivo que baixou"| C["pkg/extractor<br/>descompacta (zip-slip safe)<br/>+ stream CSV ISO-8859-1 → UTF-8"]
    C --> D["pkg/etl.Pipeline<br/>casa arquivo → tabela pelo prefixo<br/>do nome (schema.Tables)"]
    D --> E["pkg/database.DBDriver<br/>InsertBatch em lote,<br/>uma transação por lote"]
    E --> F["EnsureIndexes()<br/>roda uma vez, no fim da carga"]
    F --> J["SyncSearchIndex()<br/>se MEILISEARCH_HOST configurado"]

    G["pkg/scheduler<br/>cron diário"] -.dispara.-> D
    H["pkg/api<br/>POST /trigger-etl"] -.dispara.-> D
    I["boot (main.go)"] -.dispara.-> D
```

Boot, cron e trigger manual da API podem, em teoria, disparar `Pipeline.Run()` ao mesmo tempo — `pkg/lock` garante que só uma execução roda por vez; as demais são recusadas com um log explicativo em vez de rodar em paralelo e disputar os mesmos arquivos. Ver [Lock de execução do pipeline](#lock-de-execução-do-pipeline-pkglock) abaixo.

## Paralelismo dentro de uma execução

Diferente de uma primeira leitura ingênua do código, o download **não** termina antes da importação começar. `Downloader.DownloadStream` mantém um pool de `DOWNLOAD_WORKERS` goroutines lendo de um canal de tarefas; assim que um worker termina de baixar seu arquivo, ele mesmo o descompacta e importa, sem lock global entre workers:

```mermaid
sequenceDiagram
    participant W1 as Worker 1
    participant W2 as Worker 2
    participant DB as Banco de Dados

    W1->>W1: baixa Empresas0.zip
    W2->>W2: baixa Empresas1.zip
    W1->>W1: extrai + importa Empresas0.zip
    par Import concorrente
        W1->>DB: InsertBatch (tabela empresa)
    and
        W2->>W2: extrai + importa Empresas1.zip
        W2->>DB: InsertBatch (tabela empresa)
    end
```

Isso é seguro porque:
- `*sql.DB` é seguro para uso concorrente por múltiplas goroutines (é um pool gerenciado pelo driver).
- Cada `InsertBatch` abre sua própria transação — não há estado mutável compartilhado entre chamadas concorrentes.
- Cada arquivo é extraído para uma subpasta própria (`ExtractedDir/<nome-do-arquivo>/`), evitando colisão de nomes entre arquivos processados ao mesmo tempo.
- Bancos com uma única conexão (SQLite, via `db.SetMaxOpenConns(1)`) naturalmente serializam as transações através do próprio pool — sem precisar de nenhum lock explícito no código.

## Pacotes

| Pacote | Responsabilidade |
|---|---|
| `cmd/cnpj-etl` | Ponto de entrada: parseia flags (`--once`), carrega config, conecta no banco, inicializa schema, sobe o servidor HTTP e o scheduler. |
| `pkg/config` | Lê e valida variáveis de ambiente (`.env`); falha rápido se `API_TOKEN`/`DB_PASSWORD` obrigatórios estiverem ausentes. |
| `pkg/crawler` | Fala WebDAV com o compartilhamento Nextcloud da Receita Federal (`PROPFIND`, com fallback para `GET`); descobre a competência mais recente e lista os `.zip` de um mês. |
| `pkg/downloader` | Worker pool de download HTTP com retry exponencial simples e skip por `Content-Length` (não baixa de novo se o arquivo local já bate com o remoto). |
| `pkg/extractor` | Extrai `.zip` (com proteção contra zip-slip) e faz streaming de CSV, decodificando `ISO-8859-1` → `UTF-8` linha a linha via `csv.Reader`. |
| `pkg/schema` | Define as tabelas/colunas do domínio (`schema.Tables`) — usado tanto para gerar o DDL de cada driver quanto para casar um arquivo extraído com sua tabela de destino pelo prefixo do nome. |
| `pkg/database` | Interface `DBDriver` + implementações concretas (Postgres, MySQL, SQLite, ClickHouse). Cada driver decide seu próprio `BatchLimit` e como criar índices via `EnsureIndexes`. |
| `pkg/etl` | Orquestra crawler → downloader → extractor → database em `Pipeline.Run()`; dono do lock de execução única. |
| `pkg/api` | Servidor HTTP: middleware de autenticação, handlers REST, broadcaster de Server-Sent Events para o console de logs do dashboard. |
| `pkg/web` | Dashboard estático (HTML/CSS/JS) embutido no binário via `go:embed` — não depende de arquivos externos em produção. |
| `pkg/scheduler` | Encapsula `robfig/cron` para disparar `Pipeline.Run()` na expressão configurada em `CRON_SCHEDULE`. |
| `pkg/lock` | Lock de execução única do pipeline — local (`atomic.Bool`) por padrão, ou distribuído via Redis quando `REDIS_ADDR` está configurado. |
| `pkg/search` | Cliente Meilisearch para a busca por nome — opcional, usado quando `MEILISEARCH_HOST` está configurado. |

## Lock de execução do pipeline (`pkg/lock`)

`Pipeline.Run()` nunca pode rodar duas vezes ao mesmo tempo — duas execuções concorrentes disputariam os mesmos arquivos e poderiam corromper o controle de idempotência. A interface `lock.PipelineLock` tem duas implementações:

- **`lock.Local`** (padrão, sem Redis): um `atomic.Bool` em processo. Resolve o caso de uma única instância da aplicação — boot, cron e trigger manual da API competem pelo mesmo lock em memória.
- **`lock.Redis`** (quando `REDIS_ADDR` está configurado): uma chave Redis com `SET NX EX` (compare-and-set atômico) e TTL. Necessário assim que a aplicação roda como múltiplas instâncias/réplicas — um `atomic.Bool` de uma instância é invisível pras outras.

O `lock.Redis` tem dois detalhes de correção que valem a pena entender:

1. **Heartbeat**: o TTL da chave é curto (5 minutos) mas é renovado (`EXPIRE`) a cada TTL/3 enquanto o lock estiver em uso, por uma goroutine em background. Isso resolve a tensão entre "TTL curto o bastante pra uma instância travada não bloquear as outras pra sempre" e "carga real pode levar horas, não pode perder o lock no meio".
2. **Release seguro**: ao liberar, um script Lua confere atomicamente se a chave ainda pertence ao token dessa instância antes de apagar — sem isso, uma instância A que demorou mais que o TTL (e já perdeu o lock pra uma instância B) poderia, ao tentar liberar seu lock "achando" que ainda é dono, apagar o lock que B legitimamente adquiriu depois.

Ambos os cenários (perda de lock por TTL, tentativa de liberar um lock que já não é seu) têm testes reais em `pkg/lock/lock_test.go`, rodando contra um Redis de verdade em memória ([miniredis](https://github.com/alicebob/miniredis)) — não é só revisão de código.

## Busca por nome e Meilisearch (`pkg/search`)

Quando `MEILISEARCH_HOST` está configurado, `GET /api/v1/busca` usa o Meilisearch em vez da aceleração SQL do driver (ver [`docs/API.md`](API.md#como-a-busca-é-acelerada-varia-por-driver-e-se-o-meilisearch-está-configurado)). Isso exige manter o índice do Meilisearch sincronizado com o banco relacional, que é a fonte de verdade — o Meilisearch é só uma cópia denormalizada otimizada pra busca.

```mermaid
flowchart LR
    A["Pipeline.Run() termina<br/>uma carga com dados novos"] --> B["Pipeline.SyncSearchIndex()"]
    C["boot (main.go)"] -.se MEILISEARCH_HOST<br/>configurado.-> B
    B --> D["db.GetSearchDocuments(offset, limit)<br/>paginado, join estabelecimento+empresa"]
    D --> E["search.Client.IndexDocuments()<br/>upsert em lote no Meilisearch"]
```

Pontos de design:

- **Reindexação completa, não incremental.** Cada sincronização relê a tabela `estabelecimento` inteira (paginada via `GetSearchDocuments`) e reenvia tudo para o Meilisearch. Mais simples e mais fácil de raciocinar sobre corretude do que rastrear exatamente quais `cnpj_basico` mudaram — um custo aceitável dado que a Receita Federal só publica dados novos uma vez por mês.
- **Dois gatilhos, não só um.** `SyncSearchIndex` roda depois de qualquer `Pipeline.Run()` que efetivamente carregou arquivos novos, **e** uma vez no boot da aplicação (em background, fora do fluxo do `Run()`). O segundo gatilho existe para o caso de alguém habilitar o Meilisearch numa instalação que já tem dados carregados — sem ele, o índice ficaria vazio até a próxima competência ser publicada pela Receita Federal.
- **Assíncrono e best-effort.** `IndexDocuments` retorna assim que o Meilisearch confirma o enfileiramento da tarefa, não quando ela termina de aplicar — apropriado para um índice de busca mantido eventualmente consistente, diferente do banco relacional principal.
- **Fallback automático na leitura.** Se a consulta ao Meilisearch falhar (indisponível, erro de rede, etc.), `APIHandler.search` cai para `db.SearchCNPJ` na mesma requisição — o cliente da API nunca vê esse detalhe, só um `source: "sql"` na resposta em vez de `"meilisearch"`.

O cliente (`pkg/search/search.go`) tem testes reais em `pkg/search/search_test.go` contra um `httptest.Server` que reproduz os endpoints REST exatos do Meilisearch (path, método HTTP, código de status — conferidos na própria fonte do `meilisearch-go`), não contra uma instância real — mas exercitando a construção de requisição e parsing de resposta de verdade, não só leitura de código.

## A interface `DBDriver`

Todo banco suportado implementa a mesma interface (`pkg/database/driver.go`):

```go
type DBDriver interface {
	Connect() error
	Close() error
	InitSchema() error
	EnsureIndexes() error
	BatchLimit(numCols int) int
	GetLatestProcessedMonth() (string, error)
	SaveProcessedMonth(month string) error
	IsFileProcessed(dataMonth string, filename string) (bool, error)
	SaveProcessedFile(dataMonth string, filename string, status string) error
	InsertBatch(table schema.TableSpec, rows [][]string) error
	GetCNPJ(cnpj string) (map[string]interface{}, error)
	SearchCNPJ(query string, uf string, limit int) ([]map[string]interface{}, error)
	GetStats() (map[string]interface{}, error)
	GetSearchDocuments(offset, limit int) (docs []SearchDocument, hasMore bool, err error)
}
```

Três pontos que não são óbvios de fora:

- **`InitSchema` cria tabelas, mas não os índices secundários.** Índices são responsabilidade de `EnsureIndexes`, chamado pelo pipeline só depois que a carga termina — criar índice antes de carregar dados faz o banco manter esse índice atualizado a cada `INSERT`, o que é o maior fator de lentidão em uma carga em massa do zero.
- **`BatchLimit` não é um número fixo.** Drivers que fazem `INSERT` multi-linha com um placeholder por célula (Postgres, SQLite, e o fallback do MySQL) precisam limitar o lote para não estourar o teto de placeholders da conexão. O MySQL, enquanto `LOAD DATA LOCAL INFILE` estiver disponível, não tem essa restrição (é texto streamado, não usa `?`), então usa `BatchSize` diretamente. Ver a seção de Performance no [README](../README.md#performance) para a fórmula completa.
- **`GetSearchDocuments` existe só para exportar dados, não para servir a API.** É usado exclusivamente por `Pipeline.SyncSearchIndex` para paginar `estabelecimento` join `empresa` inteiro e alimentar o Meilisearch — `SearchCNPJ` continua sendo o caminho usado pela API quando não há Meilisearch configurado. `hasMore` é calculado buscando `limit+1` linhas e conferindo se a linha extra existiu, evitando um `COUNT(*)` separado (caro em tabelas de dezenas de milhões de linhas) só para saber se existe mais uma página.

## Adicionando um novo driver de banco

Veja [`CONTRIBUTING.md`](../CONTRIBUTING.md#adicionando-um-novo-driver-de-banco).
