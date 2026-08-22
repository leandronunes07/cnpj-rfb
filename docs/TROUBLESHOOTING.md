# Solução de problemas

## A aplicação não inicia

### `API_TOKEN não configurado: defina uma variável de ambiente API_TOKEN...`

Não existe valor padrão de propósito (versões antigas tinham um token fraco hardcoded — isso foi removido). Gere um valor forte e defina no `.env` ou nas variáveis de ambiente do container:

```bash
openssl rand -hex 32
```

### `DB_PASSWORD não configurado: obrigatório para o driver "..."`

Obrigatório para `postgres`, `mysql` e `clickhouse` (não se aplica a `sqlite`/`turso`/`duckdb`, que usam um arquivo local). Defina `DB_PASSWORD` no `.env`.

### `unsupported DB_DRIVER: ...`

Valores aceitos: `postgres`, `mysql`, `sqlite`, `turso`, `duckdb`, `clickhouse`. Note que `turso` e `duckdb` hoje são apenas um alias de `sqlite` — veja a nota em [Limitações conhecidas](../README.md#limitações-conhecidas) no README.

---

## MySQL

### `[MySQL] AVISO: LOAD DATA LOCAL INFILE indisponível no servidor (...)`

O driver tenta usar `LOAD DATA LOCAL INFILE` para carga em massa (muito mais rápido que `INSERT` em lote). Se o servidor MySQL tiver o parâmetro `local_infile` desligado (é o padrão de fábrica em muitas instalações e imagens Docker por questão de segurança), a aplicação detecta o erro automaticamente e cai para o modo `INSERT IGNORE` em lote pelo restante da execução — **nada quebra**, só fica mais lento.

Para habilitar a via rápida:

```sql
-- Requer privilégio SUPER / SYSTEM_VARIABLES_ADMIN. Efeito imediato, não persiste após restart do MySQL.
SET GLOBAL local_infile = 1;
```

Ou de forma persistente, no `my.cnf`/`my.ini` do servidor:

```ini
[mysqld]
local_infile=1
```

Se estiver rodando MySQL em um container à parte, adicione `--local-infile=1` ao comando de start do `mysqld`.

Isso é uma configuração do **servidor** MySQL, não da aplicação — não há nada a mudar no `.env` para isso.

### A carga MySQL está mais lenta do que eu esperava mesmo com `local_infile` ligado

- Confira se os índices secundários realmente foram criados só no fim (procure `[MySQL] Garantindo índices secundários` no log, deve aparecer só depois de "Processamento concluído", não no início).
- `BATCH_SIZE` controla o tamanho do lote enviado por `LOAD DATA`; valores muito baixos (ex.: os `5000` do padrão antigo) geram mais chamadas do que o necessário. O padrão atual é `10000` — considere subir se tiver RAM sobrando.
- Verifique `innodb_buffer_pool_size` do seu servidor MySQL — para tabelas de dezenas de milhões de linhas, um buffer pool pequeno faz o InnoDB bater em disco o tempo todo.

---

## Dashboard / monitoramento

### O console de logs fica travado / não atualiza em tempo real

A aplicação já cuida de três causas comuns:
- Envia `X-Accel-Buffering: no` (evita que o nginx bufferize a resposta SSE).
- Envia um `ping` a cada 15s para manter a conexão viva através de proxies com timeout de conexão ociosa.
- Reenvia o histórico recente de logs assim que você conecta, então abrir o dashboard depois do pipeline já ter começado não resulta em uma tela vazia.

Se mesmo assim persistir atrás do seu proxy reverso específico:
- **nginx**: confirme que não há `proxy_buffering on;` sobrepondo o header enviado pela aplicação, e considere `proxy_read_timeout` alto na location correspondente.
- **Cloudflare / CDN na frente do container**: alguns provedores bufferizam SSE por padrão em planos gratuitos — pode ser necessário desativar proxy (modo "DNS only") para essa rota, ou usar um plano com suporte a streaming.
- **Traefik**: normalmente não bufferiza por padrão, mas confira se não há middleware de compressão (`gzip`) na rota do SSE — compressão quebra o streaming incremental.

### As estatísticas (total de empresas etc.) não mudam

O dashboard faz polling de `/api/v1/status` a cada 15 segundos automaticamente depois do login. Se os números realmente não mudarem por minutos seguidos durante uma carga, confira o console de logs — provavelmente o pipeline está processando um arquivo grande (`estabelecimento`, por exemplo, tem dezenas de milhões de linhas) e ainda não terminou o `INSERT`/`LOAD DATA` daquele lote.

---

## Pipeline / dados

### `CNPJ não encontrado` para um CNPJ que deveria existir

A tabela `estabelecimento` só terá aquele registro depois que o arquivo correspondente (`Estabelecimentos*.zip`) da competência atual for processado com sucesso. Confira `GET /api/v1/status` → `stats.ultima_competencia`, e o console de logs para erros de importação naquele arquivo específico.

### Reprocessar um mês inteiro do zero

Os arquivos já importados ficam registrados na tabela `etl_processed_files` (com `status = 'SUCCESS'`). Para forçar uma nova importação completa de uma competência:

```sql
DELETE FROM etl_processed_files WHERE data_month = '2026-08';
DELETE FROM etl_metadata WHERE data_month = '2026-08';
```

Isso não apaga os dados já carregados nas tabelas de domínio (`empresa`, `estabelecimento` etc.) — como os `INSERT`s usam `ON CONFLICT DO NOTHING`/`INSERT IGNORE`/`INSERT OR IGNORE`, reprocessar é seguro e não duplica linhas, só é desnecessário na maioria dos casos (o objetivo do controle de arquivos processados é justamente evitar isso).

### `[ETL Pipeline] Execução já em andamento, ignorando novo disparo concorrente.`

Não é um erro — é o guard de execução única funcionando (boot, cron e trigger manual da API não podem rodar o pipeline ao mesmo tempo). Espere a execução atual terminar.

---

## Ainda com problema?

Abra uma issue no repositório descrevendo: driver de banco usado, trecho relevante do log, e o que você esperava que acontecesse.
