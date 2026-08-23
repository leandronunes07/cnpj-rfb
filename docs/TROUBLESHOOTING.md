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

### Aplicar os tipos de coluna otimizados (`VARCHAR`) em um banco MySQL que já está em produção

Desde a versão atual, `InitSchema` cria colunas como `nome_fantasia`, `logradouro`, `bairro`, `cep`, etc. como `VARCHAR(n)` em vez de `TEXT` genérico — mas isso só vale para `CREATE TABLE IF NOT EXISTS`, ou seja, **só afeta uma instalação nova**. Um banco que já existe e já tem dados não é migrado automaticamente (de propósito: um `ALTER TABLE` de conversão de tipo numa tabela com dezenas de milhões de linhas reescreve a tabela inteira, trava escritas durante a operação, e não é algo pra rodar sozinho sem você estar olhando).

Se quiser aplicar manualmente num banco existente, faça um por vez, em horário de baixo tráfego, e confira o espaço em disco disponível (a operação usa espaço extra temporário):

```sql
ALTER TABLE estabelecimento MODIFY nome_fantasia VARCHAR(60);
ALTER TABLE estabelecimento MODIFY logradouro VARCHAR(100);
-- repita para as demais colunas listadas em mysqlColumnTypeOverrides (pkg/database/mysql.go)
```

Isso é opcional — o banco continua funcionando normalmente com as colunas em `TEXT`, só não ganha o benefício de I/O/indexação menor.

---

## Busca por nome (`GET /api/v1/busca`)

### A busca no MySQL não encontra um termo curto (2 caracteres) que eu sei que existe

Esperado: termos com menos de 3 caracteres ficam abaixo do `innodb_ft_min_token_size` padrão do MySQL, então a aplicação detecta isso e usa automaticamente o `LIKE` original (mais lento, mas sem essa limitação) só para esses casos — veja [`docs/API.md`](API.md#como-a-busca-é-acelerada-varia-por-driver). Se mesmo assim não encontrar, o problema não é a busca — confira se a competência com aquele registro já foi carregada.

### `[PostgreSQL] Warning: não foi possível habilitar a extensão pg_trgm`

O usuário configurado em `DB_USER` não tem privilégio para `CREATE EXTENSION`. Em bancos gerenciados (RDS, Supabase, etc.) normalmente isso já vem liberado por padrão para extensões comuns como `pg_trgm`; se não vier, peça para alguém com privilégio de superusuário rodar uma vez:

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

Sem isso, a busca no Postgres continua funcionando normalmente, só sem aceleração por índice (volta a ser um scan completo, igual antes).

### `[MySQL] Warning ao criar índice FULLTEXT ...`

Geralmente aparece se a tabela usa um engine que não suporta `FULLTEXT` (deveria ser sempre `InnoDB` aqui, gerado pelo próprio `InitSchema`) ou se a coluna já tem um índice `FULLTEXT` com configuração incompatível de uma versão anterior do schema. Enquanto esse aviso aparecer, a busca no MySQL usa automaticamente o `LIKE` original — nada quebra, só fica mais lenta.

### Busca no MySQL demora vários segundos (até ~8s) para termos muito comuns

Esperado para um punhado de termos de uma palavra só e altíssima frequência (sobrenomes muito comuns tipo "SILVA"/"SANTOS", com centenas de milhares de ocorrências). Não é bug: o MySQL usa busca por prefixo curinga (`+termo*`) no `FULLTEXT` para permitir match parcial, e para um termo extremamente frequente isso força o MySQL a expandir e unir várias palavras do índice que começam com aquele prefixo antes de aplicar o `LIMIT`. A aplicação tem um teto de 8s (`MAX_EXECUTION_TIME` no MySQL + timeout de contexto no Go) — se estourar, cai automaticamente para o `LIKE`, então a resposta sempre volta correta, só não instantânea nesse caso específico. Termos mais específicos (nomes compostos, razão social completa) não têm esse problema. Ver números reais em [`README.md`](../README.md#-performance).

Se aparecer `[MySQL] Warning: busca FULLTEXT falhou (context deadline exceeded)` no log com frequência para termos que não são desse tipo, vale investigar (`EXPLAIN` na query, `SHOW ENGINE INNODB STATUS`) — pode indicar um problema diferente, como o servidor MySQL sob carga pesada de outra operação (ex. uma importação em andamento).

---

## Redis (lock distribuído / rate limiting)

### `[Redis] AVISO: não foi possível conectar em ...`

`REDIS_ADDR` está configurado mas a aplicação não conseguiu conectar (endereço errado, Redis fora do ar, firewall, etc.). Isso **não impede a aplicação de iniciar** — ela segue funcionando com o lock de execução do pipeline em memória (só coordena dentro da própria instância) e sem rate limiting na API. Confira:

- O host/porta em `REDIS_ADDR` está correto e acessível a partir do container (se estiver em Docker, use o nome do serviço, ex. `redis:6379`, não `localhost`).
- Se `REDIS_PASSWORD` estiver configurado, confira que bate com o `requirepass` do servidor.
- Se estiver usando o `docker-compose --profile extras`, confirme que o serviço `redis` realmente subiu (`docker-compose ps`).

### Rodando múltiplas instâncias e o lock não parece estar coordenando

Confirme que **todas** as instâncias apontam para o mesmo `REDIS_ADDR`/`REDIS_DB` — instâncias apontando para bancos lógicos (`REDIS_DB`) diferentes do mesmo Redis, ou para servidores Redis diferentes, não compartilham o lock entre si (cada uma vê seu próprio namespace).

---

## Meilisearch (busca por nome)

### `[Search] AVISO: não foi possível conectar no Meilisearch em ...`

`MEILISEARCH_HOST` está configurado mas a aplicação não conseguiu falar com ele no boot. A aplicação **continua funcionando** — a busca por nome volta a usar o caminho SQL do driver (ver [`docs/API.md`](API.md#como-a-busca-é-acelerada-varia-por-driver-e-se-o-meilisearch-está-configurado)). Confira:

- A URL em `MEILISEARCH_HOST` inclui o esquema (`http://` ou `https://`) e está acessível a partir do container.
- Se estiver usando o `docker-compose --profile extras`, o serviço se chama `meilisearch` na rede interna do compose — use `http://meilisearch:7700`, não `localhost`.
- `MEILISEARCH_API_KEY` bate com a `MEILI_MASTER_KEY` configurada no servidor Meilisearch (se houver uma).

### O índice do Meilisearch está vazio ou desatualizado

- **Instalação nova com dados que já existiam antes de configurar o Meilisearch**: a aplicação sincroniza uma vez no boot além de depois de cada carga — confira o log por `[Search] Sincronizando índice de busca (Meilisearch)...` logo após o boot. Se não aparecer, o Meilisearch não estava saudável no momento do boot (veja o item anterior); reinicie a aplicação depois de garantir que o Meilisearch está acessível.
- **Dados novos não aparecem na busca**: a sincronização só roda depois de uma execução do pipeline que efetivamente carregou arquivos novos (não em execuções que não encontraram nada pendente) — confira `GET /api/v1/status` → `stats.ultima_competencia` para saber se a carga realmente trouxe dados novos.
- A sincronização é uma **reindexação completa**, não incremental (ver [`docs/ARCHITECTURE.md`](ARCHITECTURE.md#busca-por-nome-e-meilisearch-pkgsearch)) — para uma base grande, pode levar minutos; acompanhe `[Search] Índice de busca sincronizado: N registros em ...` no log para confirmar que terminou.

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
