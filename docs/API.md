# Referência da API REST

Base URL: `http://<host>:<API_PORT>` (padrão `API_PORT=8080`).

Todos os endpoints em `/api/v1/*` exigem autenticação, incluindo o stream de logs (`/api/v1/events`).

## Autenticação

Use **um** dos três métodos abaixo, em ordem de preferência do servidor (o primeiro presente é usado):

1. Header `X-API-Token: <valor de API_TOKEN>`
2. Header `Authorization: Bearer <valor de API_TOKEN>`
3. Query string `?token=<valor de API_TOKEN>` — único método possível para o endpoint SSE (`/api/v1/events`), já que o `EventSource` do navegador não permite enviar headers customizados.

Requisição sem token válido recebe `401 Unauthorized`:

```json
{ "error": "Não autorizado. Token de API inválido ou ausente." }
```

A comparação do token é feita em tempo constante (`crypto/subtle.ConstantTimeCompare`), não vulnerável a timing attack.

### CORS

Todos os endpoints respondem com `Access-Control-Allow-Origin: *` e aceitam `OPTIONS` (preflight). Isso permite consumir a API a partir de qualquer origem no navegador, desde que o chamador tenha o token — avalie se isso é adequado ao seu caso de uso antes de expor a API publicamente.

---

## `GET /api/v1/status`

Retorna o status do serviço e estatísticas agregadas do banco.

```bash
curl -H "X-API-Token: $API_TOKEN" http://localhost:8080/api/v1/status
```

```json
{
  "status": "online",
  "engine": "Go ETL Engine v2.0",
  "stats": {
    "total_empresas": 55234891,
    "total_estabelecimentos": 61042318,
    "total_socios": 24118732,
    "ultima_competencia": "2026-08",
    "driver": "MySQL"
  }
}
```

Este é o endpoint usado pelo dashboard tanto para validar o token no login quanto para atualizar os contadores periodicamente.

---

## `GET /api/v1/cnpj/{cnpj}`

Busca um CNPJ específico (14 caracteres após remover pontuação — aceita tanto o formato numérico tradicional quanto o novo formato alfanumérico da Receita Federal).

```bash
curl -H "X-API-Token: $API_TOKEN" http://localhost:8080/api/v1/cnpj/00000000000191
# ou com máscara — pontuação é removida automaticamente:
curl -H "X-API-Token: $API_TOKEN" http://localhost:8080/api/v1/cnpj/00.000.000/0001-91
```

Resposta (`200 OK`):

```json
{
  "cnpj": "00000000000191",
  "razao_social": "BANCO DO BRASIL SA",
  "nome_fantasia": "BB",
  "situacao_cadastral": 2,
  "data_inicio_atividade": 18080101,
  "uf": "DF",
  "municipio": 9701,
  "logradouro": "SAUN QUADRA 5 LOTE B, TORRE I",
  "bairro": "ASA NORTE",
  "cep": "70040912",
  "telefone": "(61) 34939002",
  "email": "...",
  "opcao_simples": "N",
  "opcao_mei": "N"
}
```

Erros:
- `400 Bad Request` — CNPJ ausente na URL, ou com mais/menos de 14 caracteres alfanuméricos após limpeza.
- `404 Not Found` — CNPJ não encontrado na base (a competência correspondente pode ainda não ter sido carregada).

---

## `GET /api/v1/busca`

Busca por razão social / nome fantasia, com filtro opcional de UF.

| Parâmetro | Obrigatório | Descrição |
|---|---|---|
| `q` | sim | Termo de busca (case-insensitive, `LIKE %termo%` em razão social ou nome fantasia) |
| `uf` | não | Sigla da UF (2 letras) para filtrar |
| `limit` | não | Quantidade de resultados, `1`-`100`. Fora desse intervalo, usa o padrão `20` |

```bash
curl -H "X-API-Token: $API_TOKEN" "http://localhost:8080/api/v1/busca?q=agencia+taruga&uf=MG&limit=10"
```

```json
{
  "query": "agencia taruga",
  "uf": "MG",
  "total_count": 1,
  "results": [
    {
      "cnpj": "00000000000000",
      "razao_social": "AGENCIA TARUGA LTDA",
      "nome_fantasia": "TARUGA",
      "uf": "MG",
      "cnae": 6201500
    }
  ]
}
```

`400 Bad Request` se `q` estiver ausente.

---

## `POST /api/v1/trigger-etl`

Dispara manualmente uma verificação/carga de novos dados, em background — a resposta HTTP não espera o pipeline terminar.

```bash
curl -X POST -H "X-API-Token: $API_TOKEN" http://localhost:8080/api/v1/trigger-etl
```

```json
{ "message": "Rotina de verificação e carga do ETL iniciada em background." }
```

**Importante:** esta resposta é sempre `200 OK` com essa mensagem, mesmo se já houver uma execução em andamento (boot, cron ou outro trigger manual) — nesse caso o pipeline recusa a nova execução internamente e isso só aparece no stream de logs (`/api/v1/events`), não no corpo desta resposta. Acompanhe o console de logs do dashboard (ou este mesmo endpoint SSE) para ver o resultado real.

---

## `GET /api/v1/events`

Server-Sent Events (`text/event-stream`) com o log em tempo real da aplicação inteira (não só do pipeline).

```bash
curl -N -H "Accept: text/event-stream" "http://localhost:8080/api/v1/events?token=$API_TOKEN"
```

Comportamento:
- Ao conectar, o servidor reenvia imediatamente as últimas ~200 linhas de log já emitidas (histórico), então um cliente que conecta depois do pipeline já ter começado não vê um stream vazio.
- Um comentário SSE (`: ping`) é enviado a cada 15s para manter a conexão viva através de proxies reversos com timeout de conexão ociosa.
- Cada linha de log da aplicação chega como um evento `data: <linha>`.

Exemplo de consumo em JavaScript (é exatamente o que o dashboard faz):

```javascript
const sse = new EventSource(`/api/v1/events?token=${encodeURIComponent(apiToken)}`);
sse.onmessage = (event) => console.log(event.data);
sse.onerror = () => console.warn('conexão perdida, EventSource reconecta automaticamente');
```

---

## Formato de erro

Todos os erros seguem o mesmo formato:

```json
{ "error": "descrição legível do problema" }
```

| Código | Quando acontece |
|---|---|
| `400` | Parâmetro obrigatório ausente ou inválido |
| `401` | Token ausente ou inválido |
| `404` | Recurso não encontrado (ex.: CNPJ inexistente na base) |
| `500` | Erro interno (ex.: falha de conexão com o banco) |
