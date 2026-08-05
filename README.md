# CNPJ Receita Federal ETL Engine em Go 🚀

Engine de alta performance desenvolvida em **Go (Golang)** para download, descompactação em *stream*, transformação e carga incremental de dados públicos de **CNPJ e Simples Nacional** disponibilizados pela Receita Federal do Brasil.

---

## 👨‍💻 Desenvolvido por
- **Autor**: Leandro Oliveira Nunes - [leandro@agenciataruga.com](mailto:leandro@agenciataruga.com)
- **Agência**: Agência Taruga - [www.agenciataruga.com](https://www.agenciataruga.com)

---

## 🔥 Por que escolher esta versão em Go?

- **Alta Performance**: Utiliza Goroutines e Worker Pools para realizar downloads e importações paralelas.
- **Baixíssimo Consumo de Memória (Stream Processing)**: Lê e extrai arquivos `.zip` e faz o *parsing* de arquivos `.csv` sem carregar o dataset inteiro na RAM.
- **Suporte Multi-Banco (MySQL & PostgreSQL)**: Alternância transparente entre MySQL e PostgreSQL via configuração `.env`.
- **Rotina Mensal Automática**: Verifica automaticamente no servidor da Receita Federal a presença de uma nova competência mensal (`YYYY-MM`) e realiza a carga apenas quando há atualização.
- **Auto-Cleanup (Exclusão Automática)**: Deleta arquivos `.zip` e `.csv` imediatamente após a confirmação da inserção no banco de dados, poupando espaço valioso de disco em servidores VPS.
- **Pronto para Easypanel / Docker**: Auto-instalável com binário compilado estaticamente via Docker/Docker-Compose.

---

## 📐 Tabelas Suportadas

O pipeline gera e gerencia automaticamente as tabelas:

1. `empresa` (Razão Social, Porte, Capital Social, Natureza Jurídica)
2. `estabelecimento` (CNPJ Completo, Nome Fantasia, Situação Cadastral, Endereço, UF, Município, Contatos)
3. `socios` (Socios, Qualificação, CPF/CNPJ Sócio, Faixa Etária)
4. `simples` (Opção pelo Simples Nacional e MEI, Datas de Entrada/Exclusão)
5. `cnae` (Tabela de Domínio de CNAEs)
6. `moti` (Motivos de Situação Cadastral)
7. `munic` (Municípios)
8. `natju` (Naturezas Jurídicas)
9. `pais` (Países)
10. `quals` (Qualificações de Sócios)
11. `etl_metadata` (Tabela de controle da última competência processada)

---

## 🛠️ Configuração (`.env`)

Crie o arquivo `.env` baseado no `.env.example`:

```bash
cp .env.example .env
```

| Variável | Descrição | Padrão |
|---|---|---|
| `DB_DRIVER` | Driver de banco de dados (`postgres` ou `mysql`) | `postgres` |
| `DB_HOST` | Host do banco de dados | `localhost` |
| `DB_PORT` | Porta do banco de dados | `5432` / `3306` |
| `DB_USER` | Usuário do banco de dados | `postgres` / `root` |
| `DB_PASSWORD` | Senha do banco de dados | `` |
| `DB_NAME` | Nome do banco de dados | `cnpj` |
| `DOWNLOAD_WORKERS` | Quantidade de threads/goroutines para download | `4` |
| `BATCH_SIZE` | Registros por transação em lote | `5000` |
| `AUTO_CLEANUP` | Apagar arquivos baixados/extraídos pós-importação (`true`/`false`) | `true` |
| `CRON_SCHEDULE` | Expressão Cron para verificação mensal | `0 3 1 * *` |
| `RUN_ONCE` | Se `true`, executa o pipeline uma única vez e encerra | `false` |

---

## 🚀 Como Executar

### 1. Via Docker / Easypanel (Recomendado)

#### Deploy no Easypanel
1. Crie uma nova aplicação no **Easypanel** a partir de um repositório Git (`GitHub`).
2. Conecte a este repositório.
3. Configure o tipo de Build como **Dockerfile** ou **Docker Compose**.
4. Adicione as variáveis de ambiente (`DB_DRIVER`, `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, etc.).
5. Clique em **Deploy**. O container irá rodar continuamente e verificar mensalmente se existem dados novos na Receita Federal.

#### Via Docker Compose Local
```bash
docker-compose up -d --build
```

### 2. Execução Local via Go CLI

```bash
# Baixar dependências
go mod tidy

# Compilar e rodar
go run cmd/cnpj-etl/main.go

# Para rodar apenas uma vez e encerrar (modo CLI / Cron externo):
go run cmd/cnpj-etl/main.go --once
```

---

## 📄 Licença
Licenciado sob a Licença MIT. Desenvolvido com foco em alta performance pela **Agência Taruga**.
