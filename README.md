# tyi-take_yout_investiments

Baseline congelada da v2 do portal de investimentos, com frontend React/Vite e backend Flask exposto como API.

## Como rodar o backend (local)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
python backend/run.py
```

A API fica em `http://127.0.0.1:8000/api`.

## Frontend (React + Vite)

O frontend em `frontend/` consome a API Flask em `/api`.

```bash
# terminal 1 (backend)
source .venv/bin/activate
python backend/run.py

# terminal 2 (frontend)
cd frontend
npm install
npm run dev
```

Frontend: `http://127.0.0.1:5173`

## Docker

### Subir com Docker Compose (recomendado para testes locais)

```bash
# opcional: sobrescrever credenciais do Basic Auth
export BASIC_AUTH_USER=amor
export BASIC_AUTH_PASS='250109'
export ALPHA_VANTAGE_API_KEY='sua-chave-aqui'
export BRAPI_TOKEN='seu-token-aqui'
export COINGECKO_API_KEY='sua-chave-aqui'
export TWELVE_DATA_API_KEY='sua-chave-aqui'
export MARKET_DATA_PROVIDERS='twelve_data,alpha_vantage,coingecko,brapi,yahoo'

docker compose up -d --build
```

Acesse em `http://127.0.0.1:5173`.

O `docker-compose.yml` agora cria a rede `invest-net` automaticamente.

Para parar:

```bash
docker compose down
```

### Build das imagens

```bash
# backend
docker build -t invest-portal-backend ./backend

# frontend
docker build -t invest-portal-frontend ./frontend
```

### Subir containers

```bash
docker network create invest-net

# backend (sem expor porta externa, usado via proxy do frontend)
docker run -d --name backend --network invest-net -p 8000:8000 -v /srv/tyi-take_yout_investiments/investments.db:/app/investments.db -e MARKET_DATA_LOG_SOURCES=1 invest-portal-backend

# frontend com Basic Auth
docker run -d --name frontend --network invest-net -p 5173:80 -e BASIC_AUTH_USER=amor -e BASIC_AUTH_PASS='250109' invest-portal-frontend
```

Acesse em `http://127.0.0.1:5173`.

### Basic Auth

- O frontend exige `BASIC_AUTH_USER` e `BASIC_AUTH_PASS`.
- Se essas variáveis não forem informadas, o container do frontend encerra com erro.

### Providers de market data

- O backend aceita varios providers configurados por ordem de prioridade via `MARKET_DATA_PROVIDERS`.
- Exemplo com Twelve Data, Alpha Vantage, CoinGecko, Brapi e Yahoo: `MARKET_DATA_PROVIDERS=twelve_data,alpha_vantage,coingecko,brapi,yahoo`
- Para usar so Yahoo: `MARKET_DATA_PROVIDERS=yahoo`
- Para usar so Brapi: `MARKET_DATA_PROVIDERS=brapi`
- Para usar CoinGecko em cripto: `MARKET_DATA_PROVIDERS=coingecko,yahoo`
- Para usar Alpha Vantage em acoes US: `MARKET_DATA_PROVIDERS=alpha_vantage,yahoo`
- Para usar Twelve Data em acoes US: `MARKET_DATA_PROVIDERS=twelve_data,yahoo`
- Tambem e possivel configurar ordem automatica por classe com `MARKET_DATA_PROVIDERS_US`, `MARKET_DATA_PROVIDERS_CRYPTO` e `MARKET_DATA_PROVIDERS_BR`.
- Exemplo: `MARKET_DATA_PROVIDERS_US=twelve_data,alpha_vantage,yahoo`
- Exemplo: `MARKET_DATA_PROVIDERS_CRYPTO=coingecko,yahoo`
- Exemplo: `MARKET_DATA_PROVIDERS_BR=brapi,yahoo,google`
- A chave da Twelve Data deve ser informada em `TWELVE_DATA_API_KEY`.
- A chave da Alpha Vantage deve ser informada em `ALPHA_VANTAGE_API_KEY`.
- O token do Brapi deve ser informado em `BRAPI_TOKEN`.
- A chave demo/pro da CoinGecko deve ser informada em `COINGECKO_API_KEY`.
- A variavel legada `MARKET_DATA_PROVIDER` tambem funciona para um provider unico.
- `brapi` atualmente fornece metricas, perfil do ativo e historico para ativos do mercado brasileiro.
- `coingecko` atualmente fornece metricas, perfil e historico para tickers cripto no formato `BTC-USD`.
- `alpha_vantage` atualmente fornece metricas, perfil e historico para acoes US.
- `twelve_data` atualmente fornece metricas e historico para acoes US.
- `google` atualmente fornece metricas de mercado.
- `yahoo` atualmente fornece metricas, perfil do ativo e historico de precos.
- Se um provider nao suportar uma capacidade, o backend tenta o proximo da lista para aquela operacao.

### Restore do banco

Banco principal: `backend/investments.db`  
Backups: `backend/backups/*.sqlite3`

```bash
# parar backend antes do restore
docker rm -f backend

# (opcional) backup do estado atual
cp backend/investments.db backend/backups/investments_before_restore_$(date +%Y%m%d_%H%M%S).sqlite3

# restaurar um backup
cp backend/backups/investments_YYYYMMDD_HHMMSS.sqlite3 backend/investments.db
```

## Rotas

- Frontend SPA:
  - `/`
  - `/carteira`
  - `/renda-fixa`
  - `/graficos`
  - `/ativo/:ticker`
  - `/nova`
  - `/novo`
  - `/carteiras`
- Backend API:
  - `/api/*`
  - `/api/health`
  - `/api/metrics`
  - `/api/sync/market-data`
  - `/api/sync/market-data/:ticker`

## Banco SQL

- Persistencia em SQLite no arquivo `backend/investments.db`
- Schema SQL em `backend/app/schema.sql`
- O banco e criado automaticamente na primeira execucao

## Estrutura

- `backend/app/__init__.py` app factory
- `backend/app/db.py` conexao SQLite, inicializacao e seed
- `backend/app/services.py` regras de negocio e queries SQL
- `backend/app/api_routes.py` endpoints da API JSON
- `backend/app/observability.py` logs estruturados, metricas HTTP e healthcheck detalhado
- `backend/run.py` entrada do backend
- `frontend/` app React (Vite) consumindo `/api`

## Observabilidade

- Logs do backend sao emitidos em JSON, incluindo `event`, `request_id`, rota, status e duracao.
- `GET /api/health` retorna `200` quando a app esta saudavel e `503` quando ha degradacao real.
- O healthcheck inclui estado do banco, backup mais recente e status dos jobs de sync.
- `GET /api/metrics` expõe metricas basicas por rota, com contagem, erros 4xx/5xx e tempos medios/maximos.

## Limpeza aplicada nesta baseline

- backend legado baseado em templates Flask removido
- projeto mantido em modo API + SPA
- `frontend/node_modules` e artefatos locais removidos do versionamento
- dependencias do frontend fixadas para preservar esta versao

## backup do banco de daddos
- curl http://192.168.0.40:8000/api/backup/database
