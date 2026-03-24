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
export ALPHA_VANTAGE_API_KEY='sua-chave-aqui'
export BRAPI_TOKEN='seu-token-aqui'
export COINGECKO_API_KEY='sua-chave-aqui'
export TWELVE_DATA_API_KEY='sua-chave-aqui'
export MARKET_DATA_PROVIDERS='twelve_data,alpha_vantage,coingecko,brapi,yahoo'
export SQLITE_TIMEOUT_SECONDS=30

docker compose up -d --build
```

Acesse em `http://127.0.0.1:8000`.

O `docker-compose.yml` agora cria a rede `invest-net` automaticamente.
Por padrão, o OpenClaw usa `ghcr.io/openclaw/openclaw:latest`. Se você tiver uma build local (ex.: `openclaw:local`), sobrescreva com `OPENCLAW_IMAGE=openclaw:local`.
O serviço `openclaw-cli` é opcional e fica no profile `cli` (não sobe no `up` padrão). Para usar o CLI manualmente: `docker compose run --rm openclaw-cli health`.

### Integracao com Market Scanner (opcional)

O scanner fica como servico separado no profile `scanner`, com build fixo local em `./market-scanner`.

```bash
docker compose --profile scanner up -d --build market-scanner
```

- Dashboard do scanner: `http://127.0.0.1:8089/dashboard`
- Página integrada no portal: `http://127.0.0.1:8000/scanner`
- Proxy no backend Flask:
  - `GET /api/scanner/health`
  - `GET /api/scanner/signals`
  - `GET /api/scanner/signal-matrix`
  - `GET /api/scanner/trades`
  - `GET /api/scanner/ticker/<TICKER>`
  - `POST /api/scanner/scan` (dispara leitura manual de todos os tickers)
  - `POST /api/scanner/scan/<TICKER>` (dispara leitura manual de um ticker)
  - `POST /api/scanner/trades`
  - `PATCH /api/scanner/trades/<TRADE_ID>`
  - `POST /api/scanner/trades/<TRADE_ID>/close`

### Arquitetura do enriquecimento por IA

- O frontend nunca fala direto com nenhum provider de IA.
- O frontend chama apenas o backend Flask.
- O backend chama apenas o gateway do OpenClaw.
- O provider, o modelo e a autenticacao da IA ficam configurados no OpenClaw.
- Para trocar de IA, ajuste o OpenClaw. O app nao precisa ser alterado para cada provider.

Para parar:

```bash
docker compose down
```

### Portabilidade (rodar em outro lugar)

O `docker-compose.yml` principal usa bind-mounts em `/srv/...` (bom para servidor), então ao clonar em outra máquina você tem 2 opções:

- **Opção A (recomendado para rodar em qualquer pasta):** use o arquivo `docker-compose.portable.yml` (paths relativos ao repo):

```bash
docker compose -f docker-compose.yml -f docker-compose.portable.yml up -d --build
```

- **Opção B (modo servidor /srv):** crie os diretórios e rode o compose normal:

```bash
sudo mkdir -p /srv/tyi-take_yout_investiments/app_vol
sudo mkdir -p /srv/tyi-take_yout_investiments/openclaw/config
sudo mkdir -p /srv/tyi-take_yout_investiments/openclaw/workspace
docker compose up -d --build
```

Além disso:

- Copie `.env.example` para `.env` e preencha suas chaves (ex: `OPENROUTER_API_KEY`, `BRAPI_TOKEN`, etc.).
- O OpenClaw usa TLS com uma CA local. Para o **Control UI no seu PC** não reclamar de certificado, importe o arquivo `openclaw-local-ca.pem` (gerado localmente). Se você mudar de host/IP, pode ser necessário regenerar o certificado TLS do gateway.

#### OpenClaw: gerar/regenerar certificados TLS (CA local + SAN)

Quando você troca de máquina, IP da LAN, ou hostname, o navegador pode reclamar porque o certificado do gateway não tem o SAN correspondente. A forma mais simples é regenerar a CA + certificado do gateway no diretório montado do OpenClaw.

1) Escolha onde fica o config do OpenClaw no host:

- **Modo portátil (usando `docker-compose.portable.yml`):** `./openclaw/config`
- **Modo servidor (compose padrão):** `/srv/tyi-take_yout_investiments/openclaw/config`

2) Rode os comandos abaixo no host (na pasta raiz do repo, ou ajuste o path do `OPENCLAW_CONFIG_DIR`). Exemplo gerando SAN para `localhost`, o nome de serviço `openclaw-gateway` (rede do compose) e um IP de LAN:

```bash
# ajuste para o IP/hostname que você usa no browser
export OPENCLAW_TLS_IP="192.168.0.10"

export OPENCLAW_CONFIG_DIR="${OPENCLAW_CONFIG_DIR:-./openclaw/config}"
export OPENCLAW_TLS_DIR="$OPENCLAW_CONFIG_DIR/gateway/tls"

mkdir -p "$OPENCLAW_TLS_DIR"
cd "$OPENCLAW_TLS_DIR"

# 1) CA local (NÃO compartilhe o .key)
openssl genrsa -out openclaw-local-ca.key 4096
openssl req -x509 -new -nodes -key openclaw-local-ca.key -sha256 -days 3650 \
  -out openclaw-local-ca.pem -subj "/CN=openclaw-local-ca"

# 2) Cert do gateway (server)
cat > san.ext <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=@alt_names

[alt_names]
DNS.1=localhost
DNS.2=openclaw-gateway
IP.1=127.0.0.1
IP.2=$OPENCLAW_TLS_IP
EOF

openssl genrsa -out gateway-server.key 2048
openssl req -new -key gateway-server.key -out gateway-server.csr -subj "/CN=openclaw-gateway"
openssl x509 -req -in gateway-server.csr -CA openclaw-local-ca.pem -CAkey openclaw-local-ca.key \
  -CAcreateserial -out gateway-server.pem -days 825 -sha256 -extfile san.ext

# (opcional) limpeza de artefatos
rm -f gateway-server.csr openclaw-local-ca.srl san.ext
```

3) Reinicie o gateway para ele recarregar os arquivos:

```bash
docker compose restart openclaw-gateway
```

Notas:

- A CA que você precisa importar no seu PC para remover o aviso do navegador é: `openclaw-local-ca.pem` (no mesmo diretório acima).
- Se você já personalizou os paths/nome dos arquivos no `openclaw.json` (ex.: `gateway.tls.certPath/keyPath/caPath`), alinhe os nomes gerados aqui com o que está configurado.
- Se você não acessa o Control UI pelo IP da LAN, pode remover o `OPENCLAW_TLS_IP` e a linha `IP.2=...` do arquivo SAN.

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

# backend (opcionalmente exposto na 8001; frontend usa a rede interna)
docker run -d --name backend --network invest-net -p 8001:8000 \
  -v /srv/tyi-take_yout_investiments/app_vol:/app_vol \
  -e DATABASE=/app_vol/investments.db \
  -e DATABASE_BACKUP_DIR=/app_vol/backups \
  -e AUTH_SECRET_KEY_FILE=/app_vol/.flask-secret \
  -e ADMIN_BOOTSTRAP_FILE=/app_vol/admin-bootstrap.txt \
  -e BACKGROUND_JOBS_LOCK_FILE=/app_vol/.background-jobs.lock \
  -e DATABASE_STARTUP_LOCK_FILE=/app_vol/.db-startup.lock \
  -e MARKET_DATA_LOG_SOURCES=1 invest-portal-backend

# frontend
docker run -d --name frontend --network invest-net -p 8000:80 invest-portal-frontend
```

Acesse em `http://127.0.0.1:8000`.

### Usuarios e admin bootstrap

- O acesso agora e feito por login de usuario na propria aplicacao.
- No primeiro boot, o backend garante a existencia de um usuario `admin`.
- A senha inicial do `admin` e gerada aleatoriamente e fica disponivel dentro do container em `/app_vol/admin-bootstrap.txt`.
- Para consultar:

```bash
docker exec backend cat /app_vol/admin-bootstrap.txt
```

- Se o arquivo nao existir mais dentro do container, o backend regenera uma nova senha bootstrap para o `admin` no proximo startup.
- A rota `/admin` permite listar usuarios, criar novos usuarios e habilitar/desabilitar contas.
- O volume persistido recomendado e `/app_vol`, contendo:
- `investments.db`
- `admin-bootstrap.txt`
- `.flask-secret`
- `backups/`

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
- Exemplo recomendado para BR: `MARKET_DATA_PROVIDERS_BR=market_scanner`
- Para ativos BR, a estrategia recomendada e `scanner-only` (o scanner concentra chamadas BRAPI e o backend so le do banco).
- O cache dessa leitura local pode ser ajustado com `MARKET_SCANNER_DATA_TTL_SECONDS` (padrao: `120`).
- A chave da Twelve Data deve ser informada em `TWELVE_DATA_API_KEY`.
- A chave da Alpha Vantage deve ser informada em `ALPHA_VANTAGE_API_KEY`.
- O token do Brapi deve ser informado em `BRAPI_TOKEN`.
- O cache de quote do Brapi pode ser ajustado com `BRAPI_QUOTE_CACHE_TTL_SECONDS` (padrao: `120`).
- Se o Brapi retornar limite/erro temporario, use `BRAPI_RATE_LIMIT_COOLDOWN_SECONDS` para pausar novas tentativas (padrao: `300`).
- A chave demo/pro da CoinGecko deve ser informada em `COINGECKO_API_KEY`.
- Se a CoinGecko estiver retornando limite (`429`), use `COINGECKO_RATE_LIMIT_COOLDOWN_SECONDS` para pausar novas tentativas por alguns minutos (padrao: `900`).
- A variavel legada `MARKET_DATA_PROVIDER` tambem funciona para um provider unico.
- `brapi` atualmente fornece metricas, perfil do ativo e historico para ativos do mercado brasileiro.
- `coingecko` atualmente fornece metricas, perfil e historico para tickers cripto no formato `BTC-USD`.
- `alpha_vantage` atualmente fornece metricas, perfil e historico para acoes US.
- `twelve_data` atualmente fornece metricas e historico para acoes US.
- `google` atualmente fornece metricas de mercado.
- `yahoo` atualmente fornece metricas, perfil do ativo e historico de precos.
- Se um provider nao suportar uma capacidade, o backend tenta o proximo da lista para aquela operacao.

### Restore do banco

Banco principal no container: `/app_vol/investments.db`  
Backups no container: `/app_vol/backups/*.sqlite3`

```bash
# parar backend antes do restore
docker rm -f backend

# (opcional) backup do estado atual
cp /srv/tyi-take_yout_investiments/app_vol/investments.db /srv/tyi-take_yout_investiments/app_vol/backups/investments_before_restore_$(date +%Y%m%d_%H%M%S).sqlite3

# restaurar um backup
cp /srv/tyi-take_yout_investiments/app_vol/backups/investments_YYYYMMDD_HHMMSS.sqlite3 /srv/tyi-take_yout_investiments/app_vol/investments.db
```

### SQLite e jobs em background

- O backend usa SQLite com `busy_timeout` e `WAL` para reduzir concorrencia de escrita.
- O startup do banco agora usa lock de arquivo para evitar disputa entre workers do Gunicorn.
- Os jobs de background (`market_sync`, `fixed_income_snapshot`, `chart_snapshot`) rodam em apenas um worker por vez.
- Variaveis uteis:
- `SQLITE_TIMEOUT_SECONDS`: timeout das conexoes SQLite antes de falhar com lock. Padrao: `30`
- `BACKGROUND_JOBS_LOCK_FILE`: arquivo de lock que define o worker lider dos jobs. Padrao: ao lado do banco, em `.background-jobs.lock`
- `DATABASE_STARTUP_LOCK_FILE`: arquivo de lock usado durante inicializacao/migracao do banco. Padrao: ao lado do banco, em `.db-startup.lock`
- `MARKET_SYNC_ENABLED`: habilita/desabilita o job de sync de mercado. Padrao: `1`
- `MARKET_DATA_STALE_AFTER_SECONDS_CRYPTO`: SLA de staleness para cripto. Recomendado: `300` para acompanhar mercado 24/7.
- `MARKET_SYNC_INTERVAL_SECONDS`: intervalo do job de sync em segundos. Padrao: `300`
- `MARKET_SYNC_SCOPE`: escopo do job de sync (`all`, `br`, `us`, `crypto`). Padrao: `all`
- `MARKET_SYNC_FORCE_LIVE_BR`: quando `1`, ignora `market_scanner` para BR no job automatico. Padrao: `0`
- Essas configuracoes ajudam a evitar `sqlite3.OperationalError: database is locked` no startup e nos warmups dos snapshots.
- Com o `docker-compose` atual, esses arquivos ficam em `/app_vol`.

## Rotas

- Frontend SPA:
  - `/login`
  - `/`
  - `/carteira`
  - `/renda-fixa`
  - `/graficos`
  - `/ativo/:ticker`
  - `/admin`
  - `/nova`
  - `/novo`
  - `/carteiras`
- Backend API:
  - `/api/*`
  - `/api/health`
  - `/api/metrics`
  - `/api/auth/me`
  - `/api/auth/login`
  - `/api/auth/logout`
  - `/api/admin/users`
  - `/api/admin/users/:id/status`
  - `/api/scanner/scan` (leitura manual de todos os tickers)
  - `/api/scanner/scan/:ticker` (leitura manual de um ticker)
  - `/api/sync/market-data` (scan geral manual)
  - `/api/sync/market-data/:ticker` (sync manual por ativo)

## Banco SQL

- Persistencia em SQLite no arquivo configurado em `DATABASE` (no compose atual: `/app_vol/investments.db`)
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
