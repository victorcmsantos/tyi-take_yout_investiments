# tyi-take_yout_investiments

MVP de portal de investimentos em Flask, inspirado no estilo do Investidor10.

## Como rodar (local)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
python backend/run.py
```

Acesse em `http://127.0.0.1:8000`.

## Frontend separado (React + Vite)

O backend Flask agora expõe API em `/api`. O frontend em `frontend/` consome essa API.

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
docker run -d --name backend --network invest-net \
  -v "$(pwd)/backend:/data" \
  -e DATABASE=/data/investments.db \
  -e DATABASE_BACKUP_DIR=/data/backups \
  invest-portal-backend

# frontend com Basic Auth
docker run -d --name frontend --network invest-net -p 5173:80 \
  -e BASIC_AUTH_USER=admin \
  -e BASIC_AUTH_PASS='troque-esta-senha' \
  invest-portal-frontend
```

Acesse em `http://127.0.0.1:5173`.

### Basic Auth

- O frontend exige `BASIC_AUTH_USER` e `BASIC_AUTH_PASS`.
- Se essas variáveis não forem informadas, o container do frontend encerra com erro.

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

- `/` dashboard com lista de ativos e destaques
- `/api/*` endpoints JSON para frontend separado
- `/ativo/<ticker>` pagina de detalhe do ativo
- `/setores` redireciona para `/`
- `/carteira` consolidacao de carteira e dividendos mensais estimados
- `/transacoes/nova` lancamento de compra/venda (ativo novo e criado automaticamente)
- `/proventos/novo` lancamento de proventos (dividendo, jcp e aluguel)
- `/carteiras` criacao e selecao de multiplas carteiras

## Banco SQL

- Persistencia em SQLite no arquivo `backend/investments.db`
- Schema SQL em `backend/app/schema.sql`
- O banco e criado automaticamente na primeira execucao

## Estrutura

- `backend/app/__init__.py` app factory
- `backend/app/db.py` conexao SQLite, inicializacao e seed
- `backend/app/routes.py` rotas Flask
- `backend/app/services.py` regras de negocio e queries SQL
- `backend/app/templates/` paginas HTML com Jinja2
- `backend/app/static/css/style.css` estilos
- `backend/app/api_routes.py` endpoints da API JSON
- `backend/run.py` entrada do backend
- `frontend/` app React (Vite) consumindo `/api`
