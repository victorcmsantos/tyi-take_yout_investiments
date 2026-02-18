# tyi-take_yout_investiments

MVP de portal de investimentos em Flask, inspirado no estilo do Investidor10.

## Como rodar

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run.py
```

Acesse em `http://127.0.0.1:8000`.

## Frontend separado (React + Vite)

O backend Flask agora expõe API em `/api`. O frontend em `frontend/` consome essa API.

```bash
# terminal 1 (backend)
source .venv/bin/activate
python run.py

# terminal 2 (frontend)
cd frontend
npm install
npm run dev
```

Frontend: `http://127.0.0.1:5173`

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

- Persistencia em SQLite no arquivo `investments.db`
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
