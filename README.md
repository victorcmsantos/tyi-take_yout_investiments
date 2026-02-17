# tyi-take_yout_investiments

MVP de portal de investimentos em Flask, inspirado no estilo do Investidor10.

## Como rodar

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run.py
```

Acesse em `http://127.0.0.1:5000`.

## Rotas

- `/` dashboard com lista de ativos e destaques
- `/ativo/<ticker>` pagina de detalhe do ativo
- `/setores` resumo por setor
- `/carteira` consolidacao de carteira e dividendos mensais estimados
- `/transacoes/nova` lancamento de compra/venda (ativo novo e criado automaticamente)
- `/proventos/novo` lancamento de proventos (dividendo, jcp e aluguel)
- `/carteiras` criacao e selecao de multiplas carteiras

## Banco SQL

- Persistencia em SQLite no arquivo `investments.db`
- Schema SQL em `app/schema.sql`
- O banco e criado automaticamente na primeira execucao

## Estrutura

- `app/__init__.py` app factory
- `app/db.py` conexao SQLite, inicializacao e seed
- `app/routes.py` rotas Flask
- `app/services.py` regras de negocio e queries SQL
- `app/templates/` paginas HTML com Jinja2
- `app/static/css/style.css` estilos
