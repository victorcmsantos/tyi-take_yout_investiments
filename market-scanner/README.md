# Market Scanner

Quantitative B3 stock scanner built with Python, FastAPI, APScheduler, SQLAlchemy, and BRAPI.

## Features

- Market scans every 30 minutes during B3 trading sessions only (holidays excluded)
- Official B3 ticker discovery from COTAHIST + active listed companies cache
- Price history ingestion via BRAPI (`/api/quote`)
- Metrics Lab page (`/metrics-lab`) with sidebar, formula details, parameter editing, and full re-scan on save
- Modular technical metric engine
- Buy signal evaluation and weighted scoring
- SQLite persistence for official tickers, prices, metrics, and signals
- FastAPI JSON API and Jinja dashboard
- Docker-ready runtime

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py --mode all
```

Open `http://localhost:8000/dashboard`.

Set your BRAPI token in `.env` before running:

```env
BRAPI_TOKEN=seu_token_aqui
```

## Run with Docker

```bash
docker build -t market-scanner .
docker run --rm -p 8000:8000 market-scanner
```

## Run with Docker Compose

```bash
cp .env.example .env
docker compose up --build
```

## Modes

- `python main.py --mode api`: API only
- `python main.py --mode daemon`: scanner daemon only
- `python main.py --mode all`: API + embedded scheduler
