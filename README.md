# fx-volatility-tracker

Pipeline de datos para ingestión y análisis de tipos de cambio de Argentina y México.

## Fuentes de datos

- **BCRA** — Banco Central de la República Argentina
- **DolarAPI** — tipos de cambio alternativos (Argentina)
- **Banxico** — Banco de México

## Setup

```bash
cp .env.example .env
# completar variables en .env

docker-compose up -d
pip install -r requirements.txt
```
