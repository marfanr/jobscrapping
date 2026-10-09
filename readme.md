# jobscrapping

A job-posting data pipeline: scraping → lakehouse (Polaris + Trino) → transformation (dbt, Spark) → orchestration (Airflow).

## Repository Layout

```
.
├── infra/                    # Infrastructure configuration
├── jobscrapper/              # Job-posting scraper
├── docker-compose.yml
├── docker-compose.full.yml
├── .env.example
└── .python-version
```

## Prerequisites

- Docker and Docker Compose
- Python (version specified in `.python-version`)

## Getting Started

### 1. Start the infrastructure

```bash
docker compose up -d
```

### 2. Run the scraper

```bash
cd jobscrapper
pip install -r requirements.txt
python main.py scrap
```

### 3. Retrieve the Polaris credentials

The Polaris client ID and secret are printed in the `polaris-init` logs:

```bash
docker compose logs polaris-init
```

Copy `.env.example` to `.env` in the repository root and fill in the values from the logs:

```dotenv
TRINO_CLIENT_ID=<client_id>
TRINO_CLIENT_SECRET=<client_secret>
```

> **Note:** Never commit `.env` to version control.

### 4. Recreate the Trino container

Recreate Trino so that it picks up the new credentials:

```bash
docker compose up -d trino --force-recreate
```

### 5. Seed the dbt models

```bash
docker compose run dbt-seed
```

### 6. Run the main ETL

Open the Airflow UI and trigger the **`spark_warehouse`** DAG.

## Quick Reference

| Step | Action | Command |
|------|--------|---------|
| 1 | Start infrastructure | `docker compose up -d` |
| 2 | Scrape data | `cd jobscrapper && pip install -r requirements.txt && python main.py scrap` |
| 3 | Retrieve credentials | `docker compose logs polaris-init` → save to `.env` |
| 4 | Recreate Trino | `docker compose up -d trino --force-recreate` |
| 5 | Seed dbt | `docker compose run dbt-seed` |
| 6 | Run ETL | Trigger the `spark_warehouse` DAG in Airflow |