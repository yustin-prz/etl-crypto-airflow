# Crypto ETL Pipeline — Apache Airflow + Docker

Automated daily ETL pipeline orchestrated with Apache Airflow running on Docker. Extracts real-time cryptocurrency market data from the CoinGecko public API, transforms and enriches it with Python, and loads it into PostgreSQL for historical analysis.

---

## Pipeline Architecture

```
CoinGecko API
      │
      ▼
  [EXTRACT]  ── Fetches top 100 cryptos via REST API (paginated, rate-limit safe)
      │
      ▼
  [TRANSFORM] ── Cleans, renames, and engineers 4 new features
      │
      ▼
   [LOAD]  ── Appends snapshot to PostgreSQL with execution timestamp
      │
      ▼
 [VALIDATE] ── Runs 3 automated data quality checks
```

![DAG Graph](data/dag_graph.png)

---

## DAG Details

| Field | Value |
|---|---|
| DAG ID | `crypto_etl_pipeline` |
| Schedule | Daily at 08:00 AM UTC (`0 8 * * *`) |
| Owner | yustin-prz |
| Retries | 2 (with 5-minute delay) |
| Catchup | Disabled |
| Tags | crypto · etl · coingecko · portfolio |

---

## Tasks

### 1. `extract` — PythonOperator
Consumes the CoinGecko `/coins/markets` endpoint across 2 pages (50 cryptos/page = 100 total). Handles rate limiting with `time.sleep()` and retries on HTTP 429. Passes raw data to the next task via Airflow XCom.

### 2. `transform` — PythonOperator
Selects and renames 14 relevant columns, enforces data types, and engineers 4 new features:

| Feature | Description |
|---|---|
| `cap_segment` | Market cap tier: Mega/Large/Mid/Small Cap |
| `performance_24h` | 24h price movement label |
| `volume_to_cap_ratio` | Liquidity indicator |
| `snapshot_date` | DAG execution date for historical tracking |

### 3. `load` — PythonOperator
Appends the transformed snapshot to the `crypto_daily_snapshots` table in PostgreSQL using SQLAlchemy. Uses `if_exists='append'` to accumulate historical data across daily runs.

### 4. `validate` — PythonOperator
Runs 3 automated data quality checks before marking the run as successful:
- ✓ At least 90 records loaded for today
- ✓ No null prices in the top 10 cryptos
- ✓ Bitcoin (BTC) is present in the snapshot

---

## Stack

| Tool | Purpose |
|---|---|
| Apache Airflow 2.9.1 | Pipeline orchestration and scheduling |
| Docker + Docker Compose | Containerized environment |
| Python 3.x | ETL logic (extract, transform, validate) |
| PostgreSQL | Data storage (historical snapshots) |
| SQLAlchemy | Database connection layer |
| CoinGecko API | Real-time market data source |

---

## Project Structure

```
etl-crypto-airflow/
│
├── dags/
│   └── crypto_etl_dag.py        # DAG definition + ETL logic
│
├── data/
│   └── dag_graph.png            # Airflow DAG graph screenshot
│
├── logs/                        # Airflow task logs (auto-generated)
├── plugins/                     # Custom Airflow plugins (empty)
│
├── docker-compose.yml           # Airflow + PostgreSQL services
├── requirements.txt
├── .gitignore
└── README.md
```

---

## How to Reproduce

### Prerequisites
- Docker Desktop installed and running
- PostgreSQL 18 running locally on port 5432 with a `gold_analysis` database

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/yustin-prz/etl-crypto-airflow.git
cd etl-crypto-airflow

# 2. Start Airflow with Docker Compose
docker compose up -d

# 3. Wait ~2 minutes for services to initialize, then open
# http://localhost:8080
# Username: admin | Password: admin123

# 4. Activate the DAG and trigger a manual run
# Toggle ON crypto_etl_pipeline → click ▶ Trigger DAG
```

> **Note:** Update the PostgreSQL connection string in `docker-compose.yml` if your password differs from `admin123`:
> ```yaml
> COINGECKO_DB_CONN: postgresql+psycopg2://postgres:YOUR_PASSWORD@host.docker.internal:5432/gold_analysis
> ```

### Stop services
```bash
docker compose down
```

---

## Sample Run Output

```
[extract]   Page 1: 50 cryptos extracted
[extract]   Page 2: 50 cryptos extracted
[extract]   Extraction complete: 100 total records

[transform] Transformation complete: 100 rows × 18 columns

[load]      100 records loaded today / 200 total records in table

[validate]  ✓ 100 records loaded
            ✓ 0 null prices in top 10
            ✓ BTC present in snapshot
```

---

## Tools & Libraries

![Airflow](https://img.shields.io/badge/Airflow-2.9.1-017CEE?style=flat-square)
![Docker](https://img.shields.io/badge/Docker-29.4.1-2496ED?style=flat-square)
![Python](https://img.shields.io/badge/Python-3.x-blue?style=flat-square)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-336791?style=flat-square)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-red?style=flat-square)

---

## Author

**Yustin Eduardo Pérez Castro**  
[LinkedIn](https://www.linkedin.com/in/yustin-prz/) · [GitHub](https://github.com/yustin-prz)
