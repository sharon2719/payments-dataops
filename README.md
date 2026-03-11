# 💳 Payments DataOps Platform

A production-grade DataOps pipeline that processes payment transactions with full observability, data quality checks, and automated CI/CD — built to mirror the data infrastructure of a modern global fintech.

🔴 **[Live Dashboard](https://payments-dataops-26.streamlit.app/)**

---

## 🏗️ Architecture

```
                        ┌─────────────────────────────────────────┐
                        │           GitHub Actions CI/CD           │
                        │   (runs on every push + daily 6am UTC)  │
                        └──────────────┬──────────────────────────┘
                                       │
                                       ▼
┌──────────────┐    ┌──────────────────────────────────────────────────┐
│   Data Gen   │───▶│                  DuckDB Warehouse                │
│  (Faker +    │    │                                                  │
│   Python)    │    │  raw_transactions                                │
└──────────────┘    └──────────────────┬───────────────────────────────┘
                                       │
                                       ▼
                        ┌─────────────────────────────┐
                        │        dbt Core             │
                        │                             │
                        │  staging/                   │
                        │    stg_transactions         │
                        │                             │
                        │  marts/                     │
                        │    daily_settlements        │
                        │    fraud_summary            │
                        └──────────────┬──────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                   ▼
          ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐
          │  dbt Tests   │   │    Great     │   │    Streamlit     │
          │  (6 tests)   │   │ Expectations │   │   Dashboard      │
          │              │   │ (10 checks)  │   │  (Live on web)   │
          └──────────────┘   └──────────────┘   └──────────────────┘
```

---

## 📊 Data Lineage

```
raw_transactions
       │
       ▼
stg_transactions        ← cleaning, type casting, derived columns
       │
       ├──▶ daily_settlements    ← aggregated by date, country, card type
       │
       └──▶ fraud_summary        ← fraud rates by country, merchant, card type
```

---

## 🗂️ Project Structure

```
payments-dataops/
├── .github/
│   └── workflows/
│       └── pipeline.yml         # CI/CD — runs on push + daily schedule
├── dags/
│   └── payments_pipeline.py     # Airflow DAG definition
├── data/
│   ├── generate_transactions.py # Mock payment data generator
│   └── payments.duckdb          # DuckDB warehouse
├── dbt_project/
│   └── payments/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_transactions.sql
│       │   │   └── schema.yml   # dbt tests
│       │   └── marts/
│       │       ├── daily_settlements.sql
│       │       └── fraud_summary.sql
│       ├── dbt_project.yml
│       └── profiles.yml
├── tests/
│   └── validate_data.py         # Great Expectations validation
├── logs/
│   └── validation_log.txt       # Pipeline run history
├── docker-compose.yml           # Container orchestration
├── Dockerfile                   # App container
├── dashboard.py                 # Streamlit dashboard
├── requirements.txt
└── README.md
```

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Language | Python 3.12 | Core scripting and automation |
| Data warehouse | DuckDB | Fast local analytical warehouse |
| Data transforms | dbt Core | SQL models, tests, and lineage |
| Data quality | Great Expectations | 10 automated data validation checks |
| Orchestration | Apache Airflow | DAG-based pipeline scheduling |
| CI/CD | GitHub Actions | Automated testing + daily pipeline |
| Dashboard | Streamlit + Plotly | Live observability dashboard |
| Containerisation | Docker + Compose | Portable, reproducible environment |

---

## 🚀 Running Locally

### Option A — Python (quickest)

```bash
git clone https://github.com/sharon2719/payments-dataops.git
cd payments-dataops

python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Mac/Linux

pip install -r requirements.txt

# Run the full pipeline
python data/generate_transactions.py
cd dbt_project/payments && dbt run --profiles-dir . && dbt test --profiles-dir .
cd ../..
python tests/validate_data.py

# Launch dashboard
python -m streamlit run dashboard.py
```

### Option B — Docker

```bash
git clone https://github.com/sharon2719/payments-dataops.git
cd payments-dataops
docker-compose up --build
```

Dashboard will be available at `http://localhost:8501`

---

## ⚙️ CI/CD Pipeline

GitHub Actions runs automatically on:
- **Every push to main** — runs dbt tests + Great Expectations validation
- **Daily at 6am UTC (weekdays)** — full pipeline: generate → dbt run → dbt test → validate → commit

```
generate_transactions → dbt run → dbt test → great_expectations → commit data
```

---

## 🔬 Data Quality (Great Expectations)

10 automated expectations run on every pipeline execution:

| Check | Description |
|---|---|
| `not_null` | transaction_id, amount, status, country |
| `unique` | transaction_id |
| `value_between` | amount between $0 and $100,000 |
| `mean_between` | average transaction $50–$300 |
| `values_in_set` | status ∈ {approved, declined, pending, reversed} |
| `values_in_set` | country ∈ {US, GB, ZA, NG, KE, AE, SG, BR, MX, IN} |
| `row_count` | between 1,000 and 1,000,000 rows |

---

## 🧪 dbt Tests

6 automated tests across staging models:

- `unique` + `not_null` on `transaction_id`
- `not_null` on `amount`, `status`, `country`
- `accepted_values` on `status`

---

## 📈 Dashboard Features

- Total transactions, volume, fraud rate, approval rate
- Daily transaction volume and value over time
- Fraud rate by country (horizontal bar)
- Transaction breakdown by card type (pie chart)
- Fraud rate by merchant category
- Recent transactions table
- Pipeline health — last run time, dbt tests, GX validation status

---

## 🐳 Infrastructure Notes

This project is designed to be cloud-deployable with minimal changes:

- **Container orchestration** — Docker Compose locally; production target is **AWS ECS** or **GCP Cloud Run**
- **Infrastructure as Code** — Terraform configs for AWS deployment are a planned next step
- **Kubernetes** — Airflow can be deployed via the official Helm chart for production-grade orchestration
- **Secrets management** — GitHub Secrets locally; AWS Secrets Manager or GCP Secret Manager in production

---

## 🗺️ Roadmap

- [ ] Terraform configs for AWS ECS deployment
- [ ] Kubernetes manifests for Airflow
- [ ] Real-time streaming with Apache Kafka
- [ ] dbt Cloud integration
- [ ] Grafana observability dashboard
- [ ] Email alerting on pipeline failure

---

## 📝 License

MIT