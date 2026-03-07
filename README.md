# Refund Abuse Decisioning Engine

AI-augmented refund abuse detection and decisioning platform built on Databricks. Combines deterministic risk scoring with LLM-powered explanations to help Customer Service Representatives triage refund requests faster and more consistently.

## Architecture

```
UC Volume (CSVs) → Lakeflow DLT Pipeline → Gold Decisioning Tables
                    Bronze → Silver → Gold        │
                                                   ├── Genie Space (NL analytics)
                                                   ├── Serving Tables (liquid clustering)
                                                   │        │
Foundation Model API ─────────────┐                │        ▼
(Claude Sonnet 4)                 │          SQL Warehouse
                                  │                │
                                  ▼                ▼
                           Databricks App: refund-console
                           (FastAPI + React)
                                  │
                           MLflow Traces
```

## Project Structure

```
refund-engine/
├── config.env.template           # Configuration template (copy to config.env)
├── generate_data.py              # Synthetic data generation (Polars + NumPy)
├── pipeline_notebook.py          # DLT pipeline (Bronze → Silver → Gold)
├── scripts/
│   ├── 01_setup_catalog.sh       # Catalog, schemas, UC Volume
│   ├── 02_generate_data.sh       # Generate + upload mock data
│   ├── 03_deploy_pipeline.sh     # Upload notebook + create DLT pipeline
│   ├── 04_setup_serving.py       # Serving tables with liquid clustering
│   ├── 05_setup_genie.py         # Genie Space for NL analytics
│   ├── 06_deploy_app.sh          # Build frontend + deploy app
│   └── deploy_all.sh             # End-to-end orchestrator
└── refund-console/               # Databricks App
    ├── app.yaml                  # App config (uvicorn + env vars)
    ├── app.py                    # FastAPI entry point + MLflow setup
    ├── requirements.txt
    ├── server/
    │   ├── config.py             # Dual-mode auth (CLI profile / SP)
    │   ├── warehouse.py          # SQL queries + TTL cache
    │   ├── llm.py                # Foundation Model client
    │   ├── agent.py              # 4-step decisioning pipeline
    │   └── routes/
    │       ├── dashboard.py      # GET /api/dashboard
    │       ├── cases.py          # GET /api/cases, /api/cases/{id}
    │       ├── actions.py        # POST /api/cases/{id}/action
    │       ├── agent.py          # POST /api/agent/decide
    │       ├── feedback.py       # GET/POST /api/feedback
    │       └── genie.py          # GET/POST /api/genie/*
    └── frontend/                 # React + TypeScript + Tailwind
        ├── package.json
        ├── vite.config.ts
        └── src/
            ├── App.tsx
            └── pages/
                ├── Dashboard.tsx
                ├── CaseList.tsx
                ├── CaseDetail.tsx
                ├── Genie.tsx
                └── Feedback.tsx
```

## Quick Start

### Prerequisites

- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) configured with a profile
- A Databricks workspace with:
  - Serverless SQL Warehouse
  - Foundation Model endpoint (e.g., `databricks-claude-sonnet-4`)
  - Unity Catalog enabled
- Node.js 18+ (for frontend build)
- Python 3.10+

### Setup

```bash
# 1. Clone and configure
git clone <repo-url> && cd refund-engine
cp config.env.template config.env
# Edit config.env with your workspace details

# 2. Deploy everything
source config.env
bash scripts/deploy_all.sh
```

The `deploy_all.sh` script handles:
1. Catalog and schema creation
2. Synthetic data generation and upload
3. DLT pipeline deployment and execution
4. Serving table creation with liquid clustering
5. Genie Space setup
6. App build and deployment
7. Service principal permissions (catalog, MLflow, Genie)
8. App resource configuration (warehouse, serving endpoint)

### Local Development

```bash
cd refund-console
source ../config.env
pip install -r requirements.txt
cd frontend && npm install && npm run build && cd ..
uvicorn app:app --reload --port 8000
```

## App Features

| Tab | Purpose |
|-----|---------|
| **Dashboard** | KPI cards (pending, approval/rejection/escalation rates), financial metrics, risk distribution |
| **Cases** | Filterable case queue with detail view, AI analysis, and action buttons |
| **Genie** | Natural language analytics over refund data via Genie Space |
| **Feedback** | Log false positives and missed abuse for model improvement |

### AI Decisioning Pipeline

The "Run AI Analysis" button triggers a 4-step pipeline (traced with MLflow):

1. **Transaction Validation** — order exists, return window, item eligibility
2. **Policy Compliance** — auto-approve thresholds, category rules
3. **Abuse Risk Scoring** — composite score from customer history, household patterns, delivery signals
4. **LLM Recommendation** — Claude Sonnet 4 synthesizes all context into a structured recommendation

### Risk Scoring

Composite weighted score (0.0–1.0):

| Factor | Weight |
|--------|--------|
| Refund Rate | 25% |
| Frequency | 20% |
| Amount | 15% |
| Household | 15% |
| Delivery | 15% |
| Policy | 10% |

Tiers: **CRITICAL** (≥0.70), **HIGH** (0.40–0.69), **MEDIUM** (0.20–0.39), **LOW** (<0.20)

## API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/health` | GET | Health check |
| `/api/dashboard` | GET | KPI metrics and risk distribution |
| `/api/cases` | GET | Paginated case list (query: status, risk_tier, channel, limit, offset) |
| `/api/cases/{refund_id}` | GET | Full case detail with customer 360 |
| `/api/cases/{refund_id}/action` | POST | Submit CSR action (body: action, reason) |
| `/api/agent/decide` | POST | Run AI decisioning pipeline (body: refund_id) |
| `/api/genie/space` | GET | Genie Space configuration |
| `/api/genie/ask` | POST | Natural language query (body: question, conversation_id) |
| `/api/feedback` | GET | List feedback entries |
| `/api/feedback` | POST | Submit feedback (body: refund_id, feedback_type, notes) |

## Observability

MLflow tracing captures every AI decision with:
- 6 spans per trace (pipeline → 4 steps + OpenAI autolog)
- Token usage (input/output tokens per LLM call)
- Custom attributes (refund_id, risk_score, recommended_action, confidence)
- Full decision audit trail

## Configuration

All workspace-specific values are in `config.env` (not committed). See `config.env.template` for required variables:

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_PROFILE` | CLI auth profile |
| `DATABRICKS_USER` | Your email |
| `DATABRICKS_WAREHOUSE_ID` | SQL warehouse ID |
| `REFUND_CATALOG` | Unity Catalog name (default: `refund_decisioning`) |
| `SERVING_ENDPOINT` | Foundation Model endpoint |
| `APP_NAME` | Databricks App name (default: `refund-console`) |
