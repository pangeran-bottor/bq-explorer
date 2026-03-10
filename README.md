# BQ Explorer

A Streamlit app for exploring BigQuery projects — browse datasets, tables, views, and their metadata at a glance.

## Features

- Lists all datasets in a GCP project with descriptions, creation dates, and creators
- Shows tables and views with row counts, storage size, and last-modified timestamps
- Fetches dataset/table creator info from Cloud Audit Logs
- Local parquet cache for fast reloads without re-querying BigQuery
- Filter and sort across all objects or drill into individual datasets

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- GCP credentials configured via Application Default Credentials (ADC)

## Setup

1. **Authenticate with GCP** (if not already done):

   ```bash
   gcloud auth application-default login
   ```

2. **Install dependencies**:

   ```bash
   cd bq-explorer
   uv sync
   ```

## Running the app

```bash
uv run streamlit run app.py
```

The app will open in your browser. From the sidebar:

1. Set the **Region** (defaults to `US`)
2. Click **Load** to use cached data, or **Refresh from BQ** to fetch live from BigQuery

## Required GCP permissions

The authenticated account needs:

- `bigquery.datasets.get` / `bigquery.tables.list` — for INFORMATION_SCHEMA queries
- `logging.logEntries.list` — for fetching dataset/table creators from audit logs (optional; the app gracefully falls back if unavailable)
