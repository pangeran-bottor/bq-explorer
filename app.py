"""
BigQuery Explorer — Streamlit app showing datasets, tables/views, and metadata.
"""

import logging
import time

import streamlit as st
import google.auth
from google.cloud import bigquery
from google.cloud import logging_v2
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("bq-explorer")

CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)

st.set_page_config(page_title="BQ Explorer", layout="wide")


@st.cache_resource
def get_client(region: str) -> bigquery.Client:
    log.info("Authenticating with GCP (ADC) …")
    credentials, project = google.auth.default()
    log.info("Authenticated — project=%s, region=%s", project, region)
    return bigquery.Client(project=project, credentials=credentials, location=region)


# ── Data fetching ────────────────────────────────────────────────

def fetch_datasets(client: bigquery.Client, region: str) -> pd.DataFrame:
    log.info("Fetching datasets from INFORMATION_SCHEMA.SCHEMATA …")
    t0 = time.time()
    sql = f"""
    SELECT
        s.schema_name                AS dataset_id,
        s.creation_time,
        s.last_modified_time,
        s.location,
        COALESCE(opt.option_value, '') AS description
    FROM `region-{region}.INFORMATION_SCHEMA.SCHEMATA` s
    LEFT JOIN `region-{region}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS` opt
           ON s.schema_name = opt.schema_name AND opt.option_name = 'description'
    ORDER BY s.schema_name
    """
    df = client.query(sql).to_dataframe()
    log.info("Fetched %d datasets in %.1fs: %s", len(df), time.time() - t0, ", ".join(df["dataset_id"].tolist()))
    return df


def _get_log_client(project: str) -> logging_v2.Client:
    credentials, _ = google.auth.default()
    return logging_v2.Client(project=project, credentials=credentials)


def _parse_dest_table(dest: str) -> tuple[str, str] | None:
    """Parse 'projects/P/datasets/D/tables/T' into (dataset_id, table_name)."""
    parts = dest.split("/")
    if len(parts) >= 6 and "tables" in parts:
        idx = parts.index("tables")
        # Strip partition decorators like $20260308
        tbl = parts[idx + 1].split("$")[0]
        return parts[idx - 1], tbl
    return None


def fetch_dataset_creators(project: str) -> pd.DataFrame:
    """Get who created each dataset from Cloud Audit Logs."""
    log.info("Fetching dataset creators from audit logs …")
    t0 = time.time()
    try:
        log_client = _get_log_client(project)
        entries = list(log_client.list_entries(
            filter_=(
                'resource.type="bigquery_dataset" '
                'AND protoPayload.methodName='
                '"google.cloud.bigquery.v2.DatasetService.InsertDataset"'
            ),
            order_by=logging_v2.ASCENDING,
        ))
        rows = []
        seen = set()
        for entry in entries:
            payload = entry.payload
            if not isinstance(payload, dict):
                continue
            resource = payload.get("resourceName", "")
            parts = resource.split("/")
            if len(parts) >= 4 and parts[-2] == "datasets":
                ds_id = parts[-1]
                if ds_id not in seen:
                    seen.add(ds_id)
                    email = payload.get("authenticationInfo", {}).get("principalEmail", "Unknown")
                    rows.append({"dataset_id": ds_id, "created_by": email})
        log.info("Found creators for %d datasets in %.1fs", len(rows), time.time() - t0)
        return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["dataset_id", "created_by"])
    except Exception:
        log.warning("Failed to fetch dataset creators from audit logs", exc_info=True)
        return pd.DataFrame(columns=["dataset_id", "created_by"])


def fetch_table_creators(project: str) -> pd.DataFrame:
    """Get who created each table/view from Cloud Audit Logs (DDL jobs + load jobs)."""
    log.info("Fetching table/view creators from audit logs …")
    t0 = time.time()
    try:
        log_client = _get_log_client(project)
        rows = []
        seen = set()

        # 1) DDL jobs: CREATE TABLE, CREATE VIEW, CTAS, etc.
        ddl_entries = list(log_client.list_entries(
            filter_=(
                'protoPayload.metadata.jobChange.job.jobConfig.queryConfig'
                '.statementType:("CREATE_VIEW" OR "CREATE_TABLE" OR '
                '"CREATE_TABLE_AS_SELECT" OR "CREATE_MATERIALIZED_VIEW")'
            ),
            order_by=logging_v2.ASCENDING,
        ))
        for entry in ddl_entries:
            payload = entry.payload
            if not isinstance(payload, dict):
                continue
            email = payload.get("authenticationInfo", {}).get("principalEmail", "Unknown")
            config = (payload.get("metadata", {})
                      .get("jobChange", {}).get("job", {})
                      .get("jobConfig", {}).get("queryConfig", {}))
            dest = config.get("destinationTable", "")
            parsed = _parse_dest_table(dest)
            if parsed and parsed not in seen:
                seen.add(parsed)
                rows.append({"dataset_id": parsed[0], "table_name": parsed[1], "created_by": email})

        # 2) Load jobs: BQ Data Transfer, API loads, etc.
        load_entries = list(log_client.list_entries(
            filter_=(
                'protoPayload.metadata.jobChange.job.jobConfig'
                '.loadConfig.destinationTable:""'
            ),
            order_by=logging_v2.ASCENDING,
        ))
        for entry in load_entries:
            payload = entry.payload
            if not isinstance(payload, dict):
                continue
            email = payload.get("authenticationInfo", {}).get("principalEmail", "Unknown")
            config = (payload.get("metadata", {})
                      .get("jobChange", {}).get("job", {})
                      .get("jobConfig", {}).get("loadConfig", {}))
            dest = config.get("destinationTable", "")
            parsed = _parse_dest_table(dest)
            if parsed and parsed not in seen:
                seen.add(parsed)
                rows.append({"dataset_id": parsed[0], "table_name": parsed[1], "created_by": email})

        log.info("Found creators for %d tables/views in %.1fs", len(rows), time.time() - t0)
        return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["dataset_id", "table_name", "created_by"])
    except Exception:
        log.warning("Failed to fetch table creators from audit logs", exc_info=True)
        return pd.DataFrame(columns=["dataset_id", "table_name", "created_by"])


def fetch_tables(client: bigquery.Client, region: str) -> pd.DataFrame:
    log.info("Fetching tables/views from INFORMATION_SCHEMA.TABLES …")
    t0 = time.time()
    sql = f"""
    SELECT
        t.table_schema              AS dataset_id,
        t.table_name,
        t.table_type,
        t.creation_time,

        s.total_rows                AS row_count,
        s.total_logical_bytes       AS size_bytes,
        s.storage_last_modified_time AS last_modified,
        COALESCE(opt.option_value, '') AS description
    FROM `region-{region}.INFORMATION_SCHEMA.TABLES` t
    LEFT JOIN `region-{region}.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT` s
           ON t.table_schema = s.table_schema
          AND t.table_name   = s.table_name
          AND s.deleted = false
    LEFT JOIN `region-{region}.INFORMATION_SCHEMA.TABLE_OPTIONS` opt
           ON t.table_schema = opt.table_schema
          AND t.table_name   = opt.table_name
          AND opt.option_name = 'description'
    ORDER BY t.table_schema, t.table_name
    """
    df = client.query(sql).to_dataframe()
    by_type = df.groupby("table_type").size().to_dict()
    type_summary = ", ".join(f"{v} {k.lower().replace('base ', '')}" for k, v in sorted(by_type.items()))
    log.info("Fetched %d objects (%s) in %.1fs", len(df), type_summary, time.time() - t0)
    for ds_id in sorted(df["dataset_id"].unique()):
        ds_objs = df[df.dataset_id == ds_id]
        names = [f"{r['table_name']} ({r['table_type'].lower().replace('base ', '')})" for _, r in ds_objs.iterrows()]
        log.info("  %s: %s", ds_id, ", ".join(names))
    return df


def count_view_rows(client: bigquery.Client, project: str, df_tables: pd.DataFrame) -> pd.DataFrame:
    """Run SELECT COUNT(*) for each view and update row_count in-place."""
    views = df_tables[df_tables.table_type == "VIEW"]
    if views.empty:
        log.info("No views to count rows for")
        return df_tables
    view_names = [f"{r['dataset_id']}.{r['table_name']}" for _, r in views.iterrows()]
    log.info("Counting rows for %d views: %s", len(views), ", ".join(view_names))
    t0 = time.time()

    # Build a single UNION ALL query to count all views at once
    parts = []
    for _, row in views.iterrows():
        fqn = f"`{project}.{row['dataset_id']}.{row['table_name']}`"
        parts.append(
            f"SELECT '{row['dataset_id']}' AS dataset_id, "
            f"'{row['table_name']}' AS table_name, "
            f"COUNT(*) AS cnt FROM {fqn}"
        )

    sql = " UNION ALL ".join(parts)
    try:
        df_counts = client.query(sql).to_dataframe()
    except Exception:
        log.warning("Combined view-count query failed, falling back to per-view queries")
        # If the combined query fails, fall back to per-view queries
        counts = {}
        for _, row in views.iterrows():
            fqn = f"`{project}.{row['dataset_id']}.{row['table_name']}`"
            try:
                result = client.query(f"SELECT COUNT(*) AS cnt FROM {fqn}").to_dataframe()
                counts[(row["dataset_id"], row["table_name"])] = result["cnt"].iloc[0]
            except Exception:
                log.warning("Failed to count rows for %s.%s", row["dataset_id"], row["table_name"])
        for (ds, tbl), cnt in counts.items():
            mask = (df_tables.dataset_id == ds) & (df_tables.table_name == tbl)
            df_tables.loc[mask, "row_count"] = cnt
        log.info("View row counts done (fallback) in %.1fs", time.time() - t0)
        return df_tables

    for _, cr in df_counts.iterrows():
        mask = (df_tables.dataset_id == cr["dataset_id"]) & (df_tables.table_name == cr["table_name"])
        df_tables.loc[mask, "row_count"] = cr["cnt"]

    log.info("View row counts done in %.1fs", time.time() - t0)
    return df_tables


# ── Local cache ──────────────────────────────────────────────────

def _cache_path(project: str, region: str, name: str) -> Path:
    return CACHE_DIR / f"{project}_{region}_{name}.parquet"


def _meta_path(project: str, region: str) -> Path:
    return CACHE_DIR / f"{project}_{region}_meta.txt"


def save_cache(project: str, region: str, df_datasets: pd.DataFrame, df_tables: pd.DataFrame):
    log.info("Saving results to local cache …")
    df_datasets.to_parquet(_cache_path(project, region, "datasets"))
    df_tables.to_parquet(_cache_path(project, region, "tables"))
    _meta_path(project, region).write_text(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
    log.info("Cache saved")


def load_cache(project: str, region: str):
    ds_path = _cache_path(project, region, "datasets")
    tbl_path = _cache_path(project, region, "tables")
    meta_path = _meta_path(project, region)
    if ds_path.exists() and tbl_path.exists():
        log.info("Loading data from local cache …")
        df_datasets = pd.read_parquet(ds_path)
        df_tables = pd.read_parquet(tbl_path)
        cached_at = meta_path.read_text().strip() if meta_path.exists() else "unknown"
        log.info("Cache loaded — %d datasets, %d tables/views (cached at %s)", len(df_datasets), len(df_tables), cached_at)
        return df_datasets, df_tables, cached_at
    log.info("No cache found for project=%s region=%s", project, region)
    return None, None, None


# ── Formatters ───────────────────────────────────────────────────

def fmt_size(b):
    if b is None or pd.isna(b):
        return "—"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(b) < 1024:
            return f"{b:,.1f} {unit}"
        b /= 1024
    return f"{b:,.1f} PB"


def fmt_ts(ts):
    if ts is None or pd.isna(ts):
        return "—"
    if isinstance(ts, pd.Timestamp):
        return ts.strftime("%Y-%m-%d %H:%M UTC")
    return str(ts)


def build_display_df(source: pd.DataFrame, include_dataset: bool = False) -> pd.DataFrame:
    """Build a display-ready dataframe with numeric columns kept numeric for sorting."""
    has_created_by = "created_by" in source.columns
    cols = ["table_name", "table_type", "creation_time", "last_modified", "row_count", "size_bytes", "description"]
    if has_created_by:
        cols.insert(3, "created_by")
    if include_dataset:
        cols = ["dataset_id"] + cols
    df = source[cols].copy()

    # Keep row_count as numeric (Int64 to handle NaN), fill NaN with 0 for sortability
    df["row_count"] = pd.to_numeric(df["row_count"], errors="coerce").astype("Int64")

    # Keep size_bytes as numeric for sorting
    df["size_bytes"] = pd.to_numeric(df["size_bytes"], errors="coerce").astype("Int64")

    rename = {
        "table_name": "Name",
        "table_type": "Type",
        "creation_time": "Created",
        "created_by": "Created By",
        "last_modified": "Last Modified",
        "row_count": "Rows",
        "size_bytes": "Size (bytes)",
        "description": "Description",
    }
    if include_dataset:
        rename["dataset_id"] = "Dataset"

    df = df.rename(columns=rename)
    return df


# ── Sidebar ──────────────────────────────────────────────────────
st.sidebar.title("BQ Explorer")
region = st.sidebar.text_input("Region", value="US")

st.sidebar.divider()

# Check for existing cache before connecting
_cached_at_preview = None
try:
    _, _adc_project = google.auth.default()
    meta = _meta_path(_adc_project, region)
    if meta.exists():
        _cached_at_preview = meta.read_text().strip()
except Exception:
    pass

if _cached_at_preview:
    st.sidebar.caption(f"Cached: {_cached_at_preview}")

col_load, col_refresh = st.sidebar.columns(2)
load_clicked = col_load.button("Load", type="primary")
refresh_clicked = col_refresh.button("Refresh from BQ")

if refresh_clicked:
    st.session_state["load"] = True
    st.session_state["force_refresh"] = True

if load_clicked:
    st.session_state["load"] = True
    st.session_state.pop("force_refresh", None)

if not st.session_state.get("load"):
    st.info("Configure the region in the sidebar and click **Load** (uses cache if available) or **Refresh from BQ**.")
    st.stop()

client = get_client(region)
project = client.project
st.sidebar.success(f"Project: `{project}`")

# ── Fetch or load from cache ─────────────────────────────────────
force_refresh = st.session_state.pop("force_refresh", False)

if not force_refresh:
    df_datasets, df_tables, cached_at = load_cache(project, region)
    if df_datasets is not None:
        # Ensure backward compat if cache lacks created_by column
        if "created_by" not in df_datasets.columns:
            df_datasets["created_by"] = "Unknown"
        if "created_by" not in df_tables.columns:
            df_tables["created_by"] = "Unknown"
        st.sidebar.caption(f"Using cache from {cached_at}")
        st.toast(f"Loaded from cache ({cached_at})")
    else:
        force_refresh = True  # no cache exists, must fetch

if force_refresh:
    log.info("Starting fresh data fetch for project=%s region=%s", project, region)
    fetch_t0 = time.time()

    with st.status("Fetching data from BigQuery …", expanded=True) as status:
        status.update(label="Querying datasets …")
        st.write("Querying `INFORMATION_SCHEMA.SCHEMATA` …")
        df_datasets = fetch_datasets(client, region)
        ds_names = df_datasets["dataset_id"].tolist()
        st.write(f"Found **{len(ds_names)}** datasets: `{'`, `'.join(ds_names)}`")

        status.update(label="Querying tables & views …")
        st.write("Querying `INFORMATION_SCHEMA.TABLES` …")
        df_tables = fetch_tables(client, region)
        tables_df = df_tables[df_tables.table_type == "BASE TABLE"]
        views_df = df_tables[df_tables.table_type == "VIEW"]
        n_tables = len(tables_df)
        n_views = len(views_df)
        st.write(f"Found **{n_tables}** tables, **{n_views}** views")

        # List tables per dataset
        for ds_id in sorted(df_tables["dataset_id"].unique()):
            ds_objs = df_tables[df_tables.dataset_id == ds_id]
            obj_names = [
                f"`{r['table_name']}` ({r['table_type'].lower().replace('base ', '')})"
                for _, r in ds_objs.iterrows()
            ]
            st.caption(f"**{ds_id}** — {', '.join(obj_names)}")

        status.update(label="Fetching creator info from audit logs …")
        st.write("Querying Cloud Audit Logs for dataset creators …")
        ds_creators = fetch_dataset_creators(project)
        st.write(f"Found creators for {len(ds_creators)} datasets")

        st.write("Querying Cloud Audit Logs for table/view creators …")
        tbl_creators = fetch_table_creators(project)
        st.write(f"Found creators for {len(tbl_creators)} tables/views")

        if not ds_creators.empty:
            df_datasets = df_datasets.merge(ds_creators, on="dataset_id", how="left")
        else:
            df_datasets["created_by"] = None
        if not tbl_creators.empty:
            df_tables = df_tables.merge(tbl_creators, on=["dataset_id", "table_name"], how="left")
        else:
            df_tables["created_by"] = None
        df_datasets["created_by"] = df_datasets["created_by"].fillna("Unknown")
        df_tables["created_by"] = df_tables["created_by"].fillna("Unknown")

        status.update(label="Saving to cache …")
        save_cache(project, region, df_datasets, df_tables)

        elapsed = time.time() - fetch_t0
        status.update(label=f"Done — fetched in {elapsed:.1f}s", state="complete", expanded=False)
        log.info("Full fetch completed in %.1fs", elapsed)

    st.sidebar.caption("Fetched live & cached just now")

# ── Header metrics ───────────────────────────────────────────────
st.title("BigQuery — Current State")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Datasets", len(df_datasets))
c2.metric("Tables", len(df_tables[df_tables.table_type == "BASE TABLE"]))
c3.metric("Views", len(df_tables[df_tables.table_type == "VIEW"]))
total_bytes = df_tables["size_bytes"].sum()
c4.metric("Total Size", fmt_size(total_bytes if not pd.isna(total_bytes) else 0))

st.divider()

tab_by_dataset, tab_all = st.tabs(["By Dataset", "All Objects"])

# ── Tab 1: Per-dataset sections ──────────────────────────────────
with tab_by_dataset:
    for _, ds in df_datasets.iterrows():
        ds_id = ds["dataset_id"]
        ds_tables = df_tables[df_tables.dataset_id == ds_id]

        desc_part = f" — _{ds['description']}_" if ds["description"] else ""
        with st.expander(f"**{ds_id}** ({len(ds_tables)} objects){desc_part}", expanded=False):
            col_a, col_b, col_c, col_d = st.columns(4)
            col_a.markdown(f"**Created:** {fmt_ts(ds['creation_time'])}")
            col_b.markdown(f"**Modified:** {fmt_ts(ds['last_modified_time'])}")
            col_c.markdown(f"**Location:** {ds['location']}")
            created_by = ds.get("created_by", "Unknown")
            col_d.markdown(f"**Created By:** {created_by}")

            if ds_tables.empty:
                st.caption("No tables or views in this dataset.")
                continue

            st.dataframe(
                build_display_df(ds_tables),
                use_container_width=True,
                hide_index=True,
            )

# ── Tab 2: All objects in one filterable table ───────────────────
with tab_all:
    fc1, fc2 = st.columns(2)
    dataset_options = ["All"] + sorted(df_tables["dataset_id"].unique().tolist())
    type_options = ["All"] + sorted(df_tables["table_type"].unique().tolist())
    sel_dataset = fc1.selectbox("Dataset", dataset_options)
    sel_type = fc2.selectbox("Type", type_options)

    filtered = df_tables.copy()
    if sel_dataset != "All":
        filtered = filtered[filtered.dataset_id == sel_dataset]
    if sel_type != "All":
        filtered = filtered[filtered.table_type == sel_type]

    st.dataframe(
        build_display_df(filtered, include_dataset=True),
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"Showing {len(filtered)} of {len(df_tables)} objects")
