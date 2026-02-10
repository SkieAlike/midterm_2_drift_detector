"""
Airflow DAG: Baseline builder (ClickHouse -> Postgres)

What it does (daily, but can be triggered manually):
1) Reads raw_events from ClickHouse for the DAG's data interval.
2) Aggregates per (source_id, metric): count/mean/var (population variance).
3) Upserts into Postgres baseline_stats using cumulative merge (Option B):
   - new baseline_count = old_n + new_n
   - new mean/variance computed by merging two distributions
4) Updates Postgres source_metadata for all sources seen in the interval.

Manual run override:
- Trigger DAG with config JSON:
  {
    "start": "2026-02-10T00:00:00Z",
    "end":   "2026-02-11T00:00:00Z"
  }
If not provided, uses Airflow data_interval_start/end.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import pendulum
import clickhouse_connect
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


# ----------------------------
# Config (match your setup)
# ----------------------------
POSTGRES_CONN_ID = "comm_db"          # you said this is your Airflow Postgres connection id
CLICKHOUSE_HOST = "host.docker.internal"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "gvantsa"       # set yours
CLICKHOUSE_DB = "default"

CH_RAW_TABLE = "raw_events"

METRICS = ("cpu_usage", "memory_usage", "request_latency")


# ----------------------------
# Helpers
# ----------------------------
def _to_utc_dt(x: Any) -> datetime:
    """
    Convert Airflow/Pendulum/ISO string to timezone-aware UTC datetime.
    """
    if x is None:
        raise ValueError("Datetime value is None")

    if isinstance(x, datetime):
        dt = x
    else:
        # assume string
        dt = pendulum.parse(str(x))

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _get_interval(context: Dict[str, Any]) -> Tuple[datetime, datetime]:
    """
    Prefer dag_run.conf overrides; else use Airflow data_interval_start/end.
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    if "start" in conf and "end" in conf:
        start = _to_utc_dt(conf["start"])
        end = _to_utc_dt(conf["end"])
    else:
        # Airflow 2.10: these exist and are the correct interval to use
        start = _to_utc_dt(context["data_interval_start"])
        end = _to_utc_dt(context["data_interval_end"])

    if end <= start:
        raise ValueError(f"Invalid interval: start={start.isoformat()} end={end.isoformat()}")

    return start, end


def _get_pg_conn():
    """
    Use Airflow connection comm_db.
    """
    c = BaseHook.get_connection(POSTGRES_CONN_ID)
    return psycopg2.connect(
        host=c.host,
        port=c.port or 5432,
        dbname=c.schema or "postgres",
        user=c.login,
        password=c.password,
    )


def _get_ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


# ----------------------------
# Core tasks
# ----------------------------
def compute_interval_aggregates(**context) -> List[Dict[str, Any]]:
    """
    Query ClickHouse raw_events within [start, end) by ingest_time (fast for ingestion-based intervals).
    Returns list of dict rows: source_id, metric, window_start, window_end, n2, mean2, var2
    """
    start, end = _get_interval(context)

    query = f"""
        SELECT
            source_id,
            metric,
            toDateTime64({{start:DateTime64(3)}}, 3, 'UTC') AS window_start,
            toDateTime64({{end:DateTime64(3)}}, 3, 'UTC') AS window_end,
            uniqExact(event_id) AS n2,
            avg(value) AS mean2,
            varPop(value) AS var2
        FROM {CH_RAW_TABLE}
        WHERE ingest_time >= toDateTime64({{start:DateTime64(3)}}, 3, 'UTC')
          AND ingest_time <  toDateTime64({{end:DateTime64(3)}}, 3, 'UTC')
          AND metric IN {METRICS}
        GROUP BY source_id, metric
    """

    client = _get_ch_client()
    res = client.query(
        query,
        parameters={
            "start": start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "end": end.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        },
    )

    cols = list(res.column_names)
    rows = [dict(zip(cols, r)) for r in res.result_rows]

    print(f"[baseline] interval {start.isoformat()} -> {end.isoformat()} | clickhouse groups: {len(rows)}")

    # Helpful: show 1 sample row
    if rows:
        print(f"[baseline] sample row: {rows[0]}")

    # XCom return
    return rows


def upsert_baseline_and_sources(**context) -> None:
    """
    Option B: cumulative baseline merge.
    Also updates source_metadata (upsert).
    """
    ti = context["ti"]
    rows: List[Dict[str, Any]] = ti.xcom_pull(task_ids="compute_interval_aggregates") or []

    if not rows:
        print("[baseline] no aggregates returned; nothing to upsert.")
        return

    # Collect sources to upsert into source_metadata
    sources_seen = sorted({r["source_id"] for r in rows})

    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            # 1) Upsert sources into source_metadata
            # Mark seen sources active and update last_seen_at
            execute_values(
                cur,
                """
                INSERT INTO source_metadata (source_id, source_type, first_seen_at, last_seen_at, is_active)
                VALUES %s
                ON CONFLICT (source_id) DO UPDATE
                SET last_seen_at = EXCLUDED.last_seen_at,
                    is_active = TRUE
                """,
                [(sid, "dynamic", datetime.now(timezone.utc), datetime.now(timezone.utc), True) for sid in sources_seen],
                page_size=1000,
            )
            print(f"[sources] upserted/updated: {len(sources_seen)}")

            # 2) Fetch existing baselines for keys we will update
            keys = [(r["source_id"], r["metric"]) for r in rows]
            # de-dup keys
            keys = list(dict.fromkeys(keys))

            execute_values(
                cur,
                """
                SELECT source_id, metric::text, baseline_count, baseline_mean, baseline_std, window_start, window_end
                FROM baseline_stats
                WHERE (source_id, metric::text) IN %s
                """,
                [tuple(keys)],
                template=None,
                page_size=2000,
            )
            existing = {}
            for (source_id, metric, n1, mean1, std1, wstart1, wend1) in cur.fetchall():
                existing[(source_id, metric)] = {
                    "n1": int(n1),
                    "mean1": float(mean1),
                    "var1": float(std1) * float(std1),
                    "wstart1": wstart1,
                    "wend1": wend1,
                }

            # 3) Merge and upsert baseline_stats
            upsert_rows = []
            for r in rows:
                source_id = r["source_id"]
                metric = r["metric"]

                n2 = int(r["n2"] or 0)
                if n2 <= 0:
                    continue

                mean2 = float(r["mean2"] or 0.0)
                var2 = float(r["var2"] or 0.0)

                window_start = r["window_start"]
                window_end = r["window_end"]

                old = existing.get((source_id, metric))
                if old and old["n1"] > 0:
                    n1 = old["n1"]
                    mean1 = old["mean1"]
                    var1 = old["var1"]

                    n = n1 + n2
                    mean = (n1 * mean1 + n2 * mean2) / n

                    # Merge variances (population)
                    # M2_total = n1*(var1 + (mean1-mean)^2) + n2*(var2 + (mean2-mean)^2)
                    m2_total = n1 * (var1 + (mean1 - mean) ** 2) + n2 * (var2 + (mean2 - mean) ** 2)
                    var = m2_total / n
                    std = math.sqrt(max(var, 0.0))

                    wstart = min([d for d in [old["wstart1"], window_start] if d is not None])
                    wend = max([d for d in [old["wend1"], window_end] if d is not None])
                else:
                    # First baseline for this key
                    n = n2
                    mean = mean2
                    std = math.sqrt(max(var2, 0.0))
                    wstart = window_start
                    wend = window_end

                upsert_rows.append((source_id, metric, n, mean, std, wstart, wend))

            if not upsert_rows:
                print("[baseline] nothing to upsert after filtering (n2=0).")
                return

            execute_values(
                cur,
                """
                INSERT INTO baseline_stats
                    (source_id, metric, baseline_count, baseline_mean, baseline_std, window_start, window_end, updated_at)
                VALUES %s
                ON CONFLICT (source_id, metric) DO UPDATE
                SET baseline_count = EXCLUDED.baseline_count,
                    baseline_mean  = EXCLUDED.baseline_mean,
                    baseline_std   = EXCLUDED.baseline_std,
                    window_start   = LEAST(baseline_stats.window_start, EXCLUDED.window_start),
                    window_end     = GREATEST(baseline_stats.window_end, EXCLUDED.window_end),
                    updated_at     = NOW()
                """,
                [(sid, m, n, mean, std, ws, we, datetime.now(timezone.utc)) for (sid, m, n, mean, std, ws, we) in upsert_rows],
                page_size=2000,
            )

            print(f"[baseline] upserted baseline rows: {len(upsert_rows)}")

        conn.commit()


# ----------------------------
# DAG definition
# ----------------------------
with DAG(
    dag_id="baseline_clickhouse_to_postgres",
    start_date=pendulum.datetime(2026, 2, 1, tz="UTC"),
    schedule="@daily",          # best default; you can still trigger manually anytime
    catchup=False,
    tags=["midterm", "baseline", "clickhouse", "postgres"],
    default_args={"owner": "gvantsa"},
) as dag:

    t1 = PythonOperator(
        task_id="compute_interval_aggregates",
        python_callable=compute_interval_aggregates,
    )

    t2 = PythonOperator(
        task_id="upsert_baseline_and_sources",
        python_callable=upsert_baseline_and_sources,
    )

    t1 >> t2
