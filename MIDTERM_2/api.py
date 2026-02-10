from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional

import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor

import clickhouse_connect

from fastapi import FastAPI, Query
from pydantic import BaseModel


# ------------------------------------------------------------------------------
# Environment / config
# ------------------------------------------------------------------------------

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "gvantsa")
PG_PASSWORD = os.getenv("PG_PASSWORD", "gvantsa")

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "gvantsa")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

CH_DRIFT_STATUS_TABLE = os.getenv("CH_DRIFT_STATUS_TABLE", "drift_status_current")
CH_DRIFT_EVENTS_TABLE = os.getenv("CH_DRIFT_EVENTS_TABLE", "drift_events")


# ------------------------------------------------------------------------------
# App + clients
# ------------------------------------------------------------------------------

app = FastAPI(
    title="Real-time Drift Monitoring API",
    version="1.0.0",
)

pg_pool: SimpleConnectionPool | None = None
ch_client: clickhouse_connect.driver.client.Client | None = None


# ------------------------------------------------------------------------------
# Startup / shutdown
# ------------------------------------------------------------------------------

@app.on_event("startup")
def startup() -> None:
    global pg_pool, ch_client

    pg_pool = SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    ch_client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )


@app.on_event("shutdown")
def shutdown() -> None:
    global pg_pool
    if pg_pool:
        pg_pool.closeall()


def get_pg_conn():
    assert pg_pool is not None
    return pg_pool.getconn()


def put_pg_conn(conn):
    assert pg_pool is not None
    pg_pool.putconn(conn)


def get_ch():
    assert ch_client is not None
    return ch_client


# ------------------------------------------------------------------------------
# Models
# ------------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str
    postgres: str
    clickhouse: str


# ------------------------------------------------------------------------------
# Root / health
# ------------------------------------------------------------------------------

@app.get("/")
def root():
    return {
        "service": "drift-monitoring-api",
        "status": "running",
        "docs": "/docs",
    }


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    # Postgres
    try:
        conn = get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
        put_pg_conn(conn)
        pg_ok = "ok"
    except Exception as e:
        pg_ok = f"error: {type(e).__name__}"

    # ClickHouse
    try:
        ch = get_ch()
        ch.query("SELECT 1")
        ch_ok = "ok"
    except Exception as e:
        ch_ok = f"error: {type(e).__name__}"

    status = "ok" if pg_ok == "ok" and ch_ok == "ok" else "degraded"
    return HealthResponse(status=status, postgres=pg_ok, clickhouse=ch_ok)


# ------------------------------------------------------------------------------
# PostgreSQL endpoints
# ------------------------------------------------------------------------------

@app.get("/thresholds")
def list_thresholds() -> list[dict[str, Any]]:
    sql = """
        SELECT
            metric::text AS metric,
            rolling_window_seconds,
            eval_interval_seconds,
            min_samples,
            max_lateness_seconds,
            z_threshold,
            std_ratio_threshold,
            enter_consecutive_hits,
            exit_consecutive_hits,
            enabled
        FROM metric_thresholds
        ORDER BY metric;
    """
    conn = get_pg_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        put_pg_conn(conn)


@app.get("/baselines")
def list_baselines(
    source_id: Optional[str] = None,
    metric: Optional[str] = None,
    limit: int = Query(200, ge=1, le=5000),
) -> list[dict[str, Any]]:
    where = []
    params: dict[str, Any] = {"limit": limit}

    if source_id:
        where.append("source_id = %(source_id)s")
        params["source_id"] = source_id
    if metric:
        where.append("metric::text = %(metric)s")
        params["metric"] = metric

    sql = f"""
        SELECT
            source_id,
            metric::text AS metric,
            baseline_count,
            baseline_mean,
            baseline_std,
            window_start,
            window_end,
            updated_at
        FROM baseline_stats
        {("WHERE " + " AND ".join(where)) if where else ""}
        ORDER BY updated_at DESC
        LIMIT %(limit)s;
    """

    conn = get_pg_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        put_pg_conn(conn)


@app.get("/sources")
def list_sources(
    active_only: bool = True,
    limit: int = Query(500, ge=1, le=5000),
) -> list[dict[str, Any]]:
    sql = """
        SELECT source_id, source_type, first_seen_at, last_seen_at, is_active
        FROM source_metadata
        WHERE (%(active_only)s = FALSE OR is_active = TRUE)
        ORDER BY last_seen_at DESC
        LIMIT %(limit)s;
    """

    conn = get_pg_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, {"active_only": active_only, "limit": limit})
            return cur.fetchall()
    finally:
        put_pg_conn(conn)


# ------------------------------------------------------------------------------
# ClickHouse endpoints
# ------------------------------------------------------------------------------

@app.get("/drift/status")
def drift_status(
    state: Optional[str] = Query(None, description="INITIALIZING | NORMAL | DRIFTING"),
    metric: Optional[str] = None,
    limit: int = Query(200, ge=1, le=5000),
):
    conditions = []
    params: dict[str, Any] = {"limit": limit}

    if state:
        conditions.append("state = {state:String}")
        params["state"] = state
    if metric:
        conditions.append("metric = {metric:String}")
        params["metric"] = metric

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    q = f"""
        SELECT
            source_id,
            metric,
            state,
            drift_started_at,
            drift_ended_at,
            last_score_z,
            last_score_ratio,
            last_updated_at
        FROM {CH_DRIFT_STATUS_TABLE}
        {where}
        ORDER BY last_updated_at DESC
        LIMIT {{limit:UInt32}}
    """

    ch = get_ch()
    res = ch.query(q, parameters=params)
    cols = res.column_names
    return [dict(zip(cols, row)) for row in res.result_rows]


@app.get("/drift/events")
def drift_events(
    source_id: Optional[str] = None,
    metric: Optional[str] = None,
    since: Optional[datetime] = Query(None),
    limit: int = Query(200, ge=1, le=5000),
):
    conditions = []
    params: dict[str, Any] = {"limit": limit}

    if source_id:
        conditions.append("source_id = {source_id:String}")
        params["source_id"] = source_id
    if metric:
        conditions.append("metric = {metric:String}")
        params["metric"] = metric
    if since:
        conditions.append("detected_at >= {since:DateTime64(3, 'UTC')}")
        params["since"] = since

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    q = f"""
        SELECT
            drift_event_id,
            source_id,
            metric,
            drift_event_type,
            drift_started_at,
            drift_ended_at,
            detected_at,
            z_score,
            std_ratio,
            rolling_mean,
            rolling_std,
            rolling_count,
            baseline_mean,
            baseline_std,
            baseline_count
        FROM {CH_DRIFT_EVENTS_TABLE}
        {where}
        ORDER BY detected_at DESC
        LIMIT {{limit:UInt32}}
    """

    ch = get_ch()
    res = ch.query(q, parameters=params)
    cols = res.column_names
    return [dict(zip(cols, row)) for row in res.result_rows]
