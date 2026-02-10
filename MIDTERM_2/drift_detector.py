"""
Drift Detector Service (Kafka -> Rolling Stats -> Drift State -> ClickHouse)

What this service does:
1) Loads thresholds (per metric) and baseline stats (per source_id + metric) from PostgreSQL.
2) Keeps an in-memory cache of those configs and refreshes periodically in a background thread.
3) Consumes telemetry events from Kafka (multiple producers) with stable partitioning by source_id.
4) Handles:
   - duplicates (deterministic event_id + TTL cache)
   - late events (allowed lateness per metric; ignore too-late for rolling stats)
5) Maintains rolling window statistics per (source_id, metric) efficiently:
   - O(1) update + O(1) eviction using deque + running sums
6) Detects drift using:
   - mean shift z-score: |mu_roll - mu_base| / (sigma_base + eps)
   - variance ratio: sigma_roll / (sigma_base + eps)
   - sustained logic (N consecutive hits to enter/exit DRIFTING)
7) Writes:
   - drift_events (append-only transitions)
   - drift_status_current (latest state per key, written frequently)

Cold start fix (Option A):
- If Postgres baseline doesn't exist for (source_id, metric), we "bootstrap" a baseline in-memory
  from the first stable rolling window after min_samples is reached. Drift detection then works
  immediately, until Airflow later writes real historical baselines into Postgres.

Run:
  python drift_detector.py --topic telemetry.events
"""

import argparse
import hashlib
import json
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from confluent_kafka import Consumer, KafkaError

import clickhouse_connect


# --------------------------------------------------------------------------------------
# 1) Configuration (connections + defaults)
# --------------------------------------------------------------------------------------

KAFKA_BOOTSTRAP = "localhost:19092,localhost:19094,localhost:19096"

# PostgreSQL connection (adjust as needed)
PG_DSN = "host=localhost port=5432 dbname=postgres user=gvantsa password=gvantsa"

# ClickHouse connection (adjust as needed)
CH_HOST = "localhost"
CH_PORT = 8123
CH_USER = "default"
CH_PASSWORD = "gvantsa"
CH_DATABASE = "default"

DEFAULT_TOPIC = "telemetry.events"
DEFAULT_GROUP_ID = "drift-detector-v1"

# Refresh cadence for Postgres configs (thresholds + baseline stats)
DEFAULT_CONFIG_REFRESH_SECONDS = 60

# Dedupe TTL should be > max lateness + typical jitter
DEFAULT_DEDUPE_TTL_SECONDS = 600

EPS = 1e-9


# --------------------------------------------------------------------------------------
# 2) Data models for configs (thresholds + baseline)
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class MetricThreshold:
    metric: str
    rolling_window_seconds: int
    eval_interval_seconds: int
    min_samples: int
    max_lateness_seconds: int
    z_threshold: float
    std_ratio_threshold: float
    enter_consecutive_hits: int
    exit_consecutive_hits: int
    enabled: bool


@dataclass(frozen=True)
class BaselineStat:
    baseline_count: int
    baseline_mean: float
    baseline_std: float
    updated_at: Optional[datetime]


# --------------------------------------------------------------------------------------
# 3) Efficient rolling window stats per key
# --------------------------------------------------------------------------------------

@dataclass
class RollingWindowStats:
    """
    Rolling stats maintained with:
    - deque of (event_time_epoch_seconds, value)
    - running sum, sumsq, count

    This supports O(1) insertion and eviction (from the left).
    """
    window_seconds: int
    points: Deque[Tuple[float, float]]
    n: int
    s: float
    ss: float

    last_eval_epoch: float

    def __init__(self, window_seconds: int):
        self.window_seconds = window_seconds
        self.points = deque()
        self.n = 0
        self.s = 0.0
        self.ss = 0.0
        self.last_eval_epoch = 0.0

    def add(self, t_epoch: float, value: float) -> None:
        self.points.append((t_epoch, value))
        self.n += 1
        self.s += value
        self.ss += value * value

    def evict_old(self, now_epoch: float) -> None:
        cutoff = now_epoch - self.window_seconds
        while self.points and self.points[0][0] < cutoff:
            _, v = self.points.popleft()
            self.n -= 1
            self.s -= v
            self.ss -= v * v

    def mean_std(self) -> Tuple[float, float]:
        if self.n <= 0:
            return 0.0, 0.0
        mean = self.s / self.n
        var = max(0.0, (self.ss / self.n) - (mean * mean))
        std = var ** 0.5
        return mean, std


@dataclass
class DriftState:
    """
    State machine per (source_id, metric).

    Cold start handling:
    - Before Postgres baseline exists, we "bootstrap" a baseline from the first stable rolling window
      once we have enough samples (min_samples). This allows drift detection to work immediately.
    """
    state: str  # INITIALIZING | NORMAL | DRIFTING
    drift_started_at: Optional[datetime]
    drift_ended_at: Optional[datetime]

    consecutive_hits: int
    consecutive_misses: int

    max_event_time_epoch: float  # watermark-ish per key

    # --- Option A bootstrap baseline (used only when Postgres baseline missing) ---
    bootstrap_ready: bool
    bootstrap_mean: float
    bootstrap_std: float
    bootstrap_count: int
    bootstrap_set_at: Optional[datetime]


# --------------------------------------------------------------------------------------
# 4) Dedupe cache (TTL Set)
# --------------------------------------------------------------------------------------

class TTLSet:
    def __init__(self, ttl_seconds: int):
        self.ttl_seconds = ttl_seconds
        self._store: Dict[str, float] = {}
        self._lock = threading.Lock()

    def add_if_new(self, key: str, now_epoch: float) -> bool:
        exp = now_epoch + self.ttl_seconds
        with self._lock:
            existing = self._store.get(key)
            if existing is not None and existing > now_epoch:
                return False
            self._store[key] = exp
            return True

    def purge(self, now_epoch: float) -> None:
        with self._lock:
            to_del = [k for k, exp in self._store.items() if exp <= now_epoch]
            for k in to_del:
                del self._store[k]


# --------------------------------------------------------------------------------------
# 5) Config cache + background refresh
# --------------------------------------------------------------------------------------

class ConfigCache:
    def __init__(self, pg_dsn: str):
        self.pg_dsn = pg_dsn
        self._lock = threading.Lock()
        self.thresholds: Dict[str, MetricThreshold] = {}
        self.baseline: Dict[Tuple[str, str], BaselineStat] = {}

    def refresh_from_postgres(self) -> None:
        conn = psycopg2.connect(self.pg_dsn)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
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
                """)
                thr_rows = cur.fetchall()

                new_thresholds: Dict[str, MetricThreshold] = {}
                for r in thr_rows:
                    mt = MetricThreshold(
                        metric=r["metric"],
                        rolling_window_seconds=int(r["rolling_window_seconds"]),
                        eval_interval_seconds=int(r["eval_interval_seconds"]),
                        min_samples=int(r["min_samples"]),
                        max_lateness_seconds=int(r["max_lateness_seconds"]),
                        z_threshold=float(r["z_threshold"]),
                        std_ratio_threshold=float(r["std_ratio_threshold"]),
                        enter_consecutive_hits=int(r["enter_consecutive_hits"]),
                        exit_consecutive_hits=int(r["exit_consecutive_hits"]),
                        enabled=bool(r["enabled"]),
                    )
                    new_thresholds[mt.metric] = mt

                cur.execute("""
                    SELECT
                        source_id,
                        metric::text AS metric,
                        baseline_count,
                        baseline_mean,
                        baseline_std,
                        updated_at
                    FROM baseline_stats
                """)
                base_rows = cur.fetchall()

                new_baseline: Dict[Tuple[str, str], BaselineStat] = {}
                for r in base_rows:
                    key = (r["source_id"], r["metric"])
                    new_baseline[key] = BaselineStat(
                        baseline_count=int(r["baseline_count"]),
                        baseline_mean=float(r["baseline_mean"]),
                        baseline_std=float(r["baseline_std"]),
                        updated_at=r["updated_at"],
                    )

            with self._lock:
                self.thresholds = new_thresholds
                self.baseline = new_baseline

            print(f"[config] refreshed: thresholds={len(new_thresholds)} baseline_keys={len(new_baseline)}")
        finally:
            conn.close()

    def get_threshold(self, metric: str) -> Optional[MetricThreshold]:
        with self._lock:
            return self.thresholds.get(metric)

    def get_baseline(self, source_id: str, metric: str) -> Optional[BaselineStat]:
        with self._lock:
            return self.baseline.get((source_id, metric))


def start_config_refresher(cache: ConfigCache, refresh_seconds: int) -> threading.Thread:
    def loop():
        while True:
            try:
                cache.refresh_from_postgres()
            except Exception as e:
                print(f"[config] refresh failed: {e!r}")
            time.sleep(refresh_seconds)

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t


# --------------------------------------------------------------------------------------
# 6) ClickHouse writer (batching)
# --------------------------------------------------------------------------------------

class ClickHouseWriter:
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.DRIFT_EVENTS_COLS = [
            "source_id", "metric", "drift_event_type", "drift_event_id",
            "drift_started_at", "drift_ended_at", "detected_at",
            "z_score", "std_ratio",
            "rolling_mean", "rolling_std", "rolling_count",
            "baseline_mean", "baseline_std", "baseline_count",
        ]

        self.DRIFT_STATUS_COLS = [
            "source_id", "metric", "state",
            "drift_started_at", "drift_ended_at",
            "last_score_z", "last_score_ratio",
            "last_updated_at",
        ]

        self.client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database=database
        )
        self._lock = threading.Lock()
        self._events_batch: List[Dict[str, Any]] = []
        self._status_batch: List[Dict[str, Any]] = []
        self._last_flush = time.time()

    def enqueue_drift_event(self, row: Dict[str, Any]) -> None:
        with self._lock:
            self._events_batch.append(row)

    def enqueue_status(self, row: Dict[str, Any]) -> None:
        with self._lock:
            self._status_batch.append(row)

    def flush_if_needed(self, max_batch: int = 5000, flush_interval_s: float = 1.0) -> None:
        now = time.time()
        with self._lock:
            need = (
                len(self._events_batch) >= max_batch
                or len(self._status_batch) >= max_batch
                or (now - self._last_flush) >= flush_interval_s
            )
            if not need:
                return

            events = self._events_batch
            status = self._status_batch
            self._events_batch = []
            self._status_batch = []
            self._last_flush = now

        try:
            if events:
                event_rows = [tuple(e.get(c) for c in self.DRIFT_EVENTS_COLS) for e in events]
                self.client.insert("drift_events", event_rows, column_names=self.DRIFT_EVENTS_COLS)

            if status:
                status_rows = [tuple(s.get(c) for c in self.DRIFT_STATUS_COLS) for s in status]
                self.client.insert("drift_status_current", status_rows, column_names=self.DRIFT_STATUS_COLS)

        except Exception as e:
            print(f"[clickhouse] insert failed: {e!r} (events={len(events)} status={len(status)})")

    def load_existing_drifting(self) -> Dict[Tuple[str, str], DriftState]:
        result: Dict[Tuple[str, str], DriftState] = {}
        query = """
            SELECT source_id, metric, state, drift_started_at, drift_ended_at
            FROM drift_status_current
            WHERE state = 'DRIFTING'
        """
        try:
            rows = self.client.query(query).result_rows
        except Exception as e:
            print(f"[clickhouse] failed to load existing drift status: {e!r}")
            return result

        for source_id, metric, state, started_at, ended_at in rows:
            result[(source_id, metric)] = DriftState(
                state=state,
                drift_started_at=started_at,
                drift_ended_at=ended_at,
                consecutive_hits=0,
                consecutive_misses=0,
                max_event_time_epoch=0.0,

                bootstrap_ready=False,
                bootstrap_mean=0.0,
                bootstrap_std=0.0,
                bootstrap_count=0,
                bootstrap_set_at=None,
            )

        print(f"[restart] recovered drifting keys: {len(result)}")
        return result


# --------------------------------------------------------------------------------------
# 7) Drift logic
# --------------------------------------------------------------------------------------

def parse_event_time_iso(s: str) -> float:
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt.timestamp()


def make_event_id(source_id: str, metric: str, event_time_iso: str, value: float) -> str:
    payload = f"{source_id}|{metric}|{event_time_iso}|{round(value, 4)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_scores(roll_mean: float, roll_std: float, base_mean: float, base_std: float) -> Tuple[float, float]:
    z = abs(roll_mean - base_mean) / (base_std + EPS)
    r = roll_std / (base_std + EPS)
    return z, r


def drift_condition(z_score: float, std_ratio: float, thr: MetricThreshold) -> bool:
    return (z_score > thr.z_threshold) or (std_ratio > thr.std_ratio_threshold)


def dt_utc_from_epoch(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def make_drift_event_id(source_id: str, metric: str, drift_started_at: datetime, event_type: str) -> str:
    started_iso = drift_started_at.isoformat(timespec="seconds")
    raw = f"{source_id}|{metric}|{event_type}|{started_iso}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------------------
# 8) Main Kafka consumption loop
# --------------------------------------------------------------------------------------

def run_detector(topic: str, group_id: str, config_refresh_seconds: int, dedupe_ttl_seconds: int) -> None:
    cfg = ConfigCache(PG_DSN)
    cfg.refresh_from_postgres()
    start_config_refresher(cfg, config_refresh_seconds)

    ch = ClickHouseWriter(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )

    rolling: Dict[Tuple[str, str], RollingWindowStats] = {}
    drift_state: Dict[Tuple[str, str], DriftState] = {}

    drift_state.update(ch.load_existing_drifting())

    dedupe = TTLSet(ttl_seconds=dedupe_ttl_seconds)

    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "earliest",

        "enable.auto.commit": True,
        "fetch.min.bytes": 1_000_000,
        "fetch.wait.max.ms": 50,
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms": 10_000,
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    last_dedupe_purge = time.time()

    print(f"[detector] consuming topic={topic} group_id={group_id}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            now_epoch = time.time()

            if now_epoch - last_dedupe_purge >= 5.0:
                dedupe.purge(now_epoch)
                last_dedupe_purge = now_epoch

            ch.flush_if_needed(max_batch=5000, flush_interval_s=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[kafka] error: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value())
                source_id = str(payload["source_id"])
                metric = str(payload["metric"])
                value = float(payload["value"])
                event_time_iso = str(payload["event_time"])
            except Exception:
                continue

            thr = cfg.get_threshold(metric)
            if thr is None or not thr.enabled:
                continue

            eid = make_event_id(source_id, metric, event_time_iso, value)
            if not dedupe.add_if_new(eid, now_epoch):
                continue

            try:
                event_time_epoch = parse_event_time_iso(event_time_iso)
            except Exception:
                continue

            key = (source_id, metric)

            st = drift_state.get(key)
            if st is None:
                st = DriftState(
                    state="INITIALIZING",
                    drift_started_at=None,
                    drift_ended_at=None,
                    consecutive_hits=0,
                    consecutive_misses=0,
                    max_event_time_epoch=0.0,

                    bootstrap_ready=False,
                    bootstrap_mean=0.0,
                    bootstrap_std=0.0,
                    bootstrap_count=0,
                    bootstrap_set_at=None,
                )
                drift_state[key] = st

            if event_time_epoch > st.max_event_time_epoch:
                st.max_event_time_epoch = event_time_epoch

            if event_time_epoch < (st.max_event_time_epoch - thr.max_lateness_seconds):
                continue

            rw = rolling.get(key)
            if rw is None:
                rw = RollingWindowStats(window_seconds=thr.rolling_window_seconds)
                rolling[key] = rw

            rw.add(event_time_epoch, value)
            rw.evict_old(event_time_epoch)

            if rw.n < thr.min_samples:
                if st.state != "INITIALIZING":
                    st.state = "INITIALIZING"
                    st.consecutive_hits = 0
                    st.consecutive_misses = 0

                    ch.enqueue_status({
                        "source_id": source_id,
                        "metric": metric,
                        "state": "INITIALIZING",
                        "drift_started_at": st.drift_started_at,
                        "drift_ended_at": st.drift_ended_at,
                        "last_score_z": None,
                        "last_score_ratio": None,
                        "last_updated_at": dt_utc_from_epoch(now_epoch),
                    })
                continue

            if (now_epoch - rw.last_eval_epoch) < thr.eval_interval_seconds:
                continue
            rw.last_eval_epoch = now_epoch

            roll_mean, roll_std = rw.mean_std()

            # -------------------------------
            # Option A: bootstrap baseline
            # -------------------------------
            if not st.bootstrap_ready:
                st.bootstrap_ready = True
                st.bootstrap_mean = roll_mean
                st.bootstrap_std = max(roll_std, 1e-3)
                st.bootstrap_count = int(rw.n)
                st.bootstrap_set_at = dt_utc_from_epoch(now_epoch)

                if st.state == "INITIALIZING":
                    st.state = "NORMAL"
                    st.consecutive_hits = 0
                    st.consecutive_misses = 0

            base = cfg.get_baseline(source_id, metric)
            if base is not None and base.baseline_count > 0:
                base_mean = base.baseline_mean
                base_std = base.baseline_std
                base_count = base.baseline_count
            else:
                # Use bootstrapped baseline until Postgres baseline exists
                base_mean = st.bootstrap_mean
                base_std = st.bootstrap_std
                base_count = st.bootstrap_count

            z, r = compute_scores(roll_mean, roll_std, base_mean, base_std)
            is_drift = drift_condition(z, r, thr)

            if st.state == "INITIALIZING":
                st.state = "NORMAL"
                st.consecutive_hits = 0
                st.consecutive_misses = 0

            if st.state == "NORMAL":
                if is_drift:
                    st.consecutive_hits += 1
                    st.consecutive_misses = 0
                    if st.consecutive_hits >= thr.enter_consecutive_hits:
                        st.state = "DRIFTING"
                        st.drift_started_at = dt_utc_from_epoch(event_time_epoch)
                        st.drift_ended_at = None
                        st.consecutive_hits = 0
                        st.consecutive_misses = 0

                        drift_event_id = make_drift_event_id(source_id, metric, st.drift_started_at, "DRIFT_START")
                        ch.enqueue_drift_event({
                            "source_id": source_id,
                            "metric": metric,
                            "drift_event_type": "DRIFT_START",
                            "drift_event_id": drift_event_id,
                            "drift_started_at": st.drift_started_at,
                            "drift_ended_at": None,
                            "detected_at": dt_utc_from_epoch(now_epoch),
                            "z_score": z,
                            "std_ratio": r,
                            "rolling_mean": roll_mean,
                            "rolling_std": roll_std,
                            "rolling_count": int(rw.n),
                            "baseline_mean": base_mean,
                            "baseline_std": base_std,
                            "baseline_count": int(base_count),
                        })
                else:
                    st.consecutive_hits = 0
                    st.consecutive_misses = 0

            elif st.state == "DRIFTING":
                if is_drift:
                    st.consecutive_misses = 0
                else:
                    st.consecutive_misses += 1
                    if st.consecutive_misses >= thr.exit_consecutive_hits:
                        st.state = "NORMAL"
                        st.drift_ended_at = dt_utc_from_epoch(event_time_epoch)
                        st.consecutive_hits = 0
                        st.consecutive_misses = 0

                        started_at = st.drift_started_at or dt_utc_from_epoch(event_time_epoch)
                        drift_event_id = make_drift_event_id(source_id, metric, started_at, "DRIFT_END")
                        ch.enqueue_drift_event({
                            "source_id": source_id,
                            "metric": metric,
                            "drift_event_type": "DRIFT_END",
                            "drift_event_id": drift_event_id,
                            "drift_started_at": started_at,
                            "drift_ended_at": st.drift_ended_at,
                            "detected_at": dt_utc_from_epoch(now_epoch),
                            "z_score": z,
                            "std_ratio": r,
                            "rolling_mean": roll_mean,
                            "rolling_std": roll_std,
                            "rolling_count": int(rw.n),
                            "baseline_mean": base_mean,
                            "baseline_std": base_std,
                            "baseline_count": int(base_count),
                        })

            ch.enqueue_status({
                "source_id": source_id,
                "metric": metric,
                "state": st.state,
                "drift_started_at": st.drift_started_at,
                "drift_ended_at": st.drift_ended_at,
                "last_score_z": z,
                "last_score_ratio": r,
                "last_updated_at": dt_utc_from_epoch(now_epoch),
            })

    except KeyboardInterrupt:
        print("[detector] stopping...")
    finally:
        try:
            ch.flush_if_needed(max_batch=0, flush_interval_s=0)
        except Exception:
            pass
        consumer.close()


# --------------------------------------------------------------------------------------
# 9) CLI entrypoint
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID)
    parser.add_argument("--config-refresh-seconds", type=int, default=DEFAULT_CONFIG_REFRESH_SECONDS)
    parser.add_argument("--dedupe-ttl-seconds", type=int, default=DEFAULT_DEDUPE_TTL_SECONDS)
    args = parser.parse_args()

    run_detector(
        topic=args.topic,
        group_id=args.group_id,
        config_refresh_seconds=args.config_refresh_seconds,
        dedupe_ttl_seconds=args.dedupe_ttl_seconds,
    )
