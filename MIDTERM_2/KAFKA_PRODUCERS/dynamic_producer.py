"""
Dynamic telemetry producer.

- Maintains an active pool of dynamic sources (default: ~1000).
- Periodically churns the pool:
    - retires churn_count active sources
    - adds churn_count brand new source IDs
- Emits baseline telemetry for active dyn sources.
- Pauses emission for sources taken over by drift producer.

Example:
  python dynamic_producer.py --eps 5000 --topic telemetry.events --active 1000 --churn-every 180 --churn-count 50
"""

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict, List

from confluent_kafka import Producer


KAFKA_CONF = {
    "bootstrap.servers": "localhost:19092,localhost:19094,localhost:19096",
    "client.id": "test-producer-dynamic",
    "acks": "all",
    "retries": 3,
    "linger.ms": 100,
    "compression.type": "snappy",

    "enable.idempotence": True,
    "batch.num.messages": 10000,
    "queue.buffering.max.messages": 1000000,
}

METRICS = ("cpu_usage", "memory_usage", "request_latency")


def utc_iso_now_ms() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


def gen_baseline_value(metric: str) -> float:
    if metric == "cpu_usage":
        return clamp01(random.gauss(0.30, 0.05))
    if metric == "memory_usage":
        return clamp01(random.gauss(0.55, 0.06))
    return max(1.0, random.gauss(120.0, 20.0))


def load_takeover_active(path: str) -> Dict[str, Dict]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def is_taken_over(source_id: str, takeover_cache: Dict[str, Dict], now_epoch: float) -> bool:
    entry = takeover_cache.get(source_id)
    if not entry:
        return False
    return float(entry.get("until_epoch", 0.0)) > now_epoch


def churn_sources(active: List[str], next_id: int, churn_count: int) -> int:
    """
    Replace churn_count currently active sources with brand new unique IDs.
    """
    if churn_count <= 0:
        return next_id
    churn_count = min(churn_count, len(active))

    to_retire = set(random.sample(active, churn_count))
    active[:] = [sid for sid in active if sid not in to_retire]

    for _ in range(churn_count):
        active.append(f"dyn_{next_id:06d}")
        next_id += 1

    return next_id


def run(topic: str, eps: int, takeover_file: str, active_count: int, churn_every: int, churn_count: int) -> None:
    producer = Producer(KAFKA_CONF)

    active_sources = [f"dyn_{i:06d}" for i in range(1, active_count + 1)]
    next_dyn_id = active_count + 1

    interval = 1.0 / max(1, eps)
    next_send = time.perf_counter()

    takeover_cache = {}
    takeover_reload_at = 0.0

    next_churn_at = time.time() + churn_every

    sent = 0
    report_every_s = 2.0
    last_report = time.time()

    while True:
        producer.poll(0)
        now = time.time()

        if now >= takeover_reload_at:
            takeover_cache = load_takeover_active(takeover_file)
            takeover_reload_at = now + 1.0

        if now >= next_churn_at:
            next_dyn_id = churn_sources(active_sources, next_dyn_id, churn_count)
            print(f"[dynamic] churned: active={len(active_sources)} next_id={next_dyn_id}")
            next_churn_at = now + churn_every

        source_id = random.choice(active_sources)

        if is_taken_over(source_id, takeover_cache, now_epoch=now):
            next_send += interval
            sleep_for = next_send - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
            continue

        metric = random.choice(METRICS)
        msg = {
            "source_id": source_id,
            "metric": metric,
            "value": float(gen_baseline_value(metric)),
            "event_time": utc_iso_now_ms(),
        }

        producer.produce(
            topic=topic,
            key=source_id.encode("utf-8"),
            value=json.dumps(msg).encode("utf-8"),
        )

        sent += 1
        if now - last_report >= report_every_s:
            print(f"[dynamic] eps~{sent/report_every_s:.0f} active={len(active_sources)} takeover_active={len(takeover_cache)}")
            sent = 0
            last_report = now

        next_send += interval
        sleep_for = next_send - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_send = time.perf_counter()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="telemetry.events")
    parser.add_argument("--eps", type=int, default=5000)
    parser.add_argument("--takeover-file", default="./takeover_active.json")
    parser.add_argument("--active", type=int, default=1000)
    parser.add_argument("--churn-every", type=int, default=180)
    parser.add_argument("--churn-count", type=int, default=50)
    args = parser.parse_args()

    try:
        run(
            topic=args.topic,
            eps=args.eps,
            takeover_file=args.takeover_file,
            active_count=args.active,
            churn_every=args.churn_every,
            churn_count=args.churn_count,
        )
    except KeyboardInterrupt:
        pass
