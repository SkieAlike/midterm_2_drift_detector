"""
Stable telemetry producer.

- Produces baseline telemetry events for a fixed set of stable sources:
    svc_0001 ... svc_1000
- Emits JSON events to a Kafka topic (default: telemetry.events).
- Uses source_id as Kafka message key for stable partitioning.
- Pauses emission for sources currently taken over by drift producer
  (as indicated by takeover_active.json).

Event schema:
{
  "source_id": "string",
  "metric": "cpu_usage | memory_usage | request_latency",
  "value": float,
  "event_time": "ISO-8601 timestamp"
}

Example:
  python stable_producer.py --eps 5000 --topic telemetry.events
"""

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict

from confluent_kafka import Producer


# ---- Kafka configuration (keep your existing connection settings) ----
KAFKA_CONF = {
    "bootstrap.servers": "localhost:19092,localhost:19094,localhost:19096",
    "client.id": "test-producer-stable",
    "acks": "all",
    "retries": 3,
    "linger.ms": 100,
    "compression.type": "snappy",

    # Safe throughput boosters (do not change cluster topology assumptions)
    "enable.idempotence": True,
    "batch.num.messages": 10000,
    "queue.buffering.max.messages": 1000000,
}

METRICS = ("cpu_usage", "memory_usage", "request_latency")


def utc_iso_now_ms() -> str:
    """UTC ISO-8601 timestamp with milliseconds and Z."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


def gen_baseline_value(metric: str) -> float:
    """
    Baseline distributions (simple and explainable):
    - cpu_usage: mean ~0.30, std ~0.05, clipped to [0, 1]
    - memory_usage: mean ~0.55, std ~0.06, clipped to [0, 1]
    - request_latency: mean ~120ms, std ~20ms, clipped to > 0
    """
    if metric == "cpu_usage":
        return clamp01(random.gauss(0.30, 0.05))
    if metric == "memory_usage":
        return clamp01(random.gauss(0.55, 0.06))
    return max(1.0, random.gauss(120.0, 20.0))


def load_takeover_active(path: str) -> Dict[str, Dict]:
    """
    Loads takeover file used for producer coordination:

    {
      "svc_0042": {"until_epoch": 1765400000.123, "drift_type": "mean_shift"},
      ...
    }

    If missing/corrupted => returns {}.
    """
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def is_taken_over(source_id: str, takeover_cache: Dict[str, Dict], now_epoch: float) -> bool:
    """
    A source is "taken over" if present in takeover file and until_epoch is in the future.
    """
    entry = takeover_cache.get(source_id)
    if not entry:
        return False
    return float(entry.get("until_epoch", 0.0)) > now_epoch


def run(topic: str, eps: int, takeover_file: str) -> None:
    producer = Producer(KAFKA_CONF)

    stable_sources = [f"svc_{i:04d}" for i in range(1, 1001)]

    # Rate control: target overall EPS from this producer.
    interval = 1.0 / max(1, eps)
    next_send = time.perf_counter()

    takeover_cache = {}
    takeover_reload_at = 0.0  # reload takeover file once per second

    sent = 0
    report_every_s = 2.0
    last_report = time.time()

    while True:
        # Serve delivery callbacks and allow batching to proceed
        producer.poll(0)

        now = time.time()

        # Refresh takeover cache periodically (avoids constant file IO)
        if now >= takeover_reload_at:
            takeover_cache = load_takeover_active(takeover_file)
            takeover_reload_at = now + 1.0

        source_id = random.choice(stable_sources)

        # If drift producer owns this source right now, skip producing baseline for it
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

        # Key by source_id => stable partitioning, ordering per source within partition
        producer.produce(
            topic=topic,
            key=source_id.encode("utf-8"),
            value=json.dumps(msg).encode("utf-8"),
        )

        sent += 1

        # Light progress reporting (never per-message)
        if now - last_report >= report_every_s:
            print(f"[stable] eps~{sent/report_every_s:.0f} takeover_active={len(takeover_cache)}")
            sent = 0
            last_report = now

        # Pacing
        next_send += interval
        sleep_for = next_send - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            # If we fall behind, resync schedule
            next_send = time.perf_counter()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="telemetry.events")
    parser.add_argument("--eps", type=int, default=5000)
    parser.add_argument("--takeover-file", default="./takeover_active.json")
    args = parser.parse_args()

    try:
        run(topic=args.topic, eps=args.eps, takeover_file=args.takeover_file)
    except KeyboardInterrupt:
        pass
