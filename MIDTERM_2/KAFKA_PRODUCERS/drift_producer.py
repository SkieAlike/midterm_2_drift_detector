"""
Drift telemetry producer.

- Emits events to the SAME Kafka topic and SAME JSON schema as baseline producers.
- Reads takeover_active.json and produces only for sources currently taken over.
- Generates drifted values (mean shift or variance increase) so drift is obvious.

Example:
  python drift_producer.py --eps 1000 --topic telemetry.events
"""

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from confluent_kafka import Producer


KAFKA_CONF = {
    "bootstrap.servers": "localhost:19092,localhost:19094,localhost:19096",
    "client.id": "test-producer-drift",
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


def load_takeover_active(path: str) -> Dict[str, Dict]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def active_taken_over_sources(takeover: Dict[str, Dict]) -> List[Tuple[str, str]]:
    """Return list of (source_id, drift_type) that are currently active."""
    now = time.time()
    out = []
    for sid, entry in takeover.items():
        try:
            if float(entry.get("until_epoch", 0.0)) > now:
                out.append((sid, str(entry.get("drift_type", "mean_shift"))))
        except Exception:
            continue
    return out


def gen_drift_value(metric: str, drift_type: str) -> float:
    """
    Drift patterns:
    - mean_shift: mean jumps
    - variance_increase: std increases while mean stays near baseline
    """
    if metric == "cpu_usage":
        if drift_type == "mean_shift":
            return clamp01(random.gauss(0.70, 0.06))
        return clamp01(random.gauss(0.30, 0.15))

    if metric == "memory_usage":
        if drift_type == "mean_shift":
            return clamp01(random.gauss(0.85, 0.05))
        return clamp01(random.gauss(0.55, 0.15))

    # request_latency
    if drift_type == "mean_shift":
        return max(1.0, random.gauss(260.0, 40.0))
    return max(1.0, random.gauss(120.0, 70.0))


def run(topic: str, eps: int, takeover_file: str) -> None:
    producer = Producer(KAFKA_CONF)

    interval = 1.0 / max(1, eps)
    next_send = time.perf_counter()

    takeover_cache = {}
    takeover_reload_at = 0.0

    sent = 0
    report_every_s = 2.0
    last_report = time.time()

    while True:
        producer.poll(0)
        now = time.time()

        if now >= takeover_reload_at:
            takeover_cache = load_takeover_active(takeover_file)
            takeover_reload_at = now + 1.0

        active = active_taken_over_sources(takeover_cache)
        if not active:
            time.sleep(0.2)
            continue

        source_id, drift_type = random.choice(active)
        metric = random.choice(METRICS)

        msg = {
            "source_id": source_id,
            "metric": metric,
            "value": float(gen_drift_value(metric, drift_type)),
            "event_time": utc_iso_now_ms(),
        }

        producer.produce(
            topic=topic,
            key=source_id.encode("utf-8"),
            value=json.dumps(msg).encode("utf-8"),
        )

        sent += 1
        if now - last_report >= report_every_s:
            print(f"[drift] eps~{sent/report_every_s:.0f} active_takeovers={len(active)}")
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
    parser.add_argument("--eps", type=int, default=1000)
    parser.add_argument("--takeover-file", default="./takeover_active.json")
    args = parser.parse_args()

    try:
        run(topic=args.topic, eps=args.eps, takeover_file=args.takeover_file)
    except KeyboardInterrupt:
        pass
