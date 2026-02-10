"""
Drift controller.

- Does NOT write to Kafka.
- Periodically updates takeover_active.json with a set of sources that will be "taken over"
  by the drift producer for a short time window.
- This enables the drift producer to emit events using the SAME schema as baseline producers,
  without mixing two producers for the same source at the same time.

Example:
  python drift_controller.py --cycle 30 --drift-seconds 20 --takeover-count 20
"""

import argparse
import json
import os
import random
import time
from typing import Dict


def load_takeover(path: str) -> Dict[str, Dict]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_takeover(path: str, takeover: Dict[str, Dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(takeover, f)


def prune_expired(takeover: Dict[str, Dict]) -> Dict[str, Dict]:
    now = time.time()
    out = {}
    for sid, entry in takeover.items():
        try:
            if float(entry.get("until_epoch", 0.0)) > now:
                out[sid] = entry
        except Exception:
            continue
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--takeover-file", default="./takeover_active.json")

    # Frequent for demos: pick new drifting sources often
    parser.add_argument("--cycle", type=int, default=30)
    parser.add_argument("--drift-seconds", type=int, default=20)
    parser.add_argument("--takeover-count", type=int, default=20)

    args = parser.parse_args()

    stable_sources = [f"svc_{i:04d}" for i in range(1, 1001)]
    # Dynamic IDs are churned; controller chooses from a broad possible range
    possible_dyn_sources = [f"dyn_{i:06d}" for i in range(1, 20000)]

    drift_types = ["mean_shift", "variance_increase"]

    while True:
        takeover = prune_expired(load_takeover(args.takeover_file))

        # Choose mostly stable sources; include some dynamic occasionally
        stable_count = min(args.takeover_count, max(0, args.takeover_count - 5))
        dyn_count = args.takeover_count - stable_count

        chosen = random.sample(stable_sources, k=stable_count)
        if dyn_count > 0:
            chosen += random.sample(possible_dyn_sources, k=dyn_count)

        until = time.time() + args.drift_seconds
        drift_type = random.choice(drift_types)

        for sid in chosen:
            takeover[sid] = {"until_epoch": until, "drift_type": drift_type}

        save_takeover(args.takeover_file, takeover)
        print(f"[controller] drifting={len(chosen)} type={drift_type} for={args.drift_seconds}s")

        time.sleep(args.cycle)


if __name__ == "__main__":
    main()
