"""Prometheus text-format metrics written atomically to <home>/metrics/<component>.prom.

Intended for the node_exporter textfile collector (or any scraper that reads files). No metrics
server is bundled.
"""

from __future__ import annotations

import os
import time
from pathlib import Path


def _esc(v: object) -> str:
    return str(v).replace("\\", "\\\\").replace('"', '\\"')


def write_metrics(
    component: str,
    samples: list[tuple[str, dict, float]],
    home: Path | str | None = None,
) -> Path:
    """samples: (metric_name, labels, value). Rewrites the component's file atomically."""
    base = Path(home or os.environ.get("SETTLELAKE_HOME") or "data") / "metrics"
    base.mkdir(parents=True, exist_ok=True)
    samples = [
        *samples,
        ("settlelake_last_update_timestamp_seconds", {"component": component}, time.time()),
    ]
    lines, seen = [], set()
    for name, labels, value in samples:
        if name not in seen:
            seen.add(name)
            lines.append(f"# TYPE {name} gauge")
        lab = ",".join(f'{k}="{_esc(v)}"' for k, v in sorted(labels.items()))
        lines.append(f"{name}{{{lab}}} {value}" if lab else f"{name} {value}")
    target = base / f"{component}.prom"
    tmp = target.with_suffix(".tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    os.replace(tmp, target)
    return target
