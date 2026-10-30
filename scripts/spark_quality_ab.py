"""A/B measurement of the Spark quality stage on identical Iceberg tables.

Runs the three checks that used `NOT IN (subquery)` in their original form (legacy variants, copied
verbatim below) against the current left-join rewrites, plus the full current quality stage. Same data,
same session, same machine; each variant is executed once because the legacy form is very slow.

    python scripts/spark_quality_ab.py --payments 100000 --out /out/spark_quality_ab.json
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from pathlib import Path

from settlelake.config import Paths
from settlelake.quality.checks import _checks
from settlelake.simulator.generator import GenConfig, generate


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payments", type=int, default=100_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    work = Path(tempfile.mkdtemp(prefix="qab-"))
    os.environ["SETTLELAKE_CATALOG"] = "local"
    os.environ["SETTLELAKE_WAREHOUSE"] = str(work / "warehouse")
    os.environ.setdefault("SPARK_DRIVER_MEMORY", "8g")
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "16")
    from settlelake.spark import engine
    from settlelake.spark.engine import BRONZE_EVENTS, BRONZE_STATEMENTS, run_quality, t
    from settlelake.spark.session import build_spark

    paths = Paths(work / "data")
    generate(paths, GenConfig(a.payments, a.seed, "low"))
    spark = build_spark("quality-ab")
    t0 = time.perf_counter()
    res = engine.run_batch(
        spark, paths.raw, run_id="ab-prep"
    )  # includes one quality run (current form)
    prep_s = time.perf_counter() - t0

    ev, st, rec = t("silver", "events"), t("silver", "statements"), t("gold", "reconciliation")
    fs, fp = t("gold", "fact_settlements"), t("gold", "fact_payments")
    legacy = {
        "gold_reconciliation_covers_silver": f"""SELECT count(*), count(*) FILTER (WHERE match_key NOT IN (SELECT match_key FROM {rec}))
             FROM (SELECT match_key FROM {ev} WHERE event_type = 'funds.settled'
                   UNION SELECT match_key FROM {st})""",
        "gold_referential_integrity": f"""SELECT count(*), count(*) FILTER (WHERE payment_id NOT IN
             (SELECT payment_id FROM {fp})) FROM {fs}""",
        "source_referential_integrity": f"""SELECT count(*), count(*) FILTER (WHERE payment_id NOT IN
             (SELECT payment_id FROM {ev} WHERE event_type = 'payment.created')) FROM {ev}""",
    }
    tables = {"events": ev, "statements": st, "rej": t("silver", "rejected"), "qua": t("silver", "quarantine"),
              "dup": t("silver", "duplicates"), "rec": rec, "fs": fs, "fp": fp,
              "bronze": f"(SELECT 1 FROM {BRONZE_EVENTS} UNION ALL SELECT 1 FROM {BRONZE_STATEMENTS})"}  # fmt: skip
    current = {name: sql for name, _l, _s, _e, sql in _checks(tables) if name in legacy}

    def timed(sql: str):
        t1 = time.perf_counter()
        row = tuple(spark.sql(sql).collect()[0])
        return round(time.perf_counter() - t1, 2), row

    out = {"payments": a.payments, "prep_batch_seconds": round(prep_s, 2), "checks": {}}
    for name in legacy:
        old_s, old_row = timed(legacy[name])
        new_s, new_row = timed(current[name])
        out["checks"][name] = {"legacy_not_in_seconds": old_s, "left_join_seconds": new_s,
                               "same_result": old_row == new_row, "result": list(new_row)}  # fmt: skip
    t1 = time.perf_counter()
    run_quality(spark, "ab-current")
    out["current_quality_stage_seconds"] = round(time.perf_counter() - t1, 2)
    out["batch_stage_seconds_with_current_quality"] = res["timings"]
    out["legacy_three_checks_seconds"] = round(
        sum(c["legacy_not_in_seconds"] for c in out["checks"].values()), 2
    )
    out["current_three_checks_seconds"] = round(
        sum(c["left_join_seconds"] for c in out["checks"].values()), 2
    )
    Path(a.out).write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out))


if __name__ == "__main__":
    main()
