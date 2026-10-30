"""Deterministic synthetic data generator with ground-truth labelled failure injection.

Identical (payments, seed, anomalies) always yields byte-identical files. Ground truth is
written to a separate directory and is only read by the evaluation code, never by the pipeline.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import random
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from settlelake.config import (
    ANOMALY_LEVELS,
    CORRIDORS,
    CURRENCIES,
    PROVIDERS,
    Paths,
)
from settlelake.schemas.models import make_match_key
from settlelake.schemas.money import convert_minor, format_minor

BASE_TIME = datetime(2026, 1, 1)
SOURCE = "payments-core"
H = 3600

# Fraction of settled payments assigned to each scenario, per anomaly level.
_LOW = {
    "AMOUNT_MISMATCH": 0.008,
    "CURRENCY_MISMATCH": 0.004,
    "STATUS_MISMATCH": 0.004,
    "DELAYED": 0.006,
    "AMOUNT_AND_DELAYED": 0.002,
    "MISSING_PARTNER": 0.008,
    "DUP_STATEMENT": 0.006,
    "DUP_EVENT": 0.006,
    "INVALID_EVENT": 0.003,
    "INVALID_STATEMENT": 0.003,
    "CONFLICT_EVENT": 0.002,
    "AMBIGUOUS": 0.004,
    "LATE_EVENT": 0.01,
}
RATES = {
    "clean": {},
    "low": _LOW,
    "stress": {k: v * 5 for k, v in _LOW.items()},
}
GHOST_RATE = {"clean": 0.0, "low": 0.004, "stress": 0.02}  # partner-only rows
MALFORMED_RATE = {"clean": 0.0, "low": 0.003, "stress": 0.015}  # garbage lines per file kind
OUT_OF_ORDER_RATE = {"clean": 0.0, "low": 0.05, "stress": 0.3}
OUTSTANDING_RATE = 0.04  # payments that never reach funds.settled (normal, not an anomaly)

EVENT_SUFFIX = {
    "payment.created": "CRE",
    "compliance.cleared": "CLR",
    "funds.settled": "SET",
    "payout.delivered": "DEL",
}
EXPECTED = {
    "AMOUNT_MISMATCH": ["AMOUNT_MISMATCH"],
    "CURRENCY_MISMATCH": ["CURRENCY_MISMATCH"],
    "STATUS_MISMATCH": ["STATUS_MISMATCH"],
    "DELAYED": ["DELAYED_SETTLEMENT"],
    "AMOUNT_AND_DELAYED": ["AMOUNT_MISMATCH", "DELAYED_SETTLEMENT"],
    "MISSING_PARTNER": ["MISSING_PARTNER_RECORD"],
    "DUP_STATEMENT": ["DUPLICATE_STATEMENT"],
    "DUP_EVENT": ["DUPLICATE_EVENT"],
    "INVALID_EVENT": ["INVALID_RECORD"],
    "INVALID_STATEMENT": ["INVALID_RECORD"],
    "CONFLICT_EVENT": ["INVALID_RECORD"],
    "AMBIGUOUS": ["AMBIGUOUS_MATCH"],
    "LATE_EVENT": ["MATCHED"],
    "CLEAN": ["MATCHED"],
    "GHOST": ["MISSING_INTERNAL_RECORD"],
}


@dataclass(frozen=True)
class GenConfig:
    payments: int
    seed: int = 42
    anomalies: str = "low"

    def __post_init__(self) -> None:
        if self.anomalies not in ANOMALY_LEVELS:
            raise ValueError(f"anomalies must be one of {ANOMALY_LEVELS}")
        if self.payments < 1:
            raise ValueError("payments must be >= 1")


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _dump(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _assign_scenarios(cfg: GenConfig, settled: list[int], rng: random.Random) -> dict[int, str]:
    plan: list[str] = []
    for name, rate in RATES[cfg.anomalies].items():
        n = max(1, round(rate * len(settled)))
        if name == "AMBIGUOUS" and n % 2:
            n += 1  # ambiguity needs pairs of payments sharing one provider reference
        plan += [name] * n
    plan = plan[: max(0, len(settled) // 2)]  # never let anomalies dominate tiny datasets
    if plan.count("AMBIGUOUS") % 2:
        plan.remove("AMBIGUOUS")
    chosen = sorted(rng.sample(settled, len(plan)))
    rng.shuffle(plan)
    return dict(zip(chosen, plan, strict=True))


def generate(paths: Paths, cfg: GenConfig) -> dict:
    """Write raw inputs to paths.raw and ground truth to paths.ground_truth. Returns manifest."""
    rng = random.Random(cfg.seed)
    level = cfg.anomalies
    n = cfg.payments
    currencies = sorted(CURRENCIES)
    providers = sorted(PROVIDERS)

    outstanding = {i for i in range(n) if rng.random() < OUTSTANDING_RATE}
    settled_idx = [i for i in range(n) if i not in outstanding]
    scenario_of = _assign_scenarios(cfg, settled_idx, rng)

    events: list[dict] = []
    stmts: dict[str, list[dict]] = {p: [] for p in providers}
    stmt_seq = Counter()
    truth: dict[str, dict] = {}
    counts: Counter = Counter()
    dup_event_ids: list[str] = []
    conflict_event_ids: list[str] = []
    amb_pending: tuple[str, str, str] | None = None  # (provider, ref, key)

    def emit_stmt(provider: str, ref: str, amount: int, cur: str, status: str, at: datetime):
        stmt_seq[provider] += 1
        # Partners format references inconsistently; the pipeline must normalize them.
        style = rng.randrange(4)
        ext = [ref, ref.lower(), f" {ref} ", ref.replace("-", "")][style]
        row = {
            "statement_row_id": f"{provider}-S{stmt_seq[provider]:08d}",
            "provider": provider,
            "external_ref": ext,
            "amount": format_minor(amount, cur),
            "currency": cur,
            "status": status,
            "settled_at": _ts(at),
        }
        stmts[provider].append(row)
        return row

    for i in range(n):
        pid = f"PAY-{i:08d}"
        src, dst = rng.choice(CORRIDORS)
        provider = rng.choice(providers)
        exp = CURRENCIES[src]
        amount = rng.randint(1_000, 2_000_000) if exp else rng.randint(1_000, 300_000)
        dest_amount = convert_minor(amount, src, dst)
        t_created = BASE_TIME + timedelta(seconds=rng.randrange(30 * 86400))
        t_cleared = t_created + timedelta(seconds=rng.randint(30, 1800))
        t_settled = t_cleared + timedelta(seconds=rng.randint(2 * H, 30 * H))
        t_delivered = t_settled + timedelta(seconds=rng.randint(600, 6 * H))
        corridor = f"{src}-{dst}"
        scenario = scenario_of.get(i, "CLEAN")
        ref = f"{provider[:3]}-REF-{i:08d}"
        reuse_amb = False
        if scenario == "AMBIGUOUS":
            if amb_pending is None:
                key = make_match_key(provider, ref)
            else:
                provider, ref, key = amb_pending
                reuse_amb = True
        else:
            key = make_match_key(provider, ref)

        def ev(etype, t, amt, cur, *, eid=None, ingest=None, with_ref=False, extra=None):
            ingested = ingest or t + timedelta(seconds=rng.randint(1, 60))
            d = {
                "event_id": eid or f"EVT-{i:08d}-{EVENT_SUFFIX[etype]}",
                "event_type": etype,
                "payment_id": pid,
                "event_time": _ts(t),
                "ingested_at": _ts(ingested),
                "schema_version": "1.0",
                "source": SOURCE,
                "provider": provider,
                "provider_ref": ref if with_ref else None,
                "amount_minor": amt,
                "currency": cur,
                "corridor": corridor,
                "dest_currency": None,
                "dest_amount_minor": None,
            }
            d.update(extra or {})
            return d

        events.append(
            ev(
                "payment.created",
                t_created,
                amount,
                src,
                extra={"dest_currency": dst, "dest_amount_minor": dest_amount},
            )
        )
        if i in outstanding and rng.random() < 0.5:
            continue  # stuck before compliance clearance
        events.append(ev("compliance.cleared", t_cleared, amount, src))
        if i in outstanding:
            continue

        late = scenario == "LATE_EVENT"
        ingest = t_settled + timedelta(seconds=rng.randint(3 * 86400, 10 * 86400)) if late else None
        settle_ev = ev("funds.settled", t_settled, amount, src, with_ref=True, ingest=ingest)
        if scenario == "DUP_EVENT":
            events.append(dict(settle_ev))
            dup_event_ids.append(settle_ev["event_id"])
        elif scenario == "CONFLICT_EVENT":
            other = dict(settle_ev)
            other["amount_minor"] = amount + rng.randint(1, 500)
            events.append(other)
            conflict_event_ids.append(settle_ev["event_id"])
        elif scenario == "INVALID_EVENT":
            variant = rng.randrange(4)
            if variant == 0:
                settle_ev["currency"] = "ZZZ"
            elif variant == 1:
                settle_ev["amount_minor"] = -amount
            elif variant == 2:
                del settle_ev["payment_id"]
            else:
                settle_ev["amount_minor"] = "n/a"
        events.append(settle_ev)
        events.append(ev("payout.delivered", t_delivered, dest_amount, dst))

        # ---- partner statement side ----
        p_amount, p_cur, p_status = amount, src, "SETTLED"
        p_time = t_settled + timedelta(seconds=rng.randint(600, 20 * H))
        if scenario in ("AMOUNT_MISMATCH", "AMOUNT_AND_DELAYED"):
            p_amount = amount + rng.choice([-1, 1]) * rng.randint(1, 500)
            if p_amount < 1 or p_amount == amount:
                p_amount = amount + 1
        if scenario == "CURRENCY_MISMATCH":
            p_cur = rng.choice([c for c in currencies if c != src])
        if scenario == "STATUS_MISMATCH":
            p_status = rng.choice(["REJECTED", "RETURNED", "PENDING"])
        if scenario in ("DELAYED", "AMOUNT_AND_DELAYED"):
            p_time = t_settled + timedelta(seconds=rng.randint(50 * H, 120 * H))

        if scenario == "MISSING_PARTNER" or reuse_amb:
            pass
        else:
            row = emit_stmt(provider, ref, p_amount, p_cur, p_status, p_time)
            if scenario == "DUP_STATEMENT":
                stmts[provider].append(dict(row))
            elif scenario == "INVALID_STATEMENT":
                variant = rng.randrange(3)
                if variant == 0:
                    row["currency"] = "ZZZ"
                elif variant == 1:
                    row["amount"] = "abc"
                else:
                    row["amount"] = "-" + row["amount"]
        if scenario == "AMBIGUOUS":
            amb_pending = None if reuse_amb else (provider, ref, key)

        counts[scenario] += 1
        if key not in truth:
            truth[key] = {
                "match_key": key,
                "payment_id": pid,
                "scenario": scenario,
                "expected_classes": EXPECTED[scenario],
            }

    if amb_pending is not None:  # unpaired ambiguity candidate degenerates to a clean match
        key = amb_pending[2]
        truth[key]["scenario"] = "CLEAN"
        truth[key]["expected_classes"] = EXPECTED["CLEAN"]
        counts["AMBIGUOUS"] -= 1
        counts["CLEAN"] += 1
    if counts["AMBIGUOUS"]:
        counts["AMBIGUOUS"] //= 2  # a pair of payments is one ambiguous key

    # ---- partner-only (ghost) rows ----
    for g in range(round(GHOST_RATE[level] * n) or (1 if level != "clean" else 0)):
        provider = rng.choice(providers)
        cur = rng.choice(currencies)
        ref = f"GHOST-REF-{g:08d}"
        at = BASE_TIME + timedelta(seconds=rng.randrange(30 * 86400))
        emit_stmt(provider, ref, rng.randint(1000, 500_000), cur, "SETTLED", at)
        key = make_match_key(provider, ref)
        truth[key] = {
            "match_key": key,
            "payment_id": None,
            "scenario": "GHOST",
            "expected_classes": EXPECTED["GHOST"],
        }
        counts["GHOST"] += 1

    # ---- event file ordering: by ingestion time, then out-of-order displacement ----
    events.sort(key=lambda e: (e["ingested_at"], e["event_id"]))
    event_lines = [_dump(e) for e in events]
    n_ooo = round(OUT_OF_ORDER_RATE[level] * len(event_lines))
    for _ in range(n_ooo):
        a, b = rng.randrange(len(event_lines)), rng.randrange(len(event_lines))
        event_lines[a], event_lines[b] = event_lines[b], event_lines[a]
    n_bad = round(MALFORMED_RATE[level] * len(event_lines)) or (1 if level != "clean" else 0)
    bad_json = ['{"event_id": "EVT-TRUNC', "not json at all", '{"event_id": ', "[1, 2"]
    for _ in range(n_bad):
        event_lines.insert(rng.randrange(len(event_lines) + 1), rng.choice(bad_json))

    # ---- write raw files ----
    paths.raw.mkdir(parents=True, exist_ok=True)
    paths.ground_truth.mkdir(parents=True, exist_ok=True)
    for old in [*paths.raw.glob("payment_events*.jsonl"), *paths.raw.glob("statements_*")]:
        old.unlink()  # raw/ is owned by the generator; stale files would pollute a new dataset
    ev_path = paths.raw / "payment_events.jsonl"
    ev_path.write_text("\n".join(event_lines) + "\n", encoding="utf-8", newline="\n")

    stmt_files: dict[str, int] = {}
    malformed_stmt = 0
    for provider, rows in stmts.items():
        rows.sort(key=lambda r: (r["settled_at"], r["statement_row_id"]))
        fmt = PROVIDERS[provider]
        if fmt == "csv":
            cols = list(rows[0]) if rows else [
                "statement_row_id", "provider", "external_ref", "amount",
                "currency", "status", "settled_at",
            ]  # fmt: skip
            buf = io.StringIO()
            w = csv.writer(buf, lineterminator="\n")
            w.writerow(cols)
            lines = []
            for r in rows:
                b = io.StringIO()
                csv.writer(b, lineterminator="").writerow([r[c] for c in cols])
                lines.append(b.getvalue())
            for _ in range(
                round(MALFORMED_RATE[level] * len(lines)) or (1 if level != "clean" else 0)
            ):
                lines.insert(
                    rng.randrange(len(lines) + 1), rng.choice(["oops,only,three", "x", ",,"])
                )
                malformed_stmt += 1
            text = ",".join(cols) + "\n" + "\n".join(lines) + "\n"
        else:
            body = ",\n".join("  " + _dump(r) for r in rows)
            head = f'{{"provider":"{provider}","statement_date":"2026-02-01","rows":['
            text = head + "\n" + body + "\n]}\n"
        path = paths.raw / f"statements_{provider}.{fmt}"
        path.write_text(text, encoding="utf-8", newline="\n")
        stmt_files[path.name] = len(rows)

    with (paths.ground_truth / "ground_truth.jsonl").open("w", encoding="utf-8", newline="\n") as f:
        for key in sorted(truth):
            f.write(_dump(truth[key]) + "\n")

    manifest = {
        "config": {"payments": n, "seed": cfg.seed, "anomalies": level},
        "event_records": len(event_lines),
        "statement_rows": stmt_files,
        "scenario_counts": dict(sorted(counts.items())),
        "outstanding_payments": len(outstanding),
        "malformed_lines": {"events": n_bad, "statements": malformed_stmt},
        "out_of_order_swaps": n_ooo,
        "duplicate_event_ids": sorted(dup_event_ids),
        "conflicting_event_ids": sorted(conflict_event_ids),
        "files": {p.name: _sha256(p) for p in sorted(paths.raw.glob("*")) if p.is_file()},
    }
    (paths.ground_truth / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )
    return manifest
