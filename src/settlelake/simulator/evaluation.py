"""Independent evaluation dataset for the reconciliation engine.

Unlike the operational generator (simulator/generator.py), this one is *adversarial*: it injects
situations where a matcher can be wrong (look-alike references, colliding amounts, clerical
reference errors) and labels every record individually. Labels are written to a separate directory
and are only read by `observability/reconcile_eval.py`; the pipeline never sees them.

All results measured against this data are results against controlled synthetic ground truth.
"""

from __future__ import annotations

import csv
import io
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from settlelake.config import CORRIDORS, CURRENCIES, PROVIDERS, STMT_COLS, Paths
from settlelake.schemas.models import make_match_key
from settlelake.schemas.money import format_minor

BASE = datetime(2026, 3, 1)
H = 3600
# (scenario, share of payments). Two-payment scenarios consume two payments per draw.
MIX = [
    ("CLEAN", 0.50),
    ("TYPO_1", 0.05),
    ("TYPO_2", 0.03),
    ("TYPO_COLLISION", 0.02),
    ("WRONG_TARGET", 0.01),
    ("LOOKALIKE", 0.02),
    ("AMOUNT", 0.03),
    ("CURRENCY", 0.02),
    ("STATUS", 0.02),
    ("DELAYED", 0.03),
    ("MISSING_PARTNER", 0.03),
    ("PARTNER_ONLY", 0.03),
    ("DUP_STATEMENT", 0.02),
    ("DUP_EVENT", 0.02),
    ("CONFLICT_EVENT", 0.01),
    ("LATE", 0.03),
    ("COMBINED_TYPO_AMOUNT", 0.02),
]
UNRESOLVED = {
    "AMOUNT_MISMATCH", "CURRENCY_MISMATCH", "STATUS_MISMATCH", "MISSING_PARTNER_RECORD",
    "MISSING_INTERNAL_RECORD", "AMBIGUOUS_MATCH", "INVALID_RECORD", "UNRESOLVED_PAIR",
}  # fmt: skip


@dataclass(frozen=True)
class EvalConfig:
    payments: int
    seed: int


def _ts(d: datetime) -> str:
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def _dump(o: dict) -> str:
    return json.dumps(o, sort_keys=True, separators=(",", ":"))


def _lev(a: str, b: str) -> int:
    """Reference Levenshtein used only to *verify* scenario construction in the generator."""
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def generate_eval(paths: Paths, cfg: EvalConfig) -> dict:
    rng = random.Random(cfg.seed)
    providers = sorted(PROVIDERS)
    currencies = sorted(CURRENCIES)
    events: list[dict] = []
    stmts: dict[str, list[dict]] = {p: [] for p in providers}
    units: list[dict] = []
    pairs: list[dict] = []
    used_refs: set[str] = set()
    seq = {"pay": 0, "stmt": 0}

    def new_ref(prov: str) -> str:
        while True:
            r = f"{prov[:3]}-REF-{rng.randrange(10**8):08d}"
            if r not in used_refs:
                used_refs.add(r)
                return r

    def mutate(ref: str, edits: int, avoid: set[str]) -> str:
        digits = [i for i, c in enumerate(ref) if c.isdigit()]
        while True:
            chars = list(ref)
            for pos in rng.sample(digits, edits):
                chars[pos] = rng.choice([d for d in "0123456789" if d != chars[pos]])
            out = "".join(chars)
            if out not in used_refs and out not in avoid and _lev(out, ref) == edits:
                used_refs.add(out)
                return out

    def payment(prov=None, cur=None, amount=None, t_settle=None):
        seq["pay"] += 1
        src, dst = rng.choice(CORRIDORS)
        cur = cur or src
        prov = prov or rng.choice(providers)
        amount = amount or (
            rng.randint(100_000, 9_000_000) if CURRENCIES[cur] else rng.randint(10_000, 900_000)
        )
        t_settle = t_settle or BASE + timedelta(seconds=rng.randrange(20 * 86400))
        pid = f"EPAY-{seq['pay']:07d}"
        return dict(pid=pid, prov=prov, cur=cur, amount=amount, t=t_settle, corridor=f"{cur}-{dst}")

    def settle_event(p, ref, ingest_delay=None, eid=None):
        e = {
            "event_id": eid or f"EEVT-{p['pid']}-SET",
            "event_type": "funds.settled", "payment_id": p["pid"], "event_time": _ts(p["t"]),
            "ingested_at": _ts(p["t"] + timedelta(seconds=ingest_delay or rng.randint(1, 60))),
            "schema_version": "1.0", "source": "eval", "provider": p["prov"], "provider_ref": ref,
            "amount_minor": p["amount"], "currency": p["cur"], "corridor": p["corridor"],
            "dest_currency": None, "dest_amount_minor": None,
        }  # fmt: skip
        return e

    def stmt_row(p, ref, amount=None, cur=None, status="SETTLED", at=None, prov=None):
        seq["stmt"] += 1
        prov = prov or p["prov"]
        cur = cur or p["cur"]
        ext = [ref, ref.lower(), f" {ref} ", ref.replace("-", "")][rng.randrange(4)]
        row = {
            "statement_row_id": f"{prov}-ES{seq['stmt']:08d}", "provider": prov,
            "external_ref": ext, "amount": format_minor(amount or p["amount"], cur),
            "currency": cur, "status": status,
            "settled_at": _ts(at or p["t"] + timedelta(seconds=rng.randint(600, 20 * H))),
        }  # fmt: skip
        stmts[prov].append(row)
        return row

    def unit(
        kind,
        uid,
        outcome,
        expected_class,
        amount,
        cur,
        partner=None,
        internal=None,
        scen="",
        key=None,
    ):
        units.append(
            dict(kind=kind, id=uid, outcome=outcome, expected_class=expected_class,
                 unresolved=expected_class in UNRESOLVED, amount_minor=amount, currency=cur,
                 true_partner_row=partner, true_internal_event=internal, scenario=scen)
        )  # fmt: skip

    def pair(e, s, resolvable, scen):
        pairs.append(dict(event_id=e["event_id"], statement_row_id=s["statement_row_id"],
                          resolvable=resolvable, scenario=scen))  # fmt: skip

    counts: dict[str, int] = {}
    weights = [w for _, w in MIX]
    names = [n for n, _ in MIX]
    made = 0
    while made < cfg.payments:
        scen = rng.choices(names, weights)[0]
        counts[scen] = counts.get(scen, 0) + 1
        p = payment()
        ref = new_ref(p["prov"])
        made += 1
        e = settle_event(p, ref)
        if scen in ("CLEAN", "LATE", "AMOUNT", "CURRENCY", "STATUS", "DELAYED", "DUP_STATEMENT",
                    "DUP_EVENT"):  # fmt: skip
            kw, cls = {}, "MATCHED"
            if scen == "AMOUNT":
                kw["amount"] = p["amount"] + rng.choice([-1, 1]) * rng.randint(1, 900)
                cls = "AMOUNT_MISMATCH"
            elif scen == "CURRENCY":
                kw["cur"] = rng.choice([c for c in currencies if c != p["cur"]])
                cls = "CURRENCY_MISMATCH"
            elif scen == "STATUS":
                kw["status"] = rng.choice(["REJECTED", "RETURNED", "PENDING"])
                cls = "STATUS_MISMATCH"
            elif scen == "DELAYED":
                kw["at"] = p["t"] + timedelta(seconds=rng.randint(60 * H, 120 * H))
                cls = "DELAYED_SETTLEMENT"
            if scen == "LATE":
                e = settle_event(p, ref, ingest_delay=rng.randint(3 * 86400, 9 * 86400))
            s = stmt_row(p, ref, **kw)
            events.append(e)
            if scen == "DUP_STATEMENT":
                stmts[p["prov"]].append(dict(s))
                cls = "DUPLICATE_STATEMENT"
            if scen == "DUP_EVENT":
                events.append(dict(e))
                cls = "DUPLICATE_EVENT"
            unit("internal", e["event_id"], "AUTO_MATCH", cls, p["amount"], p["cur"],
                 s["statement_row_id"], scen=scen)  # fmt: skip
            pair(e, s, True, scen)
        elif scen == "TYPO_1":
            s = stmt_row(p, mutate(ref, 1, set()))
            events.append(e)
            unit("internal", e["event_id"], "AUTO_MATCH", "MATCHED", p["amount"], p["cur"],
                 s["statement_row_id"], scen=scen)  # fmt: skip
            pair(e, s, True, scen)
        elif scen in ("TYPO_2", "COMBINED_TYPO_AMOUNT"):
            kw = {}
            if scen == "COMBINED_TYPO_AMOUNT":
                kw["amount"] = p["amount"] + rng.randint(1, 500)
            s = stmt_row(p, mutate(ref, 1 if scen == "COMBINED_TYPO_AMOUNT" else 2, set()), **kw)
            events.append(e)
            unit("internal", e["event_id"], "NO_AUTO_MATCH", "UNRESOLVED_PAIR", p["amount"],
                 p["cur"], s["statement_row_id"], scen=scen)  # fmt: skip
            pair(e, s, False, scen)
        elif scen == "TYPO_COLLISION":
            # A and B: same provider/currency/amount/time, references differ in one digit.
            events.append(e)
            digits = [i for i, c in enumerate(ref) if c.isdigit()]
            pos = digits[-1]
            alt = list(ref)
            alt[pos] = rng.choice([d for d in "0123456789" if d != ref[pos]])
            ref_b = "".join(alt)
            if ref_b in used_refs:
                ref_b = ref_b[:-1] + "9" if ref_b[-1] != "9" else ref_b[:-1] + "8"
            used_refs.add(ref_b)
            made += 1
            pb = dict(payment(p["prov"], p["cur"], p["amount"], p["t"]))
            eb = settle_event(pb, ref_b)
            events.append(eb)
            others = [d for d in "0123456789" if d not in (ref[pos], ref_b[pos])]
            ra, rb = list(ref), list(ref)
            ra[pos], rb[pos] = others[0], others[1]
            sa = stmt_row(p, "".join(ra))
            sb = stmt_row(pb, "".join(rb))
            for ev, st_, pp in ((e, sa, p), (eb, sb, pb)):
                unit("internal", ev["event_id"], "ABSTAIN", "AMBIGUOUS_MATCH", pp["amount"],
                     pp["cur"], st_["statement_row_id"], scen=scen)  # fmt: skip
                pair(ev, st_, False, scen)
        elif scen == "WRONG_TARGET":
            # Partner booked A's money under B's reference (identical amount): undetectable by data.
            events.append(e)
            made += 1
            pb = payment(
                p["prov"], p["cur"], p["amount"], p["t"] + timedelta(seconds=rng.randint(60, 900))
            )
            ref_b = new_ref(p["prov"])
            eb = settle_event(pb, ref_b)
            events.append(eb)
            sa = stmt_row(p, ref_b)  # A's partner row carries B's reference
            unit("internal", e["event_id"], "MISSING_PARTNER", "MISSING_PARTNER_RECORD",
                 p["amount"], p["cur"], sa["statement_row_id"], scen=scen)  # fmt: skip
            unit("internal", eb["event_id"], "MISSING_PARTNER", "MISSING_PARTNER_RECORD",
                 pb["amount"], pb["cur"], None, scen=scen)  # fmt: skip
            pair(e, sa, False, scen)
        elif scen == "LOOKALIKE":
            # B has no partner row; an unrelated partner-only row has a 1-edit-away reference.
            events.append(e)
            unit("internal", e["event_id"], "MISSING_PARTNER", "MISSING_PARTNER_RECORD",
                 p["amount"], p["cur"], None, scen=scen)  # fmt: skip
            g = stmt_row(
                p, mutate(ref, 1, set()), at=p["t"] + timedelta(seconds=rng.randint(600, 12 * H))
            )
            unit("partner", g["statement_row_id"], "MISSING_INTERNAL", "MISSING_INTERNAL_RECORD",
                 p["amount"], p["cur"], scen=scen)  # fmt: skip
        elif scen == "MISSING_PARTNER":
            events.append(e)
            unit("internal", e["event_id"], "MISSING_PARTNER", "MISSING_PARTNER_RECORD",
                 p["amount"], p["cur"], None, scen=scen)  # fmt: skip
        elif scen == "PARTNER_ONLY":
            g = stmt_row(p, new_ref(p["prov"]))
            unit("partner", g["statement_row_id"], "MISSING_INTERNAL", "MISSING_INTERNAL_RECORD",
                 p["amount"], p["cur"], scen=scen)  # fmt: skip
        elif scen == "CONFLICT_EVENT":
            s = stmt_row(p, ref)
            e2 = dict(e)
            e2["amount_minor"] = e["amount_minor"] + rng.randint(1, 500)
            events += [e, e2]
            unit("internal", e["event_id"], "INVALID", "INVALID_RECORD", p["amount"], p["cur"],
                 s["statement_row_id"], scen=scen)  # fmt: skip
            pair(e, s, False, scen)

    ev_by_id = {e["event_id"]: e for e in events}
    for u in units:  # evaluation-side bookkeeping so rejected (conflicting) events can be located
        if u["kind"] == "internal":
            e = ev_by_id[u["id"]]
            u["match_key"] = make_match_key(e["provider"], e["provider_ref"])
    events.sort(key=lambda x: (x["ingested_at"], x["event_id"]))
    lines = [_dump(e) for e in events]
    for _ in range(round(0.1 * len(lines))):  # out-of-order delivery
        a, b = rng.randrange(len(lines)), rng.randrange(len(lines))
        lines[a], lines[b] = lines[b], lines[a]

    paths.raw.mkdir(parents=True, exist_ok=True)
    paths.ground_truth.mkdir(parents=True, exist_ok=True)
    for old in [*paths.raw.glob("payment_events*.jsonl"), *paths.raw.glob("statements_*")]:
        old.unlink()
    (paths.raw / "payment_events.jsonl").write_text(
        "\n".join(lines) + "\n", encoding="utf-8", newline="\n"
    )
    for prov, rows in stmts.items():
        rows.sort(key=lambda r: (r["settled_at"], r["statement_row_id"]))
        if PROVIDERS[prov] == "csv":
            out = []
            for r in rows:
                b = io.StringIO()
                csv.writer(b, lineterminator="").writerow([r[c] for c in STMT_COLS])
                out.append(b.getvalue())
            text = ",".join(STMT_COLS) + "\n" + "\n".join(out) + "\n"
        else:
            body = ",\n".join("  " + _dump(r) for r in rows)
            text = f'{{"provider":"{prov}","rows":[\n{body}\n]}}\n'
        (paths.raw / f"statements_{prov}.{PROVIDERS[prov]}").write_text(
            text, encoding="utf-8", newline="\n"
        )
    truth = {"config": {"payments": cfg.payments, "seed": cfg.seed}, "scenario_counts": counts,
             "units": units, "pairs": pairs}  # fmt: skip
    (paths.ground_truth / "eval_truth.json").write_text(
        json.dumps(truth), encoding="utf-8", newline="\n"
    )
    return {"payments": made, "events": len(lines), "units": len(units), "pairs": len(pairs),
            "scenario_counts": dict(sorted(counts.items()))}  # fmt: skip
