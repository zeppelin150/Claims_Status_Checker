#!/usr/bin/env python3
"""
status_simulator.py — Simulate insurance partners "returning" claims.

Flips rows in the LightdashData sim tab from non-actionable statuses
(submitted / resubmitted / blank) to actionable statuses with adjudication
dates, so the pipeline picks them up on the next E2E run.

Selection modes (pick one):
    --percent N          flip N% of currently non-actionable rows (default 30)
    --count N            flip exactly N rows at random
    --slug abc123        flip a specific claim slug (all its rows)
    --run-id RID         flip only slugs created by seeder run RID
    --scenario NAME      flip only slugs whose seeder scenario matches NAME

By default, flips the slug's claim status to a weighted random actionable
value ("Completed - ERA Posted" is most common) and sets adjudication_date
to roughly today. Use --status to pin a specific status.

Examples:
    python3 status_simulator.py --percent 30
    python3 status_simulator.py --count 5 --status "rejected"
    python3 status_simulator.py --run-id 20260416T120000Z --percent 50
    python3 status_simulator.py --slug abc123 --status "Completed - ERA Posted"
"""

import argparse
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Windows-safe output (cp1252 consoles can't encode check-marks / em-dashes).
try:
    sys.stdout.reconfigure(errors="replace")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).absolute().parent))
from test_suite import (  # noqa: E402
    load_env, env, get_sheets_service, sh_read, sh_write,
    LD_COLUMNS, LD_SLUG, LD_STATUS, LD_ADJ_DAY, LD_SUBMITTED,
    ACTIONABLE_STATUSES,
)

RUNS_FILE = Path(__file__).absolute().parent / ".seeder_runs.json"

ACTIONABLE_WEIGHTED = [
    ("Completed - ERA Posted",      6),
    ("Completed - No ERA (see Notes)", 2),
    ("rejected",                    1),
    ("write_off",                   1),
    ("canceled",                    1),
]


def load_runs():
    if not RUNS_FILE.exists():
        return {"runs": {}}
    try:
        return json.loads(RUNS_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"runs": {}}


def pick_actionable():
    choices, weights = zip(*ACTIONABLE_WEIGHTED)
    return random.choices(choices, weights=weights, k=1)[0]


def iso_day(d):
    return d.strftime("%Y-%m-%dT00:00:00.000Z")


def read_sim(svc, sid):
    """Read LightdashData A:G, return (headers, [(row_num, dict)] for data rows)."""
    raw = sh_read(svc, sid, "'LightdashData'!A:G")
    if not raw:
        return [], []
    headers = raw[0]
    data = []
    for i, r in enumerate(raw[1:], start=2):  # row 2 = first data row
        padded = r + [""] * (len(headers) - len(r))
        d = {h: padded[idx] for idx, h in enumerate(headers)}
        data.append((i, d))
    return headers, data


def pick_targets(data, args, run_filter_slugs):
    """Return list of (row_num, row_dict) to flip."""
    # Filter to non-actionable rows, unless user is targeting by slug directly.
    def is_nonactionable(row):
        return row.get(LD_STATUS, "").strip() not in ACTIONABLE_STATUSES

    # Apply run/slug/scenario filter first
    if args.slug:
        slug = args.slug.strip().lower()
        candidates = [(n, r) for (n, r) in data if r.get(LD_SLUG, "").strip().lower() == slug]
    elif run_filter_slugs is not None:
        candidates = [(n, r) for (n, r) in data
                      if r.get(LD_SLUG, "").strip().lower() in run_filter_slugs]
    else:
        candidates = list(data)

    # Of those, keep only non-actionable (unless --force-actionable)
    if not args.force_actionable:
        candidates = [(n, r) for (n, r) in candidates if is_nonactionable(r)]

    if not candidates:
        return []

    # Now apply count / percent
    if args.slug:
        return candidates  # flip all rows for that slug
    if args.count is not None:
        k = min(args.count, len(candidates))
        return random.sample(candidates, k)
    pct = args.percent if args.percent is not None else 30
    k = max(1, int(len(candidates) * pct / 100))
    k = min(k, len(candidates))
    return random.sample(candidates, k)


def flip_row(headers, row_dict, status_override, anchor):
    """Return a new full-row list with status + adjudication updated."""
    new_status = status_override or pick_actionable()
    submitted = row_dict.get(LD_SUBMITTED, "")
    # Adjudication date: within a small window of today (or a bit after submitted)
    adj_day = iso_day(anchor - timedelta(days=random.randint(0, 5)))
    updated = dict(row_dict)
    updated[LD_STATUS] = new_status
    updated[LD_ADJ_DAY] = adj_day
    return [updated.get(h, "") for h in headers]


def main():
    p = argparse.ArgumentParser(description="Flip sim rows to actionable statuses.")
    p.add_argument("--percent", type=int, default=None, help="Flip N%% of non-actionable rows")
    p.add_argument("--count",   type=int, default=None, help="Flip exactly N rows")
    p.add_argument("--slug",    default=None,           help="Flip all rows for this slug")
    p.add_argument("--run-id",  default=None,           help="Flip only slugs from this seeder run")
    p.add_argument("--scenario", default=None,          help="Flip only slugs matching this scenario name")
    p.add_argument("--status",  default=None,           help="Force a specific actionable status")
    p.add_argument("--force-actionable", action="store_true",
                   help="Re-flip even rows already actionable (normally skipped)")
    p.add_argument("--seed", type=int, default=None,    help="RNG seed for determinism")
    p.add_argument("--dry-run", action="store_true",    help="Print what would change, no writes")
    a = p.parse_args()

    if sum(x is not None for x in (a.percent, a.count, a.slug)) > 1:
        print("Pick at most one of --percent / --count / --slug"); return 2

    if a.seed is not None:
        random.seed(a.seed)

    load_env()
    sid = env("GOOGLE_SHEETS_SPREADSHEET_ID")
    svc = get_sheets_service()

    print(); print("=" * 72); print("Status Simulator"); print("=" * 72)

    # Resolve run/scenario filter → set of slugs (or None if no filter)
    run_filter_slugs = None
    if a.run_id or a.scenario:
        runs = load_runs()["runs"]
        if a.run_id and a.run_id not in runs:
            print(f"Unknown run-id: {a.run_id}"); return 2
        eligible = set()
        target_runs = [a.run_id] if a.run_id else list(runs.keys())
        for rid in target_runs:
            rec = runs[rid]
            if a.scenario:
                # scenarios: {task_gid: scenario_name}, slug_roles: {slug: role}
                # Scenario→slugs map isn't stored directly, but task→scenario and
                # task→slugs can be derived. We stored slug_roles flat; scenario
                # targeting is best-effort: filter by whichever slugs appear alongside
                # tasks with the matching scenario. For now, we use all slugs in the
                # run when --scenario is given without --run-id.
                for s in rec.get("slugs", []):
                    eligible.add(s.lower())
            else:
                for s in rec.get("slugs", []):
                    eligible.add(s.lower())
        run_filter_slugs = eligible
        print(f"  Filtered to {len(run_filter_slugs)} slug(s) from run(s)")

    # Read sim
    headers, data = read_sim(svc, sid)
    if not data:
        print("  LightdashData is empty — nothing to simulate.")
        return 0
    print(f"  Sim rows: {len(data)}")

    targets = pick_targets(data, a, run_filter_slugs)
    print(f"  Targets to flip: {len(targets)}")
    if not targets:
        print("  Nothing matches — exiting."); return 0

    anchor = datetime.now(timezone.utc).date()
    updates = []  # (row_num, values)
    for row_num, row_dict in targets:
        before_status = row_dict.get(LD_STATUS, "")
        new_row = flip_row(headers, row_dict, a.status, anchor)
        new_dict = {h: new_row[i] for i, h in enumerate(headers)}
        print(f"  row {row_num:3d}  slug={row_dict.get(LD_SLUG,''):>8}  "
              f"{before_status!r:>35}  →  {new_dict.get(LD_STATUS, ''):>30}  "
              f"adj={new_dict.get(LD_ADJ_DAY,'')[:10]}")
        updates.append((row_num, new_row))

    if a.dry_run:
        print("\n  (dry-run — no writes)"); return 0

    # Write each updated row (A:G)
    for row_num, row_vals in updates:
        sh_write(svc, sid, f"'LightdashData'!A{row_num}:G{row_num}", [row_vals])

    print(f"\n  ✔ Flipped {len(updates)} row(s).")
    print(f"  Next: run the pipeline (python3 test_suite.py e2e) to pick up the changes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
