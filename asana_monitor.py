#!/usr/bin/env python3
"""
asana_monitor.py — WOIP intake monitor (runs every 5 minutes).

Watches the "Waiting On Insurance Partner" section ONLY. For each task in that
section:
  1. Parse claim slugs from the "Please Paste" custom field.
  2. For each slug: if (slug, task_gid) is not already in Sheet1, append a row.
  3. If the claims text is unparseable, post an automation review comment
     (idempotent — checked against existing task stories).

Asana-side housekeeping (assigning CX/OPs, moving sections) is handled by an
Asana rule on submission — this monitor does NOT modify custom fields or move
tasks, it only syncs WOIP → Sheet1.

Usage:
    python3 asana_monitor.py              # one sweep, then exit   (for cron / GHA)
    python3 asana_monitor.py --loop       # sweep every 5 min until Ctrl-C (dev)
    python3 asana_monitor.py --interval 60 --loop   # custom interval (seconds)
    python3 asana_monitor.py --dry-run    # log only, no writes / comments
"""

import argparse
import logging
import signal
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

# Windows-safe output (cp1252 consoles can't encode check-marks / em-dashes).
try:
    sys.stdout.reconfigure(errors="replace")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).absolute().parent))
from test_suite import (  # noqa: E402
    load_env, env, utc_today, verify_lightdash, set_dry_run,
    asana_req, asana_get_section_tasks, asana_task_field, asana_post_comment,
    parse_claim_slugs, get_sheets_service, sh_read,
    DATA_START_ROW, COL_Z_INDEX,
)
from claims_logging import setup_logging, gen_run_id, set_run_id  # noqa: E402
from recorder import RunRecorder  # noqa: E402

logger = logging.getLogger(__name__)

REVIEW_MARKER = "[Automation]"  # marker to detect prior review comments


# ═══════════════════════════════════════════════════════════════════════════════
# SHEET — read existing (slug, task_gid) pairs
# ═══════════════════════════════════════════════════════════════════════════════
def read_existing_pairs(svc, sid):
    """Return set of (clean_slug_lower, task_gid) tuples already in Sheet1."""
    rows = sh_read(svc, sid, "Sheet1!A:Z")
    pairs = set()
    for r in rows[DATA_START_ROW - 1:]:
        slug = r[4].strip().lower() if len(r) > 4 else ""
        gid = r[COL_Z_INDEX].strip() if len(r) > COL_Z_INDEX else ""
        if slug and gid:
            pairs.add((slug, gid))
    return pairs


def append_rows(svc, sid, task_gid, task_url, slugs, today_str):
    """Append one Sheet1 row per slug for a single (task, slugs) pair.
    One API call regardless of slug count.

    Note: task_gid is prefixed with apostrophe to force text storage. 16-digit
    GIDs otherwise get coerced to scientific notation ("1.21411E+15") and lose
    precision, breaking idempotency in later sweeps.
    """
    if not slugs:
        return
    rows = []
    for slug in slugs:
        row = [
            f"cl-{slug}",     # A
            "",               # B Associate
            today_str,        # C Date added
            task_url,         # D Asana task URL
            slug,             # E Clean slug
            "", "", "", "", "", "",  # F-K (pipeline populates)
            "", "",           # L, M (PI / Ticket)
        ]
        row += [""] * 12 + [f"'{task_gid}"]
        rows.append(row)
    svc.spreadsheets().values().append(
        spreadsheetId=sid,
        range="Sheet1!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


# Backwards-compat alias removed deliberately: callers must batch via append_rows.


def task_has_review_comment(pat, task_gid):
    """Check stories for a prior [Automation] review comment (idempotency)."""
    s, b = asana_req("GET", f"/tasks/{task_gid}/stories?opt_fields=text,type", pat)
    if s != 200:
        return False
    for story in b.get("data", []):
        if story.get("type") == "comment" and REVIEW_MARKER in (story.get("text") or ""):
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# SWEEP
# ═══════════════════════════════════════════════════════════════════════════════
def sweep(pat, project_gid, svc, sid, dry_run=False, recorder=None):
    """One pass over the WOIP section. Returns dict of counters.
    If `recorder` is provided, counters/errors are recorded to it (caller
    is responsible for flushing)."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"\n[{ts}] WOIP sweep  dry_run={dry_run}")

    tasks = asana_get_section_tasks(pat, project_gid, env("ASANA_SECTION_WOIP"))
    print(f"  WOIP tasks: {len(tasks)}")

    existing = read_existing_pairs(svc, sid)
    print(f"  Sheet1 existing (slug, task) pairs: {len(existing)}")

    today_str = utc_today().strftime("%m/%d/%Y")
    counters = {
        "tasks_seen": len(tasks),
        "tasks_new": 0,
        "tasks_skipped": 0,
        "rows_appended": 0,
        "unparseable": 0,
        "review_comments_posted": 0,
        "review_comments_skipped_existing": 0,
    }

    for t in tasks:
        gid = t["gid"]
        name = t.get("name", "")
        claims_field = asana_task_field(t, env("ASANA_FIELD_CLAIMS_TEXT"))
        claims_text = (claims_field.get("text_value") or "") if claims_field else ""
        slugs, needs_review = parse_claim_slugs(claims_text)

        # Slugs we haven't recorded for this task yet
        to_append = [s for s in slugs if (s.lower(), gid) not in existing]

        if needs_review and not slugs:
            counters["unparseable"] += 1
            print(f"  ⚠ UNPARSEABLE  {gid}  {name!r:60}  claims={claims_text!r}")
            if dry_run:
                continue
            if task_has_review_comment(pat, gid):
                counters["review_comments_skipped_existing"] += 1
                print(f"    (review comment already posted — skipping)")
                continue
            s, _ = asana_post_comment(pat, gid,
                f"{REVIEW_MARKER} Could not parse claim IDs from this task. "
                "Please review the 'Please Paste' field and ensure claim IDs "
                "are 6-character alphanumeric codes (with or without 'cl-' prefix).")
            if s in (200, 201):
                counters["review_comments_posted"] += 1
                print(f"    ✔ review comment posted")
            else:
                print(f"    ✘ review comment failed: {s}")
            continue

        if not to_append:
            counters["tasks_skipped"] += 1
            continue

        counters["tasks_new"] += 1
        task_url = f"https://app.asana.com/0/{project_gid}/{gid}"
        print(f"  + NEW TASK   {gid}  {name!r:60}  slugs={to_append}")
        if dry_run:
            counters["rows_appended"] += len(to_append)
            continue
        append_rows(svc, sid, gid, task_url, to_append, today_str)
        for s in to_append:
            existing.add((s.lower(), gid))
            counters["rows_appended"] += 1

    print(f"  -> summary: new_tasks={counters['tasks_new']}  "
          f"rows={counters['rows_appended']}  "
          f"unparseable={counters['unparseable']}  "
          f"reviews_posted={counters['review_comments_posted']}  "
          f"skipped_existing_pairs={counters['tasks_skipped']}")
    if recorder is not None:
        recorder.record(**counters)
    return counters


# ═══════════════════════════════════════════════════════════════════════════════
# LOOP
# ═══════════════════════════════════════════════════════════════════════════════
_stop = False
def _on_sigterm(signum, frame):
    global _stop
    _stop = True
    print(f"\n[signal {signum}] stopping after current sweep...")


def main():
    p = argparse.ArgumentParser(description="WOIP monitor — sync Waiting On Insurance Partner tasks to Sheet1.")
    p.add_argument("--loop", action="store_true", help="Run forever (interval between sweeps)")
    p.add_argument("--interval", type=int, default=300, help="Seconds between sweeps in --loop mode (default 300)")
    p.add_argument("--dry-run", action="store_true", help="Log only, no writes or comments")
    p.add_argument("--max-sweeps", type=int, default=None, help="Stop after N sweeps (for testing)")
    a = p.parse_args()

    setup_logging()
    load_env()
    set_dry_run(a.dry_run)  # propagate to test_suite helpers
    verify_lightdash()  # fail loud on misconfigured URL/key (no-op if unset)
    pat = env("ASANA_PAT")
    project_gid = env("ASANA_PROJECT_GID")
    sid = env("GOOGLE_SHEETS_SPREADSHEET_ID")
    recorder_gid = env("ASANA_RECORDER_TASK_GID", required=False)
    svc = get_sheets_service()

    print("=" * 72); print("Asana WOIP Monitor"); print("=" * 72)
    print(f"  project:  {project_gid}")
    print(f"  section:  WOIP ({env('ASANA_SECTION_WOIP')})")
    print(f"  interval: {a.interval}s  loop={a.loop}  dry_run={a.dry_run}")
    print(f"  recorder: {recorder_gid or '(unset — sweeps will not be logged to Asana)'}")

    signal.signal(signal.SIGINT, _on_sigterm)
    try:
        signal.signal(signal.SIGTERM, _on_sigterm)
    except (AttributeError, ValueError):
        pass  # SIGTERM not available on Windows

    sweeps_done = 0
    while True:
        run_id = gen_run_id()
        set_run_id(run_id)
        recorder = RunRecorder(pat, run_id, "woip_sweep", task_gid=recorder_gid)
        try:
            sweep(pat, project_gid, svc, sid, dry_run=a.dry_run, recorder=recorder)
            recorder.flush(status="success")
        except Exception as e:
            recorder.error(repr(e))
            recorder.flush(status="error")
            raise
        sweeps_done += 1
        if not a.loop:
            break
        if a.max_sweeps and sweeps_done >= a.max_sweeps:
            print(f"\n  reached --max-sweeps={a.max_sweeps}, exiting")
            break
        if _stop:
            break
        # Sleep in 1s chunks so Ctrl-C is responsive
        for _ in range(a.interval):
            if _stop:
                break
            time.sleep(1)
        if _stop:
            break

    print(f"\n✔ done — {sweeps_done} sweep(s) completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
