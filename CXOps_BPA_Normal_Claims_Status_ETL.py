#!/usr/bin/env python3
"""
CXOps_BPA_Normal_Claims_Status_ETL.py
=====================================
Production ETL for normal claim status monitoring.

Naming convention (department_type_function_flow):
    CXOps  — owning team
    BPA    — Business Process Automation
    Normal_Claims_Status — what it operates on
    ETL    — extract / transform / load

Pipeline (Lightdash → Sheet → Asana, all in one pass):
    1. Read Sheet1 for pending rows (col E filled, col F empty).
    2. Query Lightdash for those slugs (NO sheet fallback — fails loud if
       LIGHTDASH_API_URL is unset; the LightdashData tab is reserved for
       test_suite.py running on a non-dev machine without warehouse access).
    3. Compute F-K (Return Check, Ticket counts, Aging, Send-to-Aging).
    4. Write F-K back to the sheet (one batched call).
    4b. Stale-claim check (track no-data slugs, post 30-day notification).
    5. Close completed Asana tasks (Have All Claims Returned → Yes,
       CX/OPs → needs follow-up, post confirmation comment).

Modes (controlled by env vars):

    IS_CI               WRITE_ONE_ROW       Behavior
    -----------------------------------------------------------------------
    true (on GHA)       (ignored)           Full production pipeline. All
                                            writes execute. Used by the
                                            scheduled GitHub Actions cron.

    false (local)       false               Dry-run. Print every action
                                            that would be taken; no Sheets
                                            or working-Asana mutations.

    false (local)       true                Single-row demo. Write F-K for
                                            the FIRST pending row only;
                                            skip stale check + Asana close.

Safety:
    - IS_CI=true is only honored when GITHUB_ACTIONS=true. Refuses to run
      production writes from a dev machine even if IS_CI is misconfigured.
    - LIGHTDASH_API_URL is required. No fallback to LightdashData tab.
    - The run recorder posts to a central log Asana project regardless of
      dry-run state — recording a run is not a working-data mutation.

Required env vars (see .env.example for full template):
    IS_CI, WRITE_ONE_ROW
    LIGHTDASH_API_URL, LIGHTDASH_API_KEY, LIGHTDASH_PROJECT_UUID
    GOOGLE_SERVICE_ACCOUNT_FILE | GOOGLE_SERVICE_ACCOUNT_JSON
    GOOGLE_SHEETS_SPREADSHEET_ID
    ASANA_PAT, ASANA_PROJECT_GID
    ASANA_FIELD_*, ASANA_OPT_*

Optional (recorder destination — log to stdout if unset):
    ASANA_LOG_PROJECT_GID
    CXOPS_NORMAL_CLAIMS_ETL_LOG_SECTION_GID

Usage:
    python3 CXOps_BPA_Normal_Claims_Status_ETL.py
"""
import logging
import os
import sys
from pathlib import Path

# Windows-safe console output
try:
    sys.stdout.reconfigure(errors="replace")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).absolute().parent))
from test_suite import (  # noqa: E402
    load_env, env, set_dry_run, has_lightdash, verify_lightdash,
    _get_claim_data_api, get_sheets_service, sh_read, xval,
    write_fk_batched, check_stale_claims, close_completed_tasks,
    DATA_START_ROW, LD_SLUG,
)
from claims_logging import setup_logging, gen_run_id, set_run_id  # noqa: E402
from recorder import RunRecorder  # noqa: E402

SWEEP_NAME = "CXOps_BPA_Normal_Claims_Status_ETL"
logger = logging.getLogger(SWEEP_NAME)


# ───────────────────────────────────────────────────────────────────────────
# Mode resolution
# ───────────────────────────────────────────────────────────────────────────
def parse_bool_env(name: str, default: bool = False) -> bool:
    val = os.getenv(name, "").strip().lower()
    if val in ("true", "1", "yes"):
        return True
    if val in ("false", "0", "no", ""):
        return default
    raise ValueError(f"{name} must be true/false (got {val!r})")


def resolve_mode() -> dict:
    """Read IS_CI / WRITE_ONE_ROW, enforce safety, return mode dict.

    Raises RuntimeError if IS_CI=true but GITHUB_ACTIONS != true. This guard
    prevents a dev machine with a stale env var from accidentally executing
    production writes.
    """
    is_ci = parse_bool_env("IS_CI", False)
    write_one = parse_bool_env("WRITE_ONE_ROW", False)
    on_gha = os.getenv("GITHUB_ACTIONS", "").strip().lower() == "true"

    if is_ci and not on_gha:
        raise RuntimeError(
            "IS_CI=true is only honored when GITHUB_ACTIONS=true (real "
            "GitHub Actions cron). Refusing to make production writes from "
            "a non-CI environment. To run locally: set IS_CI=false."
        )

    is_local = not is_ci
    return {
        "is_ci": is_ci,
        "is_local": is_local,
        "is_dry_run": is_local and not write_one,
        "is_single_row": is_local and write_one,
    }


def build_recorder(run_id: str) -> RunRecorder:
    """Build a recorder pointing at this file's section in the central log
    project. Falls back to stdout-only if either GID is unset."""
    pat = os.getenv("ASANA_PAT", "")
    project_gid = os.getenv("ASANA_LOG_PROJECT_GID", "").strip()
    section_gid = os.getenv("CXOPS_NORMAL_CLAIMS_ETL_LOG_SECTION_GID", "").strip()
    return RunRecorder(
        pat, run_id, SWEEP_NAME,
        log_project_gid=project_gid,
        log_section_gid=section_gid,
    )


# ───────────────────────────────────────────────────────────────────────────
# Pipeline
# ───────────────────────────────────────────────────────────────────────────
def read_pending_rows(svc, sid):
    """Returns (pending_dict, all_rows). pending = {slug: row_idx}.
    Pending = col E filled, col F empty. row_idx is 1-based."""
    all_rows = sh_read(svc, sid, "Sheet1!A:Z")
    pending = {}
    for i, row in enumerate(all_rows[DATA_START_ROW - 1:], start=DATA_START_ROW):
        slug = row[4].strip() if len(row) > 4 else ""
        ret = row[5].strip() if len(row) > 5 else ""
        if slug and not ret:
            pending[slug] = i
    return pending, all_rows


def trim_to_first_pending(pending: dict) -> dict:
    """Return a dict with only the first (slug, idx) entry. Empty in → empty out."""
    if not pending:
        return {}
    first = next(iter(pending.items()))
    return {first[0]: first[1]}


def run_pipeline(recorder: RunRecorder, mode: dict) -> None:
    pat = env("ASANA_PAT")
    sid = env("GOOGLE_SHEETS_SPREADSHEET_ID")
    svc = get_sheets_service()

    recorder.log_action(
        f"mode: ci={mode['is_ci']} dry_run={mode['is_dry_run']} "
        f"single_row={mode['is_single_row']}"
    )

    # Step 1
    pending, all_rows = read_pending_rows(svc, sid)
    recorder.record(pending_seen=len(pending))
    recorder.log_action(f"step 1: found {len(pending)} pending row(s)")

    if not pending:
        recorder.log_action("nothing to process")
        return

    # Single-row mode: trim to first pending before any other work
    if mode["is_single_row"]:
        first_slug = next(iter(pending))
        first_idx = pending[first_slug]
        recorder.log_action(
            f"single-row mode: trimming {len(pending)} pending → 1 "
            f"(row {first_idx}, slug={first_slug})"
        )
        pending = trim_to_first_pending(pending)

    # Step 2: Lightdash query (NO fallback — verify_lightdash() ran in main()).
    claim_rows = _get_claim_data_api(list(pending.keys()))
    by_slug = {}
    for row in claim_rows:
        if isinstance(row, dict):
            s = xval(row, LD_SLUG)
            if s:
                by_slug.setdefault(s, []).append(row)
    recorder.record(
        slugs_with_data=len(by_slug),
        slugs_no_data=len(pending) - len(by_slug),
    )
    recorder.log_action(
        f"step 2: Lightdash returned data for {len(by_slug)}/{len(pending)} slug(s)"
    )

    # Step 3-4: Compute + write F-K (batched).
    matched, no_data = write_fk_batched(svc, sid, pending, by_slug,
                                         log=recorder.log_action)
    recorder.record(fk_matched=matched, fk_no_data=no_data)

    # Single-row mode stops here — no stale check, no Asana close.
    if mode["is_single_row"]:
        recorder.log_action("single-row mode: skipping stale check and Asana close")
        return

    # Step 4b: stale-claim check (only when there are no-data slugs).
    if no_data > 0:
        stale = check_stale_claims(svc, sid, pat, pending, by_slug, all_rows,
                                    log=recorder.log_action)
        recorder.record(**stale)

    # Step 5: close Asana tasks whose claims are all returned.
    all_rows_after = sh_read(svc, sid, "Sheet1!A:Z")
    close = close_completed_tasks(svc, sid, pat, all_rows_after,
                                   log=recorder.log_action)
    recorder.record(**close)


# ───────────────────────────────────────────────────────────────────────────
# Entrypoint
# ───────────────────────────────────────────────────────────────────────────
def main() -> int:
    setup_logging()
    load_env()

    mode = resolve_mode()

    # Lightdash is required — no fallback.
    if not has_lightdash():
        raise RuntimeError(
            "LIGHTDASH_API_URL must be set. This production pipeline does NOT "
            "fall back to the LightdashData sheet — that's reserved for "
            "test_suite.py on non-dev machines."
        )
    verify_lightdash()

    # Apply dry-run guard for full-log mode. Single-row mode keeps writes
    # enabled for Sheets (only the first row gets written) but skips Asana
    # mutations by gating Steps 4b and 5 inside run_pipeline.
    if mode["is_dry_run"]:
        set_dry_run(True)

    run_id = gen_run_id()
    set_run_id(run_id)
    recorder = build_recorder(run_id)

    print("=" * 72)
    print(f"{SWEEP_NAME}  run_id={run_id}")
    print(f"  IS_CI         = {mode['is_ci']}")
    print(f"  WRITE_ONE_ROW = {mode['is_single_row']}")
    print(f"  effective     = {'dry-run' if mode['is_dry_run'] else 'single-row' if mode['is_single_row'] else 'production'}")
    print("=" * 72)

    try:
        run_pipeline(recorder, mode)
        recorder.flush(status="success")
    except Exception as e:
        logger.exception("pipeline_failed")
        recorder.error(repr(e))
        recorder.flush(status="error")
        raise

    return 0


if __name__ == "__main__":
    sys.exit(main())
