#!/usr/bin/env python3
"""
CXOps_BPA_Normal_Claims_Status_Preflight.py
============================================
Independent component diagnostics for the claims status ETL.

Tests every external dependency the production ETL touches — Lightdash,
Google Sheets, Asana working project, Asana custom fields, Asana central
log project — without running the actual pipeline. Use this to verify
configuration on a fresh machine, after rotating creds, or when something
in production looks off.

All checks are read-only by default. Pass --write-test to also exercise the
recorder's write path (creates one disposable task in the log project).
Working data (Sheet1, working Asana project) is NEVER mutated by this file.

Usage:
    python3 CXOps_BPA_Normal_Claims_Status_Preflight.py
    python3 CXOps_BPA_Normal_Claims_Status_Preflight.py --list
    python3 CXOps_BPA_Normal_Claims_Status_Preflight.py --check lightdash_auth
    python3 CXOps_BPA_Normal_Claims_Status_Preflight.py --write-test

Exit codes:
    0   all checks passed (or skipped)
    1   one or more checks failed
    2   bad arguments
"""
import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Windows-safe console
try:
    sys.stdout.reconfigure(errors="replace")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).absolute().parent))
from test_suite import (  # noqa: E402
    load_env, env, asana_req, ld_request, ld_submit_sql, ld_fetch_results,
    has_lightdash, get_sheets_service, _resolve_sa_source,
)


# ───────────────────────────────────────────────────────────────────────────
# Result type
# ───────────────────────────────────────────────────────────────────────────
class CheckResult:
    def __init__(self, name, status, detail="", error=None):
        self.name = name
        self.status = status  # "PASS" / "FAIL" / "SKIP"
        self.detail = detail
        self.error = error

    def line(self) -> str:
        sym = {"PASS": "PASS", "FAIL": "FAIL", "SKIP": "SKIP"}[self.status]
        s = f"  [{sym}] {self.name:30s} {self.detail}"
        if self.error:
            s += f"\n         ↳ {self.error}"
        return s


def _check(name):
    """Decorator: tag a function as a check and pin its registry name."""
    def deco(fn):
        fn._check_name = name
        return fn
    return deco


# ───────────────────────────────────────────────────────────────────────────
# Checks — each is independent and idempotent
# ───────────────────────────────────────────────────────────────────────────
@_check("env_vars")
def check_env_vars(**_):
    """All required env vars are set (no value validation, just presence)."""
    required = [
        "LIGHTDASH_API_URL", "LIGHTDASH_API_KEY", "LIGHTDASH_PROJECT_UUID",
        "GOOGLE_SHEETS_SPREADSHEET_ID",
        "ASANA_PAT", "ASANA_PROJECT_GID",
        "ASANA_SECTION_NEW", "ASANA_SECTION_WOIP",
        "ASANA_FIELD_CX_OPS", "ASANA_FIELD_CLAIMS_TEXT", "ASANA_FIELD_ALL_RETURNED",
        "ASANA_OPT_WOIP", "ASANA_OPT_NEEDS_FOLLOWUP",
        "ASANA_OPT_RETURNED_YES", "ASANA_OPT_RETURNED_NO",
    ]
    optional = [
        "ASANA_LOG_PROJECT_GID", "CXOPS_NORMAL_CLAIMS_ETL_LOG_SECTION_GID",
        "ASANA_RECORDER_TASK_GID",
    ]
    missing = [k for k in required if not os.getenv(k, "").strip()]
    if missing:
        return CheckResult("env_vars", "FAIL", f"{len(missing)} missing",
                           error=", ".join(missing))

    sa_set = bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()) \
        or bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip())
    if not sa_set:
        return CheckResult("env_vars", "FAIL",
                           "GOOGLE_SERVICE_ACCOUNT_FILE or _JSON must be set")

    opt_set = sum(1 for k in optional if os.getenv(k, "").strip())
    return CheckResult("env_vars", "PASS",
                       f"{len(required)} required set; {opt_set}/{len(optional)} optional set")


@_check("lightdash_auth")
def check_lightdash_auth(**_):
    """Lightdash /api/v1/org responds 200 with the configured API key."""
    if not has_lightdash():
        return CheckResult("lightdash_auth", "SKIP", "LIGHTDASH_API_URL unset")
    url = env("LIGHTDASH_API_URL").rstrip("/") + "/api/v1/org"
    s, b = ld_request(url, env("LIGHTDASH_API_KEY"))
    if s != 200:
        return CheckResult("lightdash_auth", "FAIL", f"status {s}",
                           error=str(b)[:200])
    org = (b.get("results") or {}).get("name", "?") if isinstance(b, dict) else "?"
    return CheckResult("lightdash_auth", "PASS", f"org='{org}'")


@_check("lightdash_query")
def check_lightdash_query(**_):
    """Lightdash SQL runner round-trip — submit SELECT 1, fetch results."""
    if not has_lightdash():
        return CheckResult("lightdash_query", "SKIP", "LIGHTDASH_API_URL unset")
    api_url = env("LIGHTDASH_API_URL").rstrip("/")
    api_key = env("LIGHTDASH_API_KEY")
    project = env("LIGHTDASH_PROJECT_UUID")
    qid, err = ld_submit_sql(api_url, api_key, project, "SELECT 1 AS test")
    if err:
        return CheckResult("lightdash_query", "FAIL", "submit failed", error=err)
    rows, err = ld_fetch_results(api_url, api_key, project, qid)
    if err:
        return CheckResult("lightdash_query", "FAIL", "fetch failed", error=err)
    return CheckResult("lightdash_query", "PASS", f"returned {len(rows)} row(s)")


@_check("sheets_auth")
def check_sheets_auth(**_):
    """Service-account auth resolves AND the configured spreadsheet is accessible."""
    try:
        kind, _value = _resolve_sa_source()
    except RuntimeError as e:
        return CheckResult("sheets_auth", "FAIL", "auth source unresolved", error=str(e))
    try:
        svc = get_sheets_service()
        sid = env("GOOGLE_SHEETS_SPREADSHEET_ID")
        meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    except Exception as e:
        return CheckResult("sheets_auth", "FAIL", "metadata fetch failed",
                           error=str(e)[:300])
    title = meta.get("properties", {}).get("title", "?")
    tabs = [s.get("properties", {}).get("title") for s in meta.get("sheets", [])]
    return CheckResult("sheets_auth", "PASS",
                       f"src={kind}, title='{title}', tabs={tabs}")


@_check("sheets_read")
def check_sheets_read(**_):
    """Sheet1 has the expected header layout (row 2 has Claim Slug / clean slug etc)."""
    try:
        svc = get_sheets_service()
        sid = env("GOOGLE_SHEETS_SPREADSHEET_ID")
        result = svc.spreadsheets().values().get(
            spreadsheetId=sid, range="Sheet1!A1:O5").execute()
    except Exception as e:
        return CheckResult("sheets_read", "FAIL", "read failed", error=str(e)[:300])
    rows = result.get("values", [])
    if len(rows) < 2:
        return CheckResult("sheets_read", "FAIL",
                           f"Sheet1 has fewer than 2 rows (got {len(rows)})")
    headers = rows[1]
    expected_a_e = ["Claim Slug", "Associate", "Date added", "Asana task", "clean claim slug"]
    if headers[:5] != expected_a_e:
        return CheckResult("sheets_read", "FAIL",
                           "header row 2 does not match expected layout",
                           error=f"got A-E = {headers[:5]}")
    return CheckResult("sheets_read", "PASS",
                       f"row 2 headers OK; data rows visible: {max(0, len(rows) - 2)}")


@_check("asana_auth")
def check_asana_auth(**_):
    """Asana PAT resolves /users/me."""
    pat = os.getenv("ASANA_PAT", "").strip()
    if not pat:
        return CheckResult("asana_auth", "SKIP", "ASANA_PAT unset")
    s, b = asana_req("GET", "/users/me", pat)
    if s != 200:
        return CheckResult("asana_auth", "FAIL", f"status {s}", error=str(b)[:200])
    name = (b.get("data") or {}).get("name", "?")
    return CheckResult("asana_auth", "PASS", f"user='{name}'")


@_check("asana_project")
def check_asana_project(**_):
    """The configured Asana working project is reachable."""
    pat = os.getenv("ASANA_PAT", "").strip()
    proj = os.getenv("ASANA_PROJECT_GID", "").strip()
    if not pat or not proj:
        return CheckResult("asana_project", "SKIP", "PAT or PROJECT_GID unset")
    s, b = asana_req("GET", f"/projects/{proj}?opt_fields=name", pat)
    if s != 200:
        return CheckResult("asana_project", "FAIL", f"status {s}", error=str(b)[:200])
    name = (b.get("data") or {}).get("name", "?")
    return CheckResult("asana_project", "PASS", f"project='{name}'")


@_check("asana_sections")
def check_asana_sections(**_):
    """ASANA_SECTION_NEW and ASANA_SECTION_WOIP both exist in the project."""
    pat = os.getenv("ASANA_PAT", "").strip()
    proj = os.getenv("ASANA_PROJECT_GID", "").strip()
    new_gid = os.getenv("ASANA_SECTION_NEW", "").strip()
    woip_gid = os.getenv("ASANA_SECTION_WOIP", "").strip()
    if not all([pat, proj, new_gid, woip_gid]):
        return CheckResult("asana_sections", "SKIP",
                           "missing one of pat/project/section gids")
    s, b = asana_req("GET",
                     f"/projects/{proj}/sections?opt_fields=name,gid", pat)
    if s != 200:
        return CheckResult("asana_sections", "FAIL", f"status {s}",
                           error=str(b)[:200])
    sections = {sec["gid"]: sec.get("name", "?") for sec in b.get("data", [])}
    missing = []
    if new_gid not in sections:
        missing.append(f"NEW({new_gid})")
    if woip_gid not in sections:
        missing.append(f"WOIP({woip_gid})")
    if missing:
        return CheckResult("asana_sections", "FAIL",
                           f"{len(missing)} section(s) not found",
                           error=f"missing={missing}, available={list(sections)}")
    return CheckResult("asana_sections", "PASS",
                       f"NEW='{sections[new_gid]}', WOIP='{sections[woip_gid]}'")


@_check("asana_custom_fields")
def check_asana_custom_fields(**_):
    """All ASANA_FIELD_* / ASANA_OPT_* GIDs resolve to real custom fields and options."""
    pat = os.getenv("ASANA_PAT", "").strip()
    proj = os.getenv("ASANA_PROJECT_GID", "").strip()
    if not pat or not proj:
        return CheckResult("asana_custom_fields", "SKIP", "PAT or project unset")
    q = ("?opt_fields=custom_field.name,custom_field.gid,"
         "custom_field.enum_options.name,custom_field.enum_options.gid")
    s, b = asana_req("GET", f"/projects/{proj}/custom_field_settings{q}", pat)
    if s != 200:
        return CheckResult("asana_custom_fields", "FAIL", f"status {s}",
                           error=str(b)[:200])
    field_gids, option_gids = {}, {}
    for setting in b.get("data", []):
        cf = setting.get("custom_field", {})
        field_gids[cf.get("gid", "")] = cf.get("name", "")
        for opt in cf.get("enum_options") or []:
            option_gids[opt.get("gid", "")] = opt.get("name", "")
    expected_fields = [
        ("ASANA_FIELD_CX_OPS", "CX/OPs"),
        ("ASANA_FIELD_CLAIMS_TEXT", "Claims Text"),
        ("ASANA_FIELD_ALL_RETURNED", "All Returned"),
    ]
    expected_opts = [
        ("ASANA_OPT_WOIP", "WOIP"),
        ("ASANA_OPT_NEEDS_FOLLOWUP", "needs follow-up"),
        ("ASANA_OPT_RETURNED_YES", "Returned Yes"),
        ("ASANA_OPT_RETURNED_NO", "Returned No"),
    ]
    problems = []
    for envk, label in expected_fields:
        gid = os.getenv(envk, "").strip()
        if not gid:
            problems.append(f"{envk} unset")
        elif gid not in field_gids:
            problems.append(f"{envk}={gid} not a field on this project")
    for envk, label in expected_opts:
        gid = os.getenv(envk, "").strip()
        if not gid:
            problems.append(f"{envk} unset")
        elif gid not in option_gids:
            problems.append(f"{envk}={gid} not an enum option on this project")
    if problems:
        return CheckResult("asana_custom_fields", "FAIL",
                           f"{len(problems)} GID problem(s)",
                           error="; ".join(problems))
    return CheckResult("asana_custom_fields", "PASS",
                       f"{len(expected_fields)} fields + {len(expected_opts)} options verified")


@_check("asana_log_project")
def check_asana_log_project(**_):
    """Central log project + section UUIDs (Mode B recorder destination) resolve."""
    pat = os.getenv("ASANA_PAT", "").strip()
    log_proj = os.getenv("ASANA_LOG_PROJECT_GID", "").strip()
    log_sec = os.getenv("CXOPS_NORMAL_CLAIMS_ETL_LOG_SECTION_GID", "").strip()
    if not log_proj and not log_sec:
        return CheckResult("asana_log_project", "SKIP",
                           "ASANA_LOG_PROJECT_GID + section unset (recorder will print to stdout)")
    if not pat:
        return CheckResult("asana_log_project", "FAIL", "ASANA_PAT unset")
    proj_name = sec_name = "(unset)"
    if log_proj:
        s, b = asana_req("GET", f"/projects/{log_proj}?opt_fields=name", pat)
        if s != 200:
            return CheckResult("asana_log_project", "FAIL",
                               f"log project unreachable ({s})",
                               error=str(b)[:200])
        proj_name = (b.get("data") or {}).get("name", "?")
    if log_sec:
        s, b = asana_req("GET", f"/sections/{log_sec}?opt_fields=name", pat)
        if s != 200:
            return CheckResult("asana_log_project", "FAIL",
                               f"log section unreachable ({s})",
                               error=str(b)[:200])
        sec_name = (b.get("data") or {}).get("name", "?")
    return CheckResult("asana_log_project", "PASS",
                       f"project='{proj_name}', section='{sec_name}'")


@_check("recorder_write_test")
def check_recorder_write_test(write_test=False, **_):
    """Real write — create a disposable task in the log project section.
    Skipped unless --write-test is passed. Working data is NOT touched."""
    if not write_test:
        return CheckResult("recorder_write_test", "SKIP",
                           "use --write-test to enable real write")
    pat = os.getenv("ASANA_PAT", "").strip()
    log_proj = os.getenv("ASANA_LOG_PROJECT_GID", "").strip()
    log_sec = os.getenv("CXOPS_NORMAL_CLAIMS_ETL_LOG_SECTION_GID", "").strip()
    if not all([pat, log_proj, log_sec]):
        return CheckResult("recorder_write_test", "SKIP",
                           "log project/section not fully configured")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    s, b = asana_req("POST", "/tasks", pat, body={
        "name": f"[preflight] connectivity test {ts}",
        "notes": "Created by CXOps_BPA_Normal_Claims_Status_Preflight.py — safe to delete.",
        "projects": [log_proj],
        "memberships": [{"project": log_proj, "section": log_sec}],
    })
    if s not in (200, 201):
        return CheckResult("recorder_write_test", "FAIL",
                           f"create failed ({s})", error=str(b)[:200])
    new_gid = (b.get("data") or {}).get("gid", "?")
    return CheckResult("recorder_write_test", "PASS",
                       f"created log task gid={new_gid}")


# ───────────────────────────────────────────────────────────────────────────
# Driver
# ───────────────────────────────────────────────────────────────────────────
CHECKS = [
    check_env_vars,
    check_lightdash_auth,
    check_lightdash_query,
    check_sheets_auth,
    check_sheets_read,
    check_asana_auth,
    check_asana_project,
    check_asana_sections,
    check_asana_custom_fields,
    check_asana_log_project,
    check_recorder_write_test,
]


def main():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--check",
                   help="Run only this single check (use --list to see names)")
    p.add_argument("--list", action="store_true",
                   help="List available check names")
    p.add_argument("--write-test", action="store_true",
                   help="Enable the recorder write test (creates one log task)")
    args = p.parse_args()

    load_env()

    if args.list:
        print("Available checks:")
        for fn in CHECKS:
            first_doc = (fn.__doc__ or "").strip().splitlines()[0]
            print(f"  {fn._check_name:25s}  {first_doc}")
        return 0

    selected = CHECKS
    if args.check:
        selected = [fn for fn in CHECKS if fn._check_name == args.check]
        if not selected:
            print(f"Unknown check: {args.check}", file=sys.stderr)
            print(f"Available: {[fn._check_name for fn in CHECKS]}", file=sys.stderr)
            return 2

    print("=" * 78)
    print("CXOps BPA Normal Claims Status — Preflight")
    if args.write_test:
        print("  --write-test is ON: recorder write test will create a real task")
    print("=" * 78)

    results = []
    for fn in selected:
        try:
            r = fn(write_test=args.write_test)
        except Exception as e:
            r = CheckResult(fn._check_name, "FAIL", "uncaught exception",
                            error=str(e)[:300])
        results.append(r)
        print(r.line())

    print("=" * 78)
    pass_n = sum(1 for r in results if r.status == "PASS")
    fail_n = sum(1 for r in results if r.status == "FAIL")
    skip_n = sum(1 for r in results if r.status == "SKIP")
    print(f"  Total: {len(results)}  Pass: {pass_n}  Fail: {fail_n}  Skip: {skip_n}")
    print("=" * 78)
    return 0 if fail_n == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
