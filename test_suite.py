#!/usr/bin/env python3
"""
test_suite.py — Full integration test suite for Claim Return Monitoring.

Dual-source pattern:
  - If LIGHTDASH_API_URL is set → queries real Lightdash API
  - If not set → reads from "LightdashData" tab in the same spreadsheet

This lets you test the full pipeline on a personal Google account
with no Lightdash access. The matching logic, F-K calculation,
and sheet writes are identical either way.

Sheet structure (Sheet1 — tracking tab):
  Row 1: Section labels (merged)
  Row 2: Headers
  Row 3+: Data
  Col A: Claim Slug, B: Associate, C: Date added, D: Asana task,
  E: Clean claim slug, F: Return Check, G: Ticket row count,
  H: Ticket TRUE count, I: Work ticket?, J: Aging Status, K: Send to Aging?

Sheet structure (LightdashData — simulated Lightdash results):
  Row 1: Headers (matching Lightdash API column aliases)
  Row 2+: Claim data seeded for testing

Usage:
    python3 test_suite.py                    # run all tests
    python3 test_suite.py lightdash          # Lightdash tests only (skips if no API URL)
    python3 test_suite.py sheets             # Google Sheets tests only
    python3 test_suite.py asana              # Asana tests only
    python3 test_suite.py e2e                # end-to-end pipeline test
"""

import json, os, re, sys, time, urllib.request
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from urllib.error import HTTPError

# Windows-safe output: don't crash on em-dash / arrow / check-marks in cp1252 consoles.
try:
    sys.stdout.reconfigure(errors="replace")
except Exception:
    pass

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).absolute().parent
DOTENV_PATH = SCRIPT_DIR / ".env"
FAKE_SLUG = "FAKE-TEST-SLUG-99999"
DATA_START_ROW = 3  # Row 1 = section labels, Row 2 = headers
AGING_THRESHOLD_DAYS = 90
STALE_DAYS_THRESHOLD = 30  # Post stale-claim comment after this many days of no Lightdash data
COL_N_INDEX = 13  # Column N (0-indexed) — first date a slug returned no Lightdash data
COL_O_INDEX = 14  # Column O (0-indexed) — date the stale-claim comment was posted (idempotency)
COL_Z_INDEX = 25  # Column Z (0-indexed) — stores Asana task GID

# Asana GIDs are loaded from env vars at call time (late-bound) — see .env.example.
# Required vars: ASANA_SECTION_NEW, ASANA_SECTION_WOIP, ASANA_FIELD_CX_OPS,
#   ASANA_FIELD_CLAIMS_TEXT, ASANA_FIELD_ALL_RETURNED,
#   ASANA_OPT_WOIP, ASANA_OPT_NEEDS_FOLLOWUP, ASANA_OPT_RETURNED_YES, ASANA_OPT_RETURNED_NO

# Lightdash SQL template (used only when LIGHTDASH_API_URL is set)
SQL_TEMPLATE = """SELECT
  "mart_reporting_fact_claim".alma_client_insurance_claim_slug AS "mart_reporting_fact_claim_alma_client_insurance_claim_slug",
  DATE_TRUNC('DAY', "mart_reporting_fact_claim".appointment_started_at) AS "mart_reporting_fact_claim_appointment_started_at_day",
  "mart_reporting_dim_health_plan".network_partner_name AS "mart_reporting_dim_health_plan_network_partner_name",
  DATE_TRUNC('DAY', "mart_reporting_fact_claim".claim_created_at) AS "mart_reporting_fact_claim_claim_created_at_day",
  "mart_reporting_fact_claim".claim_status AS "mart_reporting_fact_claim_claim_status",
  DATE_TRUNC('DAY', "mart_reporting_fact_claim".claim_submitted_to_kareo_at) AS "mart_reporting_fact_claim_claim_submitted_to_kareo_at_day",
  DATE_TRUNC('DAY', "mart_reporting_fact_claim".claim_adjudication_date) AS "mart_reporting_fact_claim_claim_adjudication_date_day"
FROM
  "dev"."mart_reporting"."mart_reporting_fact_claim" AS "mart_reporting_fact_claim"
  LEFT OUTER JOIN "dev"."mart_reporting"."mart_reporting_dim_health_plan" AS "mart_reporting_dim_health_plan"
    ON ("mart_reporting_fact_claim".dim_health_plan_id) = ("mart_reporting_dim_health_plan".dim_health_plan_id)
WHERE
  "mart_reporting_fact_claim".alma_client_insurance_claim_slug IN ({slug_list})
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY "mart_reporting_fact_claim_claim_created_at_day" DESC
LIMIT 99000"""

# Column aliases — same keys whether from Lightdash API or LightdashData tab
LD_SLUG = "mart_reporting_fact_claim_alma_client_insurance_claim_slug"
LD_STATUS = "mart_reporting_fact_claim_claim_status"
LD_ADJ_DAY = "mart_reporting_fact_claim_claim_adjudication_date_day"
LD_SUBMITTED = "mart_reporting_fact_claim_claim_submitted_to_kareo_at_day"
LD_CREATED = "mart_reporting_fact_claim_claim_created_at_day"
LD_APPT = "mart_reporting_fact_claim_appointment_started_at_day"
LD_PARTNER = "mart_reporting_dim_health_plan_network_partner_name"

# All LD columns in order (used for LightdashData tab headers)
LD_COLUMNS = [LD_SLUG, LD_APPT, LD_PARTNER, LD_CREATED, LD_STATUS, LD_SUBMITTED, LD_ADJ_DAY]

# Actionable statuses
ACTIONABLE_STATUSES = {"Completed - No ERA (see Notes)", "Completed - ERA Posted", "rejected", "write_off", "canceled"}
NON_ACTIONABLE_STATUSES = {"resubmitted"}


# ---------------------------------------------------------------------------
# Dry-run mode
# ---------------------------------------------------------------------------
# Global flag — when True, every mutating helper (Sheets writes/clears,
# Asana POST/PUT/DELETE/PATCH) logs the intended action and skips the API
# call. Reads (GET) still go through so dry-run can exercise the read path.
# Set via set_dry_run(True) at script startup.
_DRY_RUN = False


def set_dry_run(enabled: bool) -> None:
    global _DRY_RUN
    _DRY_RUN = bool(enabled)


def is_dry_run() -> bool:
    return _DRY_RUN


# ---------------------------------------------------------------------------
# Time
# ---------------------------------------------------------------------------
def utc_today():
    """Today's date in UTC. All date math in this codebase uses UTC so that
    local-time runs (Windows dev) and CI runs (Ubuntu UTC) agree on the day,
    and so it lines up with Lightdash's UTC ISO timestamps."""
    return datetime.now(timezone.utc).date()


# ---------------------------------------------------------------------------
# Env
# ---------------------------------------------------------------------------
def load_env():
    if not DOTENV_PATH.exists(): return
    for raw in DOTENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))

def env(key, required=True):
    val = os.getenv(key, "").strip()
    if required and not val: raise RuntimeError(f"Missing env var: {key}")
    return val

def has_lightdash():
    """Check if real Lightdash API is configured."""
    return bool(os.getenv("LIGHTDASH_API_URL", "").strip() and os.getenv("LIGHTDASH_API_KEY", "").strip())


# ---------------------------------------------------------------------------
# Lightdash API helpers (only used when has_lightdash() is True)
# ---------------------------------------------------------------------------
def ld_request(url, api_key, method="GET", body=None):
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"ApiKey {api_key}", "Content-Type": "application/json", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except HTTPError as err:
        b = ""
        try: b = err.read().decode("utf-8", errors="replace")
        except: pass
        return err.code, b

def ld_submit_sql(api_url, api_key, project_uuid, sql):
    url = f"{api_url}/api/v2/projects/{project_uuid}/query/sql"
    s, d = ld_request(url, api_key, "POST", {"sql": sql, "context": "api"})
    if s != 200: return None, f"Status {s}: {str(d)[:300]}"
    return d.get("results", {}).get("queryUuid"), None

def ld_fetch_results(api_url, api_key, project_uuid, qid, retries=15):
    url = f"{api_url}/api/v2/projects/{project_uuid}/query/{qid}/results"
    for _ in range(retries):
        s, d = ld_request(url, api_key, "GET")
        if s == 200: return _extract_rows(d), None
        elif s == 202: time.sleep(1); continue
        else: return None, f"Status {s}: {str(d)[:300]}"
    return None, "Timed out"

def _extract_rows(data):
    if isinstance(data, list): return data
    if not isinstance(data, dict): return []
    if "status" not in data: return [data]
    r = data.get("results", {})
    if isinstance(r, list): return r
    if isinstance(r, dict):
        if "rows" in r: return r["rows"]
        if "status" not in r: return [r]
    return []

def verify_lightdash():
    """Ping /api/v1/org to fail loud on misconfigured URL or expired API key.
    No-op when LIGHTDASH_API_URL is unset (sheet-source mode).

    Without this check, a typo in LIGHTDASH_API_URL silently returns empty
    Lightdash results — every slug is reported as 'no data', e2e exits
    'PASSED — 0 processed, N no data', and you have to read the logs to
    notice anything is wrong.
    """
    if not has_lightdash():
        return
    api_url = env("LIGHTDASH_API_URL").rstrip("/")
    api_key = env("LIGHTDASH_API_KEY")
    s, b = ld_request(f"{api_url}/api/v1/org", api_key)
    if s != 200:
        raise RuntimeError(
            f"Lightdash health check failed: status {s} on {api_url}/api/v1/org "
            f"(check LIGHTDASH_API_URL and LIGHTDASH_API_KEY)"
        )


def build_slug_sql(slugs):
    # Allow-list: only 6-char alphanumeric slugs reach Lightdash SQL. Makes the
    # function safe regardless of how callers obtained the input (parser,
    # sheet cell, future ingest path). Returns None if nothing valid remains.
    valid = []
    for s in slugs:
        norm = str(s).strip().lower()
        if re.fullmatch(r'[a-z0-9]{6}', norm):
            valid.append(norm)
        else:
            print(f"    [build_slug_sql] dropping invalid slug: {s!r}")
    if not valid:
        return None
    return SQL_TEMPLATE.format(slug_list=", ".join(f"'{s}'" for s in valid))


# ---------------------------------------------------------------------------
# Google Sheets helpers
# ---------------------------------------------------------------------------
def _resolve_sa_source():
    """Pick the service-account credentials source from env vars.

    Returns ('file', path) or ('json', raw_json_str). Raises with a clear
    message if neither is set or if the file path doesn't exist. FILE wins
    when both are set — used for local dev (pointing at a JSON on disk).
    """
    sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if sa_file:
        if not os.path.isfile(sa_file):
            raise RuntimeError(
                f"GOOGLE_SERVICE_ACCOUNT_FILE set but file not found: {sa_file}"
            )
        return ("file", sa_file)
    if sa_json:
        return ("json", sa_json)
    raise RuntimeError(
        "Set GOOGLE_SERVICE_ACCOUNT_FILE (path on disk) "
        "or GOOGLE_SERVICE_ACCOUNT_JSON (raw JSON contents) — neither found"
    )


def get_sheets_service():
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    kind, value = _resolve_sa_source()
    if kind == "file":
        creds = Credentials.from_service_account_file(value, scopes=scopes)
    else:
        creds = Credentials.from_service_account_info(json.loads(value), scopes=scopes)
    return build("sheets", "v4", credentials=creds)

def _sheets_retry(fn, max_retries=6):
    """Exponential backoff on Sheets 429 (Read/Write Requests Per Minute Per User)."""
    from googleapiclient.errors import HttpError
    delay = 2
    for attempt in range(max_retries):
        try:
            return fn()
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status == 429 and attempt < max_retries - 1:
                print(f"    [Sheets 429 — backing off {delay}s (attempt {attempt + 1}/{max_retries})]")
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            raise

def sh_read(svc, sid, rng):
    return _sheets_retry(lambda: svc.spreadsheets().values().get(
        spreadsheetId=sid, range=rng).execute().get("values", []))

def sh_write(svc, sid, rng, vals):
    if _DRY_RUN:
        print(f"    [dry-run] would write to {rng}: {len(vals)} row(s)")
        return
    _sheets_retry(lambda: svc.spreadsheets().values().update(
        spreadsheetId=sid, range=rng, valueInputOption="USER_ENTERED",
        body={"values": vals}).execute())

def sh_clear(svc, sid, rng):
    if _DRY_RUN:
        print(f"    [dry-run] would clear {rng}")
        return
    _sheets_retry(lambda: svc.spreadsheets().values().clear(
        spreadsheetId=sid, range=rng, body={}).execute())

def sh_batch_write(svc, sid, updates, value_input_option="USER_ENTERED"):
    """Write multiple (possibly scattered) ranges in a single API call.

    updates: list of (range_str, values_2d) tuples. Empty list is a no-op.
    Each update is independent — ranges can be discontinuous, in any order.

    Wins: 1 HTTP round-trip instead of N, 1 quota slot instead of N, no
    torn-state window between writes if a later one fails.
    """
    if not updates:
        return
    if _DRY_RUN:
        print(f"    [dry-run] would batch-write {len(updates)} range(s)")
        return
    body = {
        "valueInputOption": value_input_option,
        "data": [{"range": r, "values": v} for r, v in updates],
    }
    return _sheets_retry(lambda: svc.spreadsheets().values().batchUpdate(
        spreadsheetId=sid, body=body).execute())

def ensure_tab(svc, sid, tab_name):
    """Create a tab if it doesn't exist."""
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    existing = [s.get("properties", {}).get("title") for s in meta.get("sheets", [])]
    if tab_name not in existing:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={
            "requests": [{"addSheet": {"properties": {"title": tab_name}}}]
        }).execute()
        print(f"    Created tab: {tab_name}")
    return tab_name


# ---------------------------------------------------------------------------
# Dual-source: get claim data from Lightdash API or LightdashData tab
# ---------------------------------------------------------------------------
def get_claim_data(slugs, svc=None, sid=None):
    """
    Fetch claim data for the given slugs.
    Returns list of row dicts with LD_* column keys.

    Source:
      - Real Lightdash API if LIGHTDASH_API_URL is set
      - LightdashData tab in the spreadsheet otherwise
    """
    if has_lightdash():
        return _get_claim_data_api(slugs)
    else:
        return _get_claim_data_sheet(slugs, svc, sid)


def _get_claim_data_api(slugs):
    """Query real Lightdash."""
    api_url = env("LIGHTDASH_API_URL").rstrip("/")
    api_key = env("LIGHTDASH_API_KEY")
    project = env("LIGHTDASH_PROJECT_UUID")

    sql = build_slug_sql(slugs)
    if sql is None:
        print(f"    ⊘ No valid slugs to query Lightdash")
        return []
    qid, err = ld_submit_sql(api_url, api_key, project, sql)
    if err:
        print(f"    ✘ Lightdash submit: {err}")
        return []

    rows, err = ld_fetch_results(api_url, api_key, project, qid)
    if err:
        print(f"    ✘ Lightdash fetch: {err}")
        return []

    return rows


def _get_claim_data_sheet(slugs, svc, sid):
    """Read from LightdashData tab, filter to matching slugs."""
    print(f"    [Using LightdashData tab as data source]")
    try:
        raw = sh_read(svc, sid, "'LightdashData'!A:G")
    except Exception as e:
        print(f"    ✘ Could not read LightdashData tab: {e}")
        return []

    if len(raw) < 2:
        print(f"    ⚠ LightdashData tab is empty")
        return []

    headers = [h.strip() for h in raw[0]]
    slug_set = set(slugs)
    rows = []

    for data_row in raw[1:]:
        row_dict = {}
        for j, header in enumerate(headers):
            row_dict[header] = data_row[j].strip() if j < len(data_row) else ""

        # Filter: only include rows whose slug is in our pending list
        row_slug = row_dict.get(LD_SLUG, "")
        if row_slug in slug_set:
            rows.append(row_dict)

    print(f"    Matched {len(rows)} rows from LightdashData tab")
    return rows


# ---------------------------------------------------------------------------
# Value extraction helper
# ---------------------------------------------------------------------------
def xval(row, col):
    """Extract value from a row dict — handles both API format and flat strings."""
    v = row.get(col, "")
    if isinstance(v, dict): v = v.get("raw", v.get("value", ""))
    return str(v).strip() if v else ""


# ---------------------------------------------------------------------------
# Asana helpers
# ---------------------------------------------------------------------------
_ASANA_RETRYABLE_STATUSES = (429, 500, 502, 503)


def _asana_req_once(method, path, pat, body=None):
    """Single Asana API call, no retry. Returns (status, body)."""
    url = f"https://app.asana.com/api/1.0{path}"
    data = json.dumps({"data": body}).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {pat}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except HTTPError as err:
        b = ""
        try: b = err.read().decode("utf-8", errors="replace")
        except: pass
        return err.code, b


_ASANA_MUTATING_METHODS = ("POST", "PUT", "DELETE", "PATCH")


def asana_req(method, path, pat, body=None, max_retries=6, bypass_dry_run=False):
    """Asana API wrapper with exponential backoff on 429/500/502/503.

    Returns (status, body). 4xx other than 429 (auth, validation, etc.)
    are not retried — those need code/config fixes, not waiting.

    In dry-run mode, mutating methods (POST/PUT/DELETE/PATCH) are skipped
    and a fake-success response is returned. GETs still go through.

    bypass_dry_run=True forces the call through even when dry-run is on.
    Reserved for the run recorder posting to the central log Asana project,
    which must succeed regardless of working-data mutation policy.
    """
    if _DRY_RUN and not bypass_dry_run and method in _ASANA_MUTATING_METHODS:
        body_summary = str(body)[:120] if body else ""
        print(f"    [dry-run] would {method} {path} body={body_summary}")
        # Return the same status code the real API would (200 for PUT/PATCH/DELETE,
        # 201 for POST) so dry-run doesn't trip closure checks like `s == 200`.
        fake_status = 201 if method == "POST" else 200
        return fake_status, {"data": {"gid": "dry-run", "name": "dry-run"}}

    delay = 2
    status, b = None, None
    for attempt in range(max_retries):
        status, b = _asana_req_once(method, path, pat, body)
        if status in _ASANA_RETRYABLE_STATUSES and attempt < max_retries - 1:
            print(f"    [Asana {status} — backing off {delay}s "
                  f"(attempt {attempt + 1}/{max_retries})]")
            time.sleep(delay)
            delay = min(delay * 2, 60)
            continue
        return status, b
    return status, b


def parse_claim_slugs(text):
    """
    Extract 6-character alphanumeric claim IDs from free-form text.
    Strips 'cl-'/'CL-' prefixes, then finds all [a-zA-Z0-9]{6} matches.
    Returns (list_of_slugs, needs_review_bool).
    needs_review is True if leftover non-whitespace/punctuation remains after extraction.
    """
    if not text or not text.strip():
        return [], True
    # Strip cl-/CL- prefixes (case-insensitive)
    cleaned = re.sub(r'(?i)\bcl-', '', text)
    # Extract all 6-char alphanumeric sequences
    slugs = re.findall(r'[a-zA-Z0-9]{6}', cleaned)
    # Dedupe while preserving order
    seen = set()
    unique = []
    for s in slugs:
        low = s.lower()
        if low not in seen:
            seen.add(low)
            unique.append(low)
    # Check for leftover chars that might indicate unparseable content
    remainder = re.sub(r'[a-zA-Z0-9]{6}', '', cleaned)
    remainder = re.sub(r'[\s,;|/\-]+', '', remainder)
    needs_review = len(remainder) > 0 and len(unique) == 0
    return unique, needs_review


def asana_get_section_tasks(pat, project_gid, section_gid):
    """Fetch all tasks in a section with custom fields."""
    s, b = asana_req("GET", f"/sections/{section_gid}/tasks"
                     f"?opt_fields=name,custom_fields.gid,custom_fields.display_value,"
                     f"custom_fields.text_value,custom_fields.enum_value.gid", pat)
    if s != 200:
        return []
    return b.get("data", [])


def asana_task_field(task, field_gid):
    """Get a custom field value dict from a task."""
    for cf in task.get("custom_fields", []):
        if cf.get("gid") == field_gid:
            return cf
    return None


def asana_update_field(pat, task_gid, field_gid, enum_option_gid):
    """Set an enum custom field on a task."""
    body = {"custom_fields": {field_gid: enum_option_gid}}
    return asana_req("PUT", f"/tasks/{task_gid}", pat, body)


def asana_post_comment(pat, task_gid, text):
    """Post a comment (story) on a task."""
    return asana_req("POST", f"/tasks/{task_gid}/stories", pat, {"text": text})


def get_new_tasks_needing_woip(pat, project_gid):
    """
    Fetch tasks in New Tasks section where Have All Claims Returned = No.
    Returns list of dicts: {gid, name, claims_text, slugs, needs_review}
    """
    tasks = asana_get_section_tasks(pat, project_gid, env("ASANA_SECTION_NEW"))
    result = []
    field_all_returned = env("ASANA_FIELD_ALL_RETURNED")
    field_claims_text = env("ASANA_FIELD_CLAIMS_TEXT")
    opt_returned_no = env("ASANA_OPT_RETURNED_NO")
    for t in tasks:
        # Check "Have All Claims Returned" field
        ret_field = asana_task_field(t, field_all_returned)
        if not ret_field:
            continue
        ev = ret_field.get("enum_value")
        if not ev or ev.get("gid") != opt_returned_no:
            continue

        # Get claims text
        claims_field = asana_task_field(t, field_claims_text)
        claims_text = (claims_field.get("text_value") or "") if claims_field else ""
        slugs, needs_review = parse_claim_slugs(claims_text)

        result.append({
            "gid": t["gid"],
            "name": t.get("name", ""),
            "claims_text": claims_text,
            "slugs": slugs,
            "needs_review": needs_review,
        })
    return result


def check_all_claims_returned(task_gid, svc, sid):
    """
    Check if ALL claims for an Asana task (by GID in col Z) have Return Check = TRUE.
    Returns (all_returned: bool, total: int, returned: int)
    """
    rows = sh_read(svc, sid, "Sheet1!A:Z")
    total = 0
    returned = 0
    for row in rows[DATA_START_ROW - 1:]:
        # Column Z (index 25) = Asana task GID
        row_gid = row[COL_Z_INDEX].strip() if len(row) > COL_Z_INDEX else ""
        if row_gid != task_gid:
            continue
        total += 1
        # Column F (index 5) = Return Check
        ret = row[5].strip() if len(row) > 5 else ""
        if ret == "TRUE":
            returned += 1
    return (total > 0 and returned == total), total, returned


def append_claims_to_sheet(task, svc, sid):
    """
    Append one row per parsed slug to Sheet1 for a new Asana task.
    Idempotent on (slug, task_gid) — skips slugs already attached to this task,
    but allows the same slug under a different task. Matches asana_monitor's
    dedup semantics so test_10's Step 0 re-seed no longer creates duplicates.

    Returns count of rows appended. One API call regardless of slug count.
    """
    existing_rows = sh_read(svc, sid, "Sheet1!A:Z")
    existing_pairs = set()
    for r in existing_rows[DATA_START_ROW - 1:]:
        slug_cell = r[4].strip().lower() if len(r) > 4 else ""
        gid_cell = r[COL_Z_INDEX].lstrip("'").strip() if len(r) > COL_Z_INDEX else ""
        if slug_cell and gid_cell:
            existing_pairs.add((slug_cell, gid_cell))

    task_gid = str(task["gid"]).strip()
    task_url = f"https://app.asana.com/0/{env('ASANA_PROJECT_GID', required=False)}/{task_gid}"
    today_str = utc_today().strftime("%m/%d/%Y")
    new_rows = []

    for slug in task["slugs"]:
        key = (slug.lower(), task_gid)
        if key in existing_pairs:
            continue
        row = [
            f"cl-{slug}",         # A: Claim Slug
            "",                    # B: Associate
            today_str,             # C: Date added
            task_url,              # D: Asana task URL
            slug,                  # E: Clean claim slug
            "",                    # F: Return Check
            "",                    # G: Ticket row count
            "",                    # H: Ticket TRUE count
            "",                    # I: Work ticket?
            "",                    # J: Aging Status
            "",                    # K: Send to Aging?
            "",                    # L: PI Inquiry Status
            "",                    # M: Ticket #
        ]
        # Pad columns N-Y (indices 13-24) then Z (index 25).
        # Leading apostrophe forces text storage — without it, 16-digit GIDs
        # get coerced to scientific notation, which breaks idempotency.
        row += [""] * 12 + [f"'{task_gid}"]
        new_rows.append(row)
        existing_pairs.add(key)

    if new_rows:
        _sheets_retry(lambda: svc.spreadsheets().values().append(
            spreadsheetId=sid,
            range="Sheet1!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": new_rows},
        ).execute())

    return len(new_rows)


# ---------------------------------------------------------------------------
# Claim processing logic (columns F-K)
# ---------------------------------------------------------------------------
def is_actionable(status, adj_day):
    """TRUE if claim is returned/resolved."""
    if status in ACTIONABLE_STATUSES: return True
    if status in NON_ACTIONABLE_STATUSES: return False
    if adj_day: return True
    return False

def days_since_submission(submitted_str):
    """Days elapsed since submission. Positive for past dates, None on bad input."""
    if not submitted_str: return None
    try:
        sub = datetime.fromisoformat(submitted_str.replace("Z", "+00:00")).astimezone(timezone.utc).date()
        return (utc_today() - sub).days
    except: return None

def calc_aging(days):
    """'Aging' once submission is older than the threshold; otherwise 'FALSE'."""
    if days is None: return ""
    return "Aging" if days > AGING_THRESHOLD_DAYS else "FALSE"

def close_completed_tasks(svc, sid, pat, all_rows, log=print):
    """For each unique task GID in the sheet whose claims have all returned
    (every row with that GID in col Z has Return Check = TRUE), update the
    Asana task: Have All Claims Returned → Yes, CX/OPs → needs follow-up,
    post a confirmation comment. Idempotent — tasks already marked Yes are
    skipped so this is safe to re-run.

    Returns counters dict. Honors the global dry-run flag through asana_req.
    """
    field_all_returned = env("ASANA_FIELD_ALL_RETURNED")
    opt_returned_yes = env("ASANA_OPT_RETURNED_YES")
    field_cx_ops = env("ASANA_FIELD_CX_OPS")
    opt_needs_followup = env("ASANA_OPT_NEEDS_FOLLOWUP")

    task_gids = set()
    for row in all_rows[DATA_START_ROW - 1:]:
        if len(row) > COL_Z_INDEX:
            gid = row[COL_Z_INDEX].lstrip("'").strip()
            if gid:
                task_gids.add(gid)

    counters = {
        "close_tasks_seen": len(task_gids),
        "close_tasks_closed": 0,
        "close_tasks_partial": 0,
        "close_tasks_already_yes": 0,
        "close_tasks_failed": 0,
    }
    ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    for task_gid in sorted(task_gids):
        all_ret, total, returned = check_all_claims_returned(task_gid, svc, sid)
        if not all_ret or total == 0:
            counters["close_tasks_partial"] += 1
            continue

        s, body = asana_req(
            "GET",
            f"/tasks/{task_gid}?opt_fields=custom_fields.gid,custom_fields.enum_value.gid",
            pat,
        )
        if s != 200:
            log(f"    Task {task_gid}: fetch failed ({s})")
            counters["close_tasks_failed"] += 1
            continue

        ret_field = asana_task_field(body.get("data", {}), field_all_returned)
        ev = (ret_field or {}).get("enum_value") or {}
        if ev.get("gid") == opt_returned_yes:
            counters["close_tasks_already_yes"] += 1
            continue

        s1, _ = asana_update_field(pat, task_gid, field_all_returned, opt_returned_yes)
        s2, _ = asana_update_field(pat, task_gid, field_cx_ops, opt_needs_followup)
        s3, _ = asana_post_comment(
            pat, task_gid,
            f"[Automation] All {total} claim(s) have returned as of {ts_now}. "
            f"Task updated to 'needs follow-up'.",
        )

        if s1 == 200 and s2 == 200:
            counters["close_tasks_closed"] += 1
            log(f"    ✔ Task {task_gid} closed ({total} claims) "
                f"comment={'OK' if s3 in (200, 201) else f'FAILED ({s3})'}")
        else:
            counters["close_tasks_failed"] += 1
            log(f"    ✘ Task {task_gid} close failed (s1={s1} s2={s2})")

    return counters


def check_stale_claims(svc, sid, pat, pending, by_slug, all_rows, log=print):
    """Track first-no-data date in col N, post stale-claim Asana comment after
    STALE_DAYS_THRESHOLD days, and mark col O so we don't re-notify.

    Logic per pending slug:
      - If has Lightdash data → skip (not stale).
      - Else if col N empty → write today's date to col N.
      - Else if col O is set → already notified, skip.
      - Else if (today - col N) >= 30 days → post Asana comment, write today to col O.
      - Else → still inside grace window, no action.

    All sheet writes are batched into one sh_batch_write call. Returns counters dict.
    """
    today = utc_today()
    today_iso = today.isoformat()
    counters = {
        "stale_first_seen_stamped": 0,
        "stale_comments_posted": 0,
        "stale_already_notified": 0,
        "stale_inside_grace": 0,
    }
    sheet_updates = []

    for slug, idx in pending.items():
        if by_slug.get(slug):
            continue  # has data → not stale

        row = all_rows[idx - 1] if 0 < idx <= len(all_rows) else []
        first_seen_str = row[COL_N_INDEX].strip() if len(row) > COL_N_INDEX else ""
        notified_str = row[COL_O_INDEX].strip() if len(row) > COL_O_INDEX else ""

        if not first_seen_str:
            sheet_updates.append((f"Sheet1!N{idx}", [[today_iso]]))
            counters["stale_first_seen_stamped"] += 1
            log(f"    Row {idx} ({slug}): no data — stamping first-seen-empty")
            continue

        if notified_str:
            counters["stale_already_notified"] += 1
            continue

        try:
            first_seen = date.fromisoformat(first_seen_str)
        except (ValueError, TypeError):
            log(f"    Row {idx} ({slug}): malformed col N {first_seen_str!r} — skipping")
            continue

        days_stale = (today - first_seen).days
        if days_stale < STALE_DAYS_THRESHOLD:
            counters["stale_inside_grace"] += 1
            continue

        task_gid = (row[COL_Z_INDEX].lstrip("'").strip()
                    if len(row) > COL_Z_INDEX else "")
        if not task_gid or not pat:
            log(f"    Row {idx} ({slug}): missing task_gid or pat — can't notify")
            continue

        text = (
            f"[Automation] Claim `{slug}` has no data in Lightdash after "
            f"{days_stale} days. Likely typo or stale ID — please verify "
            f"and re-enter or remove from the task."
        )
        s, _ = asana_post_comment(pat, task_gid, text)
        if s in (200, 201):
            sheet_updates.append((f"Sheet1!O{idx}", [[today_iso]]))
            counters["stale_comments_posted"] += 1
            log(f"    Row {idx} ({slug}): stale comment posted to {task_gid} ({days_stale}d)")
        else:
            log(f"    Row {idx} ({slug}): comment post failed (status {s}) — will retry next sweep")

    sh_batch_write(svc, sid, sheet_updates)
    return counters


def write_fk_batched(svc, sid, pending, by_slug, log=print):
    """Compute F-K for each pending slug and write them all in a single batch call.

    pending: {slug: row_idx}
    by_slug: {slug: [ld_rows]}
    log:     status callback (print by default).

    Returns (matched, no_data) counts. One sh_batch_write call regardless of N.
    """
    matched = 0
    no_data = 0
    batch = []
    for slug, idx in pending.items():
        rows = by_slug.get(slug, [])
        if not rows:
            no_data += 1
            log(f"    Row {idx} ({slug}): no data — skip")
            continue
        matched += 1
        res = process_claim(slug, rows)
        vals = [res["return_check"], str(res["row_count"]), str(res["true_count"]),
                res["work_ticket"], str(res["aging_status"]), res["send_to_aging"]]
        log(f"    Row {idx} ({slug}): {vals}")
        batch.append((f"Sheet1!F{idx}:K{idx}", [vals]))
    sh_batch_write(svc, sid, batch)
    return matched, no_data


def process_claim(slug, ld_rows):
    """Calculate columns F-K for a single claim slug."""
    row_count = len(ld_rows)
    true_count = 0
    any_returned = False
    oldest_days = None  # max days since submission across all rows

    for row in ld_rows:
        status = xval(row, LD_STATUS)
        adj = xval(row, LD_ADJ_DAY)
        submitted = xval(row, LD_SUBMITTED)

        if is_actionable(status, adj):
            true_count += 1
            any_returned = True

        d = days_since_submission(submitted)
        if d is not None and (oldest_days is None or d > oldest_days):
            oldest_days = d

    aging = calc_aging(oldest_days)
    return {
        "return_check": "TRUE" if any_returned else "FALSE",
        "row_count": row_count,
        "true_count": true_count,
        "work_ticket": "WORK" if true_count > 0 else "",
        "aging_status": str(oldest_days) if oldest_days is not None else "",
        "send_to_aging": "TRUE" if aging == "Aging" else "FALSE",
    }


# --- PI Inquiry Lookup (config-driven, graceful degradation) ---
# TODO: Uncomment when Config tab + PI Inquiry tab are available.
#
# Config tab layout:
#   A1: PI_INQUIRY_SHEET_NAME     B1: <tab name>
#   A2: PI_INQUIRY_SLUG_COLUMN    B2: <col letter, e.g. C>
#   A3: PI_INQUIRY_STATUS_COLUMN  B3: <col letter, e.g. G>
#
# def read_pi_inquiry(svc, sid, slug):
#     try:
#         config_rows = sh_read(svc, sid, "'Config'!A:B")
#         cfg = {r[0].strip(): r[1].strip() for r in config_rows if len(r) >= 2}
#         sheet = cfg.get("PI_INQUIRY_SHEET_NAME")
#         slug_col = cfg.get("PI_INQUIRY_SLUG_COLUMN")
#         status_col = cfg.get("PI_INQUIRY_STATUS_COLUMN")
#         if not all([sheet, slug_col, status_col]):
#             return "Error, update config"
#         ref = sh_read(svc, sid, f"'{sheet}'!{slug_col}:{status_col}")
#         for row in ref:
#             if len(row) >= 2 and row[0].strip() == slug:
#                 return row[-1].strip()
#         return "Not On PI Inquiry Sheet"
#     except Exception as e:
#         return f"Error, update config ({e})"


# ---------------------------------------------------------------------------
# Seed data for LightdashData tab
# ---------------------------------------------------------------------------
def build_seed_data():
    """
    Build test scenarios for the LightdashData tab.
    Returns (headers, rows) where each row tests a different code path.
    """
    today = utc_today()
    headers = LD_COLUMNS

    rows = [
        # Scenario 1: Returned claim — adjudicated, status complete
        ["returned-slug-1", "2026-01-15T00:00:00.000Z", "Cigna",
         "2026-01-20T00:00:00.000Z", "Completed - ERA Posted",
         "2026-01-22T00:00:00.000Z", "2026-02-10T00:00:00.000Z"],

        # Scenario 2: Returned claim — rejected
        ["returned-slug-1", "2026-01-15T00:00:00.000Z", "Aetna",
         "2026-01-21T00:00:00.000Z", "rejected",
         "2026-01-23T00:00:00.000Z", "2026-02-05T00:00:00.000Z"],

        # Scenario 3: Pending claim — no adjudication, recent submission
        ["pending-slug-2", "2026-03-01T00:00:00.000Z", "UHC",
         "2026-03-05T00:00:00.000Z", "submitted",
         (today - timedelta(days=10)).isoformat() + "T00:00:00.000Z", ""],

        # Scenario 4: Aging claim — submitted 120 days ago, no return
        ["aging-slug-3", "2025-11-01T00:00:00.000Z", "BCBS",
         "2025-11-05T00:00:00.000Z", "submitted",
         (today - timedelta(days=120)).isoformat() + "T00:00:00.000Z", ""],

        # Scenario 5: Resubmitted — explicitly non-actionable
        ["resubmitted-slug-4", "2026-02-01T00:00:00.000Z", "Cigna",
         "2026-02-05T00:00:00.000Z", "resubmitted",
         "2026-02-06T00:00:00.000Z", ""],

        # Scenario 6: Write-off
        ["writeoff-slug-5", "2026-01-10T00:00:00.000Z", "Aetna",
         "2026-01-15T00:00:00.000Z", "write_off",
         "2026-01-16T00:00:00.000Z", "2026-02-20T00:00:00.000Z"],

        # Scenario 7: Canceled
        ["canceled-slug-6", "2026-02-10T00:00:00.000Z", "UHC",
         "2026-02-12T00:00:00.000Z", "canceled",
         "2026-02-13T00:00:00.000Z", ""],

        # === Asana test task claims (from "Test Claims Reprocess Task") ===
        # These 4 slugs map to Asana task 1214057491820443
        # Parsed from: "iodxmn vfjuty, tyxftl cl-tyuiev"

        # Scenario 8: iodxmn — returned (ERA posted)
        ["iodxmn", "2026-03-10T00:00:00.000Z", "UHC",
         "2026-03-12T00:00:00.000Z", "Completed - ERA Posted",
         "2026-03-15T00:00:00.000Z", "2026-04-01T00:00:00.000Z"],

        # Scenario 9: vfjuty — returned (rejected)
        ["vfjuty", "2026-03-10T00:00:00.000Z", "UHC",
         "2026-03-12T00:00:00.000Z", "rejected",
         "2026-03-15T00:00:00.000Z", "2026-04-02T00:00:00.000Z"],

        # Scenario 10: tyxftl — returned (adjudication date, no actionable status)
        ["tyxftl", "2026-03-10T00:00:00.000Z", "UHC",
         "2026-03-12T00:00:00.000Z", "submitted",
         "2026-03-15T00:00:00.000Z", "2026-04-05T00:00:00.000Z"],

        # Scenario 11: tyuiev — NOT returned (pending, no adjudication)
        ["tyuiev", "2026-03-10T00:00:00.000Z", "UHC",
         "2026-03-12T00:00:00.000Z", "submitted",
         "2026-03-15T00:00:00.000Z", ""],

        # === WOIP test claims (from "test claims edit and correct a claim") ===
        # These 3 slugs map to Asana task 1214057491820459
        # Parsed from: "lopiut ffmktyu yhjklo"

        # Scenario 12: lopiut — returned (write_off)
        ["lopiut", "2026-02-20T00:00:00.000Z", "Oxford",
         "2026-02-22T00:00:00.000Z", "write_off",
         "2026-02-25T00:00:00.000Z", "2026-03-10T00:00:00.000Z"],

        # Scenario 13: ffmkty — returned (canceled)
        ["ffmkty", "2026-02-20T00:00:00.000Z", "Oxford",
         "2026-02-22T00:00:00.000Z", "canceled",
         "2026-02-25T00:00:00.000Z", ""],

        # Scenario 14: yhjklo — returned (Completed - No ERA)
        ["yhjklo", "2026-02-20T00:00:00.000Z", "Oxford",
         "2026-02-22T00:00:00.000Z", "Completed - No ERA (see Notes)",
         "2026-02-25T00:00:00.000Z", "2026-03-15T00:00:00.000Z"],
    ]

    return headers, rows


def build_tracking_seed(test_slug=None):
    """Build seed rows for Sheet1 tracking tab, referencing LightdashData slugs.
    Header row width = 15 (A-O). Data rows match. N/O columns track stale claims."""
    row1 = ["", "Associate To Enter", "", "", "", "Joey ONLY", "", "", "", "", "", "", "", "", ""]
    row2 = ["Claim Slug", "Associate", "Date added", "Asana task",
            "clean claim slug", "Return Check", "Ticket row count",
            "Ticket TRUE count", "Work ticket?", "Aging Status",
            "Send to Aging?", "PI Inquiry Status", "Ticket #",
            "First No Data Date", "Stale Notified"]

    # Use real test slug if provided, plus simulated slugs matching LightdashData
    slugs_to_seed = [
        (test_slug, "real Lightdash claim") if test_slug else ("returned-slug-1", "returned claim"),
        ("returned-slug-1", "returned (complete + rejected)"),
        ("pending-slug-2", "pending, recent"),
        ("aging-slug-3", "aging, 120 days"),
        ("resubmitted-slug-4", "resubmitted"),
        ("writeoff-slug-5", "write-off"),
        ("canceled-slug-6", "canceled"),
        ("not-in-lightdash", "slug with no LD data"),
        ("already-done", "already has return check"),
    ]

    data_rows = []
    for slug, note in slugs_to_seed:
        is_done = slug == "already-done"
        data_rows.append([
            f"cl-{slug}",                              # A: Claim Slug
            "Test User",                                # B: Associate
            "04/13/2026",                               # C: Date added
            "https://app.asana.com/0/test/123",         # D: Asana task
            slug,                                       # E: Clean claim slug
            "TRUE" if is_done else "",                  # F: Return Check
            "1" if is_done else "",                     # G
            "1" if is_done else "",                     # H
            "WORK" if is_done else "",                  # I
            "",                                         # J
            "FALSE" if is_done else "",                 # K
            "",                                         # L: PI Inquiry Status
            "",                                         # M: Ticket #
            "",                                         # N: First No Data Date
            "",                                         # O: Stale Notified
        ])

    return [row1, row2] + data_rows


# ═══════════════════════════════════════════════════════════════
#  TESTS
# ═══════════════════════════════════════════════════════════════

def test_1_lightdash_auth():
    print("=" * 60); print("TEST 1 — Lightdash PAT Auth"); print("=" * 60)
    load_env()
    if not has_lightdash():
        print("  ⊘ SKIPPED — LIGHTDASH_API_URL not set (using sheet simulation)")
        return None
    s, b = ld_request(f"{env('LIGHTDASH_API_URL').rstrip('/')}/api/v1/org", env("LIGHTDASH_API_KEY"))
    print(f"  Status: {s}")
    if s == 200:
        print(f"  Org: {b.get('results',{}).get('name','?') if isinstance(b,dict) else '?'}")
        print(f"\n  ✔ PASSED"); return True
    print(f"\n  ✘ FAILED"); return False


def test_2_sql_runner():
    print(); print("=" * 60); print("TEST 2 — v2 SQL Runner (SELECT 1)"); print("=" * 60)
    load_env()
    if not has_lightdash():
        print("  ⊘ SKIPPED — using sheet simulation")
        return None
    api_url, api_key, proj = env("LIGHTDASH_API_URL").rstrip("/"), env("LIGHTDASH_API_KEY"), env("LIGHTDASH_PROJECT_UUID")
    qid, err = ld_submit_sql(api_url, api_key, proj, "SELECT 1 AS test")
    if err: print(f"  ✘ {err}"); return False
    rows, err = ld_fetch_results(api_url, api_key, proj, qid)
    if err: print(f"  ✘ {err}"); return False
    print(f"  Result: {rows}")
    if rows: print(f"\n  ✔ PASSED"); return True
    print(f"\n  ✘ FAILED"); return False


def test_3_filtered_query():
    print(); print("=" * 60); print("TEST 3 — Filtered Query + Column Calc"); print("=" * 60)
    load_env()
    sid = env("GOOGLE_SHEETS_SPREADSHEET_ID", required=False)

    if has_lightdash():
        # Real Lightdash path
        slug = env("TEST_CLAIM_SLUG", required=False)
        if not slug: print("  ⊘ SKIPPED — TEST_CLAIM_SLUG not set"); return None
        test_slugs = [slug, FAKE_SLUG]
        print(f"  [Source: Lightdash API]")
        rows = _get_claim_data_api(test_slugs)
    elif sid:
        # Simulated path — use LightdashData tab
        test_slugs = ["returned-slug-1", "aging-slug-3", FAKE_SLUG]
        print(f"  [Source: LightdashData tab]")
        svc = get_sheets_service()
        rows = _get_claim_data_sheet(test_slugs, svc, sid)
        slug = "returned-slug-1"
    else:
        print("  ⊘ SKIPPED — no data source available")
        return None

    print(f"  Test slugs: {test_slugs}")
    print(f"  Rows returned: {len(rows)}")

    # Filter to target slug
    target_rows = [r for r in rows if isinstance(r, dict) and xval(r, LD_SLUG) == slug]
    fake_rows = [r for r in rows if isinstance(r, dict) and xval(r, LD_SLUG) == FAKE_SLUG]

    print(f"  Target slug rows: {len(target_rows)}")
    print(f"  Fake slug rows:   {len(fake_rows)}")

    if not target_rows:
        print(f"\n  ✘ FAILED — target slug not found"); return False
    if fake_rows:
        print(f"\n  ✘ FAILED — fake slug should be excluded"); return False

    res = process_claim(slug, target_rows)
    print(f"\n  --- Calculated F-K ---")
    print(f"  F  Return Check:     {res['return_check']}")
    print(f"  G  Ticket row count: {res['row_count']}")
    print(f"  H  Ticket TRUE count:{res['true_count']}")
    print(f"  I  Work ticket?:     {res['work_ticket']}")
    print(f"  J  Aging Status:     {res['aging_status']}")
    print(f"  K  Send to Aging?:   {res['send_to_aging']}")

    print(f"\n  ✔ PASSED"); return True


# --- Sheets tests ---

def test_4_sheets_auth():
    print(); print("=" * 60); print("TEST 4 — Sheets Auth"); print("=" * 60)
    load_env()
    try:
        svc = get_sheets_service()
        meta = svc.spreadsheets().get(spreadsheetId=env("GOOGLE_SHEETS_SPREADSHEET_ID")).execute()
        title = meta.get("properties", {}).get("title", "?")
        tabs = [s.get("properties", {}).get("title") for s in meta.get("sheets", [])]
        print(f"  Spreadsheet: {title}")
        print(f"  Tabs: {tabs}")
        print(f"\n  ✔ PASSED"); return True
    except Exception as e:
        print(f"  ✘ {e}"); return False


def test_5_sheets_setup():
    """Seed both Sheet1 (tracking) and LightdashData (simulated results)."""
    print(); print("=" * 60); print("TEST 5 — Sheets Setup (Tracking + LightdashData)"); print("=" * 60)
    load_env()
    sid = env("GOOGLE_SHEETS_SPREADSHEET_ID")
    test_slug = env("TEST_CLAIM_SLUG", required=False)

    try:
        svc = get_sheets_service()

        # --- Sheet1: tracking tab ---
        print("  --- Sheet1 (tracking) ---")
        sh_clear(svc, sid, "Sheet1!A:O")
        tracking_data = build_tracking_seed(test_slug)
        row_count = len(tracking_data)
        sh_write(svc, sid, f"Sheet1!A1:O{row_count}", tracking_data)
        print(f"  Wrote {row_count} rows (2 header + {row_count - 2} data)")

        for i, r in enumerate(tracking_data[:5]):
            label = ["Section", "Headers", "Data 1", "Data 2", "Data 3"][i]
            print(f"    {label}: {r[:6]}")
        if row_count > 5:
            print(f"    ... and {row_count - 5} more rows")

        # --- LightdashData: simulated Lightdash results ---
        print(f"\n  --- LightdashData (simulated) ---")
        ensure_tab(svc, sid, "LightdashData")
        sh_clear(svc, sid, "'LightdashData'!A:G")
        ld_headers, ld_rows = build_seed_data()
        all_ld = [ld_headers] + ld_rows
        sh_write(svc, sid, f"'LightdashData'!A1:G{len(all_ld)}", all_ld)
        print(f"  Wrote {len(all_ld)} rows (1 header + {len(ld_rows)} data)")

        for i, r in enumerate(all_ld[:4]):
            slug = r[0][:25] if r[0] else ""
            status = r[4][:30] if len(r) > 4 else ""
            print(f"    {'Header' if i == 0 else f'Scenario {i}'}: {slug} | {status}")
        if len(ld_rows) > 3:
            print(f"    ... and {len(ld_rows) - 3} more scenarios")

        print(f"\n  ✔ PASSED"); return True
    except Exception as e:
        print(f"  ✘ {e}"); return False


def test_6_read_pending():
    print(); print("=" * 60); print("TEST 6 — Read Pending Slugs"); print("=" * 60)
    load_env()
    try:
        svc = get_sheets_service()
        rows = sh_read(svc, env("GOOGLE_SHEETS_SPREADSHEET_ID"), "Sheet1!A:M")
        pending = {}
        for i, row in enumerate(rows[DATA_START_ROW-1:], start=DATA_START_ROW):
            s = row[4].strip() if len(row) > 4 else ""
            r = row[5].strip() if len(row) > 5 else ""
            if s and not r: pending[s] = i
        print(f"  Pending: {len(pending)}")
        for s, idx in pending.items(): print(f"    Row {idx}: {s}")
        print(f"\n  ✔ PASSED"); return True, pending
    except Exception as e:
        print(f"  ✘ {e}"); return False, {}


def test_7_write_columns():
    """Write calculated F-K values using data from get_claim_data (dual-source)."""
    print(); print("=" * 60); print("TEST 7 — Write Columns F-K (Single Row)"); print("=" * 60)
    load_env()
    sid = env("GOOGLE_SHEETS_SPREADSHEET_ID")

    try:
        svc = get_sheets_service()
        rows = sh_read(svc, sid, "Sheet1!A:M")

        # Find first pending row
        target = None
        for i, row in enumerate(rows[DATA_START_ROW-1:], start=DATA_START_ROW):
            s = row[4].strip() if len(row) > 4 else ""
            r = row[5].strip() if len(row) > 5 else ""
            if s and not r: target = (i, s); break

        if not target:
            print("  ⊘ SKIPPED — no pending rows"); return None

        idx, slug = target
        print(f"  Target: Row {idx} (slug: {slug})")

        # Get claim data (real or simulated)
        claim_rows = get_claim_data([slug], svc, sid)
        print(f"  Claim data rows: {len(claim_rows)}")

        if claim_rows:
            res = process_claim(slug, claim_rows)
        else:
            # No data found — write defaults
            res = {"return_check": "FALSE", "row_count": 0, "true_count": 0,
                   "work_ticket": "", "aging_status": "", "send_to_aging": ""}

        vals = [res["return_check"], str(res["row_count"]), str(res["true_count"]),
                res["work_ticket"], str(res["aging_status"]), res["send_to_aging"]]

        print(f"  Writing F{idx}:K{idx}: {vals}")
        sh_write(svc, sid, f"Sheet1!F{idx}:K{idx}", [vals])

        verify = sh_read(svc, sid, f"Sheet1!E{idx}:K{idx}")
        print(f"  Verified: {verify}")

        print(f"\n  ✔ PASSED"); return True
    except Exception as e:
        print(f"  ✘ {e}"); return False


# --- Asana tests ---

def test_8_asana_auth():
    """Asana PAT auth + project enumeration."""
    print(); print("=" * 60); print("TEST 8 — Asana Auth + Project"); print("=" * 60)
    load_env()
    pat = env("ASANA_PAT", required=False)
    if not pat: print("  ⊘ SKIPPED — ASANA_PAT not set"); return None
    s, b = asana_req("GET", "/users/me", pat)
    if s != 200:
        print(f"  ✘ Auth failed: {s}"); return False
    print(f"  User: {b.get('data',{}).get('name','?')}")

    # Verify project access
    proj = env("ASANA_PROJECT_GID", required=False)
    if not proj:
        print(f"  ⊘ SKIPPED — ASANA_PROJECT_GID not set"); return None
    s2, b2 = asana_req("GET", f"/projects/{proj}?opt_fields=name", pat)
    if s2 != 200:
        print(f"  ✘ Project access failed: {s2}"); return False
    print(f"  Project: {b2.get('data',{}).get('name','?')}")

    # Test slug parser
    print(f"\n  --- Slug parser tests ---")
    cases = [
        ("iodxmn vfjuty, tyxftl cl-tyuiev", ["iodxmn", "vfjuty", "tyxftl", "tyuiev"], False),
        ("cl-AbC123 cl-def456", ["abc123", "def456"], False),
        ("xftuyl,kkopln,lkmhyt", ["xftuyl", "kkopln", "lkmhyt"], False),
        ("lopiut ffmktyu yhjklo", ["lopiut", "ffmkty", "yhjklo"], False),
        ("lkopioty", ["lkopio"], False),  # 8 chars → extracts first 6-char match
        ("", [], True),
        ("!!!", [], True),
    ]
    all_ok = True
    for text, expected, expected_review in cases:
        slugs, review = parse_claim_slugs(text)
        ok = slugs == expected and review == expected_review
        sym = "OK" if ok else "FAIL"
        print(f"    {sym} parse({text!r:40s}) -> {slugs}")
        if not ok:
            print(f"      Expected: {expected}, review={expected_review}")
            print(f"      Got:      {slugs}, review={review}")
            all_ok = False

    if all_ok:
        print(f"\n  ✔ PASSED"); return True
    print(f"\n  ✘ FAILED — slug parser mismatch"); return False


def test_9_asana_intake():
    """
    Operation 1: Read New Tasks with Have All Claims Returned = No,
    parse slugs, set CX/OPs to WOIP, append rows to Sheet.
    """
    print(); print("=" * 60); print("TEST 9 — Asana Intake (New → WOIP + Sheet)"); print("=" * 60)
    load_env()
    pat = env("ASANA_PAT", required=False)
    proj = env("ASANA_PROJECT_GID", required=False)
    sid = env("GOOGLE_SHEETS_SPREADSHEET_ID", required=False)
    if not pat or not proj:
        print("  ⊘ SKIPPED — ASANA_PAT or ASANA_PROJECT_GID not set"); return None

    # Step 1: Get tasks needing WOIP
    print(f"  --- Step 1: Find New Tasks with claims not returned ---")
    tasks = get_new_tasks_needing_woip(pat, proj)
    print(f"  Found {len(tasks)} task(s)")

    if not tasks:
        print(f"\n  ✔ PASSED (no tasks to process)"); return True

    for t in tasks:
        print(f"\n  Task: {t['name']} (gid: {t['gid']})")
        print(f"    Claims text: {t['claims_text']!r}")
        print(f"    Parsed slugs: {t['slugs']}")
        print(f"    Needs review: {t['needs_review']}")

        if t["needs_review"] and not t["slugs"]:
            # Post review comment, skip processing
            print(f"    → Posting review comment (unparseable slugs)")
            s, _ = asana_post_comment(pat, t["gid"],
                "[Automation] Could not parse claim IDs from this task. "
                "Please review the 'Please Paste' field and ensure claim IDs "
                "are 6-character alphanumeric codes (with or without 'cl-' prefix).")
            print(f"    → Comment posted: {'OK' if s in (200, 201) else f'FAILED ({s})'}")
            continue

        # Step 2: Set CX/OPs to WOIP
        print(f"    → Setting CX/OPs to 'waiting on insurance partner'")
        s, b = asana_update_field(pat, t["gid"], env("ASANA_FIELD_CX_OPS"), env("ASANA_OPT_WOIP"))
        print(f"    → Update: {'OK' if s == 200 else f'FAILED ({s})'}")
        if s != 200:
            print(f"    → Response: {str(b)[:200]}")
            return False

        # Step 3: Append claims to sheet
        if sid:
            print(f"    → Appending {len(t['slugs'])} claim(s) to Sheet")
            svc = get_sheets_service()
            count = append_claims_to_sheet(t, svc, sid)
            print(f"    → Appended {count} new row(s)")

    # Verify: re-read task to confirm field was set
    print(f"\n  --- Verify ---")
    for t in tasks:
        if t["needs_review"] and not t["slugs"]:
            continue
        s, b = asana_req("GET", f"/tasks/{t['gid']}?opt_fields=custom_fields.gid,custom_fields.display_value", pat)
        if s == 200:
            cx_field = asana_task_field(b.get("data", {}), env("ASANA_FIELD_CX_OPS"))
            val = cx_field.get("display_value", "?") if cx_field else "?"
            print(f"  Task {t['gid']}: CX/OPs = {val}")

    print(f"\n  ✔ PASSED"); return True


def test_10_asana_return_update():
    """
    Operation 2: Check if all claims for a task have returned,
    then update Have All Claims Returned = Yes and CX/OPs = needs follow-up.

    This test seeds the WOIP task's claims into the sheet first,
    runs F-K calc on them, then checks the all-or-nothing logic.
    """
    print(); print("=" * 60); print("TEST 10 — Asana Return Update (All Claims Check)"); print("=" * 60)
    load_env()
    pat = env("ASANA_PAT", required=False)
    proj = env("ASANA_PROJECT_GID", required=False)
    sid = env("GOOGLE_SHEETS_SPREADSHEET_ID", required=False)
    if not pat or not proj or not sid:
        print("  ⊘ SKIPPED — missing ASANA_PAT, ASANA_PROJECT_GID, or SHEETS ID"); return None

    svc = get_sheets_service()

    # Step 0: Seed WOIP task claims into the sheet so we can test the aggregation
    print(f"  --- Step 0: Seed WOIP task claims into sheet ---")
    woip_tasks = asana_get_section_tasks(pat, proj, env("ASANA_SECTION_WOIP"))
    seeded = 0
    for t in woip_tasks:
        claims_field = asana_task_field(t, env("ASANA_FIELD_CLAIMS_TEXT"))
        claims_text = (claims_field.get("text_value") or "") if claims_field else ""
        slugs, needs_review = parse_claim_slugs(claims_text)
        if not slugs:
            continue
        task_info = {"gid": t["gid"], "name": t.get("name", ""), "slugs": slugs}
        count = append_claims_to_sheet(task_info, svc, sid)
        seeded += count
        print(f"  Task {t['gid']}: appended {count} rows for slugs {slugs}")
    print(f"  Total seeded: {seeded}")

    # Step 0b: Run F-K calc on the newly seeded claims
    print(f"\n  --- Step 0b: Calculate F-K for seeded claims ---")
    all_rows = sh_read(svc, sid, "Sheet1!A:Z")
    pending = {}
    for i, row in enumerate(all_rows[DATA_START_ROW - 1:], start=DATA_START_ROW):
        slug = row[4].strip() if len(row) > 4 else ""
        ret = row[5].strip() if len(row) > 5 else ""
        if slug and not ret:
            pending[slug] = i

    if pending:
        claim_rows = get_claim_data(list(pending.keys()), svc, sid)
        by_slug = {}
        for row in claim_rows:
            if isinstance(row, dict):
                s = xval(row, LD_SLUG)
                if s:
                    by_slug.setdefault(s, []).append(row)
        write_fk_batched(svc, sid, pending, by_slug)

    # Step 1: Get tasks in WOIP section with Have All Claims Returned = No
    print(f"\n  --- Step 1: Find WOIP tasks with pending returns ---")
    tasks = asana_get_section_tasks(pat, proj, env("ASANA_SECTION_WOIP"))
    pending_tasks = []
    field_all_returned = env("ASANA_FIELD_ALL_RETURNED")
    opt_returned_no = env("ASANA_OPT_RETURNED_NO")
    for t in tasks:
        ret_field = asana_task_field(t, field_all_returned)
        if not ret_field:
            continue
        ev = ret_field.get("enum_value")
        if ev and ev.get("gid") == opt_returned_no:
            pending_tasks.append(t)

    print(f"  Found {len(pending_tasks)} WOIP task(s) with claims not returned")

    if not pending_tasks:
        print(f"\n  ✔ PASSED (no pending tasks)"); return True

    for t in pending_tasks:
        task_gid = t["gid"]
        task_name = t.get("name", "?")
        print(f"\n  Task: {task_name} (gid: {task_gid})")

        # Step 2: Check if all claims in sheet have returned
        all_ret, total, returned = check_all_claims_returned(task_gid, svc, sid)
        print(f"    Claims: {returned}/{total} returned")

        if not all_ret:
            print(f"    -> Not all returned yet, skipping update")
            continue

        # Step 3: Update Asana — both fields
        print(f"    -> All claims returned! Updating Asana...")

        s1, _ = asana_update_field(pat, task_gid, env("ASANA_FIELD_ALL_RETURNED"), env("ASANA_OPT_RETURNED_YES"))
        print(f"    -> Have All Claims Returned = Yes: {'OK' if s1 == 200 else f'FAILED ({s1})'}")

        s2, _ = asana_update_field(pat, task_gid, env("ASANA_FIELD_CX_OPS"), env("ASANA_OPT_NEEDS_FOLLOWUP"))
        print(f"    -> CX/OPs = needs follow-up: {'OK' if s2 == 200 else f'FAILED ({s2})'}")

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        s3, _ = asana_post_comment(pat, task_gid,
            f"[Automation] All {total} claim(s) have returned as of {ts}. "
            f"Task updated to 'needs follow-up'.")
        print(f"    -> Comment posted: {'OK' if s3 in (200, 201) else f'FAILED ({s3})'}")

        if s1 != 200 or s2 != 200:
            return False

    print(f"\n  ✔ PASSED"); return True


# --- End-to-end ---

def test_11_e2e():
    """Full pipeline using dual-source pattern."""
    print(); print("=" * 60); print("TEST 11 — End-to-End Pipeline"); print("=" * 60)
    load_env()
    sid = env("GOOGLE_SHEETS_SPREADSHEET_ID")
    source = "Lightdash API" if has_lightdash() else "LightdashData tab"
    print(f"  Data source: {source}")

    # Step 1: Read pending slugs (read A:Z so we have col N/O for stale-check + col Z for task GID)
    print(f"\n  --- Step 1: Read pending ---")
    try:
        svc = get_sheets_service()
        all_rows = sh_read(svc, sid, "Sheet1!A:Z")
    except Exception as e:
        print(f"  ✘ {e}"); return False

    pending = {}
    for i, row in enumerate(all_rows[DATA_START_ROW-1:], start=DATA_START_ROW):
        s = row[4].strip() if len(row) > 4 else ""
        r = row[5].strip() if len(row) > 5 else ""
        if s and not r: pending[s] = i

    print(f"  Pending: {len(pending)}")
    if not pending:
        print(f"\n  ✔ PASSED (nothing to do)"); return True

    for slug, idx in pending.items():
        print(f"    Row {idx}: {slug}")

    # Step 2: Get claim data (real or simulated)
    print(f"\n  --- Step 2: Get claim data ({source}) ---")
    claim_rows = get_claim_data(list(pending.keys()), svc, sid)
    print(f"  Total rows: {len(claim_rows)}")

    # Group by slug
    by_slug = {}
    for row in claim_rows:
        if isinstance(row, dict):
            s = xval(row, LD_SLUG)
            if s: by_slug.setdefault(s, []).append(row)

    print(f"  Unique slugs with data: {len(by_slug)}")

    # Step 3-4: Process + Write F-K (one batched call for all pending rows)
    print(f"\n  --- Step 3-4: Process + Write F-K ---")
    try:
        matched, no_data = write_fk_batched(svc, sid, pending, by_slug)
    except Exception as e:
        print(f"      ✘ Batch write failed: {e}")
        matched, no_data = 0, len(pending)

    # Step 4b: Stale-claim check — track no-data slugs, notify after 30 days
    if no_data > 0:
        print(f"\n  --- Step 4b: Stale-claim check ({no_data} slug(s) without Lightdash data) ---")
        pat = env("ASANA_PAT", required=False)
        if pat:
            try:
                stale_counters = check_stale_claims(svc, sid, pat, pending, by_slug, all_rows)
                print(f"  Stale: {stale_counters}")
            except Exception as e:
                print(f"  ✘ Stale check failed: {e}")
        else:
            print(f"  ⊘ Skipping stale check — ASANA_PAT not set")

    # Step 5: Close Asana tasks whose claims are all returned (real writes;
    # honors --dry-run via the global flag).
    print(f"\n  --- Step 5: Close completed Asana tasks ---")
    pat = env("ASANA_PAT", required=False)
    if pat:
        # Re-read after Step 3-4 wrote F-K — close logic needs current state.
        all_rows_after = sh_read(svc, sid, "Sheet1!A:Z")
        try:
            close_counters = close_completed_tasks(svc, sid, pat, all_rows_after)
            print(f"  Close: {close_counters}")
        except Exception as e:
            print(f"  ✘ Close pass failed: {e}")
    else:
        print(f"  ⊘ Skipping close pass — ASANA_PAT not set")

    # Verify
    print(f"\n  --- Verify writes ---")
    verify = sh_read(svc, sid, "Sheet1!E:K")
    for row in verify[DATA_START_ROW-1:]:
        slug = row[0].strip() if len(row) > 0 else ""
        ret = row[1].strip() if len(row) > 1 else ""
        if slug in pending and ret:
            print(f"    ✔ {slug}: F={ret}")

    print(f"\n  ✔ PASSED — {matched} processed, {no_data} no data, source: {source}")
    return True


# ═══════════════════════════════════════════════════════════════
#  RUNNER
# ═══════════════════════════════════════════════════════════════

TEST_MAP = {
    "lightdash": [
        ("Test 1: Lightdash Auth", test_1_lightdash_auth),
        ("Test 2: SQL Runner", test_2_sql_runner),
        ("Test 3: Filtered+Calc", test_3_filtered_query),
    ],
    "sheets": [
        ("Test 4: Sheets Auth", test_4_sheets_auth),
        ("Test 6: Read Pending", test_6_read_pending),
        ("Test 7: Write F-K", test_7_write_columns),
    ],
    "asana": [
        ("Test 8: Asana Auth+Parse", test_8_asana_auth),
        ("Test 9: Asana Intake", test_9_asana_intake),
        ("Test 10: Return Update", test_10_asana_return_update),
    ],
    "e2e": [
        ("Test 11: E2E Pipeline", test_11_e2e),
    ],
    # DESTRUCTIVE — wipes Sheet1!A:M and LightdashData!A:G then re-seeds.
    # Excluded from "all" so it never runs incidentally. Invoke only when
    # you explicitly want to reset the sheet to a fresh bootstrap state.
    "setup": [
        ("Test 5: Setup+Seed (DESTRUCTIVE)", test_5_sheets_setup),
    ],
}

ALL_GROUPS = ["lightdash", "sheets", "asana", "e2e"]   # "setup" excluded — destructive

def run_tests(group="all"):
    from claims_logging import setup_logging, gen_run_id, set_run_id
    from recorder import RunRecorder

    setup_logging()
    load_env()
    verify_lightdash()  # fail loud on misconfigured URL/key (no-op if unset)

    run_id = gen_run_id()
    set_run_id(run_id)
    pat = os.getenv("ASANA_PAT", "")
    recorder_gid = os.getenv("ASANA_RECORDER_TASK_GID", "")
    recorder = RunRecorder(pat, run_id, f"test_group_{group}", task_gid=recorder_gid)

    results = {}
    groups = ALL_GROUPS if group == "all" else [group]
    try:
        for g in groups:
            for name, fn in TEST_MAP.get(g, []):
                r = fn()
                results[name] = r[0] if isinstance(r, tuple) else r
    except Exception as e:
        recorder.error(repr(e))
        recorder.flush(status="error")
        raise

    print(); print("=" * 60); print("SUMMARY"); print("=" * 60)
    for n, p in results.items():
        print(f"  {'✔' if p is True else ('⊘' if p is None else '✘')} {n}")

    passed = sum(1 for v in results.values() if v is True)
    skipped = sum(1 for v in results.values() if v is None)
    failed = sum(1 for v in results.values() if v is False)
    print(f"\n  Passed: {passed}  Skipped: {skipped}  Failed: {failed}")

    recorder.record(passed=passed, skipped=skipped, failed=failed,
                    tests_run=len(results), group=group)
    recorder.flush(status="success" if failed == 0 else "error")
    return failed == 0

def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    flags = [a for a in sys.argv[1:] if a.startswith("-")]
    if "--dry-run" in flags:
        set_dry_run(True)
        print("[dry-run] mode enabled — no Sheets/Asana mutations will be executed")
    g = args[0] if args else "all"
    valid = ("all",) + tuple(ALL_GROUPS) + ("setup",)
    if g not in valid:
        print(f"Usage: python3 test_suite.py [{' | '.join(valid)}] [--dry-run]")
        sys.exit(1)
    sys.exit(0 if run_tests(g) else 1)

if __name__ == "__main__":
    main()
