#!/usr/bin/env python3
"""
asana_seeder.py — Create varied Asana tasks + correlated LightdashData sim rows.

Generates 20-30 (configurable) realistic test tasks across the project's sections
with varied custom-field values, claim-ID formats, and completion states. For each
task, writes matching rows to the LightdashData tab so the full pipeline (monitor
→ sheet → E2E → Asana update) has correlated data to resolve.

Run metadata is persisted to .seeder_runs.json so the status simulator can
target-flip specific runs' claim slugs.

Usage:
    python3 asana_seeder.py --count 25
    python3 asana_seeder.py --count 10 --run-id smoke-test
    python3 asana_seeder.py --list          # show past runs
    python3 asana_seeder.py --dry-run       # print what would happen, no API calls
"""

import argparse
import json
import random
import string
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Windows-safe output: don't crash on em-dash / check-marks etc. in cp1252 consoles.
try:
    sys.stdout.reconfigure(errors="replace")
except Exception:
    pass

# Reuse helpers + constants from test_suite.py so the seeder stays in sync.
sys.path.insert(0, str(Path(__file__).absolute().parent))
from test_suite import (  # noqa: E402
    load_env, env, asana_req, get_sheets_service, sh_read, sh_write, ensure_tab,
    LD_COLUMNS, LD_SLUG, LD_STATUS, LD_ADJ_DAY, LD_SUBMITTED, LD_CREATED, LD_APPT, LD_PARTNER,
)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
RUNS_FILE = Path(__file__).absolute().parent / ".seeder_runs.json"

INSURANCE_PARTNERS = ["UHC", "Oscar", "Cigna", "Oxford", "Aetna", "Anthem"]

# Map readable names used in scenarios → real option values in the Asana project.
# Anchor these to what `discover_project()` returned on 2026-04-16.
FIELD_NAMES = {
    "cx_ops":        "cx/ops task progress",
    "cat_action":    "cat action requested",
    "all_returned":  "have all claims returned",
    "insurance":     "insurance payer",
    "claims_text":   "please paste the claims id associated with the impacted claims",
}
CX_OPS_NOT_STARTED = "not-started"
CX_OPS_WOIP        = "waiting on insurance partner"
CX_OPS_COMPLETED   = "complted"   # (sic — that's the real option name in Asana)
# Allowed CAT Action Requested values we'll pull from randomly.
CAT_ACTIONS = ["reprocess claim", "edit/correct a claim", "cancel a claim",
               "submit new claim", "adjusted an invoice"]

# Weighted pool for actionable statuses (ERA Posted is most common in reality).
ACTIONABLE_POOL = [
    "Completed - ERA Posted", "Completed - ERA Posted", "Completed - ERA Posted",
    "Completed - No ERA (see Notes)", "rejected", "write_off", "canceled",
]
PENDING_STATUS = "submitted"
RESUBMITTED_STATUS = "resubmitted"

# Claim-text format variants exercised by the scenarios.
# The parser at parse_claim_slugs() strips `cl-`/`CL-` prefixes and finds
# 6-char alphanumeric tokens. We vary the format to stress it.
def fmt_claims_cl_comma(slugs):     return ", ".join(f"cl-{s}" for s in slugs)
def fmt_claims_cl_space(slugs):     return " ".join(f"cl-{s}" for s in slugs)
def fmt_claims_bare(slugs):         return ", ".join(slugs)
def fmt_claims_mixed_case(slugs):   return " ".join(
    f"CL-{s.upper()}" if i % 2 else f"cl-{s}" for i, s in enumerate(slugs))
def fmt_claims_sloppy(slugs):
    # Uses ONLY <6-char words so the parser doesn't false-positive on our noise.
    # (parse_claim_slugs extracts any [A-Za-z0-9]{6} run, so "please"/"thanks"/
    # "reprocess" would be wrongly tagged as slugs — by design per the
    # test_suite.py contract, but we don't want our seeds to exercise it.)
    prefix = random.choice(["pls fix", "asap:", "help on", "fix pls", "see this:"])
    suffix = random.choice(["thx", "pls", "asap", "k thx", "!!"])
    body = ", ".join(f"cl-{s}" for s in slugs)
    return f"{prefix} {body} {suffix}"

CLAIM_FORMATTERS = [fmt_claims_cl_comma, fmt_claims_cl_space, fmt_claims_bare,
                    fmt_claims_mixed_case, fmt_claims_sloppy]

# Strings with no 6-char alphanumeric token — trigger the review-comment path.
UNPARSEABLE_TEXT = ["???", "see notes", "tbd", "asap", "need ids", "?????"]

# ═══════════════════════════════════════════════════════════════════════════════
# RUN METADATA (persisted)
# ═══════════════════════════════════════════════════════════════════════════════
def load_runs():
    if not RUNS_FILE.exists():
        return {"runs": {}}
    try:
        return json.loads(RUNS_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"runs": {}}

def save_run(run_id, record):
    data = load_runs()
    data["runs"][run_id] = record
    RUNS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")

# ═══════════════════════════════════════════════════════════════════════════════
# DISCOVERY — enumerate sections + custom fields at startup
# ═══════════════════════════════════════════════════════════════════════════════
def discover_project(pat, project_gid):
    """
    Return {
        sections: {name_lower: gid},
        fields:   {name_lower: {gid, type, options: {name_lower: gid}}}
    }
    """
    # Sections
    s, b = asana_req("GET", f"/projects/{project_gid}/sections?opt_fields=name,gid", pat)
    if s != 200:
        raise RuntimeError(f"Failed to list sections: {s} {b}")
    sections = {sec["name"].strip().lower(): sec["gid"] for sec in b.get("data", [])}

    # Custom field settings (project-scoped)
    q = ("?opt_fields=custom_field.name,custom_field.gid,custom_field.resource_subtype,"
         "custom_field.enum_options.name,custom_field.enum_options.gid,"
         "custom_field.enum_options.enabled")
    s, b = asana_req("GET", f"/projects/{project_gid}/custom_field_settings{q}", pat)
    if s != 200:
        raise RuntimeError(f"Failed to list custom field settings: {s} {b}")

    fields = {}
    for row in b.get("data", []):
        cf = row.get("custom_field", {})
        name = cf.get("name", "").strip().lower()
        if not name:
            continue
        opts = {}
        for opt in cf.get("enum_options", []) or []:
            if opt.get("enabled", True):
                opts[opt["name"].strip().lower()] = opt["gid"]
        fields[name] = {
            "gid": cf["gid"],
            "type": cf.get("resource_subtype", "text"),
            "options": opts,
        }
    return {"sections": sections, "fields": fields}


def section_gid(discovery, name):
    key = name.strip().lower()
    if key not in discovery["sections"]:
        raise RuntimeError(f"Section '{name}' not found. Available: {list(discovery['sections'])}")
    return discovery["sections"][key]


def resolve_cf(discovery, field_name, value):
    """Translate (field_name, human value) -> (gid, api-formatted value)."""
    key = field_name.strip().lower()
    # Handle fields matched by prefix (e.g. "please paste the claims id (no apostrophes..." etc.)
    if key not in discovery["fields"]:
        # Try prefix match — useful for the long "Please Paste" field name.
        matches = [k for k in discovery["fields"] if k.startswith(key) or key in k]
        if len(matches) == 1:
            key = matches[0]
        else:
            raise RuntimeError(f"Field '{field_name}' not found. Available: {list(discovery['fields'])}")
    f = discovery["fields"][key]
    if f["type"] == "enum":
        vkey = str(value).strip().lower()
        if vkey not in f["options"]:
            # Also try prefix match on option name (some option names have trailing spaces)
            opt_matches = [k for k in f["options"] if k.startswith(vkey) or vkey in k]
            if len(opt_matches) == 1:
                vkey = opt_matches[0]
            else:
                raise RuntimeError(
                    f"Option '{value}' not found on field '{field_name}'. "
                    f"Available: {list(f['options'])}")
        return f["gid"], f["options"][vkey]
    # text / other — pass raw string
    return f["gid"], str(value)


# ═══════════════════════════════════════════════════════════════════════════════
# GENERATORS
# ═══════════════════════════════════════════════════════════════════════════════
def make_slug_generator(exclude):
    """Return a zero-arg callable that yields unused 6-char lowercase alnum slugs."""
    used = set(s.lower() for s in exclude)
    alphabet = string.ascii_lowercase + string.digits

    def next_slug():
        for _ in range(200):
            s = "".join(random.choices(alphabet, k=6))
            if s not in used:
                used.add(s)
                return s
        raise RuntimeError("Could not generate unique slug after 200 attempts")
    return next_slug


def iso_day(d):
    """Format a date as the sim's ISO timestamp convention."""
    return d.strftime("%Y-%m-%dT00:00:00.000Z")


def make_sim_row(slug, partner, role, anchor=None):
    """
    role ∈ {'actionable', 'pending', 'aging', 'resubmitted'}
    Returns a list in LD_COLUMNS order.
    """
    anchor = anchor or datetime.now(timezone.utc).date()

    if role == "aging":
        submitted = anchor - timedelta(days=random.randint(100, 200))
        created = submitted - timedelta(days=random.randint(1, 5))
        appt = created - timedelta(days=random.randint(1, 7))
        status = random.choice([PENDING_STATUS, PENDING_STATUS, "Completed - ERA Posted"])
        adj = ""  # aging implies no adjudication yet (if actionable, still counts)
        if status.startswith("Completed"):
            adj = iso_day(submitted + timedelta(days=random.randint(30, 80)))
    elif role == "actionable":
        submitted = anchor - timedelta(days=random.randint(20, 70))
        created = submitted - timedelta(days=random.randint(1, 5))
        appt = created - timedelta(days=random.randint(1, 7))
        status = random.choice(ACTIONABLE_POOL)
        # 50% of time provide an explicit adjudication date; otherwise rely on status
        adj = iso_day(submitted + timedelta(days=random.randint(10, 30))) if random.random() < 0.5 else ""
    elif role == "resubmitted":
        submitted = anchor - timedelta(days=random.randint(10, 40))
        created = submitted - timedelta(days=random.randint(1, 5))
        appt = created - timedelta(days=random.randint(1, 7))
        status = RESUBMITTED_STATUS
        adj = ""
    else:  # pending
        submitted = anchor - timedelta(days=random.randint(5, 60))
        created = submitted - timedelta(days=random.randint(1, 5))
        appt = created - timedelta(days=random.randint(1, 7))
        status = PENDING_STATUS
        adj = ""

    row_dict = {
        LD_SLUG: slug,
        LD_APPT: iso_day(appt),
        LD_PARTNER: partner,
        LD_CREATED: iso_day(created),
        LD_STATUS: status,
        LD_SUBMITTED: iso_day(submitted),
        LD_ADJ_DAY: adj,
    }
    return [row_dict[c] for c in LD_COLUMNS]


# ═══════════════════════════════════════════════════════════════════════════════
# SCENARIOS
# Each scenario returns (task_body_spec, sim_rows, slug_roles)
# task_body_spec is a dict with:
#   name                (str)
#   section             (human name)
#   fields              (dict of field_name → human value; text fields pass raw string)
# slug_roles is {slug: role} for downstream tracking.
# ═══════════════════════════════════════════════════════════════════════════════
def _mk(scenario, name, section, fields, sim_rows, slug_roles):
    return {
        "scenario": scenario,
        "spec": {"name": name, "section": section, "fields": fields},
        "sim_rows": sim_rows,
        "slug_roles": slug_roles,
    }


def _claim_text(slugs, allow_sloppy=True):
    formatters = CLAIM_FORMATTERS if allow_sloppy else CLAIM_FORMATTERS[:4]
    return random.choice(formatters)(slugs)


def scn_new_single_actionable(counter, next_slug):
    slug = next_slug()
    partner = random.choice(INSURANCE_PARTNERS)
    return _mk(
        "new-single-actionable",
        f"[SEED-{counter}] Reprocess single claim",
        "New Tasks",
        {
            FIELD_NAMES["cx_ops"]: CX_OPS_NOT_STARTED,
            FIELD_NAMES["all_returned"]: "no",
            FIELD_NAMES["cat_action"]: random.choice(["reprocess claim", "edit/correct a claim"]),
            FIELD_NAMES["insurance"]: partner,
            FIELD_NAMES["claims_text"]: _claim_text([slug]),
        },
        [make_sim_row(slug, partner, "actionable")],
        {slug: "actionable"},
    )


def scn_new_multi_actionable(counter, next_slug):
    n = random.randint(2, 4)
    slugs = [next_slug() for _ in range(n)]
    partner = random.choice(INSURANCE_PARTNERS)
    roles = [random.choice(["actionable", "actionable", "pending"]) for _ in slugs]
    return _mk(
        "new-multi-actionable",
        f"[SEED-{counter}] Reprocess {n} claims — mixed",
        "New Tasks",
        {
            FIELD_NAMES["cx_ops"]: CX_OPS_NOT_STARTED,
            FIELD_NAMES["all_returned"]: "no",
            FIELD_NAMES["cat_action"]: random.choice(["reprocess claim", "edit/correct a claim"]),
            FIELD_NAMES["insurance"]: partner,
            FIELD_NAMES["claims_text"]: _claim_text(slugs),
        },
        [make_sim_row(s, partner, r) for s, r in zip(slugs, roles)],
        dict(zip(slugs, roles)),
    )


def scn_new_multi_pending(counter, next_slug):
    n = random.randint(2, 3)
    slugs = [next_slug() for _ in range(n)]
    partner = random.choice(INSURANCE_PARTNERS)
    return _mk(
        "new-multi-pending",
        f"[SEED-{counter}] Submit {n} new claims",
        "New Tasks",
        {
            FIELD_NAMES["cx_ops"]: CX_OPS_NOT_STARTED,
            FIELD_NAMES["all_returned"]: "no",
            FIELD_NAMES["cat_action"]: "submit new claim",
            FIELD_NAMES["insurance"]: partner,
            FIELD_NAMES["claims_text"]: _claim_text(slugs, allow_sloppy=False),
        },
        [make_sim_row(s, partner, "pending") for s in slugs],
        {s: "pending" for s in slugs},
    )


def scn_new_unparseable(counter, next_slug):
    partner = random.choice(INSURANCE_PARTNERS)
    return _mk(
        "new-unparseable",
        f"[SEED-{counter}] URGENT — claim id missing",
        "New Tasks",
        {
            FIELD_NAMES["cx_ops"]: CX_OPS_NOT_STARTED,
            FIELD_NAMES["all_returned"]: "no",
            FIELD_NAMES["cat_action"]: "reprocess claim",
            FIELD_NAMES["insurance"]: partner,
            FIELD_NAMES["claims_text"]: random.choice(UNPARSEABLE_TEXT),
        },
        [],  # no sim rows — no slugs
        {},
    )


def scn_woip_all_returned(counter, next_slug):
    n = random.randint(2, 4)
    slugs = [next_slug() for _ in range(n)]
    partner = random.choice(INSURANCE_PARTNERS)
    return _mk(
        "woip-all-returned",
        f"[SEED-{counter}] WOIP — {n} claims all back",
        "Waiting On Insurance Partner",
        {
            FIELD_NAMES["cx_ops"]: CX_OPS_WOIP,
            FIELD_NAMES["all_returned"]: "no",
            FIELD_NAMES["cat_action"]: random.choice(["reprocess claim", "edit/correct a claim"]),
            FIELD_NAMES["insurance"]: partner,
            FIELD_NAMES["claims_text"]: _claim_text(slugs),
        },
        [make_sim_row(s, partner, "actionable") for s in slugs],
        {s: "actionable" for s in slugs},
    )


def scn_woip_partial_returned(counter, next_slug):
    n = random.randint(3, 5)
    slugs = [next_slug() for _ in range(n)]
    partner = random.choice(INSURANCE_PARTNERS)
    # Mix: about half actionable, rest pending
    roles = []
    for i in range(n):
        roles.append("actionable" if i < n // 2 else "pending")
    random.shuffle(roles)
    return _mk(
        "woip-partial-returned",
        f"[SEED-{counter}] WOIP — {n} claims partial",
        "Waiting On Insurance Partner",
        {
            FIELD_NAMES["cx_ops"]: CX_OPS_WOIP,
            FIELD_NAMES["all_returned"]: "no",
            FIELD_NAMES["cat_action"]: random.choice(["reprocess claim", "edit/correct a claim"]),
            FIELD_NAMES["insurance"]: partner,
            FIELD_NAMES["claims_text"]: _claim_text(slugs),
        },
        [make_sim_row(s, partner, r) for s, r in zip(slugs, roles)],
        dict(zip(slugs, roles)),
    )


def scn_woip_all_pending(counter, next_slug):
    n = random.randint(2, 3)
    slugs = [next_slug() for _ in range(n)]
    partner = random.choice(INSURANCE_PARTNERS)
    return _mk(
        "woip-all-pending",
        f"[SEED-{counter}] WOIP — {n} claims pending",
        "Waiting On Insurance Partner",
        {
            FIELD_NAMES["cx_ops"]: CX_OPS_WOIP,
            FIELD_NAMES["all_returned"]: "no",
            FIELD_NAMES["cat_action"]: "reprocess claim",
            FIELD_NAMES["insurance"]: partner,
            FIELD_NAMES["claims_text"]: _claim_text(slugs),
        },
        [make_sim_row(s, partner, "pending") for s in slugs],
        {s: "pending" for s in slugs},
    )


def scn_woip_aging(counter, next_slug):
    n = random.randint(1, 2)
    slugs = [next_slug() for _ in range(n)]
    partner = random.choice(INSURANCE_PARTNERS)
    return _mk(
        "woip-aging",
        f"[SEED-{counter}] WOIP AGING — {n} old claims",
        "Waiting On Insurance Partner",
        {
            FIELD_NAMES["cx_ops"]: CX_OPS_WOIP,
            FIELD_NAMES["all_returned"]: "no",
            FIELD_NAMES["cat_action"]: "reprocess claim",
            FIELD_NAMES["insurance"]: partner,
            FIELD_NAMES["claims_text"]: _claim_text(slugs),
        },
        [make_sim_row(s, partner, "aging") for s in slugs],
        {s: "aging" for s in slugs},
    )


def scn_ongoing_various(counter, next_slug, discovery):
    slug = next_slug()
    partner = random.choice(INSURANCE_PARTNERS)
    # Pick any CX/OPs value except WOIP for ongoing tasks
    cx_ops_field = discovery["fields"].get("cx/ops task progress", {})
    options = [k for k in cx_ops_field.get("options", {})
               if "waiting" not in k and "completed" not in k]
    cx_val = random.choice(options) if options else "in progress"
    return _mk(
        "ongoing-various",
        f"[SEED-{counter}] Ongoing — {cx_val}",
        "Ongoing Tasks",
        {
            FIELD_NAMES["cx_ops"]: cx_val,
            FIELD_NAMES["all_returned"]: "no",
            FIELD_NAMES["cat_action"]: random.choice(
                ["reprocess claim", "edit/correct a claim", "cancel a claim"]),
            FIELD_NAMES["insurance"]: partner,
            FIELD_NAMES["claims_text"]: _claim_text([slug]),
        },
        [make_sim_row(slug, partner, random.choice(["pending", "resubmitted"]))],
        {slug: "ongoing"},
    )


def scn_completed_done(counter, next_slug):
    n = random.randint(1, 2)
    slugs = [next_slug() for _ in range(n)]
    partner = random.choice(INSURANCE_PARTNERS)
    return _mk(
        "completed-done",
        f"[SEED-{counter}] Completed task — {n} claims resolved",
        "Completed",
        {
            FIELD_NAMES["cx_ops"]: CX_OPS_COMPLETED,
            FIELD_NAMES["all_returned"]: "yes",
            FIELD_NAMES["cat_action"]: random.choice(["reprocess claim", "submit new claim"]),
            FIELD_NAMES["insurance"]: partner,
            FIELD_NAMES["claims_text"]: _claim_text(slugs),
        },
        [make_sim_row(s, partner, "actionable") for s in slugs],
        {s: "actionable" for s in slugs},
    )


# (scenario_name, weight, func, needs_discovery)
SCENARIO_WEIGHTS = [
    ("new-single-actionable",  5, scn_new_single_actionable,  False),
    ("new-multi-actionable",   3, scn_new_multi_actionable,   False),
    ("new-multi-pending",      3, scn_new_multi_pending,      False),
    ("new-unparseable",        1, scn_new_unparseable,        False),
    ("woip-all-returned",      4, scn_woip_all_returned,      False),
    ("woip-partial-returned",  3, scn_woip_partial_returned,  False),
    ("woip-all-pending",       2, scn_woip_all_pending,       False),
    ("woip-aging",             1, scn_woip_aging,             False),
    ("ongoing-various",        2, scn_ongoing_various,        True),
    ("completed-done",         1, scn_completed_done,         False),
]


def pick_scenario():
    weights = [w for _, w, _, _ in SCENARIO_WEIGHTS]
    return random.choices(SCENARIO_WEIGHTS, weights=weights, k=1)[0]


# ═══════════════════════════════════════════════════════════════════════════════
# ASANA CREATION
# ═══════════════════════════════════════════════════════════════════════════════
def create_task(pat, project_gid, spec, discovery):
    """Create a task from a spec dict. Returns (gid, error_or_none)."""
    sec_gid = section_gid(discovery, spec["section"])

    cf = {}
    for fname, fval in spec["fields"].items():
        gid, value = resolve_cf(discovery, fname, fval)
        cf[gid] = value

    body = {
        "name": spec["name"],
        "custom_fields": cf,
        "memberships": [{"project": project_gid, "section": sec_gid}],
        "projects": [project_gid],
    }
    # Snyk Code flags this as SSRF — false positive. URL is hardcoded inside
    # asana_req (https://app.asana.com/api/1.0); only the body varies, and the
    # body is internal Asana data, not URL routing input. CLI-only dev tool.
    # See .snyk for the policy entry.
    s, b = asana_req("POST", "/tasks", pat, body)  # noqa: snyk python/Ssrf
    if s in (200, 201):
        return b.get("data", {}).get("gid"), None
    return None, f"{s} {str(b)[:300]}"


# ═══════════════════════════════════════════════════════════════════════════════
# SIM SHEET — append rows to LightdashData tab
# ═══════════════════════════════════════════════════════════════════════════════
def ensure_sim_tab_ready(svc, sid):
    """Make sure LightdashData exists and has headers. Returns current row count."""
    ensure_tab(svc, sid, "LightdashData")
    existing = sh_read(svc, sid, "'LightdashData'!A:G")
    if not existing:
        sh_write(svc, sid, "'LightdashData'!A1:G1", [LD_COLUMNS])
        return 1
    return len(existing)


def append_sim_rows(svc, sid, rows):
    """Append to LightdashData using the Sheets API's append mode."""
    if not rows:
        return
    svc.spreadsheets().values().append(
        spreadsheetId=sid,
        range="'LightdashData'!A:G",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def existing_slugs(svc, sid):
    rows = sh_read(svc, sid, "'LightdashData'!A:A")
    out = set()
    for r in rows[1:]:
        if r and r[0]:
            out.add(r[0].strip().lower())
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# DRIVER
# ═══════════════════════════════════════════════════════════════════════════════
def banner(msg):
    print(); print("=" * 72); print(msg); print("=" * 72)


def cmd_list():
    data = load_runs()
    if not data["runs"]:
        print("(no recorded runs)"); return
    for rid, rec in sorted(data["runs"].items()):
        print(f"\n{rid}  — {rec.get('created_at', '?')}")
        print(f"  tasks:  {len(rec.get('tasks', []))}")
        print(f"  slugs:  {len(rec.get('slugs', []))}")
        scn_counts = {}
        for s in rec.get("scenarios", {}).values():
            scn_counts[s] = scn_counts.get(s, 0) + 1
        for k, v in sorted(scn_counts.items()):
            print(f"    {v:3d}  {k}")


def run_seeder(count, run_id, dry_run, seed):
    if seed is not None:
        random.seed(seed)

    load_env()
    pat = env("ASANA_PAT")
    project_gid = env("ASANA_PROJECT_GID")
    sid = env("GOOGLE_SHEETS_SPREADSHEET_ID")

    banner(f"Asana Seeder  —  run_id={run_id}  count={count}  dry_run={dry_run}")

    # Discovery
    print("\n[1/4] Discovering project sections + custom fields...")
    discovery = discover_project(pat, project_gid)
    print(f"  sections: {list(discovery['sections'].keys())}")
    for fname, f in discovery["fields"].items():
        opts = list(f["options"].keys()) if f["options"] else "(text)"
        print(f"  field: {fname!r} -> {opts}")

    # Prepare sim sheet
    print("\n[2/4] Preparing LightdashData tab...")
    svc = get_sheets_service() if not dry_run else None
    if not dry_run:
        row_count = ensure_sim_tab_ready(svc, sid)
        existing = existing_slugs(svc, sid)
        print(f"  Existing sim rows: {row_count - 1}, unique slugs: {len(existing)}")
    else:
        existing = set()

    next_slug = make_slug_generator(existing)

    # Generate + create
    print(f"\n[3/4] Generating {count} scenarios...")
    created = []          # list of task gids
    all_sim_rows = []     # flat list of sim rows
    slug_roles = {}       # slug -> role
    scenarios_used = {}   # task_gid (or temp id) -> scenario name
    failures = []

    for i in range(1, count + 1):
        scn_name, _, fn, needs_disc = pick_scenario()
        counter = f"{run_id}-{i:02d}"
        if needs_disc:
            pkg = fn(counter, next_slug, discovery)
        else:
            pkg = fn(counter, next_slug)

        print(f"  [{i:02d}/{count}] {scn_name:25s}  section={pkg['spec']['section']:30s}  "
              f"slugs={list(pkg['slug_roles'].keys())}")

        # Pre-flight: resolve all fields + options so we fail fast on name mismatches
        # even in dry-run (resolve_cf is pure lookup, no network).
        try:
            _ = section_gid(discovery, pkg["spec"]["section"])
            for fname, fval in pkg["spec"]["fields"].items():
                resolve_cf(discovery, fname, fval)
        except RuntimeError as resolve_err:
            print(f"    ✘ pre-flight resolve: {resolve_err}")
            failures.append((scn_name, f"pre-flight: {resolve_err}"))
            continue

        if dry_run:
            scenarios_used[f"dry-{i}"] = scn_name
            slug_roles.update(pkg["slug_roles"])
            all_sim_rows.extend(pkg["sim_rows"])
            continue

        # Calls asana_req under the hood — Snyk Code SSRF false positive.
        # URL is fixed; CLI-derived values flow into the body, not URL host.
        gid, err = create_task(pat, project_gid, pkg["spec"], discovery)  # noqa: snyk python/Ssrf
        if err:
            print(f"    ✘ create failed: {err}")
            failures.append((scn_name, err))
            continue
        created.append(gid)
        scenarios_used[gid] = scn_name
        slug_roles.update(pkg["slug_roles"])
        all_sim_rows.extend(pkg["sim_rows"])

    # Append sim rows
    print(f"\n[4/4] Appending {len(all_sim_rows)} sim rows to LightdashData...")
    if not dry_run and all_sim_rows:
        append_sim_rows(svc, sid, all_sim_rows)

    # Persist run record
    if not dry_run:
        save_run(run_id, {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "tasks": created,
            "slugs": list(slug_roles.keys()),
            "scenarios": scenarios_used,
            "slug_roles": slug_roles,
        })

    # Summary
    banner("SUMMARY")
    print(f"  Tasks created: {len(created)}{'  (DRY RUN)' if dry_run else ''}")
    print(f"  Sim rows appended: {len(all_sim_rows)}")
    print(f"  Unique claim slugs: {len(slug_roles)}")
    print(f"  Failures: {len(failures)}")
    for name, err in failures[:10]:
        print(f"    ✘ {name}: {err}")
    scn_counts = {}
    for s in scenarios_used.values():
        scn_counts[s] = scn_counts.get(s, 0) + 1
    print("\n  Scenario distribution:")
    for k, v in sorted(scn_counts.items()):
        print(f"    {v:3d}  {k}")
    print(f"\n  Run record: {run_id}")
    return len(failures) == 0


def main():
    p = argparse.ArgumentParser(description="Seed Asana tasks + LightdashData sim rows")
    p.add_argument("--count", type=int, default=25, help="Number of tasks to create (default 25)")
    p.add_argument("--run-id", default=None, help="Custom run ID (default: timestamp)")
    p.add_argument("--dry-run", action="store_true", help="Print plan only, no API calls")
    p.add_argument("--list", action="store_true", help="List past runs")
    p.add_argument("--seed", type=int, default=None, help="RNG seed for deterministic runs")
    a = p.parse_args()

    if a.list:
        cmd_list(); return 0

    if a.count < 1 or a.count > 200:
        print("--count must be 1..200"); return 2

    run_id = a.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ok = run_seeder(a.count, run_id, a.dry_run, a.seed)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
