#!/usr/bin/env python3
"""
CXOps_BPA_Asana_GID_Discovery.py
=================================
One-shot helper that enumerates sections, custom fields, and enum options
for one or more Asana projects, then prints env-var mappings ready to paste
into .env.

Use cases:
- New dev machine setup: dump every GID for the working project so .env
  fills out in 30 seconds instead of 30 minutes of clicking through Asana.
- Adding a new automation: dump GIDs for the central log project to map
  the per-function section to its env var.
- After a project structure change: re-dump and diff against current .env.

Usage:
    # Default: discover ASANA_PROJECT_GID and ASANA_LOG_PROJECT_GID from env
    python3 CXOps_BPA_Asana_GID_Discovery.py

    # Specific project(s) — pass --project repeatedly
    python3 CXOps_BPA_Asana_GID_Discovery.py --project 1234567890 --project 9876543210

    # Raw JSON (for scripting downstream tools)
    python3 CXOps_BPA_Asana_GID_Discovery.py --json

Read-only: this tool never writes to Asana.
"""
import argparse
import json
import os
import re
import sys
from pathlib import Path

# Windows-safe console output
try:
    sys.stdout.reconfigure(errors="replace")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).absolute().parent))
from test_suite import load_env, env, asana_req  # noqa: E402


# ──────────────────────────────────────────────────────────────────────────
# Asana fetchers
# ──────────────────────────────────────────────────────────────────────────
def fetch_project(pat, project_gid):
    s, b = asana_req("GET", f"/projects/{project_gid}?opt_fields=name,gid", pat)
    if s != 200:
        return None, f"status {s}: {str(b)[:200]}"
    return b.get("data", {}), None


def fetch_sections(pat, project_gid):
    s, b = asana_req(
        "GET", f"/projects/{project_gid}/sections?opt_fields=name,gid", pat
    )
    if s != 200:
        return [], f"status {s}: {str(b)[:200]}"
    return b.get("data", []), None


def fetch_custom_fields(pat, project_gid):
    q = (
        "?opt_fields=custom_field.name,custom_field.gid,"
        "custom_field.resource_subtype,"
        "custom_field.enum_options.name,custom_field.enum_options.gid,"
        "custom_field.enum_options.enabled"
    )
    s, b = asana_req("GET", f"/projects/{project_gid}/custom_field_settings{q}", pat)
    if s != 200:
        return [], f"status {s}: {str(b)[:200]}"
    return [s.get("custom_field", {}) for s in b.get("data", [])], None


# ──────────────────────────────────────────────────────────────────────────
# Heuristic name → env var mappings
# Patterns are case-insensitive substring/regex matches against Asana names.
# Add more here as new automations need new env vars.
# ──────────────────────────────────────────────────────────────────────────
SECTION_PATTERNS = [
    ("ASANA_SECTION_NEW",  r"new\s+tasks?"),
    ("ASANA_SECTION_WOIP", r"waiting\s+on\s+insurance"),
]

FIELD_PATTERNS = [
    ("ASANA_FIELD_CX_OPS",        r"cx[/\s]*ops.*progress"),
    ("ASANA_FIELD_CLAIMS_TEXT",   r"please\s+paste"),
    ("ASANA_FIELD_ALL_RETURNED",  r"all\s+claims\s+returned"),
]

# (env_var, parent_field_pattern, option_name_pattern)
OPTION_PATTERNS = [
    ("ASANA_OPT_WOIP",            r"cx[/\s]*ops",            r"waiting\s+on\s+insurance"),
    ("ASANA_OPT_NEEDS_FOLLOWUP",  r"cx[/\s]*ops",            r"needs\s+follow"),
    ("ASANA_OPT_RETURNED_YES",    r"all\s+claims\s+returned", r"^\s*yes\s*$"),
    ("ASANA_OPT_RETURNED_NO",     r"all\s+claims\s+returned", r"^\s*no\s*$"),
]


def _match(pattern, text):
    if not text:
        return False
    return bool(re.search(pattern, text, re.IGNORECASE))


def discover(pat, project_gid):
    """Return ({project, sections, fields}, None) or (None, err_str)."""
    proj, err = fetch_project(pat, project_gid)
    if err:
        return None, err
    sections, err = fetch_sections(pat, project_gid)
    if err:
        return None, err
    fields, err = fetch_custom_fields(pat, project_gid)
    if err:
        return None, err
    return {"project": proj, "sections": sections, "fields": fields}, None


# ──────────────────────────────────────────────────────────────────────────
# Output formatters
# ──────────────────────────────────────────────────────────────────────────
def print_human(info):
    proj = info["project"]
    print(f"\nProject: {proj.get('name', '?')}")
    print(f"  GID: {proj.get('gid', '?')}")

    print(f"\n  Sections ({len(info['sections'])}):")
    for sec in info["sections"]:
        print(f"    {sec.get('name', '?'):<35}  {sec.get('gid', '?')}")

    print(f"\n  Custom fields ({len(info['fields'])}):")
    for f in info["fields"]:
        ftype = f.get("resource_subtype", "?")
        print(f"    [{ftype:<6}] {f.get('name', '?'):<35}  {f.get('gid', '?')}")
        for opt in (f.get("enum_options") or []):
            if opt.get("enabled", True):
                print(f"           - {opt.get('name', '?'):<30}  {opt.get('gid', '?')}")


def print_env_mappings(info):
    """Heuristic-mapped env var suggestions. User must verify before pasting."""
    proj = info["project"]
    print(f"\n  Suggested .env entries for \"{proj.get('name', '?')}\":")

    # Project itself
    print(f"  ASANA_PROJECT_GID={proj.get('gid', '?')}")

    # Sections
    sections = info["sections"]
    for env_var, pattern in SECTION_PATTERNS:
        match = next(
            (s for s in sections if _match(pattern, s.get("name", ""))), None
        )
        if match:
            print(f"  {env_var}={match.get('gid', '')}"
                  f"  # \"{match.get('name', '')}\"")
        else:
            print(f"  # {env_var}=  (no section matched /{pattern}/)")

    # Fields
    fields = info["fields"]
    for env_var, pattern in FIELD_PATTERNS:
        match = next(
            (f for f in fields if _match(pattern, f.get("name", ""))), None
        )
        if match:
            print(f"  {env_var}={match.get('gid', '')}"
                  f"  # \"{match.get('name', '')}\"")
        else:
            print(f"  # {env_var}=  (no field matched /{pattern}/)")

    # Options (need parent field match too)
    for env_var, parent_pat, opt_pat in OPTION_PATTERNS:
        found = None
        for f in fields:
            if _match(parent_pat, f.get("name", "")):
                for opt in (f.get("enum_options") or []):
                    if _match(opt_pat, opt.get("name", "")):
                        found = (f, opt)
                        break
            if found:
                break
        if found:
            f, opt = found
            print(f"  {env_var}={opt.get('gid', '')}"
                  f"  # \"{opt.get('name', '')}\" in field \"{f.get('name', '')}\"")
        else:
            print(f"  # {env_var}=  (no option matched parent=/{parent_pat}/ option=/{opt_pat}/)")


def print_log_project_hint(info):
    """For the central log project, point at the section that should be set
    as CXOPS_NORMAL_CLAIMS_ETL_LOG_SECTION_GID."""
    proj_name_lc = info["project"].get("name", "").lower()
    if "log" not in proj_name_lc and "audit" not in proj_name_lc:
        return  # probably not the log project
    sections = info["sections"]
    print(f"\n  Looks like a log project. Section candidates:")
    print(f"  ASANA_LOG_PROJECT_GID={info['project'].get('gid', '?')}")
    for s in sections:
        print(f"  # CXOPS_NORMAL_CLAIMS_ETL_LOG_SECTION_GID="
              f"{s.get('gid', '')}  # \"{s.get('name', '')}\" — pick the right one")


# ──────────────────────────────────────────────────────────────────────────
# Driver
# ──────────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--project", action="append", default=[],
        help="Project GID (can pass multiple). "
             "Default: ASANA_PROJECT_GID + ASANA_LOG_PROJECT_GID from env.",
    )
    p.add_argument(
        "--json", action="store_true",
        help="Output raw JSON instead of human-readable + env mappings.",
    )
    args = p.parse_args()

    load_env()
    pat = env("ASANA_PAT")

    project_gids = args.project[:]
    if not project_gids:
        for envk in ("ASANA_PROJECT_GID", "ASANA_LOG_PROJECT_GID"):
            v = os.getenv(envk, "").strip()
            if v and v not in project_gids:
                project_gids.append(v)

    if not project_gids:
        print(
            "No projects to discover.\n"
            "Set ASANA_PROJECT_GID and/or ASANA_LOG_PROJECT_GID in .env, "
            "or pass --project <gid> on the command line.",
            file=sys.stderr,
        )
        return 2

    print("=" * 78)
    print("Asana GID Discovery")
    print("=" * 78)
    print(f"  Discovering {len(project_gids)} project(s): {project_gids}")

    all_results = []
    for gid in project_gids:
        info, err = discover(pat, gid)
        if err:
            print(f"\n  ✘ Failed to discover project {gid}: {err}", file=sys.stderr)
            continue
        all_results.append(info)

    if args.json:
        print(json.dumps(all_results, indent=2))
        return 0

    for info in all_results:
        print()
        print("=" * 78)
        print_human(info)
        print()
        print_env_mappings(info)
        print_log_project_hint(info)

    print()
    print("=" * 78)
    print("Done. Copy the suggested mappings to .env and verify each one is correct.")
    print("Heuristic matches are best-effort — if a name doesn't match the patterns,")
    print("the line is commented out and you'll need to pick the right GID by hand.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
