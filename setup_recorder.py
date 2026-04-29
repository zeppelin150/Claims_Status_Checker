#!/usr/bin/env python3
"""setup_recorder.py — create the Asana run-recorder task once.

Posts a new task to ASANA_PROJECT_GID and prints its GID. Add the GID to
.env / GH secrets as ASANA_RECORDER_TASK_GID.

Idempotent: if ASANA_RECORDER_TASK_GID is already set and resolves to a
real task, exits without creating a duplicate.

Usage:
    python3 setup_recorder.py
"""
import sys
from pathlib import Path

# Windows-safe console output
try:
    sys.stdout.reconfigure(errors="replace")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).absolute().parent))
from test_suite import load_env, env, asana_req  # noqa: E402


RECORDER_TASK_NAME = "Pipeline Run Log"
RECORDER_TASK_NOTES = (
    "Automated run records — append-only via comments. "
    "Created by setup_recorder.py. "
    "Each comment is one pipeline sweep tagged with a run_id."
)


def main():
    load_env()
    pat = env("ASANA_PAT")
    project_gid = env("ASANA_PROJECT_GID")

    existing = (env("ASANA_RECORDER_TASK_GID", required=False) or "").strip()
    if existing:
        s, b = asana_req("GET", f"/tasks/{existing}?opt_fields=name", pat)
        if s == 200:
            name = b.get("data", {}).get("name", "?")
            print(f"Recorder task already exists: {existing} ({name})")
            return 0
        print(f"Configured GID {existing} not found ({s}). Creating a new one.")

    s, b = asana_req("POST", "/tasks", pat, {
        "name": RECORDER_TASK_NAME,
        "notes": RECORDER_TASK_NOTES,
        "projects": [project_gid],
    })
    if s not in (200, 201):
        print(f"Failed to create recorder task: status={s} body={b}", file=sys.stderr)
        return 1

    gid = b.get("data", {}).get("gid", "")
    if not gid:
        print(f"Created task but no GID returned in response: {b}", file=sys.stderr)
        return 1

    print(f"Created recorder task: {gid}")
    print()
    print("Add to .env (and GitHub repo secrets):")
    print(f"  ASANA_RECORDER_TASK_GID={gid}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
