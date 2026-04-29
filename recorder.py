"""Run recorder — one structured Asana record per pipeline run.

Two modes of writing to Asana, picked by which constructor args are set:

  Mode A — comment on an existing task (legacy pattern, used by asana_monitor).
      RunRecorder(pat, run_id, sweep_name, task_gid="...")
      Each run appends a comment to that single task.

  Mode B — create a new task in a section of a central log project (new pattern,
            used by CXOps_BPA_Normal_Claims_Status_ETL.py).
      RunRecorder(pat, run_id, sweep_name,
                  log_project_gid="...", log_section_gid="...")
      Each run creates a fresh task in the section, with the run summary as
      the task body. One Asana task per run, easy to scan in the project view.

  No mode — neither task_gid nor section_gid set: log to stdout / stderr only
            (warning emitted on flush). Useful before UUIDs are configured.

All Asana writes use bypass_dry_run=True. The recorder is meta-level —
recording a run is not the same as mutating working data, so it must succeed
even when the dry-run flag is muting working-data writes.

Usage:
    rec = RunRecorder(pat, run_id, "my_sweep",
                      log_project_gid=os.getenv("ASANA_LOG_PROJECT_GID"),
                      log_section_gid=os.getenv("MY_SWEEP_LOG_SECTION_GID"))
    try:
        rec.log_action("step 1: read 28 pending rows")
        rec.record(rows_processed=28)
        rec.flush(status="success")
    except Exception as e:
        rec.error(repr(e))
        rec.flush(status="error")
        raise
"""
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


class RunRecorder:
    def __init__(self, pat: str, run_id: str, sweep_name: str = "sweep",
                 *,
                 task_gid: Optional[str] = None,
                 log_project_gid: Optional[str] = None,
                 log_section_gid: Optional[str] = None):
        self.pat = pat
        self.run_id = run_id
        self.sweep_name = sweep_name
        self.task_gid = (task_gid or "").strip()
        self.log_project_gid = (log_project_gid or "").strip()
        self.log_section_gid = (log_section_gid or "").strip()
        self._start = time.monotonic()
        self.counters: dict = {}
        self.errors: list = []
        self.action_log: list = []
        self._flushed = False

    # -- Collection ----------------------------------------------------------

    def record(self, **kwargs) -> None:
        """Accumulate counters (int values are added) or set non-int values."""
        for k, v in kwargs.items():
            if isinstance(v, int) and isinstance(self.counters.get(k), int):
                self.counters[k] += v
            elif isinstance(v, int) and k not in self.counters:
                self.counters[k] = v
            else:
                self.counters[k] = v

    def error(self, msg: str) -> None:
        self.errors.append(msg)

    def log_action(self, msg: str) -> None:
        """Append a free-form action description ("Step N: read X pending rows",
        "Sheet1!F5:K5 = [...] (would-write)", etc.). Captured in the run
        record body so a reader sees what the run actually did."""
        self.action_log.append(msg)

    # -- Rendering -----------------------------------------------------------

    def format_comment(self, status: str = "success",
                       duration_s: Optional[float] = None) -> str:
        """Render the Asana body. Public so tests can assert format."""
        if duration_s is None:
            duration_s = time.monotonic() - self._start
        lines = [
            f"[run {self.run_id}] {self.sweep_name} (status={status})",
            f"duration: {duration_s:.1f}s",
        ]
        for k in sorted(self.counters):
            lines.append(f"{k}: {self.counters[k]}")
        if self.errors:
            lines.append("")
            lines.append("errors:")
            for e in self.errors:
                lines.append(f"  - {e}")
        if self.action_log:
            lines.append("")
            lines.append("actions:")
            for a in self.action_log:
                lines.append(f"  {a}")
        return "\n".join(lines)

    # -- Posting -------------------------------------------------------------

    def _post_new_task_in_section(self, body: str, status: str) -> tuple:
        """Mode B: create a fresh task in the central log project section."""
        from test_suite import asana_req
        return asana_req(
            "POST", "/tasks", self.pat,
            body={
                "name": f"{self.sweep_name} {self.run_id} ({status})",
                "notes": body,
                "projects": [self.log_project_gid],
                "memberships": [{
                    "project": self.log_project_gid,
                    "section": self.log_section_gid,
                }],
            },
            bypass_dry_run=True,  # log writes always go through
        )

    def _post_comment_on_task(self, body: str) -> tuple:
        """Mode A: append a comment to the existing recorder task."""
        from test_suite import asana_req
        return asana_req(
            "POST", f"/tasks/{self.task_gid}/stories", self.pat,
            body={"text": body},
            bypass_dry_run=True,
        )

    def flush(self, status: str = "success") -> None:
        """Write the run record. Idempotent — second flush is a no-op so
        callers can flush in both happy and error paths."""
        if self._flushed:
            return
        self._flushed = True
        body = self.format_comment(status)

        if self.log_project_gid and self.log_section_gid:
            s, b = self._post_new_task_in_section(body, status)
            if s in (200, 201):
                logger.info("recorder_task_created",
                            extra={"new_task_gid": (b or {}).get("data", {}).get("gid", "")})
            else:
                logger.error("recorder_task_create_failed",
                             extra={"status": s, "section": self.log_section_gid})
            return

        if self.task_gid:
            s, _ = self._post_comment_on_task(body)
            if s in (200, 201):
                logger.info("recorder_post_ok",
                            extra={"task_gid": self.task_gid, "status_code": s})
            else:
                logger.error("recorder_post_failed",
                             extra={"status": s, "task_gid": self.task_gid})
            return

        logger.warning("recorder_log_destination_unset",
                       extra={"would_post_first_200_chars": body[:200]})
        # Also dump to stdout so dev runs without UUIDs configured see the body.
        print("\n" + "─" * 72)
        print("RUN RECORD (no Asana destination configured — printing only)")
        print("─" * 72)
        print(body)
        print("─" * 72)
