"""Structured logging + run_id propagation.

One JSON line per event. run_id is held in a contextvar so it propagates
across helper calls without explicit threading. Designed to be the single
import for any module that wants structured logs:

    from claims_logging import setup_logging, gen_run_id, set_run_id, get_run_id
    setup_logging()
    set_run_id(gen_run_id())
    logger = logging.getLogger(__name__)
    logger.info("woip_sweep_start", extra={"task_count": 12})

Output (one line):
    {"ts": "2026-04-29T14:05:02Z", "level": "INFO", "run_id": "20260429T140502Z-a3f9b1",
     "logger": "asana_monitor", "msg": "woip_sweep_start", "task_count": 12}
"""
import json
import logging
import secrets
import sys
from contextvars import ContextVar
from datetime import datetime, timezone

_run_id_var: ContextVar[str] = ContextVar("run_id", default="")

# Logging record attributes that the stdlib library populates — skipped when
# we serialize "extra" fields, since they're noise in run records.
_STD_LOGRECORD_ATTRS = frozenset({
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message", "taskName", "asctime",
})


def gen_run_id() -> str:
    """Sortable, ~1-in-a-million-collision run identifier.
    Format: '20260429T140502Z-a3f9b1' (UTC iso-compact + 3 hex bytes)."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}-{secrets.token_hex(3)}"


def set_run_id(rid: str) -> None:
    _run_id_var.set(rid)


def get_run_id() -> str:
    return _run_id_var.get()


class JsonFormatter(logging.Formatter):
    """Formats LogRecords as single-line JSON. Includes run_id from contextvar
    plus any kwargs the caller passed via `extra={...}`."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "logger": record.name,
            "run_id": _run_id_var.get(),
            "msg": record.getMessage(),
        }
        for k, v in record.__dict__.items():
            if k in _STD_LOGRECORD_ATTRS:
                continue
            payload[k] = v
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: int = logging.INFO, stream=None) -> None:
    """Wire the root logger to emit JSON lines on stderr. Idempotent —
    safe to call from multiple entrypoints in the same process."""
    root = logging.getLogger()
    if any(getattr(h, "_claims_json", False) for h in root.handlers):
        # Already configured (likely by another module's setup_logging call).
        return
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JsonFormatter())
    handler._claims_json = True  # marker for idempotency check
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
