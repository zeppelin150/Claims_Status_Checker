"""Unit tests — pure functions, no I/O, no network.

Each test should run in <50ms. The whole file should run in <1s.
"""
import os
from datetime import date, datetime, timedelta, timezone

import pytest

import test_suite as ts


# ---------------------------------------------------------------------------
# parse_claim_slugs
# ---------------------------------------------------------------------------
class TestParseClaimSlugs:
    def test_extracts_bare_slugs(self):
        slugs, review = ts.parse_claim_slugs("abc123 def456")
        assert slugs == ["abc123", "def456"]
        assert review is False

    def test_strips_cl_prefix_case_insensitive(self):
        slugs, _ = ts.parse_claim_slugs("cl-AbC123 CL-DEF456")
        assert slugs == ["abc123", "def456"]

    def test_dedupes_preserving_order(self):
        slugs, _ = ts.parse_claim_slugs("abc123, abc123, def456")
        assert slugs == ["abc123", "def456"]

    def test_empty_string_needs_review(self):
        slugs, review = ts.parse_claim_slugs("")
        assert slugs == []
        assert review is True

    def test_only_punctuation_needs_review(self):
        slugs, review = ts.parse_claim_slugs("!!! ??? ---")
        assert slugs == []
        assert review is True

    def test_eight_char_extracts_first_six(self):
        # Documented behavior: 8-char token like "lkopioty" → first 6 chars.
        slugs, _ = ts.parse_claim_slugs("lkopioty")
        assert slugs == ["lkopio"]


# ---------------------------------------------------------------------------
# is_actionable
# ---------------------------------------------------------------------------
class TestIsActionable:
    def test_completed_era_posted_is_actionable(self):
        assert ts.is_actionable("Completed - ERA Posted", "") is True

    def test_rejected_is_actionable(self):
        assert ts.is_actionable("rejected", "") is True

    def test_resubmitted_is_not_actionable_even_with_adj_date(self):
        # "resubmitted" is in NON_ACTIONABLE_STATUSES — wins over adj_day fallback.
        assert ts.is_actionable("resubmitted", "2026-01-01") is False

    def test_submitted_with_adj_date_is_actionable(self):
        # No status match either way → adj_day populated → True.
        assert ts.is_actionable("submitted", "2026-01-01") is True

    def test_submitted_without_adj_date_is_not_actionable(self):
        assert ts.is_actionable("submitted", "") is False


# ---------------------------------------------------------------------------
# days_since_submission
# ---------------------------------------------------------------------------
class TestDaysSinceSubmission:
    def _iso(self, d):
        return d.strftime("%Y-%m-%dT00:00:00.000Z")

    def test_past_date_returns_positive(self):
        ten_days_ago = ts.utc_today() - timedelta(days=10)
        assert ts.days_since_submission(self._iso(ten_days_ago)) == 10

    def test_today_returns_zero(self):
        assert ts.days_since_submission(self._iso(ts.utc_today())) == 0

    def test_future_date_returns_negative(self):
        # Nonsensical in production but the function shouldn't crash.
        future = ts.utc_today() + timedelta(days=5)
        assert ts.days_since_submission(self._iso(future)) == -5

    def test_empty_string_returns_none(self):
        assert ts.days_since_submission("") is None

    def test_none_returns_none(self):
        assert ts.days_since_submission(None) is None

    def test_malformed_returns_none(self):
        assert ts.days_since_submission("not a date") is None

    def test_handles_z_suffix_iso(self):
        ninety_days_ago = ts.utc_today() - timedelta(days=90)
        # Both Z-suffixed and +00:00-suffixed should parse identically.
        assert ts.days_since_submission(self._iso(ninety_days_ago)) == 90


# ---------------------------------------------------------------------------
# calc_aging
# ---------------------------------------------------------------------------
class TestCalcAging:
    def test_under_threshold_is_false(self):
        assert ts.calc_aging(30) == "FALSE"

    def test_over_threshold_is_aging(self):
        assert ts.calc_aging(120) == "Aging"

    def test_at_threshold_is_false(self):
        # Strict >, not >=: exactly 90 days is NOT yet aging.
        assert ts.calc_aging(ts.AGING_THRESHOLD_DAYS) == "FALSE"

    def test_one_past_threshold_is_aging(self):
        assert ts.calc_aging(ts.AGING_THRESHOLD_DAYS + 1) == "Aging"

    def test_none_returns_empty(self):
        assert ts.calc_aging(None) == ""

    def test_zero_returns_false(self):
        assert ts.calc_aging(0) == "FALSE"


# ---------------------------------------------------------------------------
# process_claim integration of pure pieces
# ---------------------------------------------------------------------------
class TestProcessClaim:
    def _row(self, status="submitted", adj="", submitted=""):
        return {
            ts.LD_STATUS: status,
            ts.LD_ADJ_DAY: adj,
            ts.LD_SUBMITTED: submitted,
        }

    def test_single_actionable_row(self):
        res = ts.process_claim("abc123", [self._row(status="rejected")])
        assert res["return_check"] == "TRUE"
        assert res["true_count"] == 1
        assert res["row_count"] == 1
        assert res["work_ticket"] == "WORK"

    def test_no_actionable_rows(self):
        res = ts.process_claim("abc123", [self._row(status="submitted")])
        assert res["return_check"] == "FALSE"
        assert res["work_ticket"] == ""

    def test_aging_uses_oldest_submission(self):
        # Two rows: one 30 days old, one 120 days old. Aging should fire
        # on the 120 (the oldest) — proves we track max days, not min.
        iso = lambda d: d.strftime("%Y-%m-%dT00:00:00.000Z")
        recent = iso(ts.utc_today() - timedelta(days=30))
        old = iso(ts.utc_today() - timedelta(days=120))
        res = ts.process_claim("abc123", [
            self._row(status="submitted", submitted=recent),
            self._row(status="submitted", submitted=old),
        ])
        assert res["send_to_aging"] == "TRUE"
        assert res["aging_status"] == "120"


# ---------------------------------------------------------------------------
# build_slug_sql
# ---------------------------------------------------------------------------
class TestBuildSlugSql:
    def test_valid_slugs_produce_sql(self):
        sql = ts.build_slug_sql(["abc123", "def456"])
        assert sql is not None
        assert "'abc123'" in sql
        assert "'def456'" in sql

    def test_drops_invalid_slugs(self):
        sql = ts.build_slug_sql(["abc123", "TOO_LONG_TO_BE_VALID"])
        assert "'abc123'" in sql
        assert "TOO_LONG" not in sql

    def test_returns_none_when_all_invalid(self):
        assert ts.build_slug_sql(["bad slug", "also bad"]) is None

    def test_returns_none_for_empty_input(self):
        assert ts.build_slug_sql([]) is None

    def test_normalizes_to_lowercase(self):
        sql = ts.build_slug_sql(["ABC123"])
        assert "'abc123'" in sql


# ---------------------------------------------------------------------------
# _resolve_sa_source — service account auth path selection
# ---------------------------------------------------------------------------
class TestResolveSASource:
    def test_file_wins_when_both_set(self, monkeypatch, tmp_path):
        sa_file = tmp_path / "sa.json"
        sa_file.write_text("{}")
        monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(sa_file))
        monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_JSON", '{"type":"service_account"}')
        kind, value = ts._resolve_sa_source()
        assert kind == "file"
        assert value == str(sa_file)

    def test_json_when_only_json_set(self, monkeypatch):
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
        monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_JSON", '{"type":"service_account"}')
        kind, value = ts._resolve_sa_source()
        assert kind == "json"
        assert "service_account" in value

    @pytest.mark.skip(reason="Temp-demo-fallback in test_suite._resolve_sa_source "
                             "currently returns ('adc', None) instead of raising. "
                             "Re-enable this test when that block is removed post-demo.")
    def test_raises_when_neither_set(self, monkeypatch):
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_JSON", raising=False)
        monkeypatch.delenv("GOOGLE_USE_ADC", raising=False)
        with pytest.raises(RuntimeError, match="GOOGLE_SERVICE_ACCOUNT"):
            ts._resolve_sa_source()

    def test_raises_when_file_path_does_not_exist(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", "/nonexistent/path.json")
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_JSON", raising=False)
        monkeypatch.delenv("GOOGLE_USE_ADC", raising=False)
        with pytest.raises(RuntimeError, match="not found"):
            ts._resolve_sa_source()

    def test_adc_path_when_use_adc_true_and_others_unset(self, monkeypatch):
        # Dev machines without GCP access use this path: gcloud user login.
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_JSON", raising=False)
        monkeypatch.setenv("GOOGLE_USE_ADC", "true")
        kind, value = ts._resolve_sa_source()
        assert kind == "adc"
        assert value is None

    def test_file_still_wins_over_adc_when_both_set(self, monkeypatch, tmp_path):
        sa_file = tmp_path / "sa.json"
        sa_file.write_text("{}")
        monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(sa_file))
        monkeypatch.setenv("GOOGLE_USE_ADC", "true")
        kind, _ = ts._resolve_sa_source()
        assert kind == "file"

    def test_temp_demo_fallback_returns_adc_when_nothing_set(self, monkeypatch, capsys):
        """TEMP DEMO BEHAVIOR: with no SA env vars set, falls back to ADC
        instead of raising. When the temp-demo-fallback block is removed
        post-demo, this test should be deleted alongside it (and the
        test_error_message_mentions_all_three_options test re-enabled)."""
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_JSON", raising=False)
        monkeypatch.delenv("GOOGLE_USE_ADC", raising=False)
        kind, value = ts._resolve_sa_source()
        assert kind == "adc"
        assert value is None
        # The fallback prints a warning to stdout so the operator notices.
        captured = capsys.readouterr()
        assert "temp-demo-fallback" in captured.out


# ---------------------------------------------------------------------------
# sh_batch_write
# ---------------------------------------------------------------------------
class TestShBatchWrite:
    def test_empty_updates_is_noop(self, mock_sheets_svc):
        ts.sh_batch_write(mock_sheets_svc, "sid", [])
        mock_sheets_svc.spreadsheets().values().batchUpdate.assert_not_called()

    def test_single_range_formats_request(self, mock_sheets_svc):
        ts.sh_batch_write(mock_sheets_svc, "sid", [
            ("Sheet1!F5:K5", [["TRUE", "1", "1", "WORK", "10", "FALSE"]]),
        ])
        call = mock_sheets_svc.spreadsheets().values().batchUpdate.call_args
        body = call.kwargs["body"]
        assert body["valueInputOption"] == "USER_ENTERED"
        assert len(body["data"]) == 1
        assert body["data"][0]["range"] == "Sheet1!F5:K5"
        assert body["data"][0]["values"] == [["TRUE", "1", "1", "WORK", "10", "FALSE"]]

    def test_scattered_ranges_all_present_in_single_call(self, mock_sheets_svc):
        # The whole point: discontinuous row indices in one HTTP call.
        updates = [
            ("Sheet1!F5:K5", [["a"]]),
            ("Sheet1!F47:K47", [["b"]]),
            ("Sheet1!F188:K188", [["c"]]),
            ("Sheet1!F23:K23", [["d"]]),
        ]
        ts.sh_batch_write(mock_sheets_svc, "sid", updates)

        # batchUpdate should be called exactly once (after the spreadsheets()/values() chain).
        # Find the actual batchUpdate call (filter out the chained-construction calls).
        bu = mock_sheets_svc.spreadsheets().values().batchUpdate
        actual_calls = [c for c in bu.call_args_list if "body" in c.kwargs]
        assert len(actual_calls) == 1
        body = actual_calls[0].kwargs["body"]
        assert len(body["data"]) == 4
        ranges = [d["range"] for d in body["data"]]
        assert "Sheet1!F5:K5" in ranges
        assert "Sheet1!F47:K47" in ranges
        assert "Sheet1!F188:K188" in ranges
        assert "Sheet1!F23:K23" in ranges

    def test_custom_value_input_option(self, mock_sheets_svc):
        ts.sh_batch_write(mock_sheets_svc, "sid",
                          [("Sheet1!A1:A1", [["x"]])],
                          value_input_option="RAW")
        call = mock_sheets_svc.spreadsheets().values().batchUpdate.call_args
        assert call.kwargs["body"]["valueInputOption"] == "RAW"


# ---------------------------------------------------------------------------
# CXOps_BPA_Normal_Claims_Status_ETL — mode resolution + helpers
# ---------------------------------------------------------------------------
class TestResolveMode:
    """The IS_CI / WRITE_ONE_ROW / GITHUB_ACTIONS state machine, in isolation."""

    def teardown_method(self):
        # Clean any env vars these tests poke at so they don't leak.
        for k in ("IS_CI", "WRITE_ONE_ROW", "GITHUB_ACTIONS"):
            os.environ.pop(k, None)

    def test_local_default_is_dry_run(self, monkeypatch):
        import importlib
        monkeypatch.delenv("IS_CI", raising=False)
        monkeypatch.delenv("WRITE_ONE_ROW", raising=False)
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
        etl = importlib.import_module("CXOps_BPA_Normal_Claims_Status_ETL")
        m = etl.resolve_mode()
        assert m["is_local"] is True
        assert m["is_dry_run"] is True
        assert m["is_single_row"] is False
        assert m["is_ci"] is False

    def test_local_with_write_one_row_is_single_row_mode(self, monkeypatch):
        import importlib
        monkeypatch.setenv("IS_CI", "false")
        monkeypatch.setenv("WRITE_ONE_ROW", "true")
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
        etl = importlib.import_module("CXOps_BPA_Normal_Claims_Status_ETL")
        m = etl.resolve_mode()
        assert m["is_dry_run"] is False
        assert m["is_single_row"] is True

    def test_is_ci_true_only_honored_on_github_actions(self, monkeypatch):
        """The safety guard: IS_CI=true on a dev machine refuses to run."""
        import importlib
        monkeypatch.setenv("IS_CI", "true")
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
        etl = importlib.import_module("CXOps_BPA_Normal_Claims_Status_ETL")
        with pytest.raises(RuntimeError, match="GITHUB_ACTIONS"):
            etl.resolve_mode()

    def test_is_ci_true_on_github_actions_passes(self, monkeypatch):
        import importlib
        monkeypatch.setenv("IS_CI", "true")
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        etl = importlib.import_module("CXOps_BPA_Normal_Claims_Status_ETL")
        m = etl.resolve_mode()
        assert m["is_ci"] is True
        assert m["is_local"] is False
        assert m["is_dry_run"] is False
        assert m["is_single_row"] is False

    def test_invalid_bool_raises(self, monkeypatch):
        import importlib
        monkeypatch.setenv("IS_CI", "maybe")
        etl = importlib.import_module("CXOps_BPA_Normal_Claims_Status_ETL")
        with pytest.raises(ValueError, match="must be true/false"):
            etl.resolve_mode()


class TestTrimToFirstPending:
    def test_empty_returns_empty(self):
        import importlib
        etl = importlib.import_module("CXOps_BPA_Normal_Claims_Status_ETL")
        assert etl.trim_to_first_pending({}) == {}

    def test_keeps_first_only(self):
        import importlib
        etl = importlib.import_module("CXOps_BPA_Normal_Claims_Status_ETL")
        # Python 3.7+: dict preserves insertion order, so "first" is well-defined.
        result = etl.trim_to_first_pending({"abc": 5, "def": 47, "ghi": 188})
        assert result == {"abc": 5}


# ---------------------------------------------------------------------------
# claims_logging
# ---------------------------------------------------------------------------
class TestGenRunId:
    def test_format_is_sortable_iso_plus_hex(self):
        import re
        from claims_logging import gen_run_id
        rid = gen_run_id()
        assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{6}", rid), rid

    def test_two_calls_differ(self):
        from claims_logging import gen_run_id
        assert gen_run_id() != gen_run_id()


class TestRunIdContextVar:
    def test_set_get_round_trip(self):
        from claims_logging import set_run_id, get_run_id
        set_run_id("test-rid-123")
        assert get_run_id() == "test-rid-123"

    def test_default_is_empty_string(self):
        from claims_logging import get_run_id, _run_id_var
        # Reset by setting empty
        _run_id_var.set("")
        assert get_run_id() == ""


class TestJsonFormatter:
    def _capture(self, **extra):
        import io
        import logging
        from claims_logging import JsonFormatter
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="x", lineno=0,
            msg="hello", args=(), exc_info=None,
        )
        for k, v in extra.items():
            setattr(record, k, v)
        return formatter.format(record)

    def test_emits_valid_json(self):
        import json as _json
        out = self._capture()
        parsed = _json.loads(out)
        assert parsed["msg"] == "hello"
        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "test"

    def test_includes_run_id_from_contextvar(self):
        import json as _json
        from claims_logging import set_run_id
        set_run_id("ctx-run-123")
        out = self._capture()
        assert _json.loads(out)["run_id"] == "ctx-run-123"

    def test_propagates_extra_fields(self):
        import json as _json
        out = self._capture(task_count=12, slug="abc123")
        parsed = _json.loads(out)
        assert parsed["task_count"] == 12
        assert parsed["slug"] == "abc123"

    def test_skips_stdlib_logrecord_attrs(self):
        import json as _json
        out = self._capture()
        parsed = _json.loads(out)
        # Standard attrs like 'pathname', 'thread', etc. should not be in output.
        for noisy in ("pathname", "thread", "lineno", "funcName"):
            assert noisy not in parsed


class TestSetupLogging:
    def test_idempotent(self):
        import logging
        from claims_logging import setup_logging
        setup_logging()
        before = list(logging.getLogger().handlers)
        setup_logging()
        setup_logging()
        after = list(logging.getLogger().handlers)
        assert len(before) == len(after) == 1


# ---------------------------------------------------------------------------
# RunRecorder
# ---------------------------------------------------------------------------
class TestRunRecorder:
    def test_collects_int_counters_additively(self):
        from recorder import RunRecorder
        r = RunRecorder("pat", "run-1", task_gid="task-gid")
        r.record(rows_appended=3)
        r.record(rows_appended=5)
        r.record(tasks_seen=12)
        assert r.counters["rows_appended"] == 8
        assert r.counters["tasks_seen"] == 12

    def test_non_int_values_set_replaces(self):
        from recorder import RunRecorder
        r = RunRecorder("pat", "run-1", task_gid="task-gid")
        r.record(status_label="pending")
        r.record(status_label="done")
        assert r.counters["status_label"] == "done"

    def test_format_comment_structure(self):
        from recorder import RunRecorder
        r = RunRecorder("pat", "run-X", "woip_sweep", task_gid="task-gid")
        r.record(rows_appended=3, tasks_seen=12)
        body = r.format_comment(status="success", duration_s=14.2)
        assert body.startswith("[run run-X] woip_sweep (status=success)")
        assert "duration: 14.2s" in body
        assert "rows_appended: 3" in body
        assert "tasks_seen: 12" in body

    def test_format_comment_includes_errors(self):
        from recorder import RunRecorder
        r = RunRecorder("pat", "run-X", task_gid="task-gid")
        r.error("Sheets 429 after 6 retries")
        r.error("task gid 12345 update failed")
        body = r.format_comment(status="partial", duration_s=8.0)
        assert "errors:" in body
        assert "Sheets 429" in body
        assert "12345" in body

    def test_flush_skips_post_when_no_destination(self, monkeypatch):
        # Neither task_gid nor section set → flush warns + falls back to stdout.
        from recorder import RunRecorder
        import test_suite

        called = {}
        def fake_req(*args, **kwargs):
            called["yes"] = True
            return 201, {}
        monkeypatch.setattr(test_suite, "asana_req", fake_req)

        r = RunRecorder("pat", "run-1")  # no task_gid, no section
        r.flush(status="success")
        assert "yes" not in called

    def test_flush_posts_when_task_gid_set(self, monkeypatch):
        from recorder import RunRecorder
        import test_suite

        captured = {}
        def fake_req(method, path, pat, body=None, **kw):
            captured["method"] = method
            captured["path"] = path
            captured["pat"] = pat
            captured["body"] = body
            captured["bypass_dry_run"] = kw.get("bypass_dry_run", False)
            return 201, {}

        monkeypatch.setattr(test_suite, "asana_req", fake_req)
        r = RunRecorder("test-pat", "run-1", "test_sweep", task_gid="recorder-gid")
        r.record(x=1)
        r.flush(status="success")
        assert captured["pat"] == "test-pat"
        assert captured["path"] == "/tasks/recorder-gid/stories"
        assert "[run run-1] test_sweep (status=success)" in captured["body"]["text"]
        assert "x: 1" in captured["body"]["text"]
        assert captured["bypass_dry_run"] is True

    def test_flush_idempotent(self, monkeypatch):
        from recorder import RunRecorder
        import test_suite

        calls = []
        monkeypatch.setattr(test_suite, "asana_req",
                            lambda *a, **kw: (calls.append(a) or (201, {})))

        r = RunRecorder("pat", "run-1", task_gid="recorder-gid")
        r.flush(status="success")
        r.flush(status="success")
        r.flush(status="error")
        assert len(calls) == 1, "second/third flush should be no-op"

    # -- Mode B: create new task in central log project section ---------------

    def test_flush_creates_task_in_section_when_log_project_set(self, monkeypatch):
        from recorder import RunRecorder
        import test_suite

        captured = {}
        def fake_req(method, path, pat, body=None, **kw):
            captured["method"] = method
            captured["path"] = path
            captured["body"] = body
            captured["bypass"] = kw.get("bypass_dry_run", False)
            return 201, {"data": {"gid": "newly-created-task"}}

        monkeypatch.setattr(test_suite, "asana_req", fake_req)

        r = RunRecorder("pat", "run-9", "etl_run",
                        log_project_gid="proj-1", log_section_gid="sec-1")
        r.record(rows=10)
        r.flush(status="success")

        assert captured["method"] == "POST"
        assert captured["path"] == "/tasks"
        assert captured["body"]["projects"] == ["proj-1"]
        assert captured["body"]["memberships"] == [
            {"project": "proj-1", "section": "sec-1"}
        ]
        assert "etl_run run-9 (success)" in captured["body"]["name"]
        assert "rows: 10" in captured["body"]["notes"]
        assert captured["bypass"] is True

    def test_flush_uses_section_mode_when_both_set(self, monkeypatch):
        """If both log_section_gid AND task_gid are set, section wins."""
        from recorder import RunRecorder
        import test_suite

        captured = []
        monkeypatch.setattr(test_suite, "asana_req",
                            lambda *a, **kw: (captured.append((a, kw)) or (201, {"data": {"gid": "x"}})))

        r = RunRecorder("pat", "run-1", "x",
                        task_gid="legacy-task",
                        log_project_gid="p", log_section_gid="s")
        r.flush(status="success")

        # Should be ONE call — to /tasks (section mode), not /tasks/.../stories
        assert len(captured) == 1
        method, path = captured[0][0][0], captured[0][0][1]
        assert method == "POST"
        assert path == "/tasks"

    def test_log_action_appears_in_comment_body(self):
        from recorder import RunRecorder
        r = RunRecorder("pat", "run-1", "x", task_gid="t")
        r.log_action("step 1: read 28 pending")
        r.log_action("Sheet1!F5:K5 = ['TRUE', '1', ...] (slug=abc123)")
        body = r.format_comment(status="success", duration_s=2.0)
        assert "actions:" in body
        assert "step 1: read 28 pending" in body
        assert "abc123" in body


# ---------------------------------------------------------------------------
# check_stale_claims — 30-day stale-claim notification
# ---------------------------------------------------------------------------
class TestCheckStaleClaims:
    """The function expects an `all_rows` argument shaped like what sh_read
    returns: a list where index = row_idx - 1 (0-indexed), each row is a list
    of cell values. Helper builds rows with controlled col N/O/Z values."""

    def _row(self, slug="abc123", first_no_data="", notified="",
             task_gid="1234567890123456"):
        # Pad to 26 cols (A-Z); col E (4) = slug, col N (13) = first_no_data,
        # col O (14) = notified, col Z (25) = "'task_gid"
        row = [""] * 26
        row[4] = slug
        row[ts.COL_N_INDEX] = first_no_data
        row[ts.COL_O_INDEX] = notified
        row[ts.COL_Z_INDEX] = f"'{task_gid}" if task_gid else ""
        return row

    def _all_rows(self, *rows_at_indices):
        """rows_at_indices: list of (row_idx, row_data) — builds a sparse all_rows."""
        max_idx = max((i for i, _ in rows_at_indices), default=0)
        all_rows = [[] for _ in range(max_idx)]
        for idx, row in rows_at_indices:
            all_rows[idx - 1] = row
        return all_rows

    def test_first_sweep_with_no_data_stamps_col_n(self, mock_sheets_svc, monkeypatch):
        from test_suite import COL_N_INDEX, COL_O_INDEX, check_stale_claims, utc_today
        posted = []
        monkeypatch.setattr(ts, "asana_post_comment",
                            lambda *a, **kw: (posted.append(a) or (201, {})))

        all_rows = self._all_rows((5, self._row(slug="abc123")))
        pending = {"abc123": 5}
        by_slug = {}  # no Lightdash data

        counters = check_stale_claims(mock_sheets_svc, "sid", "pat",
                                       pending, by_slug, all_rows, log=lambda _: None)
        assert counters["stale_first_seen_stamped"] == 1
        assert counters["stale_comments_posted"] == 0
        assert posted == []  # no Asana comment on first sweep

        # Sheets write to col N with today's ISO date
        body = mock_sheets_svc.spreadsheets().values().batchUpdate.call_args.kwargs["body"]
        assert any(d["range"] == "Sheet1!N5" for d in body["data"])
        n_update = next(d for d in body["data"] if d["range"] == "Sheet1!N5")
        assert n_update["values"] == [[utc_today().isoformat()]]

    def test_inside_30_day_grace_no_action(self, mock_sheets_svc, monkeypatch):
        from test_suite import check_stale_claims, utc_today
        posted = []
        monkeypatch.setattr(ts, "asana_post_comment",
                            lambda *a, **kw: (posted.append(a) or (201, {})))

        ten_days_ago = (utc_today() - timedelta(days=10)).isoformat()
        all_rows = self._all_rows((5, self._row(first_no_data=ten_days_ago)))
        pending = {"abc123": 5}
        by_slug = {}

        counters = check_stale_claims(mock_sheets_svc, "sid", "pat",
                                       pending, by_slug, all_rows, log=lambda _: None)
        assert counters["stale_inside_grace"] == 1
        assert counters["stale_comments_posted"] == 0
        assert posted == []

    def test_at_30_days_posts_comment_and_marks_col_o(self, mock_sheets_svc, monkeypatch):
        from test_suite import check_stale_claims, utc_today
        posted = []
        def fake_post(pat, task_gid, text):
            posted.append({"pat": pat, "task_gid": task_gid, "text": text})
            return 201, {}
        monkeypatch.setattr(ts, "asana_post_comment", fake_post)

        thirty_days_ago = (utc_today() - timedelta(days=30)).isoformat()
        all_rows = self._all_rows((
            5, self._row(slug="abc124", first_no_data=thirty_days_ago,
                         task_gid="9999999999999999"),
        ))
        pending = {"abc124": 5}
        by_slug = {}

        counters = check_stale_claims(mock_sheets_svc, "sid", "test-pat",
                                       pending, by_slug, all_rows, log=lambda _: None)
        assert counters["stale_comments_posted"] == 1
        assert len(posted) == 1
        assert posted[0]["task_gid"] == "9999999999999999"
        assert "abc124" in posted[0]["text"]
        assert "30 days" in posted[0]["text"]
        assert "typo or stale ID" in posted[0]["text"]

        # Col O marked with today's date
        body = mock_sheets_svc.spreadsheets().values().batchUpdate.call_args.kwargs["body"]
        o_update = next(d for d in body["data"] if d["range"] == "Sheet1!O5")
        assert o_update["values"] == [[utc_today().isoformat()]]

    def test_already_notified_no_repost(self, mock_sheets_svc, monkeypatch):
        from test_suite import check_stale_claims, utc_today
        posted = []
        monkeypatch.setattr(ts, "asana_post_comment",
                            lambda *a, **kw: (posted.append(a) or (201, {})))

        forty_days_ago = (utc_today() - timedelta(days=40)).isoformat()
        five_days_ago = (utc_today() - timedelta(days=5)).isoformat()
        all_rows = self._all_rows((
            5, self._row(first_no_data=forty_days_ago, notified=five_days_ago),
        ))
        pending = {"abc123": 5}
        by_slug = {}

        counters = check_stale_claims(mock_sheets_svc, "sid", "pat",
                                       pending, by_slug, all_rows, log=lambda _: None)
        assert counters["stale_already_notified"] == 1
        assert counters["stale_comments_posted"] == 0
        assert posted == []

    def test_slug_with_data_skipped(self, mock_sheets_svc, monkeypatch):
        from test_suite import check_stale_claims, utc_today
        posted = []
        monkeypatch.setattr(ts, "asana_post_comment",
                            lambda *a, **kw: (posted.append(a) or (201, {})))

        all_rows = self._all_rows((5, self._row()))
        pending = {"abc123": 5}
        by_slug = {"abc123": [{ts.LD_STATUS: "rejected", ts.LD_ADJ_DAY: "",
                               ts.LD_SUBMITTED: "2026-04-01T00:00:00.000Z"}]}

        counters = check_stale_claims(mock_sheets_svc, "sid", "pat",
                                       pending, by_slug, all_rows, log=lambda _: None)
        assert counters["stale_first_seen_stamped"] == 0
        assert counters["stale_comments_posted"] == 0
        assert posted == []

    def test_failed_post_does_not_mark_col_o(self, mock_sheets_svc, monkeypatch):
        """If asana_post_comment fails, col O stays unset so the next sweep retries."""
        from test_suite import check_stale_claims, utc_today
        monkeypatch.setattr(ts, "asana_post_comment",
                            lambda *a, **kw: (500, "Server Error"))

        thirty_days_ago = (utc_today() - timedelta(days=30)).isoformat()
        all_rows = self._all_rows((5, self._row(first_no_data=thirty_days_ago)))
        pending = {"abc123": 5}
        by_slug = {}

        counters = check_stale_claims(mock_sheets_svc, "sid", "pat",
                                       pending, by_slug, all_rows, log=lambda _: None)
        assert counters["stale_comments_posted"] == 0
        # No Sheets update either, since we only mark on successful post
        bu = mock_sheets_svc.spreadsheets().values().batchUpdate
        if bu.call_args is not None:
            body = bu.call_args.kwargs.get("body", {})
            assert all(d["range"] != "Sheet1!O5" for d in body.get("data", []))


# ---------------------------------------------------------------------------
# mark_tasks_ready_to_work — Step 5 of the e2e pipeline
# ---------------------------------------------------------------------------
class TestMarkTasksReadyToWork:
    """Tests the field-update path that test_11 used to fake with prints.
    Asserts the task is NEVER closed/completed/archived — only two custom
    field updates and a confirmation comment."""

    def _row(self, slug, return_check, task_gid):
        row = [""] * 26
        row[4] = slug                                 # E: clean slug
        row[5] = return_check                         # F: Return Check
        row[ts.COL_Z_INDEX] = f"'{task_gid}"          # Z: task GID
        return row

    def _sheet_rows(self, *data_rows):
        # Pad rows 1-2 (headers) so DATA_START_ROW = 3 indexing works.
        return [["row1"], ["row2"]] + list(data_rows)

    def test_closes_task_when_all_claims_returned(self, mock_sheets_svc, env_setup, monkeypatch):
        # Mock check_all_claims_returned to short-circuit the sh_read inside it.
        monkeypatch.setattr(ts, "check_all_claims_returned",
                            lambda gid, svc, sid: (True, 3, 3))

        asana_calls = []
        def fake_asana_req(method, path, pat, body=None, **kw):
            asana_calls.append((method, path, body))
            if method == "GET":
                return 200, {"data": {"custom_fields": [
                    {"gid": "5555555555555555", "enum_value": {"gid": "9999999999999999"}},
                ]}}
            return 200, {"data": {}}

        monkeypatch.setattr(ts, "asana_req", fake_asana_req)
        # asana_update_field and asana_post_comment route through asana_req,
        # so mocking asana_req is enough.

        rows = self._sheet_rows(
            self._row("abc123", "TRUE", "1111111111111111"),
            self._row("def456", "TRUE", "1111111111111111"),
            self._row("ghi789", "TRUE", "1111111111111111"),
        )

        counters = ts.mark_tasks_ready_to_work(mock_sheets_svc, "sid", "pat",
                                             rows, log=lambda _: None)
        assert counters["marked_ready"] == 1
        assert counters["marked_tasks_seen"] == 1

        methods = [m for m, _, _ in asana_calls]
        assert methods.count("PUT") == 2  # two field updates
        assert methods.count("POST") == 1  # one comment

    def test_skips_task_with_partial_returns(self, mock_sheets_svc, env_setup, monkeypatch):
        monkeypatch.setattr(ts, "check_all_claims_returned",
                            lambda gid, svc, sid: (False, 3, 2))
        asana_calls = []
        monkeypatch.setattr(ts, "asana_req",
                            lambda *a, **kw: (asana_calls.append(a) or (200, {"data": {}})))

        rows = self._sheet_rows(
            self._row("abc123", "TRUE", "1111111111111111"),
            self._row("def456", "FALSE", "1111111111111111"),
        )
        counters = ts.mark_tasks_ready_to_work(mock_sheets_svc, "sid", "pat",
                                             rows, log=lambda _: None)
        assert counters["marked_partial"] == 1
        assert counters["marked_ready"] == 0
        assert asana_calls == []  # no API calls for partial tasks

    def test_skips_task_already_marked_yes(self, mock_sheets_svc, env_setup, monkeypatch):
        import os
        monkeypatch.setattr(ts, "check_all_claims_returned",
                            lambda gid, svc, sid: (True, 2, 2))

        # Asana GET returns enum_value.gid matching ASANA_OPT_RETURNED_YES from env_setup
        opt_yes = os.environ["ASANA_OPT_RETURNED_YES"]
        field_returned = os.environ["ASANA_FIELD_ALL_RETURNED"]

        asana_calls = []
        def fake_asana_req(method, path, pat, body=None, **kw):
            asana_calls.append((method, path))
            if method == "GET":
                return 200, {"data": {"custom_fields": [
                    {"gid": field_returned, "enum_value": {"gid": opt_yes}},
                ]}}
            return 200, {"data": {}}

        monkeypatch.setattr(ts, "asana_req", fake_asana_req)

        rows = self._sheet_rows(self._row("abc123", "TRUE", "1111111111111111"))
        counters = ts.mark_tasks_ready_to_work(mock_sheets_svc, "sid", "pat",
                                             rows, log=lambda _: None)
        assert counters["marked_already_yes"] == 1
        assert counters["marked_ready"] == 0
        # Only the GET happened — no PUT/POST after we saw already-Yes.
        methods = [m for m, _ in asana_calls]
        assert "PUT" not in methods
        assert "POST" not in methods

    def test_dry_run_skips_writes_via_global_flag(self, mock_sheets_svc, env_setup, monkeypatch):
        ts.set_dry_run(True)
        try:
            monkeypatch.setattr(ts, "check_all_claims_returned",
                                lambda gid, svc, sid: (True, 1, 1))

            once_calls = []
            def fake_once(method, path, pat, body=None):
                once_calls.append((method, path))
                if method == "GET":
                    return 200, {"data": {"custom_fields": []}}  # not yet Yes
                return 200, {"data": {}}
            monkeypatch.setattr(ts, "_asana_req_once", fake_once)

            rows = self._sheet_rows(self._row("abc123", "TRUE", "1111111111111111"))
            counters = ts.mark_tasks_ready_to_work(mock_sheets_svc, "sid", "pat",
                                                 rows, log=lambda _: None)

            # Closure was attempted (counted) but no PUT/POST reached _asana_req_once.
            assert counters["marked_ready"] == 1
            methods = [m for m, _ in once_calls]
            assert "PUT" not in methods
            assert "POST" not in methods
        finally:
            ts.set_dry_run(False)

    def test_no_task_gids_returns_zero_seen(self, mock_sheets_svc, env_setup, monkeypatch):
        monkeypatch.setattr(ts, "check_all_claims_returned", lambda *a: (False, 0, 0))
        rows = [["r1"], ["r2"]]  # no data rows
        counters = ts.mark_tasks_ready_to_work(mock_sheets_svc, "sid", "pat",
                                             rows, log=lambda _: None)
        assert counters["marked_tasks_seen"] == 0
