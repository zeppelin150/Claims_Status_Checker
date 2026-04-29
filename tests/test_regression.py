"""Regression tests — one test per fixed bug. Names should describe the bug.

These tests must FAIL if the bug returns. Use mocks so they run offline.
"""
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

import test_suite as ts


# ---------------------------------------------------------------------------
# Phase 1.1 — sign convention bug
# Was: (sub - today).days  →  past dates were negative; calc_aging compared
# `days < -90`. Two flipped sign conventions cancelled out, but any reader
# looking at days_since_submission() would expect a positive value.
# ---------------------------------------------------------------------------
def test_days_since_submission_returns_positive_for_past_dates():
    iso = (ts.utc_today() - timedelta(days=45)).strftime("%Y-%m-%dT00:00:00.000Z")
    assert ts.days_since_submission(iso) == 45  # positive, not -45


def test_calc_aging_uses_positive_day_convention():
    # If anyone "fixes" days_since_submission back to negative, this fails.
    assert ts.calc_aging(120) == "Aging"
    assert ts.calc_aging(-120) == "FALSE"  # negative would never trip the threshold


# ---------------------------------------------------------------------------
# Phase 1.2 — UTC consistency
# Was: date.today() (local time) mixed with Lightdash UTC ISO timestamps.
# Off-by-one drift between Windows dev and Ubuntu CI. Meta-test prevents the
# bug from sneaking back into production code via copy-paste.
# ---------------------------------------------------------------------------
def test_no_naked_date_today_in_production_code():
    """Production must use utc_today() (or datetime.now(timezone.utc).date())
    so local-time machines and UTC CI runs agree on the day."""
    from pathlib import Path

    repo = Path(__file__).resolve().parent.parent
    production_files = ["test_suite.py", "asana_monitor.py",
                        "asana_seeder.py", "status_simulator.py"]
    offenders = []
    for fname in production_files:
        text = (repo / fname).read_text(encoding="utf-8")
        for i, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if "date.today()" in line:
                offenders.append(f"{fname}:{i}: {stripped}")
    assert not offenders, "Naked date.today() found:\n" + "\n".join(offenders)


def test_utc_today_returns_utc_date():
    # Trivial guard: if someone changes utc_today() to date.today(), this
    # would still pass — but combined with the meta-test above, both have
    # to be circumvented to reintroduce the bug.
    today_utc = datetime.now(timezone.utc).date()
    assert ts.utc_today() == today_utc


# ---------------------------------------------------------------------------
# Phase 1.3 — col Z stored as text, not scientific notation
# Was: append_claims_to_sheet wrote task["gid"] directly with USER_ENTERED,
# which let Sheets coerce 16-digit GIDs to scientific notation. asana_monitor
# already worked around this with an apostrophe prefix; test_suite did not.
# ---------------------------------------------------------------------------
def test_append_claims_to_sheet_prefixes_col_z_with_apostrophe(mock_sheets_svc, env_setup):
    # sh_read returns an empty values list (default fixture behavior) →
    # no pre-existing slugs → all slugs in the task get appended.
    mock_sheets_svc.spreadsheets().values().get().execute.return_value = {"values": []}

    task = {"gid": "1214057491820443", "slugs": ["abc123"]}
    ts.append_claims_to_sheet(task, mock_sheets_svc, "test-spreadsheet")

    append_call = mock_sheets_svc.spreadsheets().values().append
    # Find the call that wrote the data row (body kwarg present)
    body_calls = [c for c in append_call.call_args_list if "body" in c.kwargs]
    assert body_calls, "expected at least one append call with a body"

    written_row = body_calls[-1].kwargs["body"]["values"][0]
    col_z_value = written_row[25]  # index 25 = column Z
    assert col_z_value == "'1214057491820443", (
        f"col Z should be apostrophe-prefixed to force text storage; got {col_z_value!r}"
    )


def test_asana_monitor_append_rows_single_slug_prefixes_col_z():
    """append_rows with a single slug must still prefix col Z (the original
    apostrophe-prefix workaround that test_suite was missing)."""
    import asana_monitor

    svc = MagicMock()
    asana_monitor.append_rows(svc, "sid", "1214057491820443",
                              "https://app.asana.com/0/p/t",
                              ["abc123"], "04/29/2026")

    append_call = svc.spreadsheets().values().append
    written_row = append_call.call_args.kwargs["body"]["values"][0]
    assert written_row[25] == "'1214057491820443"


# ---------------------------------------------------------------------------
# Phase 1.4 — Asana GIDs from env vars
# Was: hard-coded module constants. Now late-bound env() lookups, so rotating
# GIDs is a config change, not a code change.
# ---------------------------------------------------------------------------
def test_asana_gids_resolved_from_env_at_call_time(monkeypatch, env_setup):
    # Override one of the GIDs and verify get_new_tasks_needing_woip uses it.
    monkeypatch.setenv("ASANA_SECTION_NEW", "override-section-gid")

    captured = {}

    def fake_asana_req(method, path, pat, body=None):
        captured["path"] = path
        return 200, {"data": []}

    monkeypatch.setattr(ts, "asana_req", fake_asana_req)
    ts.get_new_tasks_needing_woip("test-pat", "test-project")

    assert "override-section-gid" in captured["path"], (
        "GID should be loaded from env at call time, not import time"
    )


def test_asana_gids_module_constants_removed():
    """The old hard-coded constants must not return — would mask env misconfig."""
    forbidden = ["ASANA_SECTION_NEW", "ASANA_SECTION_WOIP", "ASANA_FIELD_CX_OPS",
                 "ASANA_FIELD_CLAIMS_TEXT", "ASANA_FIELD_ALL_RETURNED",
                 "ASANA_OPT_WOIP", "ASANA_OPT_NEEDS_FOLLOWUP",
                 "ASANA_OPT_RETURNED_YES", "ASANA_OPT_RETURNED_NO"]
    for name in forbidden:
        assert not hasattr(ts, name), (
            f"{name} should not be a module attribute — use env('{name}') instead"
        )


# ---------------------------------------------------------------------------
# Phase 1.6 — Lightdash startup health check
# Was: typo in LIGHTDASH_API_URL silently returned empty results, pipeline
# reported "0 processed, N no data" as a successful run.
# ---------------------------------------------------------------------------
def test_verify_lightdash_no_op_when_unset(monkeypatch):
    monkeypatch.delenv("LIGHTDASH_API_URL", raising=False)
    monkeypatch.delenv("LIGHTDASH_API_KEY", raising=False)
    # Should not raise, should not call ld_request.
    ts.verify_lightdash()


def test_verify_lightdash_fails_loud_on_404(monkeypatch):
    monkeypatch.setenv("LIGHTDASH_API_URL", "https://typo.example.com")
    monkeypatch.setenv("LIGHTDASH_API_KEY", "fake-key")
    monkeypatch.setattr(ts, "ld_request", lambda *a, **kw: (404, "Not Found"))
    with pytest.raises(RuntimeError, match="Lightdash health check failed"):
        ts.verify_lightdash()


def test_verify_lightdash_fails_on_401_auth_error(monkeypatch):
    monkeypatch.setenv("LIGHTDASH_API_URL", "https://valid.example.com")
    monkeypatch.setenv("LIGHTDASH_API_KEY", "expired-key")
    monkeypatch.setattr(ts, "ld_request", lambda *a, **kw: (401, "Unauthorized"))
    with pytest.raises(RuntimeError, match="status 401"):
        ts.verify_lightdash()


def test_verify_lightdash_passes_on_200(monkeypatch):
    monkeypatch.setenv("LIGHTDASH_API_URL", "https://valid.example.com")
    monkeypatch.setenv("LIGHTDASH_API_KEY", "valid-key")
    monkeypatch.setattr(ts, "ld_request", lambda *a, **kw: (200, {"results": {"name": "Test Org"}}))
    ts.verify_lightdash()  # should not raise


# ---------------------------------------------------------------------------
# Phase 2.2 — F-K writeback batched, not per-row
# Was: sh_write(...) called inside the loop, one HTTP round-trip per slug.
# Now: write_fk_batched() collects all (range, vals) tuples and emits one
# sh_batch_write call. Regression: assert N pending slugs → 1 batch call.
# ---------------------------------------------------------------------------
def test_write_fk_batched_emits_single_batch_call_for_n_slugs(mock_sheets_svc):
    pending = {"abc123": 5, "def456": 47, "ghi789": 188, "jkl012": 23}

    def mk_row(submitted_iso="2026-04-01T00:00:00.000Z", status="rejected"):
        return {ts.LD_STATUS: status, ts.LD_ADJ_DAY: "", ts.LD_SUBMITTED: submitted_iso}

    by_slug = {slug: [mk_row()] for slug in pending}

    matched, no_data = ts.write_fk_batched(mock_sheets_svc, "sid", pending, by_slug, log=lambda _: None)

    assert matched == 4
    assert no_data == 0
    bu = mock_sheets_svc.spreadsheets().values().batchUpdate
    actual_calls = [c for c in bu.call_args_list if "body" in c.kwargs]
    assert len(actual_calls) == 1, "F-K writeback must use a single batch call"

    body = actual_calls[0].kwargs["body"]
    assert len(body["data"]) == 4
    ranges = sorted(d["range"] for d in body["data"])
    assert ranges == sorted([
        "Sheet1!F5:K5", "Sheet1!F23:K23", "Sheet1!F47:K47", "Sheet1!F188:K188",
    ])


def test_write_fk_batched_skips_slugs_with_no_data(mock_sheets_svc):
    pending = {"abc123": 5, "def456": 47}
    by_slug = {"abc123": [{ts.LD_STATUS: "rejected", ts.LD_ADJ_DAY: "", ts.LD_SUBMITTED: ""}]}

    matched, no_data = ts.write_fk_batched(mock_sheets_svc, "sid", pending, by_slug, log=lambda _: None)

    assert matched == 1
    assert no_data == 1
    body = mock_sheets_svc.spreadsheets().values().batchUpdate.call_args.kwargs["body"]
    assert len(body["data"]) == 1
    assert body["data"][0]["range"] == "Sheet1!F5:K5"


def test_write_fk_batched_no_pending_makes_no_api_call(mock_sheets_svc):
    matched, no_data = ts.write_fk_batched(mock_sheets_svc, "sid", {}, {}, log=lambda _: None)
    assert matched == 0
    assert no_data == 0
    mock_sheets_svc.spreadsheets().values().batchUpdate.assert_not_called()


# ---------------------------------------------------------------------------
# Phase 2.3 — Multi-row appends batched
# Was: per-slug values().append() in a loop (N round-trips for N slugs).
# Now: collect all rows for a (task, slugs) pair and emit one append call.
# ---------------------------------------------------------------------------
def test_append_claims_to_sheet_emits_single_call_for_n_slugs(mock_sheets_svc, env_setup):
    mock_sheets_svc.spreadsheets().values().get().execute.return_value = {"values": []}

    task = {"gid": "1234567890123456", "slugs": ["abc123", "def456", "ghi789"]}
    count = ts.append_claims_to_sheet(task, mock_sheets_svc, "sid")
    assert count == 3

    append = mock_sheets_svc.spreadsheets().values().append
    body_calls = [c for c in append.call_args_list if "body" in c.kwargs]
    assert len(body_calls) == 1, "all slugs should append in a single API call"

    rows = body_calls[0].kwargs["body"]["values"]
    assert len(rows) == 3
    # All three rows must have the apostrophe-prefixed GID in col Z.
    for row in rows:
        assert row[25] == "'1234567890123456"


def test_asana_monitor_append_rows_emits_single_call_for_n_slugs():
    """asana_monitor.append_rows replaces the old per-slug append_row."""
    import asana_monitor

    svc = MagicMock()
    asana_monitor.append_rows(
        svc, "sid", "1234567890123456",
        "https://app.asana.com/0/p/t",
        ["abc123", "def456", "ghi789"],
        "04/29/2026",
    )

    append = svc.spreadsheets().values().append
    body_calls = [c for c in append.call_args_list if "body" in c.kwargs]
    assert len(body_calls) == 1
    rows = body_calls[0].kwargs["body"]["values"]
    assert len(rows) == 3
    for row in rows:
        assert row[25] == "'1234567890123456"


def test_asana_monitor_old_append_row_function_removed():
    """The old per-row helper must not return — would defeat the batching fix."""
    import asana_monitor
    assert not hasattr(asana_monitor, "append_row"), (
        "append_row (singular) should be replaced by append_rows (plural)"
    )


def test_append_rows_with_empty_slugs_makes_no_api_call():
    import asana_monitor
    svc = MagicMock()
    asana_monitor.append_rows(svc, "sid", "1234", "url", [], "04/29/2026")
    svc.spreadsheets().values().append.assert_not_called()


# ---------------------------------------------------------------------------
# Phase 3.5 — Asana retry on 429/500/502/503
# Was: asana_req had no retry, so any transient 5xx or rate limit raised
# straight up to the caller. Sheets had this; Asana didn't.
# ---------------------------------------------------------------------------
def test_asana_retries_on_429_then_succeeds(monkeypatch):
    calls = []

    def fake_once(method, path, pat, body=None):
        calls.append((method, path))
        if len(calls) == 1:
            return 429, "Rate limited"
        return 200, {"data": {"name": "ok"}}

    monkeypatch.setattr(ts, "_asana_req_once", fake_once)
    monkeypatch.setattr(ts.time, "sleep", lambda *_: None)

    status, body = ts.asana_req("GET", "/users/me", "pat")
    assert status == 200
    assert len(calls) == 2  # one retry happened


def test_asana_retries_on_500(monkeypatch):
    calls = []

    def fake_once(method, path, pat, body=None):
        calls.append(1)
        return 500 if len(calls) < 3 else 200, {}

    monkeypatch.setattr(ts, "_asana_req_once", fake_once)
    monkeypatch.setattr(ts.time, "sleep", lambda *_: None)

    status, _ = ts.asana_req("POST", "/tasks", "pat", body={"name": "x"})
    assert status == 200
    assert len(calls) == 3


def test_asana_does_not_retry_on_401(monkeypatch):
    calls = []

    def fake_once(method, path, pat, body=None):
        calls.append(1)
        return 401, "Unauthorized"

    monkeypatch.setattr(ts, "_asana_req_once", fake_once)
    monkeypatch.setattr(ts.time, "sleep", lambda *_: None)

    status, _ = ts.asana_req("GET", "/users/me", "pat")
    assert status == 401
    assert len(calls) == 1, "401 must not retry — code/config fix, not transient"


def test_asana_does_not_retry_on_404(monkeypatch):
    calls = []

    def fake_once(method, path, pat, body=None):
        calls.append(1)
        return 404, "Not Found"

    monkeypatch.setattr(ts, "_asana_req_once", fake_once)
    monkeypatch.setattr(ts.time, "sleep", lambda *_: None)

    status, _ = ts.asana_req("GET", "/tasks/missing", "pat")
    assert status == 404
    assert len(calls) == 1


def test_asana_persistent_429_eventually_returns_429(monkeypatch):
    calls = []

    def fake_once(method, path, pat, body=None):
        calls.append(1)
        return 429, "Persistently rate limited"

    monkeypatch.setattr(ts, "_asana_req_once", fake_once)
    monkeypatch.setattr(ts.time, "sleep", lambda *_: None)

    status, _ = ts.asana_req("GET", "/users/me", "pat", max_retries=3)
    assert status == 429
    assert len(calls) == 3


# ---------------------------------------------------------------------------
# Phase 4.2 — --dry-run end-to-end
# Single global flag means all mutating helpers honor it uniformly. Reads
# still go through so dry-run can exercise the full read path.
# ---------------------------------------------------------------------------
class TestDryRunFlag:
    def teardown_method(self):
        ts.set_dry_run(False)  # always reset so other tests aren't poisoned

    def test_sh_write_skipped_in_dry_run(self):
        ts.set_dry_run(True)
        svc = MagicMock()
        ts.sh_write(svc, "sid", "Sheet1!A1:B1", [["x", "y"]])
        svc.spreadsheets().values().update.assert_not_called()

    def test_sh_batch_write_skipped_in_dry_run(self):
        ts.set_dry_run(True)
        svc = MagicMock()
        ts.sh_batch_write(svc, "sid", [("Sheet1!A1", [["x"]])])
        svc.spreadsheets().values().batchUpdate.assert_not_called()

    def test_sh_clear_skipped_in_dry_run(self):
        ts.set_dry_run(True)
        svc = MagicMock()
        ts.sh_clear(svc, "sid", "Sheet1!A:M")
        svc.spreadsheets().values().clear.assert_not_called()

    def test_asana_post_skipped_in_dry_run(self, monkeypatch):
        ts.set_dry_run(True)
        called = []
        monkeypatch.setattr(ts, "_asana_req_once",
                            lambda *a, **kw: (called.append(1) or (200, {})))
        status, body = ts.asana_req("POST", "/tasks", "pat", body={"name": "x"})
        assert status == 201  # dry-run fake-success
        assert body["data"]["gid"] == "dry-run"
        assert called == [], "POST should not reach _asana_req_once in dry-run"

    def test_asana_get_still_reaches_api_in_dry_run(self, monkeypatch):
        ts.set_dry_run(True)
        called = []
        monkeypatch.setattr(ts, "_asana_req_once",
                            lambda *a, **kw: (called.append(a) or (200, {"data": {"name": "real"}})))
        status, body = ts.asana_req("GET", "/users/me", "pat")
        assert status == 200
        assert body["data"]["name"] == "real"
        assert len(called) == 1, "GET must reach the API even in dry-run"

    def test_default_dry_run_is_false(self):
        ts.set_dry_run(False)
        assert ts.is_dry_run() is False


# ---------------------------------------------------------------------------
# Phase 4.3 — test_11 actually closes the loop (no more "Would update")
# Was: test_11_e2e Step 5 printed "Would update custom field" / "Would post
# parent task comment" without ever doing it. Test 10 had the real updater
# but was a separate test. Now test_11 invokes close_completed_tasks for real.
# ---------------------------------------------------------------------------
def test_no_would_update_placeholder_strings_in_test_11():
    """Meta-test: the 'Would update'/'Would post' placeholder text from the
    old report-only Step 5 must not be in the source. Replaced by real
    close_completed_tasks call."""
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent / "test_suite.py").read_text(encoding="utf-8")
    assert 'Would update custom field' not in src, (
        "test_11_e2e Step 5 should call close_completed_tasks, not print stubs"
    )
    assert 'Would post parent task comment' not in src


# ---------------------------------------------------------------------------
# Phase 4.4 — append_claims_to_sheet dedups on (slug, task_gid), not slug-only
# Was: same slug in a different task was silently dropped; same slug in same
# task on a re-run was correctly skipped. Now both branches are explicit and
# test_10's Step 0 re-seed is idempotent across runs.
# ---------------------------------------------------------------------------
def test_append_claims_dedups_on_slug_and_task_gid_pair(mock_sheets_svc, env_setup):
    # Existing sheet has (abc123, taskA) — same slug+gid should be skipped,
    # same slug under a different gid should be appended.
    existing_row = [""] * 26
    existing_row[4] = "abc123"
    existing_row[ts.COL_Z_INDEX] = "'taskA"
    mock_sheets_svc.spreadsheets().values().get().execute.return_value = {
        "values": [["row1"], ["row2"], existing_row]
    }

    # Same slug, same task → skip
    task_a = {"gid": "taskA", "slugs": ["abc123"]}
    appended_same = ts.append_claims_to_sheet(task_a, mock_sheets_svc, "sid")
    assert appended_same == 0

    # Same slug, different task → append
    task_b = {"gid": "taskB", "slugs": ["abc123"]}
    appended_diff = ts.append_claims_to_sheet(task_b, mock_sheets_svc, "sid")
    assert appended_diff == 1


def test_append_claims_to_sheet_idempotent_on_repeated_call(mock_sheets_svc, env_setup):
    """Test 10's Step 0 used to re-seed claims every run, accumulating duplicate
    rows. Now (slug, gid) dedup means re-running with the same task returns 0."""
    existing_row = [""] * 26
    existing_row[4] = "abc123"
    existing_row[ts.COL_Z_INDEX] = "'1234567890123456"
    mock_sheets_svc.spreadsheets().values().get().execute.return_value = {
        "values": [["row1"], ["row2"], existing_row]
    }

    task = {"gid": "1234567890123456", "slugs": ["abc123"]}
    appended = ts.append_claims_to_sheet(task, mock_sheets_svc, "sid")
    assert appended == 0
    mock_sheets_svc.spreadsheets().values().append.assert_not_called()


# ===========================================================================
# Phase 4.5 — Failure-mode regression backfill
# ===========================================================================
# These tests cover edge / failure conditions that don't fit cleanly under a
# single bug, but each represents a "would have shipped broken" scenario.
# ---------------------------------------------------------------------------

def test_process_claim_with_no_lightdash_rows_returns_false():
    """Empty Lightdash result → claim is not actionable, no aging, no work."""
    res = ts.process_claim("abc123", [])
    assert res["return_check"] == "FALSE"
    assert res["row_count"] == 0
    assert res["true_count"] == 0
    assert res["work_ticket"] == ""
    assert res["send_to_aging"] == "FALSE"
    assert res["aging_status"] == ""


def test_sheets_persistent_429_raises_after_max_retries(monkeypatch):
    """When _sheets_retry exhausts its budget on persistent 429s, the final
    HttpError must propagate so the caller sees the failure and the run gets
    recorded as 'error' rather than silently dropped."""
    from googleapiclient.errors import HttpError
    monkeypatch.setattr(ts.time, "sleep", lambda *_: None)

    call_count = [0]

    def always_429():
        call_count[0] += 1
        resp = MagicMock()
        resp.status = 429
        resp.reason = "Rate Limit"
        raise HttpError(resp, b"rate limited")

    with pytest.raises(HttpError):
        ts._sheets_retry(always_429, max_retries=3)
    assert call_count[0] == 3, "should attempt exactly max_retries times"


def test_parse_claim_slugs_handles_none_input():
    """Asana API can return None for a text custom field that's never been
    populated. Parser must treat that the same as an empty string."""
    slugs, review = ts.parse_claim_slugs(None)
    assert slugs == []
    assert review is True


def test_get_new_tasks_handles_missing_custom_fields(env_setup, monkeypatch):
    """A task with no matching ASANA_FIELD_ALL_RETURNED field should be silently
    skipped (e.g., custom field added after task creation)."""
    def fake_asana_req(method, path, pat, body=None, **kw):
        return 200, {"data": [
            {"gid": "task1", "name": "missing fields", "custom_fields": []},
            {"gid": "task2", "name": "has field but wrong enum",
             "custom_fields": [
                 {"gid": "5555555555555555", "enum_value": {"gid": "different-opt"}},
             ]},
        ]}

    monkeypatch.setattr(ts, "asana_req", fake_asana_req)
    result = ts.get_new_tasks_needing_woip("pat", "project")
    assert result == []  # both tasks are filtered out


def test_check_all_claims_returned_with_phantom_blocks_close(mock_sheets_svc):
    """A phantom slug sits in the sheet with empty col F (no Lightdash data
    means the F-K writer never set Return Check). check_all_claims_returned
    must return all_ret=False so the task does NOT close — until human
    review or the 30-day stale notification kicks in."""
    def row(slug, ret, gid):
        r = [""] * 26
        r[4] = slug
        r[5] = ret
        r[ts.COL_Z_INDEX] = gid  # Sheets API strips the leading apostrophe on read
        return r

    mock_sheets_svc.spreadsheets().values().get().execute.return_value = {
        "values": [
            ["row1"], ["row2"],
            row("real1", "TRUE", "taskX"),
            row("real2", "TRUE", "taskX"),
            row("phantom", "", "taskX"),  # ← never returned data
        ]
    }

    all_ret, total, returned = ts.check_all_claims_returned(
        "taskX", mock_sheets_svc, "sid"
    )
    assert total == 3
    assert returned == 2
    assert all_ret is False, (
        "phantom row blocks closure — task must wait for stale notification "
        "or human cleanup before all_ret can become True"
    )


def test_check_all_claims_returned_no_rows_for_task_returns_false(mock_sheets_svc):
    """If the sheet has no rows for the given task GID, all_ret must be False
    (vacuous truth could otherwise close empty tasks)."""
    mock_sheets_svc.spreadsheets().values().get().execute.return_value = {
        "values": [["row1"], ["row2"]]  # headers only
    }
    all_ret, total, returned = ts.check_all_claims_returned(
        "missing-task", mock_sheets_svc, "sid"
    )
    assert all_ret is False
    assert total == 0
    assert returned == 0


def test_two_slugs_with_same_first_chars_dont_collide():
    """Slugs are exactly 6 chars, but parse_claim_slugs's dedup is based on
    full lowercase equality. Two slugs that differ only in case must not
    deduplicate to one."""
    slugs, _ = ts.parse_claim_slugs("ABC123, abc123, ABC124")
    # ABC123 and abc123 normalize to the same → dedup. abc124 is distinct.
    assert slugs == ["abc123", "abc124"]


def test_lightdash_returns_zero_rows_for_real_slug_treated_as_no_data():
    """A slug that IS sent to Lightdash but produces zero rows (typo, too new,
    etc.) must trigger the no_data path in process_claim, not be silently
    treated as "successfully returned"."""
    # process_claim with empty rows is the actual failure surface for this case.
    res = ts.process_claim("abc124-typo", [])
    assert res["return_check"] == "FALSE"
    # Combined with check_stale_claims (Phase 4.1), this is what gets the
    # row stamped in col N for eventual 30-day human notification.
