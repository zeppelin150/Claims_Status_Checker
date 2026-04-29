"""Shared pytest fixtures for unit + regression tests.

Tests in this directory must NOT hit live APIs. Network-effecting helpers are
mocked here. Live integration tests live in test_suite.py at the repo root.
"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Make `test_suite` importable from the repo root.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture
def mock_sheets_svc():
    """A MagicMock standing in for the Sheets API service object.

    Records every call. Each .execute() defaults to returning {} — override
    in the test if a specific shape is needed:
        mock_sheets_svc.spreadsheets().values().get().execute.return_value = {"values": [...]}
    """
    svc = MagicMock(name="sheets_svc")
    # Default: every chained .execute() returns {}.
    svc.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {}
    svc.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {}
    svc.spreadsheets.return_value.values.return_value.append.return_value.execute.return_value = {}
    svc.spreadsheets.return_value.values.return_value.batchUpdate.return_value.execute.return_value = {}
    svc.spreadsheets.return_value.values.return_value.clear.return_value.execute.return_value = {}
    svc.spreadsheets.return_value.get.return_value.execute.return_value = {"sheets": []}
    svc.spreadsheets.return_value.batchUpdate.return_value.execute.return_value = {}
    return svc


@pytest.fixture
def fake_asana_req(monkeypatch):
    """Replace test_suite.asana_req with a recorder.

    Tests can set fake_asana_req.responses to a dict keyed by (method, path_prefix)
    for canned responses, or fake_asana_req.default to override the default 200/{}.
    All calls are recorded in fake_asana_req.calls as (method, path, body) tuples.
    """
    import test_suite

    class Recorder:
        def __init__(self):
            self.calls = []
            self.responses = {}
            self.default = (200, {"data": {}})

        def __call__(self, method, path, pat, body=None):
            self.calls.append((method, path, body))
            for (m, prefix), resp in self.responses.items():
                if method == m and path.startswith(prefix):
                    return resp
            return self.default

    rec = Recorder()
    monkeypatch.setattr(test_suite, "asana_req", rec)
    return rec


@pytest.fixture
def env_setup(monkeypatch, tmp_path):
    """Set up minimal env vars so env() calls don't blow up.

    Tests can override individual vars via monkeypatch.setenv().
    """
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "test-spreadsheet")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_JSON", json.dumps({
        "type": "service_account", "project_id": "test",
        "private_key": "-----BEGIN-----\ntest\n-----END-----\n",
        "client_email": "test@test.iam.gserviceaccount.com",
    }))
    monkeypatch.setenv("ASANA_PAT", "test-pat")
    monkeypatch.setenv("ASANA_PROJECT_GID", "test-project-gid")
    # Asana GIDs (Phase 1.4 makes these env-driven)
    monkeypatch.setenv("ASANA_SECTION_NEW", "1111111111111111")
    monkeypatch.setenv("ASANA_SECTION_WOIP", "2222222222222222")
    monkeypatch.setenv("ASANA_FIELD_CX_OPS", "3333333333333333")
    monkeypatch.setenv("ASANA_FIELD_CLAIMS_TEXT", "4444444444444444")
    monkeypatch.setenv("ASANA_FIELD_ALL_RETURNED", "5555555555555555")
    monkeypatch.setenv("ASANA_OPT_WOIP", "6666666666666666")
    monkeypatch.setenv("ASANA_OPT_NEEDS_FOLLOWUP", "7777777777777777")
    monkeypatch.setenv("ASANA_OPT_RETURNED_YES", "8888888888888888")
    monkeypatch.setenv("ASANA_OPT_RETURNED_NO", "9999999999999999")
    return monkeypatch
