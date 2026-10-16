"""Tests for detect_from_sheet.process_sheet — a failed DB write must leave the sheet row pending."""
from unittest.mock import MagicMock, patch

import detect_from_sheet as dfs

_HEADER = ["Company", "Domain", "Job URL", "ATS Platform", "ATS Slug", "Status", "Notes"]


def _run(db_ok):
    ws = MagicMock()
    ws.get_all_values.return_value = [list(_HEADER), ["Acme", "acme.com", "https://x.test/job/1", "", "", "pending", ""]]
    with patch.object(dfs, "get_sheet", return_value=ws), \
         patch("jobs.ats.patterns.match_ats_pattern", return_value={"platform": "greenhouse", "slug": "acme"}), \
         patch.object(dfs, "_apply_to_db", return_value=db_ok):
        dfs.process_sheet(apply_db=True)
    return ws


def test_row_marked_auto_when_db_write_succeeds():
    ws = _run(True)
    batch = ws.batch_update.call_args[0][0]
    assert batch[0]["values"][0][2] == "auto"


def test_row_left_pending_when_db_write_fails():
    ws = _run(False)
    ws.batch_update.assert_not_called()


def test_apply_to_db_returns_false_on_skip_and_error():
    with patch("jobs.ats_detector.override_ats", return_value=False):
        assert dfs._apply_to_db("Acme", "greenhouse", "acme") is False
    with patch("jobs.ats_detector.override_ats", side_effect=RuntimeError("x")):
        assert dfs._apply_to_db("Acme", "greenhouse", "acme") is False
    with patch("jobs.ats_detector.override_ats", return_value=True):
        assert dfs._apply_to_db("Acme", "greenhouse", "acme") is True
