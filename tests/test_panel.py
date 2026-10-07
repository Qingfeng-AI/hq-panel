import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import pandas as pd
import panel

class PanelTests(unittest.TestCase):
    def test_missing_input_is_error(self):
        with tempfile.TemporaryDirectory() as root, patch.object(panel, "RAW", Path(root)):
            with self.assertRaisesRegex(ValueError, "缺失"):
                panel.load_fred()

    def test_failed_fetch_is_error(self):
        with tempfile.TemporaryDirectory() as root, patch.object(panel, "RAW", Path(root)), patch.object(panel, "SERIES", {"y10": "DGS10"}), patch.object(panel, "get_csv", side_effect=RuntimeError("offline")):
            with self.assertRaises(RuntimeError):
                panel.cmd_fetch(None)

    def test_stale_input_is_error(self):
        with tempfile.TemporaryDirectory() as root, patch.object(panel, "RAW", Path(root)), patch.object(panel, "SERIES", {"y10": "DGS10"}):
            pd.DataFrame({"date": ["2020-01-01"], "y10": [2]}).to_csv(Path(root) / "y10.csv", index=False)
            with self.assertRaisesRegex(ValueError, "过期"):
                panel.load_fred()

    def test_observation_date_not_forward_fill_date(self):
        with tempfile.TemporaryDirectory() as root, patch.object(panel, "RAW", Path(root)), patch.object(panel, "OUT", Path(root)), patch.object(panel, "MAN", Path(root)), patch.object(panel, "SERIES", {"y10": "DGS10"}), patch.object(panel, "END", "2026-10-07"):
            pd.DataFrame({"date": ["2026-10-05"], "y10": [4.2]}).to_csv(Path(root) / "y10.csv", index=False)
            result = panel.report(panel.build(panel.load_fred()))
            self.assertIn("4.20% | 2026-10-05", result)
            self.assertIn("| 判读 | 待接入 |", result)

    def test_empty_signals_are_not_healthy(self):
        with tempfile.TemporaryDirectory() as root, patch.object(panel, "MAN", Path(root)):
            result = panel.report(pd.DataFrame(index=pd.bdate_range("2026-01-01", periods=2)))
            self.assertNotIn("未亮", result)
            self.assertNotIn("载体倒退", result)

    def test_synthetic_cqi_rejected(self):
        with tempfile.TemporaryDirectory() as root, patch.object(panel, "OUT", Path(root)):
            pd.DataFrame({"date": ["2026-10-06"], "cqi": [1], "灯": ["黄"], "data_kind": ["synthetic"]}).set_index("date").to_csv(Path(root) / "cqi_daily.csv")
            (Path(root) / "build_status.json").write_text(json.dumps({"status": "ok"}))
            with self.assertRaisesRegex(ValueError, "合成"):
                panel.build(pd.DataFrame(index=pd.bdate_range("2026-10-01", periods=5)))

    def test_cqi_lamp_matches_latest_reading_not_old_alert(self):
        with tempfile.TemporaryDirectory() as root, patch.object(panel, "MAN", Path(root)):
            frame = pd.DataFrame({"cqi": [2.5, 0.1], "cqi_lamp": ["红", float("nan")]}, index=pd.to_datetime(["2026-10-05", "2026-10-06"]))
            self.assertIn("0.10 | 2026-10-06 | —", panel.report(frame))

    def test_fred_fetch_requests_only_model_date_range(self):
        with tempfile.TemporaryDirectory() as root, patch.object(panel, "RAW", Path(root)), patch.object(panel, "SERIES", {"y10": "DGS10"}), patch.object(panel, "get_csv", return_value=pd.DataFrame({"observation_date": ["2026-10-05"], "DGS10": [4.2]})) as get:
            panel.cmd_fetch(None)
            self.assertIn("&cosd=" + panel.START + "&coed=" + panel.END, get.call_args.args[0])
