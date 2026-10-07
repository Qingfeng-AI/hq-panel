"""Offline source-contract and fail-closed CQI regression tests."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd


SPEC = importlib.util.spec_from_file_location("cqi_under_test", Path(__file__).parents[1] / "cqi.py")
cqi = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cqi)


class CQITest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.addCleanup(self.tmp.cleanup)
        for name, value in {"RAW": self.root / "data/raw", "OUT": self.root / "data",
                            "REP": self.root / "reports", "END": "2024-06-10"}.items():
            self.stack.enter_context(patch.object(cqi, name, value))
        self.stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        self.stack.enter_context(contextlib.redirect_stderr(io.StringIO()))

    def manifest(self):
        result = {"status": "ok", "completed_at": cqi.utc_now(), "sources": {
            k: {"status": "ok", "latest_observation": "2024-06-10"} for k in cqi.FETCHERS}}
        cqi.write_json(cqi.RAW / "fetch_status.json", result)
        return result

    @staticmethod
    def output(values=(0.25,)):
        idx = pd.bdate_range(end="2024-06-10", periods=len(values))
        return pd.DataFrame({"cqi": values, "成分数": 3, "灯": "", "data_status": "ok"}, index=idx)

    def test_numeric_commas_are_parsed(self):
        values = cqi.num(pd.Series(["1,234.5", "."]))
        self.assertEqual(values.iloc[0], 1234.5)
        self.assertTrue(pd.isna(values.iloc[1]))

    def test_empty_sofr_fails_without_publishing(self):
        with patch.object(cqi, "get", return_value={"refRates": []}):
            with self.assertRaisesRegex(cqi.PipelineError, "empty"):
                cqi.fetch_sofr()
        self.assertFalse((cqi.RAW / "sofr.csv").exists())

    def test_sofr_schema_change_fails(self):
        with patch.object(cqi, "get", return_value={"refRates": [{"effectiveDate": "2024-06-10", "rate": 5}]}):
            with self.assertRaisesRegex(cqi.PipelineError, "percentRate"):
                cqi.fetch_sofr()

    def test_fred_columns_are_validated_not_positional(self):
        with patch.object(cqi, "get", return_value=b"unexpected,IOER\n2024-06-10,5.0\n"):
            with self.assertRaisesRegex(cqi.PipelineError, "observation date"):
                cqi.fetch_admin_rate()

    def test_buyback_known_schema_aliases_are_normalized(self):
        data = pd.DataFrame({"operation_date": ["2024-06-07"], "total_par_amt_offered": ["3,000"],
                             "total_par_amt_accepted": ["1,000"], "security_type": ["Nominal Coupon"]})
        result, latest = cqi.validate_source("buybacks", data)
        self.assertEqual(latest, "2024-06-07")
        self.assertEqual(result["total_offered"].iloc[0], 3000)
        self.assertEqual(result["total_accepted"].iloc[0], 1000)

    def test_buyback_incomplete_pagination_fails(self):
        payload = {"data": [{"operation_date": "2024-06-07", "total_offered": 300, "total_accepted": 100}],
                   "meta": {"total-count": 2}, "links": {"next": None}}
        with patch.object(cqi, "get", return_value=payload):
            with self.assertRaisesRegex(cqi.PipelineError, "incomplete pagination"):
                cqi.fetch_buybacks()

    def test_cftc_archive_failure_is_not_silently_skipped(self):
        with patch.object(cqi, "get", side_effect=RuntimeError("network unavailable")):
            with self.assertRaisesRegex(cqi.PipelineError, "CFTC 2016: network unavailable"):
                cqi.fetch_cftc()

    def test_stale_source_fails_even_with_many_rows(self):
        data = pd.DataFrame({"date": pd.bdate_range("2020-01-01", "2023-12-29"), "sofr": 5.0})
        with self.assertRaisesRegex(cqi.PipelineError, "stale"):
            cqi.validate_source("sofr", data)

    def test_conflicting_duplicate_dates_fail(self):
        data = pd.DataFrame({"date": ["2024-06-10"] * 2, "sofr": [5.0, 4.0]})
        with self.assertRaisesRegex(cqi.PipelineError, "conflicting duplicate"):
            cqi.validate_source("sofr", data)

    def test_missing_repo_amount_is_not_zero(self):
        data = pd.DataFrame({"operationDate": ["2024-06-10"], "operationType": ["Repo"], "totalAmtAccepted": [None]})
        with self.assertRaisesRegex(cqi.PipelineError, "missing values are not zero"):
            cqi.validate_source("repo_ops", data)

    def test_bad_refresh_preserves_raw_file_and_reports_failure(self):
        target = cqi.RAW / "sofr.csv"
        target.parent.mkdir(parents=True)
        target.write_text("previous good file")
        with self.assertRaises(cqi.PipelineError):
            cqi.save_source("sofr", pd.DataFrame())
        self.assertEqual(target.read_text(), "previous good file")

    def test_fetch_failure_is_nonzero_and_invalidates_old_success(self):
        cqi.write_json(cqi.OUT / "build_status.json", {"status": "ok"})
        fetchers = {"sofr": lambda: (_ for _ in ()).throw(RuntimeError("HTTP 503"))}
        with patch.object(cqi, "FETCHERS", fetchers):
            self.assertEqual(cqi.main(["fetch"]), 1)
        status = json.loads((cqi.RAW / "fetch_status.json").read_text())
        self.assertEqual(status["status"], "failed")
        self.assertIn("HTTP 503", status["sources"]["sofr"]["error"])
        self.assertEqual(json.loads((cqi.OUT / "build_status.json").read_text())["status"], "failed")

    def test_all_stops_after_failed_fetch(self):
        with patch.object(cqi, "cmd_fetch", side_effect=cqi.PipelineError("broken")), patch.object(cqi, "cmd_build") as build:
            self.assertEqual(cqi.main(["all"]), 1)
            build.assert_not_called()

    def test_missing_fetch_manifest_fails_build(self):
        self.assertEqual(cqi.main(["build"]), 1)
        self.assertFalse((cqi.OUT / "cqi_daily.csv").exists())
        self.assertEqual(json.loads((cqi.OUT / "build_status.json").read_text())["status"], "failed")

    def test_failed_fetch_manifest_cannot_reuse_old_sources(self):
        status = self.manifest()
        status["status"] = "failed"
        cqi.write_json(cqi.RAW / "fetch_status.json", status)
        with self.assertRaisesRegex(cqi.PipelineError, "did not succeed"):
            cqi.check_fetch_status()

    def test_stale_fetch_manifest_cannot_appear_healthy(self):
        status = self.manifest()
        status["completed_at"] = "2020-01-01T00:00:00+00:00"
        cqi.write_json(cqi.RAW / "fetch_status.json", status)
        with self.assertRaisesRegex(cqi.PipelineError, "stale"):
            cqi.check_fetch_status()

    def test_no_usable_build_is_nonzero(self):
        self.manifest()
        with patch.object(cqi, "build", return_value=self.output([np.nan])):
            self.assertEqual(cqi.main(["build"]), 1)
        self.assertFalse((cqi.OUT / "cqi_daily.csv").exists())

    def test_old_usable_reading_does_not_mask_missing_current_reading(self):
        self.manifest()
        with patch.object(cqi, "build", return_value=self.output([0.2, np.nan])):
            self.assertEqual(cqi.main(["build"]), 1)
        self.assertFalse((cqi.OUT / "cqi_daily.csv").exists())

    def test_successful_build_writes_real_kind_and_status(self):
        self.manifest()
        with patch.object(cqi, "build", return_value=self.output()):
            self.assertEqual(cqi.main(["build"]), 0)
        output = pd.read_csv(cqi.OUT / "cqi_daily.csv")
        self.assertEqual(output["data_kind"].iloc[-1], "real")
        status = json.loads((cqi.OUT / "build_status.json").read_text())
        self.assertEqual(status["status"], "ok")
        self.assertEqual(status["latest_cqi_date"], "2024-06-10")

    def test_missing_cqi_has_explicit_unknown_status(self):
        with patch.object(cqi, "START", "2024-06-03"):
            out = cqi.build({"only_one": pd.Series(0.0, index=pd.bdate_range("2024-06-03", "2024-06-10"))})
        self.assertTrue(out["cqi"].isna().all())
        self.assertTrue(out["灯"].eq("数据缺失").all())
        self.assertTrue(out["data_status"].eq("insufficient_components").all())

    def test_repo_usage_outside_observation_span_is_unknown(self):
        data = pd.DataFrame({"operationDate": ["2024-06-04", "2024-06-06"], "operationType": ["Repo"] * 2,
                             "totalAmtAccepted": [1e9, 2e9]})
        with patch.object(cqi, "read", return_value=data):
            result = cqi.comp_srp_usage(pd.bdate_range("2024-06-03", "2024-06-10"))
        self.assertTrue(pd.isna(result.loc["2024-06-03"]))
        self.assertTrue(pd.isna(result.loc["2024-06-10"]))
        self.assertTrue(np.isfinite(result.loc["2024-06-05"]))

    def test_repo_rates_do_not_forward_fill_forever(self):
        sofr = pd.DataFrame({"date": ["2024-05-20"], "sofr": [5.1]})
        admin = pd.DataFrame({"date": ["2021-07-28", "2024-05-20"], "rate": [0.1, 5.0], "series": ["IOER", "IORB"]})
        with patch.object(cqi, "read", side_effect=[sofr, admin]):
            result = cqi.comp_repo_pressure(pd.bdate_range("2024-05-20", "2024-06-10"))
        self.assertAlmostEqual(result.iloc[0], 10)
        self.assertTrue(pd.isna(result.iloc[-1]))

    def test_demo_never_overwrites_production(self):
        cqi.OUT.mkdir(parents=True)
        target = cqi.OUT / "cqi_daily.csv"
        target.write_text("production sentinel")
        with patch.object(cqi, "build", return_value=self.output()), patch.object(cqi, "backtest", return_value=("demo", "未通过")):
            self.assertEqual(cqi.main(["demo"]), 0)
        self.assertEqual(target.read_text(), "production sentinel")
        demo = pd.read_csv(cqi.OUT / "demo/cqi_daily.csv")
        self.assertEqual(demo["data_kind"].iloc[0], "synthetic")


if __name__ == "__main__":
    unittest.main()
