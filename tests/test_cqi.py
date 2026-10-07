"""Offline source-contract and fail-closed CQI regression tests."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

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
        self.stack.enter_context(patch.object(cqi, "require_api_key"))
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
        with patch.object(cqi, "fetch_series", return_value=pd.DataFrame({"unexpected": ["2024-06-10"], "value": [5.0]})):
            with self.assertRaisesRegex(cqi.PipelineError, "missing required columns"):
                cqi.fetch_admin_rate()

    def test_fred_requests_only_needed_dates_and_caps_discontinued_ioer(self):
        responses = [pd.DataFrame({"date": ["2021-07-28"], "value": [0.15]}),
                     pd.DataFrame({"date": ["2024-06-10"], "value": [5.4]})]
        with patch.object(cqi, "fetch_series", side_effect=responses) as get:
            result = cqi.fetch_admin_rate()
        self.assertEqual(set(result["series"]), {"IOER", "IORB"})
        for call, sid, end in zip(get.call_args_list, ("IOER", "IORB"), ("2021-07-28", cqi.END)):
            self.assertEqual(call.args, (sid, cqi.START, end))

    def test_trust_fetch_validates_named_columns_and_bounded_dates(self):
        responses = [pd.DataFrame({"value": [4.4], "date": ["2024-06-10"]}),
                     pd.DataFrame({"date": ["2024-06-07"], "value": [121.5]})]
        with patch.object(cqi, "fetch_series", side_effect=responses) as get:
            result = cqi.fetch_trust()
        self.assertEqual(set(result["series"]), {"DGS10", "DTWEXBGS"})
        self.assertTrue((cqi.RAW / "trust.csv").exists())
        for call, sid in zip(get.call_args_list, ("DGS10", "DTWEXBGS")):
            self.assertEqual(call.args, (sid, cqi.START, cqi.END))

    def test_fred_failures_identify_the_exact_series_without_raw_exception(self):
        old = pd.DataFrame({"date": ["2021-07-28"], "value": [0.15]})
        current = pd.DataFrame({"date": ["2024-06-10"], "value": [4.4]})
        scenarios = [(cqi.fetch_admin_rate, "IOER", [RuntimeError("private details")]),
                     (cqi.fetch_admin_rate, "IORB", [old, RuntimeError("private details")]),
                     (cqi.fetch_trust, "DGS10", [RuntimeError("private details")]),
                     (cqi.fetch_trust, "DTWEXBGS", [current, RuntimeError("private details")])]
        for fetch, sid, responses in scenarios:
            with self.subTest(series=sid), patch.object(cqi, "fetch_series", side_effect=responses):
                with self.assertRaisesRegex(cqi.PipelineError, sid + ": FRED API request failed") as error:
                    fetch()
                self.assertNotIn("private details", str(error.exception))

    def test_trust_requires_both_series_and_checks_each_freshness(self):
        for stale_sid in ("DGS10", "DTWEXBGS"):
            rows = [{"date": "2024-05-01" if sid == stale_sid else "2024-06-10",
                     "value": 4.5 if sid == "DGS10" else 120, "series": sid}
                    for sid in ("DGS10", "DTWEXBGS")]
            with self.subTest(series=stale_sid), self.assertRaisesRegex(cqi.PipelineError, stale_sid + " stale"):
                cqi.validate_source("trust", pd.DataFrame(rows))
        for sid in ("DGS10", "DTWEXBGS"):
            with self.subTest(missing=sid), self.assertRaisesRegex(cqi.PipelineError, "both DGS10 and DTWEXBGS"):
                cqi.validate_source("trust", pd.DataFrame({"date": ["2024-06-10"], "value": [4.5], "series": [sid]}))

    def test_trust_conflicting_duplicates_fail_and_exact_duplicates_deduplicate(self):
        rows = [{"date": "2024-06-10", "value": 4.5, "series": "DGS10"},
                {"date": "2024-06-07", "value": 120, "series": "DTWEXBGS"}]
        result, latest = cqi.validate_source("trust", pd.DataFrame(rows + [rows[0]]))
        self.assertEqual(len(result), 2)
        self.assertEqual(latest, "2024-06-07")
        with self.assertRaisesRegex(cqi.PipelineError, "conflicting duplicate"):
            cqi.validate_source("trust", pd.DataFrame(rows + [dict(rows[0], value=4.6)]))

    def test_trust_divergence_uses_exact_five_business_day_rule(self):
        bdays = pd.bdate_range("2024-06-03", periods=6)
        for dy, dollar_change, expected in ((0.05, -5, 5), (0.05, 5, 0), (-0.05, -5, 0),
                                            (0, -5, 0), (0.05, 0, 0)):
            rows = [{"date": day, "value": 4.0 + dy * i / 5, "series": "DGS10"}
                    for i, day in enumerate(bdays)]
            rows += [{"date": day, "value": 100 + dollar_change * i / 5, "series": "DTWEXBGS"}
                     for i, day in enumerate(bdays)]
            with self.subTest(dy=dy, dollar_change=dollar_change), patch.object(cqi, "read", return_value=pd.DataFrame(rows[::-1])):
                result = cqi.comp_trust_divergence(bdays)
            self.assertTrue(result.iloc[:5].isna().all())
            self.assertAlmostEqual(result.iloc[-1], expected)

    def test_trust_fill_is_chronological_and_limited(self):
        bdays = pd.bdate_range("2024-05-01", periods=20)
        rows = [{"date": day, "value": 4.0 + i / 100, "series": "DGS10"}
                for i, day in enumerate(bdays[:6])]
        rows += [{"date": day, "value": 100 - i, "series": "DTWEXBGS"}
                 for i, day in enumerate(bdays[:6])]
        with patch.object(cqi, "read", return_value=pd.DataFrame(rows).sample(frac=1, random_state=7)):
            result = cqi.comp_trust_divergence(bdays)
        self.assertAlmostEqual(result.iloc[5], 5)
        self.assertAlmostEqual(result.iloc[6], 4)
        self.assertTrue(result.iloc[11:].isna().all())

    @staticmethod
    def source_frames():
        return {
            "sofr": pd.DataFrame({"date": ["2024-06-10"], "sofr": [5.3]}),
            "admin_rate": pd.DataFrame({"date": ["2021-07-28", "2024-06-10"], "rate": [0.15, 5.4], "series": ["IOER", "IORB"]}),
            "trust": pd.DataFrame({"date": ["2024-06-10", "2024-06-07"], "value": [4.4, 120], "series": ["DGS10", "DTWEXBGS"]}),
            "repo_ops": pd.DataFrame({"operationDate": ["2024-06-10"], "operationType": ["Repo"], "totalAmtAccepted": [1e9]}),
            "auctions": pd.DataFrame({"auctionDate": ["2024-06-10"], "securityType": ["Note"], "securityTerm": ["10-Year"],
                                      "bidToCoverRatio": [2.5], "primaryDealerAccepted": [100], "competitiveAccepted": [1000]}),
            "buybacks": pd.DataFrame({"operation_date": ["2024-06-10"], "total_offered": [300], "total_accepted": [100]}),
            "cftc": pd.DataFrame({"market": ["UST 10Y NOTE"], "date": ["2024-06-04"], "lev_short": [100], "lev_long": [50]}),
        }

    def test_every_optional_fetch_failure_is_explicitly_degraded(self):
        def fail():
            raise RuntimeError("HTTP 503")
        for source in cqi.OPTIONAL_SOURCES:
            fetchers = {k: (lambda frame=frame: frame) for k, frame in self.source_frames().items()}
            fetchers[source] = fail
            with self.subTest(source=source), patch.object(cqi, "FETCHERS", fetchers), patch.object(cqi, "annotate") as annotations:
                self.assertEqual(cqi.main(["fetch"]), 0)
                status = cqi.check_fetch_status()
            self.assertEqual(status["status"], "degraded")
            self.assertEqual(status["sources"][source]["status"], "failed")
            self.assertEqual(status["sources"][source]["tier"], "optional")
            self.assertIn("HTTP 503", status["sources"][source]["error"])
            self.assertTrue(any(call.args[0] == "warning" and source in call.args[1] for call in annotations.call_args_list))
            self.assertEqual(status["sources"]["trust"]["latest_observations"], {"DGS10": "2024-06-10", "DTWEXBGS": "2024-06-07"})

    def test_every_core_fetch_failure_blocks_publication(self):
        def fail():
            raise RuntimeError("core unavailable")
        target = cqi.OUT / "cqi_daily.csv"
        target.parent.mkdir(parents=True)
        target.write_text("previous verified reading")
        for source in cqi.CORE_SOURCES:
            fetchers = {k: (lambda frame=frame: frame) for k, frame in self.source_frames().items()}
            fetchers[source] = fail
            with self.subTest(source=source), patch.object(cqi, "FETCHERS", fetchers), patch.object(cqi, "cmd_build") as build:
                self.assertEqual(cqi.main(["all"]), 1)
                build.assert_not_called()
            self.assertEqual(json.loads((cqi.RAW / "fetch_status.json").read_text())["status"], "failed")
            self.assertEqual(json.loads((cqi.OUT / "build_status.json").read_text())["status"], "failed")
            self.assertEqual(target.read_text(), "previous verified reading")

    def test_inconsistent_or_incomplete_degraded_manifest_is_rejected(self):
        for change in ("core_failed", "missing_optional", "invalid_optional", "hidden_failure", "false_degradation", "no_error", "invalid_time"):
            status = self.manifest()
            if change == "core_failed":
                status["status"] = "degraded"
                status["sources"]["trust"]["status"] = "failed"
            elif change == "missing_optional":
                del status["sources"]["repo_ops"]
            elif change == "invalid_optional":
                status["sources"]["repo_ops"]["status"] = "running"
            elif change == "hidden_failure":
                status["sources"]["repo_ops"] = {"status": "failed", "error": "network"}
            elif change == "false_degradation":
                status["status"] = "degraded"
            elif change == "no_error":
                status["status"] = "degraded"
                status["sources"]["repo_ops"] = {"status": "failed"}
            else:
                status["completed_at"] = "NaT"
            cqi.write_json(cqi.RAW / "fetch_status.json", status)
            with self.subTest(change=change), self.assertRaises(cqi.PipelineError):
                cqi.check_fetch_status()

    def test_failed_optional_component_is_never_read_from_old_files(self):
        funcs = {k: (lambda bdays: pd.Series(np.arange(len(bdays)), index=bdays)) for k in cqi.COMPONENTS}
        with patch.object(cqi, "COMPONENTS", funcs), patch.dict(funcs, {"央行回购使用量": lambda bdays: self.fail("failed source read")}), \
                patch.object(cqi, "rolling_z", new=lambda series: series):
            result = cqi.build(ok_sources=set(cqi.FETCHERS) - {"repo_ops"})
        self.assertNotIn("raw_央行回购使用量", result)
        self.assertNotIn("z_央行回购使用量", result)
        self.assertEqual(result["成分数"].iloc[-1], 5)

    def test_optional_build_validation_failure_degrades_but_core_failure_stops(self):
        def fail(_):
            raise cqi.PipelineError("stale local input")
        for component in ("央行回购使用量", "信任背离", "回购压力"):
            funcs = {k: (lambda bdays: pd.Series(np.arange(len(bdays)), index=bdays)) for k in cqi.COMPONENTS}
            funcs[component] = fail
            with self.subTest(component=component), patch.object(cqi, "COMPONENTS", funcs), \
                    patch.object(cqi, "rolling_z", new=lambda series: series):
                if component != "央行回购使用量":
                    with self.assertRaisesRegex(cqi.PipelineError, "stale local input"):
                        cqi.build(ok_sources=set(cqi.FETCHERS))
                else:
                    self.manifest()
                    out = cqi.cmd_build(None)
                    status = json.loads((cqi.OUT / "build_status.json").read_text())
                    self.assertEqual(status["status"], "degraded")
                    self.assertEqual(status["sources"]["repo_ops"]["status"], "failed")
                    self.assertEqual(status["sources"]["repo_ops"]["stage"], "build")
                    self.assertNotIn("raw_央行回购使用量", out)

    def test_degraded_build_discloses_status_in_manifest_and_standalone_csv(self):
        status = self.manifest()
        status["status"] = "degraded"
        status["sources"]["cftc"] = {"status": "failed", "error": "HTTP 503", "tier": "optional"}
        cqi.write_json(cqi.RAW / "fetch_status.json", status)
        with patch.object(cqi, "build", return_value=self.output()) as build:
            result = cqi.cmd_build(None)
        self.assertNotIn("cftc", build.call_args.kwargs["ok_sources"])
        status = json.loads((cqi.OUT / "build_status.json").read_text())
        self.assertEqual(status["status"], "degraded")
        self.assertEqual(status["missing_components"], ["基差平仓速度"])
        self.assertEqual(result["data_status"].iloc[-1], "degraded")
        output = pd.read_csv(cqi.OUT / "cqi_daily.csv")
        self.assertEqual(output["run_status"].iloc[-1], "degraded")
        self.assertEqual(output["missing_components"].iloc[-1], "基差平仓速度")
        self.assertEqual(output["data_kind"].iloc[-1], "real")

    def test_degraded_build_still_requires_three_components(self):
        funcs = {k: (lambda bdays: pd.Series(np.arange(len(bdays)), index=bdays)) for k in cqi.COMPONENTS}
        status = self.manifest()
        status["status"] = "degraded"
        for source in cqi.OPTIONAL_SOURCES:
            status["sources"][source] = {"status": "failed", "error": "offline"}
        cqi.write_json(cqi.RAW / "fetch_status.json", status)
        with patch.object(cqi, "COMPONENTS", funcs), patch.object(cqi, "rolling_z", new=lambda series: series):
            self.assertEqual(cqi.main(["build"]), 1)
        self.assertFalse((cqi.OUT / "cqi_daily.csv").exists())

    def test_degraded_backtest_never_claims_formal_pass(self):
        out = self.output([2.0])
        with patch.object(cqi, "EVENTS", {"event": "2024-06-10"}), patch.object(cqi, "MUST_FLAG", ["event"]), patch.object(cqi, "CALM", {}):
            normal_md, normal_verdict = cqi.backtest(out)
            md, verdict = cqi.backtest(out, missing_components=["基差平仓速度"], degraded=True)
        self.assertEqual(normal_verdict, "通过")
        self.assertIn("可提交卷六关口", normal_md)
        self.assertNotEqual(verdict, "通过")
        self.assertTrue(md.startswith("> **降级运行**"))
        self.assertIn("基差平仓速度", md)
        self.assertIn("不能作为第二阶段关口的正式依据", md)
        self.assertNotIn("可提交卷六关口", md)

    def test_backtest_command_propagates_degraded_state(self):
        out = self.output()
        out["data_kind"] = "real"
        out["data_status"] = "degraded"
        cqi.write_csv(cqi.OUT / "cqi_daily.csv", out, index_label="date")
        cqi.write_json(cqi.OUT / "build_status.json", {"status": "degraded", "missing_components": ["央行回购使用量"]})
        with patch.object(cqi, "backtest", return_value=("degraded report", "不作正式判定")) as backtest, \
                patch.dict("sys.modules", {"matplotlib": None}):
            self.assertEqual(cqi.main(["backtest"]), 1)  # Plot failure must remain an error.
        self.assertEqual(backtest.call_args.kwargs, {"missing_components": ["央行回购使用量"], "degraded": True})

    def test_annotations_escape_untrusted_message_text(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            cqi.annotate("warning", "source,invalid:title\nnext", "HTTP 50%\n::error::injected")
        self.assertEqual(len(output.getvalue().splitlines()), 1)
        self.assertIn("source%2Cinvalid%3Atitle%0Anext", output.getvalue())
        self.assertIn("50%25", output.getvalue())

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

    def test_buyback_ampersand_pagination_replaces_query(self):
        from urllib.parse import parse_qs, urlparse
        row = {"operation_date": "2024-06-07", "total_offered": 300, "total_accepted": 100}
        pages = [{"data": [row], "links": {"next": "&page%5Bnumber%5D=2&page%5Bsize%5D=1"}},
                 {"data": [dict(row, operation_date="2024-06-10")], "links": {"next": None},
                  "meta": {"total-count": 2}}]
        with patch.object(cqi, "get", side_effect=pages) as get:
            result = cqi.fetch_buybacks()
        self.assertEqual(len(result), 2)
        second_url = get.call_args_list[1].args[0]
        parsed = urlparse(second_url)
        self.assertEqual(parsed.path, urlparse(cqi.URL["buybacks"]).path)
        self.assertEqual(parse_qs(parsed.query)["page[number]"], ["2"])
        self.assertEqual(parse_qs(parsed.query)["page[size]"], ["1"])

    @staticmethod
    def cftc_row(date="2024-06-04", market="UST 10Y NOTE - CHICAGO BOARD OF TRADE", **changes):
        return {"market_and_exchange_names": market, "report_date_as_yyyy_mm_dd": date + "T00:00:00.000",
                "lev_money_positions_short": "1234", "lev_money_positions_long": "567",
                "futonly_or_combined": "FutOnly", **changes}

    def test_cftc_api_failure_is_not_silently_skipped_or_overwritten(self):
        target = cqi.RAW / "cftc_tff.csv"
        target.parent.mkdir(parents=True)
        target.write_text("previous good file")
        with patch.object(cqi, "get", side_effect=RuntimeError("network unavailable")):
            with self.assertRaisesRegex(cqi.PipelineError, "CFTC API: network unavailable"):
                cqi.fetch_cftc()
        self.assertEqual(target.read_text(), "previous good file")

    def test_cftc_api_paginates_and_preserves_normalized_contract(self):
        rows = [self.cftc_row(date="2016-01-05"), self.cftc_row(date="2024-05-28"), self.cftc_row()]
        count = [{"row_count": "3"}]
        with patch.object(cqi, "CFTC_PAGE_SIZE", 2), patch.object(cqi, "get", side_effect=[count, rows[:2], rows[2:], count]) as get:
            result = cqi.fetch_cftc()
        self.assertEqual(result.columns.tolist(), ["market", "date", "lev_short", "lev_long"])
        self.assertEqual(len(result), 3)
        self.assertTrue(result["lev_short"].eq(1234).all())
        self.assertTrue(result["lev_long"].eq(567).all())
        self.assertEqual(result["date"].max(), pd.Timestamp("2024-06-04"))
        self.assertTrue((cqi.RAW / "cftc_tff.csv").exists())
        queries = [parse_qs(urlparse(call.args[0]).query) for call in get.call_args_list]
        self.assertEqual(queries[1]["$offset"], ["0"])
        self.assertEqual(queries[2]["$offset"], ["2"])
        self.assertEqual(queries[1]["$limit"], ["2"])
        self.assertEqual(queries[2]["$limit"], ["1"])
        self.assertEqual(queries[1]["$order"], ["report_date_as_yyyy_mm_dd,market_and_exchange_names"])
        self.assertEqual(queries[0]["$where"], queries[-1]["$where"])
        for query in queries:
            where = query["$where"][0]
            for required in ("futonly_or_combined = 'FutOnly'", "2016-01-01T00:00:00", "2024-06-11T00:00:00",
                             "'%UST%'", "'%TREASURY%'", "'%NOTE%'", "'%BOND%'", "not like '%MICRO%'", "not like '%SWAP%'"):
                self.assertIn(required, where)

    def test_cftc_api_invalid_or_unbounded_count_fails_before_data_fetch(self):
        for response in ({"error": "bad request"}, [], [{"row_count": "0"}], [{"row_count": "1.5"}],
                         [{"row_count": "-1"}], [{"row_count": "100001"}]):
            with self.subTest(response=response), patch.object(cqi, "get", return_value=response) as get:
                with self.assertRaises(cqi.PipelineError):
                    cqi.fetch_cftc()
                self.assertEqual(get.call_count, 1)
        self.assertFalse((cqi.RAW / "cftc_tff.csv").exists())

    def test_cftc_api_incomplete_page_fails(self):
        with patch.object(cqi, "get", side_effect=[[{"row_count": "2"}], [self.cftc_row()]]):
            with self.assertRaisesRegex(cqi.PipelineError, "incomplete pagination"):
                cqi.fetch_cftc()
        self.assertFalse((cqi.RAW / "cftc_tff.csv").exists())

    def test_cftc_api_count_change_during_pagination_fails(self):
        with patch.object(cqi, "get", side_effect=[[{"row_count": "1"}], [self.cftc_row()], [{"row_count": "2"}]]):
            with self.assertRaisesRegex(cqi.PipelineError, "count changed"):
                cqi.fetch_cftc()

    def test_cftc_api_repeated_page_is_not_silently_deduplicated(self):
        count = [{"row_count": "2"}]
        with patch.object(cqi, "CFTC_PAGE_SIZE", 1), patch.object(cqi, "get", side_effect=[count, [self.cftc_row()], [self.cftc_row()], count]):
            with self.assertRaisesRegex(cqi.PipelineError, "duplicate market/date"):
                cqi.fetch_cftc()

    def test_cftc_api_missing_field_fails(self):
        row = self.cftc_row()
        del row["lev_money_positions_short"]
        count = [{"row_count": "1"}]
        with patch.object(cqi, "get", side_effect=[count, [row], count]):
            with self.assertRaisesRegex(cqi.PipelineError, "missing required columns"):
                cqi.fetch_cftc()

    def test_cftc_api_invalid_counts_fail(self):
        count = [{"row_count": "1"}]
        for bad in (None, "unknown", "NaN", "inf", "-1", "1.5", True):
            with self.subTest(value=bad), patch.object(cqi, "get", side_effect=[count, [self.cftc_row(lev_money_positions_short=bad)], count]):
                with self.assertRaisesRegex(cqi.PipelineError, "invalid position counts"):
                    cqi.fetch_cftc()

    def test_cftc_api_out_of_scope_and_stale_records_fail(self):
        rows = [self.cftc_row(futonly_or_combined="Combined"), self.cftc_row(date="2015-12-29"),
                self.cftc_row(date="2024-06-11"), self.cftc_row(date="2024-01-02"),
                self.cftc_row(market="MICRO UST 10Y NOTE"), self.cftc_row(market="UST NOTE SWAP"),
                self.cftc_row(market="EURO FX"), self.cftc_row(market="UST INDEX")]
        count = [{"row_count": "1"}]
        for row in rows:
            with self.subTest(row=row), patch.object(cqi, "get", side_effect=[count, [row], count]):
                with self.assertRaises(cqi.PipelineError):
                    cqi.fetch_cftc()
        self.assertFalse((cqi.RAW / "cftc_tff.csv").exists())

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
        with patch.object(cqi, "require_api_key"), patch.object(cqi, "cmd_fetch", side_effect=cqi.PipelineError("broken")) as fetch, patch.object(cqi, "cmd_build") as build:
            self.assertEqual(cqi.main(["all"]), 1)
            fetch.assert_called_once()
            build.assert_not_called()

    def test_missing_api_key_stops_cli_before_any_fetch(self):
        with patch.object(cqi, "require_api_key", side_effect=cqi.FredAPIError("FRED_API_KEY missing")), patch.object(cqi, "cmd_fetch") as fetch:
            self.assertEqual(cqi.main(["all"]), 1)
            fetch.assert_not_called()

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

    def test_no_components_never_looks_healthy(self):
        with patch.object(cqi, "START", "2024-06-03"):
            out = cqi.build({})
        self.assertTrue(out["cqi"].isna().all())
        self.assertTrue(out["灯"].eq("数据缺失").all())

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

    def test_backtest_missing_input_is_nonzero(self):
        self.assertEqual(cqi.main(["backtest"]), 1)

    def test_backtest_plot_errors_are_nonzero(self):
        out = self.output()
        out["data_kind"] = "real"
        cqi.write_csv(cqi.OUT / "cqi_daily.csv", out, index_label="date")
        cqi.write_json(cqi.OUT / "build_status.json", {"status": "ok"})
        with patch.object(cqi, "backtest", return_value=("report", "未通过")), patch.dict("sys.modules", {"matplotlib": None}):
            self.assertEqual(cqi.main(["backtest"]), 1)


if __name__ == "__main__":
    unittest.main()
