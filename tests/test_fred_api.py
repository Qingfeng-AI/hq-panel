"""Offline official-API contract and credential-redaction regressions."""
import os
import traceback
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import requests

import fred_api


# Explicit test-only value. Never read a real credential or make an HTTP call.
DUMMY_KEY = "dummyfredapitestonly".ljust(32, "0")


def page(rows=None, **changes):
    if rows is None:
        rows = [{"date": "2024-06-10", "value": "4.5"}]
    result = {
        "count": len(rows), "offset": 0, "limit": fred_api.PAGE_LIMIT,
        "observations": rows, "observation_start": "2024-06-01",
        "observation_end": "2024-06-10", "units": "lin", "output_type": 1,
        "file_type": "json", "sort_order": "asc",
    }
    result.update(changes)
    return result


def response(payload=None, status=200):
    result = Mock(status_code=status)
    result.json.return_value = page() if payload is None else payload
    return result


class FredAPITests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"FRED_API_KEY": DUMMY_KEY}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.get_patcher = patch.object(fred_api.requests, "get", side_effect=AssertionError("unexpected HTTP call"))
        self.get = self.get_patcher.start()
        self.addCleanup(self.get_patcher.stop)
        self.sleep_patcher = patch.object(fred_api.time, "sleep")
        self.sleep = self.sleep_patcher.start()
        self.addCleanup(self.sleep_patcher.stop)

    def fetch(self):
        return fred_api.fetch_series("DGS10", "2024-06-01", "2024-06-10")

    def serve(self, *responses):
        self.get.side_effect = list(responses)

    def test_preflight_returns_no_credential(self):
        self.assertIsNone(fred_api.require_api_key())
        self.get.assert_not_called()

    def test_missing_and_invalid_key_fail_before_network(self):
        for value in (None, "", "  ", "not-a-valid-key"):
            with self.subTest(value=value), patch.dict(os.environ, {}, clear=True):
                if value is not None:
                    os.environ["FRED_API_KEY"] = value
                with self.assertRaisesRegex(fred_api.FredAPIError, "FRED_API_KEY"):
                    self.fetch()
        self.get.assert_not_called()

    def test_format_diagnostics_are_fixed_categories_without_credential_details(self):
        cases = [
            (DUMMY_KEY[:8] + " " + DUMMY_KEY[8:], "embedded_whitespace"),
            ('"' + DUMMY_KEY + '"', "surrounding_or_embedded_quotes"),
            (DUMMY_KEY + "0", "expected_character_count_not_met"),
            ("D" + DUMMY_KEY[1:], "disallowed_character_class"),
        ]
        for value, category in cases:
            with self.subTest(category=category), patch.dict(os.environ, {"FRED_API_KEY": value}):
                try:
                    fred_api.require_api_key()
                except fred_api.FredAPIError as error:
                    text = "".join(traceback.format_exception(error))
                    expected = f"FRED_API_KEY has an invalid format; category={category}; update the configured secret with only the official key value"
                    self.assertEqual(str(error), expected)
                    self.assertNotIn(value, text)
                    self.assertNotIn(DUMMY_KEY, text)
                else:
                    self.fail("malformed fixture was accepted")
        self.get.assert_not_called()

    def test_official_alphanumeric_keys_are_not_restricted_to_hex(self):
        with patch.dict(os.environ, {"FRED_API_KEY": "z" * 32}):
            self.assertIsNone(fred_api.require_api_key())
        self.get.assert_not_called()

    def test_outer_copy_whitespace_is_trimmed_without_disclosure(self):
        with patch.dict(os.environ, {"FRED_API_KEY": "  " + DUMMY_KEY + "\n"}):
            self.assertIsNone(fred_api.require_api_key())
        self.get.assert_not_called()

    def test_levels_native_frequency_https_and_no_redirects(self):
        reply = response(page([{"date": "2024-06-07", "value": "4.5"},
                               {"date": "2024-06-10", "value": "."}]))
        self.serve(reply)
        frame = self.fetch()
        self.assertEqual(list(frame.columns), ["date", "value"])
        self.assertEqual(frame["date"].dtype, "datetime64[ns]")
        self.assertEqual(frame["value"].dtype, "float64")
        self.assertEqual(frame.iloc[0]["value"], 4.5)
        self.assertTrue(pd.isna(frame.iloc[1]["value"]))
        args, kwargs = self.get.call_args
        self.assertEqual(args, ("https://api.stlouisfed.org/fred/series/observations",))
        self.assertEqual(kwargs["timeout"], fred_api.TIMEOUT)
        self.assertFalse(kwargs["allow_redirects"])
        self.assertEqual(kwargs["params"], {
            "api_key": DUMMY_KEY, "series_id": "DGS10", "file_type": "json",
            "observation_start": "2024-06-01", "observation_end": "2024-06-10",
            "units": "lin", "output_type": 1, "sort_order": "asc",
            "limit": fred_api.PAGE_LIMIT, "offset": 0,
        })
        reply.close.assert_called_once()

    def test_pagination_collects_every_observation(self):
        first = page([{"date": "2024-06-03", "value": "4.1"},
                      {"date": "2024-06-04", "value": "."}], count=3, limit=2)
        second = page([{"date": "2024-06-10", "value": "4.5"}], count=3, limit=2, offset=2)
        with patch.object(fred_api, "PAGE_LIMIT", 2):
            self.serve(response(first), response(second))
            frame = self.fetch()
        self.assertEqual(len(frame), 3)
        self.assertEqual([call.kwargs["params"]["offset"] for call in self.get.call_args_list], [0, 2])
        self.assertTrue(pd.isna(frame.iloc[1]["value"]))

    def test_auth_and_redirect_errors_are_never_retried(self):
        for status in (400, 401, 403, 404, 301, 302, 307, 308):
            with self.subTest(status=status):
                self.get.reset_mock()
                reply = response(status=status)
                self.serve(reply)
                with self.assertRaisesRegex(fred_api.FredAPIError, f"HTTP {status}"):
                    self.fetch()
                self.get.assert_called_once()
                reply.json.assert_not_called()
        self.sleep.assert_not_called()

    def test_transient_http_errors_retry_then_succeed(self):
        for status in (429, 500, 502, 503, 504):
            with self.subTest(status=status):
                self.get.reset_mock()
                self.serve(response(status=status), response())
                self.assertEqual(len(self.fetch()), 1)
                self.assertEqual(self.get.call_count, 2)

    def test_transient_http_retries_are_bounded(self):
        self.serve(*(response(status=503) for _ in range(fred_api.MAX_ATTEMPTS)))
        with self.assertRaisesRegex(fred_api.FredAPIError, "HTTP 503 after bounded retries"):
            self.fetch()
        self.assertEqual(self.get.call_count, fred_api.MAX_ATTEMPTS)
        self.assertEqual(self.sleep.call_count, fred_api.MAX_ATTEMPTS - 1)

    def test_timeout_and_connection_retries_are_bounded(self):
        for kind in (requests.Timeout, requests.ConnectionError):
            with self.subTest(kind=kind):
                self.get.reset_mock()
                self.sleep.reset_mock()
                self.serve(*(kind("sensitive request text") for _ in range(fred_api.MAX_ATTEMPTS)))
                with self.assertRaisesRegex(fred_api.FredAPIError, "connection or timeout"):
                    self.fetch()
                self.assertEqual(self.get.call_count, fred_api.MAX_ATTEMPTS)
                self.assertEqual(self.sleep.call_count, fred_api.MAX_ATTEMPTS - 1)

    def test_malformed_json_is_not_retried(self):
        reply = response()
        reply.json.side_effect = ValueError("invalid body")
        self.serve(reply)
        with self.assertRaisesRegex(fred_api.FredAPIError, "invalid JSON"):
            self.fetch()
        self.get.assert_called_once()

    def test_schema_empty_and_all_missing_data_fail(self):
        cases = [[], {"error_code": 400, "error_message": DUMMY_KEY},
                 page([]), page([{"date": "2024-06-10", "value": "."}]),
                 page([{"date": "2024-06-10"}]),
                 page(["not an observation"]), page(observations={}),
                 page(count="1"), page(offset=True), page(limit=0),
                 page(units="pch"), page(file_type="csv"), page(output_type=4), page(output_type=True),
                 page(observation_start="2020-01-01"), page(sort_order="desc")]
        for payload in cases:
            with self.subTest(payload=payload):
                self.serve(response(payload))
                with self.assertRaises(fred_api.FredAPIError):
                    self.fetch()

    def test_invalid_values_are_not_silently_converted_to_missing(self):
        for value in (None, True, 4.5, "", "garbage", "nan", "NaN", "inf", "-inf", "1e999", "1_000", " 4 "):
            with self.subTest(value=value):
                self.serve(response(page([{"date": "2024-06-10", "value": value}])))
                with self.assertRaisesRegex(fred_api.FredAPIError, "invalid observation value"):
                    self.fetch()

    def test_dates_must_be_valid_in_range_unique_and_ordered(self):
        for dates in (("2024-06-11",), ("2024-05-31",), ("2024-02-30",),
                      ("2024-6-1",), (None,), ("2024-06-03", "2024-06-03"),
                      ("2024-06-04", "2024-06-03")):
            with self.subTest(dates=dates):
                self.serve(response(page([{"date": day, "value": "4"} for day in dates])))
                with self.assertRaises(fred_api.FredAPIError):
                    self.fetch()

    def test_duplicate_on_page_boundary_fails(self):
        self.serve(response(page(count=2, limit=1)),
                   response(page(count=2, limit=1, offset=1)))
        with self.assertRaisesRegex(fred_api.FredAPIError, "duplicate"):
            self.fetch()

    def test_incomplete_and_changing_pagination_fail(self):
        for payload in (page(count=2), page(offset=1), page(count=11), page(limit=100001)):
            with self.subTest(payload=payload):
                self.serve(response(payload))
                with self.assertRaises(fred_api.FredAPIError):
                    self.fetch()
        first = page([{"date": "2024-06-03", "value": "4"}], count=2, limit=1)
        for second in (page(count=3, offset=1, limit=1),
                       page([], count=2, offset=1, limit=1),
                       page(count=2, offset=0, limit=1)):
            with self.subTest(second=second):
                self.serve(response(first), response(second))
                with self.assertRaises(fred_api.FredAPIError):
                    self.fetch()

    def test_old_valid_observation_is_not_relabelled_or_filled(self):
        # Cadence-specific freshness is a pipeline responsibility. Its validator
        # must see the original valid date even if later FRED rows are missing.
        self.serve(response(page([{"date": "2024-06-03", "value": "4"},
                                  {"date": "2024-06-10", "value": "."}])))
        frame = self.fetch()
        valid = frame.dropna(subset=["value"])
        self.assertEqual(valid["date"].max(), pd.Timestamp("2024-06-03"))
        self.assertEqual(len(frame), 2)

    def test_invalid_arguments_fail_without_network(self):
        for series_id, start, end in (("invalid\nidentifier", "2024-06-01", "2024-06-10"),
                                       ("DGS10", "2024-06-11", "2024-06-10"),
                                       ("DGS10", "June 1", "2024-06-10")):
            with self.subTest(series=series_id):
                with self.assertRaises(fred_api.FredAPIError):
                    fred_api.fetch_series(series_id, start, end)
        self.get.assert_not_called()

    def test_underlying_secret_url_never_appears_in_errors_or_tracebacks(self):
        secret_url = "https://api.stlouisfed.org/fred/series/observations?api_key=" + DUMMY_KEY
        malformed = response()
        malformed.json.side_effect = ValueError(secret_url)
        cases = [([requests.Timeout(secret_url)] * fred_api.MAX_ATTEMPTS),
                 [requests.RequestException(secret_url)], [RuntimeError(secret_url)],
                 [malformed], [response({"error_message": secret_url}, status=400)],
                 [response({"error_message": secret_url})],
                 [response(page([{"date": secret_url, "value": "4"}]))],
                 [response(page([{"date": "2024-06-10", "value": secret_url}]))]]
        for replies in cases:
            with self.subTest():
                self.serve(*replies)
                try:
                    self.fetch()
                except fred_api.FredAPIError as error:
                    rendered = "".join(traceback.format_exception(type(error), error, error.__traceback__))
                    self.assertNotIn(DUMMY_KEY, str(error))
                    self.assertNotIn(DUMMY_KEY, repr(error))
                    self.assertNotIn(secret_url, rendered)
                    self.assertNotIn(DUMMY_KEY, rendered)
                    self.assertTrue(error.__suppress_context__)
                else:
                    self.fail("expected a sanitized error")


if __name__ == "__main__":
    unittest.main()
