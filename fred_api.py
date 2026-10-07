"""Fail-closed client for the official FRED observations API.

The application reads FRED_API_KEY at runtime. Never log request URLs, params,
response bodies, or underlying HTTP exceptions: they may contain the key.
Source: https://fred.stlouisfed.org/docs/api/fred/series_observations.html
"""
import math
import os
import re
import time
from datetime import date

import pandas as pd
import requests


ENDPOINT = "https://api.stlouisfed.org/fred/series/observations"
PAGE_LIMIT = 100000
MAX_ATTEMPTS = 3
TIMEOUT = (10, 45)


class FredAPIError(RuntimeError):
    """A safe-to-log error containing no HTTP body, URL, or credential."""


def _api_key():
    key = os.environ.get("FRED_API_KEY", "").strip()
    if not key:
        raise FredAPIError("FRED_API_KEY is missing; configure the repository secret before fetching") from None
    # Fixed categories only: never reveal the value, actual length, characters,
    # prefix/suffix, hash, or any derived identifier in logs.
    if any(char.isspace() for char in key):
        category = "embedded_whitespace"
    elif any(char in key for char in ("'", '"', "`", "“", "”", "‘", "’")):
        category = "surrounding_or_embedded_quotes"
    elif len(key) != 32:
        category = "expected_character_count_not_met"
    elif not re.fullmatch(r"[a-z0-9]{32}", key):
        category = "disallowed_character_class"
    else:
        category = None
    if category:
        raise FredAPIError(f"FRED_API_KEY has an invalid format; category={category}; update the configured secret with only the official key value") from None
    return key


def require_api_key():
    """Fail before a pipeline starts if its key is absent or malformed.

    This preflight deliberately returns no credential.
    """
    _api_key()


def _parse_date(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError
    return date.fromisoformat(value)


def _request_page(series_id, params):
    for attempt in range(MAX_ATTEMPTS):
        try:
            response = requests.get(
                ENDPOINT, params=params, timeout=TIMEOUT, allow_redirects=False,
                headers={"Accept": "application/json", "User-Agent": "HQ-Research/0.1"},
            )
        except (requests.Timeout, requests.ConnectionError):
            if attempt + 1 == MAX_ATTEMPTS:
                raise FredAPIError(f"FRED {series_id}: connection or timeout failure after bounded retries") from None
            time.sleep(2 ** attempt)
            continue
        except Exception:
            # Do not propagate an exception with a prepared URL or request repr.
            raise FredAPIError(f"FRED {series_id}: request failed") from None

        try:
            status = response.status_code
            if type(status) is not int:
                raise FredAPIError(f"FRED {series_id}: invalid HTTP status") from None
            if status == 429 or 500 <= status <= 599:
                if attempt + 1 == MAX_ATTEMPTS:
                    raise FredAPIError(f"FRED {series_id}: HTTP {status} after bounded retries") from None
            elif status != 200:
                # Do not follow redirects, parse an error body, or raise_for_status.
                raise FredAPIError(f"FRED {series_id}: HTTP {status}; request rejected") from None
            else:
                try:
                    return response.json()
                except Exception:
                    raise FredAPIError(f"FRED {series_id}: invalid JSON response") from None
        finally:
            # Closing is best-effort and must not replace a sanitized exception.
            try:
                response.close()
            except Exception:
                pass
        time.sleep(2 ** attempt)
    raise FredAPIError(f"FRED {series_id}: request did not complete") from None


def fetch_series(series_id, start, end):
    """Return all native-frequency levels in [start, end] as date/value columns.

    Dates are datetime64 and values are floats. FRED's '.' stays NaN; observations
    are never forward-filled or silently deduplicated. The caller must apply its
    series-specific freshness/release-lag rules to the original valid dates.
    There is no CSV fallback or cached-data fallback.
    """
    key = _api_key()
    if not isinstance(series_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", series_id):
        raise FredAPIError("FRED: invalid series identifier") from None
    try:
        first, last = _parse_date(start), _parse_date(end)
        if first > last:
            raise ValueError
    except (TypeError, ValueError):
        raise FredAPIError(f"FRED {series_id}: invalid requested date range") from None

    params = {
        "api_key": key,
        "series_id": series_id,
        "file_type": "json",
        "observation_start": start,
        "observation_end": end,
        "units": "lin",
        "output_type": 1,
        "sort_order": "asc",
        "limit": PAGE_LIMIT,
        # Deliberately omit frequency and aggregation_method.
    }
    rows = []
    expected_count = None
    previous_date = None
    offset = 0
    while True:
        payload = _request_page(series_id, dict(params, offset=offset))
        if not isinstance(payload, dict):
            raise FredAPIError(f"FRED {series_id}: invalid response schema") from None
        count, returned_offset, limit = (payload.get(field) for field in ("count", "offset", "limit"))
        observations = payload.get("observations")
        if (any(type(value) is not int for value in (count, returned_offset, limit))
                or not isinstance(observations, list)
                or not 0 <= count <= (last - first).days + 1
                or returned_offset != offset or not 1 <= limit <= PAGE_LIMIT):
            raise FredAPIError(f"FRED {series_id}: invalid pagination schema") from None
        for field, expected in {
            "observation_start": start, "observation_end": end,
            "units": "lin", "output_type": 1, "file_type": "json", "sort_order": "asc",
        }.items():
            if type(payload.get(field)) is not type(expected) or payload.get(field) != expected:
                raise FredAPIError(f"FRED {series_id}: unexpected response metadata") from None
        if count == 0:
            raise FredAPIError(f"FRED {series_id}: empty observations") from None
        if expected_count is None:
            expected_count = count
        if count != expected_count or len(observations) != min(limit, count - offset):
            raise FredAPIError(f"FRED {series_id}: incomplete or changed pagination") from None
        for observation in observations:
            if not isinstance(observation, dict) or not {"date", "value"} <= observation.keys():
                raise FredAPIError(f"FRED {series_id}: invalid observation schema") from None
            try:
                observed_date = _parse_date(observation["date"])
            except (TypeError, ValueError):
                raise FredAPIError(f"FRED {series_id}: invalid observation date") from None
            if not first <= observed_date <= last:
                raise FredAPIError(f"FRED {series_id}: observation outside requested date range") from None
            if previous_date is not None and observed_date <= previous_date:
                raise FredAPIError(f"FRED {series_id}: duplicate or unordered observation dates") from None
            previous_date = observed_date
            raw_value = observation["value"]
            try:
                if not isinstance(raw_value, str):
                    raise ValueError
                if raw_value != "." and not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", raw_value):
                    raise ValueError
                value = float("nan") if raw_value == "." else float(raw_value)
                if raw_value != "." and not math.isfinite(value):
                    raise ValueError
            except (TypeError, ValueError, OverflowError):
                raise FredAPIError(f"FRED {series_id}: invalid observation value") from None
            rows.append((observed_date, value))
        offset += len(observations)
        if offset == expected_count:
            break

    if not any(math.isfinite(value) for _, value in rows):
        raise FredAPIError(f"FRED {series_id}: no valid numeric observations") from None
    try:
        frame = pd.DataFrame(rows, columns=["date", "value"])
        frame["date"] = pd.to_datetime(frame["date"], errors="raise")
        frame["value"] = frame["value"].astype(float)
    except Exception:
        raise FredAPIError(f"FRED {series_id}: observations cannot be represented") from None
    return frame
