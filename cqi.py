#!/usr/bin/env python3
"""
抵押品质量指数（公开版）数据管线
HQ Research · 美债体系卷六 · V0.3

子命令：
  probe     逐一探测七组数据源，打印字段，确认接口可用
  fetch     抓取原始数据到 data/raw/
  build     计算六个成分、标准化、合成指数，输出 data/cqi_daily.csv
  backtest  按卷六第二节的标准回测，输出 reports/backtest.md
  all       fetch + build + backtest
  demo      用合成数据跑通 build + backtest（不联网，用于检查管线本身）

依赖：pandas、numpy、requests；画图可选 matplotlib。
"""
import argparse, io, json, os, sys, tempfile, time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import numpy as np
import pandas as pd
from fred_api import FredAPIError, fetch_series, require_api_key

ROOT = Path(__file__).resolve().parent
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data"
REP = ROOT / "reports"

# ───────────────────────── 配置 ─────────────────────────
START = "2018-04-02"             # SOFR 首次发布日
END = date.today().isoformat()
Z_WINDOW = 756                   # 滚动 3 年（交易日）
Z_MIN = 252                      # 至少 1 年才出读数
BREAK_SIGMA = 2.0                # 断裂规则：单日变动超过 2 个标准差
YELLOW, RED = 1.0, 2.0           # 初始阈值，回测后校准
MIN_COMPONENTS = 3               # 合成至少需要 3 个成分有读数

URL = {
    "sofr": "https://markets.newyorkfed.org/api/rates/secured/sofr/search.json?startDate={s}&endDate={e}",
    "repo_ops": "https://markets.newyorkfed.org/api/rp/results/search.json?startDate={s}&endDate={e}",
    "auctions": ("https://www.treasurydirect.gov/TA_WS/securities/search?startDate={s}&endDate={e}"
                 "&dateFieldName=auctionDate&compact=false&format=json"),
    "buybacks": ("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
                 "v1/accounting/od/buybacks_operations?page[size]=10000"),
    "cftc_tff": "https://publicreporting.cftc.gov/resource/gpe5-46if.json",
}
CFTC_PAGE_SIZE = 5000
CFTC_MAX_PAGES = 20

# 回测事件：卷六第二节
EVENTS = {
    "2019-09 回购利率飙升": "2019-09-16",
    "2020-03 现金争夺": "2020-03-09",
    "2023-03 硅谷银行": "2023-03-10",
    "2025-04 关税冲击": "2025-04-03",
    "2025-11 月末回购压力": "2025-11-25",
    "2026-09 长端新高": "2026-09-22",
}
MUST_FLAG = ["2019-09 回购利率飙升", "2020-03 现金争夺", "2025-04 关税冲击"]
CALM = {"2021 全年": ("2021-01-01", "2021-12-31"), "2024 上半年": ("2024-01-01", "2024-06-30")}
CALM_MAX_SHARE = 0.10            # 平静期亮黄灯的天数占比上限

UA = {"User-Agent": "HQ-Research-CQI/0.3 (research pipeline)"}

# Calendar-day allowances accommodate weekends/publication lags, not unlimited carry-forward.
SOURCE_FILES = {"sofr": "sofr", "admin_rate": "admin_rate", "trust": "trust", "repo_ops": "repo_ops",
                "auctions": "auctions", "buybacks": "buybacks", "cftc": "cftc_tff"}
SOURCE_MAX_AGE_DAYS = {"sofr": 7, "admin_rate": 7, "trust": 10, "repo_ops": 7,
                       "auctions": 45, "buybacks": 45, "cftc": 14}

# V0.3：核心来源必须全部成功，否则任务失败、不发布；可选来源失败时进入“降级”状态，
# 对应成分不参与合成（绝不沿用旧数据），并在报告与运行页面上明确标出。
CORE_SOURCES = ("sofr", "admin_rate", "trust")
OPTIONAL_SOURCES = ("repo_ops", "auctions", "buybacks", "cftc")
COMPONENT_SOURCES = {
    "回购压力": ("sofr", "admin_rate"),
    "央行回购使用量": ("repo_ops",),
    "拍卖吸收度": ("auctions",),
    "回购卖压": ("buybacks",),
    "基差平仓速度": ("cftc",),
    "信任背离": ("trust",),
}


def annotate(level, title, message):
    """在 GitHub Actions 运行页面生成公开可见的注释；本地运行时只是普通输出。"""
    msg = " ".join(str(message).split())[:900].replace("%", "%25")
    title = str(title).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A").replace(":", "%3A").replace(",", "%2C")
    print(f"::{level} title={title}::{msg}", flush=True)


class PipelineError(RuntimeError):
    """A source or build cannot be trusted; CLI callers must fail the job."""


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def atomic_write(path, writer):
    """Never leave a truncated published file when fetching/building fails."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    os.close(fd)
    try:
        writer(Path(temporary))
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_json(path, obj):
    atomic_write(path, lambda p: p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8"))


def write_csv(path, frame, **kwargs):
    atomic_write(path, lambda p: frame.to_csv(p, **kwargs))


def require_columns(df, columns, source):
    if df.empty:
        raise PipelineError(f"{source}: empty response/data")
    missing = set(columns) - set(df.columns)
    if missing:
        raise PipelineError(f"{source}: missing required columns: {', '.join(sorted(missing))}")


def pick_column(df, candidates, source, label, predicate=None):
    for column in candidates:
        if column in df.columns:
            return column
    matches = [c for c in df.columns if predicate and predicate(c)]
    if len(matches) == 1:
        return matches[0]
    raise PipelineError(f"{source}: missing or ambiguous {label} column")


def validate_source(source, df, check_freshness=True):
    """Normalize known schemas, reject empty/unusable data, and inspect observation dates."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    date_col = "date"
    if source == "sofr":
        required, numeric = ["date", "sofr"], ["sofr"]
    elif source == "admin_rate":
        required, numeric = ["date", "rate", "series"], ["rate"]
    elif source == "repo_ops":
        date_col = "operationDate"
        required = [date_col, "operationType", "totalAmtAccepted"]
        numeric = ["totalAmtAccepted"]
    elif source == "auctions":
        date_col = "auctionDate"
        denom = pick_column(df, ["competitiveAccepted", "totalAccepted"], source, "accepted amount")
        required = [date_col, "securityType", "securityTerm", "bidToCoverRatio", "primaryDealerAccepted", denom]
        numeric = ["bidToCoverRatio", "primaryDealerAccepted", denom]
    elif source == "buybacks":
        date_col = pick_column(df, ["operation_date"], source, "operation date")
        offered = pick_column(df, ["total_offered", "total_amount_offered", "total_par_amt_offered"],
                              source, "offered amount", lambda c: "offered" in c and "total" in c)
        accepted = pick_column(df, ["total_accepted", "total_amount_accepted", "total_par_amt_accepted"],
                               source, "accepted amount", lambda c: "accepted" in c and "total" in c)
        df = df.rename(columns={offered: "total_offered", accepted: "total_accepted"})
        required, numeric = [date_col, "total_offered", "total_accepted"], ["total_offered", "total_accepted"]
    elif source == "cftc":
        required, numeric = ["market", "date", "lev_short", "lev_long"], ["lev_short", "lev_long"]
    elif source == "trust":
        required, numeric = ["date", "value", "series"], ["value"]
    else:
        raise PipelineError(f"unknown source: {source}")
    require_columns(df, required, source)
    parsed = pd.to_datetime(df[date_col], errors="coerce")
    if parsed.isna().any():
        raise PipelineError(f"{source}: invalid or missing observation dates")
    df[date_col] = parsed.dt.normalize()
    df = df.loc[df[date_col] <= pd.Timestamp(END)].copy()
    for c in numeric:
        df[c] = num(df[c]).replace([np.inf, -np.inf], np.nan)
    if source == "trust":
        df = df.dropna(subset=numeric)
        if not {"DGS10", "DTWEXBGS"}.issubset(set(df["series"])):
            raise PipelineError("trust: both DGS10 and DTWEXBGS history are required")
        usable = df
    elif source == "admin_rate":
        df = df.dropna(subset=numeric)
        if not {"IOER", "IORB"}.issubset(set(df["series"])):
            raise PipelineError("admin_rate: both IOER and IORB history are required")
        usable = df.loc[df["series"].eq("IORB")]
    elif source == "repo_ops":
        df = df.loc[df["operationType"].astype(str).str.strip().str.lower().eq("repo")].copy()
        df["operationType"] = "Repo"
        usable = df.dropna(subset=numeric)
        if len(usable) != len(df) or (usable["totalAmtAccepted"] < 0).any():
            raise PipelineError("repo_ops: invalid accepted amounts; missing values are not zero")
    elif source == "auctions":
        usable = df.loc[df["securityType"].isin(["Note", "Bond"])].dropna(subset=numeric)
        usable = usable.loc[(usable[denom] > 0) & (usable["bidToCoverRatio"] > 0)
                            & (usable["primaryDealerAccepted"] >= 0)]
        df = usable.copy()
    elif source == "buybacks":
        if "security_type" in df:
            df = df.loc[~df["security_type"].astype(str).str.contains("TIPS", case=False)].copy()
        usable = df.dropna(subset=numeric)
        usable = usable.loc[(usable["total_accepted"] > 0) & (usable["total_offered"] >= 0)]
        df = usable.copy()
    elif source == "cftc":
        market = df["market"].astype(str).str.upper()
        mask = market.str.contains("UST|TREASURY") & market.str.contains("NOTE|BOND") & ~market.str.contains("MICRO|SWAP")
        df = df.loc[mask].copy()
        usable = df.dropna(subset=numeric)
        if len(usable) != len(df) or (usable[numeric] < 0).any().any():
            raise PipelineError("cftc: invalid position counts")
    else:
        usable = df.dropna(subset=numeric)
        if len(usable) != len(df):
            raise PipelineError(f"{source}: invalid numeric observations")
    if usable.empty:
        raise PipelineError(f"{source}: no usable observations")
    latest = usable[date_col].max()
    if source == "trust":
        # A fresh series must never hide a stale counterpart needed by the signal.
        latest_by_series = usable.groupby("series")[date_col].max().loc[["DGS10", "DTWEXBGS"]]
        for sid, observation in latest_by_series.items():
            series_age = (pd.Timestamp(END) - observation).days
            if check_freshness and series_age > SOURCE_MAX_AGE_DAYS[source]:
                raise PipelineError(f"trust: {sid} stale observations (latest {observation.date()}, {series_age} calendar days old)")
        latest = latest_by_series.min()
    age = (pd.Timestamp(END) - latest).days
    if check_freshness and age > SOURCE_MAX_AGE_DAYS[source]:
        raise PipelineError(f"{source}: stale observations (latest {latest.date()}, {age} calendar days old)")
    keys = [date_col]
    if source in {"admin_rate", "trust"}:
        keys += ["series"]
    elif source == "cftc":
        keys += ["market"]
    if source in {"sofr", "admin_rate", "cftc", "trust"}:
        if df.drop_duplicates().duplicated(keys).any():
            raise PipelineError(f"{source}: conflicting duplicate observations")
        df = df.drop_duplicates(keys)
    else:
        df = df.drop_duplicates()
    return df.sort_values(date_col).reset_index(drop=True), latest.date().isoformat()


def save_source(source, df):
    df, _ = validate_source(source, df)
    write_csv(RAW / f"{SOURCE_FILES[source]}.csv", df, index=False)
    return df


# ───────────────────────── 工具函数 ─────────────────────────
def get(url, as_json=True, retries=3):
    import requests
    for i in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            r.raise_for_status()
            return r.json() if as_json else r.content
        except Exception as ex:
            if i == retries - 1:
                raise
            time.sleep(2 * (i + 1))


def year_chunks(start, end):
    s, e = pd.Timestamp(start), pd.Timestamp(end)
    for y in range(s.year, e.year + 1):
        a = max(s, pd.Timestamp(f"{y}-01-01"))
        b = min(e, pd.Timestamp(f"{y}-12-31"))
        yield a.date().isoformat(), b.date().isoformat()


def find_records(obj, must_key):
    """在嵌套 JSON 中找出所有含 must_key 的字典（接口结构不稳定时用）。"""
    out = []
    if isinstance(obj, dict):
        if must_key in obj:
            out.append(obj)
        for v in obj.values():
            out += find_records(v, must_key)
    elif isinstance(obj, list):
        for v in obj:
            out += find_records(v, must_key)
    return out


def num(x):
    if isinstance(x, pd.Series) and (x.dtype == object or pd.api.types.is_string_dtype(x)):
        x = x.astype(str).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(x, errors="coerce")


def rolling_z(s, window=Z_WINDOW, minp=Z_MIN):
    m = s.rolling(window, min_periods=minp).mean()
    sd = s.rolling(window, min_periods=minp).std()
    return (s - m) / sd.replace(0, np.nan)


# ───────────────────────── 抓取 ─────────────────────────
def fetch_sofr():
    rows = []
    for s, e in year_chunks(START, END):
        js = get(URL["sofr"].format(s=s, e=e))
        rows += find_records(js, "effectiveDate")
    df = pd.DataFrame(rows)
    require_columns(df, ["effectiveDate", "percentRate"], "sofr")
    df = df[["effectiveDate", "percentRate"]].rename(
        columns={"effectiveDate": "date", "percentRate": "sofr"})
    return save_source("sofr", df)


def fred_frame(sid, start, end):
    try:
        frame = fetch_series(sid, start, end)
    except FredAPIError as ex:
        raise PipelineError(str(ex)) from None
    except Exception:
        raise PipelineError(f"{sid}: FRED API request failed") from None
    require_columns(frame, ["date", "value"], sid)
    return frame[["date", "value"]].copy()


def fetch_admin_rate():
    frames = []
    for sid in ("IOER", "IORB"):
        end = min(END, "2021-07-28") if sid == "IOER" else END
        d = fred_frame(sid, START, end).rename(columns={"value": "rate"})
        d["series"] = sid
        frames.append(d)
    return save_source("admin_rate", pd.concat(frames, ignore_index=True))


def fetch_trust():
    """V0.3 信任背离输入，使用官方 FRED observations API。"""
    frames = []
    for sid in ("DGS10", "DTWEXBGS"):
        d = fred_frame(sid, START, END)
        d["series"] = sid
        frames.append(d)
    return save_source("trust", pd.concat(frames, ignore_index=True))


def fetch_repo_ops():
    rows = []
    for s, e in year_chunks("2019-09-01", END):
        # 按季度切片，避免单次返回过大
        for q in pd.date_range(s, e, freq="QS").union([pd.Timestamp(s)]):
            qs = max(q, pd.Timestamp(s)).date().isoformat()
            qe = min(q + pd.offsets.QuarterEnd(0), pd.Timestamp(e)).date().isoformat()
            js = get(URL["repo_ops"].format(s=qs, e=qe))
            rows += find_records(js, "operationType")
    df = pd.DataFrame(rows)
    keep = [c for c in ("operationId", "operationDate", "operationType", "totalAmtAccepted") if c in df]
    return save_source("repo_ops", df[keep].drop_duplicates())


def fetch_auctions():
    rows = []
    for s, e in year_chunks("2016-01-01", END):   # 多抓两年，供滚动均值热身
        js = get(URL["auctions"].format(s=s, e=e))
        rows += js if isinstance(js, list) else find_records(js, "cusip")
    df = pd.DataFrame(rows)
    keep = [c for c in ("auctionDate", "securityType", "securityTerm", "cusip", "reopening",
                        "bidToCoverRatio", "primaryDealerAccepted", "competitiveAccepted",
                        "totalAccepted", "highYield") if c in df]
    return save_source("auctions", df[keep])


def fetch_buybacks():
    frames, seen = [], set()
    url = URL["buybacks"]
    while url:
        if url in seen:
            raise PipelineError("buybacks: pagination loop")
        seen.add(url)
        js = get(url)
        if not isinstance(js, dict) or not isinstance(js.get("data"), list):
            raise PipelineError("buybacks: expected a JSON data array")
        if not js["data"]:
            raise PipelineError("buybacks: empty response page")
        frames.append(pd.DataFrame(js["data"]))
        next_page = js.get("links", {}).get("next")
        if next_page:
            from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
            if next_page.startswith("&"):
                parts = urlparse(url)
                query = dict(parse_qsl(parts.query))
                query.update(parse_qsl(next_page.lstrip("&")))
                candidate = urlunparse(parts._replace(query=urlencode(query)))
            else:
                candidate = urljoin(url, next_page)
            if urlparse(candidate).netloc != urlparse(URL["buybacks"]).netloc:
                raise PipelineError("buybacks: unexpected pagination host")
            url = candidate
        else:
            expected = js.get("meta", {}).get("total-count")
            if expected is not None and sum(len(f) for f in frames) < int(expected):
                raise PipelineError("buybacks: incomplete pagination")
            url = None
    return save_source("buybacks", pd.concat(frames, ignore_index=True))


def cftc_query(**query):
    """Official TFF futures-only API; use the same Treasury contract universe as before."""
    next_day = (pd.Timestamp(END) + pd.Timedelta(days=1)).date().isoformat()
    market = "upper(market_and_exchange_names)"
    where = ("futonly_or_combined = 'FutOnly' "
             "AND report_date_as_yyyy_mm_dd >= '2016-01-01T00:00:00' "
             f"AND report_date_as_yyyy_mm_dd < '{next_day}T00:00:00' "
             f"AND ({market} like '%UST%' OR {market} like '%TREASURY%') "
             f"AND ({market} like '%NOTE%' OR {market} like '%BOND%') "
             f"AND {market} not like '%MICRO%' AND {market} not like '%SWAP%'")
    return URL["cftc_tff"] + "?" + urlencode({"$where": where, **query})


def fetch_cftc():
    # CFTC documents anonymous API access and the historical PRE datasets in FAQ 12–13:
    # https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm
    columns = {"market_and_exchange_names": "market", "report_date_as_yyyy_mm_dd": "date",
               "lev_money_positions_short": "lev_short", "lev_money_positions_long": "lev_long"}

    def row_count():
        response = get(cftc_query(**{"$select": "count(*) as row_count"}))
        if (not isinstance(response, list) or len(response) != 1
                or not isinstance(response[0], dict)
                or not str(response[0].get("row_count", "")).isdecimal()):
            raise PipelineError("invalid row count response")
        count = int(response[0]["row_count"])
        if count == 0:
            raise PipelineError("empty response/data")
        if count > CFTC_PAGE_SIZE * CFTC_MAX_PAGES:
            raise PipelineError("row count exceeds pagination limit")
        return count

    try:
        expected = row_count()
        rows = []
        for offset in range(0, expected, CFTC_PAGE_SIZE):
            limit = min(CFTC_PAGE_SIZE, expected - offset)
            page = get(cftc_query(**{
                "$select": ",".join(columns) + ",futonly_or_combined",
                "$order": "report_date_as_yyyy_mm_dd,market_and_exchange_names",
                "$limit": limit, "$offset": offset,
            }))
            if not isinstance(page, list) or not all(isinstance(row, dict) for row in page):
                raise PipelineError("expected a JSON data array")
            if len(page) != limit:
                raise PipelineError("incomplete pagination: unexpected page length")
            rows.extend(page)
        if row_count() != expected:
            raise PipelineError("row count changed during pagination; retry the fetch")
        df = pd.DataFrame(rows)
        require_columns(df, list(columns) + ["futonly_or_combined"], "cftc")
        if not df["futonly_or_combined"].eq("FutOnly").all():
            raise PipelineError("unexpected non-futures-only records")
        df = df[list(columns)].rename(columns=columns)
        dates = pd.to_datetime(df["date"], errors="coerce")
        if dates.isna().any() or not dates.between("2016-01-01", END).all():
            raise PipelineError("invalid or out-of-range report dates")
        df["date"] = dates.dt.normalize()
        if df.duplicated(["date", "market"]).any():
            raise PipelineError("duplicate market/date records in pagination")
        for column in ("lev_short", "lev_long"):
            values = num(df[column])
            if (df[column].map(lambda value: isinstance(value, bool)).any()
                    or (values.isna() | ~np.isfinite(values) | (values < 0) | (values % 1 != 0)).any()):
                raise PipelineError("invalid position counts")
            df[column] = values
        df, _ = validate_source("cftc", df)
        if len(df) != expected:
            raise PipelineError("unexpected contracts or incomplete selected history")
        return save_source("cftc", df)
    except Exception as ex:
        raise PipelineError(f"CFTC API: {ex}") from ex


FETCHERS = {"sofr": fetch_sofr, "admin_rate": fetch_admin_rate, "trust": fetch_trust, "repo_ops": fetch_repo_ops,
            "auctions": fetch_auctions, "buybacks": fetch_buybacks, "cftc": fetch_cftc}


def cmd_fetch(_):
    RAW.mkdir(parents=True, exist_ok=True)
    status = {"status": "running", "started_at": utc_now(), "as_of": END, "sources": {}}
    write_json(RAW / "fetch_status.json", status)
    # Invalidate a prior success before touching any input files.
    write_json(OUT / "build_status.json", {"status": "pending", "generated_at": utc_now(), "as_of": END})
    failures, degraded = [], []
    for k, f in FETCHERS.items():
        print(f"抓取 {k} …")
        tier = "core" if k in CORE_SOURCES else "optional"
        try:
            df = f()
            _, latest = validate_source(k, df)
            status["sources"][k] = {"status": "ok", "tier": tier, "rows": len(df), "latest_observation": latest}
            if k == "trust":
                status["sources"][k]["latest_observations"] = {
                    sid: value.date().isoformat()
                    for sid, value in df.assign(date=pd.to_datetime(df["date"])).groupby("series")["date"].max().items()
                }
            print(f"  完成：{len(df)} 行")
            observations = status["sources"][k].get("latest_observations", latest)
            annotate("notice", f"来源成功 {k}", f"{len(df)} 行；最新原始观测：{observations}")
        except Exception as ex:
            status["sources"][k] = {"status": "failed", "tier": tier, "error": str(ex)}
            print(f"  失败：{ex}", file=sys.stderr)
            if tier == "core":
                failures.append(f"{k}: {ex}")
                annotate("error", f"核心来源失败 {k}", ex)
            else:
                degraded.append(f"{k}: {ex}")
                annotate("warning", f"可选来源失败 {k}（降级运行）", ex)
        write_json(RAW / "fetch_status.json", status)
    status.update(status="failed" if failures else ("degraded" if degraded else "ok"),
                  completed_at=utc_now(), degraded=degraded)
    write_json(RAW / "fetch_status.json", status)
    if failures:
        message = "核心来源抓取失败；未发布新的真实 CQI。 " + "; ".join(failures)
        write_json(OUT / "build_status.json", {"status": "failed", "generated_at": utc_now(), "as_of": END, "error": message})
        raise PipelineError(message)
    return status


def cmd_probe(_):
    tests = {
        "sofr": lambda: get(URL["sofr"].format(s="2026-09-01", e="2026-09-05")),
        "admin_rate": lambda: fred_frame("IORB", START, END).tail(2).astype(str).to_dict("records"),
        "trust": lambda: {sid: fred_frame(sid, START, END).tail(2).astype(str).to_dict("records") for sid in ("DGS10", "DTWEXBGS")},
        "repo_ops": lambda: get(URL["repo_ops"].format(s="2025-12-29", e="2025-12-31")),
        "auctions": lambda: get(URL["auctions"].format(s="2026-08-10", e="2026-08-14")),
        "buybacks": lambda: get(URL["buybacks"].replace("page[size]=10000", "page[size]=2")),
        "cftc": lambda: get(cftc_query(**{"$limit": 2, "$order": "report_date_as_yyyy_mm_dd DESC,market_and_exchange_names"})),
    }
    failures = []
    for k, t in tests.items():
        try:
            r = t()
            txt = r.decode("utf-8", "ignore") if isinstance(r, bytes) else json.dumps(r, ensure_ascii=False)[:600]
            print(f"[通过] {k}\n  {txt[:600]}\n")
        except Exception as ex:
            failures.append(k)
            print(f"[失败] {k}：{ex}\n")
    if failures:
        raise PipelineError("接口探测失败：" + ", ".join(failures))


# ───────────────────────── 成分计算 ─────────────────────────
def read(name):
    p = RAW / f"{name}.csv"
    if not p.exists():
        raise PipelineError(f"missing raw data: {p.name}; run fetch")
    try:
        df = pd.read_csv(p)
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as ex:
        raise PipelineError(f"{p.name}: empty or malformed CSV") from ex
    source = "cftc" if name == "cftc_tff" else name
    return validate_source(source, df)[0]


def comp_repo_pressure(bdays):
    sofr, adm = read("sofr"), read("admin_rate")
    if sofr is None or adm is None:
        return None
    sofr["date"] = pd.to_datetime(sofr["date"])
    adm["date"] = pd.to_datetime(adm["date"])
    adm["rate"] = num(adm["rate"])
    ioer = adm[adm.series == "IOER"].set_index("date")["rate"]
    iorb = adm[adm.series == "IORB"].set_index("date")["rate"]
    admin = pd.concat([ioer[ioer.index < "2021-07-29"], iorb[iorb.index >= "2021-07-29"]])
    s = sofr.set_index("date")["sofr"].astype(float).reindex(bdays).ffill(limit=5)
    a = admin.reindex(bdays).ffill(limit=5)
    return (s - a) * 100            # 基点；越高越差


def comp_srp_usage(bdays):
    ops = read("repo_ops")
    if ops is None:
        return None
    ops = ops[ops["operationType"].astype(str).str.lower().eq("repo")].copy()
    ops["date"] = pd.to_datetime(ops["operationDate"])
    daily = num(ops["totalAmtAccepted"]).groupby(ops["date"]).sum(min_count=1) / 1e9   # 十亿美元
    s = daily.reindex(bdays)
    observed_span = (s.index >= daily.index.min()) & (s.index <= daily.index.max())
    # No operation inside a successfully fetched history may be zero; absent future data is unknown.
    s.loc[observed_span] = s.loc[observed_span].fillna(0.0)
    return s.rolling(5, min_periods=1).mean().where(observed_span)            # 平滑单日噪声


def comp_auction_absorption(bdays):
    au = read("auctions")
    if au is None:
        return None
    au = au[au["securityType"].isin(["Note", "Bond"])].copy()
    au["date"] = pd.to_datetime(au["auctionDate"])
    au["btc"] = num(au["bidToCoverRatio"])
    denom = num(au["competitiveAccepted"]) if "competitiveAccepted" in au else num(au["totalAccepted"])
    au["pd_share"] = num(au["primaryDealerAccepted"]) / denom.replace(0, np.nan)
    au = au.dropna(subset=["btc", "pd_share"]).sort_values("date")
    g = au.groupby("securityTerm")
    au["pd_dev"] = au["pd_share"] - g["pd_share"].transform(lambda x: x.shift(1).rolling(6, min_periods=3).mean())
    au["btc_dev"] = g["btc"].transform(lambda x: x.shift(1).rolling(6, min_periods=3).mean()) - au["btc"]
    # 两项先各自在全样本标准化，再平均；越高越差（交易商被动承接多、认购倍数低）
    score = ((au["pd_dev"] - au["pd_dev"].mean()) / au["pd_dev"].std()
             + (au["btc_dev"] - au["btc_dev"].mean()) / au["btc_dev"].std()) / 2
    ev = score.groupby(au["date"]).mean()
    return ev.reindex(bdays).ffill(limit=10)     # 事件型：两次拍卖之间沿用上一读数


def comp_buyback_pressure(bdays):
    bb = read("buybacks")
    if bb is None or bb.empty:
        return None
    date_col = next(c for c in bb.columns if "operation_date" in c)
    off = next((c for c in bb.columns if "offered" in c and "total" in c), None)
    acc = next((c for c in bb.columns if "accepted" in c and "total" in c), None)
    if off is None or acc is None:
        print("  回购数据未找到报价/接受字段，请用 probe 查看字段名后修改 comp_buyback_pressure")
        return None
    if "security_type" in bb:                               # 只看名义附息债
        bb = bb[~bb["security_type"].astype(str).str.contains("TIPS", case=False)]
    bb["date"] = pd.to_datetime(bb[date_col])
    ratio = (num(bb[off]) / num(bb[acc])).replace([np.inf, -np.inf], np.nan)
    ev = ratio.groupby(bb["date"]).mean()
    return ev.reindex(bdays).ffill(limit=15)


def comp_basis_unwind(bdays):
    cf = read("cftc_tff")
    if cf is None:
        return None
    m = cf["market"].astype(str).str.upper()
    is_ust = (m.str.contains("UST|TREASURY") & m.str.contains("NOTE|BOND") & ~m.str.contains("MICRO|SWAP"))
    cf = cf[is_ust].copy()
    cf["date"] = pd.to_datetime(cf["date"])
    weekly = num(cf["lev_short"]).groupby(cf["date"]).sum()
    # 平仓速度：4 周空头降幅（正值 = 快速平仓 = 越差）
    unwind = -(weekly.pct_change(4, fill_method=None)).replace([np.inf, -np.inf], np.nan)
    return unwind.reindex(bdays).ffill(limit=7)


def comp_trust_divergence(bdays):
    """信任背离：10 年期收益率 5 日上行且广义美元指数 5 日下跌时，取上行幅度（基点），否则为 0。
    事件级验证发现 2025 年 4 月的压力体现为“收益率上行而美元下跌”，使用压力类成分捕捉不到。"""
    t = read("trust")
    y = t.loc[t["series"].eq("DGS10")].set_index("date")["value"]
    usd = t.loc[t["series"].eq("DTWEXBGS")].set_index("date")["value"]
    y.index = pd.to_datetime(y.index)
    usd.index = pd.to_datetime(usd.index)
    y = y.reindex(bdays.union(y.index).sort_values()).ffill(limit=5).reindex(bdays)
    usd = usd.reindex(bdays.union(usd.index).sort_values()).ffill(limit=5).reindex(bdays)
    dy = y.diff(5) * 100
    dusd = usd.pct_change(5, fill_method=None)
    out = dy.where((dy > 0) & (dusd < 0), 0.0)
    return out.where(dy.notna() & dusd.notna())


COMPONENTS = {
    "回购压力": comp_repo_pressure,
    "央行回购使用量": comp_srp_usage,
    "拍卖吸收度": comp_auction_absorption,
    "回购卖压": comp_buyback_pressure,
    "基差平仓速度": comp_basis_unwind,
    "信任背离": comp_trust_divergence,
}


def build(raw_components=None, ok_sources=None):
    bdays = pd.bdate_range(START, END)
    source_errors = {}
    if raw_components is None:
        if ok_sources is not None and not set(CORE_SOURCES).issubset(ok_sources):
            raise PipelineError("cannot build without every core source")
        raw_components = {}
        for k, f in COMPONENTS.items():
            if ok_sources is not None and not all(src in ok_sources for src in COMPONENT_SOURCES[k]):
                print(f"  成分缺失（来源失败，不使用旧数据）：{k}")
                raw_components[k] = None
                continue
            try:
                s = f(bdays)
                if s is None:
                    raise PipelineError(f"{k}: no component input")
            except PipelineError as ex:
                sources = COMPONENT_SOURCES[k]
                if any(src in CORE_SOURCES for src in sources):
                    raise
                # Revalidation can fail after fetch (e.g. stale/corrupt local input).
                # Exclude the complete failed component; never reuse its old values.
                for source in sources:
                    source_errors[source] = str(ex)
                annotate("warning", f"可选成分失败 {k}（降级运行）", ex)
                s = None
            raw_components[k] = s
    comp = pd.DataFrame({k: v for k, v in raw_components.items() if v is not None}, index=bdays)
    comp = comp.replace([np.inf, -np.inf], np.nan)
    z = comp.apply(rolling_z)
    n_avail = z.notna().sum(axis=1)
    cqi = z.mean(axis=1, skipna=True).where(n_avail >= MIN_COMPONENTS)
    # 断裂规则：任一成分单日变动超过 2 个标准差
    dz = z.diff()
    dsd = dz.rolling(Z_WINDOW, min_periods=Z_MIN).std()
    brk = (dz.abs() > BREAK_SIGMA * dsd)
    out = pd.DataFrame({"cqi": cqi, "成分数": n_avail,
                        "断裂": brk.any(axis=1),
                        "断裂成分": brk.apply(lambda r: "、".join(r.index[r]), axis=1)})
    out["灯"] = np.select([out.cqi >= RED, out.cqi >= YELLOW], ["红", "黄"], default="")
    out.loc[out["cqi"].isna(), "灯"] = "数据缺失"
    out["data_status"] = np.where(out["cqi"].notna(), "ok", "insufficient_components")
    out = out.join(z.add_prefix("z_")).join(comp.add_prefix("raw_"))
    out.attrs["source_errors"] = source_errors
    return out


def check_fetch_status():
    p = RAW / "fetch_status.json"
    if not p.exists():
        raise PipelineError("no verified fetch manifest; run fetch before build")
    try:
        status = json.loads(p.read_text(encoding="utf-8"))
        completed = pd.Timestamp(status["completed_at"])
    except (ValueError, KeyError, TypeError) as ex:
        raise PipelineError("invalid or incomplete fetch manifest; run fetch") from ex
    srcs = status.get("sources", {})
    if status.get("status") not in ("ok", "degraded") or any(srcs.get(k, {}).get("status") != "ok" for k in CORE_SOURCES):
        raise PipelineError("latest fetch did not succeed for every core source; run fetch")
    if any(k not in srcs for k in FETCHERS):
        raise PipelineError("invalid or incomplete fetch manifest; run fetch")
    if any(not isinstance(srcs[k], dict) or srcs[k].get("status") not in ("ok", "failed") for k in FETCHERS):
        raise PipelineError("invalid or incomplete source status; run fetch")
    failed_optional = [k for k in OPTIONAL_SOURCES if srcs[k]["status"] == "failed"]
    if ((status["status"] == "degraded") != bool(failed_optional)
            or any(not srcs[k].get("error") for k in failed_optional)):
        raise PipelineError("inconsistent fetch degradation status; run fetch")
    now = pd.Timestamp.now(tz="UTC")
    if pd.isna(completed) or completed.tzinfo is None or completed > now + pd.Timedelta(minutes=5) or now - completed > pd.Timedelta(days=3):
        raise PipelineError("fetch manifest is stale or has an invalid timestamp; run fetch")
    return status


def cmd_build(_):
    OUT.mkdir(parents=True, exist_ok=True)
    status = {"status": "running", "generated_at": utc_now(), "as_of": END, "data_kind": "real"}
    write_json(OUT / "build_status.json", status)
    try:
        fetch_status = check_fetch_status()
        ok_sources = {k for k, v in fetch_status.get("sources", {}).items() if v.get("status") == "ok"}
        out = build(ok_sources=ok_sources)
        if out.empty or out["cqi"].dropna().empty:
            raise PipelineError("no usable CQI: insufficient components or standardization history")
        if pd.isna(out["cqi"].iloc[-1]) or not np.isfinite(out["cqi"].iloc[-1]):
            raise PipelineError("latest business day has no usable CQI; refusing to publish an old reading as current")
        if out["成分数"].iloc[-1] < MIN_COMPONENTS:
            raise PipelineError("latest business day has fewer than the minimum components")
        for source, error in out.attrs.get("source_errors", {}).items():
            ok_sources.discard(source)
            fetch_status["sources"][source] = {"status": "failed", "tier": "optional", "stage": "build", "error": error}
        out["data_kind"] = "real"
        missing = [k for k, srcs in COMPONENT_SOURCES.items() if not all(x in ok_sources for x in srcs)]
        state = "degraded" if missing else "ok"
        out.loc[out["cqi"].notna(), "data_status"] = state
        out["run_status"] = state
        out["missing_components"] = "、".join(missing)
        write_csv(OUT / "cqi_daily.csv", out, encoding="utf-8-sig", index_label="date")
        status.update(status=state, generated_at=utc_now(), latest_cqi_date=out.index[-1].date().isoformat(),
                      component_count=int(out["成分数"].iloc[-1]), missing_components=missing,
                      sources=fetch_status["sources"])
        write_json(OUT / "build_status.json", status)
        r = out.iloc[-1]
        print(f"最新读数 {out.index[-1].date()}：CQI = {r.cqi:.2f}  灯：{r['灯'] or '无'}  成分数：{r['成分数']}")
        if missing:
            annotate("warning", "CQI 降级运行", "缺失成分：" + "、".join(missing) + "；其余成分照常合成")
        annotate("notice", "CQI 最新读数", f"{out.index[-1].date()} CQI={r.cqi:.2f} 灯={r['灯'] or '无'} 成分数={r['成分数']}")
        return out
    except Exception as ex:
        status.update(status="failed", generated_at=utc_now(), error=str(ex))
        write_json(OUT / "build_status.json", status)
        raise


# ───────────────────────── 回测 ─────────────────────────
def backtest(out, missing_components=None, degraded=False):
    missing_components = missing_components or []
    degraded = degraded or bool(missing_components)
    lines = ["# 抵押品质量指数（公开版）回测报告", "",
             f"阈值：黄灯 ≥ {YELLOW}，红灯 ≥ {RED}；标准化窗口 {Z_WINDOW} 个交易日。", "",
             "## 压力事件", "", "| 事件 | 窗口内最高 CQI | 灯 | 首次亮灯日 | 主要贡献成分 |", "| --- | --- | --- | --- | --- |"]
    zcols = [c for c in out.columns if c.startswith("z_")]
    passed_events = {}
    for name, d in EVENTS.items():
        d = pd.Timestamp(d)
        w = out.loc[d - pd.offsets.BDay(5): d + pd.offsets.BDay(20)]
        if w["cqi"].dropna().empty:
            lines.append(f"| {name} | 无读数 | — | — | — |")
            passed_events[name] = False
            continue
        mx = w["cqi"].max()
        lamp = "红" if mx >= RED else ("黄" if mx >= YELLOW else "无")
        first = w.index[w["cqi"] >= YELLOW]
        first = first[0].date().isoformat() if len(first) else "—"
        top = w[zcols].max().sort_values(ascending=False).head(2)
        lines.append(f"| {name} | {mx:.2f} | {lamp} | {first} | "
                     + "、".join(f"{k[2:]}({v:.1f})" for k, v in top.items()) + " |")
        passed_events[name] = mx >= YELLOW
    lines += ["", "## 平静期误报", "", "| 时期 | 亮黄灯及以上的天数占比 | 是否合格 |", "| --- | --- | --- |"]
    calm_ok = True
    for name, (a, b) in CALM.items():
        w = out.loc[a:b, "cqi"].dropna()
        share = float((w >= YELLOW).mean()) if len(w) else float("nan")
        ok = bool(share <= CALM_MAX_SHARE) if len(w) else False
        calm_ok &= ok
        lines.append(f"| {name} | {share:.1%} | {'合格' if ok else '不合格'} |")
    must_ok = all(passed_events.get(k, False) for k in MUST_FLAG)
    diagnostic_verdict = "通过" if (must_ok and calm_ok) else "未通过"
    verdict = "不作正式判定（降级运行）" if degraded else diagnostic_verdict
    conclusion = (f"**降级回测：现有成分指标{diagnostic_verdict}。**不能作为第二阶段关口的正式依据。" if degraded else
                  f"**回测{verdict}。**" + ("可提交卷六关口。" if verdict == "通过" else "请调整权重或阈值后重跑，并在卷六登记调整。"))
    lines += ["", "## 结论", "",
              f"必须识别的三次事件：{'全部亮灯' if must_ok else '未全部亮灯'}；平静期误报：{'合格' if calm_ok else '不合格'}。",
              conclusion, ""]
    if degraded:
        missing = "、".join(missing_components) or "来源异常（详见生成状态）"
        lines = ["> **降级运行**：本次缺失成分 " + missing
                 + "（来源失败，未使用旧数据）。回测结论仅对现有成分有效，不能作为第二阶段关口的正式依据。", ""] + lines
    return "\n".join(lines), verdict


def cmd_backtest(_):
    p = OUT / "cqi_daily.csv"
    if not p.exists():
        raise PipelineError("missing cqi_daily.csv; run build before backtest")
    out = pd.read_csv(p, index_col=0, parse_dates=True)
    if "data_kind" not in out or not out["data_kind"].eq("real").all():
        raise PipelineError("backtest requires verified real CQI data; use demo for synthetic data")
    status_path = OUT / "build_status.json"
    build_status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.exists() else {}
    if build_status.get("status") not in ("ok", "degraded"):
        raise PipelineError("latest real build did not succeed; run build before backtest")
    degraded = build_status["status"] == "degraded"
    if not degraded and "data_status" in out and out["data_status"].eq("degraded").any():
        raise PipelineError("inconsistent build degradation status; run build before backtest")
    md, verdict = backtest(out, missing_components=build_status.get("missing_components"), degraded=degraded)
    annotate("notice", "CQI 回测结论", verdict)
    REP.mkdir(parents=True, exist_ok=True)
    atomic_write(REP / "backtest.md", lambda p: p.write_text(md, encoding="utf-8"))
    print(md)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(11, 4))
        ax.plot(out.index, out["cqi"], lw=1)
        ax.axhline(YELLOW, ls="--", lw=0.8)
        ax.axhline(RED, ls="--", lw=0.8)
        for d in EVENTS.values():
            ax.axvline(pd.Timestamp(d), lw=0.5, alpha=0.4)
        ax.set_title("Collateral Quality Index (public version)" + (" - DEGRADED" if degraded else ""))
        fig.tight_layout()
        fig.savefig(REP / "cqi.png", dpi=150)
        plt.close(fig)
    except Exception as ex:
        raise PipelineError(f"backtest plot generation failed: {ex}") from ex


# ───────────────────────── 演示（合成数据） ─────────────────────────
def cmd_demo(_):
    """不联网：生成带压力尖峰的合成成分，检查 build 与 backtest 能否跑通。"""
    rng = np.random.default_rng(7)
    bdays = pd.bdate_range(START, END)
    n = len(bdays)
    comps = {k: pd.Series(rng.normal(0, 1, n).cumsum() * 0.05 + rng.normal(0, 1, n), index=bdays)
             for k in COMPONENTS}
    for d in EVENTS.values():
        i = bdays.searchsorted(pd.Timestamp(d))
        for k in comps:
            comps[k].iloc[i:i + 10] += rng.uniform(4, 8)
    out = build(comps)
    OUT.mkdir(parents=True, exist_ok=True)
    out["data_kind"] = "synthetic"
    write_csv(OUT / "demo" / "cqi_daily.csv", out, encoding="utf-8-sig", index_label="date")
    md, verdict = backtest(out)
    print(md)
    print(f"[演示] 管线跑通。合成数据的回测结论（{verdict}）不代表真实读数。")


def main(argv=None):
    ap = argparse.ArgumentParser(description="抵押品质量指数（公开版）")
    ap.add_argument("cmd", choices=["probe", "fetch", "build", "backtest", "all", "demo"])
    a = ap.parse_args(argv)
    try:
        if a.cmd in {"fetch", "all", "probe"}:
            require_api_key()
        if a.cmd == "all":
            cmd_fetch(a); cmd_build(a); cmd_backtest(a)
        else:
            {"probe": cmd_probe, "fetch": cmd_fetch, "build": cmd_build,
             "backtest": cmd_backtest, "demo": cmd_demo}[a.cmd](a)
    except Exception as ex:
        print(f"[失败] {ex}", file=sys.stderr)
        annotate("error", f"CQI {a.cmd} 失败", ex)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
