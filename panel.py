#!/usr/bin/env python3
"""
判决面板 · 全链管线
HQ Research · 美国金融与资本研究中心 · V0.1（2026-10-01）

把《判决面板 · 全链总表 V1》变成每日可生成的数据版：
  - 日频读数：全部来自 FRED 公开 CSV，无需密钥，自动抓取
  - 低频读数：TIC、COFER、融资融券、远期市盈率等，用 manual/ 下的录入表，每月或每季照官方发布填一行
  - 抵押品质量指数：读取 cqi.py 的输出 data/cqi_daily.csv（若存在）

子命令：
  fetch    抓取 FRED 日频数据到 data/raw/fred/
  build    计算三边相变信号、各组读数与灯号，输出 data/panel_daily.csv 与 reports/panel_latest.md
  all      fetch + build
  demo     合成数据跑通（不联网）
"""
import argparse, io, json, time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
RAW = ROOT / "data" / "raw" / "fred"
MAN = ROOT / "manual"
OUT = ROOT / "data"
REP = ROOT / "reports"
START = "2015-01-01"
END = date.today().isoformat()
FRED = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}"

# ───────────── FRED 序列（名称：序列代码）─────────────
SERIES = {
    "y30": "DGS30",              # 30 年期名义收益率
    "y10": "DGS10",              # 10 年期名义收益率
    "y2": "DGS2",                # 2 年期名义收益率
    "real10": "DFII10",          # 10 年期 TIPS 实际收益率
    "tp10": "THREEFYTP10",       # Kim-Wright 10 年期期限溢价
    "be10": "T10YIE",            # 10 年期盈亏平衡通胀
    "sofr": "SOFR",
    "iorb": "IORB",
    "mort30": "MORTGAGE30US",    # 30 年固定房贷利率（周）
    "ig_oas": "BAMLC0A0CM",      # 投资级期权调整利差（百分点）
    "hy_oas": "BAMLH0A0HYM2",    # 高收益期权调整利差（百分点）
    "brent": "DCOILBRENTEU",     # 布伦特原油
    "usd_broad": "DTWEXBGS",     # 美联储广义美元指数（公开替代美元指数）
    "spx": "SP500",              # 标普 500（FRED 仅提供近 10 年）
    "fed_tsy": "TREAST",         # 美联储持有国债（周，百万美元）
    "labor_share": "PRS85006173",  # 非农企业部门劳动份额（季）
    "lab_prod": "OPHNFB",        # 非农企业劳动生产率（季）
    "ulc": "ULCNFB",             # 非农企业单位劳动成本（季）
}

# ───────────── 阈值（继承美债体系卷五，待卷六回测校准）─────────────
THRESH = {  # 名称: (黄, 红, 方向)  方向 1 = 越高越差
    "y30": (5.80, 6.00, 1),
    "real10": (2.80, 3.00, 1),
    "tp10": (1.20, 1.50, 1),
    "mort30": (7.25, 7.50, 1),
    "ig_oas": (1.10, 1.50, 1),
    "hy_oas": (3.50, 4.50, 1),
    "brent": (110, 130, 1),
}
DUR10 = 8.5          # 10 年期国债近似久期，用于由收益率变动近似债券回报
CORR_WIN = 63        # 股债相关窗口
SIG2_DAYS = 20       # 相关性连续为正多少个交易日视为“持续”
SIG3_WIN = 126       # 同跌统计窗口（约 6 个月）


def get_csv(url, retries=3):
    from urllib.request import Request, urlopen
    for i in range(retries):
        try:
            with urlopen(Request(url, headers={"User-Agent": "HQ-Research-Panel/0.1"}), timeout=60) as response:
                return pd.read_csv(io.BytesIO(response.read()))
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(2 * (i + 1))


def cmd_fetch(_):
    RAW.mkdir(parents=True, exist_ok=True)
    failures = []
    for name, sid in SERIES.items():
        try:
            df = get_csv(FRED.format(sid=sid) + f"&cosd={START}&coed={END}")
            if df.shape[1] != 2:
                raise ValueError("FRED 响应必须含日期和一个序列")
            df.columns = ["date", name]
            df["date"] = pd.to_datetime(df["date"], errors="raise")
            df[name] = pd.to_numeric(df[name], errors="coerce")
            if df[name].notna().sum() == 0:
                raise ValueError("FRED 序列没有有效数值")
            df.to_csv(RAW / f"{name}.csv", index=False)
            print(f"[完成] {name:12s} {sid:14s} {len(df)} 行")
        except Exception as ex:
            failures.append(name)
            print(f"[失败] {name:12s} {sid:14s} {ex}")
    if failures:
        raise RuntimeError("FRED 抓取未完成，停止生成，避免沿用旧数据：" + ", ".join(failures))


def load_fred():
    bdays = pd.bdate_range(START, END)
    cols = {}
    observed = {}
    for name in SERIES:
        p = RAW / f"{name}.csv"
        if not p.exists():
            continue
        d = pd.read_csv(p)
        d["date"] = pd.to_datetime(d["date"])
        s = pd.to_numeric(d[name], errors="coerce")
        s.index = d["date"]
        s = s[~s.index.duplicated(keep="last")].sort_index()
        s = s.loc[:END]
        valid = s.dropna()
        if valid.empty:
            raise ValueError(f"{name} 无有效观测")
        observed[name] = valid.index[-1]
        max_age = 200 if name in {"labor_share", "lab_prod", "ulc"} else (21 if name in {"mort30", "fed_tsy"} else 10)
        if (pd.Timestamp(END) - observed[name]).days > max_age:
            raise ValueError(f"{name} 最新观测 {observed[name].date()} 已过期")
        cols[name] = s.reindex(bdays.union(s.index)).sort_index().ffill().reindex(bdays)
    missing = set(SERIES) - set(cols)
    if missing:
        raise ValueError("FRED 原始数据缺失：" + ", ".join(sorted(missing)))
    frame = pd.DataFrame(cols, index=bdays)
    frame.attrs["observed"] = observed
    return frame


def read_manual(name):
    p = MAN / f"{name}.csv"
    if not p.exists():
        return None
    d = pd.read_csv(p, comment="#")
    d["date"] = pd.to_datetime(d["date"])
    return d.sort_values("date")


# ───────────── 三边相变信号 ─────────────
def edge_signals(f):
    out = pd.DataFrame(index=f.index)
    # ① 收益率上行而美元走弱：20 日 10 年期上行 ≥ 15bp 且广义美元指数 20 日下跌
    if {"y10", "usd_broad"} <= set(f):
        dy = f["y10"].diff(20) * 100
        dusd = f["usd_broad"].pct_change(20)
        out["sig1_day"] = (dy >= 15) & (dusd < 0)
        out["sig1_count60"] = out["sig1_day"].rolling(60).sum()
        out["sig1"] = out["sig1_count60"] >= 10           # 近 60 日中至少 10 日背离
    # ② 股债相关性由负转正并持续：标普日回报与 10 年期近似回报的 63 日相关
    if {"spx", "y10"} <= set(f):
        r_eq = f["spx"].pct_change()
        r_bd = -DUR10 * f["y10"].diff() / 100
        corr = r_eq.rolling(CORR_WIN).corr(r_bd)
        out["corr63"] = corr
        pos = (corr > 0).astype(int)
        run = pos.groupby((pos != pos.shift()).cumsum()).cumsum() * pos
        out["corr_pos_run"] = run
        out["sig2"] = run >= SIG2_DAYS
    # ③ 美股与美元同跌成为常态：标普跌超 1% 的日子里美元同时下跌的比例
    if {"spx", "usd_broad"} <= set(f):
        r_eq = f["spx"].pct_change()
        r_usd = f["usd_broad"].pct_change()
        big_down = r_eq < -0.01
        both = big_down & (r_usd < 0)
        n_big = big_down.rolling(SIG3_WIN).sum()
        out["sig3_share"] = both.rolling(SIG3_WIN).sum() / n_big.replace(0, np.nan)
        out["sig3"] = (out["sig3_share"] > 0.5) & (n_big >= 5)
    return out


def lamp(value, name):
    if name not in THRESH or pd.isna(value):
        return ""
    y, r, d = THRESH[name]
    v = value * d
    return "红" if v >= r * d else ("黄" if v >= y * d else "")


# ───────────── 合成与报告 ─────────────
def build(f):
    sig = edge_signals(f)
    panel = f.join(sig)
    if "sofr" in f and "iorb" in f:
        panel["sofr_minus_iorb_bp"] = (f["sofr"] - f["iorb"]) * 100
    cqi_p = OUT / "cqi_daily.csv"
    if cqi_p.exists():
        status_path = OUT / "build_status.json"
        if not status_path.exists() or json.loads(status_path.read_text(encoding="utf-8")).get("status") != "ok":
            raise ValueError("CQI 最新构建未通过；请先运行 cqi.py all，不能沿用旧结果")
        cqi = pd.read_csv(cqi_p, index_col=0, parse_dates=True)
        if "data_kind" not in cqi or not cqi["data_kind"].eq("real").all():
            raise ValueError("CQI 缺少真实数据标记或为合成数据；请先运行 cqi.py all")
        valid = cqi["cqi"].dropna()
        if valid.empty or (pd.Timestamp(END) - valid.index[-1]).days > 7:
            raise ValueError("CQI 无有效近期读数")
        cqi = cqi[["cqi", "灯"]]
        panel = panel.join(cqi.rename(columns={"灯": "cqi_lamp"}))
    panel.attrs["observed"] = f.attrs.get("observed", {})
    return panel


def last_valid(s):
    s = s.dropna()
    return (s.index[-1], s.iloc[-1]) if len(s) else (None, np.nan)


def fmt(v, nd=2, unit=""):
    return "—" if pd.isna(v) else f"{v:.{nd}f}{unit}"


def report(panel):
    observed = panel.attrs.get("observed", {})
    def observation_date(key, fallback):
        if key == "sofr_minus_iorb_bp":
            dates = [observed[k] for k in ("sofr", "iorb") if k in observed]
            return min(dates) if dates else fallback
        return observed.get(key, fallback)
    L = ["# 判决面板 · 全链数据版", "",
         f"生成时间：{date.today().isoformat()}；日频读数截至各序列最新可得日。阈值继承美债体系卷五，待卷六回测校准。", ""]

    # 三边
    L += ["## 三边相变信号", "", "| 边 | 信号 | 当期读数 | 状态 |", "| --- | --- | --- | --- |"]
    d, v = last_valid(panel.get("sig1_count60", pd.Series(dtype=float)))
    L.append(f"| 美元—美债 | 收益率上行而美元走弱 | 近 60 日背离 {fmt(v, 0)} 天 | {'待接入' if pd.isna(v) else ('已亮' if v >= 10 else '未亮')} |")
    d, c = last_valid(panel.get("corr63", pd.Series(dtype=float)))
    _, run = last_valid(panel.get("corr_pos_run", pd.Series(dtype=float)))
    L.append(f"| 美债—美股 | 股债相关性由负转正并持续 | 63 日相关 {fmt(c)}；连续为正 {fmt(run, 0)} 日 | {'待接入' if pd.isna(c) or pd.isna(run) else ('已亮' if run >= SIG2_DAYS else '未亮')} |")
    d, sh = last_valid(panel.get("sig3_share", pd.Series(dtype=float)))
    L.append(f"| 美股—美元 | 美股与美元同跌成为常态 | 大跌日美元同跌占比 {fmt(sh * 100 if not pd.isna(sh) else sh, 0, '%')} | "
             f"{'已亮' if bool(panel['sig3'].dropna().iloc[-1]) else '未亮'} |" if "sig3" in panel and panel["sig3"].notna().any()
             else "| 美股—美元 | 美股与美元同跌成为常态 | — | 待接入 |")
    L.append("")

    # 美债组
    L += ["## 美债组", "", "| 读数 | 当期 | 日期 | 灯 |", "| --- | --- | --- | --- |"]
    for k, label, unit in [("y30", "30 年期收益率", "%"), ("y10", "10 年期收益率", "%"), ("y2", "2 年期收益率", "%"),
                           ("real10", "10 年期 TIPS 实际收益率", "%"), ("tp10", "Kim-Wright 期限溢价", "%"),
                           ("be10", "10 年期盈亏平衡通胀", "%"), ("sofr_minus_iorb_bp", "SOFR 减准备金利率", "bp"),
                           ("mort30", "30 年固定房贷利率", "%"), ("fed_tsy", "美联储持有国债", " 百万美元"),
                           ("cqi", "抵押品质量指数（公开版）", "")]:
        if k in panel:
            d, v = last_valid(panel[k])
            lp = lamp(v, k)
            if k == "cqi" and d is not None and "cqi_lamp" in panel:
                lp = panel.loc[d, "cqi_lamp"]
                lp = "" if pd.isna(lp) else lp
            L.append(f"| {label} | {fmt(v, 0 if k == 'fed_tsy' else 2, unit)} | {observation_date(k, d).date() if d is not None else '—'} | {lp or '—'} |")
    L.append("")

    # 美股组
    L += ["## 美股组", "", "| 读数 | 当期 | 日期 | 灯 |", "| --- | --- | --- | --- |"]
    for k, label, unit in [("spx", "标普 500", ""), ("ig_oas", "投资级利差", " 个百分点"),
                           ("hy_oas", "高收益利差", " 个百分点"), ("brent", "布伦特原油", " 美元")]:
        if k in panel:
            d, v = last_valid(panel[k])
            L.append(f"| {label} | {fmt(v, 2, unit)} | {observation_date(k, d).date() if d is not None else '—'} | {lamp(v, k) or '—'} |")
    val = read_manual("valuation")
    if val is not None and "real10" in panel:
        r = val.dropna(subset=["fwd_pe"]).iloc[-1]
        ey = 100 / r.fwd_pe
        _, real = last_valid(panel["real10"])
        _, nom = last_valid(panel["y10"])
        L.append(f"| 远期市盈率（录入） | {r.fwd_pe:.2f} | {r.date.date()} | — |")
        L.append(f"| 股权风险溢价：名义口径 | {ey - nom:.2f} 个百分点 | 自算 | {'亮' if ey - nom < 0 else '—'} |")
        L.append(f"| 股权风险溢价：实际口径 | {ey - real:.2f} 个百分点 | 自算 | — |")
    mon = read_manual("monthly")
    if mon is not None and mon["margin_debt_tn"].notna().any():
        r = mon.dropna(subset=["margin_debt_tn"]).iloc[-1]
        L.append(f"| 融资融券余额（录入） | {r.margin_debt_tn:.3f} 万亿美元 | {r.date.date()} | — |")
    L.append("")

    # 美元组
    L += ["## 美元组", "", "| 读数 | 当期 | 日期 |", "| --- | --- | --- |"]
    if "usd_broad" in panel:
        d, v = last_valid(panel["usd_broad"])
        L.append(f"| 广义美元指数 | {fmt(v)} | {observation_date('usd_broad', d).date() if d is not None else '—'} |")
    q = read_manual("quarterly")
    if q is not None:
        r = q.iloc[-1]
        L.append(f"| 储备中的美元份额（录入） | {r.cofer_usd_share:.2f}% | {r.date.date()} |")
        L.append(f"| 境外非银行美元信贷（录入） | {r.bis_usd_credit_tn:.1f} 万亿美元 | {r.date.date()} |")
    L.append("")

    # 回流组
    L += ["## 回流组（TIC，录入）", ""]
    tm = read_manual("tic_monthly")
    seed = read_manual("tic_12m_seed")
    if tm is not None and len(tm) >= 12:
        last12 = tm.tail(12)
        eq, corp, tsy, tot = (last12[c].sum() for c in ("eq_net", "corp_net", "tsy_net", "total_lt_net"))
        basis = f"由最近 12 个月录入计算（截至 {tm.date.iloc[-1].date()}）"
    elif seed is not None:
        r = seed.iloc[-1]
        eq, corp, tsy, tot = r.eq_net, r.corp_net, r.tsy_net, r.total_lt_net
        basis = f"取种子值（12 个月截至 {r.date.date()}）；月度录入满 12 个月后自动改为滚动计算"
    else:
        eq = corp = tsy = tot = np.nan
        basis = "无数据"
    slope = eq + corp - tsy
    state = ("待接入" if pd.isna(slope) or pd.isna(tot) else
             "回流加速：斜率为正、总量为正" if slope > 0 and tot > 0 else
             "回流收缩：斜率为正但总量为负" if slope > 0 else "载体倒退回主权资产")
    L += [f"口径：{basis}。单位十亿美元。", "",
          "| 读数 | 当期 |", "| --- | --- |",
          f"| TIC 资产斜率（股票 + 公司债 − 中长期国债） | {fmt(slope, 1)} |",
          f"| 外资净买入美国长期证券 | {fmt(tot, 1)} |",
          f"| 判读 | {state} |", ""]
    if tm is not None and len(tm):
        r = tm.iloc[-1]
        L += [f"最新单月（{r.date.date()}）：长期证券净买入 {r.total_lt_net:.1f}，其中股票 {r.eq_net:.1f}、公司债 {r.corp_net:.1f}、中长期国债 {r.tsy_net:.1f}。", ""]

    # 中心组
    L += ["## 中心组", "", "| 读数 | 当期 | 日期 |", "| --- | --- | --- |"]
    for k, label in [("labor_share", "劳动份额（指数）"), ("lab_prod", "劳动生产率（指数）"), ("ulc", "单位劳动成本（指数）")]:
        if k in panel:
            d, v = last_valid(panel[k])
            L.append(f"| {label} | {fmt(v)} | {observation_date('usd_broad', d).date() if d is not None else '—'} |")
    L += ["", "全要素生产率为年度与季度学术序列，暂不自动抓取，见总表第二节。", ""]
    return "\n".join(L)


def cmd_build(_):
    f = load_fred()
    panel = build(f)
    OUT.mkdir(parents=True, exist_ok=True)
    REP.mkdir(parents=True, exist_ok=True)
    panel.to_csv(OUT / "panel_daily.csv", encoding="utf-8-sig", index_label="date")
    md = report(panel)
    (REP / "panel_latest.md").write_text(md, encoding="utf-8")
    print(md)


def cmd_demo(_):
    """合成数据只写 data/demo/panel，绝不覆盖真实数据。"""
    global RAW, OUT, REP
    RAW, OUT, REP = ROOT / "data/demo/panel/raw", ROOT / "data/demo/panel", ROOT / "data/demo/panel/reports"
    rng = np.random.default_rng(3)
    RAW.mkdir(parents=True, exist_ok=True)
    b = pd.bdate_range(START, END)
    n = len(b)
    walk = lambda s0, sd: s0 + np.cumsum(rng.normal(0, sd, n))
    fake = {
        "y30": walk(3.0, .03), "y10": walk(2.5, .03), "y2": walk(1.5, .03), "real10": walk(.5, .02),
        "tp10": walk(0, .01), "be10": walk(2.2, .01), "sofr": walk(1, .01), "iorb": walk(1, .01),
        "mort30": walk(4, .02), "ig_oas": np.abs(walk(1, .01)), "hy_oas": np.abs(walk(4, .03)),
        "brent": np.abs(walk(60, .8)), "usd_broad": walk(110, .3), "spx": np.exp(walk(np.log(2000), .01)),
        "fed_tsy": walk(4e6, 1e3), "labor_share": walk(100, .05), "lab_prod": walk(100, .05), "ulc": walk(100, .05),
    }
    for k, v in fake.items():
        pd.DataFrame({"date": b.strftime("%Y-%m-%d"), k: v}).to_csv(RAW / f"{k}.csv", index=False)
    cmd_build(_)
    p = REP / "panel_latest.md"
    p.write_text("⚠ 合成演示数据，不代表真实市场。\n\n" + p.read_text(encoding="utf-8"), encoding="utf-8")
    print("\n[演示] 管线跑通。合成数据的读数不代表真实市场。")


def main():
    ap = argparse.ArgumentParser(description="判决面板 · 全链管线")
    ap.add_argument("cmd", choices=["fetch", "build", "all", "demo"])
    a = ap.parse_args()
    if a.cmd == "all":
        cmd_fetch(a); cmd_build(a)
    else:
        {"fetch": cmd_fetch, "build": cmd_build, "demo": cmd_demo}[a.cmd](a)


if __name__ == "__main__":
    main()
