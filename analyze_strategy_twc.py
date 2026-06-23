from __future__ import annotations

import csv
import json
import re
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
OUT = ROOT / "outputs" / "strategy_twc_analysis"

TRADES_SETTLED = ROOT / "polymarket_weather_trades_settled (2).csv"
PERF_EVENT = ROOT / "polymarket_weather_performance_by_event (1).csv"
PERF_CYCLE = ROOT / "polymarket_weather_performance_by_cycle (1).csv"
TWC_RAW = ROOT / "polymarket_weather_twc_raw_wide (2).csv"


def money(x: float) -> str:
    return f"${x:,.2f}"


def pct(x: float) -> str:
    if pd.isna(x):
        return ""
    return f"{x * 100:,.1f}%"


def read_twc_light(path: Path) -> pd.DataFrame:
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
    forecast_cols = [c for c in header if re.fullmatch(r"forecast_p\d{2}", c)]
    observed_cols = [c for c in header if re.fullmatch(r"observed_h\d{2}", c)]
    usecols = [
        "city",
        "kind",
        "station",
        "target_date",
        "cycle_id",
        "observed_at_utc",
        "city_local_time",
        "combined_high",
        "combined_low",
        "combined_point_count",
        "forecast_point_count",
        "observed_point_count",
        "first_valid_time_local",
        "last_valid_time_local",
    ] + forecast_cols + observed_cols

    df = pd.read_csv(path, usecols=[c for c in usecols if c in header], low_memory=False)
    df["observed_at_utc"] = pd.to_datetime(df["observed_at_utc"], errors="coerce", utc=True)
    for col in forecast_cols + observed_cols + ["combined_high", "combined_low"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["forecast_high_calc"] = df[forecast_cols].max(axis=1, skipna=True)
    df["forecast_low_calc"] = df[forecast_cols].min(axis=1, skipna=True)
    df["observed_high_calc"] = df[observed_cols].max(axis=1, skipna=True)
    df["observed_low_calc"] = df[observed_cols].min(axis=1, skipna=True)
    df["forecast_extreme"] = df["forecast_high_calc"].where(
        df["kind"].eq("Highest"), df["forecast_low_calc"]
    )
    df["observed_extreme"] = df["observed_high_calc"].where(
        df["kind"].eq("Highest"), df["observed_low_calc"]
    )
    df = df.sort_values(["city", "kind", "observed_at_utc", "cycle_id"])
    df["drift_from_prev"] = df.groupby(["city", "kind"])["forecast_extreme"].diff()
    df["drift_from_first"] = df["forecast_extreme"] - df.groupby(["city", "kind"])[
        "forecast_extreme"
    ].transform("first")
    return df


def summarize_pnl() -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    trades = pd.read_csv(TRADES_SETTLED)
    event = pd.read_csv(PERF_EVENT)
    cycle = pd.read_csv(PERF_CYCLE)

    for frame in (trades, event, cycle):
        for col in frame.columns:
            if col.endswith("_usdc") or col in {
                "realized_roi_on_settled_cost",
                "win_rate_settled",
                "settled_count",
                "open_count",
                "trade_count",
                "win_count",
                "loss_count",
            }:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")

    total_realized = float(event["realized_pnl_usdc"].sum())
    settled_cost = float(event["settled_cost_usdc"].sum())
    total_cost = float(event["total_cost_usdc"].sum())
    open_cost = float(event["open_cost_usdc"].sum())
    total_fees = float(event["total_fees_usdc"].sum())
    total_payout = float(event["total_payout_usdc"].sum())
    settled_count = int(event["settled_count"].sum())
    open_count = int(event["open_count"].sum())
    trade_count = int(event["trade_count"].sum())
    wins = int(event["win_count"].sum())
    losses = int(event["loss_count"].sum())

    by_kind = (
        event.groupby("kind", dropna=False)
        .agg(
            realized_pnl_usdc=("realized_pnl_usdc", "sum"),
            settled_cost_usdc=("settled_cost_usdc", "sum"),
            total_cost_usdc=("total_cost_usdc", "sum"),
            open_cost_usdc=("open_cost_usdc", "sum"),
            trade_count=("trade_count", "sum"),
            settled_count=("settled_count", "sum"),
            win_count=("win_count", "sum"),
            loss_count=("loss_count", "sum"),
        )
        .reset_index()
    )
    by_kind["roi_on_settled_cost"] = by_kind["realized_pnl_usdc"] / by_kind[
        "settled_cost_usdc"
    ].replace(0, pd.NA)

    by_event = event.sort_values("realized_pnl_usdc")
    by_cycle = cycle.sort_values("cycle_id").copy()
    by_cycle["cum_realized_pnl_usdc"] = by_cycle["realized_pnl_usdc"].cumsum()

    summary = {
        "trade_count": trade_count,
        "settled_count": settled_count,
        "open_count": open_count,
        "wins": wins,
        "losses": losses,
        "win_rate": wins / settled_count if settled_count else None,
        "total_realized_pnl_usdc": total_realized,
        "settled_cost_usdc": settled_cost,
        "open_cost_usdc": open_cost,
        "total_cost_usdc": total_cost,
        "total_fees_usdc": total_fees,
        "total_payout_usdc": total_payout,
        "roi_on_settled_cost": total_realized / settled_cost if settled_cost else None,
        "best_event": by_event.iloc[-1].to_dict() if len(by_event) else None,
        "worst_event": by_event.iloc[0].to_dict() if len(by_event) else None,
        "status_counts": trades["status"].value_counts(dropna=False).to_dict()
        if "status" in trades
        else {},
        "exit_reason_counts": trades["exit_reason"].value_counts(dropna=False).head(12).to_dict()
        if "exit_reason" in trades
        else {},
    }

    return summary, by_kind, by_cycle


def series_payload(df: pd.DataFrame) -> dict:
    out = {}
    for (city, kind), g in df.groupby(["city", "kind"], dropna=False):
        key = f"{city} | {kind}"
        points = []
        for _, row in g.iterrows():
            points.append(
                {
                    "t": row["observed_at_utc"].isoformat() if pd.notna(row["observed_at_utc"]) else "",
                    "cycle": str(row["cycle_id"]),
                    "forecast_extreme": none_if_nan(row["forecast_extreme"]),
                    "forecast_high": none_if_nan(row["forecast_high_calc"]),
                    "forecast_low": none_if_nan(row["forecast_low_calc"]),
                    "combined_high": none_if_nan(row.get("combined_high")),
                    "combined_low": none_if_nan(row.get("combined_low")),
                    "observed_extreme": none_if_nan(row["observed_extreme"]),
                    "drift_prev": none_if_nan(row["drift_from_prev"]),
                    "drift_first": none_if_nan(row["drift_from_first"]),
                }
            )
        out[key] = points
    return out


def none_if_nan(value):
    if pd.isna(value):
        return None
    return float(value)


def write_html(summary: dict, by_kind: pd.DataFrame, by_cycle: pd.DataFrame, twc: pd.DataFrame) -> Path:
    payload = {
        "summary": summary,
        "by_kind": json.loads(by_kind.to_json(orient="records")),
        "cycle": json.loads(
            by_cycle[
                [
                    "cycle_id",
                    "realized_pnl_usdc",
                    "cum_realized_pnl_usdc",
                    "trade_count",
                    "settled_count",
                    "win_count",
                    "loss_count",
                ]
            ].to_json(orient="records")
        ),
        "twc": series_payload(twc),
    }
    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>TWC Forecast Drift & Strategy PnL</title>
<style>
:root {{ --ink:#17212b; --muted:#667085; --grid:#d8dee8; --panel:#f7f8fb; --accent:#006d77; --loss:#c2410c; --gain:#047857; --blue:#2f6fed; }}
body {{ margin:0; font-family: "Segoe UI", "Microsoft YaHei", sans-serif; color:var(--ink); background:#fbfcfe; }}
header {{ padding:28px 34px 18px; border-bottom:1px solid #e6eaf0; background:linear-gradient(115deg,#ffffff,#edf7f5); }}
h1 {{ margin:0 0 8px; font-size:26px; font-weight:720; }}
.sub {{ color:var(--muted); font-size:14px; }}
main {{ padding:22px 34px 40px; }}
.kpis {{ display:grid; grid-template-columns:repeat(6,minmax(120px,1fr)); gap:12px; margin-bottom:22px; }}
.kpi {{ background:white; border:1px solid #e5e9f0; border-radius:8px; padding:13px 14px; box-shadow:0 1px 2px rgba(15,23,42,.04); }}
.kpi .label {{ color:var(--muted); font-size:12px; }}
.kpi .value {{ font-size:22px; font-weight:760; margin-top:4px; }}
.gain {{ color:var(--gain); }} .loss {{ color:var(--loss); }}
.toolbar {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin:12px 0 14px; }}
select {{ padding:8px 10px; border:1px solid #cfd7e3; border-radius:6px; background:white; color:var(--ink); }}
.grid {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
.panel {{ background:white; border:1px solid #e5e9f0; border-radius:8px; padding:16px; box-shadow:0 1px 2px rgba(15,23,42,.04); }}
.panel h2 {{ margin:0 0 10px; font-size:17px; }}
svg {{ width:100%; height:360px; overflow:visible; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th, td {{ border-bottom:1px solid #edf0f5; padding:8px 6px; text-align:right; }}
th:first-child, td:first-child {{ text-align:left; }}
th {{ color:var(--muted); font-weight:650; }}
.note {{ color:var(--muted); font-size:12px; margin-top:8px; line-height:1.5; }}
@media (max-width: 1050px) {{ .kpis {{ grid-template-columns:repeat(2,1fr); }} .grid {{ grid-template-columns:1fr; }} main, header {{ padding-left:18px; padding-right:18px; }} }}
</style>
</head>
<body>
<header>
  <h1>TWC API 原始预报偏移 & 策略盈亏</h1>
  <div class="sub">来源：服务器 CSV，本地离线生成；温度单位沿用导出文件 F。</div>
</header>
<main>
  <section class="kpis" id="kpis"></section>
  <section class="grid">
    <div class="panel">
      <h2>累计已实现 PnL</h2>
      <svg id="pnlChart" role="img" aria-label="累计已实现 PnL"></svg>
      <div class="note">曲线按 cycle 顺序累计；只统计 performance CSV 里已结算口径，未平仓成本单独展示。</div>
    </div>
    <div class="panel">
      <h2>按 High / Low 市场拆分</h2>
      <table id="kindTable"></table>
    </div>
  </section>
  <section class="panel" style="margin-top:18px;">
    <h2>TWC 预报偏移</h2>
    <div class="toolbar">
      <label>城市/市场 <select id="seriesSelect"></select></label>
      <label>指标 <select id="metricSelect">
        <option value="forecast_extreme">策略关注温度 High/Low</option>
        <option value="drift_first">相对首个周期偏移</option>
        <option value="drift_prev">相对上一周期偏移</option>
        <option value="forecast_high">预报最高温</option>
        <option value="forecast_low">预报最低温</option>
      </select></label>
    </div>
    <svg id="twcChart" role="img" aria-label="TWC forecast drift"></svg>
    <div class="note" id="twcNote"></div>
  </section>
</main>
<script>
const DATA = {json.dumps(payload, ensure_ascii=False)};
const fmtMoney = v => (v < 0 ? "-" : "") + "$" + Math.abs(v).toLocaleString(undefined, {{minimumFractionDigits:2, maximumFractionDigits:2}});
const fmtPct = v => v == null ? "" : (v*100).toFixed(1) + "%";
function cls(v) {{ return v < 0 ? "loss" : "gain"; }}
document.getElementById("kpis").innerHTML = [
  ["已实现 PnL", fmtMoney(DATA.summary.total_realized_pnl_usdc), cls(DATA.summary.total_realized_pnl_usdc)],
  ["已结算 ROI", fmtPct(DATA.summary.roi_on_settled_cost), cls(DATA.summary.roi_on_settled_cost)],
  ["交易数", DATA.summary.trade_count.toLocaleString(), ""],
  ["已结算 / 未平仓", DATA.summary.settled_count + " / " + DATA.summary.open_count, ""],
  ["胜率", fmtPct(DATA.summary.win_rate), ""],
  ["费用", fmtMoney(DATA.summary.total_fees_usdc), "loss"],
].map(k => `<div class="kpi"><div class="label">${{k[0]}}</div><div class="value ${{k[2]}}">${{k[1]}}</div></div>`).join("");

function lineChart(svgId, points, yKey, color) {{
  const svg = document.getElementById(svgId);
  svg.innerHTML = "";
  const W = svg.clientWidth || 800, H = 360, m = {{l:54,r:18,t:20,b:42}};
  const vals = points.map(p => p[yKey]).filter(v => v != null && Number.isFinite(v));
  if (!vals.length) {{ svg.innerHTML = `<text x="20" y="40" fill="#667085">没有可绘制数据</text>`; return; }}
  let ymin = Math.min(...vals), ymax = Math.max(...vals);
  if (ymin === ymax) {{ ymin -= 1; ymax += 1; }}
  const pad = (ymax-ymin)*0.12 || 1; ymin -= pad; ymax += pad;
  const x = i => m.l + (points.length <= 1 ? 0 : i*(W-m.l-m.r)/(points.length-1));
  const y = v => m.t + (ymax-v)*(H-m.t-m.b)/(ymax-ymin);
  const grid = [0,.25,.5,.75,1].map(t => {{
    const yy = m.t + t*(H-m.t-m.b), val = ymax - t*(ymax-ymin);
    return `<line x1="${{m.l}}" x2="${{W-m.r}}" y1="${{yy}}" y2="${{yy}}" stroke="#d8dee8"/><text x="8" y="${{yy+4}}" fill="#667085" font-size="11">${{val.toFixed(2)}}</text>`;
  }}).join("");
  const d = points.map((p,i) => p[yKey] == null ? "" : `${{i ? "L" : "M"}}${{x(i).toFixed(1)}},${{y(p[yKey]).toFixed(1)}}`).join(" ");
  const dots = points.map((p,i) => p[yKey] == null ? "" : `<circle cx="${{x(i)}}" cy="${{y(p[yKey])}}" r="3" fill="${{color}}"><title>${{p.cycle || ""}}\\n${{yKey}}: ${{p[yKey]}}</title></circle>`).join("");
  svg.innerHTML = `<rect x="0" y="0" width="${{W}}" height="${{H}}" fill="white"/>${{grid}}<line x1="${{m.l}}" x2="${{W-m.r}}" y1="${{H-m.b}}" y2="${{H-m.b}}" stroke="#98a2b3"/><path d="${{d}}" fill="none" stroke="${{color}}" stroke-width="2.5"/>${{dots}}`;
}}

document.getElementById("kindTable").innerHTML = `<thead><tr><th>kind</th><th>PnL</th><th>ROI</th><th>trades</th><th>settled</th><th>W/L</th></tr></thead><tbody>` +
DATA.by_kind.map(r => `<tr><td>${{r.kind}}</td><td class="${{cls(r.realized_pnl_usdc)}}">${{fmtMoney(r.realized_pnl_usdc)}}</td><td class="${{cls(r.roi_on_settled_cost)}}">${{fmtPct(r.roi_on_settled_cost)}}</td><td>${{r.trade_count}}</td><td>${{r.settled_count}}</td><td>${{r.win_count}}/${{r.loss_count}}</td></tr>`).join("") + `</tbody>`;

lineChart("pnlChart", DATA.cycle, "cum_realized_pnl_usdc", "#006d77");
const sel = document.getElementById("seriesSelect");
Object.keys(DATA.twc).forEach(k => sel.add(new Option(k, k)));
function renderTwc() {{
  const key = sel.value, metric = document.getElementById("metricSelect").value;
  lineChart("twcChart", DATA.twc[key] || [], metric, metric.startsWith("drift") ? "#c2410c" : "#2f6fed");
  const pts = DATA.twc[key] || [];
  const last = pts[pts.length-1] || {{}};
  document.getElementById("twcNote").textContent = `${{key}}：共 ${{pts.length}} 个抓取周期；最后一次策略关注温度=${{last.forecast_extreme ?? "NA"}}，相对首个周期偏移=${{last.drift_first ?? "NA"}}，相对上一周期偏移=${{last.drift_prev ?? "NA"}}。`;
}}
sel.addEventListener("change", renderTwc);
document.getElementById("metricSelect").addEventListener("change", renderTwc);
renderTwc();
</script>
</body>
</html>"""
    out = OUT / "strategy_twc_analysis.html"
    out.write_text(html, encoding="utf-8")
    return out


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    summary, by_kind, by_cycle = summarize_pnl()
    twc = read_twc_light(TWC_RAW)

    by_city_kind = (
        twc.groupby(["city", "kind"], dropna=False)
        .agg(
            cycles=("cycle_id", "count"),
            first_forecast=("forecast_extreme", "first"),
            last_forecast=("forecast_extreme", "last"),
            min_forecast=("forecast_extreme", "min"),
            max_forecast=("forecast_extreme", "max"),
            final_drift_from_first=("drift_from_first", "last"),
            max_abs_step_drift=("drift_from_prev", lambda s: s.abs().max()),
            observed_points=("observed_extreme", lambda s: s.notna().sum()),
        )
        .reset_index()
    )

    html_path = write_html(summary, by_kind, by_cycle, twc)
    by_kind.to_csv(OUT / "pnl_by_kind.csv", index=False)
    by_cycle.to_csv(OUT / "pnl_by_cycle.csv", index=False)
    by_city_kind.to_csv(OUT / "twc_forecast_drift_by_city_kind.csv", index=False)
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps({
        "html": str(html_path),
        "summary": summary,
        "by_kind": json.loads(by_kind.to_json(orient="records")),
        "twc_rows": int(len(twc)),
        "twc_groups": int(len(by_city_kind)),
        "drift_csv": str(OUT / "twc_forecast_drift_by_city_kind.csv"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
