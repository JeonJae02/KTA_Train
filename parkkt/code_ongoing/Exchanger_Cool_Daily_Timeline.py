"""exchanger 와 cool(워킹 Tank 1 BACK Cooling SOL) 이 각각 언제 켜지는지, 그리고
둘이 겹치는 시간대가 언제인지를 **날짜별로 따로따로** 그래프 한 장씩 만든다.

(Exchanger_Feed_Daily_Timeline.py 와 동일한 구조 — feed 대신 cool.)
cool 은 이미 장기구간으로 받아둔 CSV(exchanger+cool 포함)가 있어서 재추출 없이
그걸 그대로 쓴다.

사용법:
    PYTHONIOENCODING=utf-8 python Exchanger_Cool_Daily_Timeline.py
"""

import glob
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "parkkt", "extracted_csv")
OUT_DIR = os.path.join(BASE_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")
DAILY_DIR = os.path.join(OUT_DIR, "exchanger_cool_daily")

EXCH_TAG = "워킹 Tank 1 BACK Exchanger SOL"
COOL_TAG = "워킹 Tank 1 BACK Cooling SOL"

COLOR_EXCH = "#0072B2"
COLOR_COOL = "#D55E00"
COLOR_OVERLAP = "#009E73"


def latest_csv_with(*tags):
    cands = sorted(glob.glob(os.path.join(RAW_DIR, "*_analysis.csv")))
    for p in reversed(cands):
        cols = pd.read_csv(p, nrows=0).columns
        if all(t in cols for t in tags):
            return p
    return None


def load_series(tag, path):
    raw = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=["Time", tag])
    raw = raw[~raw.index.duplicated(keep="last")].sort_index()
    return raw[tag].dropna()


def to_step(series, start, end):
    s = series.loc[start:end].sort_index()
    if s.empty:
        before = series.loc[:start]
        v0 = before.iloc[-1] if len(before) else 0
        return pd.Series([v0, v0], index=[start, end])
    if s.index[0] > start:
        before = series.loc[:start]
        v0 = before.iloc[-1] if len(before) else s.iloc[0]
        s = pd.concat([pd.Series([v0], index=[start]), s])
    if s.index[-1] < end:
        s = pd.concat([s, pd.Series([s.iloc[-1]], index=[end])])
    return s


def overlap_spans(exch_day, cool_day):
    idx = exch_day.index.union(cool_day.index)
    e = exch_day.reindex(idx, method="ffill").fillna(0)
    c = cool_day.reindex(idx, method="ffill").fillna(0)
    both = (e == 1) & (c == 1)
    if not both.any():
        return []
    grp = (both & ~both.shift(1, fill_value=False)).cumsum()[both]
    spans = [(sub.index[0], sub.index[-1]) for _, sub in idx[both].to_series().groupby(grp)]
    return spans


def plot_day(day, exch, cool):
    day_start = pd.Timestamp(day)
    day_end = day_start + pd.Timedelta(days=1)

    exch_step = to_step(exch, day_start, day_end)
    cool_step = to_step(cool, day_start, day_end)
    spans = overlap_spans(exch.loc[day_start:day_end], cool.loc[day_start:day_end])

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 5), sharex=True)

    ax1.step(exch_step.index, exch_step.values, where="post", color=COLOR_EXCH, linewidth=1.3)
    ax1.set_ylim(-0.15, 1.15)
    ax1.set_yticks([0, 1])
    ax1.set_ylabel("exchanger")
    ax1.set_title(f"{day:%Y-%m-%d} — exchanger / cool 켜진 시간대", loc="left", fontsize=12)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    ax2.step(cool_step.index, cool_step.values, where="post", color=COLOR_COOL, linewidth=1.3)
    ax2.set_ylim(-0.15, 1.15)
    ax2.set_yticks([0, 1])
    ax2.set_ylabel("cool")
    ax2.set_xlim(day_start, day_end)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    for s, e in spans:
        ax1.axvspan(s, e, color=COLOR_OVERLAP, alpha=0.25, zorder=0)
        ax2.axvspan(s, e, color=COLOR_OVERLAP, alpha=0.25, zorder=0)

    handles = [plt.Line2D([0], [0], color=COLOR_EXCH, label="exchanger"),
               plt.Line2D([0], [0], color=COLOR_COOL, label="cool"),
               plt.Rectangle((0, 0), 1, 1, color=COLOR_OVERLAP, alpha=0.25, label=f"겹침({len(spans)}회)")]
    ax1.legend(handles=handles, loc="upper right", fontsize=8, frameon=False)

    ax2.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    fig.tight_layout()
    os.makedirs(DAILY_DIR, exist_ok=True)
    out_path = os.path.join(DAILY_DIR, f"{day:%Y%m%d}.png")
    fig.savefig(out_path, dpi=130)
    plt.close(fig)
    return out_path, len(spans)


def main():
    path = latest_csv_with(EXCH_TAG, COOL_TAG)
    if path is None:
        raise FileNotFoundError("exchanger+cool 둘 다 포함된 CSV 없음")
    print(f"원본 CSV: {path}")
    exch = load_series(EXCH_TAG, path)
    cool = load_series(COOL_TAG, path)

    days = sorted(set(exch.index.normalize()) | set(cool.index.normalize()))
    print(f"총 {len(days)}일 — 하루씩 그래프 생성")

    summary = []
    for day in days:
        out_path, n_overlap = plot_day(day, exch, cool)
        summary.append({"day": day.date(), "overlap_count": n_overlap, "file": out_path})
        print(f"  {day.date()} -> {out_path}  (겹침 {n_overlap}회)")

    pd.DataFrame(summary).to_csv(os.path.join(OUT_DIR, "exchanger_cool_daily_summary.csv"),
                                  index=False, encoding="utf-8-sig")
    print(f"\n요약 저장: {os.path.join(OUT_DIR, 'exchanger_cool_daily_summary.csv')}")
    print(f"그래프 폴더: {DAILY_DIR}")


if __name__ == "__main__":
    main()
