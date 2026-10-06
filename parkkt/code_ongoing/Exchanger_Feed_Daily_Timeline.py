"""exchanger 와 feed(워킹 Tank 1 BACK Feeding SOL) 가 각각 언제 켜지는지, 그리고
둘이 겹치는 시간대가 언제인지를 **날짜별로 따로따로** 그래프 한 장씩 만든다.

태그별로 따로 패널을 나눈다(위: exchanger, 아래: feed), 겹치는 구간은 두 패널
다 초록색으로 음영 처리한다.

사용법 (exchanger/cool 이미 받아둔 장기 CSV 가 있어야 함 —
Exchanger_Cool_Overlap_By_Shift_LongRange.py 참고):
    PYTHONIOENCODING=utf-8 python Exchanger_Feed_Daily_Timeline.py
"""

import glob
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT_DIR)
from Log_Extractor import LogExtractor

ENV_PATH = os.path.join(ROOT_DIR, ".env")
RAW_DIR = os.path.join(ROOT_DIR, "parkkt", "extracted_csv")
OUT_DIR = os.path.join(ROOT_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")
DAILY_DIR = os.path.join(OUT_DIR, "exchanger_feed_daily")

EXCH_TAG = "워킹 Tank 1 BACK Exchanger SOL"
FEED_TAG = "워킹 Tank 1 BACK Feeding SOL"

START = "2026-09-09 10:00:00"
END = "now()"

COLOR_EXCH = "#0072B2"
COLOR_FEED = "#E69F00"
COLOR_OVERLAP = "#009E73"


def latest_csv_with(tag):
    cands = sorted(glob.glob(os.path.join(RAW_DIR, "*_analysis.csv")))
    for p in reversed(cands):
        if tag in pd.read_csv(p, nrows=0).columns:
            return p
    return None


def fetch_feed():
    extractor = LogExtractor(env_path=ENV_PATH)
    df = extractor.get_data(start_time=START, end_time=END, target_tags=[FEED_TAG])
    os.makedirs(RAW_DIR, exist_ok=True)
    extractor.save_to_csv(df, save_dir=RAW_DIR)
    return df[FEED_TAG].dropna()


def load_exch():
    path = latest_csv_with(EXCH_TAG)
    if path is None:
        raise FileNotFoundError(f"{EXCH_TAG} 포함된 CSV 없음")
    raw = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=["Time", EXCH_TAG])
    raw = raw[~raw.index.duplicated(keep="last")].sort_index()
    return raw[EXCH_TAG].dropna()


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


def overlap_spans(exch_day, feed_day, day_start, day_end):
    """exch==1 and feed==1 인 구간을 (시작,끝) 리스트로."""
    idx = exch_day.index.union(feed_day.index)
    e = exch_day.reindex(idx, method="ffill").fillna(0)
    f = feed_day.reindex(idx, method="ffill").fillna(0)
    both = (e == 1) & (f == 1)
    if not both.any():
        return []
    grp = (both & ~both.shift(1, fill_value=False)).cumsum()[both]
    spans = [(sub.index[0], sub.index[-1]) for _, sub in idx[both].to_series().groupby(grp)]
    return spans


def plot_day(day, exch, feed):
    day_start = pd.Timestamp(day)
    day_end = day_start + pd.Timedelta(days=1)

    exch_step = to_step(exch, day_start, day_end)
    feed_step = to_step(feed, day_start, day_end)
    spans = overlap_spans(exch.loc[day_start:day_end], feed.loc[day_start:day_end], day_start, day_end)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 5), sharex=True)

    ax1.step(exch_step.index, exch_step.values, where="post", color=COLOR_EXCH, linewidth=1.3)
    ax1.set_ylim(-0.15, 1.15)
    ax1.set_yticks([0, 1])
    ax1.set_ylabel("exchanger")
    ax1.set_title(f"{day:%Y-%m-%d} — exchanger / feed 켜진 시간대", loc="left", fontsize=12)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    ax2.step(feed_step.index, feed_step.values, where="post", color=COLOR_FEED, linewidth=1.3)
    ax2.set_ylim(-0.15, 1.15)
    ax2.set_yticks([0, 1])
    ax2.set_ylabel("feed")
    ax2.set_xlim(day_start, day_end)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    for s, e in spans:
        ax1.axvspan(s, e, color=COLOR_OVERLAP, alpha=0.25, zorder=0)
        ax2.axvspan(s, e, color=COLOR_OVERLAP, alpha=0.25, zorder=0)

    handles = [plt.Line2D([0], [0], color=COLOR_EXCH, label="exchanger"),
               plt.Line2D([0], [0], color=COLOR_FEED, label="feed"),
               plt.Rectangle((0, 0), 1, 1, color=COLOR_OVERLAP, alpha=0.25, label=f"겹침({len(spans)}회)")]
    ax1.legend(handles=handles, loc="upper right", fontsize=8, frameon=False)

    import matplotlib.dates as mdates
    ax2.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    fig.tight_layout()
    os.makedirs(DAILY_DIR, exist_ok=True)
    out_path = os.path.join(DAILY_DIR, f"{day:%Y%m%d}.png")
    fig.savefig(out_path, dpi=130)
    plt.close(fig)
    return out_path, len(spans)


def main():
    feed = fetch_feed()
    exch = load_exch()

    days = sorted(set(exch.index.normalize()) | set(feed.index.normalize()))
    print(f"총 {len(days)}일 — 하루씩 그래프 생성")

    summary = []
    for day in days:
        out_path, n_overlap = plot_day(day, exch, feed)
        summary.append({"day": day.date(), "overlap_count": n_overlap, "file": out_path})
        print(f"  {day.date()} -> {out_path}  (겹침 {n_overlap}회)")

    pd.DataFrame(summary).to_csv(os.path.join(OUT_DIR, "exchanger_feed_daily_summary.csv"),
                                  index=False, encoding="utf-8-sig")
    print(f"\n요약 저장: {os.path.join(OUT_DIR, 'exchanger_feed_daily_summary.csv')}")
    print(f"그래프 폴더: {DAILY_DIR}")


if __name__ == "__main__":
    main()
