"""두 가지를 보여준다.

1. **경계 근처(20~60초) 이벤트의 실제 타임라인** — 산점도만으로는 "이게 진짜
   사이클인지 채터링인지" 확신이 안 서니, 그 시각 전후 원시 0/1 전환을 그대로
   나열해서 사람이 직접 판단할 수 있게 한다.
2. **3시간 창 단위 전/후 비교** — 원시 신호(합치기 전)와, 임계값
   `THRESHOLD_SEC`로 짧은 깜빡임을 이어붙인 논리 신호(합친 후)를 같은 창에
   겹쳐 그려서 얼마나 달라지는지 보여준다.

**필터 없이 raw 전체**를 쓴다. `Exchanger_Off_On_Gap_Analysis.py` 의 "야간/
비가동 제외"는 통계용 가공이었고, 여기서는 병합 로직 자체(사이클 경계를
어떻게 자르는가)를 보는 거라 원본 그대로 쓰는 게 맞다 — 그래야 밤에 완전히
꺼져 있는 구간도 "그냥 아주 긴 사이클 OFF"로 똑같은 규칙으로 처리된다.

사용법 (Exchanger_Off_On_Gap_Analysis.py 를 먼저 돌려 raw CSV 를 만들어둘 것):
    PYTHONIOENCODING=utf-8 python Exchanger_Cycle_Before_After.py
"""

import glob
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "parkkt", "extracted_csv")
OUT_DIR = os.path.join(BASE_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")
CHUNK_DIR = os.path.join(OUT_DIR, "before_after_3h")

EXCH_TAG = "워킹 Tank 1 BACK Exchanger SOL"
THRESHOLD_SEC = 180
TIMELINE_WINDOW_SEC = 180   # 경계 이벤트 전후로 이만큼씩 보여준다
BORDERLINE_RANGE = (15, 70)  # 타임라인을 뽑아줄 gap 범위(초) — 20~60초대 포함

COLOR_RAW = "#0072B2"
COLOR_MERGED = "#D55E00"


def latest_raw_csv():
    cands = sorted(glob.glob(os.path.join(RAW_DIR, "*_analysis.csv")))
    if not cands:
        raise FileNotFoundError(f"{RAW_DIR} 에 *_analysis.csv 가 없습니다 — "
                                 "Exchanger_Off_On_Gap_Analysis.py 먼저 실행")
    return cands[-1]


def load_exch_series():
    path = latest_raw_csv()
    raw = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=["Time", EXCH_TAG])
    raw = raw[~raw.index.duplicated(keep="last")].sort_index()
    return raw[EXCH_TAG].dropna(), path


def raw_segments(exch):
    """exch==1 로 연속된 (on_start, off_start) 구간 목록. off_start 는 '꺼지기
    시작한 시각'(= 그 구간의 마지막 1 다음 행)."""
    s = exch[exch.diff() != 0]  # 변화 지점만
    changes = pd.DataFrame({"val": s})
    changes["prev"] = changes["val"].shift(1)

    on_starts = changes.index[(changes["prev"].isna() | (changes["prev"] == 0)) & (changes["val"] == 1)]
    off_starts = changes.index[(changes["prev"] == 1) & (changes["val"] == 0)]

    segs = []
    oi = 0
    for on_t in on_starts:
        while oi < len(off_starts) and off_starts[oi] <= on_t:
            oi += 1
        if oi >= len(off_starts):
            break
        segs.append((on_t, off_starts[oi]))
    return segs, on_starts, off_starts


def gaps_from_segments(segs):
    rows = []
    for i in range(len(segs) - 1):
        off_t = segs[i][1]
        on_t = segs[i + 1][0]
        rows.append({"off_time": off_t, "on_time": on_t, "gap_sec": (on_t - off_t).total_seconds()})
    return pd.DataFrame(rows)


def merge_cycles(segs, threshold):
    """(on_start, off_start) 구간들을 threshold 기준으로 이어붙여
    (cycle_start, cycle_end) 목록을 만든다."""
    if not segs:
        return []
    cycles = []
    cycle_start = segs[0][0]
    for i in range(len(segs) - 1):
        gap = (segs[i + 1][0] - segs[i][1]).total_seconds()
        if gap >= threshold:
            cycles.append((cycle_start, segs[i][1]))
            cycle_start = segs[i + 1][0]
    cycles.append((cycle_start, segs[-1][1]))
    return cycles


# --------------------------------------------------------------------------- #
# 1) 경계(20~60초) 이벤트 타임라인
# --------------------------------------------------------------------------- #

def print_borderline_timelines(exch, gaps):
    sub = gaps[(gaps["gap_sec"] >= BORDERLINE_RANGE[0]) & (gaps["gap_sec"] <= BORDERLINE_RANGE[1])]
    sub = sub.sort_values("off_time")
    print(f"\n=== {BORDERLINE_RANGE[0]}~{BORDERLINE_RANGE[1]}초 구간 이벤트 {len(sub)}건 — "
          f"전후 ±{TIMELINE_WINDOW_SEC}초 원시 타임라인 ===")

    windows = []
    for _, r in sub.iterrows():
        off_t, on_t, gap_sec = r["off_time"], r["on_time"], r["gap_sec"]
        lo = off_t - pd.Timedelta(seconds=TIMELINE_WINDOW_SEC)
        hi = on_t + pd.Timedelta(seconds=TIMELINE_WINDOW_SEC)
        seg = exch.loc[lo:hi]
        seg = seg[seg.diff().fillna(1) != 0]  # 변화 지점만 (첫 값 포함)

        print(f"\n--- OFF {off_t} -> ON {on_t}  (간격 {gap_sec:.1f}초) ---")
        prev_t = None
        for t, v in seg.items():
            marker = " <== 이 OFF" if abs((t - off_t).total_seconds()) < 0.01 else (
                      " <== 이 ON" if abs((t - on_t).total_seconds()) < 0.01 else "")
            dur = f"  (+{(t - prev_t).total_seconds():.1f}초)" if prev_t is not None else ""
            print(f"    {t}  val={int(v)}{dur}{marker}")
            prev_t = t
        windows.append((off_t.floor("3h"), off_t, on_t))
    return windows


# --------------------------------------------------------------------------- #
# 2) 3시간 창 전/후 비교 그래프
# --------------------------------------------------------------------------- #

def plot_window(exch, cycles, win_start, win_end, highlight=None):
    raw_seg = exch.loc[win_start:win_end]
    if raw_seg.empty:
        return None

    # step plot 용: 시작/끝에 경계값 보강
    def to_step(idx_vals, start, end):
        idx_vals = idx_vals.sort_index()
        if idx_vals.index[0] > start:
            idx_vals = pd.concat([pd.Series([idx_vals.iloc[0]], index=[start]), idx_vals])
        if idx_vals.index[-1] < end:
            idx_vals = pd.concat([idx_vals, pd.Series([idx_vals.iloc[-1]], index=[end])])
        return idx_vals

    raw_step = to_step(raw_seg, win_start, win_end)

    merged_idx, merged_val = [], []
    for cs, ce in cycles:
        if ce < win_start or cs > win_end:
            continue
        s, e = max(cs, win_start), min(ce, win_end)
        merged_idx += [s, e]
        merged_val += [1, 1]
    merged = pd.Series(merged_val, index=merged_idx).sort_index() if merged_idx else pd.Series(dtype=float)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 5.5), sharex=True)

    ax1.step(raw_step.index, raw_step.values, where="post", color=COLOR_RAW, linewidth=1.3)
    ax1.set_ylim(-0.15, 1.15)
    ax1.set_yticks([0, 1])
    ax1.set_ylabel("원시(합치기 전)")
    ax1.set_title(f"{win_start:%Y-%m-%d %H:%M} ~ {win_end:%H:%M} — exchanger 신호 전/후 비교", loc="left", fontsize=11)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    ax2.set_ylim(-0.15, 1.15)
    ax2.set_yticks([0, 1])
    ax2.set_ylabel(f"합친 후(임계값{THRESHOLD_SEC}s)")
    ax2.axhline(0, color=COLOR_MERGED, linewidth=1.3, alpha=0.3)
    for cs, ce in cycles:
        if ce < win_start or cs > win_end:
            continue
        s, e = max(cs, win_start), min(ce, win_end)
        ax2.fill_between([s, e], 0, 1, step="post", color=COLOR_MERGED, alpha=0.85, linewidth=0)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    if highlight:
        for off_t, on_t in highlight:
            if win_start <= off_t <= win_end:
                for ax in (ax1, ax2):
                    ax.axvspan(off_t, on_t, color="#009E73", alpha=0.15, zorder=0)

    fig.tight_layout()
    return fig


def main():
    exch, path = load_exch_series()
    print(f"원시 CSV 로드: {path} ({len(exch)}행)")

    segs, on_starts, off_starts = raw_segments(exch)
    gaps = gaps_from_segments(segs)
    print(f"raw ON 구간 {len(segs)}개, OFF 갭 {len(gaps)}개")

    windows = print_borderline_timelines(exch, gaps)

    cycles = merge_cycles(segs, THRESHOLD_SEC)
    print(f"\n임계값 {THRESHOLD_SEC}초로 합친 뒤 논리 사이클 {len(cycles)}개 (raw ON 구간 {len(segs)}개에서 축소)")

    os.makedirs(CHUNK_DIR, exist_ok=True)
    t0 = exch.index.min().floor("3h")
    t1 = exch.index.max().ceil("3h")
    all_windows = pd.date_range(t0, t1, freq="3h")

    saved = []
    for ws in all_windows[:-1]:
        we = ws + pd.Timedelta(hours=3)
        fig = plot_window(exch, cycles, ws, we,
                           highlight=[(r["off_time"], r["on_time"]) for _, r in
                                      gaps[(gaps["gap_sec"] >= BORDERLINE_RANGE[0]) &
                                           (gaps["gap_sec"] <= BORDERLINE_RANGE[1])].iterrows()])
        if fig is None:
            continue
        fname = f"{ws:%Y%m%d_%H%M}.png"
        fig.savefig(os.path.join(CHUNK_DIR, fname), dpi=130)
        plt.close(fig)
        saved.append(fname)

    print(f"\n3시간 창 전/후 비교 그래프 {len(saved)}개 저장: {CHUNK_DIR}")

    highlight_windows = sorted(set(w[0] for w in windows))
    print("\n경계 이벤트가 들어있는 창(바로 보면 됨):")
    for w in highlight_windows:
        print(f"  {os.path.join(CHUNK_DIR, f'{w:%Y%m%d_%H%M}.png')}")


if __name__ == "__main__":
    main()
