"""두 가지를 본다.

1. 헤드별 Shot_Injection vs Test_Shot_Injection 관계 — 겹치는지, 테스트가
   항상 같이 일어나는지.
2. exchanger 사이클의 "하루 첫/마지막" 판정을, 달력 자정 기준이 아니라
   **실제 injection(생산) 신호가 켜져 있던 구간** 기준으로 다시 잡으면
   어떻게 달라지는지 — 09-09 18:00 데이터부터 비교.

사용법 (injection/test-shot 14개 태그 CSV 가 이미 받아져 있어야 함):
    PYTHONIOENCODING=utf-8 python Injection_TestShot_And_DayBoundary.py
"""

import glob
import os

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "parkkt", "extracted_csv")
OUT_DIR = os.path.join(BASE_DIR, "parkkt", "analysis_out", "exchanger_off_on_gap")

HEAD_PAIRS = [("HD1", "P1"), ("HD1", "P2"), ("HD2", "P1"), ("HD2", "P2"),
              ("HD3", "P1"), ("HD3", "P2"), ("HD4", "P1")]
GAP_MERGE_SEC = 1800  # 생산 구간을 가를 때, 이보다 짧은 무발사 공백은 그냥 쉬는 시간으로 보고 안 끊음


def latest_csv_with(*tags):
    cands = sorted(glob.glob(os.path.join(RAW_DIR, "*_analysis.csv")))
    for p in reversed(cands):
        cols = pd.read_csv(p, nrows=0).columns
        if all(t in cols for t in tags):
            return p
    return None


def pulse_starts(s):
    s = s.dropna()
    s2 = s[s.diff() != 0]
    ch = pd.DataFrame({"val": s2})
    ch["prev"] = ch["val"].shift(1)
    return ch.index[((ch["prev"] == 0) | (ch["prev"].isna())) & (ch["val"] == 1)]


def part1_injection_vs_test(path):
    cols = ["Time"]
    for hd, p in HEAD_PAIRS:
        cols += [f"Shot_Injection_{hd}_{p}", f"Test_Shot_Injection_{hd}_{p}"]
    df = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=cols)
    df = df[~df.index.duplicated(keep="last")].sort_index()

    print("=== 1) 헤드별 Shot_Injection vs Test_Shot_Injection ===\n")
    for hd, p in HEAD_PAIRS:
        shot_col, test_col = f"Shot_Injection_{hd}_{p}", f"Test_Shot_Injection_{hd}_{p}"
        shot_on = pulse_starts(df[shot_col])
        test_on = pulse_starts(df[test_col])
        print(f"--- {hd}_{p} --- 정규 샷 {len(shot_on)}회, 테스트샷 {len(test_on)}회")
        for t in test_on:
            window = df[shot_col].loc[t - pd.Timedelta(minutes=5): t + pd.Timedelta(minutes=5)]
            active = (window == 1).any()
            print(f"    테스트샷 {t} — 전후 5분 내 정규 샷 활동: {'있음' if active else '없음(고립)'}")
        if len(test_on) == 0:
            print("    (이 헤드는 관측 기간 중 테스트샷 없음)")
    print()


def production_windows(path):
    """모든 헤드 Shot_Injection 중 하나라도 켜지면 '생산중' — 그 구간을
    GAP_MERGE_SEC 이상의 공백으로 끊어 하루치 생산 윈도우를 만든다."""
    cols = ["Time"] + [f"Shot_Injection_{hd}_{p}" for hd, p in HEAD_PAIRS]
    df = pd.read_csv(path, index_col="Time", parse_dates=True, usecols=cols)
    df = df[~df.index.duplicated(keep="last")].sort_index()
    any_shot = (df.fillna(0) == 1).any(axis=1).astype(int)
    any_shot = any_shot[any_shot.diff() != 0]

    changes = pd.DataFrame({"val": any_shot})
    changes["prev"] = changes["val"].shift(1)
    on_starts = changes.index[((changes["prev"].isna()) | (changes["prev"] == 0)) & (changes["val"] == 1)]
    off_starts = changes.index[(changes["prev"] == 1) & (changes["val"] == 0)]

    segs, oi = [], 0
    for on_t in on_starts:
        while oi < len(off_starts) and off_starts[oi] <= on_t:
            oi += 1
        if oi >= len(off_starts):
            break
        segs.append((on_t, off_starts[oi]))

    # GAP_MERGE_SEC 미만 공백은 이어붙여 하루 단위 생산윈도우로
    windows = []
    cur_start = segs[0][0]
    for i in range(len(segs) - 1):
        gap = (segs[i + 1][0] - segs[i][1]).total_seconds()
        if gap < GAP_MERGE_SEC:
            continue
        windows.append((cur_start, segs[i][1]))
        cur_start = segs[i + 1][0]
    windows.append((cur_start, segs[-1][1]))
    return windows


def part2_day_boundary(injection_path, exch_cycles_path):
    windows = production_windows(injection_path)
    print(f"=== 2) injection 기준 생산윈도우 {len(windows)}개 (09-09 18:00~) ===")
    for s, e in windows:
        print(f"  {s} ~ {e}  ({(e-s).total_seconds()/3600:.1f}시간)")

    cycles = pd.read_csv(exch_cycles_path, parse_dates=["cycle_start", "cycle_end", "next_on"])
    cycles = cycles[cycles["cycle_start"] >= windows[0][0] - pd.Timedelta(hours=1)].copy()

    def classify(t0):
        for s, e in windows:
            if s <= t0 <= e:
                return "윈도우 내부"
        return "윈도우 밖"

    # 기존 방식(달력일 기준 첫/마지막)과 새 방식(injection 생산윈도우 기준 첫/마지막) 비교
    cycles["day_old"] = cycles["cycle_start"].dt.date
    first_old = cycles.groupby("day_old")["cycle_start"].idxmin()
    last_old = cycles.groupby("day_old")["cycle_start"].idxmax()
    cycles["is_first_old"] = cycles.index.isin(first_old)
    cycles["is_last_old"] = cycles.index.isin(last_old)

    win_id = []
    for t0 in cycles["cycle_start"]:
        found = None
        for i, (s, e) in enumerate(windows):
            if s - pd.Timedelta(hours=2) <= t0 <= e + pd.Timedelta(hours=2):
                found = i
                break
        win_id.append(found)
    cycles["win_id"] = win_id

    valid = cycles.dropna(subset=["win_id"]).copy()
    valid["win_id"] = valid["win_id"].astype(int)
    first_new = valid.groupby("win_id")["cycle_start"].idxmin()
    last_new = valid.groupby("win_id")["cycle_start"].idxmax()
    cycles["is_first_new"] = cycles.index.isin(first_new)
    cycles["is_last_new"] = cycles.index.isin(last_new)

    print("\n=== 기존(달력일 기준) vs 새 방식(injection 생산윈도우 기준) 비교 ===")
    diff_first = cycles[cycles["is_first_old"] != cycles["is_first_new"]]
    diff_last = cycles[cycles["is_last_old"] != cycles["is_last_new"]]
    print(f"'첫가동' 판정이 달라진 사이클: {len(diff_first)}개")
    if len(diff_first):
        print(diff_first[["cycle_start", "cycle_end", "is_first_old", "is_first_new"]].to_string(index=False))
    print(f"\n'셧다운직전' 판정이 달라진 사이클: {len(diff_last)}개")
    if len(diff_last):
        print(diff_last[["cycle_start", "cycle_end", "is_last_old", "is_last_new"]].to_string(index=False))

    out_path = os.path.join(OUT_DIR, "exchanger_cycles_injection_boundary.csv")
    cycles.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n결과 저장: {out_path}")


def main():
    inj_path = latest_csv_with("Shot_Injection_HD1_P1", "Test_Shot_Injection_HD1_P1")
    print(f"injection CSV: {inj_path}\n")
    part1_injection_vs_test(inj_path)

    exch_cycles_path = os.path.join(OUT_DIR, "cycles_long_range_09-09_to_now.csv")
    part2_day_boundary(inj_path, exch_cycles_path)


if __name__ == "__main__":
    main()
