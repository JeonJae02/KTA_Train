"""
HD1~4 x P1/P2 인젝션 사이클 카운트 + ON_T/OFF_T 값 분포(최빈값 등).

PLC 로직: SOL & PNP(후진감지) 가 동시에 True 일 때 MOV 로 타이머값이 ON_T 로
옮겨간다(사용자 확인). 하지만 DB 수집은 ~0.5초 간격이라, PLC 내부에서 몇 ms 만
True 였다 꺼지는 PNP 순간을 우리가 놓칠 수 있다 — 실제로 9/17 20:16 HD2_P2
이상구간에서 PNP=1 이 샘플에 단 한 번도 안 잡혔는데 ON_T 는 30으로 갱신된
사례를 확인했다(에일리어싱). 반면 SOL 은 펄스 길이가 0.5~1.5초로 충분히 길어서
0.5초 샘플링으로도 거의 항상 잡힌다.

그래서 사이클 경계는 **SOL 단독 펄스(0→1→0)** 로 잡는다. "SOL 이 True 였다"는
곧 그 순간 PNP 도 True 였다는 뜻이므로(안 그러면 MOV 자체가 안 된다), SOL
펄스 개수 = 실제 사이클 개수로 봐도 된다. 값은 펄스가 끝난 직후(SOL 이 다시
0 이 된) 첫 행에서 읽는다 — MOV 가 이미 반영된 상태여야 하기 때문이다.

ON_T/OFF_T 는 계속 증가하다 리셋되는 라이브 카운터가 아니라, 직전 완료된
구간의 최종 값을 다음 완료 시점까지 유지하는 값이다(현장 6h 샘플로 확인).
그래서 값이 바뀌었는지(diff!=0)로 사이클을 세면 "이전과 같은 값이 다시 나온
사이클"을 놓친다 — 반드시 SOL 펄스 자체로 세야 한다.

기간: 2026-09-10 ~ now, 제외 규칙 없음 (사용자 지정, 이전 SV/타이머 통계와 동일 범위).
"""
import os, sys, pickle
import numpy as np
import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT_DIR)
from Log_Extractor import LogExtractor

START = "2026-09-09 19:00:00"  # KST 로 해석됨 (Z 붙이면 UTC 로 잘못 해석되는 버그 있었음 — 09/10 00:00~09:00 KST 누락 원인)
END = "now()"

CACHE_DIR = os.path.join(ROOT_DIR, "parkkt", "cache")
os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_PATH = os.path.join(CACHE_DIR, "injection_cycle_raw_20260909_1900.pkl")

# 사용자가 준 순서 그대로. HD4 는 P2 가 없다(이전 조사에서 확인됨).
PAIRS = [
    ("HD1_P1", "Head_1 Injection SOL", "Head_1 Injection 후진감지 (PNP)"),
    ("HD1_P2", "Head_1-2 Injection SOL", "Head_1-2 Injection 후진감지 (PNP)"),
    ("HD2_P1", "Head_2 Injection 1 SOL", "Head_2 Injection 후진감지1 (PNP)"),
    ("HD2_P2", "Head_2 Injection_2 SOL", "Head_2 Injection 후진감지2 (PNP)"),
    ("HD3_P1", "Head_3 Injection SOL", "Head_3 Injection 후진감지 (PNP)"),
    ("HD3_P2", "Head_3-2 Injection SOL", "Head_3-2 Injection 후진감지 (PNP)"),
    ("HD4_P1", "Head_4 Injection SOL", "Head_4 Injection 후진감지 (PNP)"),
]


def all_tags():
    tags = []
    for pid, sol, pnp in PAIRS:
        tags += [sol, pnp, f"{pid}_인젝션_ON_T", f"{pid}_인젝션_OFF_T"]
    return tags


def load_raw():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, "rb") as f:
            return pickle.load(f)
    ex = LogExtractor(env_path=os.path.join(ROOT_DIR, ".env"))
    df = ex.get_data(start_time=START, end_time=END, target_tags=all_tags())
    with open(CACHE_PATH, "wb") as f:
        pickle.dump(df, f)
    return df


def pulse_end_values(df, sol_col, pnp_col, value_cols):
    """SOL 이 1인 연속 구간(펄스)마다 값을 뽑는다.

    **MOV 가 정확히 언제 반영되는지 샘플링만으론 확정할 수 없다.** 실측으로 두 가지
    상반된 사례를 직접 찍어서 확인했다:
      - HD1_P1 9/16 06:51 (peak=28): 값이 펄스가 끝나기 *전*, 마지막 True 행에서
        이미 28 로 바뀌어 있었고 펄스 종료 직후 행은 0 으로 리셋돼 있었다.
        → '펄스 종료 직후 행'을 읽으면 이 경우를 놓친다.
      - HD2_P2 9/28 20:16 (peak=33): 반대로 마지막 True 행은 아직 이전 값(9)이었고,
        펄스가 끝난 *다음* 행에서야 33 으로 바뀌었다.
        → '마지막 True 행'만 읽으면 이 경우를 놓친다.
    그래서 두 후보(마지막 True 행 / 펄스 종료 직후 행)를 다 보고, 펄스 시작 전
    값(baseline)과 다른 쪽을 우선한다 — 두 경우를 다 잡기 위한 절충이다. 그래도
    SOL 펄스 자체가 샘플링에 안 잡히는 경우(아래 invisible_commit_rate 참고)는
    이 방식으로도 복구 불가능하다.

    그 구간에서 PNP 가 한 번이라도 1로 관측됐는지도 같이 남겨서, 0.5초 샘플링이
    PNP 를 얼마나 놓치는지 정량화한다."""
    sol = (df[sol_col] == 1).to_numpy()
    pnp = (df[pnp_col] == 1).to_numpy()
    n = len(df)
    idx = df.index
    vals = {c: df[c].to_numpy() for c in value_cols}

    rows = []
    i = 0
    while i < n:
        if not sol[i]:
            i += 1
            continue
        start = i
        while i < n and sol[i]:
            i += 1
        end = i - 1  # 펄스의 마지막 True 행
        after = end + 1 if end + 1 < n else end  # 펄스 종료 직후 행(없으면 end 재사용)
        pnp_seen = bool(pnp[start:end + 1].any())
        baseline_idx = start - 1 if start > 0 else start

        rec = {"time": idx[end], "pnp_seen": pnp_seen}
        for c in value_cols:
            base = vals[c][baseline_idx]
            cand_last = vals[c][end]
            cand_after = vals[c][after]
            if cand_last != base:
                rec[c] = cand_last
            elif cand_after != base and cand_after != 0:
                rec[c] = cand_after
            else:
                rec[c] = cand_last
        rows.append(rec)
    return pd.DataFrame(rows)


def invisible_commit_rate(df, sol_col, value_col, win=2):
    """ON_T 가 0이 아닌 새 값으로 바뀐(=실제 사이클 완료) 시점 각각에 대해,
    그 주변(±win 틱)에서 SOL=1 이 단 한 번도 관측 안 된 비율을 구한다.
    이 비율이 곧 'SOL 펄스 자체가 0.5초 샘플링에 안 잡혀서 완전히 안 보이는 사이클'
    의 하한 추정치다(같은 값이 연속으로 나온 사이클은 애초에 집계에서 빠지므로
    실제 누락은 이보다 더 클 수 있다)."""
    v = df[value_col].to_numpy()
    s = (df[sol_col].to_numpy() == 1)
    n = len(df)
    prev = np.r_[np.nan, v[:-1]]
    commit_idx = np.where((v != prev) & (v != 0))[0]
    if len(commit_idx) == 0:
        return 0, 0, 0.0
    invisible = sum(
        1 for i in commit_idx
        if not s[max(0, i - win):min(n, i + win + 1)].any()
    )
    return len(commit_idx), invisible, invisible / len(commit_idx) * 100


def main():
    df = load_raw()
    print(f"원본: {len(df):,}행 x {len(df.columns)}컬럼 "
          f"({df.index.min()} ~ {df.index.max()})")

    missing = [t for t in all_tags() if t not in df.columns]
    if missing:
        print(f"⚠️ 없는 태그: {missing}")

    out_dir = os.path.join(ROOT_DIR, "parkkt", "analysis_out", "injection_cycle_value")
    os.makedirs(out_dir, exist_ok=True)

    summary_rows = []
    dist_frames = {}

    for pid, sol, pnp in PAIRS:
        on_t, off_t = f"{pid}_인젝션_ON_T", f"{pid}_인젝션_OFF_T"
        need = [sol, pnp, on_t, off_t]
        if not all(c in df.columns for c in need):
            print(f"⚠️ {pid}: 태그 부족, 건너뜀")
            continue

        sub = df[need].dropna()
        cycles = pulse_end_values(sub, sol, pnp, [on_t, off_t])
        n_cycles = len(cycles)
        pnp_seen_pct = cycles["pnp_seen"].mean() * 100 if n_cycles else float("nan")

        n_commit, n_invisible, pct_invisible = invisible_commit_rate(sub, sol, on_t)
        print(f"  [진단] {pid} ON_T 값변경(0제외) {n_commit}건 중 SOL 자체가 "
              f"근처(±2틱)에서도 전혀 안 보인 경우 {n_invisible}건 ({pct_invisible:.2f}%)")

        for label, col in [("ON_T", on_t), ("OFF_T", off_t)]:
            vc = cycles[col].value_counts().sort_index()
            mode_val = cycles[col].mode()
            summary_rows.append({
                "pair": pid,
                "timer": label,
                "n_cycles": n_cycles,
                "pnp_seen_pct": pnp_seen_pct,
                "n_distinct_values": vc.shape[0],
                "mode": mode_val.iloc[0] if not mode_val.empty else np.nan,
                "mode_count": int(vc.max()) if not vc.empty else 0,
                "mean": cycles[col].mean(),
                "max": cycles[col].max(),
                "min": cycles[col].min(),
                "std": cycles[col].std(),
            })
            dist_frames[f"{pid}_{label}"] = vc.rename("count").reset_index().rename(columns={"index": "value"})

        print(f"{pid}: 사이클 {n_cycles}개 (PNP 동시관측률 {pnp_seen_pct:.1f}%)")

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(os.path.join(out_dir, "cycle_value_summary.csv"),
                       index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(os.path.join(out_dir, "value_distributions_v2.xlsx")) as xw:
        for name, d in dist_frames.items():
            d.to_excel(xw, sheet_name=name[:31], index=False)

    print("\n=== 페어별 사이클 수 및 값 분포 요약 ===")
    print(summary_df.round(3).to_string(index=False))
    print(f"\n저장 위치: {out_dir}")


if __name__ == "__main__":
    main()
