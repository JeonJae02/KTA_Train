"""exchanger 신호(15초 이상 유지된 것만) 이후, 온도가 살짝 더 오르다가 다시
꺾여 내려가는 "heat limit" 복귀까지 걸리는 시간을 탱크별/날짜별로 본다.

**"heat limit" 태그를 쓰지 않는 이유.** InfluxDB 에 `TK_Heat_Limit_P1`,
`TK_Heat_Limit_I2` 태그가 존재하지만, 실측(2026-09-27~09-30, 3일)하면 값이
계속 0이다 — 이건 정상 운전 중의 온도 하한(설정값)이 아니라 좀처럼 안 뜨는
트립/알람 플래그다. 대신 실측 파형을 보면 물리적으로 뚜렷한 패턴이 있다:
exchanger 가 켜져서(냉각) 일정 시간 유지된 뒤 —

    켜진 동안/직후    온도가 살짝 더 오른다 (열교환 개시 지연 또는 국소혼합 추정)
    그 다음          온도가 빠르게 꺾여 떨어진다 (실측: P1 한 이벤트에서 약
                     236 -> 181 로 130초 만에 떨어짐 — 단위는 0.1℃ 로 약 5.5℃)
    바닥(trough)     찍고 다시 서서히 올라간다 (히터/주변열 유입으로 추정)

그 "바닥을 찍고 반등이 확인되는 시점"을 heat-limit 복귀로 조작적으로 정의한다
(reversal-confirmed trough). 반등 확인 없이 그냥 최소값을 잡으면 노이즈(±1~2
단위 잡음)에 의한 가짜 바닥을 주울 수 있어서, 일정 폭(REVERSAL_MARGIN) 이상
회복된 채로 일정 시간(REVERSAL_CONFIRM_SEC) 이상 유지돼야 진짜 반등으로 본다.

사용법:
    python Exchanger_Heat_Recovery_Time.py
"""

import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAVE_DIR = os.path.join(BASE_DIR, "extracted_csv")
OUT_DIR = os.path.join(BASE_DIR, "analysis_out", "exchanger_heat_recovery")

WAGON_TAG = "Now_Actual_Wagon_Num"

TANKS = {
    "P1": {"temp": "Scale_Out___TT_P1", "exchanger": "워킹 Tank 1 BACK Exchanger SOL",
           "heat": "워킹 Tank 1 BACK Heater"},
    "I2": {"temp": "Scale_Out___TT_I2", "exchanger": "워킹 Tank 2-2 MDI Exchanger SOL",
           "heat": "워킹 Tank 2 MDI  Heater"},
}

EXCLUDE_HOURS = (0, 7)          # Exchanger_Relay_Check.py 와 동일 — 야간 비가동 시간대 제외
RUNNING_WINDOW_MIN = 30
MIN_EVENT_SEC = 15              # "15초 정도 유지"된 exchanger ON 만 유효 신호로 본다

SMOOTH_WINDOW = 7                # 온도 스무딩(rolling median) 샘플 수 — 잡음 제거용
PEAK_GRACE_SEC = 90              # 이벤트 종료 후 이만큼까지도 "피크 탐색" 구간에 포함
#: P1 은 이벤트 후 4~6분 안에 바닥+반등이 다 끝나지만, I2 는 하강이 훨씬 완만해서
#: (실측: 트리거 후 13분에 바닥, 반등 확정까지 40~45분) 1200초로는 반등 확정 전에
#: 창이 끝나 버린다(전량 미확정). 두 탱크가 같은 상수를 쓰므로 더 느린 쪽(I2)에 맞춘다.
MAX_SEARCH_SEC = 3600             # 이벤트 시작 후 최대 이만큼까지만 바닥(반등)을 찾는다
REVERSAL_MARGIN = 5              # 바닥 대비 이 폭 이상 회복돼야 "반등"으로 인정
REVERSAL_CONFIRM_SEC = 60        # 그 회복 상태가 이만큼 유지돼야 확정


def load(tank_key, csv_path):
    cfg = TANKS[tank_key]
    raw = pd.read_csv(csv_path, index_col="Time", parse_dates=True).sort_index()
    raw = raw[~raw.index.duplicated(keep="last")]
    d = pd.DataFrame({
        "T": raw[cfg["temp"]],
        "exch": raw[cfg["exchanger"]],
        "heat": raw[cfg["heat"]],
        "wagon": raw[WAGON_TAG],
        "diag": (raw["source"] == "DIAGNOSTIC").astype(float) if "source" in raw.columns else 0.0,
    }).dropna(subset=["T", "exch"])

    w = d["wagon"].resample("1min").last().ffill()
    run = (w.diff().abs() > 0).rolling(RUNNING_WINDOW_MIN, min_periods=1).max()
    d["running"] = run.reindex(d.index, method="ffill").fillna(0)
    h = d.index.hour
    d["ok"] = (((h < EXCLUDE_HOURS[0]) | (h >= EXCLUDE_HOURS[1])) & (d["diag"] == 0) & (d["running"] == 1))

    d["T_smooth"] = d["T"].rolling(SMOOTH_WINDOW, min_periods=1, center=True).median()
    return d


def raw_events(d):
    """exch ON 이벤트의 (시작, 끝). MIN_EVENT_SEC 미만 chattering 은 제외."""
    s = (d["exch"] == 1) & d["ok"]
    grp = (s & ~s.shift(1, fill_value=False)).cumsum()[s]
    events = [(idx.iloc[0], idx.iloc[-1]) for _, idx in d.index[s].to_series().groupby(grp)
              if (idx.iloc[-1] - idx.iloc[0]).total_seconds() >= MIN_EVENT_SEC]
    return events


def find_recovery(d, t0, t1, next_t0):
    """이벤트(t0~t1) 이후 피크 -> 반등확인바닥(trough) 을 찾는다.

    반환: dict(피크시각, 피크값, 바닥시각, 바닥값, 확정여부) 또는 탐색구간에
    데이터가 부족하면 None.
    """
    search_end = min(t0 + pd.Timedelta(seconds=MAX_SEARCH_SEC),
                      next_t0 if next_t0 is not None else t0 + pd.Timedelta(seconds=MAX_SEARCH_SEC))
    seg = d.loc[t0:search_end, "T_smooth"].dropna()
    if len(seg) < 5:
        return None

    rise_end = min(t1 + pd.Timedelta(seconds=PEAK_GRACE_SEC), search_end)
    rise_seg = seg.loc[t0:rise_end]
    if rise_seg.empty:
        return None
    peak_time = rise_seg.idxmax()
    peak_val = rise_seg.loc[peak_time]

    after = seg.loc[peak_time:]
    running_min_val = after.iloc[0]
    running_min_time = after.index[0]
    confirmed = False
    trough_time, trough_val = running_min_time, running_min_val
    recover_start_time = None  # 새 최저점 이후 "마진 이상 회복" 상태가 시작된 시각

    for t, v in after.items():
        if v < running_min_val:
            running_min_val = v
            running_min_time = t
            recover_start_time = None
            continue
        if v >= running_min_val + REVERSAL_MARGIN:
            if recover_start_time is None:
                recover_start_time = t
            elif (t - recover_start_time).total_seconds() >= REVERSAL_CONFIRM_SEC:
                confirmed = True
                trough_time, trough_val = running_min_time, running_min_val
                break
        else:
            recover_start_time = None

    if not confirmed:
        trough_time, trough_val = running_min_time, running_min_val

    return {
        "peak_time": peak_time, "peak_val": peak_val,
        "trough_time": trough_time, "trough_val": trough_val,
        "confirmed": confirmed,
    }


def extract_recovery_table(tank_key, d):
    events = raw_events(d)
    rows = []
    for i, (t0, t1) in enumerate(events):
        next_t0 = events[i + 1][0] if i + 1 < len(events) else None
        rec = find_recovery(d, t0, t1, next_t0)
        if rec is None:
            continue
        t_start_val = d["T_smooth"].asof(t0)
        rows.append({
            "tank": tank_key,
            "day": t0.date(),
            "event_start": t0,
            "event_end": t1,
            "event_dur_sec": (t1 - t0).total_seconds(),
            "T_at_start": t_start_val,
            "peak_time": rec["peak_time"],
            "peak_val": rec["peak_val"],
            "rise_amount": rec["peak_val"] - t_start_val if pd.notna(t_start_val) else np.nan,
            "trough_time": rec["trough_time"],
            "trough_val": rec["trough_val"],
            "drop_depth": rec["peak_val"] - rec["trough_val"],
            "recovery_sec_from_start": (rec["trough_time"] - t0).total_seconds(),
            "recovery_sec_from_peak": (rec["trough_time"] - rec["peak_time"]).total_seconds(),
            "confirmed": rec["confirmed"],
        })
    return pd.DataFrame(rows)


def summarize(df):
    conf = df[df["confirmed"]]
    print(f"\n=== {df['tank'].iloc[0] if len(df) else '?'}: 전체 이벤트 {len(df)}개, "
          f"반등 확정 {len(conf)}개 ({100*len(conf)/len(df):.0f}%) ===")
    if len(conf) == 0:
        return
    q = conf["recovery_sec_from_start"].quantile([0.1, 0.25, 0.5, 0.75, 0.9])
    print("복귀시간(초, 이벤트 시작 기준) 분포:")
    print(q.to_string())
    print(f"평균 {conf['recovery_sec_from_start'].mean():.0f}초, "
          f"표준편차 {conf['recovery_sec_from_start'].std():.0f}초")
    per_day = conf.groupby("day")["recovery_sec_from_start"].agg(["count", "median"])
    print("\n날짜별 이벤트수/중앙값(초):")
    print(per_day.to_string())


# 팔레트: 고정 카테고리 순서, 색맹 안전 (Okabe-Ito 계열)
TANK_COLORS = {"P1": "#0072B2", "I2": "#D55E00"}


def plot_scatter_by_day(all_df):
    tanks = list(TANKS.keys())
    fig, axes = plt.subplots(len(tanks), 1, figsize=(14, 4.2 * len(tanks)), sharex=True)
    if len(tanks) == 1:
        axes = [axes]

    all_days = sorted(all_df["day"].unique())
    day_pos = {d: i for i, d in enumerate(all_days)}

    for ax, tank in zip(axes, tanks):
        sub = all_df[(all_df["tank"] == tank) & (all_df["confirmed"])]
        color = TANK_COLORS[tank]
        rng = np.random.default_rng(0)

        # 날짜별 박스플롯(분포 요약) — 옅게, 산점도가 주역이 되도록
        by_day = [sub[sub["day"] == d]["recovery_sec_from_start"] / 60.0 for d in all_days]
        positions = list(range(len(all_days)))
        bp = ax.boxplot(by_day, positions=positions, widths=0.5, showfliers=False,
                         patch_artist=True, zorder=2)
        for box in bp["boxes"]:
            box.set(facecolor=color, alpha=0.12, edgecolor=color, linewidth=1.0)
        for med in bp["medians"]:
            med.set(color=color, linewidth=1.6)
        for whisk in bp["whiskers"] + bp["caps"]:
            whisk.set(color=color, alpha=0.4, linewidth=1.0)

        # 산점도(지터 적용) — 개별 이벤트
        xs = sub["day"].map(day_pos).values.astype(float)
        xs = xs + rng.uniform(-0.18, 0.18, size=len(xs))
        ys = sub["recovery_sec_from_start"].values / 60.0
        ax.scatter(xs, ys, s=16, color=color, alpha=0.55, linewidths=0, zorder=3)

        n_by_day = sub.groupby("day").size().reindex(all_days, fill_value=0)
        label_transform = ax.get_xaxis_transform()  # x: 데이터 좌표, y: 축 기준 비율(0~1)
        for i, d in enumerate(all_days):
            if n_by_day.loc[d] > 0:
                ax.text(i, 1.02, f"n={n_by_day.loc[d]}", transform=label_transform,
                        ha="center", va="bottom", fontsize=7, color="#666666")

        ax.set_ylabel("복귀 시간 (분)", fontsize=10)
        ax.set_title(f"{tank} — exchanger ON(≥{MIN_EVENT_SEC}s) 이후 온도가 다시 꺾여 바닥을 찍기까지",
                     fontsize=11, loc="left", pad=18)
        ax.grid(axis="y", alpha=0.25, zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    axes[-1].set_xticks(range(len(all_days)))
    axes[-1].set_xticklabels([d.strftime("%m-%d") for d in all_days], rotation=45, ha="right")
    fig.suptitle("Exchanger 신호 이후 온도 재상승(peak) → 재하강(heat-limit 복귀) 소요시간, 날짜별",
                 fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.97])

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "recovery_time_by_day_scatter.png")
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    print(f"\n그래프 저장: {out_path}")
    return out_path


def main(csv_paths):
    all_rows = []
    for tank_key, csv_path in csv_paths.items():
        d = load(tank_key, csv_path)
        df = extract_recovery_table(tank_key, d)
        summarize(df)
        all_rows.append(df)

    all_df = pd.concat(all_rows, ignore_index=True)
    os.makedirs(OUT_DIR, exist_ok=True)
    csv_out = os.path.join(OUT_DIR, "recovery_events.csv")
    all_df.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"\n이벤트 표 저장: {csv_out}")

    plot_scatter_by_day(all_df)
    return all_df


if __name__ == "__main__":
    paths = {
        "P1": os.path.join(SAVE_DIR, "P1_exch_21d.csv"),
        "I2": os.path.join(SAVE_DIR, "I2_exch_21d.csv"),
    }
    main(paths)
