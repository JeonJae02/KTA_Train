# parkkt/Anomaly_Viewer.py
"""이상탐지 결과를 날짜/시간 축에 얹어 들여다보는 그림 도구.

`Anomaly_Viewer_Cycle.ipynb` / `Anomaly_Viewer_Tick.ipynb` 가 같이 쓴다.
두 모델의 단위(사이클 vs 틱)가 달라도 "시각 + 이상점수" 두 컬럼만 있으면 그려진다.
"""
import os

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")


def cache_path(name):
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, name)


def add_time_cols(df, time_col="Start_Time"):
    out = df.copy()
    t = pd.to_datetime(out[time_col])
    out["Date"] = t.dt.date
    out["Hour"] = t.dt.hour
    return out


# ---------------------------------------------------------------- 히트맵
def hourly_heatmap(df, threshold, time_col="Start_Time", score_col="Recon_Error",
                    value="rate", title=None, figsize=(15, 6)):
    """날짜(행) x 시각(열) 격자.

    value="rate"  : 그 칸에서 임계치를 넘은 비율(%)
    value="mean"  : 그 칸의 평균 재구성오차
    value="count" : 그 칸의 이상 건수
    """
    d = add_time_cols(df, time_col)
    d["is_anom"] = d[score_col] > threshold

    if value == "rate":
        piv = d.pivot_table(index="Date", columns="Hour", values="is_anom", aggfunc="mean") * 100
        label, fmt = "이상률 (%)", "%.1f"
    elif value == "mean":
        piv = d.pivot_table(index="Date", columns="Hour", values=score_col, aggfunc="mean")
        label, fmt = "평균 재구성오차", "%.3f"
    else:
        piv = d.pivot_table(index="Date", columns="Hour", values="is_anom", aggfunc="sum")
        label, fmt = "이상 건수", "%.0f"

    piv = piv.reindex(columns=range(24))
    n_piv = d.pivot_table(index="Date", columns="Hour", values=score_col,
                          aggfunc="size").reindex(columns=range(24))

    fig, ax = plt.subplots(figsize=figsize)
    masked = np.ma.masked_invalid(piv.values.astype(float))
    cmap = plt.get_cmap("YlOrRd").copy()
    cmap.set_bad("#e8e8e8")          # 데이터 없는 칸은 회색
    im = ax.imshow(masked, aspect="auto", cmap=cmap)

    ax.set_xticks(range(24))
    ax.set_xticklabels(range(24), fontsize=9)
    ax.set_yticks(range(len(piv.index)))
    ax.set_yticklabels([str(x) for x in piv.index], fontsize=9)
    ax.set_xlabel("시각 (KST)")
    ax.set_title(title or f"날짜 x 시간대 {label}  (회색 = 데이터 없음)")

    # 칸마다 숫자 - 표본이 적은 칸은 흐리게 (해석 주의 표시)
    for r in range(piv.shape[0]):
        for c in range(piv.shape[1]):
            v = piv.values[r, c]
            if np.isnan(v):
                continue
            n = n_piv.values[r, c]
            alpha = 1.0 if (not np.isnan(n) and n >= 20) else 0.45
            ax.text(c, r, fmt % v, ha="center", va="center", fontsize=7.5,
                    color="black", alpha=alpha)

    fig.colorbar(im, ax=ax, label=label, fraction=0.025)
    plt.tight_layout()
    return fig, piv


# ---------------------------------------------------------------- 하루 상세
def daily_detail(df, date, threshold, time_col="Start_Time", score_col="Recon_Error",
                  extra_cols=(), figsize=(15, 8)):
    """하루치를 시계열로 펼친다. 위: 이상점수, 아래: 원하는 원본 지표."""
    d = add_time_cols(df, time_col)
    day = d[d["Date"] == pd.to_datetime(date).date()].sort_values(time_col)
    if day.empty:
        print(f"{date} 데이터 없음")
        return None

    n_ax = 1 + len(extra_cols)
    fig, axes = plt.subplots(n_ax, 1, figsize=figsize, sharex=True)
    axes = np.atleast_1d(axes)

    t = pd.to_datetime(day[time_col])
    axes[0].plot(t, day[score_col], lw=.7, alpha=.8, color="tab:blue")
    axes[0].axhline(threshold, color="red", ls="--", lw=1, label=f"임계치 {threshold:.3f}")
    an = day[day[score_col] > threshold]
    axes[0].scatter(pd.to_datetime(an[time_col]), an[score_col], color="red", s=22, zorder=5,
                    label=f"이상 {len(an)}건 / 전체 {len(day)} ({len(an)/len(day)*100:.2f}%)")
    axes[0].set_ylabel("재구성오차")
    axes[0].legend(loc="upper left", fontsize=9)
    axes[0].grid(alpha=.3)
    axes[0].set_title(f"{date} — 이상점수 추이")

    for ax, col in zip(axes[1:], extra_cols):
        ax.plot(t, day[col], lw=.7, alpha=.8, color="tab:green")
        for tt in pd.to_datetime(an[time_col]):
            ax.axvline(tt, color="red", alpha=.15, lw=1)
        ax.set_ylabel(col)
        ax.grid(alpha=.3)

    axes[-1].set_xlabel("시각")
    plt.tight_layout()
    return fig


# ---------------------------------------------------------------- 기여도 히트맵
def contribution_heatmap(per_feat, feature_cols, scores, times, threshold,
                          top_n=40, figsize=(14, 10)):
    """이상 상위 N건 x 피처 기여도(%) 히트맵. 어떤 피처가 이상을 만들었는지 한눈에."""
    idx = np.argsort(-scores)
    idx = [i for i in idx if scores[i] > threshold][:top_n]
    if not idx:
        print("임계치를 넘는 건이 없습니다")
        return None, None

    mat = per_feat[idx]
    mat = mat / mat.sum(axis=1, keepdims=True) * 100
    labels = [f"{pd.to_datetime(times[i]).strftime('%m-%d %H:%M')}  ({scores[i]:.2f})" for i in idx]

    # 기여가 큰 피처가 위로 오도록 정렬
    order = np.argsort(-mat.mean(axis=0))
    mat, cols = mat[:, order], [feature_cols[j] for j in order]

    fig, ax = plt.subplots(figsize=figsize)
    im = ax.imshow(mat, aspect="auto", cmap="YlOrRd")
    ax.set_xticks(range(len(cols)))
    ax.set_xticklabels(cols, rotation=90, fontsize=8)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=7.5)
    ax.set_title(f"이상 상위 {len(idx)}건의 피처별 기여도 (%)  — 왼쪽일수록 자주 범인인 피처")
    fig.colorbar(im, ax=ax, label="기여도 (%)", fraction=0.02)
    plt.tight_layout()
    return fig, pd.DataFrame(mat, index=labels, columns=cols)


# ---------------------------------------------------------------- 원본 확대
def zoom_raw(raw_df, center_time, pump_id, before_sec=20, after_sec=20,
              inj_col=None, figsize=(13, 8)):
    """이상이 난 시점 앞뒤의 원본 틱을 PT / FT / Hz 로 펼쳐 본다."""
    t = pd.to_datetime(center_time)
    lo, hi = t - pd.Timedelta(seconds=before_sec), t + pd.Timedelta(seconds=after_sec)
    seg = raw_df[(raw_df.index >= lo) & (raw_df.index <= hi)]
    if seg.empty:
        print("구간에 데이터 없음")
        return None

    series = [(f"Scale_Out___PT_{pump_id}", "PT (압력)", "tab:orange"),
              (f"Scale_Out___FT_{pump_id}", "FT (유량)", "tab:purple"),
              (f"Hz_Out_{pump_id}", "Hz (인버터)", "tab:blue"),
              (f"Pump_BuildUp_{pump_id}", "BuildUp", "black")]
    series = [s for s in series if s[0] in seg.columns]

    fig, axes = plt.subplots(len(series), 1, figsize=figsize, sharex=True)
    axes = np.atleast_1d(axes)
    for ax, (col, name, color) in zip(axes, series):
        ax.plot(seg.index, seg[col], marker="o", ms=3, lw=.9, color=color,
                drawstyle="steps-post" if "BuildUp" in col else "default")
        ax.set_ylabel(name, fontsize=9)
        ax.axvline(t, color="red", ls="--", alpha=.7)
        ax.grid(alpha=.3)
        if inj_col and inj_col in seg.columns:
            on = seg[seg[inj_col] != 0]
            for tt in on.index:
                ax.axvline(tt, color="green", alpha=.25, lw=1)

    sv = f"g_s_SV_{pump_id}"
    if sv in seg.columns:
        axes[1].plot(seg.index, seg[sv], ls=":", color="green", label="SV")
        axes[1].legend(fontsize=8)

    axes[0].set_title(f"{t}  부근 원본 (빨강 점선 = 이상 시점, 초록 = 사출)")
    axes[-1].set_xlabel("시각")
    plt.tight_layout()
    return fig
