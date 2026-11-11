"""검토용 그림 두 가지: 설비별 경고·조치 타임라인, 조치 전후 원시 태그."""
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import font_manager

# 맥·윈도우·리눅스 중 설치된 한글 글꼴 하나만 고른다 (없는 글꼴을 넣으면 경고가 쏟아진다)
_installed = {f.name for f in font_manager.fontManager.ttflist}
plt.rcParams["font.family"] = next((f for f in ("AppleGothic", "Malgun Gothic", "NanumGothic") if f in _installed),
                                   "DejaVu Sans")
plt.rcParams["axes.unicode_minus"] = False


def timeline(ep, inc, row_col="key", start=None, end=None, ax=None):
    """행 = 경고 대상(row_col), 가로 막대 = 경고 에피소드(onset~last_detected), 세로선 = 조치 시작."""
    ep = ep.copy()
    for c in ("onset", "last_detected"):
        ep[c] = pd.to_datetime(ep[c])
    if start is not None:
        ep = ep[ep["last_detected"] >= start]
    if end is not None:
        ep = ep[ep["onset"] <= end]
    rows = sorted(ep[row_col].astype(str).unique())
    ax = ax or plt.subplots(figsize=(16, max(3, 0.35 * len(rows))))[1]
    for i, r in enumerate(rows):
        for _, e in ep[ep[row_col].astype(str) == r].iterrows():
            ax.barh(i, e["last_detected"] - e["onset"], left=e["onset"], height=0.6,
                    color="tab:red" if e.get("max_level") == "알림" or e.get("max_sev") == 3 else "tab:orange",
                    alpha=0.7)
    for _, a in inc.iterrows():
        t = pd.Timestamp(a["start_dt"])
        ax.axvline(t, color="k", lw=1, ls="--")
        ax.text(t, len(rows) - 0.4, a["incident_id"], rotation=90, va="top", ha="right", fontsize=8)
    ax.set_yticks(range(len(rows)), rows)
    ax.set_xlim(start, end)
    ax.grid(axis="x", alpha=0.3)
    return ax


def around(df, tags, t0, t1=None, onsets=(), title=None, pad=pd.Timedelta(hours=6)):
    """조치 구간 [t0, t1] 전후 원시 태그. onsets 는 경고 onset 시각 목록(빨간 점선)."""
    t1 = t1 if t1 is not None and pd.notna(t1) else t0
    seg = df[(df.index >= t0 - pad) & (df.index <= t1 + pad)]
    fig, axes = plt.subplots(len(tags), 1, figsize=(16, 2.2 * len(tags)), sharex=True, squeeze=False)
    for ax, tag in zip(axes[:, 0], tags):
        if tag in seg:
            ax.plot(seg.index, seg[tag], lw=0.6)
        ax.set_ylabel(tag, rotation=0, ha="right", fontsize=8)
        ax.axvspan(t0, t1 if t1 > t0 else t0 + pd.Timedelta(minutes=10), color="gray", alpha=0.3)
        for o in onsets:
            ax.axvline(pd.Timestamp(o), color="tab:red", ls=":", lw=1)
    axes[0, 0].set_title(title or f"{t0:%m-%d %H:%M} 조치 전후")
    fig.tight_layout()
    return fig
