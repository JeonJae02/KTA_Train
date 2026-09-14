"""ARX 기반 60초 온도변화(ΔT) 예측 모델 — 탱크별로 따로 학습하고, feeding 포함/미포함을 비교한다.

**예측 대상은 온도 수준이 아니라 변화량이다.** 수준을 맞히면 "현재값 그대로 내놓기"
만으로 R²=0.99(I2)가 나와서, 모델이 액추에이터를 이해했는지 알 수 없다.

**입력은 "미래 60초 동안의 액추에이터 상태"다.** 미래를 맞히는 게 목적이 아니라
"이만큼 명령이 나갔으면 온도가 이만큼 변해야 정상"을 모델링하는 게 목적이라서다
(릴레이 고장 감지). 열 지연 때문에 과거 60초 항도 같이 넣는다.

    ΔT(t → t+60s) = a·T(t) + Σ b·u_future + Σ c·u_past + d

**heat 와 cool 은 완전 상보(둘 다 ON/OFF 인 순간이 0건)라서 동시에 넣을 수 없다**
(heat = 1 - cool, 완전 다중공선성). heat 만 넣고, 그 계수를 "full-heat 와 full-cool
의 차이"로 읽는다. 어차피 둘 중 하나라도 릴레이가 죽으면 이 차이가 무너지므로
고장 감지 목적에는 충분하다.

제외 구간: 00:00~07:00 (정체불명의 운전 모드로 의심됨), 알람(DIAGNOSTIC) 구간.
윈도우가 제외 구간을 걸치는 행도 같이 버린다.
"""

import glob
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.rcParams["font.family"] = "Malgun Gothic"
matplotlib.rcParams["axes.unicode_minus"] = False

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from Tank_Temp_Factor_Analysis import TANKS, WAGON_TAG

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAVE_DIR = os.path.join(BASE_DIR, "extracted_csv")
OUT_DIR = os.path.join(BASE_DIR, "analysis_out")

GRID_SEC = 5            # 리샘플 간격
HORIZON_SEC = 60        # 예측 지평
EXCLUDE_HOURS = (0, 7)  # 이 시간대 제외 [00:00, 07:00)
TEST_FRACTION = 0.3     # 뒤쪽 30% 를 테스트로 (시간순 분할)

#: **라인 정지 구간 제외.** wagon 카운터가 이 시간 안에 한 번도 안 움직였으면
#: 설비가 서 있는 것으로 본다. 2026-09-13 은 하루 종일 가동률 0% 였고, 그때는
#: exchanger/feeding 이 정확히 0회에 온도도 거의 안 움직인다(ΔT 표준편차 7.5 -> 0.64).
#: 생산 중 데이터로 학습한 모델을 여기에 갖다 대면 R² 가 음수로 무너진다.
#: 30분으로 잡은 이유: 정상 사이클에도 7~9분짜리 중간 정지가 흔해서, 그건
#: 살려둬야 한다(그 동안에도 온도 제어는 계속 돈다).
RUNNING_WINDOW_MIN = 30

STEPS = HORIZON_SEC // GRID_SEC   # 12

MODEL_A = ["T", "heat_fut", "heat_past", "exch_fut", "exch_past"]
MODEL_B = MODEL_A + ["feed_fut", "feed_past"]


def build_frame(tank_key):
    """탱크 CSV -> 5초 격자 + ARX 특징. 제외 구간은 mask 로 표시만 하고 나중에 버린다."""
    cfg = TANKS[tank_key]
    path = sorted(glob.glob(os.path.join(SAVE_DIR, f"{tank_key}_Temp_vs_Factors_*.csv")))[-1]
    raw = pd.read_csv(path, index_col="Time", parse_dates=True).sort_index()

    num = pd.DataFrame({
        "T": raw[cfg["temp"]],
        "heat": raw[cfg["heat"]],
        "cool": raw[cfg["cool"]],
        "exch": raw[cfg["exchanger"]],
        "feed": raw[cfg["feeding"]],
        "wagon": raw[WAGON_TAG],
        "diag": (raw["source"] == "DIAGNOSTIC").astype(float),
    })
    # 특징은 **끊기지 않은 전체 시계열** 위에서 먼저 만든다.
    # (00~07 을 먼저 지우면 롤링 윈도우가 그 구멍을 건너뛰어 붙어버린다)
    g = num.resample(f"{GRID_SEC}s").ffill().dropna()

    out = pd.DataFrame(index=g.index)
    out["T"] = g["T"]
    out["y"] = g["T"].shift(-STEPS) - g["T"]          # 목표: 앞으로 60초 동안의 변화량

    for name, col in [("heat", "heat"), ("exch", "exch"), ("feed", "feed")]:
        # 미래 60초 ON 비율: [t, t+60)
        out[f"{name}_fut"] = col_fut = g[col].rolling(STEPS).mean().shift(-(STEPS - 1))
        # 과거 60초 ON 비율: [t-60, t)
        out[f"{name}_past"] = g[col].rolling(STEPS).mean().shift(1)

    # 라인 가동 여부: wagon 이 최근 RUNNING_WINDOW_MIN 분 안에 움직였는가
    moved = (g["wagon"].diff().abs() > 0).astype(float)
    running = moved.rolling(int(RUNNING_WINDOW_MIN * 60 / GRID_SEC), min_periods=1).max()

    # 유효 구간: [t-60, t+60] 전체가 허용 시간대 + 알람 없음 + 라인 가동중
    hour = g.index.hour
    ok = pd.Series(((hour < EXCLUDE_HOURS[0]) | (hour >= EXCLUDE_HOURS[1])) & (g["diag"] == 0),
                   index=g.index).astype(float) * running
    span = 2 * STEPS + 1
    out["ok"] = ok.rolling(span, center=True).min()

    # heat/cool 상보성 확인 (완전 상보면 cool 은 넣을 수 없다)
    both_on = int(((g["heat"] == 1) & (g["cool"] == 1)).sum())
    both_off = int(((g["heat"] == 0) & (g["cool"] == 0)).sum())

    out = out.dropna()
    out = out[out["ok"] == 1].drop(columns="ok")
    return out, (both_on, both_off)


def ols(X, y):
    """계수와 표준오차를 같이 돌려준다 (해석용)."""
    XtX_inv = np.linalg.pinv(X.T @ X)
    beta = XtX_inv @ X.T @ y
    resid = y - X @ beta
    dof = max(len(y) - X.shape[1], 1)
    sigma2 = (resid @ resid) / dof
    se = np.sqrt(np.diag(sigma2 * XtX_inv))
    return beta, se


def fit_eval(df, features):
    """시간순 분할 학습/평가. 윈도우 겹침 방지를 위해 경계에 구멍(purge)을 둔다."""
    n = len(df)
    split = int(n * (1 - TEST_FRACTION))
    train = df.iloc[: split - STEPS]      # purge
    test = df.iloc[split:]

    Xtr = np.column_stack([np.ones(len(train))] + [train[f].values for f in features])
    Xte = np.column_stack([np.ones(len(test))] + [test[f].values for f in features])
    ytr, yte = train["y"].values, test["y"].values

    beta, se = ols(Xtr, ytr)
    pred = Xte @ beta

    rmse = float(np.sqrt(np.mean((yte - pred) ** 2)))
    r2 = float(1 - np.sum((yte - pred) ** 2) / np.sum((yte - yte.mean()) ** 2))
    return {
        "beta": beta, "se": se, "features": ["const"] + features,
        "n_train": len(train), "n_test": len(test),
        "rmse": rmse, "r2": r2,
        "pred": pred, "actual": yte, "test_index": test.index,
        "test_df": test,
    }


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    summary, coef_rows = [], []

    for tank in TANKS:
        df, (both_on, both_off) = build_frame(tank)
        if len(df) < 1000:
            print(f"⚠️ [{tank}] 유효 표본 {len(df)}개 — 건너뜀")
            continue

        A = fit_eval(df, MODEL_A)
        B = fit_eval(df, MODEL_B)

        # feeding 이 실제로 작동한 구간에서만 따로 비교 (전체 평균으로는 1% 구간이 묻힌다)
        mask = B["test_df"]["feed_fut"].values > 0
        if mask.sum() >= 30:
            fa = float(np.sqrt(np.mean((A["actual"][mask] - A["pred"][mask]) ** 2)))
            fb = float(np.sqrt(np.mean((B["actual"][mask] - B["pred"][mask]) ** 2)))
        else:
            fa = fb = np.nan

        summary.append({
            "탱크": tank, "학습": A["n_train"], "테스트": A["n_test"],
            "heat/cool_둘다ON": both_on, "heat/cool_둘다OFF": both_off,
            "A_R2": round(A["r2"], 4), "B_R2": round(B["r2"], 4),
            "A_RMSE": round(A["rmse"], 3), "B_RMSE": round(B["rmse"], 3),
            "feed구간_n": int(mask.sum()),
            "feed구간_A_RMSE": round(fa, 3) if fa == fa else None,
            "feed구간_B_RMSE": round(fb, 3) if fb == fb else None,
        })

        for name, res in [("A", A), ("B", B)]:
            for f, b, s in zip(res["features"], res["beta"], res["se"]):
                coef_rows.append({"탱크": tank, "모델": name, "특징": f,
                                  "계수": round(float(b), 4), "표준오차": round(float(s), 4),
                                  "t값": round(float(b / s), 2) if s > 0 else None})

        # 테스트 구간 실제 vs 예측
        fig, ax = plt.subplots(figsize=(15, 4))
        ax.plot(B["test_index"], B["actual"], color="#444444", linewidth=0.8, label="실제 ΔT")
        ax.plot(B["test_index"], B["pred"], color="#d62728", linewidth=0.8, alpha=0.8, label="예측 ΔT (모델 B)")
        ax.set_title(f"{tank} — 60초 온도변화 예측 (테스트 구간)  R²={B['r2']:.3f}  RMSE={B['rmse']:.2f}")
        ax.set_ylabel("ΔT (60초)")
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(os.path.join(OUT_DIR, f"ARX_{tank}_pred.png"), dpi=120)
        plt.close(fig)

    s = pd.DataFrame(summary)
    c = pd.DataFrame(coef_rows)
    s.to_csv(os.path.join(OUT_DIR, "ARX_모델비교.csv"), index=False, encoding="utf-8-sig")
    c.to_csv(os.path.join(OUT_DIR, "ARX_계수.csv"), index=False, encoding="utf-8-sig")

    print("=== 모델 A(heat,cool,exchanger) vs 모델 B(+feeding) ===")
    print(s.to_string(index=False))
    print(f"\n저장: {OUT_DIR}\\ARX_모델비교.csv, ARX_계수.csv, ARX_<탱크>_pred.png")
    return s, c


if __name__ == "__main__":
    main()
