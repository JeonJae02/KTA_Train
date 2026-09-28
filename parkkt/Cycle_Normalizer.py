# parkkt/Cycle_Normalizer.py
"""시작 조건에 끌려다니는 피처를 '표준화 잔차'로 바꾼다.

왜 필요한가
-----------
`Decay_Slope_PT`(펌프를 끈 뒤 압력이 빠지는 기울기)는 **끄기 직전 압력이 높을수록
가파릅니다.** 실측(P1, 32,817 사이클):

    시작압력  90 -> 평균 기울기  -6.2
    시작압력 133 -> 평균 기울기 -19.6      (약 3배)

그래서 원시 기울기만 보면 "이 사이클이 유독 빨리 샜다" 인지 "그냥 시작 압력이
높았을 뿐" 인지 구분이 안 된다. 반대로 압력이 높은데도 **덜** 빠진 사이클(어딘가
막혀 압력이 갇힌 신호일 수 있다)은 원시값이 평범해서 그냥 지나친다.

어떻게 고치나
-------------
두 단계를 모두 적용한다. 한 단계만으로는 부족하다는 걸 실측으로 확인했다.

    1) 위치 보정 : 예상기울기 = a*P0 + b
    2) 폭   보정 : 예상흩어짐 = (sa*P0 + sb) * sqrt(pi/2)
    3) 피처      = (실제 - 예상기울기) / 예상흩어짐

실측 비교 (시작압력과의 상관 / 구간별 표준편차 최대·최소 비율):

    원본        -0.530 / 1.93
    잔차만      -0.001 / 1.91     <- 평균은 맞췄지만 흩어짐이 그대로
    표준화 잔차 +0.009 / 1.08     <- 둘 다 해결

2번이 필요한 이유는 흩어짐도 압력을 따라가기 때문이다(5.02 ~ 9.61, 1.91배).
같은 잔차 -12 라도 압력 92 에서는 -2.04 시그마(상위 2.3%)지만 압력 131 에서는
-1.24 시그마(상위 14.4%) 로, 심각도가 전혀 다르다.

구간(bin)을 나누지 않는 이유
----------------------------
10구간으로 쪼개 확인했을 때 직선 예측과 실제 구간평균의 차이가 최대 0.87 로,
기울기 표준편차(8.56)의 10% 수준이었다. 관계가 충분히 선형이라 구간을 나눌
이유가 없고, 나누면 경계에서 값이 튀고 구간 수 선택이 임의적이 된다.

누수 방지
---------
두 회귀 모두 **학습 구간에서만** 적합하고 Valid/Test 에는 그 식을 그대로 적용한다.
전체로 적합하면 "학습 때와 달라진 정도"가 0 으로 눌려서 안 보이게 된다.
"""
import numpy as np
import pandas as pd

#: 정규화할 피처 -> 그 피처를 끌고 다니는 기준 변수들
#: (상관계수는 P1 32,817 사이클 실측)
NORMALIZE_SPECS = {
    "Decay_Slope_PT":   ["PT_at_End"],            # -0.530
    "Decay_Slope_FT":   ["FT_at_End"],            # -0.888
    "FT_Jump_From_Pre": ["PreBuildUp_FT"],        # -0.517
    "Cum_FT_Error":     ["SV_mean", "N_Ticks"],   #  0.936 (FT_std 경유)
    "FT_std":           ["FT_mean"],              #  0.936
    "Current_Proxy_I":  ["PT_mean"],              #  0.997
}

SUFFIX = "_z"          # 정규화된 피처 이름에 붙는 꼬리
_MAD_TO_SD = np.sqrt(np.pi / 2)   # 평균절대편차 -> 표준편차 환산 (정규분포 가정)


class ResidualNormalizer:
    def __init__(self, specs=None):
        self.specs = specs or NORMALIZE_SPECS
        self.coef_ = {}       # target -> (평균회귀 계수, 흩어짐회귀 계수, 최소 스케일)
        self.fitted_ = False

    @staticmethod
    def _design(df, drivers):
        X = df[drivers].to_numpy(dtype=float)
        return np.column_stack([X, np.ones(len(X))])

    def fit(self, df):
        """df 는 학습 구간 사이클만 담고 있어야 한다."""
        self.coef_ = {}
        for target, drivers in self.specs.items():
            need = [target] + drivers
            sub = df.dropna(subset=need)
            if len(sub) < 50:
                print(f"  ! {target}: 표본 {len(sub)}개로 부족 - 정규화 건너뜀")
                continue

            X = self._design(sub, drivers)
            y = sub[target].to_numpy(dtype=float)

            beta, *_ = np.linalg.lstsq(X, y, rcond=None)          # 위치
            resid = y - X @ beta
            gamma, *_ = np.linalg.lstsq(X, np.abs(resid), rcond=None)  # 폭

            # 스케일이 0 이나 음수로 가면 나눗셈이 터지므로 바닥을 깐다
            floor = max(np.abs(resid).mean() * 0.1, 1e-6)
            self.coef_[target] = (beta, gamma, floor)
        self.fitted_ = True
        return self

    def transform(self, df):
        """정규화된 컬럼(<이름>_z)을 덧붙인 복사본을 돌려준다. 원본 컬럼은 남긴다."""
        if not self.fitted_:
            raise RuntimeError("fit() 을 먼저 호출하세요")
        out = df.copy()
        for target, (beta, gamma, floor) in self.coef_.items():
            drivers = self.specs[target]
            X = self._design(out, drivers)
            pred = X @ beta
            scale = np.clip(X @ gamma, floor, None) * _MAD_TO_SD
            out[target + SUFFIX] = (out[target].to_numpy(dtype=float) - pred) / scale
        return out

    def fit_transform(self, train_df, *others):
        self.fit(train_df)
        return tuple(self.transform(d) for d in (train_df,) + others)

    def report(self, df):
        """정규화가 실제로 먹혔는지 확인용 표."""
        rows = []
        for target, drivers in self.specs.items():
            if target not in self.coef_:
                continue
            zcol = target + SUFFIX
            if zcol not in df.columns:
                continue
            d0 = drivers[0]
            sub = df.dropna(subset=[target, zcol, d0])
            rows.append({
                "피처": target,
                "기준변수": d0,
                "원본 상관": round(sub[target].corr(sub[d0]), 3),
                "정규화 후 상관": round(sub[zcol].corr(sub[d0]), 3),
                "정규화 후 평균": round(sub[zcol].mean(), 3),
                "정규화 후 표준편차": round(sub[zcol].std(), 3),
            })
        return pd.DataFrame(rows)
