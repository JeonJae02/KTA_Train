# tools_yeji/common/time_utils.py
"""시각 변환, 운영일 경계, 근무 교대."""
import numpy as np
import pandas as pd

from config.operation_config import SHIFTS

KST = "Asia/Seoul"
DAY_START_HOUR = 6      #: 운영일 경계(한국시간). 06:00 전은 전날로 친다


def _build_shifts():
    """SHIFTS -> [(시작, 끝)] 자정 기준 Timedelta. 순서·범위가 어긋나면 부팅 때 터뜨린다."""
    def hm(s):
        h, m = map(int, s.split(":"))
        return pd.Timedelta(hours=h, minutes=m)

    out = [(hm(s), hm(e)) for s, e in SHIFTS]
    for i, (s, e) in enumerate(out):
        if not (pd.Timedelta(0) <= s < e <= pd.Timedelta(days=1)):
            raise ValueError(f"[time_utils] 근무 {SHIFTS[i]} — 시작 < 끝, 같은 날 안이어야 한다")
        if i and s < out[i - 1][1]:
            raise ValueError(f"[time_utils] 근무 {SHIFTS[i]} 가 앞 근무 {SHIFTS[i - 1]} 와 겹친다")
    return out


_SHIFTS = _build_shifts()


def to_kst(t):
    """시각 -> tz 없는 한국시간 Timestamp. epoch 초(float) 나 tz 붙은 시각도 받는다.

    tz 없는 시각은 이미 한국시간으로 본다.
    """
    if isinstance(t, (int, float, np.integer, np.floating)):
        return pd.Timestamp(t, unit="s", tz="UTC").tz_convert(KST).tz_localize(None)
    t = pd.Timestamp(t)
    if t.tzinfo is not None:
        t = t.tz_convert(KST).tz_localize(None)
    return t


def to_utc_iso(t):
    """시각 -> 프론트·uplink 로 보내는 UTC ISO 문자열 ("2026-10-08T04:30:00.000Z").

    받는 형태는 to_kst 와 같다 (epoch 초, tz 붙은 시각, tz 없는 한국시간).
    """
    t = to_kst(t).tz_localize(KST).tz_convert("UTC").round("ms")
    return t.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def day_key(t):
    """오전 6시 경계 기준 '운영일'. 06:00 전은 전날로 친다."""
    return (t - pd.Timedelta(hours=DAY_START_HOUR)).date()


def hold_until(t):
    """경보 유지 만료 시각 — t 이후 처음 시작하는 근무의 끝. t 는 tz 없는 KST Timestamp.

    근무 중이면 다음 근무 끝, 휴게·야간이면 곧 시작할 근무 끝. 근무 시작 시각 정각은
    그 근무 안으로 친다(다음 근무 끝까지).
    근무표를 못 읽었으면(SHIFTS 비어 있음) 다음 운영일 경계(06:00)로 대신한다 — 경보가 영원히 남지 않게.
    """
    if not _SHIFTS:
        return pd.Timestamp(day_key(t)) + pd.Timedelta(days=1, hours=DAY_START_HOUR)
    for day in (0, 1):
        base = t.normalize() + pd.Timedelta(days=day)
        for start, end in _SHIFTS:
            if base + start > t:
                return base + end


def is_missing(v):
    """이번 tick 에 값이 없음 (None / NaN)."""
    return v is None or pd.isna(v)
