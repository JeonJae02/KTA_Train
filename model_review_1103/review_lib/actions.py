"""실무자 일지(filtering.xlsx) → 검토용 조치 표.

규칙
- 한글 열 = 일지 원본 그대로. 영문 열 = 우리가 붙인 것.
- 원본 xlsx 는 고치지 않는다. 사람 판단(설비키·모델범위·사건 묶음)은
  `warning/actions/action_map.csv` 에 적고, build_inputs.py 가 둘을 합친다.
"""
import re

import pandas as pd

from . import paths

#: 2라인 필터 시트 → 도메인. "필터링 된 것" 은 세 시트의 합이라 쓰지 않는다.
SHEET_DOMAIN = {"CV": "conveyor", "HD+PUMP": "pump", "Others": "etc"}

#: 원본에서 남길 열. 나머지(업무구분·펌프관련·날짜근거·기준일·일지작성일·적용범위·원문소요_분·출처수)는 버린다.
KEEP_COLS = ["작업ID", "작업일", "기록시간", "기록구간_분", "라인번호", "공정표기", "설비분류",
             "부품태그", "설비상세", "이슈", "조치", "결과", "생산영향", "담당", "확인메모", "중복의심", "출처"]

#: action_map.csv 열 — 사람이 채운다.
MAP_COLS = ["작업ID", "incident_id", "domain", "device_key", "sub_unit", "wagon_no",
            "model_scope", "expected_reasons", "map_note"]

_T = re.compile(r"(\d{1,2}):(\d{2})")


def parse_time_range(work_date, raw):
    """기록시간 문자열 → (start, end). 못 읽으면 NaT.

    처리하는 모양: '09:30~10:30', '19:40:00', '24:40~01:20'(=다음날 00:40),
    '08:50;11:00', '17:00~18:26;20:45;21:21~22:26;22:26~' (첫 시각 ~ 마지막 확정 시각).
    끝이 시작보다 이르면 자정을 넘긴 것으로 본다.
    """
    if pd.isna(work_date) or pd.isna(raw):
        return pd.NaT, pd.NaT
    day = pd.Timestamp(work_date).normalize()
    times = _T.findall(str(raw))
    if not times:
        return pd.NaT, pd.NaT

    def at(hm, base):
        h, m = int(hm[0]), int(hm[1])
        return base + pd.Timedelta(hours=h, minutes=m)   # 24:40 → 다음날 00:40

    start = at(times[0], day)
    end = at(times[-1], day) if ("~" in str(raw) or len(times) > 1) else pd.NaT
    if pd.notna(end) and end < start:
        end += pd.Timedelta(days=1)
    return start, end


def read_sheet(sheet):
    df = pd.read_excel(paths.FILTERING_XLSX, sheet_name=sheet)
    df["작업일"] = pd.to_datetime(df["작업일"], errors="coerce")
    return df


def journal_of(action_id):
    """작업ID 접두로 일지 종류. M=기계실, C/G/X=공무반."""
    s = str(action_id)
    return "기계실" if s.startswith("M-") else "공무반"


def load_filtered(start=None):
    """2라인 필터 시트 세 개를 합쳐 검토 기간만 남긴다."""
    start = start or paths.REVIEW_START
    parts = []
    for sheet, dom in SHEET_DOMAIN.items():
        df = read_sheet(sheet)
        df = df[df["작업일"] >= start][KEEP_COLS].copy()
        df["sheet_src"] = sheet
        df["sheet_domain"] = dom
        parts.append(df)
    return pd.concat(parts, ignore_index=True)


def load_unfiltered_line2(filtered_ids, start=None):
    """'전체' 시트의 2라인 기록 중 필터 시트에 없는 것. 놓친 후보 확인용."""
    start = start or paths.REVIEW_START
    df = read_sheet("전체")
    line2 = df["라인번호"].astype(str).str.fullmatch(r"2")
    df = df[(df["작업일"] >= start) & line2 & ~df["작업ID"].isin(filtered_ids)][KEEP_COLS].copy()
    df["sheet_src"] = "전체"
    return df


def add_derived(df):
    se = df.apply(lambda r: parse_time_range(r["작업일"], r["기록시간"]), axis=1, result_type="expand")
    df["start_dt"], df["end_dt"] = se[0], se[1]
    df["journal"] = df["작업ID"].map(journal_of)
    df["작업일"] = df["작업일"].dt.date
    return df
