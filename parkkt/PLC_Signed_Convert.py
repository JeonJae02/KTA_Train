"""InfluxDB 에서 뽑은 CSV 의 16비트 PLC 레지스터 값을 부호 있는 실수로 바꿔서 다시 저장한다.

**왜 필요한가.** `Ana_In / Gain / OffSet / Scale_Out` 은 PLC 쪽에서 부호 있는
16비트 정수(-32768~32767)로 관리되는데, InfluxDB 에는 부호 없는 값(0~65535)
그대로 찍혀 있다. 예: OffSet___TT_P5 가 CSV 에는 `65526` 으로 보이지만 실제
값은 `-10` 이다 (65526 - 65536). 32767 보다 큰 값은 전부 이런 식으로
"65536 을 빼야 하는" 값이지, 어떤 고정된 숫자로 치환하는 게 아니다.

    실제 값 = raw 값                (raw <= 32767 일 때)
    실제 값 = raw 값 - 65536        (raw >  32767 일 때)

`Scale_Out = (Ana_In/Ana_Max)*Scale_Max*(Gain/1000)+OffSet` 공식은 이 변환을
적용한 뒤에만 맞아떨어진다는 걸 P1~P5/ISO1~3/RAW_EX_ISO1,3 데이터로 확인했다.

이 스크립트는 `TT_Group_Extractor.py` / `Heat_Cool_Out_Extractor.py` 가
저장한 CSV(들)를 읽어 **모든 숫자 컬럼**에 이 변환을 적용하고, 원본은 그대로
둔 채 별도 폴더에 다시 저장한다. (`source` 같은 문자열 컬럼은 건드리지 않는다.)
"""

import argparse
import glob
import os

import pandas as pd

UINT16_SIGNED_CUTOFF = 32767
UINT16_MOD = 65536

SRC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "extracted_csv")
DST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "extracted_csv_signed")


def to_signed16(series):
    """raw(0~65535) 값을 부호 있는 16비트 실제 값으로 바꾼다."""
    s = pd.to_numeric(series, errors="coerce")
    return s.where(s <= UINT16_SIGNED_CUTOFF, s - UINT16_MOD)


def convert_dataframe(df):
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            continue  # 'source' 같은 문자열 컬럼은 그대로 둔다
        out[col] = to_signed16(out[col])
    return out


def convert_file(input_path, output_path):
    df = pd.read_csv(input_path, index_col="Time", parse_dates=True)
    out = convert_dataframe(df)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    out.to_csv(output_path, encoding="utf-8-sig")
    print(f"  {os.path.basename(input_path)} -> {output_path}")


def convert_dir(src_dir=SRC_DIR, dst_dir=DST_DIR, pattern="*.csv"):
    files = sorted(glob.glob(os.path.join(src_dir, pattern)))
    if not files:
        print(f"⚠️ 변환할 CSV 가 없습니다: {src_dir}")
        return []

    print(f"🔁 {len(files)}개 CSV 변환 시작 ({src_dir} -> {dst_dir})")
    out_paths = []
    for f in files:
        out_path = os.path.join(dst_dir, os.path.basename(f))
        convert_file(f, out_path)
        out_paths.append(out_path)
    print("✅ 변환 완료")
    return out_paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", default=None,
                         help="변환할 CSV 파일 하나. 생략하면 extracted_csv/ 전체를 변환한다.")
    parser.add_argument("-o", "--output", default=None,
                         help="단일 파일 변환 시 저장 경로 (기본: extracted_csv_signed/ 아래 같은 이름)")
    args = parser.parse_args()

    if args.input:
        out_path = args.output or os.path.join(DST_DIR, os.path.basename(args.input))
        convert_file(args.input, out_path)
    else:
        convert_dir()
