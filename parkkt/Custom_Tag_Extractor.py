"""아무 태그나 몇 개 골라서 CSV로 뽑는 범용 스크립트.

그룹 단위(P1~P5 등)로 고정된 게 아니라, 그때그때 궁금한 태그 몇 개를
바로 뽑고 싶을 때 쓴다. `Log_Extractor.LogExtractor` 를 그대로 쓴다.

사용 예:
    python Custom_Tag_Extractor.py "워킹 Tank 1 BACK Heater"
    python Custom_Tag_Extractor.py "tagA" "tagB" --start "2026-09-10 06:00:00" --name My_Check
"""

import os
import sys
from datetime import datetime

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

from Log_Extractor import LogExtractor

ENV_PATH = os.path.join(ROOT_DIR, ".env")
SAVE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "extracted_csv")

START_TIME = "-1d"
END_TIME = "now()"


def main(tags, start_time=START_TIME, end_time=END_TIME, name=None):
    extractor = LogExtractor(env_path=ENV_PATH)
    df = extractor.get_data(start_time=start_time, end_time=end_time, target_tags=tags)

    if df.empty:
        print("⚠️ 저장할 데이터가 없습니다.")
        return None

    os.makedirs(SAVE_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    prefix = name or "Custom"
    file_path = os.path.join(SAVE_DIR, f"{prefix}_{timestamp}.csv")
    df.to_csv(file_path, encoding="utf-8-sig")
    print(f"💾 저장 완료: {file_path}")
    return file_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tags", nargs="+", help="추출할 태그 이름(들)")
    parser.add_argument("--start", default=START_TIME,
                         help="시작 시각. '-1d' 같은 상대시간 또는 '2026-09-10 06:00:00'(KST) 형식")
    parser.add_argument("--end", default=END_TIME, help="끝 시각. 기본 now()")
    parser.add_argument("--name", default=None, help="저장 파일 이름 접두어 (기본: Custom)")
    args = parser.parse_args()

    main(args.tags, start_time=args.start, end_time=args.end, name=args.name)
