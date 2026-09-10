# Fast_Log_Ext.py
"""`Log_Extractor.LogExtractor` 로 통합됨. 이 파일은 이름만 유지하는 껍데기다.

**왜 통합했나.** 예전 `Fast_LogExtractor` 는 Flux 쿼리에 태그 필터가 없었다.
서버에서 버킷의 전체 시리즈(약 1만개 — 물리점 하나가 사람이름과 IL주소
두 벌로 적재된다)를 피벗한 뒤, 그걸 전부 네트워크로 받아, 클라이언트에서
필요한 컬럼만 골라내고 나머지를 버렸다.

    |> filter(fn: (r) => r._measurement == "plc_line2")
    |> pivot(...)                      ← 여기서 전체를 펼침
    ...
    chunk_df = chunk_df[cols_to_keep]  ← 받은 뒤에야 걸러냄

이름과 달리 더 느렸고, 무엇보다 **현장 InfluxDB 의 메모리를 통째로 먹었다.**
`Log_Extractor` 쪽은 처음부터 pivot 앞에서 걸렀으므로 그쪽으로 합친다.

기존 노트북(`from Fast_Log_Ext import Fast_LogExtractor`)은 그대로 돌아간다.
새로 쓰는 코드는 `Log_Extractor.LogExtractor` 를 직접 쓰면 된다.
"""

from Log_Extractor import LogExtractor


class Fast_LogExtractor(LogExtractor):
    """구버전 이름 호환용. 동작은 LogExtractor 와 완전히 같다."""
    pass


if __name__ == "__main__":
    print("이 파일은 Log_Extractor.LogExtractor 로 통합되었습니다.")
    print("실행 예시는 Log_Extractor.py 의 __main__ 을 보세요.")
