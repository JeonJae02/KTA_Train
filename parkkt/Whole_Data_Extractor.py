import os
import re
import time
import pandas as pd
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

#: 백엔드가 **값이 바뀐 태그만** 기록하고, 전체 스냅샷은 300초 주기로만 남긴다
#: (backend/utils/DB_Logger.py 의 SNAPSHOT_INTERVAL_SEC=300). 그래서 요청 구간
#: 맨 앞은 "아직 한 번도 안 나타난" 태그가 비어 있고 ffill 로도 못 채운다.
#: 시작을 이만큼 앞당겨 받은 뒤 잘라내야 첫 줄부터 값이 찬다.
#:
#: **300 이 아니라 600 인 이유.** 스냅샷 주기가 정확히 300초가 아니다. 실측하면
#: 300~302초로 흔들린다 — 매 틱 `now - last >= interval` 을 검사하는 방식이라
#: 틱 간격만큼 늦게 걸린다. 패딩이 주기와 딱 같으면 302초 간격일 때 300초 창에
#: 스냅샷이 하나도 안 들어오는 경우가 생긴다. 주기보다 넉넉해야 한다.
#: (600초를 받아도 버리는 건 10분치뿐이라 비용은 없다.)
SNAPSHOT_PAD_SEC = 600

#: 한 번에 요청할 구간. 길수록 왕복은 줄지만 서버 메모리를 오래 잡는다.
CHUNK_HOURS = 1

#: 온도 계열은 PLC 의 `Scale_Max___TT_*` 값에 따라 단위가 달라진다.
#: 100 이면 1℃, 1000 이면 0.1℃ 단위다. 2026-04 에는 100 이었고 지금은 1000 이라
#: **같은 태그인데 4월 데이터와 자릿수가 다르다.** 여기서 나누지는 않는다 —
#: PLC 의 실제 스케일 연산식을 확인하기 전에 값을 바꾸면 더 위험하다.
#: 대신 뽑을 때 현재 Scale_Max 를 조회해 경고로 알린다.
TEMP_HINT = ("Temp_PV", "Temp_SV", "Temp_H_Set", "Temp_L_Set")


class LogExtractor:
    def __init__(self, env_path=".env"):
        """
        팀원들과 깃허브로 협업하기 위해, 민감한 정보는 .env에서 불러옵니다.
        :param env_path: .env 파일이 있는 상대 경로 (구조에 맞게 수정하세요)
        """
        # .env 파일 로드
        load_dotenv(dotenv_path=env_path)

        db_port = os.getenv("INFLUXDB_PORT", "8086")
        self.db_url = os.getenv("INFLUXDB_URL", f"http://localhost:{db_port}")
        
        self.token = os.getenv("INFLUXDB_ADMIN_TOKEN")
        self.org = os.getenv("INFLUXDB_ORG")
        self.bucket = os.getenv("INFLUXDB_BUCKET")

        if not self.token:
            print("❌ [.env 오류] 토큰을 찾을 수 없습니다. .env 파일 경로와 내용을 확인해주세요.")

        # timeout 은 **밀리초**다. 예전 3000000 은 50분이라, 잘못된 쿼리 하나가
        # 현장 DB 를 그만큼 붙잡고 있었다. 청크 하나에 2분이면 충분하다.
        self.client = InfluxDBClient(url=self.db_url, token=self.token, org=self.org, timeout=120_000)
        self.query_api = self.client.query_api()
        
        print("🔌 [Extractor] InfluxDB 분석용 추출기 연결 완료!")

    def _parse_time(self, t_str):
        """
        입력된 시간 문자열을 분석하여 UTC datetime 객체로 반환합니다.
        1. now() / 상대시간(-5d 등) 처리
        2. UTC ISO 포맷(Z 포함) 처리 -> 변환 건너뜀
        3. 일반 날짜 문자열 처리 -> KST로 간주하고 UTC로 변환
        """
        now = datetime.now(timezone.utc)
        
        if t_str == "now()":
            return now
            
        if isinstance(t_str, str):
            # 1. 상대 시간 파싱 (-6h, -5d 등)
            if t_str.startswith("-"):
                val = int(''.join(filter(str.isdigit, t_str)))
                unit = t_str[-1]
                if unit == 'd': return now - timedelta(days=val)
                if unit == 'h': return now - timedelta(hours=val)
                if unit == 'm': return now - timedelta(minutes=val)
                return now - timedelta(hours=val)

            # 2. UTC ISO 포맷 체크 (Z가 붙어있으면 이미 UTC임)
            if 'Z' in t_str.upper() or '+00:00' in t_str:
                # pd.to_datetime이 알아서 UTC로 인식함
                return pd.to_datetime(t_str).to_pydatetime()

        # 3. 그 외 (예: "2026-04-10 13:00:00") -> KST로 간주하고 UTC로 변환
        try:
            return pd.to_datetime(t_str).tz_localize('Asia/Seoul').tz_convert('UTC').to_pydatetime()
        except Exception as e:
            print(f"⚠️ 시간 파싱 주의: {t_str}를 UTC로 변환하는 중 오류 발생, 원문 사용 시도. ({e})")
            return pd.to_datetime(t_str).to_pydatetime()

    @staticmethod
    def _tag_regex(tags):
        """태그 목록을 Flux 정규식으로 바꾼다.

        **`contains(value: r.tag_name, set: [...])` 를 쓰면 안 된다.** 그건
        스토리지 계층으로 내려가지 않아서, InfluxDB 가 시리즈를 **전부 읽은 뒤**
        Flux 엔진에서 하나씩 걸러낸다. 정규식은 밀어 넣어진다(pushdown).

        실측 — 태그 333개 · 5분 구간 · 결과는 580줄로 동일:

            contains()   7.1초   heap 89.9MB
            정규식        0.3초   heap 69.4MB      ← 24배

        구간이 길어질수록 차이가 벌어진다. 태그 이름에 `/ ( ) .` 같은 문자가
        들어 있으므로(`FT_P1_Imp/L HMI_Real`, `STK_Feed_Pump_Out_P1(BACK)`)
        반드시 이스케이프한다.
        """
        esc = [re.sub(r'([.^$*+?()\[\]{}|\\/])', r'\\\1', t) for t in tags]
        return "/^(" + "|".join(esc) + ")$/"

    def discover_tags(self, start_time, end_time):
        """구간 내 measurement `plc_line2` 에 실제로 값이 있었던 tag_name 을 모두 찾는다.

        pivot 없이 tag_name 만 distinct 로 뽑으므로 가볍다. `get_data` 를
        `target_tags` 없이 부르는 건 여전히 막혀 있다 — 대신 여기서 그 구간에
        **실제로 존재하는** 태그 목록을 먼저 받아서 `get_data(target_tags=...)`
        에 넘기는 두 단계 방식으로, 전체를 받으면서도 서버가 필요 이상을
        펼치지 않게 한다.
        """
        start_dt = self._parse_time(start_time)
        end_dt = self._parse_time(end_time)
        str_start = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        str_end = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        print(f"🔎 [Discover] {str_start} ~ {str_end} 구간의 태그 목록 조회 중 ...")

        flux_query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: {str_start}, stop: {str_end})
            |> filter(fn: (r) => r._measurement == "plc_line2")
            |> keep(columns: ["tag_name"])
            |> group()
            |> distinct(column: "tag_name")
            |> keep(columns: ["_value"])
        """
        df = self.query_api.query_data_frame(query=flux_query)
        if isinstance(df, list):
            df = pd.concat(df) if df else pd.DataFrame()

        if df.empty or "_value" not in df.columns:
            print("⚠️ 해당 구간에 tag_name 이 하나도 없습니다.")
            return []

        tags = sorted(df["_value"].dropna().unique().tolist())
        print(f"✅ [Discover] 태그 {len(tags)}개 발견")
        return tags

    def get_full_day(self, date_str, target_tags=None):
        """KST 기준 하루(00:00~24:00) 전체를 받는다.

        `target_tags` 를 안 주면 `discover_tags` 로 그 날 실제 값이 있던
        태그를 먼저 찾아서 쓴다 — 서버에 시리즈 전체를 무필터로 펼치는 게
        아니라, 딱 그 날 존재했던 태그만 pivot 한다.

        :param date_str: 'YYYY-MM-DD' (KST 기준 하루)
        """
        start_time = f"{date_str} 00:00:00"
        end_time = (pd.to_datetime(date_str) + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

        if target_tags is None:
            target_tags = self.discover_tags(start_time, end_time)
            if not target_tags:
                return pd.DataFrame()

        return self.get_data(start_time, end_time, target_tags)

    def extract_day_to_csv(self, date_str, save_dir="./extracted_csv", target_tags=None):
        """하루 전체(KST 00:00~24:00)를 청크(1시간)마다 바로 CSV 에 이어 쓴다.

        `get_data` 는 전체 청크를 메모리에 다 모았다가 한 번에 concat 한다.
        태그 1만개 규모에선 1시간 청크 하나가 실측 ~480MB 라, 24개를 다 들고
        있으면 이 PC 가용 메모리(테스트 시점 약 12GB)를 넘길 위험이 있다.
        그래서 청크 하나를 처리하는 즉시 디스크에 쓰고 메모리에서 비운다.

        ffill 은 청크 경계를 넘어 이어져야 한다 — 청크마다 따로 ffill 하면
        "이전 청크에서 바뀐 뒤 이번 청크엔 한 번도 안 바뀐" 태그가 NaN 으로
        보인다. 그래서 직전 청크의 마지막 값(`last_state`)을 들고 다니다가
        다음 청크 맨 앞에 합성 행으로 끼워 넣고 ffill 한 뒤 그 합성 행만
        떼어낸다.
        """
        start_time = f"{date_str} 00:00:00"
        end_time = (pd.to_datetime(date_str) + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

        if target_tags is None:
            target_tags = self.discover_tags(start_time, end_time)
            if not target_tags:
                return None

        start_dt = self._parse_time(start_time)
        end_dt = self._parse_time(end_time)

        os.makedirs(save_dir, exist_ok=True)
        file_path = os.path.join(save_dir, f"{date_str}_full_day.csv")

        filter_query = f'|> filter(fn: (r) => r.tag_name =~ {self._tag_regex(target_tags)})'

        fetch_start = start_dt - timedelta(seconds=SNAPSHOT_PAD_SEC)
        current_start = fetch_start
        last_state = None          # 직전 청크 마지막 행 (pd.Series), ffill 이어받기용
        wrote_header = False
        total_rows = 0
        failed = []

        print(f"🚀 [Streaming] {start_dt.isoformat()} ~ {end_dt.isoformat()}")
        print(f"   태그 {len(target_tags)}개 · 청크마다 바로 {file_path} 에 이어 씁니다")

        while current_start < end_dt:
            current_end = min(current_start + timedelta(hours=CHUNK_HOURS), end_dt)
            str_start = current_start.strftime('%Y-%m-%dT%H:%M:%SZ')
            str_end   = current_end.strftime('%Y-%m-%dT%H:%M:%SZ')
            print(f"📦 [Chunk] {str_start} ~ {str_end} ...", end=" ", flush=True)

            flux_query = f"""
            from(bucket: "{self.bucket}")
                |> range(start: {str_start}, stop: {str_end})
                |> filter(fn: (r) => r._measurement == "plc_line2")
                {filter_query}
                |> pivot(rowKey:["_time"], columnKey: ["tag_name"], valueColumn: "_value")
                |> drop(columns: ["_start", "_stop", "_measurement", "result", "table", "_field"])
            """

            try:
                chunk_df = self.query_api.query_data_frame(query=flux_query)
                if isinstance(chunk_df, list):
                    chunk_df = pd.concat(chunk_df) if chunk_df else pd.DataFrame()
            except Exception as e:
                failed.append(str_start)
                print(f"실패 — {e}")
                current_start = current_end
                continue

            chunk_df = chunk_df.drop(columns=[c for c in ("result", "table") if c in chunk_df.columns])

            if chunk_df.empty or '_time' not in chunk_df.columns:
                print("데이터 없음" + ("" if last_state is None else " (직전 상태 유지)"))
                current_start = current_end
                continue

            chunk_df['_time'] = pd.to_datetime(chunk_df['_time']).dt.tz_convert('Asia/Seoul').dt.tz_localize(None)
            chunk_df = chunk_df.set_index('_time').sort_index()
            chunk_df.index.name = 'Time'

            # 매 청크마다 컬럼 구성이 다를 수 있다 (그 시간에 안 바뀐 태그는
            # 애초에 pivot 에 안 나옴) — CSV 이어쓰기를 위해 전체 태그로 맞춘다.
            chunk_df = chunk_df.reindex(columns=target_tags)

            if last_state is not None:
                carry_idx = pd.Timestamp(current_start).tz_convert('Asia/Seoul').tz_localize(None)
                carry_row = pd.DataFrame([last_state], index=[carry_idx])
                chunk_df = pd.concat([carry_row, chunk_df]).sort_index()

            chunk_df = chunk_df.ffill()

            if last_state is None:
                cutoff = pd.Timestamp(start_dt).tz_convert('Asia/Seoul').tz_localize(None)
                chunk_df = chunk_df[chunk_df.index >= cutoff]
            else:
                chunk_df = chunk_df.iloc[1:]  # 합성 carry 행 제거

            if not chunk_df.empty:
                last_state = chunk_df.iloc[-1]
                chunk_df.to_csv(file_path, mode='a', header=not wrote_header, encoding='utf-8-sig')
                wrote_header = True
                total_rows += len(chunk_df)
                print(f"{len(chunk_df)}행 저장 (누적 {total_rows})")
            else:
                print("빈 청크")

            current_start = current_end

        if failed:
            print(f"⚠️ 실패한 구간 {len(failed)}개: {', '.join(failed[:3])}"
                  f"{' …' if len(failed) > 3 else ''}")
            print("   그 구간은 CSV 에서 통째로 빠져 있습니다.")

        if total_rows == 0:
            print("⚠️ 저장된 데이터가 전혀 없습니다.")
            return None

        size_mb = os.path.getsize(file_path) / 1024**2
        print(f"✅ [Streaming 완료] {file_path}")
        print(f"   총 {total_rows}행 × {len(target_tags)}컬럼 · 파일 크기 {size_mb:,.1f} MB")
        return file_path

    def get_data(self, start_time, end_time, target_tags):
        """구간 데이터를 태그별 컬럼으로 펼쳐 DataFrame 으로 돌려준다.

        `target_tags` 는 **필수다.** 생략하면 버킷의 시리즈 1만개를 서버에서
        피벗하게 되는데, 그건 현장 VM 을 멎게 할 수 있다. 물리점 하나가
        사람이름과 IL주소(T1254 등) 두 시리즈로 적재돼 있어 개수가 부풀어 있다.
        """
        if not target_tags:
            raise ValueError(
                "target_tags 는 필수입니다. 생략하면 전체 시리즈(약 1만개)를 "
                "서버에서 피벗하게 되어 현장 DB 에 부하가 큽니다. "
                "뽑을 태그 목록을 명시해 주세요.")

        start_dt = self._parse_time(start_time)
        end_dt   = self._parse_time(end_time)

        # 앞을 스냅샷 주기만큼 당겨 받는다. 안 그러면 맨 앞 구간이 NaN 이다.
        fetch_start = start_dt - timedelta(seconds=SNAPSHOT_PAD_SEC)

        print(f"🚀 추출 시작: {start_dt.isoformat()} ~ {end_dt.isoformat()}")
        print(f"   태그 {len(target_tags)}개 · 앞쪽 {SNAPSHOT_PAD_SEC}초는 결측 복원용으로 더 받습니다")

        filter_query = f'|> filter(fn: (r) => r.tag_name =~ {self._tag_regex(target_tags)})'

        all_chunks = []
        current_start = fetch_start
        failed = []

        while current_start < end_dt:
            current_end = min(current_start + timedelta(hours=CHUNK_HOURS), end_dt)
            str_start = current_start.strftime('%Y-%m-%dT%H:%M:%SZ')
            str_end   = current_end.strftime('%Y-%m-%dT%H:%M:%SZ')

            print(f"📦 [Chunk] {str_start} ~ {str_end} ...", end=" ", flush=True)

            # 태그 필터를 **pivot 앞에** 둔다. 뒤에 두면 서버가 전체를 펼친 뒤
            # 버리게 된다 — 느린 정도가 아니라 DB 메모리를 통째로 먹는다.
            # result/table/_field 도 여기서 버린다. 남겨두면 CSV 에 쓰레기
            # 컬럼으로 끼어든다(예전 추출물 헤더가 그랬다).
            flux_query = f"""
            from(bucket: "{self.bucket}")
                |> range(start: {str_start}, stop: {str_end})
                |> filter(fn: (r) => r._measurement == "plc_line2")
                {filter_query}
                |> pivot(rowKey:["_time"], columnKey: ["tag_name"], valueColumn: "_value")
                |> drop(columns: ["_start", "_stop", "_measurement", "result", "table", "_field"])
            """

            try:
                chunk_df = self.query_api.query_data_frame(query=flux_query)
                if isinstance(chunk_df, list):
                    chunk_df = pd.concat(chunk_df) if chunk_df else pd.DataFrame()
                if not chunk_df.empty:
                    all_chunks.append(chunk_df)
                    print(f"{len(chunk_df)}행")
                else:
                    print("데이터 없음")
            except Exception as e:
                failed.append(str_start)
                print(f"실패 — {e}")

            current_start = current_end

        if failed:
            print(f"⚠️ 실패한 구간 {len(failed)}개: {', '.join(failed[:3])}"
                  f"{' …' if len(failed) > 3 else ''}")
            print("   그 구간은 결과에서 통째로 빠져 있습니다. 그래프의 빈칸을 "
                  "'설비가 멈춤'으로 읽지 마세요.")

        if not all_chunks:
            print("⚠️ 수집된 데이터가 전혀 없습니다.")
            return pd.DataFrame()

        full_df = pd.concat(all_chunks)

        # result/table 은 influxdb_client 가 붙이는 주석 컬럼이라 Flux 의
        # drop() 으로는 안 없어진다. 받은 뒤에 뗀다.
        full_df = full_df.drop(columns=[c for c in ("result", "table")
                                        if c in full_df.columns])

        if '_time' in full_df.columns:
            full_df['_time'] = pd.to_datetime(full_df['_time']).dt.tz_convert('Asia/Seoul')
            full_df['_time'] = full_df['_time'].dt.tz_localize(None)
            full_df.set_index('_time', inplace=True)
            full_df.index.name = 'Time'
            full_df = full_df.sort_index()

            # 값이 바뀐 순간만 저장돼 있으므로 앞 값으로 채운다.
            full_df = full_df.ffill()

            # 패딩 구간을 잘라낸다 — ffill 을 **끝낸 뒤에** 잘라야 의미가 있다.
            # 인덱스는 naive KST 이므로 기준 시각도 같은 모양으로 맞춘다.
            cutoff = pd.Timestamp(start_dt)
            if cutoff.tzinfo is not None:
                cutoff = cutoff.tz_convert('Asia/Seoul').tz_localize(None)
            full_df = full_df[full_df.index >= cutoff]

        self._report(full_df, target_tags)
        return full_df

    # ------------------------------------------------------------------ #
    # 뽑은 뒤 알려줘야 하는 것들
    # ------------------------------------------------------------------ #

    def _report(self, df, target_tags):
        """받은 결과를 사람이 검토할 수 있게 요약한다.

        조용히 빠진 태그가 제일 위험하다 — 그래프에 선이 하나 없는 걸
        아무도 눈치채지 못한다.
        """
        print(f"✅ 통합 완료 — {len(df)}행 × {len(df.columns)}컬럼")
        if df.empty:
            return

        missing = [t for t in target_tags if t not in df.columns]
        if missing:
            print(f"❗ 요청했지만 **데이터가 없는 태그 {len(missing)}개**: "
                  f"{', '.join(missing[:8])}{' …' if len(missing) > 8 else ''}")
            print("   수집 블록(block_list) 밖이거나, 그 모드에서 안 받는 태그입니다.")

        head_gap = [c for c in df.columns if df[c].isna().iloc[0]]
        if head_gap:
            print(f"❗ **첫 행이 비어 있는 컬럼 {len(head_gap)}개** — 앞쪽 그래프가 끊겨 보입니다.")
            print("   흔한 원인 두 가지입니다.")
            print(f"     · 그 시각 수집이 멈춰 있었다 (패딩 {SNAPSHOT_PAD_SEC}초 안에 스냅샷이 없음)")
            print("     · **그때는 아예 수집 대상이 아니던 태그다.** 수집 태그 집합은"
                  " block_list 를 고칠 때 같이 바뀐다 — 실제로 2026-09-09 15:53 에"
                  " 48개가 새로 들어왔다.")
            print("   두 번째라면 시작 시각을 그 이후로 잡아야 합니다.")

        empty_cols = [c for c in df.columns if df[c].isna().all()]
        if empty_cols:
            print(f"❗ 전부 비어 있는 컬럼 {len(empty_cols)}개: "
                  f"{', '.join(empty_cols[:8])}{' …' if len(empty_cols) > 8 else ''}")

        self._warn_temp_scale(df)

    def _warn_temp_scale(self, df):
        """온도 컬럼마다 **그 채널의** Scale_Max 를 붙여서 알려준다.

        TT 채널은 TK/STK/JK/EX 로 여러 벌이고 Scale_Max 도 제각각이다
        (실측: 0 / 100 / 500 / 1000). 하나로 뭉뚱그려 알리면 엉뚱한 채널에
        나눗셈을 적용하게 된다.
        """
        temp_cols = [c for c in df.columns if any(h in c for h in TEMP_HINT)]
        if not temp_cols:
            return

        scale = {}
        try:
            import warnings as _w
            q = (f'from(bucket: "{self.bucket}") |> range(start: -30m)'
                 ' |> filter(fn: (r) => r.tag_name =~ /^Scale_Max___TT/)'
                 ' |> last() |> keep(columns:["tag_name","_value"])')
            with _w.catch_warnings():
                _w.simplefilter("ignore")
                sm = self.query_api.query_data_frame(query=q)
            if isinstance(sm, list):
                sm = pd.concat(sm) if sm else pd.DataFrame()
            if not sm.empty:
                scale = dict(zip(sm["tag_name"], sm["_value"]))
        except Exception as e:
            print(f"   (Scale_Max 조회 실패: {e})")

        UNIT = {100.0: ("1℃", 1), 1000.0: ("0.1℃", 10), 500.0: ("0.2℃", 5)}
        print(f"🌡️  온도 계열 컬럼 {len(temp_cols)}개 — **단위가 태그마다 다르다.**")
        for c in sorted(temp_cols):
            suffix = c.rsplit("_", 1)[-1]                 # P1 / I1 ...
            key = f"Scale_Max___TT_{suffix}"
            if "STK_" in c:  key = f"Scale_Max___TT_STK_{suffix}"
            elif "JK_" in c: key = f"Scale_Max___TT_JK_{suffix}"
            sm_v = scale.get(key)
            if sm_v is None:
                note = f"{key} 를 못 찾음 — 자릿수 직접 확인"
            elif sm_v in UNIT:
                unit, div = UNIT[sm_v]
                note = f"{key}={sm_v:g} → {unit} 단위, 값÷{div}"
            elif sm_v == 0:
                note = f"{key}=0 → 미사용/미설정 채널. 값을 믿지 말 것"
            else:
                note = f"{key}={sm_v:g} → 모르는 스케일. 확인 필요"
            print(f"     {c:<24} {note}")
        print("     ※ 2026-04 데이터는 Scale_Max___TT_P1=100 이었다. 지금은 1000 이라")
        print("       같은 태그가 22 → 228 로 찍힌다. 두 시기를 섞지 말 것.")

    def save_to_csv(self, df, save_dir="./extracted_csv"):
        """
        [핵심] 뽑아낸 데이터프레임을 날짜시간이 박힌 CSV 파일로 깔끔하게 저장합니다.
        """
        if df.empty:
            print("⚠️ 저장할 데이터가 없습니다.")
            return

        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # 파일명 자동 생성 (예: 2026-04-02_143000_analysis.csv)
        current_time = time.strftime("%Y-%m-%d_%H%M%S")
        file_name = f"{current_time}_analysis.csv"
        file_path = os.path.join(save_dir, file_name)

        # utf-8-sig 옵션: 한글 태그명이 엑셀에서 깨지는 것을 방지
        df.to_csv(file_path, encoding='utf-8-sig')
        print(f"💾 [저장 완료] 분석용 CSV 파일이 생성되었습니다: {file_path}")

# ==========================================
# 🧪 실행부
# ==========================================
if __name__ == "__main__":
    # 1. 추출기 가동 (.env 경로만 잘 맞춰주십쇼)
    extractor = LogExtractor(env_path=".env")

    # 2. KST 기준 하루 전체 (그 날 있던 태그 전체) 를 받는다.
    TARGET_DATE = "2026-09-20"
    my_df = extractor.get_full_day(TARGET_DATE)

    # 3. 콘솔에서 살짝 확인
    print(my_df.head())

    # 4. 분석용 CSV로 내려받기 (딱! 저장됩니다)
    extractor.save_to_csv(my_df)