import os
import time
import pandas as pd
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv

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

        self.client = InfluxDBClient(url=self.db_url, token=self.token, org=self.org, timeout=30000)
        self.query_api = self.client.query_api()
        
        print("🔌 [Extractor] InfluxDB 분석용 추출기 연결 완료!")

    def get_data(self, start_time, end_time, target_tags):
        print(f"🔍 데이터 추출 시작... ({start_time} ~ {end_time})")
        
        tag_filters = " or ".join([f'r.tag_name == "{tag}"' for tag in target_tags])

        # pivot으로 세로 데이터를 가로(Excel) 포맷으로 쫙 펴줌
        flux_query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: {start_time}, stop: {end_time})
            |> filter(fn: (r) => r._measurement == "plc_data_int")  # 🚨 아까 바꾼 테이블 이름!
            |> filter(fn: (r) => {tag_filters})
            |> pivot(rowKey:["_time"], columnKey: ["tag_name"], valueColumn: "_value")
            |> drop(columns: ["_start", "_stop", "_measurement"])
        """

        df = self.query_api.query_data_frame(query=flux_query)
        
        if isinstance(df, list):
            if len(df) == 0:
                print("⚠️ 해당 조건의 데이터가 없습니다.")
                return pd.DataFrame()
            df = pd.concat(df)

        if '_time' in df.columns:
            # 한국 시간으로 깔끔하게 변환
            df['_time'] = pd.to_datetime(df['_time']).dt.tz_convert('Asia/Seoul')
            # 엑셀에서 보기 편하도록 시간 포맷에서 +09:00 같은 시간대 정보(tz) 제거
            df['_time'] = df['_time'].dt.tz_localize(None) 
            df.set_index('_time', inplace=True)
            df.index.name = 'Time'

        print(f"✅ 추출 완료! 총 {len(df)}행 데이터 확보.")
        return df

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

    # 2. 원하는 데이터 검색
    my_df = extractor.get_data(
        start_time="-6h", 
        end_time="now()", 
        target_tags=["AL_Warning_Line", "P00030"]
    )

    # 3. 콘솔에서 살짝 확인
    print(my_df.head())

    # 4. 분석용 CSV로 내려받기 (딱! 저장됩니다)
    extractor.save_to_csv(my_df)