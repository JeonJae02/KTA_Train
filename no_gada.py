import os

def convert_vars_to_list_format(input_filepath, output_filepath="formatted_vars.txt"):
    """
    세로로 적힌 변수명 파일(csv/txt)을 읽어 큰따옴표와 콤마가 붙은 문자열로 변환하고 txt로 저장합니다.
    """
    try:
        # 1. 파일 읽기 (utf-8-sig로 읽어서 한글 깨짐 및 BOM 문자 방지)
        with open(input_filepath, 'r', encoding='utf-8-sig') as file:
            lines = file.readlines()
        
        # 2. 양옆 공백 및 줄바꿈 제거, 빈 줄 무시, 큰따옴표로 감싸기
        # 엑셀에서 복붙했을 때 섞여 들어올 수 있는 이상한 공백(strip)도 다 날려줍니다.
        formatted_vars = [f'"{line.strip()}"' for line in lines if line.strip()]
        
        # 3. 콤마와 띄어쓰기로 예쁘게 연결
        result_string = ", ".join(formatted_vars)
        
        # 4. 텍스트 파일로 쫙 뽑아내기
        with open(output_filepath, 'w', encoding='utf-8') as file:
            file.write(result_string)
            
        print(f"✅ 변환 완료! 총 {len(formatted_vars)}개의 변수가 처리되었습니다.")
        print(f"💾 저장 위치: {os.path.abspath(output_filepath)}")
        
        # 확인용으로 앞의 3개만 살짝 보여주기
        print(f"🔍 미리보기: {result_string[:80]} ...")
        
    except FileNotFoundError:
        print(f"❌ 에러: '{input_filepath}' 파일을 찾을 수 없습니다. 파일명과 경로를 확인해 주세요.")
    except Exception as e:
        print(f"❌ 알 수 없는 에러가 발생했습니다: {e}")

# ==========================================
# 🚀 실행 방법
# ==========================================
# 1. 올려주신 변수명들을 'raw_vars.csv' (또는 .txt) 파일로 저장해 둡니다.
# 2. 아래 함수를 실행합니다.

# convert_vars_to_list_format("raw_vars.csv", "p1_target_tags.txt")

if __name__ == "__main__":
    convert_vars_to_list_format("./data/raw_vars.csv", "p1_target_tags.txt")