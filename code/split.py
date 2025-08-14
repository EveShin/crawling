import pandas as pd
import os
from math import ceil

def split_excel_file(input_file, rows_per_file=100, output_dir="split_files", preserve_spacing=True):
    """
    Excel 파일을 지정된 행 수로 분할하는 함수
    - 이름, 소속(원본): 그대로 유지
    - 소속(전공/부서), 소속(대학/기관): 컬럼은 유지하되 값은 공란
    - 빈 행도 유지하여 원본 구조 보존
    """
    
    # 출력 디렉토리 생성
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Excel 파일 읽기
    try:
        df = pd.read_excel(input_file, keep_default_na=False)  # NaN 값을 빈 문자열로 처리
        print(f"원본 파일 읽기 완료: {len(df)}행, {len(df.columns)}개 컬럼")
        print(f"컬럼 목록: {list(df.columns)}")
        
        # 필요한 4개 컬럼만 선택
        required_columns = ["이름", "소속(원본)", "소속(전공/부서)", "소속(대학/기관)"]
        
        # 실제 존재하는 컬럼 확인
        available_columns = []
        for col in required_columns:
            if col in df.columns:
                available_columns.append(col)
            else:
                print(f"경고: '{col}' 컬럼을 찾을 수 없습니다.")
        
        if len(available_columns) < 2:
            print("오류: 최소한 '이름'과 '소속(원본)' 컬럼이 필요합니다.")
            return
        
        # 데이터 프레임 정리
        df_clean = pd.DataFrame()
        
        # 이름과 소속(원본)은 그대로 복사 (빈 행도 포함)
        if "이름" in df.columns:
            df_clean["이름"] = df["이름"].fillna("")  # NaN을 빈 문자열로
        if "소속(원본)" in df.columns:
            df_clean["소속(원본)"] = df["소속(원본)"].fillna("")
        
        # 소속(전공/부서), 소속(대학/기관)은 빈 컬럼으로 생성
        df_clean["소속(전공/부서)"] = ""
        df_clean["소속(대학/기관)"] = ""
        
        # 빈 행 감지 및 보존
        if preserve_spacing:
            print("빈 행 분석 중...")
            empty_rows = []
            for idx, row in df_clean.iterrows():
                if row["이름"].strip() == "" and row["소속(원본)"].strip() == "":
                    empty_rows.append(idx)
            print(f"감지된 빈 행: {len(empty_rows)}개")
        
        print(f"처리된 데이터: {len(df_clean)}행, {len(df_clean.columns)}개 컬럼")
        print(f"최종 컬럼: {list(df_clean.columns)}")
        
    except Exception as e:
        print(f"파일 읽기 오류: {e}")
        return
    
    # 총 파일 개수 계산
    total_files = ceil(len(df_clean) / rows_per_file)
    
    # 파일명에서 확장자 제거
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    
    # 분할 실행
    for i in range(total_files):
        start_idx = i * rows_per_file
        end_idx = min((i + 1) * rows_per_file, len(df_clean))
        
        # 해당 범위의 데이터 추출
        chunk = df_clean.iloc[start_idx:end_idx].copy()
        
        # CSV 파일로 저장 (빈 행도 포함)
        output_file = os.path.join(output_dir, f"{base_name}_part_{i+1:03d}.csv")
        chunk.to_csv(output_file, index=False, encoding='utf-8-sig', na_rep='')
        
        # 빈 행 통계
        empty_in_chunk = len(chunk[(chunk["이름"] == "") & (chunk["소속(원본)"] == "")])
        data_in_chunk = len(chunk) - empty_in_chunk
        
        print(f"파일 {i+1}/{total_files} 생성: {output_file}")
        print(f"  - 총 {len(chunk)}행 (데이터: {data_in_chunk}행, 빈 행: {empty_in_chunk}행)")
        
        # 첫 번째 파일의 샘플 확인
        if i == 0:
            print("\n첫 번째 파일 샘플 (빈 행 포함):")
            sample = chunk.head(10)
            for idx, row in sample.iterrows():
                if row["이름"].strip() == "" and row["소속(원본)"].strip() == "":
                    print(f"  {idx}: [빈 행]")
                else:
                    print(f"  {idx}: {row['이름'][:20]}...")
            print()
    
    print(f"\n분할 완료! 총 {total_files}개 파일이 '{output_dir}' 폴더에 생성되었습니다.")
    print("생성된 파일 구조:")
    print("   - 이름: 원본 데이터 유지 (빈 행 포함)")
    print("   - 소속(원본): 원본 데이터 유지 (빈 행 포함)") 
    print("   - 소속(전공/부서): 빈 컬럼")
    print("   - 소속(대학/기관): 빈 컬럼")
    print("   - 원본 가독성을 위한 빈 행 구분 유지")

def split_excel_by_size(input_file, max_size_mb=5, output_dir="split_files", preserve_spacing=True):
    """
    Excel 파일을 파일 크기 기준으로 분할하는 함수 (빈 행 유지)
    """
    
    # 출력 디렉토리 생성
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Excel 파일 읽기
    try:
        df = pd.read_excel(input_file, keep_default_na=False)
        print(f"원본 파일 읽기 완료: {len(df)}행, {len(df.columns)}개 컬럼")
        
        # 데이터 프레임 정리 (위와 동일한 로직)
        df_clean = pd.DataFrame()
        
        if "이름" in df.columns:
            df_clean["이름"] = df["이름"].fillna("")
        if "소속(원본)" in df.columns:
            df_clean["소속(원본)"] = df["소속(원본)"].fillna("")
        
        df_clean["소속(전공/부서)"] = ""
        df_clean["소속(대학/기관)"] = ""
        
        print(f"처리된 데이터: {len(df_clean)}행, {len(df_clean.columns)}개 컬럼")
        
    except Exception as e:
        print(f"파일 읽기 오류: {e}")
        return
    
    # 파일명에서 확장자 제거
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    
    # 테스트로 작은 청크 크기부터 시작
    chunk_size = 50
    file_count = 1
    start_idx = 0
    
    while start_idx < len(df_clean):
        # 청크 크기를 점진적으로 늘려가며 최적 크기 찾기
        while True:
            end_idx = min(start_idx + chunk_size, len(df_clean))
            chunk = df_clean.iloc[start_idx:end_idx]
            
            # 임시 파일로 크기 테스트
            temp_file = f"temp_test.csv"
            chunk.to_csv(temp_file, index=False, encoding='utf-8-sig', na_rep='')
            
            file_size_mb = os.path.getsize(temp_file) / (1024 * 1024)
            os.remove(temp_file)  # 임시 파일 삭제
            
            if file_size_mb > max_size_mb and chunk_size > 10:
                chunk_size -= 10
                break
            elif end_idx == len(df_clean) or file_size_mb >= max_size_mb * 0.9:
                break
            else:
                chunk_size += 10
        
        # 최종 청크 생성
        end_idx = min(start_idx + chunk_size, len(df_clean))
        chunk = df_clean.iloc[start_idx:end_idx]
        
        # 파일 저장
        output_file = os.path.join(output_dir, f"{base_name}_part_{file_count:03d}.csv")
        chunk.to_csv(output_file, index=False, encoding='utf-8-sig', na_rep='')
        
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        
        # 빈 행 통계
        empty_in_chunk = len(chunk[(chunk["이름"] == "") & (chunk["소속(원본)"] == "")])
        data_in_chunk = len(chunk) - empty_in_chunk
        
        print(f"파일 {file_count} 생성: {output_file}")
        print(f"  - {len(chunk)}행 ({file_size_mb:.2f}MB) | 데이터: {data_in_chunk}행, 빈 행: {empty_in_chunk}행")
        
        start_idx = end_idx
        file_count += 1
    
    print(f"\n총 {file_count-1}개 파일이 생성되었습니다.")

def preview_file_structure(input_file):
    """
    파일 구조 미리보기 함수 (빈 행 정보 포함)
    """
    try:
        df = pd.read_excel(input_file, keep_default_na=False)
        
        # 빈 행 분석
        empty_rows = 0
        for idx, row in df.iterrows():
            if all(str(cell).strip() == "" for cell in row):
                empty_rows += 1
        
        print("파일 구조 분석:")
        print(f"   총 행 수: {len(df)}")
        print(f"   데이터 행: {len(df) - empty_rows}")
        print(f"   빈 행: {empty_rows}")
        print(f"   총 컬럼 수: {len(df.columns)}")
        print(f"   컬럼 목록: {list(df.columns)}")
        
        print("\n데이터 샘플 (상위 10행, 빈 행 표시):")
        sample = df.head(10)
        for idx, row in sample.iterrows():
            if all(str(cell).strip() == "" for cell in row):
                print(f"  {idx}: [빈 행]")
            else:
                first_col = str(row.iloc[0])[:30] + "..." if len(str(row.iloc[0])) > 30 else str(row.iloc[0])
                print(f"  {idx}: {first_col}")
        
        print("\n" + "="*50)
        return True
    except Exception as e:
        print(f"파일 읽기 오류: {e}")
        return False

# 사용 예시
if __name__ == "__main__":
    # 사용할 파일 경로를 여기에 입력
    input_file_path = "splitfile.xlsx"  # 실제 파일 경로로 변경
    
    print("Excel 파일 분할기 (빈 행 유지 버전)")
    print("="*50)
    
    # 파일 구조 미리보기
    if not preview_file_structure(input_file_path):
        exit()
    
    print("\nExcel 파일 분할 옵션:")
    print("1. 행 수 기준 분할 (빈 행 유지)")
    print("2. 파일 크기 기준 분할 (빈 행 유지)")
    print("3. 종료")
    
    choice = input("\n선택하세요 (1, 2, 또는 3): ")
    
    if choice == "1":
        rows = int(input("파일당 행 수를 입력하세요 (기본값: 100): ") or 100)
        print(f"\n{rows}행씩 분할을 시작합니다. (빈 행 포함)")
        split_excel_file(input_file_path, rows_per_file=rows, preserve_spacing=True)
    
    elif choice == "2":
        size = float(input("최대 파일 크기(MB)를 입력하세요 (기본값: 5): ") or 5)
        print(f"\n{size}MB 단위로 분할을 시작합니다. (빈 행 포함)")
        split_excel_by_size(input_file_path, max_size_mb=size, preserve_spacing=True)
    
    elif choice == "3":
        print("프로그램을 종료합니다.")
    
    else:
        print("잘못된 선택입니다.")

# split_excel_file("splitfile.xlsx", rows_per_file=50, preserve_spacing=True)