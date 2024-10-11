import pandas as pd
from django.http import JsonResponse, HttpResponse
import json
from django.shortcuts import render
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
from io import BytesIO
import threading
from zipfile import ZipFile

def home(request):
    return render(request, 'getdata/home.html')

def combined_crawling_view(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        username = data.get('username')
        password = data.get('password')
        startDate = data.get('start_date') + ' 00:00'
        endDate = data.get('end_date') + ' 00:00'
        urls = data.get('urls')
        stores = data.get('stores')
        url_store_dict = dict(zip(urls, stores))

        # 멀티스레딩을 위한 스레드 리스트
        threads = []
        results = []

        def crawl_and_save(url):
            df = get_payment_crawling(username, password, startDate, endDate, url)
            if isinstance(df, JsonResponse):
                return df
            
            detailed_names, expense_df = get_expense_crawling(username, password, startDate, url, df)
            final_data = create_final_excel(df, detailed_names, expense_df, startDate)
            excel_file = save_to_excel(final_data)
            results.append((url, excel_file))

        # 크롤링 작업을 스레드로 실행
        for url in urls:
            thread = threading.Thread(target=crawl_and_save, args=(url,))
            thread.start()
            threads.append(thread)
        
        # 모든 스레드가 종료될 때까지 기다림
        for thread in threads:
            thread.join()

        # 파일 다운로드 응답 반환
        response = HttpResponse(content_type='application/zip')
        response['Content-Disposition'] = 'attachment; filename="결제정보.zip"'

        with ZipFile(response, 'w') as zip_file:
            for url, excel_file in results:
                excel_file.seek(0)
                zip_file.writestr(f"{url_store_dict[url]}_결제정보.xlsx", excel_file.read())

        # 파일 저장 경로 응답 반환
        return response
    
    return JsonResponse({'error': 'Invalid request method'}, status=400)

def get_payment_crawling(username, password, startDate, endDate, url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    time.sleep(1)

    # 로그인
    input_id = driver.find_element(By.ID, 'id')
    input_pwd = driver.find_element(By.ID, 'pwd')
    login_Btn = driver.find_element(By.ID, 'loginBtn')
    input_id.send_keys(username)
    input_pwd.send_keys(password)
    login_Btn.click()
    time.sleep(1)

    # 결제정보 버튼 클릭
    driver.execute_script("goUrl('/sell/list')")
    time.sleep(3)
    driver.switch_to.frame("_body2")

    # 검색 조건 설정
    is_first_page = True
    start_date_str = (datetime.strptime(startDate, '%Y-%m-%d %H:%M') - relativedelta(months=4)).strftime('%Y-%m-%d %H:%M')
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M')
    end_date = datetime.strptime(endDate, '%Y-%m-%d %H:%M')
    end_signal = True
    df = pd.DataFrame()

    while end_signal:
        try:
            table = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '.table.table-bordered'))
            )
            rows = table.find_elements(By.TAG_NAME, 'tr')
            page_data = []

            for row in rows:
                heads = row.find_elements(By.TAG_NAME, 'th')
                cells = row.find_elements(By.TAG_NAME, 'td')
                row_data = [head.text.strip() if head.text.strip() != '' else None for head in heads] + [cell.text.strip() if cell.text.strip() != '' else None for cell in cells]
                page_data.append(row_data)
            
            if is_first_page:
                header = page_data[0]
                df = pd.DataFrame(columns=header)
                is_first_page = False
            
            for page_row in page_data[1:]:
                try:
                    row_date = datetime.strptime(page_row[0], '%Y-%m-%d %H:%M')
                    if row_date > end_date:
                        continue
                    elif (end_date >= row_date) & (row_date > start_date):
                        df.loc[len(df)] = page_row
                    else:
                        print(f"{start_date} 이후의 데이터 수집 완료.")
                        end_signal = False
                        break
                except Exception as e:
                    return JsonResponse({"message" : "날짜 파싱 오류", "error" : str(e)})
            
            # 페이지네이션
            pagination = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'pagination'))
            )
            current_active_li = pagination.find_element(By.CLASS_NAME, 'active')
            next_li = current_active_li.find_element(By.XPATH, 'following-sibling::li')

            if next_li:
                next_page_link = next_li.find_element(By.TAG_NAME, 'a')
                next_page_link.click()
                time.sleep(3)
            else:
                break
        
        except Exception as e:
            return JsonResponse({"error" : str(e)})
    
    driver.quit()
    return df

def get_expense_crawling(username, password, startDate, url, df):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)

    target_date = datetime.strptime(startDate, '%Y-%m-%d %H:%M')
    df['배정날짜'] = pd.to_datetime(df['배정날짜'])
    unique_names = pd.DataFrame(df[(df['배정날짜'] >= target_date)]['이름'].unique(), columns=['이름'])
    detailed_names = pd.DataFrame(columns=['이름', '전화번호', '재등록', '재구매 횟수'])

    driver.get(url)
    time.sleep(1)

    # 로그인
    id_input = driver.find_element(By.ID, 'id')
    pwd_input = driver.find_element(By.ID, 'pwd')
    login_Btn = driver.find_element(By.ID, 'loginBtn')
    id_input.send_keys(username)
    pwd_input.send_keys(password)
    login_Btn.click()
    time.sleep(1)

    expense_df = pd.DataFrame(columns=['이름', '전화번호', '재구매 횟수', '일자', '입금', '출금', '구분', '적요'])

    for idx, row in unique_names.iterrows():
        driver.execute_script("goUrl('/member/list')")
        driver.switch_to.frame("_body2")

        input_name = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, 'keywordInput'))
        )
        input_btn = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.ID, 'searchBtn'))
        )

        name = row['이름']
        print(f'{idx} :', name)

        input_name.clear()
        input_name.send_keys(name)
        input_btn.click()

        table = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.table.table-bordered'))
        )
        rows = table.find_elements(By.TAG_NAME, 'tr')

        for i in range(1, len(rows)):
            name_table = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '.table.table-bordered'))
            )
            name_rows = name_table.find_elements(By.TAG_NAME, 'tr')
            name_cells = name_rows[i].find_elements(By.TAG_NAME, 'td')
            phone = name_cells[-1].text.strip()
            person_name = name_cells[2].text.strip()

            if person_name == name:
                name_cells[2].click()

                expense_history_Btn = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((By.ID, 'expense_history_Btn'))
                )
                expense_history_Btn.click()
                time.sleep(0.5)

                repurchases_text = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.ID, 'grid_info'))
                )
                if repurchases_text.text != '':
                    repurchases = re.search(r'/ (\d+)', repurchases_text.text).group(1)
                else:
                    repurchases = None
                detailed_names = pd.concat([detailed_names, pd.DataFrame({
                    '이름' : [name],
                    '전화번호' : [phone],
                    '재등록' : '',
                    '재구매 횟수' : [repurchases]
                })], ignore_index=True)

                ex_df = collect_expense_data(driver, name, phone, repurchases)
                expense_df = pd.concat([expense_df, ex_df], ignore_index=True)

                driver.switch_to.default_content()
                driver.execute_script("goUrl('/member/list')")
                driver.switch_to.frame("_body2")
                input_name = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.ID, 'keywordInput'))
                )
                input_btn = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((By.ID, 'searchBtn'))
                )
                input_name.clear()
                input_name.send_keys(name)
                input_btn.click()
        
        driver.switch_to.default_content()
    
    driver.quit()
    return detailed_names, expense_df

def collect_expense_data(driver, name, phone, repurchases):
    ex_page_data = []
    while True:
        expense_table = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, 'grid'))
        )
        expense_rows = expense_table.find_elements(By.TAG_NAME, 'tr')

        ex_header = ['이름', '전화번호', '재구매 횟수']
        for row in expense_rows:
            ex_heads = row.find_elements(By.TAG_NAME, 'th')
            ex_cells = row.find_elements(By.TAG_NAME, 'td')
            ex_row_data = [name, phone, repurchases]

            for head in ex_heads:
                ex_header.append(head.text.strip() if head.text.strip() != '' else None)
            for cell in ex_cells:
                ex_row_data.append(cell.text.strip() if cell.text.strip() != '' else None)
            if (len(ex_row_data) > 3) and (repurchases is not None):
                ex_page_data.append(ex_row_data)
        
        ex_next_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.ID, 'grid_next'))
        )
        if ('disabled' in ex_next_button.get_attribute('class')) or (not repurchases):
            break
        else:
            ex_next_button.click()
    
    ex_df = pd.DataFrame(columns=ex_header)
    for ex_row in ex_page_data:
        ex_df.loc[len(ex_df)] = ex_row
    
    return ex_df

def create_final_excel(df, detailed_names, expense_df, startDate):
    data = df
    namesData = detailed_names
    expense_data = expense_df
    target_date = datetime.strptime(startDate, '%Y-%m-%d %H:%M')
    
    expense_data['일자'] = pd.to_datetime(expense_data['일자'], format='%Y.%m.%d').dt.date

    data['금액'] = data['금액'].astype(str).str.replace(',', '').astype(int)
    expense_data['입금'].astype(int)

    data['요금종류'] = data['요금종류'].fillna('기타')
    data.loc[data['요금종류'].str.contains('연장'), '요금종류'] = '기간연장'
    data.loc[data['요금종류'].str.contains('사물함'), '요금종류'] = '사물함'
    data.loc[data['요금종류'].str.contains('스터디룸'), '요금종류'] = '스터디룸'
    data.loc[data['요금종류'].str.contains('다락방'), '요금종류'] = '다락방'
    data.loc[data['요금종류'].str.contains('정액권'), '요금종류'] = '정액권'
    data.loc[data['요금종류'].str.contains('정기권'), '요금종류'] = '정기권'
    data.loc[data['요금종류'].str.contains('시간'), '요금종류'] = '1회이용권'

    data = data.merge(namesData[['이름', '전화번호', '재등록', '재구매 횟수']], on='이름', how='left')

    # 각 행을 순회하며 '재등록' 값 채우기
    for index, row in data.iterrows():
        name = row['이름']
        phone = row['전화번호']
        charge_type = row['요금종류']
        pament_date = pd.to_datetime(row['배정날짜']).date()

        # if pament_date.month != target_date.month:
        #     continue

        # 해당 회원의 결제 내역 찾기
        expenses = expense_data[
            (expense_data['이름'] == name) & (expense_data['전화번호'] == phone) & (expense_data['일자'] == pament_date)
        ]

        if not expenses.empty:
            # 일치하는 결제 내역이 있을 경우
            matching_date = expenses.iloc[0]['일자']

            # 4개월 전 날짜 계산
            four_months_ago = matching_date - relativedelta(months=4)

            # 4개월 동안의 결제 내역 확인
            prior_expenses = expense_data[
                (expense_data['이름'] == name) & 
                (expense_data['전화번호'] == phone) & 
                (expense_data['일자'] < matching_date) & 
                (expense_data['일자'] >= four_months_ago)
            ]

            if prior_expenses.empty:
                # 지난 4달간 결제 내역이 없을 경우
                data.at[index, '재등록'] = '신규'
            else:
                # 지난 4달 동안 결제 이력이 있는 경우
                if charge_type in ['1회이용권', '스터디룸', '다락방']:
                    data.at[index, '재등록'] = '재등록(일반)'
                elif charge_type in ['정기권', '정액권']:
                    # 직전 결제 이력 찾기
                    last_payment = prior_expenses.iloc[0]
                    last_date = last_payment['일자']
                    last_amount = last_payment['입금']

                    # 직전 결제 이력의 요금종류 확인
                    previous_payment = data[
                        (data['이름'] == name) & 
                        (pd.to_datetime(data['배정날짜']).dt.date == last_date) & 
                        (data['금액'] == last_amount)
                    ]

                    if not previous_payment.empty and previous_payment['요금종류'].values[0] in ['정기권', '정액권']:
                        data.at[index, '재등록'] = '재등록(장기)'
                    elif not previous_payment.empty and previous_payment['요금종류'].values[0] in ['1회이용권', '스터디룸', '다락방']:
                        data.at[index, '재등록'] = '재등록(전환)'
    
    data = data[pd.to_datetime(data['배정날짜']) >= target_date]
    data = data[(data['재등록'] != '') & (data['재등록'].notna())]
    data['재구매 횟수'] = data.groupby(['이름', '전화번호']).cumcount(ascending=False) + 1
    
    return data

# 엑셀 파일을 생성하거나 기존 파일에 데이터를 추가할 수 있는 함수
def save_to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return output