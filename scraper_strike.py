import time
import re
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# --- 設定項目 ---
MIN_REVENUE_THRESHOLD_REAL_ESTATE = 3  # 不動産案件の売上高最低ライン（単位：億円）
MIN_REVENUE_THRESHOLD_OTHER = 5  # 不動産以外の案件の売上高最低ライン（単位：億円）
TARGET_URL = "https://www.strike.co.jp/smart/smart_search.html?issearch=on"
MAX_DEALS_TO_PROCESS = 100  # 処理する案件数の上限
# ----------------

def parse_revenue(revenue_text, is_real_estate_deal=True):
    """「3億円」「2億円～5億円」等を数値に変換する"""
    if not revenue_text:
        return 0
    
    if "億円" not in revenue_text:
        return 0
    
    if "～" in revenue_text:
        parts = revenue_text.split("～")
        if len(parts) == 2:
            upper_match = re.search(r'([\d\.]+)', parts[1])
            upper_value = float(upper_match.group(1)) if upper_match else 0
            if not is_real_estate_deal and upper_value == 5.0:
                return upper_value
            return upper_value
    
    match = re.search(r'([\d\.]+)', revenue_text)
    if match:
        return float(match.group(1))
    
    return 0

def is_real_estate_deal(title):
    return "不動産" in title

def get_revenue_threshold(title):
    if is_real_estate_deal(title):
        return MIN_REVENUE_THRESHOLD_REAL_ESTATE
    else:
        return MIN_REVENUE_THRESHOLD_OTHER

def meets_revenue_criteria(revenue_text, title):
    is_real_estate = is_real_estate_deal(title)
    threshold = get_revenue_threshold(title)
    revenue_value = parse_revenue(revenue_text, is_real_estate)
    
    if is_real_estate:
        return revenue_value >= threshold
    else:
        return revenue_value > threshold

def extract_deal_info_from_strike(soup, max_deals):
    deals = []
    ss_elements = soup.find_all(string=re.compile(r'SS\d{6}'))
    
    for ss_element in ss_elements[:max_deals]:
        try:
            ss_number = ss_element.strip()
            parent = ss_element.parent
            if not parent:
                continue
            
            search_area = parent.parent if parent.parent else parent
            title = ""
            title_candidates = search_area.find_all(string=True)
            for i, candidate in enumerate(title_candidates):
                if ss_number in candidate:
                    for j in range(i + 1, min(i + 5, len(title_candidates))):
                        next_text = title_candidates[j].strip()
                        if next_text and not re.match(r'^[＋－\s]*$', next_text) and '所在地' not in next_text and '売上高' not in next_text:
                            title = next_text
                            break
                    break
            
            revenue_text = ""
            all_text = search_area.get_text()
            revenue_patterns = [
                r'売上高[^\d]*([^\n]+)',
                r'- 売上高([^\n]+)',
                r'売上高：([^\n]+)'
            ]
            
            for pattern in revenue_patterns:
                revenue_match = re.search(pattern, all_text)
                if revenue_match:
                    revenue_text = revenue_match.group(1).strip()
                    break
            
            link = ""
            link_element = search_area.find('a', href=re.compile(r'sell_details\.html.*code=' + ss_number))
            if link_element:
                link = "https://www.strike.co.jp" + link_element.get('href')
            
            if title and revenue_text:
                deals.append({
                    'ss_number': ss_number,
                    'title': title,
                    'revenue': revenue_text,
                    'link': link
                })
        except Exception as e:
            print(f"    [警告] {ss_number} の解析中にエラー: {e}")
            continue
    return deals

def main():
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    deals_found = []
    print(f"🎯 ストライク（SMART）の案件を解析中...（上限: {MAX_DEALS_TO_PROCESS}件）")
    try:
        driver.get(TARGET_URL)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        all_deals = extract_deal_info_from_strike(soup, MAX_DEALS_TO_PROCESS)
        for i, deal in enumerate(all_deals, 1):
            if meets_revenue_criteria(deal['revenue'], deal['title']):
                deals_found.append(deal)
    finally:
        driver.quit()

    headers = ['案件番号', 'タイトル', '分類', '売上高', 'リンク']
    data_as_list = []
    for deal in deals_found:
        deal_type = "不動産案件" if is_real_estate_deal(deal['title']) else "その他案件"
        data_as_list.append([
            deal.get('ss_number', ''),
            deal.get('title', ''),
            deal_type,
            deal.get('revenue', ''),
            deal.get('link', '')
        ])
    return {"headers": headers, "data": data_as_list}

if __name__ == "__main__":
    results = main()
    if results:
        print("\n" + "="*60)
        print(f"🎉 解析完了！条件に合致する {len(results['data'])} 件の案件が見つかりました。")
        print("="*60)
        for deal in results['data']:
            print(f"案件番号: {deal[0]}")
            print(f"タイトル: {deal[1]}")
            print(f"分類: {deal[2]}")
            print(f"売上高: {deal[3]}")
            print(f"リンク: {deal[4]}")
            print("-" * 40)