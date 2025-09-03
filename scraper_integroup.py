import time
import re
import configparser
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from google_sheets_client import GoogleSheetsClient

def load_config():
    """設定ファイル(config.ini)を読み込む"""
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8-sig')
    return config

# --- 設定項目 ---
# 売上高の最低ライン（単位：円）
MIN_REVENUE_THRESHOLD = 300_000_000
# 取得する最大ページ数
MAX_PAGES_TO_SCRAPE = 5
# ----------------

def parse_financial_value(text):
    """
    「１～５億円」のような文字列を数値(円)に変換する。
    範囲の場合は上限値を採用する。
    """
    if not text or "非公開" in text:
        return 0
    
    # 全角数字・記号を半角に変換
    text = text.translate(str.maketrans('０１２３４５６７８９～', '0123456789~'))
    
    # 範囲指定（~）がある場合、後半部分（上限値）を対象にする
    if "~" in text:
        text = text.split("~", 1)[1]

    # 数字（小数点含む）を抽出
    match = re.search(r'([\d\.]+)', text)
    if not match:
        return 0

    value = float(match.group(1))

    # 単位に基づいて円に換算
    if '億' in text:
        value *= 100_000_000
    elif '千万' in text:
        value *= 10_000_000
    elif '万' in text:
        value *= 10_000
    
    return int(value)

def main():
    """メインの実行関数"""
    config = load_config()
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    base_url = "https://www.integroup.jp/sell/"
    deals_found = []
    processed_ids = set()

    print("インテグループの案件リストのスクレイピングを開始します。")

    try:
        for page_num in range(1, MAX_PAGES_TO_SCRAPE + 1):
            # 1ページ目と2ページ目以降でURL形式を正しく分岐
            if page_num == 1:
                target_url = base_url
            else:
                target_url = f"{base_url}page/{page_num}/"
            
            print(f"\n--- ページ {page_num} ({target_url}) を解析中 ---")
            driver.get(target_url)
            
            # 案件リストの最初の要素が表示されるまで最大20秒待機
            try:
                wait = WebDriverWait(driver, 20)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.seller-list-box")))
                print("  案件リストの表示を確認しました。")
            except TimeoutException:
                print("  このページに案件が見つかりませんでした。処理を終了します。")
                break

            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # 各案件は <div class="seller-list-box"> で囲まれている
            deal_list = soup.select("div.seller-list-box")
            
            if not deal_list:
                print("  HTML構造内に案件リストが見つかりませんでした。")
                continue

            print(f"{len(deal_list)}件の案件が見つかりました。情報を抽出します。")

            for deal_item in deal_list:
                # --- HTML構造に合わせて各情報を正確に抽出 ---
                
                title_tag = deal_item.find("h3")
                title = title_tag.get_text(strip=True) if title_tag else "タイトル不明"

                link_tag = deal_item.find("a", class_="detail-btn")
                link = link_tag['href'] if link_tag else "リンク不明"

                # テーブルから案件番号と売上高を抽出
                case_id = "ID不明"
                revenue_text = "情報なし"
                table_rows = deal_item.select("table.talbe_style tr")
                for row in table_rows:
                    th = row.find("th")
                    td = row.find("td")
                    if th and td:
                        key = th.get_text(strip=True)
                        value = td.get_text(strip=True)
                        if "案件番号" in key:
                            case_id = value
                        elif "売上高" in key:
                            revenue_text = value

                # 重複案件はスキップ
                if case_id in processed_ids:
                    continue
                processed_ids.add(case_id)
                
                print(f"  - ID: {case_id} ({title}) を確認中...")
                print(f"    売上高: {revenue_text}")

                # --- 抽出条件の判定ロジック ---
                revenue_in_yen = parse_financial_value(revenue_text)
                
                if revenue_in_yen >= MIN_REVENUE_THRESHOLD:
                    print(f"    [✓] 条件に合致！")
                    deals_found.append({
                        'id': case_id,
                        'title': title,
                        'revenue': revenue_text,
                        'profit': '-', # 営業利益は「-」を格納
                        'link': link
                    })
                else:
                    print(f"    [×] 条件を満たしませんでした。")

    finally:
        driver.quit()

    # --- 最終結果をGoogle Sheetsに出力 ---
    if deals_found:
        sheet_name = config['GoogleSheet']['SheetName']
        worksheet_name = config['GoogleSheet']['WorksheetName']
        
        headers = ['案件ID', 'タイトル', '売上高', '営業利益', 'リンク']
        data_to_write = []
        for deal in deals_found:
            data_to_write.append([
                deal.get('id', ''),
                deal.get('title', ''),
                deal.get('revenue', ''),
                deal.get('profit', '-'),
                deal.get('link', '')
            ])
        
        print(f"\n--- {len(deals_found)}件の案件をGoogle Sheetsに保存します ---\n")
        gs_client = GoogleSheetsClient()
        gs_client.write_data(sheet_name, worksheet_name, headers, data_to_write)
        print("--- 保存完了 ---\\n")
    else:
        print("\n条件に合致する案件は見つかりませんでした。Google Sheetsには何も書き込みません。\n")

    print(f"\n--- {len(deals_found)}件の案件を抽出しました ---")
    headers = ['案件ID', 'タイトル', '売上高', '営業利益', 'リンク']
    
    # Convert list of dicts to list of lists
    data_as_list = []
    for deal in deals_found:
        data_as_list.append([
            deal.get('id', ''),
            deal.get('title', ''),
            deal.get('revenue', ''),
            deal.get('profit', '-'),
            deal.get('link', '')
        ])

    return {"headers": headers, "data": data_as_list}

if __name__ == "__main__":
    results = main()
    if results:
        print("\n" + "="*50)
        print(f"調査完了！ 条件に合致する {len(results['data'])} 件の案件を抽出しました。")
        print("="*50)
        for deal in results['data'][:3]:
            # Assuming the order is [ID, Title, Revenue, Profit, Link]
            print(f"案件ID: {deal[0]}")
            print(f"タイトル: {deal[1]}")
            print(f"売上高: {deal[2]}")
            print(f"営業利益: {deal[3]}")
            print(f"リンク: {deal[4]}")
            print("---")
