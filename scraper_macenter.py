import time
import re
import configparser
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from google_sheets_client import GoogleSheetsClient

def load_config():
    """設定ファイル(config.ini)を読み込む"""
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8-sig')
    return config

# --- 設定項目 ---
MIN_REVENUE_THRESHOLD = 3  # 売上高の最低ライン（単位：億円）
MAX_PAGES_TO_SCRAPE = 5    # 取得する最大ページ数
# ----------------

def parse_revenue(revenue_text):
    ""「3億円」「2億円～5億円」等を数値に変換する（範囲の場合は上限値を採用）"""
    if not revenue_text:
        return 0
    
    # 範囲表記の場合（例：2億円～5億円）
    if "～" in revenue_text:
        # 上限値を取得（～の後の数値）
        upper_part = revenue_text.split("～")[1]
        match = re.search(r'([\d\.]+)', upper_part)
        if match:
            return float(match.group(1))
    
    # 単一値の場合（例：3億円）
    match = re.search(r'([\d\.]+)', revenue_text)
    if match:
        return float(match.group(1))
    
    return 0

def extract_deal_info_from_listing(deal_element):
    ""一覧ページの案件要素から情報を抽出する"""
    try:
        # タイトルとリンクを取得
        title_link = deal_element.find('a')
        if not title_link:
            return None
            
        title = title_link.get('title') or title_link.get_text(strip=True)
        link = title_link.get('href')
        
        # 売上高情報を探す（複数のパターンを試行）
        revenue_text = ""
        
        # パターン1: 売上高というテキストの近くを探す
        revenue_elements = deal_element.find_all(string=re.compile(r'売上高|売上'))
        if revenue_elements:
            for elem in revenue_elements:
                parent = elem.parent
                if parent:
                    # 同じ行または近くの要素から数値を探す
                    siblings = parent.find_next_siblings() + parent.find_previous_siblings()
                    for sibling in siblings:
                        text = sibling.get_text(strip=True)
                        if re.search(r'\d+億円', text):
                            revenue_text = text
                            break
                    if revenue_text:
                        break
        
        # パターン2: 億円を含むテキストを直接探す
        if not revenue_text:
            revenue_candidates = deal_element.find_all(string=re.compile(r'\d+億円'))
            if revenue_candidates:
                revenue_text = revenue_candidates[0].strip()
        
        # パターン3: テーブル構造から探す
        if not revenue_text:
            tds = deal_element.find_all('td')
            for td in tds:
                text = td.get_text(strip=True)
                if '億円' in text and re.search(r'\d+', text):
                    revenue_text = text
                    break
        
        return {
            'title': title,
            'link': f"https://www.nihon-ma.co.jp{link}" if link.startswith('/') else link,
            'revenue': revenue_text
        }
    except Exception as e:
        print(f"    [警告] 要素の解析中にエラー: {e}")
        return None

def main():
    ""メインの実行関数"""
    config = load_config()
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    base_url = "https://www.nihon-ma.co.jp"
    deals_found = []

    print("一覧ページから直接売上高情報を取得します。")

    try:
        for page_num in range(1, MAX_PAGES_TO_SCRAPE + 1):
            print(f"\nページ {page_num} を解析中...")
            target_url = f"{base_url}/anken/needs_convey.php?p={page_num}"
            driver.get(target_url)
            time.sleep(2)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # 案件リストの構造を特定（複数のパターンを試行）
            # パターン1: テーブル行
            deal_rows = soup.find_all('tr')
            if not deal_rows:
                # パターン2: divベースのリスト
                deal_rows = soup.find_all('div', class_=re.compile(r'item|deal|anken'))
            
            page_deals_count = 0
            
            for row in deal_rows:
                # リンクを含む行のみ処理
                if not row.find('a', href=re.compile(r'needs_convey_single\.php')):
                    continue
                    
                deal_info = extract_deal_info_from_listing(row)
                if not deal_info:
                    continue
                    
                page_deals_count += 1
                print(f"  - 発見: {deal_info['title'][:50]}..." if len(deal_info['title']) > 50 else f"  - 発見: {deal_info['title']}")
                print(f"    売上高: {deal_info['revenue'] if deal_info['revenue'] else '情報なし'}")
                
                # 売上高の条件判定
                if deal_info['revenue']:
                    # 「非公開」パターンをチェック
                    if re.search(r'非公開|非開示|未公開|confidential|private', deal_info['revenue'], re.IGNORECASE):
                        print(f"    [SKIP] 売上高が非公開のため対象外")
                        continue
                    
                    revenue_value = parse_revenue(deal_info['revenue'])
                    if revenue_value >= MIN_REVENUE_THRESHOLD:
                        print(f"    [✓] 条件に合致！")
                        deals_found.append(deal_info)
                    else:
                        print(f"    [×] 売上高が基準値以下 ({revenue_value}億円 < {MIN_REVENUE_THRESHOLD}億円)")
                else:
                    print(f"    [SKIP] 売上高情報が見つからないため対象外（詳細ページアクセスを省略）")
            
            print(f"ページ {page_num}: {page_deals_count} 件の案件を処理")
            
            if page_deals_count == 0:
                print("案件が見つからないため、スクレイピングを終了します。")
                break

    finally:
        driver.quit()

    # --- 最終結果をGoogle Sheetsに出力 ---
    if deals_found:
        sheet_name = config['GoogleSheet']['SheetName']
        worksheet_name = config['GoogleSheet']['WorksheetName']
        
        headers = ['タイトル', 'リンク', '売上高']
        data_to_write = []
        for deal in deals_found:
            data_to_write.append([
                deal.get('title', ''),
                deal.get('link', ''),
                deal.get('revenue', '')
            ])
        
        print(f"\n--- {len(deals_found)}件の案件をGoogle Sheetsに保存します ---\n")
        gs_client = GoogleSheetsClient()
        gs_client.write_data(sheet_name, worksheet_name, headers, data_to_write)
        print("--- 保存完了 ---
")
    else:
        print("\n条件に合致する案件は見つかりませんでした。Google Sheetsには何も書き込みません。\n")

    print(f"\n--- {len(deals_found)}件の案件を抽出しました ---")
    headers = ['タイトル', 'リンク', '売上高']
    
    data_as_list = []
    for deal in deals_found:
        data_as_list.append([
            deal.get('title', ''),
            deal.get('link', ''),
            deal.get('revenue', '')
        ])

    return {"headers": headers, "data": data_as_list}

if __name__ == "__main__":
    results = main()
    if results:
        print("\n" + "="*50)
        print(f"最適化完了！ 条件に合致する {len(results['data'])} 件の案件が見つかりました。")
        print("="*50)
        for deal in results['data'][:3]:
            print(f"タイトル: {deal[0]}")
            print(f"リンク: {deal[1]}")
            print(f"売上高: {deal[2]}")
            print("-" * 30)
