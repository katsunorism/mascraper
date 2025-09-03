import time
import re
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# --- 設定項目 ---
# 売上高の最低ライン（単位：円）
MIN_REVENUE_THRESHOLD = 300_000_000
# 取得する最大ページ数
MAX_PAGES_TO_SCRAPE = 5
# 出力するCSVファイル名
OUTPUT_CSV_FILE = "speedma_results.csv"
# ベースURL
BASE_DOMAIN = "https://speed-ma.com"
# ----------------

def parse_financial_value(text):
    """
    「3億円」「7500万円〜1億円」のような文字列を数値(円)に変換する。
    範囲の場合は上限値を採用し、単位を自動で判定する。
    """
    if not text or "非公開" in text or "応相談" in text:
        return 0
    
    # 「〜」や「～」で分割し、上限値（最後の部分）を取得
    parts = re.split(r'[〜～]', text)
    target_text = parts[-1]
    
    # 全角数字を半角に、カンマを除去
    target_text = target_text.translate(str.maketrans('０１２３４５６７８９', '0123456789')).replace(',', '')
    
    # 数字（小数点含む）を抽出
    match = re.search(r'([\d\.]+)', target_text)
    if not match:
        return 0

    value = float(match.group(1))

    # 単位に基づいて円に換算
    if '億' in target_text:
        value *= 100_000_000
    elif '千万' in target_text:
        value *= 10_000_000
    elif '万' in target_text:
        value *= 10_000
    
    return int(value)

def format_link(link):
    """
    リンクを完全なURLに変換する
    """
    if not link:
        return ""
    
    # 既に完全なURLの場合はそのまま返す
    if link.startswith('http'):
        return link
    
    # 相対パスの場合はベースドメインを追加
    if link.startswith('/'):
        return BASE_DOMAIN + link
    else:
        return BASE_DOMAIN + '/' + link

def main():
    "メインの実行関数"
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    base_url = "https://speed-ma.com/projects"
    deals_found = []
    processed_links = set()

    print("スピードM&Aの案件リストのスクレイピングを開始します。")

    try:
        for page_num in range(1, MAX_PAGES_TO_SCRAPE + 1):
            # 1ページ目と2ページ目以降でURL形式を正しく分岐
            if page_num == 1:
                target_url = base_url
            else:
                target_url = f"{base_url}?p={page_num}"
            
            print(f"\n--- ページ {page_num} ({target_url}) を解析中 ---")
            driver.get(target_url)
            
            # 案件リストの最初の要素が表示されるまで最大20秒待機
            try:
                wait = WebDriverWait(driver, 20)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.pcard__box")))
                print("  案件リストの表示を確認しました。")
            except TimeoutException:
                print("  このページに案件が見つかりませんでした。処理を終了します。")
                break

            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # 各案件は <a class="pcard__box"> で囲まれている
            deal_list = soup.select("a.pcard__box")
            
            if not deal_list:
                print("  HTML構造内に案件リストが見つかりませんでした。")
                continue

            print(f"{len(deal_list)}件の案件が見つかりました。情報を抽出します。")

            for deal_item in deal_list:
                # --- HTML構造に合わせて各情報を正確に抽出 ---
                
                link = deal_item.get('href')
                if not link:
                    continue
                
                # リンクを完全なURLに変換
                full_link = format_link(link)
                
                if full_link in processed_links:
                    continue
                processed_links.add(full_link)

                title_tag = deal_item.select_one("span.pcard__title-title")
                title = title_tag.get_text(strip=True) if title_tag else "タイトル不明"

                # 売上高と譲渡価格を抽出
                revenue_text = "情報なし"
                price_text = "情報なし"
                info_list = deal_item.select("dl.pcard__box__info01__list")
                for dl in info_list:
                    dt = dl.find("dt")
                    dd = dl.find("dd")
                    if dt and dd:
                        key = dt.get_text(strip=True)
                        value = dd.get_text(strip=True)
                        if "売上高" in key:
                            revenue_text = value
                        elif "譲渡価格" in key:
                            price_text = value
                
                print(f"  - ({title}) を確認中...")
                print(f"    売上高: {revenue_text}, 譲渡価格: {price_text}")

                # --- 抽出条件の判定ロジック ---
                revenue_in_yen = parse_financial_value(revenue_text)
                
                if revenue_in_yen >= MIN_REVENUE_THRESHOLD:
                    print(f"    [✓] 条件に合致！")
                    deals_found.append({
                        'title': title,
                        'revenue': revenue_text,
                        'profit': '-', # 営業利益は「-」を格納
                        'price': price_text,
                        'link': full_link  # 完全なURLを格納
                    })
                else:
                    print(f"    [×] 条件を満たしませんでした。")

    finally:
        driver.quit()

    print(f"\n--- {len(deals_found)}件の案件を抽出しました ---")
    headers = ['タイトル', '売上高', '営業利益', '譲渡価格', 'リンク']
    
    # Convert list of dicts to list of lists
    data_as_list = []
    for deal in deals_found:
        data_as_list.append([
            deal.get('title', ''),
            deal.get('revenue', ''),
            deal.get('profit', '-'),
            deal.get('price', ''),
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
            # Assuming the order is [Title, Revenue, Profit, Price, Link]
            print(f"タイトル: {deal[0]}")
            print(f"売上高: {deal[1]}")
            print(f"譲渡価格: {deal[3]}")
            print(f"リンク: {deal[4]}")
            print("-" * 30)
