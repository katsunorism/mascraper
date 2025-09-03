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
# 出力するCSVファイル名
OUTPUT_CSV_FILE = "newold_results.csv"
# ベースURL
BASE_DOMAIN = "https://newold.co.jp"
TARGET_URL = "https://newold.co.jp/anken/"
# ----------------

def parse_financial_value(text):
    """
    「3億円～5億円」「5,000万円～1億円」のような文字列を数値(円)に変換する。
    範囲の場合は上限値を採用し、単位を自動で判定する。
    """
    if not text or "非公開" in text or "応相談" in text:
        return 0
    
    # 「～」「〜」で分割し、上限値（最後の部分）を取得
    parts = re.split(r'[〜～]', text)
    target_text = parts[-1].strip()
    
    # 全角数字を半角に、カンマを除去
    target_text = target_text.translate(str.maketrans('０１２３４５６７８９', '0123456789')).replace(',', '')
    
    # 数字（小数点、カンマ含む）を抽出
    match = re.search(r'([\d,\.]+)', target_text)
    if not match:
        return 0

    value_str = match.group(1).replace(',', '')
    try:
        value = float(value_str)
    except ValueError:
        return 0

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

def clean_text(text):
    """
    テキストから不要な空白を除去し、整形する
    """
    if not text:
        return text
    
    # 改行を空白に置換
    text = re.sub(r'\n+', ' ', text)
    # 連続する空白を単一の空白に置換
    text = re.sub(r'\s+', ' ', text)
    # 前後の空白を除去
    text = text.strip()
    
    return text

def main():
    "メインの実行関数"
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    deals_found = []
    processed_deal_ids = set()  # 重複チェック用

    print("NEWOLD CAPITALの案件リストのスクレイピングを開始します。")
    print(f"対象URL: {TARGET_URL}")

    try:
        driver.get(TARGET_URL)
        
        # ページの読み込みを待機
        try:
            wait = WebDriverWait(driver, 20)
            # メインコンテンツが読み込まれるまで待機
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(3)  # 追加の待機時間
            print("ページの読み込みが完了しました。")
        except TimeoutException:
            print("ページの読み込みがタイムアウトしました。処理を終了します。")
            return

        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # デバッグ用：ページのテキスト内容を一部確認
        page_text = soup.get_text()
        print(f"ページテキストの最初の500文字: {page_text[:500]}")
        
        # より柔軟な案件ID検索パターンを試行
        deal_id_patterns = [
            r'案件ID[：:]\s*(\d+)',  # 「案件ID：」または「案件ID:」
            r'案件ID\s+(\d+)',       # 「案件ID 」（スペース区切り）
            r'ID[：:]\s*(\d+)',      # 「ID：」
        ]
        
        deal_matches = []
        for pattern in deal_id_patterns:
            matches = list(re.finditer(pattern, page_text))
            if matches:
                print(f"パターン '{pattern}' で {len(matches)} 件の案件を発見")
                deal_matches = matches
                break
        
        if not deal_matches:
            print("案件IDが見つかりませんでした。HTMLの詳細を確認します。")
            # HTMLソースの詳細確認
            with open('debug_page_source.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print("ページソースを debug_page_source.html に保存しました。")
            return
        
        print(f"発見された案件数: {len(deal_matches)}")
        
        for match in deal_matches:
            deal_id = match.group(1)
            
            # 重複チェック
            if deal_id in processed_deal_ids:
                print(f"  案件ID {deal_id}: 重複のためスキップ")
                continue
            processed_deal_ids.add(deal_id)
            
            # 案件IDから次の案件IDまでのテキストを抽出
            start_pos = match.start()
            
            # 次の案件IDの位置を探す
            next_match = None
            for next_candidate in deal_matches:
                if next_candidate.start() > start_pos:
                    next_match = next_candidate
                    break
            
            if next_match:
                deal_content = page_text[start_pos:next_match.start()]
            else:
                # 最後の案件の場合
                deal_content = page_text[start_pos:]
            
            # タイトルを抽出（案件IDの後の最初の行）
            content_lines = deal_content.split('\n')
            title = "タイトル不明"
            for i, line in enumerate(content_lines):
                line = line.strip()
                if line and not line.startswith('案件ID') and not line.startswith('ID'):
                    title = clean_text(line)
                    break
            
            # 各種情報を抽出
            revenue_text = "情報なし"
            industry_text = "業種不明"
            region_text = "地域不明"
            
            # 売上高を抽出
            revenue_patterns = [
                r'売上高[：:\s-]*([^\n]+)',
                r'売上[：:\s-]*([^\n]+)',
            ]
            for pattern in revenue_patterns:
                revenue_match = re.search(pattern, deal_content)
                if revenue_match:
                    revenue_text = clean_text(revenue_match.group(1))
                    break
            
            # 業種を抽出
            industry_patterns = [
                r'業種[：:\s-]*([^\n]+)',
            ]
            for pattern in industry_patterns:
                industry_match = re.search(pattern, deal_content)
                if industry_match:
                    industry_text = clean_text(industry_match.group(1))
                    break
            
            # 地域を抽出
            region_patterns = [
                r'地域[：:\s-]*([^\n]+)',
            ]
            for pattern in region_patterns:
                region_match = re.search(pattern, deal_content)
                if region_match:
                    region_text = clean_text(region_match.group(1))
                    break
            
            # 詳細ページのリンクを探す
            link_pattern = rf'/anken-list/{deal_id}/'
            link_element = soup.find('a', href=re.compile(link_pattern))
            detail_link = ""
            if link_element:
                detail_link = format_link(link_element.get('href'))
            
            print(f"  案件ID {deal_id}: {title}")
            print(f"    業種: {industry_text}")
            print(f"    地域: {region_text}")
            print(f"    売上高: {revenue_text}")
            
            # --- 抽出条件の判定ロジック ---
            revenue_in_yen = parse_financial_value(revenue_text)
            
            if revenue_in_yen >= MIN_REVENUE_THRESHOLD:
                print(f"    [✓] 条件に合致！（{revenue_in_yen:,}円）")
                deals_found.append({
                    'deal_id': deal_id,
                    'title': title,
                    'industry': industry_text,
                    'region': region_text,
                    'revenue': revenue_text,
                    'profit': '-',  # 営業利益情報は無いため「-」
                    'price': '-',   # 譲渡価格情報は無いため「-」
                    'link': detail_link
                })
            else:
                print(f"    [×] 条件を満たしませんでした。（{revenue_in_yen:,}円 < {MIN_REVENUE_THRESHOLD:,}円）")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
    finally:
        driver.quit()

    # --- 最終結果をCSVに出力 ---
    print(f"\n--- {len(deals_found)}件の案件を「{OUTPUT_CSV_FILE}」に保存します ---")
    try:
        with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['案件ID', 'タイトル', '業種', '地域', '売上高', '営業利益', '譲渡価格', 'リンク'])
            for deal in deals_found:
                writer.writerow([
                    deal['deal_id'], deal['title'], deal['industry'], 
                    deal['region'], deal['revenue'], deal['profit'], 
                    deal['price'], deal['link']
                ])
        print("--- 保存完了 ---")
    except Exception as e:
        print(f"!!! ファイルの保存中にエラーが発生しました: {e} !!!")

    # --- コンソールに最終結果を表示 ---
    print("\n" + "="*60)
    print(f"調査完了！ 条件に合致する {len(deals_found)} 件の案件を抽出しました。")
    print("="*60)

    for deal in deals_found:
        print(f"案件ID: {deal['deal_id']}")
        print(f"タイトル: {deal['title']}")
        print(f"業種: {deal['industry']}")
        print(f"地域: {deal['region']}")
        print(f"売上高: {deal['revenue']}")
        print(f"営業利益: {deal['profit']}")
        print(f"譲渡価格: {deal['price']}")
        print(f"リンク: {deal['link']}")
        print("-" * 40)

if __name__ == "__main__":
    results = main()
    if results:
        print("\n" + "="*60)
        print(f"調査完了！ 条件に合致する {len(results['data'])} 件の案件を抽出しました。")
        print("="*60)
        for deal in results['data'][:3]:
            print(f"案件ID: {deal[0]}")
            print(f"タイトル: {deal[1]}")
            print(f"業種: {deal[2]}")
            print(f"地域: {deal[3]}")
            print(f"売上高: {deal[4]}")
            print(f"リンク: {deal[7]}")
            print("-" * 40)
