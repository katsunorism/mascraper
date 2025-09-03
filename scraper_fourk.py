import time
import re
import configparser
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import sys
import os
from google_sheets_client import GoogleSheetsClient

def load_config():
    """設定ファイル(config.ini)を読み込む"""
    config = configparser.ConfigParser()
    
    # config.iniファイルの存在確認
    if not os.path.exists('config.ini'):
        print("エラー: config.iniファイルが見つかりません。")
        return None
        
    try:
        config.read('config.ini', encoding='utf-8-sig')
    except Exception as e:
        print(f"設定ファイルの読み込みに失敗しました: {e}")
        return None
    
    # Configure stdout to use UTF-8
    if sys.stdout.encoding != 'utf-8':
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except AttributeError:
            # Python 3.6以下の場合の対応
            pass
        
    return config

def setup_chrome_driver():
    """Chrome WebDriverの設定と初期化"""
    try:
        chrome_options = Options()
        # ヘッドレスモードを無効化（デバッグ用）
        # chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.implicitly_wait(10)
        
        return driver
    except Exception as e:
        print(f"ChromeDriverの初期化に失敗しました: {e}")
        return None

# --- 設定項目 ---
# 売上高の最低ライン（単位：円）
MIN_REVENUE_THRESHOLD = 300_000_000
# ベースURL
BASE_DOMAIN = "https://fourk.jp"
TARGET_URL = "https://fourk.jp/sys/pub/ma/article/cede"
# ----------------

def parse_financial_value(text):
    """
    「3億円～5億円」「5,000万円～1億円」のような文字列を数値(円)に変換する。
    範囲の場合は上限値を採用し、単位を自動で判定する。
    """
    if not text or "非公開" in text or "応相談" in text or text == "情報なし":
        return 0
    
    # 「～」「〜」「-」で分割し、上限値（最後の部分）を取得
    parts = re.split(r'[〜～\-]', text)
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

def extract_deal_info(deal_content, deal_id):
    """案件情報を抽出する"""
    # 案件概要を抽出
    title = "案件概要不明"
    concept_patterns = [
        r'案件概要[：:\s\-]*([^\n\-]+)',
        r'企業概要[：:\s\-]*([^\n\-]+)',
        r'業種[：:\s\-]*([^\n\-]+)'
    ]
    for pattern in concept_patterns:
        concept_match = re.search(pattern, deal_content)
        if concept_match:
            title = clean_text(concept_match.group(1))
            break
    
    # 各種情報を抽出
    revenue_text = "情報なし"
    price_text = "情報なし"
    region_text = "地域不明"
    
    # 売上規模を抽出
    revenue_patterns = [
        r'売上規模[：:\s\-]*([^\n]+)',
        r'売上高[：:\s\-]*([^\n]+)',
        r'年商[：:\s\-]*([^\n]+)'
    ]
    for pattern in revenue_patterns:
        revenue_match = re.search(pattern, deal_content)
        if revenue_match:
            revenue_text = clean_text(revenue_match.group(1))
            break
    
    # 価格目線を抽出
    price_patterns = [
        r'価格目線[：:\s\-]*([^\n]+)',
        r'希望価格[：:\s\-]*([^\n]+)',
        r'譲渡価格[：:\s\-]*([^\n]+)'
    ]
    for pattern in price_patterns:
        price_match = re.search(pattern, deal_content)
        if price_match:
            price_text = clean_text(price_match.group(1))
            break
    
    # 地域を抽出
    region_patterns = [
        r'地\s*域[：:\s\-]*([^\n]+)',
        r'所在地[：:\s\-]*([^\n]+)',
        r'エリア[：:\s\-]*([^\n]+)'
    ]
    for pattern in region_patterns:
        region_match = re.search(pattern, deal_content)
        if region_match:
            region_text = clean_text(region_match.group(1))
            break
    
    return {
        'deal_id': deal_id,
        'title': title,
        'region': region_text,
        'revenue': revenue_text,
        'profit': '-',  # 営業利益情報は無いため「-」
        'price': price_text,
        'link': TARGET_URL  # 全て同じページなので固定URL
    }

def main():
    """メインの実行関数"""
    # 設定ファイル読み込み
    config = load_config()
    if config is None:
        return []
    
    # WebDriver初期化
    driver = setup_chrome_driver()
    if driver is None:
        return []
    
    deals_found = []
    processed_deal_ids = set()  # 重複チェック用

    print("フォーナレッジの案件リストのスクレイピングを開始します。")
    print(f"対象URL: {TARGET_URL}")

    try:
        driver.get(TARGET_URL)
        print("ページにアクセスしました。")
        
        # ページの読み込みを待機
        try:
            wait = WebDriverWait(driver, 30)
            # メインコンテンツが読み込まれるまで待機
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(5)  # 追加の待機時間
            print("ページの読み込みが完了しました。")
        except TimeoutException:
            print("ページの読み込みがタイムアウトしました。処理を終了します。")
            return []

        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # デバッグ用：ページのテキスト内容を一部確認
        page_text = soup.get_text()
        print(f"ページテキストの最初の500文字: {page_text[:500]}")
        
        # より柔軟な案件ID検索パターン
        deal_id_patterns = [
            r'■\s*([S]\d+)',          # ■ S1080
            r'●\s*([S]\d+)',          # ● S1080
            r'・\s*([S]\d+)',         # ・ S1080
            r'([S]\d{3,4})',          # S1080（単体）
            r'案件ID[：:\s]*([S]\d+)', # 案件ID: S1080
        ]
        
        deal_matches = []
        used_pattern = ""
        
        for pattern in deal_id_patterns:
            matches = list(re.finditer(pattern, page_text, re.IGNORECASE))
            if matches:
                print(f"パターン '{pattern}' で {len(matches)} 件の案件を発見")
                deal_matches = matches
                used_pattern = pattern
                break
        
        if not deal_matches:
            print("案件IDが見つかりませんでした。HTMLの詳細を確認します。")
            # HTMLソースの詳細確認
            debug_filename = 'debug_fourk_page_source.html'
            try:
                with open(debug_filename, 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                print(f"ページソースを {debug_filename} に保存しました。")
            except Exception as e:
                print(f"デバッグファイルの保存に失敗しました: {e}")
            return []
        
        print(f"発見された案件数: {len(deal_matches)}")
        
        for i, match in enumerate(deal_matches):
            try:
                deal_id = match.group(1)  # S1080形式
                
                # 重複チェック
                if deal_id in processed_deal_ids:
                    print(f"  案件ID {deal_id}: 重複のためスキップ")
                    continue
                processed_deal_ids.add(deal_id)
                
                # 案件IDから次の案件IDまでのテキストを抽出
                start_pos = match.start()
                
                # 次の案件IDの位置を探す
                next_match = None
                for j in range(i + 1, len(deal_matches)):
                    next_candidate = deal_matches[j]
                    if next_candidate.start() > start_pos:
                        next_match = next_candidate
                        break
                
                if next_match:
                    deal_content = page_text[start_pos:next_match.start()]
                else:
                    # 最後の案件の場合
                    deal_content = page_text[start_pos:start_pos + 3000]  # 適度な長さで区切り
                
                # 【成約】が含まれる場合はスキップ
                if '【成約】' in deal_content or '成約済' in deal_content:
                    print(f"  案件ID {deal_id}: 成約済みのためスキップ")
                    continue
                
                # 案件情報を抽出
                deal_info = extract_deal_info(deal_content, deal_id)
                
                print(f"  案件ID {deal_id}: {deal_info['title']}")
                print(f"    地域: {deal_info['region']}")
                print(f"    売上規模: {deal_info['revenue']}")
                print(f"    価格目線: {deal_info['price']}")
                
                # --- 抽出条件の判定ロジック ---
                revenue_in_yen = parse_financial_value(deal_info['revenue'])
                
                if revenue_in_yen >= MIN_REVENUE_THRESHOLD:
                    print(f"    [✓] 条件に合致！（{revenue_in_yen:,}円）")
                    deals_found.append(deal_info)
                else:
                    print(f"    [×] 条件を満たしませんでした。（{revenue_in_yen:,}円 < {MIN_REVENUE_THRESHOLD:,}円）")
                    
            except Exception as e:
                print(f"  案件 {i+1} の処理中にエラーが発生しました: {e}")
                continue

    except WebDriverException as e:
        print(f"WebDriverエラーが発生しました: {e}")
    except Exception as e:
        print(f"予期しないエラーが発生しました: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass

    # --- 最終結果をGoogle Sheetsに出力 ---
    if deals_found:
        try:
            sheet_name = config['GoogleSheet']['SheetName']
            worksheet_name = config['GoogleSheet']['WorksheetName']
            
            headers = ['案件ID', 'タイトル', '地域', '売上規模', '営業利益', '価格目線', 'リンク']
            data_to_write = []
            for deal in deals_found:
                data_to_write.append([
                    deal.get('deal_id', ''),
                    deal.get('title', ''),
                    deal.get('region', ''),
                    deal.get('revenue', ''),
                    deal.get('profit', ''),
                    deal.get('price', ''),
                    deal.get('link', '')
                ])
            
            print(f"\n--- {len(deals_found)}件の案件をGoogle Sheetsに保存します ---\n")
            gs_client = GoogleSheetsClient()
            gs_client.write_data(sheet_name, worksheet_name, headers, data_to_write)
            print("--- 保存完了---\n")
            
        except KeyError as e:
            print(f"設定ファイルに必要なキーが見つかりません: {e}")
        except Exception as e:
            print(f"Google Sheetsへの書き込みに失敗しました: {e}")
    else:
        print("\n条件に合致する案件は見つかりませんでした。Google Sheetsには何も書き込みません。\n")

    return deals_found

if __name__ == "__main__":
    try:
        results = main()
        if results:
            # --- コンソールに最終結果を表示 ---
            print("\n" + "="*60)
            print(f"調査完了！ 条件に合致する {len(results)} 件の案件を抽出しました。")
            print("="*60)

            for i, deal in enumerate(results, 1):
                print(f"{i}. 案件ID: {deal['deal_id']}")
                print(f"   タイトル: {deal['title']}")
                print(f"   地域: {deal['region']}")
                print(f"   売上規模: {deal['revenue']}")
                print(f"   営業利益: {deal['profit']}")
                print(f"   価格目線: {deal['price']}")
                print(f"   リンク: {deal['link']}")
                print("-" * 40)
        else:
            print("条件に合致する案件が見つかりませんでした。")
            
    except KeyboardInterrupt:
        print("\n処理が中断されました。")
    except Exception as e:
        print(f"プログラムの実行中にエラーが発生しました: {e}")