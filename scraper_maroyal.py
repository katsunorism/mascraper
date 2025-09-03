import time
import re
import configparser
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

def load_config():
    """設定ファイル(config.ini)を読み込む"""
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8-sig')
    return config

# --- 設定項目 ---
MIN_REVENUE_THRESHOLD = 300_000_000  # 売上高の最低ライン（単位：円）
MIN_PROFIT_THRESHOLD = 30_000_000     # 営業利益の最低ライン（単位：円）
MAX_PAGES_TO_SCRAPE = 5               # 取得する最大ページ数
SERVICE_ACCOUNT_FILE = 'test-key.json' # サービスアカウントキーファイルのパス
SITE_NAME = "M&Aキャピタルパートナーズ" # サイト名
# ----------------

class GoogleSheetsServiceAccount:
    """サービスアカウントを使用したGoogle Sheets クライアント"""
    
    def __init__(self, service_account_file):
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        credentials = Credentials.from_service_account_file(
            service_account_file, 
            scopes=scopes
        )
        
        self.gc = gspread.authorize(credentials)
        print(f"サービスアカウントでGoogle Sheets APIに接続しました: {service_account_file}")
    
    def write_data(self, spreadsheet_id, worksheet_name, headers, data):
        try:
            spreadsheet = self.gc.open_by_key(spreadsheet_id)
            print(f"スプレッドシート '{spreadsheet.title}' を開きました")
            
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                print(f"既存のワークシート '{worksheet_name}' を使用します")
                
                existing_data = worksheet.get_all_values()
                
                if existing_data:
                    print(f"既存データが {len(existing_data)} 行見つかりました。データを追加します。")
                    
                    if existing_data[0] != headers:
                        print("ヘッダーが異なるか存在しません。ヘッダーを追加します。")
                        worksheet.append_row(headers)
                    else:
                        print("ヘッダーは既に存在するため、スキップします。")
                    
                    if data:
                        for row in data:
                            worksheet.append_row(row)
                        print(f"{len(data)} 行のデータを既存データの下に追加しました")
                else:
                    print("既存データが見つかりませんでした。新規でデータを書き込みます。")
                    
                    if headers:
                        worksheet.append_row(headers)
                        print(f"ヘッダーを書き込みました: {headers}")
                    
                    if data:
                        for row in data:
                            worksheet.append_row(row)
                        print(f"{len(data)} 行のデータを書き込みました")
                            
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(
                    title=worksheet_name, 
                    rows=len(data) + 10, 
                    cols=len(headers) + 5
                )
                print(f"新しいワークシート '{worksheet_name}' を作成しました")
                
                if headers:
                    worksheet.append_row(headers)
                    print(f"ヘッダーを書き込みました: {headers}")
                
                if data:
                    for row in data:
                        worksheet.append_row(row)
                    print(f"{len(data)} 行のデータを書き込みました")
            
            print(f"Google Sheetsへの書き込み完了！")
            print(f"URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
            
        except Exception as e:
            print(f"Google Sheetsへの書き込み中にエラーが発生しました: {e}")
            raise

def parse_financial_value(text):
    """「3億円」「2.5億円～5億円」「5,000万円」のような文字列を数値(円)に変換する。"""
    if not text or "非公開" in text or "応相談" in text:
        return 0
    
    if "～" in text:
        text = text.split("～")[1]

    text = text.replace(',', '')
    match = re.search(r'([\d\.]+)', text)
    if not match:
        return 0

    value = float(match.group(1))

    if '億' in text:
        value *= 100_000_000
    elif '千万' in text:
        value *= 10_000_000
    elif '万' in text:
        value *= 10_000
    
    return int(value)

def convert_to_million_yen_format(text):
    """「2.5億円～5億円」のような文字列を「250百万円～500百万円」形式に変換する。"""
    if not text or "非公開" in text or "応相談" in text:
        return text
    
    if "～" in text:
        parts = text.split("～")
        converted_parts = []
        for part in parts:
            converted_parts.append(convert_single_value_to_million(part.strip()))
        return "～".join(converted_parts)
    else:
        return convert_single_value_to_million(text)

def convert_single_value_to_million(text):
    """単一の金額文字列を百万円単位に変換する。"""
    if not text:
        return text
    
    clean_text = text.replace(',', '')
    match = re.search(r'([\d\.]+)', clean_text)
    if not match:
        return text

    value = float(match.group(1))
    
    if '億' in text:
        million_value = value * 100
        return f"{int(million_value):,}百万円" if million_value.is_integer() else f"{million_value:,}百万円"
    elif '千万' in text:
        million_value = value * 10
        return f"{int(million_value):,}百万円" if million_value.is_integer() else f"{million_value:,}百万円"
    elif '万' in text:
        million_value = value / 100
        return f"{int(million_value):,}百万円" if million_value >= 1 and million_value.is_integer() else (f"{million_value:,}百万円" if million_value >= 1 else text)
    else:
        return text

def get_feature_from_detail_page(driver, url):
    """案件詳細ページにアクセスし、「事業概要」や「事業内容」の情報を抽出する。"""
    try:
        print(f"      - 詳細ページにアクセス中: {url}")
        driver.get(url)
        time.sleep(3)
        
        detail_soup = BeautifulSoup(driver.page_source, "html.parser")
        feature_text = ""
        
        target_keywords = ["事業概要", "事業内容", "特色", "企業の特徴"]
        for keyword in target_keywords:
            target_h4 = detail_soup.find("h4", string=lambda t: t and keyword in t)
            if target_h4:
                print(f"      - 見出し「{keyword}」を発見")
                next_element = target_h4.find_next_sibling()
                collected_text = []
                
                while next_element:
                    if next_element.name == "h4":
                        break
                    
                    if next_element.name == "ul":
                        li_texts = [li.get_text(strip=True) for li in next_element.find_all("li") if li.get_text(strip=True)]
                        if li_texts:
                            collected_text.extend(li_texts)
                    
                    elif next_element.name == "p":
                        p_text = next_element.get_text(strip=True)
                        if p_text and p_text not in ["", "・"]:
                            collected_text.append(p_text)
                    
                    elif next_element.name == "div":
                        div_text = next_element.get_text(strip=True)
                        if div_text and len(div_text) > 5:
                            collected_text.append(div_text)
                    
                    next_element = next_element.find_next_sibling()
                
                if collected_text:
                    feature_text = "\n".join(collected_text)
                    print(f"      - 特色情報を取得しました: {len(collected_text)}項目")
                    break
        
        if not feature_text:
            print("      - 見出しベースでの検索に失敗。ページ全体から箇条書きを探しています...")
            all_text = detail_soup.get_text()
            bullet_lines = []
            for line in all_text.split('\n'):
                line = line.strip()
                if line.startswith('・') and len(line) > 5:
                    bullet_lines.append(line)
            
            if bullet_lines and len(bullet_lines) >= 2:
                feature_text = "\n".join(bullet_lines[:10])
                print(f"      - 箇条書きパターンから特色情報を取得: {len(bullet_lines)}項目")
        
        return feature_text

    except Exception as e:
        print(f"      - 詳細ページの解析中にエラーが発生しました: {e}")
    
    print("      - 特色情報は見つかりませんでした。")
    return ""

def main():
    """メインの実行関数"""
    config = load_config()
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    base_url = "https://www.ma-cp.com"
    deals_found = []
    processed_ids = set()
    
    current_date = datetime.now().strftime("%Y/%m/%d")

    print("M&Aキャピタルパートナーズの案件リストのスクレイピングを開始します。")

    qualified_deals = []

    try:
        # フェーズ1: 一覧ページから条件に合致する案件を抽出
        print("\n=== フェーズ1: 条件に合致する案件を抽出中 ===")
        for page_num in range(1, MAX_PAGES_TO_SCRAPE + 1):
            if page_num == 1:
                target_url = f"{base_url}/deal/"
            else:
                target_url = f"{base_url}/deal/?p={page_num}"
            
            print(f"\n--- ページ {page_num} ({target_url}) を解析中 ---")
            driver.get(target_url)
            time.sleep(3)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            deal_articles = soup.select("article.c-filter-project")

            if not deal_articles:
                print("このページに案件が見つかりませんでした。")
                continue
            
            print(f"{len(deal_articles)}件の案件が見つかりました。条件を確認します。")

            for article in deal_articles:
                id_tag = article.find("p", class_="c-filter-project__no")
                case_id_text = id_tag.get_text(strip=True) if id_tag else "ID不明"
                case_id = case_id_text.replace("案件No：", "").strip()

                if case_id in processed_ids:
                    continue
                processed_ids.add(case_id)

                title_tag = article.find("h4", class_="c-filter-project__ttl")
                title = title_tag.get_text(strip=True) if title_tag else "タイトル不明"

                link_tag = article.find("a", class_="c-cta")
                if link_tag and link_tag.get('href'):
                    relative_link = link_tag['href']
                    if relative_link.startswith('/'):
                        link = base_url + relative_link
                    else:
                        link = relative_link
                else:
                    link = "リンク不明"

                revenue_text = ""
                profit_text = ""
                location_text = ""
                desired_amount_text = ""
                
                data_list = article.select("dl.c-filter-project__dataList")
                for dl in data_list:
                    dt = dl.find("dt")
                    dd = dl.find("dd")
                    if dt and dd:
                        key = dt.get_text(strip=True)
                        value = dd.get_text(strip=True)
                        if "概算売上" in key:
                            revenue_text = value
                        elif "営業利益" in key:
                            profit_text = value
                        elif "所在地" in key:
                            location_text = value
                        elif "希望金額" in key or "希望価格" in key or "譲渡価格" in key:
                            desired_amount_text = value
                
                print(f"  - ID: {case_id} ({title}) を確認中...")
                print(f"    売上高: {revenue_text or '情報なし'}, 営業利益: {profit_text or '情報なし'}")

                revenue_in_yen = parse_financial_value(revenue_text)
                profit_in_yen = parse_financial_value(profit_text)

                is_revenue_ok = revenue_in_yen >= MIN_REVENUE_THRESHOLD
                is_profit_ok = profit_in_yen >= MIN_PROFIT_THRESHOLD

                if is_revenue_ok and is_profit_ok:
                    print(f"    [OK] 条件に合致！")
                    
                    qualified_deals.append({
                        'date': current_date,
                        'site': SITE_NAME,
                        'id': case_id,
                        'title': title,
                        'location': location_text,
                        'revenue': convert_to_million_yen_format(revenue_text),
                        'profit': convert_to_million_yen_format(profit_text),
                        'desired_amount': convert_to_million_yen_format(desired_amount_text),  # ★ 変更点: 百万円単位に変換
                        'link': link
                    })
                else:
                    print(f"    [×] 条件を満しませんでした。")

        print(f"\n=== フェーズ1完了: {len(qualified_deals)}件の条件合致案件を発見 ===")
        
        # フェーズ2: 条件に合致した案件の詳細ページから特色情報を取得
        print(f"\n=== フェーズ2: {len(qualified_deals)}件の詳細情報を取得中 ===")
        for i, deal in enumerate(qualified_deals, 1):
            print(f"\n--- {i}/{len(qualified_deals)}: ID {deal['id']} の詳細情報を取得中 ---")
            feature_text = ""
            if deal['link'] != "リンク不明":
                feature_text = get_feature_from_detail_page(driver, deal['link'])
            
            deal['feature'] = feature_text
            deals_found.append(deal)
            
            if i < len(qualified_deals):
                time.sleep(1)

    finally:
        driver.quit()

    print(f"\n--- 最終結果: {len(deals_found)}件の案件情報を取得完了 ---")

    headers = ['抽出日時', 'サイト名', '案件ID', 'タイトル', '所在地', '売上高', '営業利益', '希望金額', '特色', 'リンク']
    
    data_as_list = []
    for deal in deals_found:
        data_as_list.append([
            deal.get('date', ''),
            deal.get('site', ''),
            deal.get('id', ''),
            deal.get('title', ''),
            deal.get('location', ''),
            deal.get('revenue', ''),
            deal.get('profit', ''),
            deal.get('desired_amount', ''),
            deal.get('feature', ''),
            deal.get('link', '')
        ])

    return {"headers": headers, "data": data_as_list}

if __name__ == "__main__":
    results = main()
    if results and results['data']:
        print("\n" + "="*50)
        print(f"調査完了！ 条件に合致する {len(results['data'])} 件の案件が見つかりました。")
        print("="*50)
        for deal in results['data'][:3]: # 最初の3件をプレビュー
            print(f"抽出日時: {deal[0]}")
            print(f"サイト名: {deal[1]}")
            print(f"案件ID: {deal[2]}")
            print(f"タイトル: {deal[3]}")
            print(f"所在地: {deal[4]}")
            print(f"売上高: {deal[5]}")
            print(f"営業利益: {deal[6]}")
            print(f"希望金額: {deal[7]}")
            print(f"特色: {deal[8]}")
            print(f"リンク: {deal[9]}")
            print("-" * 30)

        # Google Sheetsへの書き込み
        try:
            gs_client = GoogleSheetsServiceAccount(SERVICE_ACCOUNT_FILE)
            spreadsheet_id = "1B3cRFiAMTwCscQyLkJbS1libVjRQTyIJhZ7PRPfzcww"
            worksheet_name = "一覧"
            
            print(f"Google Sheetsにデータを書き込み中... (スプレッドシートID: {spreadsheet_id}, ワークシート: {worksheet_name})")
            gs_client.write_data(spreadsheet_id, worksheet_name, results['headers'], results['data'])
        except Exception as e:
            print(f"Google Sheetsへの書き込み中にエラーが発生しました: {e}")
    elif results:
        print("\n条件に合致する案件は見つかりませんでした。")