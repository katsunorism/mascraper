import time
import re
import csv
import configparser
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def load_config():
    """設定ファイル(config.ini)を読み込む"""
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8-sig')
    return config

def _extract_single_value(text):
    """単一の金額文字列から数値を抽出する"""
    if not text: 
        return 0
    
    original_text = text  # デバッグ用に元のテキストを保持
    text = text.replace(",", "").replace("，", "")  # 全角カンマも除去
    
    # より柔軟な数値抽出（小数点も含む）
    match = re.search(r'([\d\.]+)', text)
    if not match: 
        print(f"        [DEBUG] 数値が見つかりません: '{original_text}' → 処理後: '{text}'")
        return 0
    
    value = float(match.group(1))
    original_value = value
    
    # より詳細な単位判定
    if "億" in text: 
        value *= 100_000_000
        print(f"        [DEBUG] '{original_text}' → {original_value}億円 → {value:,}円")
    elif "万" in text: 
        value *= 10_000
        print(f"        [DEBUG] '{original_text}' → {original_value}万円 → {value:,}円")
    elif "千" in text:  # 「千万円」対応を追加
        value *= 1_000
        print(f"        [DEBUG] '{original_text}' → {original_value}千円 → {value:,}円")
    else:
        print(f"        [DEBUG] '{original_text}' → {value:,}円（単位なし）")
    
    return int(value)

def parse_financial_value(text):
    """「3億円」「5,000万円」「2億円～3億円」を数値(円)に変換する"""
    if not text: 
        return (0, 0)
    
    print(f"    [DEBUG] 元テキスト: '{text}'")
    
    # 様々な区切り文字に対応（～、〜、?、-など）
    separators = ["～", "〜", "?", "？", "-", "ー", "–", "—"]
    is_range = False
    parts = [text]
    
    for sep in separators:
        if sep in text:
            parts = text.split(sep)
            is_range = True
            print(f"    [DEBUG] 区切り文字'{sep}'で分割: {parts}")
            break
    
    if is_range and len(parts) >= 2:
        min_val = _extract_single_value(parts[0].strip())
        max_val = _extract_single_value(parts[1].strip())
        print(f"    [DEBUG] 範囲結果: {min_val:,}円 ～ {max_val:,}円")
        return (min_val, max_val)
    else:
        # 単一値の場合
        single_val = _extract_single_value(text)
        print(f"    [DEBUG] 単一値結果: {single_val:,}円")
        return (single_val, single_val)

def meets_condition(value_range, threshold):
    """範囲が条件を満たすかチェック（範囲の最大値が閾値以上であればOK）"""
    if isinstance(value_range, tuple):
        min_val, max_val = value_range
        # 範囲の最大値が閾値以上であればOK
        result = max_val >= threshold
        print(f"        [DEBUG] 判定: {min_val:,}円～{max_val:,}円 vs {threshold:,}円")
        print(f"        [DEBUG] 最大値{max_val:,}円 >= 条件{threshold:,}円 → {result}")
        return result
    else:
        # 後方互換性のため
        return value_range >= threshold

def find_case_value(deal_soup, label):
    """案件情報の中から、複数パターンのHTML構造に対応して値を取得する"""
    # パターン1: <dt>ラベル</dt><dd>値</dd>
    dt_tag = deal_soup.find("dt", string=label)
    if dt_tag:
        dd_tag = dt_tag.find_next_sibling("dd")
        if dd_tag: return dd_tag.get_text(strip=True)
            
    # パターン2: <span><span class="v-chip__content">ラベル</span></span><span class="btz-body-m-bold">値</span>
    chip_span = deal_soup.find("span", class_="v-chip__content", string=re.compile(label))
    if chip_span:
        parent_chip = chip_span.find_parent("span", class_="v-chip")
        if parent_chip:
            value_span = parent_chip.find_next_sibling("span", class_="btz-body-m-bold")
            if value_span: return value_span.get_text(strip=True)

    return None

def main():
    """メインの実行関数"""
    config = load_config()
    creds = config['BatonzCredentials']
    conds = config['ScrapingConditions']
    output = config['Output']

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    all_found_deals = []

    try:
        driver.get("https://batonz.jp/user/sell_cases")
        
        print("\n" + "="*60)
        print("ブラウザが起動しました。手動でログインを完了させてください。")
        print(f"Email: {creds['Email']}")
        print("ログイン後、案件一覧ページが表示されたら、この黒い画面に戻って")
        input("Enterキーを押してください...")
        print("="*60 + "\n")
        print("--- ログイン後の処理を再開します ---")

        wait = WebDriverWait(driver, 20)
        
        for page_num in range(1, int(conds['MaxPages']) + 1):
            if page_num > 1:
                target_url = f"https://batonz.jp/user/sell_cases?page={page_num}"
                print(f"\n--- ページ {page_num} の調査を開始 ---")
                driver.get(target_url)
            else:
                print(f"\n--- ページ {page_num} の調査を開始 ---")
            
            try:
                # どちらかのパターンの案件が表示されるまで待つ
                wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "article.p-sellCase--item, a[href*='/sell_cases/']")
                ))
                print("  案件リストの表示を確認しました。")
            except Exception as e:
                print(f"  このページに案件が見つかりませんでした。")
                continue

            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # ★★★★★★★ 2種類の「箱」を両方とも探し出す ★★★★★★★
            all_deals_on_page = soup.select("article.p-sellCase--item, a[href*='/sell_cases/']")

            for deal in all_deals_on_page:
                title_tag = deal.find("h3", class_="p-sellCase--title") or deal.find("div", {{"data-testid": "sell-case-card-sell-case-title"}})
                
                if not title_tag: continue
                
                link_tag = title_tag.find("a") if title_tag.find("a") else deal
                
                title = title_tag.get_text(strip=True)
                link = link_tag.get("href")
                
                revenue_str = find_case_value(deal, "売上高")
                profit_str = find_case_value(deal, "事業の利益") or find_case_value(deal, "営業利益")

                revenue_range = parse_financial_value(revenue_str)
                profit_range = parse_financial_value(profit_str)

                # 条件判定：範囲内に閾値が含まれるかチェック
                min_revenue = int(conds['MinRevenue'])  # 3億円 = 300,000,000
                min_profit = int(conds['MinProfit'])    # 3千万円 = 30,000,000
                
                print(f"    [DEBUG] 設定条件: 売上高≥{min_revenue:,}円, 利益≥{min_profit:,}円")

                # デバッグ用の詳細出力（判定前に全て表示）
                revenue_display = f"{revenue_str} (範囲: {revenue_range[0]:,}円～{revenue_range[1]:,}円)" if isinstance(revenue_range, tuple) else f"{revenue_str} ({revenue_range:,}円)"
                profit_display = f"{profit_str} (範囲: {profit_range[0]:,}円～{profit_range[1]:,}円)" if isinstance(profit_range, tuple) else f"{profit_str} ({profit_range:,}円)"
                
                print(f"    案件: {title}")
                print(f"         売上高: {revenue_display}")
                revenue_ok = meets_condition(revenue_range, min_revenue)
                print(f"         → {'OK' if revenue_ok else 'NG'}（条件：{min_revenue:,}円以上）")
                
                print(f"         利益: {profit_display}")
                profit_ok = meets_condition(profit_range, min_profit)
                print(f"         → {'OK' if profit_ok else 'NG'}（条件：{min_profit:,}円以上）")
                
                if revenue_ok and profit_ok:
                    print(f"    [合格] 両方の条件を満たしています")
                    all_found_deals.append([
                        title,
                        revenue_str or "情報なし",
                        profit_str or "情報なし",
                        link
                    ])
                else:
                    print(f"    [除外] 条件を満たしていません（売上高:{revenue_ok}, 利益:{profit_ok}）")
    
    finally:
        print("\n--- 処理が完了しました。ブラウザを閉じます ---")
        driver.quit()

    print(f"\n--- {len(all_found_deals)}件の案件を抽出しました ---")
    headers = ['タイトル', 'リンク', '売上高', '事業の利益']
    formatted_deals = []
    for deal in all_found_deals:
        full_link = "https://batonz.jp" + deal[3] if deal[3].startswith('/') else deal[3]
        formatted_deals.append([deal[0], full_link, deal[1], deal[2]])

    return {"headers": headers, "data": formatted_deals}

if __name__ == "__main__":
    results = main()
    if results:
        print("\n" + "="*50)
        print(f"調査完了！ 条件に合致する {len(results['data'])} 件の案件が見つかりました。")
        print("="*50)
        # Print first 3 results as a sample
        for deal in results['data'][:3]:
            print(f"【タイトル】{deal[0]}\n【リンク】{deal[1]}\n【売上高】{deal[2]}\n【事業の利益】{deal[3]}\n" + "-"*20)
