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
from selenium.common.exceptions import TimeoutException, NoSuchElementException

def load_config():
    """設定ファイル(config.ini)を読み込む"""
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8-sig')
    return config

def auto_login(driver, email, password):
    """M&Aクラウドに自動ログインを試行する"""
    try:
        print("--- 自動ログインを試行します ---")
        driver.get("https://macloud.jp/business/login")
        
        wait = WebDriverWait(driver, 10)
        
        # メールアドレス入力フィールドを探して入力
        email_selectors = [
            'input[type="email"]',
            'input[name="email"]',
            'input[id="email"]',
            'input[placeholder*="メール"]',
            'input[placeholder*="mail"]',
            'input[placeholder*="Email"]'
        ]
        
        email_field = None
        for selector in email_selectors:
            try:
                email_field = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                break
            except TimeoutException:
                continue
        
        if not email_field:
            print("メールアドレス入力フィールドが見つかりませんでした")
            return False
        
        print("メールアドレスを入力中...")
        email_field.clear()
        email_field.send_keys(email)
        
        # パスワード入力フィールドを探して入力
        password_selectors = [
            'input[type="password"]',
            'input[name="password"]',
            'input[id="password"]'
        ]
        
        password_field = None
        for selector in password_selectors:
            try:
                password_field = driver.find_element(By.CSS_SELECTOR, selector)
                break
            except NoSuchElementException:
                continue
        
        if not password_field:
            print("パスワード入力フィールドが見つかりませんでした")
            return False
        
        print("パスワードを入力中...")
        password_field.clear()
        password_field.send_keys(password)
        
        # ログインボタンを探してクリック
        login_button_selectors = [
            'button[type="submit"]',
            'input[type="submit"]',
            'button:contains("ログイン")',
            'input[value*="ログイン"]',
            '.login-button',
            '.btn-login'
        ]
        
        login_button = None
        for selector in login_button_selectors:
            try:
                if ':contains(' in selector:
                    # XPathを使用
                    xpath = f"//button[contains(text(), 'ログイン')] | //input[contains(@value, 'ログイン')]"
                    login_button = driver.find_element(By.XPATH, xpath)
                else:
                    login_button = driver.find_element(By.CSS_SELECTOR, selector)
                break
            except NoSuchElementException:
                continue
        
        if not login_button:
            print("ログインボタンが見つかりませんでした")
            return False
        
        print("ログインボタンをクリック中...")
        login_button.click()
        
        # ログイン成功を確認（URLの変化や特定要素の存在をチェック）
        time.sleep(3)
        
        # ログイン後のページ判定
        success_indicators = [
            lambda: "login" not in driver.current_url,
            lambda: driver.find_elements(By.CSS_SELECTOR, ".user-menu"),
            lambda: driver.find_elements(By.CSS_SELECTOR, ".logout"),
            lambda: driver.find_elements(By.CSS_SELECTOR, "[data-cy*='user']"),
            lambda: "business" in driver.current_url
        ]
        
        login_success = False
        for indicator in success_indicators:
            try:
                if indicator():
                    login_success = True
                    break
            except:
                continue
        
        if login_success:
            print("✓ 自動ログインに成功しました！")
            
            # 案件一覧ページへの自動遷移
            print("案件一覧ページへ自動遷移します...")
            time.sleep(2)  # ページの安定化を待つ
            
            # 直接案件一覧ページのURLに遷移
            search_url = "https://macloud.jp/business/selling_targets?per_page=100&order=recommended"
            driver.get(search_url)
            
            # 案件一覧ページが正常に読み込まれたか確認
            try:
                wait = WebDriverWait(driver, 10)
                # 案件カードの読み込みを待つ（複数のセレクターで試行）
                card_selectors = [
                    "[class*='card']",
                    "[class*='item']", 
                    "[class*='project']",
                    "[class*='deal']",
                    "div[id*='3291']",  # IDから推測される案件要素
                    "a[href*='selling_targets']"
                ]
                
                cards_found = False
                for selector in card_selectors:
                    try:
                        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                        print(f"✓ 案件一覧ページへの遷移が完了しました！(セレクター: {selector})")
                        cards_found = True
                        break
                    except TimeoutException:
                        continue
                
                if not cards_found:
                    print("案件一覧ページの読み込みに時間がかかっています...")
                    return True  # エラーでも続行
                else:
                    return True
                    
            except TimeoutException:
                print("案件一覧ページの読み込みに時間がかかっています...")
                return True  # エラーでも続行
                
        else:
            print("自動ログインが失敗した可能性があります")
            return False
            
    except Exception as e:
        print(f"自動ログイン中にエラーが発生しました: {e}")
        return False

def manual_login_fallback(driver, email):
    """自動ログインに失敗した場合の手動ログイン"""
    print("\n" + "="*60)
    print("自動ログインに失敗しました。手動でログインしてください。")
    print(f"Email: {email}")
    print("ログイン後、案件一覧ページなどが表示されたら、この黒い画面に戻って")
    input("Enterキーを押してください...")
    print("="*60 + "\n")
    print("--- ログイン後の処理を再開します ---")

def _extract_single_value(text):
    """単一の金額文字列から数値を抽出する (「百万円」「▲」表記に対応)"""
    if not text:
        return 0

    original_text = text
    text = text.replace(",", "").replace("，", "")
    text = text.replace("▲", "-") # マイナス記号に置換

    # 数値（マイナス、小数点含む）を抽出
    match = re.search(r'(-?[\\d\.]+)', text)
    if not match:
        return 0

    value = float(match.group(1))

    # 単位の判定
    if "億" in text:
        value *= 100_000_000
    elif "百万円" in text:
        value *= 1_000_000
    elif "万" in text:
        value *= 10_000
    elif "千" in text:
        value *= 1_000

    return int(value)

def parse_financial_value(text):
    """「1億円～2億5,000万円」「500万円未満」「▲1,000万円〜」等を数値(円)の範囲に変換する"""
    if not text or "応相談" in text:
        return (0, 0)

    separators = ["～", "〜", "~", "ー"]
    
    if "未満" in text or "以下" in text:
        val = _extract_single_value(text)
        return (0, val)
    
    if "以上" in text or text.strip().endswith("〜"):
        val = _extract_single_value(text)
        if val < 0:
            return (float('-inf'), val)
        else:
            return (val, float('inf'))

    parts = [text]
    for sep in separators:
        if sep in text:
            parts = text.split(sep)
            break
    
    if len(parts) >= 2:
        min_val = _extract_single_value(parts[0].strip())
        max_val = _extract_single_value(parts[1].strip())
        return (min_val, max_val)
    else:
        single_val = _extract_single_value(text)
        return (single_val, single_val)

def meets_condition(value_range, threshold):
    """範囲が条件を満たすかチェック（範囲の最大値が閾値以上であればOK）"""
    min_val, max_val = value_range
    
    if max_val < 0 and threshold > 0:
        result = False
    else:
        result = max_val >= threshold
    
    return result

def extract_deal_info(deal_soup):
    """M&Aクラウドの案件カードから情報を抽出する（修正版）"""
    # 複数のセレクターで試行
    title_selectors = [
        "h2", "h3", "h4",
        "[class*='title']", 
        "[class*='heading']",
    ]
    
    title = None
    for selector in title_selectors:
        element = deal_soup.select_one(selector)
        if element and element.get_text(strip=True):
            title = element.get_text(strip=True)
            break
    
    # タイトルが見つからない場合、長いテキストを探す
    if not title:
        elements = deal_soup.find_all(text=re.compile(r'.{20,}'))  # 20文字以上のテキスト
        for element in elements:
            text = element.strip()
            if text and len(text) > 20 and '前期' not in text and 'ID' not in text:
                title = text
                break
    
    # 案件IDを抽出（複数の方法で試行）
    project_id = None
    
    # 方法1: ID: XXXX の形式を探す
    id_text = deal_soup.get_text()
    id_match = re.search(r'ID\s*[:：]\s*(\d+)', id_text)
    if id_match:
        project_id = id_match.group(1)
    
    # 方法2: data属性から探す
    if not project_id:
        for element in deal_soup.find_all():
            for attr_name, attr_value in element.attrs.items():
                if 'id' in attr_name.lower() and str(attr_value).isdigit():
                    project_id = str(attr_value)
                    break
    
    # 方法3: URLから推測
    if not project_id:
        links = deal_soup.find_all('a', href=True)
        for link in links:
            href = link['href']
            id_match = re.search(r'/(\d+)', href)
            if id_match:
                project_id = id_match.group(1)
                break
    
    # 前期売上と前期営業損益を抽出（修正版）
    text_content = deal_soup.get_text()
    print(f"    [DEBUG] === 案件の全テキスト内容 ===")
    print(f"    [DEBUG] {text_content[:500]}...")  # 最初の500文字のみ表示
    print(f"    [DEBUG] === テキスト内容終了 ===")
    
    revenue = None
    profit = None
    
    # 1. まず、表形式のデータを探す（最も確実な方法）
    table_pattern = r'前期売上\s*([^\s]+)\s*前期営業損益\s*([^\s]+)'
    table_match = re.search(table_pattern, text_content)
    
    if table_match:
        revenue_candidate = table_match.group(1).strip()
        profit_candidate = table_match.group(2).strip()
        
        # 金額パターンに一致するかチェック
        amount_pattern = r'[-▲]?\d+[,，]?\d*(?:億|万|千)?円'
        if re.match(amount_pattern, revenue_candidate):
            revenue = revenue_candidate
            print(f"    [DEBUG] ★★★ 表形式から前期売上を特定: {revenue}")
        if re.match(amount_pattern, profit_candidate):
            profit = profit_candidate
            print(f"    [DEBUG] ★★★ 表形式から前期営業損益を特定: {profit}")
    
    # 2. 表形式で見つからない場合、より詳細な検索
    if not revenue or not profit:
        # より包括的な金額パターン
        amount_patterns = [
            r'[-▲]?\d+\.?\d*億円',      # 6.4億円
            r'[-▲]?\d+[,，]?\d+万円',   # 4,000万円
            r'[-▲]?\d+万円',            # 4000万円
            r'[-▲]?\d+千円',            # 1000千円
            r'[-▲]?\d+[,，]?\d+円',     # 4,000円
            r'[-▲]?\d+円'               # 基本の円
        ]
        
        # 財務項目を含むセクションを特定
        lines = text_content.split('\n')
        financial_section_found = False
        
        for i, line in enumerate(lines):
            line_clean = line.strip()
            
            # 財務項目のヘッダー行を探す
            if ('前期売上' in line_clean and '前期営業損益' in line_clean):
                print(f"    [DEBUG] ★★★ 財務ヘッダー行発見: {line_clean}")
                financial_section_found = True
                
                # ヘッダー行内の項目位置を取得
                revenue_header_pos = line_clean.find('前期売上')
                profit_header_pos = line_clean.find('前期営業損益')
                
                # データ行を探す（ヘッダーの後の数行）
                for j in range(i + 1, min(len(lines), i + 5)):
                    data_line = lines[j].strip()
                    if not data_line:
                        continue
                    
                    # データ行から金額を抽出
                    amounts_found = []
                    for pattern in amount_patterns:
                        matches = list(re.finditer(pattern, data_line))
                        for match in matches:
                            amount = match.group()
                            position = match.start()
                            amounts_found.append((amount, position))
                    
                    if len(amounts_found) >= 2:
                        # 位置でソート
                        amounts_found.sort(key=lambda x: x[1])
                        print(f"    [DEBUG] データ行で発見された金額: {amounts_found}")
                        
                        # ヘッダーの位置関係に基づいて割り当て
                        if revenue_header_pos < profit_header_pos:
                            if not revenue:
                                revenue = amounts_found[0][0]
                                print(f"    [DEBUG] ★★★ 位置関係から前期売上: {revenue}")
                            if not profit and len(amounts_found) > 1:
                                profit = amounts_found[1][0]
                                print(f"    [DEBUG] ★★★ 位置関係から前期営業損益: {profit}")
                        else:
                            if not profit:
                                profit = amounts_found[0][0]
                                print(f"    [DEBUG] ★★★ 位置関係から前期営業損益: {profit}")
                            if not revenue and len(amounts_found) > 1:
                                revenue = amounts_found[1][0]
                                print(f"    [DEBUG] ★★★ 位置関係から前期売上: {revenue}")
                        break
                break
    
    # 3. まだ見つからない場合、個別に検索（重複を避ける）
    if not revenue or not profit:
        print(f"    [DEBUG] === 個別検索モード ===")
        
        # 使用済みの金額を追跡
        used_amounts = set()
        
        # 前期売上を探す
        if not revenue:
            revenue_patterns = [
                r'前期売上\s*[:：]?\s*([-▲]?\d+[,，\.\d]*(?:億|万|千)?円)',
                r'前期売上\s+([-▲]?\d+[,，\.\d]*(?:億|万|千)?円)',
                r'売上\s*[:：]?\s*([-▲]?\d+[,，\.\d]*(?:億|万|千)?円)',
            ]
            
            for pattern in revenue_patterns:
                match = re.search(pattern, text_content)
                if match:
                    revenue_candidate = match.group(1)
                    # 前期営業損益の文字列が含まれていないかチェック
                    context_start = max(0, match.start() - 50)
                    context_end = min(len(text_content), match.end() + 50)
                    context = text_content[context_start:context_end]
                    
                    if '前期営業損益' not in context and revenue_candidate not in used_amounts:
                        revenue = revenue_candidate
                        used_amounts.add(revenue_candidate)
                        print(f"    [DEBUG] ★★★ 個別検索で前期売上: {revenue}")
                        print(f"    [DEBUG] コンテキスト: {context}")
                        break
        
        # 前期営業損益を探す
        if not profit:
            profit_patterns = [
                r'前期営業損益\s*[:：]?\s*([-▲]?\d+[,，\.\d]*(?:億|万|千)?円)',
                r'前期営業損益\s+([-▲]?\d+[,，\.\d]*(?:億|万|千)?円)',
                r'営業損益\s*[:：]?\s*([-▲]?\d+[,，\.\d]*(?:億|万|千)?円)',
                r'営業利益\s*[:：]?\s*([-▲]?\d+[,，\.\d]*(?:億|万|千)?円)',
            ]
            
            for pattern in profit_patterns:
                match = re.search(pattern, text_content)
                if match:
                    profit_candidate = match.group(1)
                    # 前期売上の文字列が含まれていないかチェック
                    context_start = max(0, match.start() - 50)
                    context_end = min(len(text_content), match.end() + 50)
                    context = text_content[context_start:context_end]
                    
                    if '前期売上' not in context and profit_candidate not in used_amounts:
                        profit = profit_candidate
                        used_amounts.add(profit_candidate)
                        print(f"    [DEBUG] ★★★ 個別検索で前期営業損益: {profit}")
                        print(f"    [DEBUG] コンテキスト: {context}")
                        break
    
    # 4. 最終フォールバック: 金額を順番に割り当て（ただし異なる値のみ）
    if not revenue or not profit:
        print(f"    [DEBUG] === 最終フォールバック ===")
        
        all_amounts = []
        amount_pattern = r'[-▲]?\d+[,，\.\d]*(?:億|万|千)?円'
        
        for match in re.finditer(amount_pattern, text_content):
            amount = match.group()
            # 既に見つけた金額と異なるもののみ追加
            if (not revenue or amount != revenue) and (not profit or amount != profit):
                all_amounts.append(amount)
        
        # 重複を除去
        unique_amounts = []
        for amount in all_amounts:
            if amount not in unique_amounts:
                unique_amounts.append(amount)
        
        print(f"    [DEBUG] ユニークな金額リスト: {unique_amounts[:5]}...")  # 最初の5個のみ表示
        
        if not revenue and len(unique_amounts) >= 1:
            revenue = unique_amounts[0]
            print(f"    [DEBUG] フォールバックで前期売上: {revenue}")
            
        if not profit and len(unique_amounts) >= 2:
            profit = unique_amounts[1]
            print(f"    [DEBUG] フォールバックで前期営業損益: {profit}")
    
    # 詳細ページへのリンクを探す
    detail_link = None
    links = deal_soup.find_all('a', href=True)
    for link in links:
        link_text = link.get_text().strip()
        href = link['href']
        if '詳しく見る' in link_text or 'detail' in href or 'selling_targets' in href:
            detail_link = href
            if not detail_link.startswith('http'):
                detail_link = 'https://macloud.jp' + detail_link
            break
    
    print(f"    [DEBUG] 最終抽出結果 - タイトル: {title}, ID: {project_id}, 売上: {revenue}, 営業損益: {profit}")
    
    # 最終確認: 売上と営業損益が同じ値でないかチェック
    if revenue and profit and revenue == profit:
        print(f"    [WARNING] 前期売上と前期営業損益が同じ値です: {revenue}")
        # この場合、営業損益をクリア（売上を優先）
        profit = None
        print(f"    [WARNING] 前期営業損益をクリアしました")
    
    return {
        'title': title or 'タイトル不明',
        'project_id': project_id or '不明',
        'revenue': revenue,
        'profit': profit,
        'link': detail_link
    }

def main():
    """メインの実行関数"""
    config = load_config()
    creds = config['MACloudCredentials']
    conds = config['ScrapingConditions']
    output = config['MACloudOutput']

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    all_found_deals = []
    processed_ids = set()

    try:
        # 自動ログインを試行
        auto_login_success = auto_login(driver, creds['Email'], creds['Password'])
        
        if auto_login_success:
            # 自動ログイン成功の場合、既に案件一覧ページに遷移済み
            print("自動ログイン・遷移が完了しました。案件の読み込みを開始します。")
        else:
            # 自動ログインに失敗した場合は手動ログインにフォールバック
            manual_login_fallback(driver, creds['Email'])
            
            # 手動ログイン後に案件一覧ページに遷移
            SEARCH_URL = "https://macloud.jp/business/selling_targets?per_page=100&order=recommended"
            print(f"案件一覧ページに移動します: {SEARCH_URL}")
            driver.get(SEARCH_URL)

        wait = WebDriverWait(driver, 20)
        
        # ページの読み込みを待つ
        time.sleep(5)
        
        print("\n--- 案件情報の抽出を開始します ---")

        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # 案件カードの候補となる要素を探す（複数のセレクターで試行）
        possible_selectors = [
            "div[class*='card']",
            "div[class*='item']",
            "div[class*='project']",
            "div[class*='deal']",
            "article",
            "section"
        ]
        
        all_deals = []
        for selector in possible_selectors:
            elements = soup.select(selector)
            if elements:
                print(f"セレクター '{selector}' で {len(elements)} 個の要素が見つかりました")
                
                # 各要素をチェックして案件情報が含まれているかを確認
                for element in elements:
                    text_content = element.get_text()
                    if ('万円' in text_content and len(text_content) > 100) or 'ID' in text_content:
                        all_deals.append(element)
                
                if all_deals:
                    print(f"案件情報を含む要素を {len(all_deals)} 個発見しました")
                    break
        
        if not all_deals:
            print("案件カードが見つかりませんでした。ページ全体から情報を抽出します...")
            # フォールバック: ページ全体を1つの案件として処理
            page_info = extract_deal_info(soup)
            if page_info['revenue'] or page_info['profit']:
                all_deals = [soup]

        print(f"処理対象の案件数: {len(all_deals)}")

        for deal in all_deals:
            deal_info = extract_deal_info(deal)
            
            project_id = deal_info['project_id']
            if project_id in processed_ids:
                continue
            processed_ids.add(project_id)
            
            title = deal_info['title']
            revenue_str = deal_info['revenue']
            profit_str = deal_info['profit']
            full_link = deal_info['link'] or "リンク不明"

            revenue_range = parse_financial_value(revenue_str)
            profit_range = parse_financial_value(profit_str)

            min_revenue = int(conds['MinRevenue'])
            min_profit = int(conds['MinProfit'])
            
            print(f"\n    案件ID: {project_id}")
            print(f"    タイトル: {title}")
            print(f"         前期売上: {revenue_str or '情報なし'}")
            revenue_ok = meets_condition(revenue_range, min_revenue)
            
            print(f"         前期営業損益: {profit_str or '情報なし'}")
            profit_ok = meets_condition(profit_range, min_profit)

            if revenue_ok and profit_ok:
                print(f"    [合格] 両方の条件を満たしています")
                all_found_deals.append([
                    title,
                    project_id,
                    revenue_str or "情報なし",
                    profit_str or "情報なし",
                    full_link
                ])
            else:
                print(f"    [除外] 条件を満たしていません")

    finally:
        print("\n--- 処理が完了しました。ブラウザを閉じます ---")
        driver.quit()

    output_filename = output['FileName']
    print(f"\n--- {len(all_found_deals)}件の案件を「{output_filename}」に保存します ---")
    try:
        with open(output_filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['タイトル', '案件ID', '前期売上', '前期営業損益', 'リンク'])
            writer.writerows(all_found_deals)
        print("--- 保存完了 ---")
    except Exception as e:
        print(f"!!! ファイルの保存中にエラーが発生しました: {e} !!!")

if __name__ == "__main__":
    main()
