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
    """M&Aサクシードに自動ログインを試行する"""
    try:
        print("--- 自動ログインを試行します ---")
        driver.get("https://cs.ma-succeed.jp/login")
        
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
            lambda: "dashboard" in driver.current_url or "mypage" in driver.current_url
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
            
            # マイページから案件検索ページへの自動遷移
            print("マイページから案件検索ページへ自動遷移します...")
            time.sleep(2)  # ページの安定化を待つ
            
            # 直接検索ページのURLに遷移
            search_url = "https://cs.ma-succeed.jp/search?projectStatusCds=PUB&projectStatusCds=AST&projectStatusCds=NEG&orderByCd=LAT"
            driver.get(search_url)
            
            # 検索ページが正常に読み込まれたか確認
            try:
                wait = WebDriverWait(driver, 10)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.scd-card.buy-project-card")))
                print("✓ 案件検索ページへの遷移が完了しました！")
                return True
            except TimeoutException:
                print("案件検索ページの読み込みに時間がかかっています...")
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
    print("ログイン後、マイページなどが表示されたら、この黒い画面に戻って")
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
    match = re.search(r'(-?[\d\.]+)', text)
    if not match:
        # print(f"        [DEBUG] 数値が見つかりません: '{original_text}'") # デバッグ時以外はコメントアウト
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
    
    # print(f"    [DEBUG] 元テキスト: '{text}'") # デバッグ時以外はコメントアウト

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
        # print(f"    [DEBUG] 範囲結果: {min_val:,}円 ～ {max_val:,}円") # デバッグ時以外はコメントアウト
        return (min_val, max_val)
    else:
        single_val = _extract_single_value(text)
        # print(f"    [DEBUG] 単一値結果: {single_val:,}円") # デバッグ時以外はコメントアウト
        return (single_val, single_val)

def meets_condition(value_range, threshold):
    """範囲が条件を満たすかチェック（範囲の最大値が閾値以上であればOK）"""
    min_val, max_val = value_range
    
    if max_val < 0 and threshold > 0:
        result = False
    else:
        result = max_val >= threshold
    
    # print(f"        [DEBUG] 最大値{max_val:,}円 >= 条件{threshold:,}円 → {result}") # デバッグ時以外はコメントアウト
    return result

def find_case_value_succeed(deal_soup, label_text):
    """サクシードの案件カードから指定されたラベルの値を取得する"""
    def_list = deal_soup.select_one("ul.buy-project-card-def-list")
    if not def_list:
        return None
    
    for item in def_list.select("li"):
        label_div = item.select_one("div.label")
        if label_div and label_text in label_div.get_text(strip=True):
            value_span = item.select_one("span.scd-typography")
            if value_span:
                return value_span.get_text(strip=True)
    return None

def main():
    """メインの実行関数"""
    config = load_config()
    creds = config['SucceedCredentials']
    conds = config['ScrapingConditions']
    output = config['SucceedOutput']

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    all_found_deals = []
    processed_links = set()

    try:
        # 自動ログインを試行
        auto_login_success = auto_login(driver, creds['Email'], creds['Password'])
        
        if auto_login_success:
            # 自動ログイン成功の場合、既に案件検索ページに遷移済み
            print("自動ログイン・遷移が完了しました。案件の読み込みを開始します。")
        else:
            # 自動ログインに失敗した場合は手動ログインにフォールバック
            manual_login_fallback(driver, creds['Email'])
            
            # 手動ログイン後に案件検索ページに遷移
            SEARCH_URL = "https://cs.ma-succeed.jp/search?projectStatusCds=PUB&projectStatusCds=AST&projectStatusCds=NEG&orderByCd=LAT"
            print(f"案件一覧ページに移動します: {SEARCH_URL}")
            driver.get(SEARCH_URL)

        wait = WebDriverWait(driver, 20)
        
        # 案件カードの読み込みを待つ（自動ログイン時は既に遷移済みの可能性があるため）
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.scd-card.buy-project-card")))
        except TimeoutException:
            print("案件カードの読み込みを再試行します...")
            # 案件検索ページに再度遷移
            SEARCH_URL = "https://cs.ma-succeed.jp/search?projectStatusCds=PUB&projectStatusCds=AST&projectStatusCds=NEG&orderByCd=LAT"
            driver.get(SEARCH_URL)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.scd-card.buy-project-card")))

        # ★★★ 修正点: 最後の案件要素にスクロールする、より確実なロジック ★★★
        TARGET_DEAL_COUNT = 100
        print(f"--- 新着{TARGET_DEAL_COUNT}件以上の案件が読み込まれるまでスクロールします ---")
        
        while True:
            # Seleniumで現在の案件要素をすべて取得
            deal_elements = driver.find_elements(By.CSS_SELECTOR, "a.scd-card.buy-project-card")
            num_deals = len(deal_elements)

            print(f"  現在 {num_deals} 件の案件を読み込み済み...")

            if num_deals >= TARGET_DEAL_COUNT:
                print(f"  目標の{TARGET_DEAL_COUNT}件以上の案件を読み込みました。スクロールを終了します。")
                break

            if not deal_elements:
                print("  案件が見つかりません。処理を終了します。")
                break

            # 最後の案件要素までスクロール（これが無限スクロールのトリガーとなる）
            print("  リストの末尾へスクロールします...")
            last_deal_element = deal_elements[-1]
            driver.execute_script("arguments[0].scrollIntoView();", last_deal_element)
            time.sleep(5) # 新しい案件が読み込まれるのを待つ

            # スクロール後に案件数が変わったかチェック
            new_deal_elements = driver.find_elements(By.CSS_SELECTOR, "a.scd-card.buy-project-card")
            if len(new_deal_elements) == num_deals:
                print("  スクロールしても新しい案件が読み込まれませんでした。ページの最下部に到達したと判断します。")
                break
        
        print("\n--- 全案件の読み込み完了。条件の合う案件を抽出します ---")

        soup = BeautifulSoup(driver.page_source, "html.parser")
        all_deals_on_page = soup.select("a.scd-card.buy-project-card")
        print(f"ページ上から{len(all_deals_on_page)}件の案件が見つかりました。1件ずつ条件を確認します。")

        for deal in all_deals_on_page:
            project_id = deal.get("data-cy-project-id")
            if not project_id:
                continue
            
            link = f"/project/{project_id}"

            if link in processed_links:
                continue
            processed_links.add(link)
            
            full_link = "https://cs.ma-succeed.jp" + link

            title_tag = deal.select_one("div.scd-typography-ellipsis-multi-row")
            title = title_tag.get_text(strip=True) if title_tag else "タイトル不明"

            revenue_str = find_case_value_succeed(deal, "売上高")
            profit_str = find_case_value_succeed(deal, "営業利益") or find_case_value_succeed(deal, "事業利益")

            revenue_range = parse_financial_value(revenue_str)
            profit_range = parse_financial_value(profit_str)

            min_revenue = int(conds['MinRevenue'])
            min_profit = int(conds['MinProfit'])
            
            print(f"\n    案件: {title}")
            print(f"         売上高: {revenue_str or '情報なし'}")
            revenue_ok = meets_condition(revenue_range, min_revenue)
            
            print(f"         利益: {profit_str or '情報なし'}")
            profit_ok = meets_condition(profit_range, min_profit)

            if revenue_ok and profit_ok:
                print(f"    [合格] 両方の条件を満たしています")
                all_found_deals.append([
                    title,
                    full_link,
                    revenue_str or "情報なし",
                    profit_str or "情報なし"
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
            writer.writerow(['タイトル', 'リンク', '売上高', '利益'])
            writer.writerows(all_found_deals)
        print("--- 保存完了 ---")
    except Exception as e:
        print(f"!!! ファイルの保存中にエラーが発生しました: {e} !!!")

    print("\n" + "="*50)
    print(f"調査完了！ 条件に合致する {len(all_found_deals)} 件の案件が見つかりました。")
    print("="*50)
    for deal in all_found_deals:
        print(f"【タイトル】{deal[0]}\n【売上高】{deal[2]}\n【利益】{deal[3]}\n【リンク】{deal[1]}\n" + "-"*20)

if __name__ == "__main__":
    main()
