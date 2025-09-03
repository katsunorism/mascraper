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
from selenium.webdriver.common.keys import Keys

def load_config():
    """設定ファイル(config.ini)を読み込む"""
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8-sig')
    return config

def handle_terms_agreement(driver, wait):
    """利用規約同意処理を行う（修正版）"""
    try:
        print("--- 利用規約同意処理を開始します ---")
        
        # まず利用規約リンクを探す
        terms_link = None
        try:
            # label要素を直接探す（HTML構造から）
            terms_link = driver.find_element(By.CSS_SELECTOR, 'label[for="viwer_popup"]')
            print("利用規約リンクを見つけました")
        except NoSuchElementException:
            # フォールバック: テキストで探す
            try:
                terms_link = driver.find_element(By.XPATH, "//a[contains(text(), '利用規約')]" )
                print("利用規約リンクを見つけました（フォールバック）")
            except NoSuchElementException:
                print("利用規約リンクが見つかりません")
                return False
        
        if terms_link:
            print("利用規約リンクをクリックします...")
            driver.execute_script("arguments[0].click();", terms_link)
            time.sleep(3)
            
            # モーダルが表示されるまで待機
            try:
                popup = wait.until(EC.presence_of_element_located((By.ID, "popup")))
                print("利用規約ポップアップが表示されました")
                
                # ポップアップ内のスクロール可能要素を探す
                scrollable_element = None
                try:
                    # プレビュー要素を探す
                    scrollable_element = popup.find_element(By.ID, "preview")
                    print("スクロール可能要素（preview）を見つけました")
                except NoSuchElementException:
                    # フォールバック: 他のスクロール可能要素
                    try:
                        scrollable_element = popup.find_element(By.CSS_SELECTOR, ".preview")
                        print("スクロール可能要素（.preview）を見つけました")
                    except NoSuchElementException:
                        scrollable_element = popup
                        print("ポップアップ自体をスクロール対象とします")
                
                # スクロールを実行
                if scrollable_element:
                    print("利用規約を最下部までスクロールします...")
                    
                    # JavaScriptでスクロール実行
                    driver.execute_script("""
                        const element = arguments[0];
                        element.scrollTop = element.scrollHeight;
                    """, scrollable_element)
                    
                    time.sleep(2)
                    print("スクロール完了")
                
                # モーダルを閉じるボタンを探してクリック
                close_button = None
                close_selectors = [
                    '.btn_close',
                    '.close',
                    'button:contains("閉じる")',
                    '[onclick*="close"]',
                    '.popup_close'
                ]
                
                for selector in close_selectors:
                    try:
                        if ':contains(' in selector:
                            close_button = popup.find_element(By.XPATH, ".//button[contains(text(), '閉じる')]" )
                        else:
                            close_button = popup.find_element(By.CSS_SELECTOR, selector)
                        if close_button and close_button.is_displayed():
                            break
                    except NoSuchElementException:
                        continue
                
                if close_button:
                    print("閉じるボタンをクリックします...")
                    driver.execute_script("arguments[0].click();", close_button)
                    time.sleep(2)
                else:
                    print("閉じるボタンが見つからないため、ESCキーで閉じます")
                    driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                    time.sleep(2)
                    
            except TimeoutException:
                print("ポップアップの表示を確認できませんでした")
        
        # 利用規約同意チェックボックスを処理
        print("利用規約同意チェックボックスを処理します...")
        try:
            # チェックボックスを探す（HTMLから特定）
            checkbox = driver.find_element(By.ID, "agreement")
            
            # JavaScriptでチェックボックスを有効化してチェック
            driver.execute_script("""
                const checkbox = arguments[0];
                checkbox.disabled = false;
                checkbox.checked = true;
                
                // changeイベントを発火
                const changeEvent = new Event('change', { bubbles: true });
                checkbox.dispatchEvent(changeEvent);
                
                // clickイベントも発火
                const clickEvent = new Event('click', { bubbles: true });
                checkbox.dispatchEvent(clickEvent);
            """, checkbox)
            
            time.sleep(1)
            
            # チェック状態を確認
            is_checked = driver.execute_script("return arguments[0].checked;", checkbox)
            print(f"チェックボックス状態: {'チェック済み' if is_checked else 'チェックなし'}")
            
            if is_checked:
                print("[OK] 利用規約同意処理が完了しました")
                return True
            else:
                print("チェックボックスの状態変更に失敗しました")
                return False
                
        except NoSuchElementException:
            print("利用規約同意チェックボックスが見つかりません")
            return False
            
    except Exception as e:
        print(f"利用規約同意処理中にエラーが発生しました: {e}")
        return False

def auto_login(driver, login_id, password):
    """MAXに自動ログインを試行する（修正版）"""
    try:
        print("--- MAX自動ログインを試行します ---")
        driver.get("https://max.btix-ma.com/login/")
        
        wait = WebDriverWait(driver, 15)
        
        # ページの読み込み完了を待つ
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(2)
        
        # 利用規約同意手順を実行
        if not handle_terms_agreement(driver, wait):
            print("利用規約同意処理に失敗しました")
            return False
        
        # ログインフォームの要素が利用可能になるまで少し待機
        time.sleep(2)
        
        # ログインIDフィールドを探して入力
        try:
            # より具体的なセレクターを使用
            id_field = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[type="text"], input[name*="id"], input[name*="login"]')))
            print("ログインIDフィールドを見つけました")
            
            id_field.clear()
            id_field.send_keys(login_id)
            print("ログインIDを入力しました")
            
        except TimeoutException:
            print("ログインIDフィールドが見つかりませんでした")
            return False
        
        # パスワードフィールドを探して入力
        try:
            password_field = driver.find_element(By.CSS_SELECTOR, 'input[type="password"]')
            password_field.clear()
            password_field.send_keys(password)
            print("パスワードを入力しました")
            
        except NoSuchElementException:
            print("パスワードフィールドが見つかりませんでした")
            return False
        
        # ログインボタンを探してクリック
        try:
            # ログインボタンが有効になるまで待機
            login_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[type="submit"], input[type="submit"], .login-btn, #loginBtn')))
            
            print("ログインボタンをクリックします...")
            driver.execute_script("arguments[0].click();", login_button)
            
        except TimeoutException:
            print("ログインボタンが見つからないか、クリックできません")
            return False
        
        # ログイン処理の完了を待つ
        time.sleep(5)
        
        # ログイン成功の確認
        current_url = driver.current_url
        if "login" not in current_url or "top" in current_url or "matter" in current_url:
            print("[OK] MAX自動ログインに成功しました！")
            
            # 案件検索ページへの自動遷移
            try:
                search_url = "https://max.btix-ma.com/top/matter_search"
                print(f"案件検索ページに遷移します: {search_url}")
                driver.get(search_url)
                
                # ページの読み込み完了を待つ
                time.sleep(5)
                print("[OK] 案件検索ページへの遷移が完了しました")
                return True
                
            except Exception as e:
                print(f"案件検索ページへの遷移でエラー: {e}")
                return True  # ログインは成功しているので継続
                
        else:
            print("MAX自動ログインに失敗した可能性があります")
            print(f"現在のURL: {current_url}")
            return False
            
    except Exception as e:
        print(f"MAX自動ログイン中にエラーが発生しました: {e}")
        return False

def manual_login_fallback(driver, login_id):
    """自動ログインに失敗した場合の手動ログイン"""
    print("\n" + "="*60)
    print("自動ログインに失敗しました。手動でログインしてください。")
    print(f"Login ID: {login_id}")
    print("ログイン後、案件検索ページなどが表示されたら、この黒い画面に戻って")
    # input("Enterキーを押してください...")
    print("="*60 + "\n")
    print("--- ログイン後の処理を再開します ---")

def normalize_text(text):
    """テキストの表記揺れを統一する（強化版）"""
    if not text:
        return text
    
    # 全角・半角の統一
    text = text.replace('０', '0').replace('１', '1').replace('２', '2').replace('３', '3').replace('４', '4')
    text = text.replace('５', '5').replace('６', '6').replace('７', '7').replace('８', '8').replace('９', '9')
    text = text.replace('，', ',').replace('、', ',')
    
    # チルダ系の統一
    text = text.replace('〜', '～').replace('~', '～').replace('ー', '～')
    
    # マイナス記号の統一
    text = text.replace('▲', '-').replace('△', '-').replace('−', '-').replace('—', '-')
    
    # スラッシュの統一
    text = text.replace('／', '/')
    
    return text

def _extract_single_value(text):
    """単一の金額文字列から数値を抽出する（表記揺れ対応強化版）"""
    if not text:
        return 0

    # 表記揺れを統一
    text = normalize_text(text)
    
    print(f"        [VALUE_DEBUG] 正規化後のテキスト: '{text}'")
    
    # 数値（マイナス、小数点含む）を抽出（より柔軟なパターン）
    number_patterns = [
        r'(-?[\d,，]+\.?\d*)',  # 基本的な数値パターン
        r'(-?[\d,，]+)',        # 整数パターン
        r'(-?\d+\.?\d*)',       # シンプルな数値パターン
    ]
    
    value = 0
    for pattern in number_patterns:
        match = re.search(pattern, text)
        if match:
            number_str = match.group(1).replace(',', '').replace('，', '')
            try:
                value = float(number_str)
                print(f"        [VALUE_DEBUG] 抽出された数値: {value}")
                break
            except ValueError:
                continue
    
    if value == 0:
        print(f"        [VALUE_DEBUG] 数値を抽出できませんでした")
        return 0

    # 単位の判定（より包括的に）
    unit_multiplier = 1
    if "億" in text:
        unit_multiplier = 100_000_000
        print(f"        [VALUE_DEBUG] 単位: 億円")
    elif "百万円" in text:
        unit_multiplier = 1_000_000
        print(f"        [VALUE_DEBUG] 単位: 百万円")
    elif "万" in text:
        unit_multiplier = 10_000
        print(f"        [VALUE_DEBUG] 単位: 万円")
    elif "千" in text:
        unit_multiplier = 1_000
        print(f"        [VALUE_DEBUG] 単位: 千円")
    else:
        print(f"        [VALUE_DEBUG] 単位: 円（デフォルト）")

    final_value = int(value * unit_multiplier)
    print(f"        [VALUE_DEBUG] 最終計算値: {final_value:,} 円")
    
    return final_value

def parse_financial_value(text):
    """「６億円/年間」、「７億２，０００万円/年間」等を数値(円)の範囲に変換する（表記揺れ対応強化版）"""
    if not text:
        return (0, 0)
        
    print(f"    [PARSE_DEBUG] 元のテキスト: '{text}'")
    
    # 特殊ケースの処理
    if any(keyword in text for keyword in ["応相談", "非開示", "要相談", "別途", "その他"]):
        print(f"    [PARSE_DEBUG] 特殊ケース（相談・非開示等）として除外")
        return (0, 0)

    # 表記揺れを統一
    normalized_text = normalize_text(text)
    print(f"    [PARSE_DEBUG] 正規化後: '{normalized_text}'")
    
    # 「/年間」、「/年」の除去
    normalized_text = re.sub(r'/年間?', '', normalized_text)
    print(f"    [PARSE_DEBUG] 年間表記除去後: '{normalized_text}'")
    
    # 範囲を示す区切り文字（表記揺れ対応）
    separators = ["～", "〜", "~", "ー", "-", "から", "〜"]
    
    # 「未満」「以下」の処理
    if "未満" in normalized_text or "以下" in normalized_text:
        val = _extract_single_value(normalized_text)
        print(f"    [PARSE_DEBUG] 未満/以下パターン: 0 ～ {val:,}")
        return (0, val)
    
    # 「以上」または「～」で終わるパターン
    if "以上" in normalized_text or normalized_text.rstrip().endswith("～"):
        val = _extract_single_value(normalized_text)
        if val < 0:
            print(f"    [PARSE_DEBUG] マイナス以上パターン: -∞ ～ {val:,}")
            return (float('-inf'), val)
        else:
            print(f"    [PARSE_DEBUG] 以上パターン: {val:,} ～ +∞")
            return (val, float('inf'))

    # 範囲パターンの分割
    parts = [normalized_text]
    used_separator = None
    for sep in separators:
        if sep in normalized_text:
            parts = normalized_text.split(sep, 1)  # 最初の区切り文字のみで分割
            used_separator = sep
            break
    
    print(f"    [PARSE_DEBUG] 分割結果: {parts} (区切り文字: {used_separator})")
    
    if len(parts) >= 2:
        min_part = parts[0].strip()
        max_part = parts[1].strip()
        
        min_val = _extract_single_value(min_part)
        max_val = _extract_single_value(max_part)
        
        print(f"    [PARSE_DEBUG] 範囲パターン: {min_val:,} ～ {max_val:,}")
        return (min_val, max_val)
    else:
        single_val = _extract_single_value(normalized_text)
        print(f"    [PARSE_DEBUG] 単一値パターン: {single_val:,}")
        return (single_val, single_val)

def meets_condition(value_range, threshold):
    """範囲が条件を満たすかチェック（範囲の最大値が閾値以上であればOK）"""
    min_val, max_val = value_range
    
    print(f"        [CONDITION_DEBUG] 範囲: {min_val:,} ～ {max_val:,}, 閾値: {threshold:,}")
    
    if max_val < 0 and threshold > 0:
        result = False
        print(f"        [CONDITION_DEBUG] マイナス値のため除外")
    else:
        result = max_val >= threshold
        print(f"        [CONDITION_DEBUG] 判定結果: {'合格' if result else '不合格'}")
    
    return result

def extract_industry_info(text_content):
    """業種・業態を抽出する改良版関数"""
    # より包括的な業界キーワード辞書
    industry_patterns = {
        # IT・通信・システム関連
        'IT・システム': ['IT', 'システム', 'ソフトウェア', 'アプリ', 'プログラム', 'SE', 'エンジニア', 'プラットフォーム', 'クラウド', 'AI', 'IoT', 'DX'],
        '通信・インターネット': ['通信', 'インターネット', 'ネット', 'Web', 'ウェブ', 'SNS', 'ECサイト', 'オンライン', 'デジタル', 'データ'],
        
        # 製造・建設関連  
        '製造業': ['製造', '工場', '生産', 'メーカー', '部品', '機械', '設備', '装置', '電子', '精密', '金属', '化学', '素材', '材料'],
        '建設・不動産': ['建設', '工事', '施工', '建築', '土木', '不動産', '住宅', 'マンション', 'ビル', '物件', '賃貸', '売買', '仲介'],
        
        # サービス・小売関連
        'サービス業': ['サービス', '清掃', '警備', '人材', '派遣', '紹介', 'コンサル', 'コンサルティング', '広告', 'PR', '企画', '代行'],
        '小売・卸売': ['小売', '卸売', '販売', '売買', '商社', '貿易', '輸出', '輸入', '流通', '物流', '配送', '運送', '倉庫'],
        
        # 飲食・宿泊関連
        '飲食業': ['飲食', 'レストラン', '居酒屋', 'カフェ', '喫茶', '食堂', '弁当', '仕出し', 'ケータリング', '食品', '料理'],
        '宿泊・観光': ['ホテル', '旅館', '宿泊', '観光', '旅行', 'ツアー', 'レジャー', 'リゾート', '温泉'],
        
        # 医療・福祉・教育関連
        '医療・介護': ['医療', '病院', 'クリニック', '診療所', '薬局', '介護', 'デイサービス', '福祉', 'ヘルパー', 'ケア', '看護'],
        '教育・研修': ['教育', '学習', '塾', 'スクール', '研修', 'セミナー', '講座', 'トレーニング', 'eラーニング'],
        
        # 金融・保険関連
        '金融・保険': ['金融', '銀行', '証券', '保険', 'ファイナンス', '投資', '融資', 'ローン', 'クレジット', 'リース'],
        
        # その他専門サービス
        '専門サービス': ['法律', '税理', '会計', '監査', '特許', '翻訳', '通訳', 'デザイン', '印刷', '出版'],
        '美容・健康': ['美容', 'エステ', '化粧品', 'コスメ', '健康', 'フィットネス', 'ジム', 'マッサージ', '整体'],
        'その他': ['その他', 'other', 'サポート', 'メンテナンス', '保守', '管理']
    }
    
    # テキストから業界を判定
    found_industries = []
    text_lower = text_content.lower()
    
    for industry, keywords in industry_patterns.items():
        for keyword in keywords:
            if keyword.lower() in text_lower or keyword in text_content:
                if industry not in found_industries:
                    found_industries.append(industry)
                    print(f"    [INDUSTRY_DEBUG] 業界キーワード '{keyword}' から '{industry}' を検出")
    
    # 複数見つかった場合は最初のものを返す、見つからなかった場合はパターンマッチング
    if found_industries:
        return found_industries[0]
    
    # より高度なパターンマッチングを実行
    advanced_patterns = [
        (r'.*業$', '製造業'),  # 〜業で終わる
        (r'.*サービス', 'サービス業'),
        (r'.*システム', 'IT・システム'),
        (r'.*商事|.*商社', '小売・卸売'),
        (r'.*建設|.*工業', '建設・不動産'),
        (r'株式会社.*', None)  # 会社名パターンは除外
    ]
    
    for pattern, industry in advanced_patterns:
        if industry and re.search(pattern, text_content):
            print(f"    [INDUSTRY_DEBUG] パターン '{pattern}' から '{industry}' を検出")
            return industry
    
    print(f"    [INDUSTRY_DEBUG] 業界を特定できませんでした")
    return None

def extract_deal_info(deal_soup):
    """MAXの案件情報から必要な項目を抽出する（業種・業態抽出強化版）"""
    
    text_content = deal_soup.get_text()
    
    # デバッグ: 処理中の要素の全テキストを表示（最初の300文字）
    print(f"    [DEBUG] 処理中の要素内容: {text_content[:300]}...")
    
    # テーブル行の場合、セルごとに分割して処理
    cells = deal_soup.find_all(['td', 'th'])
    cell_texts = [cell.get_text(strip=True) for cell in cells]
    
    print(f"    [DEBUG] テーブルセル数: {len(cells)}")
    for i, cell_text in enumerate(cell_texts[:10]):  # 最初の10セルを表示
        print(f"    [DEBUG] セル{i+1}: '{cell_text}'")
    
    # 初期化
    project_id = None
    industry = None
    title = None
    region = None
    revenue = None
    price = None
    
    # 1. まず全体テキストから業種・業態を抽出（優先）
    industry = extract_industry_info(text_content)
    
    # 2. セル単位での業種・業態抽出（補完）
    if not industry and len(cell_texts) > 1:
        for i, cell_text in enumerate(cell_texts[:8]):  # 最初の8セルをチェック
            cell_industry = extract_industry_info(cell_text)
            if cell_industry:
                industry = cell_industry
                print(f"    [DEBUG] セル{i+1}から業界検出: '{industry}'")
                break
    
    # MAXの標準的なテーブル構造に基づく抽出
    if len(cell_texts) >= 5:  # 最低5つのセルがある場合
        # 案件ID（最初のセル、数値のみまたは英数字）
        if cell_texts[0] and re.match(r'^[A-Z0-9\-_]+$', cell_texts[0]):
            project_id = cell_texts[0]
            print(f"    [DEBUG] 案件ID発見（セル1）: '{project_id}'")
        
        # タイトル抽出の改善
        for i, cell_text in enumerate(cell_texts):
            if (cell_text and len(cell_text) > 10 and len(cell_text) < 200 and 
                not any(keyword in cell_text for keyword in ['円', '万', '億', '県', '都', '府', '道']) and
                not re.match(r'^[A-Z0-9\-_]+$', cell_text)):  # IDっぽくない
                if not title or len(cell_text) > len(title):  # より長い説明文を優先
                    title = cell_text
                    print(f"    [DEBUG] タイトル候補発見（セル{i+1}）: '{title[:50]}...'")
        
        # 地域（地域らしいキーワード）
        for i, cell_text in enumerate(cell_texts):
            if cell_text:
                region_keywords = ['地方', '圏', '県', '都', '府', '道', '市', '区', '町', '村']
                if any(keyword in cell_text for keyword in region_keywords) and len(cell_text) < 50:
                    region = cell_text
                    print(f"    [DEBUG] 地域発見（セル{i+1}）: '{region}'")
                    break
        
        # 売上規模と希望価格（金額表記を含むセルを全てチェック）
        money_cells = []
        for i, cell_text in enumerate(cell_texts):
            if cell_text and any(keyword in cell_text for keyword in ['円', '万', '億']):
                money_cells.append((i, cell_text))
                print(f"    [DEBUG] 金額セル発見（セル{i+1}）: '{cell_text}'")
        
        # 売上と価格の判別ロジック改善
        for i, cell_text in money_cells:
            # 売上関連キーワードが含まれる場合
            if any(keyword in cell_text for keyword in ['売上', '年商', '収益', '営業', '業績']) and not revenue:
                revenue = cell_text
                print(f"    [DEBUG] 売上規模発見（セル{i+1}）: '{revenue}'")
            # 価格関連キーワードが含まれる場合
            elif any(keyword in cell_text for keyword in ['価格', '譲渡', '希望', '売却']) and not price:
                price = cell_text
                print(f"    [DEBUG] 希望価格発見（セル{i+1}）: '{price}'")
        
        # 売上と価格がまだ見つからない場合、位置で推定
        if money_cells and not revenue and not price:
            if len(money_cells) == 1:
                # 1つだけの場合は売上として扱う
                revenue = money_cells[0][1]
                print(f"    [DEBUG] 単一金額を売上規模として扱います: '{revenue}'")
            elif len(money_cells) >= 2:
                # 複数ある場合、最初を売上、2番目を価格として扱う
                revenue = money_cells[0][1]
                price = money_cells[1][1]
                print(f"    [DEBUG] 位置推定 - 売上規模: '{revenue}', 希望価格: '{price}'")
    
    # フォールバック: パターンマッチングによる抽出
    if not project_id:
        id_patterns = [
            r'([0-9]{5,})',           # 5桁以上の数字
            r'([A-Z]{2,}[0-9]{3,})',  # 英字+数字
            r'([0-9]{4,}[A-Z]{2,})',  # 数字+英字
            r'ID[:\s]*([A-Z0-9\-_]+)', # ID:の後に続く文字列
        ]
        
        for pattern in id_patterns:
            id_match = re.search(pattern, text_content)
            if id_match:
                project_id = id_match.group(1)
                print(f"    [DEBUG] パターンから案件ID発見: '{project_id}'")
                break
    
    if not revenue:
        # より柔軟な売上規模抽出
        revenue_patterns = [
            r'売上(?:規模|高|実績)?[\s:：]*([0-9,，]+(?:億|万|千|百万)?円(?:/年間?)?)',
            r'年商[\s:：]*([0-9,，]+(?:億|万|千|百万)?円[^\s\n]*)',
            r'([0-9,，]+(?:億|万|千|百万)?円)(?:\s*/年|\s*年間|\s*の売上)',
            r'収益[\s:：]*([0-9,，]+(?:億|万|千|百万)?円[^\s\n]*)',
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, text_content)
            if match:
                revenue_candidate = match.group(1).strip()
                if len(revenue_candidate) < 100 and revenue_candidate != price:
                    revenue = revenue_candidate
                    print(f"    [DEBUG] パターンから売上規模発見: '{revenue}'")
                    break
    
    if not price:
        # 希望価格の抽出
        price_patterns = [
            r'(?:希望|譲渡|売却)?価格[\s:：]*([0-9,，]+(?:億|万|千|百万)?円[^\s\n]*)',
            r'([0-9,，]+(?:億|万|千|百万)?円)(?:\s*で|\s*にて)(?:譲渡|売却)',
            r'譲渡[\s:：]*([0-9,，]+(?:億|万|千|百万)?円[^\s\n]*)',
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, text_content)
            if match:
                price_candidate = match.group(1).strip()
                if price_candidate != revenue:
                    price = price_candidate
                    print(f"    [DEBUG] パターンから希望価格発見: '{price}'")
                    break
    
    # IDが見つからない場合は自動生成
    if not project_id:
        import random
        project_id = f"UNKNOWN_{random.randint(1000, 9999)}"
        print(f"    [DEBUG] ID未発見のため自動生成: '{project_id}'")
    
    # タイトルが見つからない場合のフォールバック
    if not title:
        # 最長のテキストセルをタイトル候補とする
        longest_text = ""
        for cell_text in cell_texts:
            if (cell_text and len(cell_text) > len(longest_text) and 
                len(cell_text) > 15 and len(cell_text) < 200 and
                not any(keyword in cell_text for keyword in ['円', 'ID']) and
                not re.match(r'^[A-Z0-9\-_]+$', cell_text)):
                longest_text = cell_text
        
        if longest_text:
            title = longest_text
            print(f"    [DEBUG] 最長テキストをタイトルとして採用: '{title[:50]}...'")
    
    # リンク抽出
    detail_link = None
    links = deal_soup.find_all('a', href=True)
    for link in links:
        href = link['href']
        if href and ('detail' in href or 'matter' in href):
            detail_link = href
            if not detail_link.startswith('http'):
                detail_link = 'https://max.btix-ma.com' + detail_link
            break
    
    result = {
        'title': title or 'タイトル不明',
        'project_id': project_id,
        'revenue': revenue,
        'profit': None,  # 営業利益は別途抽出が必要な場合
        'price': price,
        'industry': industry or '業界不明',  # 業界が見つからない場合のデフォルト
        'region': region,
        'link': detail_link
    }
    
    print(f"    [DEBUG] 最終抽出結果: {result}")
    
    return result

def scrape_all_pages(driver, min_revenue):
    """全ページから案件情報を抽出する（表記揺れ対応強化版）"""
    all_found_deals = []
    processed_ids = set()
    page_num = 1
    
    wait = WebDriverWait(driver, 20)
    
    while True:
        print(f"\n=== ページ {page_num} の処理を開始します ===")
        
        # 現在のページのURLを表示
        print(f"現在のURL: {driver.current_url}")
        
        # ページの読み込みを待つ
        time.sleep(5)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # MAXサイトの案件要素を探す（テーブル行に特化）
        print("テーブル行から案件情報を抽出します...")
        
        # まずすべてのtr要素を取得
        all_rows = soup.select("tr")
        print(f"全テーブル行数: {len(all_rows)} 個")
        
        current_page_deals = []
        header_skipped = False
        
        for i, row in enumerate(all_rows):
            text_content = row.get_text()
            
            # デバッグ用：最初の10行の内容を表示
            if i < 10:
                print(f"    行 {i+1}: {text_content[:100]}...")
            
            # ヘッダー行をスキップ（一般的なヘッダーキーワードを含む行）
            if not header_skipped and ('タイトル' in text_content or 'ヘッダー' in text_content or 
                                     '案件名' in text_content or ('ID' in text_content and '売上' in text_content)):
                print(f"    ヘッダー行をスキップ: 行 {i+1}")
                header_skipped = True
                continue
            
            # より緩い条件で案件行を判定
            is_deal_row = False
            
            # 条件1: 金額情報を含む（円、万、億のいずれかが含まれる）
            has_money = ('円' in text_content or '万' in text_content or '億' in text_content)
            
            # 条件2: 案件らしい情報を含む
            has_business_info = (
                '売上' in text_content or '価格' in text_content or '規模' in text_content or
                '事業' in text_content or '会社' in text_content or 'ID' in text_content or
                '譲渡' in text_content or '買収' in text_content or '業界' in text_content or
                '製造' in text_content or 'サービス' in text_content or '地方' in text_content or
                '建設' in text_content or '不動産' in text_content
            )
            
            # 条件3: 最小限のテキスト長（空行や短すぎる行を除外）
            has_sufficient_content = len(text_content.strip()) > 20
            
            # 条件4: 明らかにナビゲーション要素ではない
            not_navigation = not ('ページ' in text_content and ('前へ' in text_content or '次へ' in text_content))
            
            # 総合判定（条件を緩和）
            if has_sufficient_content and not_navigation and (has_money or has_business_info):
                is_deal_row = True
                print(f"    [OK] 案件行として判定: 行 {i+1}")
            elif i < 10:  # 最初の10行については なぜ除外されたかを表示
                print(f"    [NG] 除外: 行 {i+1} (金額:{has_money}, 事業情報:{has_business_info}, 十分な長さ:{has_sufficient_content})")
            
            if is_deal_row:
                current_page_deals.append(row)
        
        print(f"案件として判定された行数: {len(current_page_deals)} 個")
        
        if not current_page_deals:
            print("案件要素が見つかりませんでした。ページ全体から情報を抽出します...")
            # フォールバック: ページ全体を1つの案件として処理
            page_info = extract_deal_info(soup)
            if page_info['revenue'] or page_info['price']:
                current_page_deals = [soup]
        
        print(f"ページ {page_num} で処理対象の案件数: {len(current_page_deals)}")
        
        # 現在のページで見つかった案件数をカウント
        page_new_deals = 0
        
        for deal in current_page_deals:
            deal_info = extract_deal_info(deal)
            
            project_id = deal_info['project_id']
            if project_id in processed_ids:
                print(f"    案件ID {project_id} は既に処理済みのため、スキップします")
                continue
            processed_ids.add(project_id)
            
            title = deal_info['title']
            revenue_str = deal_info['revenue']
            profit_str = deal_info['profit']     # 営業利益（抽出のみ）
            price_str = deal_info['price']       # 希望価格（情報のみ）
            industry = deal_info['industry']
            region = deal_info.get('region', '')
            full_link = deal_info['link'] or "リンク不明"

            print(f"\n    案件ID: {project_id}")
            print(f"    タイトル: {title}")
            print(f"    業界: {industry}")  # 改善された業界情報
            print(f"    地域: {region or '不明'}")
            print(f"    売上規模: {revenue_str or '情報なし'}")
            
            # 売上規模の条件判定（表記揺れ対応）
            revenue_ok = False
            if revenue_str:
                revenue_range = parse_financial_value(revenue_str)
                revenue_ok = meets_condition(revenue_range, min_revenue)
            
            print(f"         営業利益: {profit_str or '情報なし'}")
            print(f"         希望価格: {price_str or '情報なし'}")

            if revenue_ok:
                print(f"    [合格] 売上規模の条件を満たしています")
                all_found_deals.append([
                    title,
                    project_id,
                    industry,  # 改善された業界情報
                    revenue_str or "情報なし",
                    "-",  # 営業利益は常に「-」として表記
                    price_str or "情報なし",    # 希望価格
                    full_link
                ])
                page_new_deals += 1
            else:
                print(f"    [除外] 売上規模の条件を満たしていません")
        
        print(f"\nページ {page_num} で新規追加された案件: {page_new_deals} 件")
        print(f"累計合格案件数: {len(all_found_deals)} 件")
        
        # 次のページへのリンクを探す
        next_page_found = False
        try:
            # 一般的な「次へ」ボタンのセレクター
            next_selectors = [
                "a:contains('次')",
                "a:contains('>')", 
                "a:contains('→')",
                ".next",
                ".pagination-next",
                "[class*='next']",
                "a[href*='page']",
                "a[href*='Page']"
            ]
            
            next_link = None
            for selector in next_selectors:
                try:
                    if ':contains(' in selector:
                        # XPathで検索
                        next_elements = driver.find_elements(By.XPATH, f"//a[contains(text(), '次') or contains(text(), '>') or contains(text(), '→')]" )
                        for element in next_elements:
                            if element.is_displayed() and element.is_enabled():
                                next_link = element
                                break
                    else:
                        # CSSセレクターで検索
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        for element in elements:
                            if element.is_displayed() and element.is_enabled():
                                # リンクテキストや属性をチェック
                                link_text = element.text.strip().lower()
                                href = element.get_attribute('href') or ''
                                if ('次' in element.text or 'next' in link_text or 
                                    '>' in element.text or 'page' in href):
                                    next_link = element
                                    break
                    
                    if next_link:
                        break
                        
                except Exception as e:
                    continue
            
            # ページ番号による直接遷移も試行
            if not next_link:
                next_page_num = page_num + 1
                page_links = driver.find_elements(By.XPATH, f"//a[contains(text(), '{next_page_num}')]")
                for link in page_links:
                    if link.is_displayed() and link.is_enabled():
                        next_link = link
                        break
            
            if next_link:
                print(f"次のページ（{page_num + 1}）へ移動します...")
                try:
                    # JavaScriptでクリック（より確実）
                    driver.execute_script("arguments[0].click();", next_link)
                    page_num += 1
                    next_page_found = True
                    
                    # ページ遷移の完了を待つ
                    time.sleep(5)
                    
                    # URLが変わったかチェック（オプション）
                    new_url = driver.current_url
                    print(f"新しいページのURL: {new_url}")
                    
                except Exception as e:
                    print(f"次のページへの遷移でエラーが発生しました: {e}")
                    break
            else:
                print("次のページが見つかりません。全ページの処理が完了しました。")
                break
                
        except Exception as e:
            print(f"ページネーション処理でエラーが発生しました: {e}")
            break
        
        if not next_page_found:
            break
    
    print(f"\n=== 全 {page_num} ページの処理が完了しました ===")
    print(f"合計処理案件数: {len(processed_ids)} 件")
    print(f"条件に合致する案件数: {len(all_found_deals)} 件")
    
    return all_found_deals

def main():
    """メインの実行関数（表記揺れ対応強化版）"""
    config = load_config()
    
    # MAX用の設定セクションを追加  
    try:
        creds = config['MAXCredentials']
        conds = config['ScrapingConditions'] 
        output = config['MAXOutput']
    except KeyError as e:
        print(f"設定ファイルに必要なセクションが見つかりません: {e}")
        print("config.iniに以下のセクションを追加してください:")
        print("""
[MAXCredentials]
LoginID = あなたのログインID
Password = あなたのパスワード

[MAXOutput]
FileName = max_deals.csv
""")
        return

    # Chromeオプションを追加（安定性向上のため）
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    
    all_found_deals = []

    try:
        # 自動ログインを試行
        auto_login_success = auto_login(driver, creds['LoginID'], creds['Password'])
        
        if auto_login_success:
            print("MAX自動ログイン・遷移が完了しました。案件の読み込みを開始します。")
        else:
            # 自動ログインに失敗した場合は手動ログインにフォールバック
            manual_login_fallback(driver, creds['LoginID'])
            
            # 手動ログイン後に案件検索ページに遷移
            SEARCH_URL = "https://max.btix-ma.com/top/matter_search"
            print(f"案件検索ページに移動します: {SEARCH_URL}")
            driver.get(SEARCH_URL)

        min_revenue = int(conds.get('MinRevenue', 0))
        print(f"売上規模の最小条件: {min_revenue:,} 円")
        
        print("\n--- MAX案件情報の全ページ抽出を開始します ---")
        
        # 全ページから案件情報を抽出（表記揺れ対応強化版）
        all_found_deals = scrape_all_pages(driver, min_revenue)

    finally:
        print("\n--- 処理が完了しました。ブラウザを閉じます ---")
        driver.quit()

    print(f"\n--- {len(all_found_deals)}件の案件を抽出しました ---")
    headers = ['タイトル', '案件ID', '業界', '売上規模', '営業利益', '希望価格', 'リンク']
    
    return {"headers": headers, "data": all_found_deals}

if __name__ == "__main__":
    results = main()
    if results:
        print("\n" + "="*50)
        print(f"MAX調査完了！ 条件に一致する {len(results['data'])} 件の案件が見つかりました。")
        print("="*50)
        for deal in results['data'][:3]:
            print(f"【タイトル】{deal[0]}\n【案件ID】{deal[1]}\n【業界】{deal[2]}\n【売上規模】{deal[3]}\n【営業利益】{deal[4]}\n【希望価格】{deal[5]}\n【リンク】{deal[6]}\n" + "-"*20)