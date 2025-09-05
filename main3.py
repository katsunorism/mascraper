# main3.py - スピードM&A専用スクレイピングコード（修正版）
import httpx
from bs4 import BeautifulSoup, Tag
import gspread
from google.oauth2.service_account import Credentials
import datetime
import hashlib
import traceback
import logging
import yaml
import time
import os
import re
import random
from functools import wraps
from typing import Optional, Dict, List, Set, Any
from dataclasses import dataclass, fields
from enum import Enum

# Selenium関連
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- グローバル設定 ---
CONFIG: Dict[str, Any] = {}

# --- 定数と構造化データクラス ---
class Constants:
    FIELD_EXTRACTION_TIME = "extraction_time"
    FIELD_SITE_NAME = "site_name"
    FIELD_DEAL_ID = "deal_id"
    FIELD_TITLE = "title"
    FIELD_LOCATION = "location"
    FIELD_REVENUE = "revenue"
    FIELD_PROFIT = "profit"
    FIELD_PRICE = "price"
    FIELD_FEATURES = "features"
    FIELD_LINK = "link"
    FIELD_UNIQUE_ID = "unique_id"

class ScrapingStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    BLOCKED = "blocked"

@dataclass
class RawDealData:
    site_name: str
    title: str
    deal_id: str
    link: str
    revenue_text: str = ""
    profit_text: str = ""
    location_text: str = ""
    price_text: str = ""
    features_text: str = ""

@dataclass
class FormattedDealData:
    extraction_time: str
    site_name: str
    deal_id: str
    title: str
    features: str
    location: str
    revenue: str
    profit: str
    price: str
    link: str
    unique_id: str

# --- データ変換クラス ---
class SpeedMADataConverter:
    @staticmethod
    def parse_financial_value(text: str) -> int:
        """スピードM&Aの財務テキストを数値に変換"""
        if not text or any(keyword in text for keyword in ["非公開", "応相談", "赤字", "N/A", "希望なし", "**"]):
            return 0
        
        # 全角数字を半角に変換、カンマ除去
        text = text.translate(str.maketrans('０１２３４５６７８９', '0123456789')).replace(',', '')
        
        # レンジ表記の場合は下限値を取得
        if any(separator in text for separator in ['〜', '～', '-', '?']):
            for separator in ['〜', '～', '-', '?']:
                if separator in text:
                    parts = text.split(separator)
                    if len(parts) >= 1:
                        text = parts[0].strip()
                    break
        
        # 数値とマルチプライヤーを抽出
        match = re.search(r'([\d\.]+)', text)
        if not match:
            return 0
        
        try:
            value = float(match.group(1))
        except ValueError:
            return 0
        
        # 単位による乗算
        multipliers = {
            '億円': 100_000_000,
            '億': 100_000_000,
            '千万円': 10_000_000,
            '千万': 10_000_000,
            '百万円': 1_000_000,
            '百万': 1_000_000,
            '万円': 10_000,
            '万': 10_000
        }
        
        for unit, multiplier in multipliers.items():
            if unit in text:
                value *= multiplier
                break
        
        return int(value)
    
    @staticmethod
    def format_to_million_yen(text: str) -> str:
        """財務テキストを百万円単位に統一フォーマット"""
        if not text or any(keyword in text for keyword in ["非公開", "応相談", "赤字", "N/A", "希望なし", "**"]):
            return text or "N/A"
        
        def _convert_to_million(text_part: str) -> str:
            clean_text = text_part.replace(',', '').strip()
            match = re.search(r'([\d\.]+)', clean_text)
            if not match:
                return text_part
            
            try:
                value = float(match.group(1))
            except ValueError:
                return text_part
            
            million_value = 0
            if '億' in text_part:
                million_value = value * 100
            elif '千万' in text_part:
                million_value = value * 10
            elif '百万' in text_part:
                million_value = value
            elif '万' in text_part and '千万' not in text_part and '百万' not in text_part:
                million_value = value / 100
            else:
                # 単位が明示されていない場合、元のテキストを返す
                return text_part
            
            if million_value >= 1:
                return f"{int(million_value):,}百万円" if million_value == int(million_value) else f"{million_value:.1f}百万円"
            else:
                return text_part
        
        # レンジ表記の処理
        range_separators = ['〜', '～', '-', '?']
        for separator in range_separators:
            if separator in text:
                parts = text.split(separator)
                if len(parts) == 2:
                    lower = _convert_to_million(parts[0].strip())
                    upper = _convert_to_million(parts[1].strip())
                    return f"{lower}～{upper}"
                break
        
        return _convert_to_million(text)

# --- アンチブロッキング管理クラス ---
class AntiBlockingManager:
    def __init__(self):
        self.blocked_detected = False
        self.retry_count = 0
        self.max_retries = 1
        
    def get_human_like_delay(self, base_min: int = 3, base_max: int = 8) -> float:
        return random.uniform(base_min, base_max)
    
    def get_recovery_delay(self) -> float:
        return random.uniform(15, 30)
    
    def is_blocked_response(self, html_content: str) -> bool:
        if not html_content:
            return False
            
        blocked_indicators = [
            "403 ERROR", "The request could not be satisfied",
            "Request blocked", "cloudfront", "Access Denied", "Forbidden"
        ]
        
        content_lower = html_content.lower()
        
        for indicator in blocked_indicators:
            if indicator.lower() in content_lower:
                return True
        
        soup = BeautifulSoup(html_content, 'lxml')
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text().lower()
            if 'error' in title_text or 'blocked' in title_text or 'denied' in title_text:
                return True
        
        return False

# --- WebDriver管理クラス ---
class WebDriverManager:
    def __init__(self, headless: bool = True, anti_blocking: AntiBlockingManager = None):
        self.headless = headless
        self.driver: Optional[webdriver.Chrome] = None
        self.anti_blocking = anti_blocking or AntiBlockingManager()

    def __enter__(self) -> webdriver.Chrome:
        logging.info("Initializing Selenium WebDriver for SpeedM&A...")
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        chrome_options.add_argument(f"--user-agent={user_agent}")
        
        try:
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            logging.info("✅ WebDriver initialized successfully.")
            return self.driver
        except Exception as e:
            logging.error(f"Failed to initialize WebDriver: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            try:
                self.driver.quit()
                logging.info("WebDriver has been closed.")
            except Exception as e:
                logging.error(f"Error closing WebDriver: {e}")

# --- スピードM&A専用パーサークラス ---
class SpeedMAParser:
    @staticmethod
    def parse_list_page(html_content: str) -> List[RawDealData]:
        """スピードM&Aの一覧ページパーサー（修正版）"""
        soup = BeautifulSoup(html_content, 'lxml')
        results = []
        
        # デバッグ用HTMLファイル保存
        if CONFIG.get('debug', {}).get('save_html_files', False):
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            debug_file = os.path.join("debug", f"debug_speedma_list_{timestamp}.html")
            os.makedirs("debug", exist_ok=True)
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logging.info(f"Debug: HTML saved to {debug_file}")
        
        # 修正：正しい案件アイテムセレクタを使用
        # 実際のHTMLに合わせてセレクタを調整
        items = soup.select('a.swiper-slide.p_card')  # 修正されたセレクタ
        
        if not items:
            # 別のセレクタも試す
            items = soup.select('a[href*="/projects/"]')
            logging.info(f"Alternative selector found {len(items)} items")
        
        logging.info(f"Found {len(items)} deal items on the page")
        
        for i, item in enumerate(items):
            try:
                logging.info(f"Processing item {i+1}/{len(items)}")
                
                # リンクの抽出
                link = item.get('href', '')
                if not link:
                    logging.warning(f"No link found in item {i+1}")
                    continue
                
                if not link.startswith('http'):
                    link = f"https://speed-ma.com{link}"
                
                # 案件IDの抽出（URLから）
                deal_id_match = re.search(r'/projects/(\d+)', link)
                if not deal_id_match:
                    logging.warning(f"No deal ID found in URL: {link}")
                    continue
                
                deal_id = deal_id_match.group(1)
                logging.info(f"Found deal ID: {deal_id}")
                
                # 売上高の事前チェック（一覧ページから）
                revenue_elem = item.select_one('.p_sales div')
                if revenue_elem:
                    revenue_text = revenue_elem.get_text(strip=True)
                    revenue_value = SpeedMADataConverter.parse_financial_value(revenue_text)
                    min_revenue = CONFIG.get('speed_ma', {}).get('revenue_threshold', 300000000)
                    
                    logging.info(f"Deal {deal_id}: Revenue from list = {revenue_text} ({revenue_value:,})")
                    
                    # 売上高が基準を満たさない場合はスキップ
                    if revenue_value < min_revenue:
                        logging.info(f"Skipping deal {deal_id}: Revenue {revenue_value:,} < {min_revenue:,}")
                        continue
                else:
                    logging.warning(f"No revenue info found in list for deal {deal_id}")
                
                # タイトルの抽出
                title_elem = item.select_one('.pcard__title-title-front')
                if title_elem:
                    title = title_elem.get_text(strip=True)
                else:
                    title = f"スピードM&A案件_{deal_id}"
                
                deal_data = RawDealData(
                    site_name="スピードM&A",
                    deal_id=deal_id,
                    title=title,
                    link=link,
                    revenue_text="",
                    profit_text="",
                    location_text="",
                    price_text="",
                    features_text=""
                )
                
                results.append(deal_data)
                logging.info(f"Successfully extracted deal: {deal_id} (passed revenue filter)")
                
            except Exception as e:
                logging.error(f"Error parsing item {i+1}: {e}")
                continue
        
        logging.info(f"スピードM&A: Successfully extracted {len(results)} deals from list page (after revenue filtering)")
        return results

# --- 詳細ページスクレイパークラス ---
class SpeedMADetailScraper:
    def __init__(self, driver: webdriver.Chrome, anti_blocking: AntiBlockingManager):
        self.driver = driver
        self.anti_blocking = anti_blocking

    def enhance_deal_with_details(self, deal: RawDealData) -> RawDealData:
        """詳細ページから情報を取得してdealを拡張（修正版）"""
        try:
            logging.info(f"    -> Accessing detail page: {deal.link}")
            
            # 人間らしい待機時間
            delay = self.anti_blocking.get_human_like_delay(2, 5)
            logging.info(f"    -> Waiting {delay:.1f} seconds before access...")
            time.sleep(delay)
            
            # ページにアクセス
            self.driver.get(deal.link)
            time.sleep(3)  # ページ読み込み待機
            
            html_content = self.driver.page_source
            
            # 403ブロックの検出
            if self.anti_blocking.is_blocked_response(html_content):
                logging.warning(f"    -> 🚫 403 BLOCK DETECTED for deal: {deal.deal_id}")
                return deal
            
            # デバッグ用HTMLファイル保存
            if CONFIG.get('debug', {}).get('save_html_files', False):
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                debug_file = os.path.join("debug", f"debug_speedma_detail_{deal.deal_id}_{timestamp}.html")
                os.makedirs("debug", exist_ok=True)
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                logging.info(f"Debug: Detail HTML saved to {debug_file}")
            
            detail_soup = BeautifulSoup(html_content, 'lxml')
            
            # 各情報を抽出
            deal.title = self._extract_title(detail_soup, deal.deal_id)
            deal.location_text = self._extract_location(detail_soup)
            deal.revenue_text = self._extract_revenue(detail_soup)
            deal.profit_text = self._extract_profit(detail_soup)
            deal.price_text = self._extract_price(detail_soup)
            
            logging.info(f"    -> Enhanced deal: {deal.deal_id}")
            logging.info(f"    -> Revenue: {deal.revenue_text}, Profit: {deal.profit_text}")
            
            return deal
            
        except Exception as e:
            logging.error(f"    -> Error enhancing deal {deal.deal_id}: {e}")
            return deal

    def _extract_title(self, soup: BeautifulSoup, deal_id: str) -> str:
        """事業概要を抽出してタイトルとする"""
        try:
            # 事業概要を探す
            items = soup.select('li.single_project_overviewList__item')
            for item in items:
                title_elem = item.select_one('p.single_project_overviewList__item-title')
                if title_elem and '事業概要' in title_elem.get_text(strip=True):
                    text_elem = item.select_one('div.single_project_overviewList__item-text')
                    if text_elem:
                        # brタグを改行に変換
                        for br in text_elem.find_all('br'):
                            br.replace_with('\n')
                        
                        title_text = text_elem.get_text(strip=True)
                        if title_text and len(title_text) > 10:
                            # 長すぎる場合は適度にトリミング
                            if len(title_text) > 200:
                                title_text = title_text[:200] + "..."
                            return title_text
            
            return f"スピードM&A案件_{deal_id}"
            
        except Exception as e:
            logging.error(f"Error extracting title: {e}")
            return f"スピードM&A案件_{deal_id}"

    def _extract_location(self, soup: BeautifulSoup) -> str:
        """地域を抽出"""
        try:
            items = soup.select('li.single_project_overviewList__item')
            for item in items:
                title_elem = item.select_one('p.single_project_overviewList__item-title')
                if title_elem and '地域' in title_elem.get_text(strip=True):
                    text_elem = item.select_one('div.single_project_overviewList__item-text')
                    if text_elem:
                        return text_elem.get_text(strip=True)
            
            return ""
            
        except Exception as e:
            logging.error(f"Error extracting location: {e}")
            return ""

    def _extract_revenue(self, soup: BeautifulSoup) -> str:
        """売上高を抽出（修正版）"""
        try:
            items = soup.select('li.single_project_overviewList__item')
            for item in items:
                title_elem = item.select_one('p.single_project_overviewList__item-title')
                if title_elem and '売上高' in title_elem.get_text(strip=True):
                    text_elem = item.select_one('div.single_project_overviewList__item-text')
                    if text_elem:
                        revenue_text = text_elem.get_text(strip=True)
                        # マスクされている場合の対処
                        if "**" in revenue_text:
                            logging.warning("Revenue is masked (not logged in)")
                            return ""
                        return revenue_text
            
            return ""
            
        except Exception as e:
            logging.error(f"Error extracting revenue: {e}")
            return ""

    def _extract_profit(self, soup: BeautifulSoup) -> str:
        """営業利益を抽出（修正版）"""
        try:
            # まず、案件概要から営業利益を探す
            items = soup.select('li.single_project_overviewList__item')
            for item in items:
                title_elem = item.select_one('p.single_project_overviewList__item-title')
                if title_elem:
                    title_text = title_elem.get_text(strip=True)
                    if '営業利益' in title_text or '利益' in title_text:
                        text_elem = item.select_one('div.single_project_overviewList__item-text')
                        if text_elem:
                            profit_text = text_elem.get_text(strip=True)
                            if "**" in profit_text:
                                logging.warning("Profit is masked (not logged in)")
                                return ""
                            return profit_text
            
            # 財務情報セクションからも探す（ログイン時用）
            financial_items = soup.select('li.single_project_financialList__item')
            for item in financial_items:
                title_elem = item.select_one('p.single_project_financialList__item-title')
                if title_elem and '営業利益' in title_elem.get_text(strip=True):
                    text_elem = item.select_one('div.single_project_financialList__item-text')
                    if text_elem:
                        profit_text = text_elem.get_text(strip=True)
                        if "**" in profit_text:
                            logging.warning("Profit is masked (not logged in)")
                            return ""
                        return profit_text
            
            return ""
            
        except Exception as e:
            logging.error(f"Error extracting profit: {e}")
            return ""

    def _extract_price(self, soup: BeautifulSoup) -> str:
        """希望譲渡価格を抽出"""
        try:
            items = soup.select('li.single_project_overviewList__item')
            for item in items:
                title_elem = item.select_one('p.single_project_overviewList__item-title')
                if title_elem and '希望譲渡価格' in title_elem.get_text(strip=True):
                    text_elem = item.select_one('div.single_project_overviewList__item-text')
                    if text_elem:
                        return text_elem.get_text(strip=True)
            
            return ""
            
        except Exception as e:
            logging.error(f"Error extracting price: {e}")
            return ""

# --- Google Sheets接続クラス ---
class GSheetConnector:
    def __init__(self, config: Dict):
        self.config = config['google_sheets']
        self.worksheet = self._connect()

    def _connect(self):
        logging.info("Connecting to Google Sheets...")
        try:
            creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if not creds_path:
                raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

            scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
            client = gspread.authorize(creds)
            spreadsheet = client.open_by_key(self.config['spreadsheet_id'])
            worksheet = spreadsheet.worksheet(self.config['sheet_name'])
            logging.info("✅ Successfully connected to Google Sheets.")
            return worksheet
        except Exception as e:
            logging.critical(f"❌ Google Sheets connection error: {e}")
            return None

    def get_existing_ids(self) -> Set[str]:
        """既存のユニークIDを取得"""
        if not self.worksheet:
            return set()
        logging.info("Fetching existing deal IDs from the sheet...")
        try:
            all_data = self.worksheet.get_all_records()
            if all_data and Constants.FIELD_UNIQUE_ID in all_data[0]:
                ids = {row[Constants.FIELD_UNIQUE_ID] for row in all_data if row.get(Constants.FIELD_UNIQUE_ID)}
                logging.info(f"Found {len(ids)} existing IDs.")
                return ids
            logging.info("No existing data or 'unique_id' column found.")
            return set()
        except Exception as e:
            logging.error(f"Error fetching existing IDs: {e}")
            return set()

    def write_deals(self, new_deals: List[FormattedDealData]) -> None:
        """新しい案件データをスプレッドシートに書き込み"""
        if not self.worksheet or not new_deals:
            return
        logging.info(f"Writing {len(new_deals)} new deals to the spreadsheet...")
        try:
            all_values = self.worksheet.get_all_values()
            headers = [f.name for f in fields(FormattedDealData)]
            if not all_values:
                self.worksheet.append_row(headers, value_input_option='USER_ENTERED')
            existing_headers = self.worksheet.row_values(1) if all_values else headers
            rows_to_append = [[getattr(deal, key, '') for key in existing_headers] for deal in new_deals]
            if rows_to_append:
                self.worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
            logging.info(f"✅ Successfully appended {len(new_deals)} rows.")
        except Exception as e:
            logging.error(f"Error writing to spreadsheet: {e}")

# --- ユーティリティ関数 ---
def load_config(file_path: str = 'config.yaml') -> None:
    """設定ファイルの読み込み"""
    global CONFIG
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        if 'GOOGLE_SHEETS_ID' in os.environ:
            config['google_sheets']['spreadsheet_id'] = os.environ['GOOGLE_SHEETS_ID']
        CONFIG = config
    except Exception as e:
        print(f"❌ Config file read error: {e}")
        raise

def setup_logging(config: Dict) -> None:
    """ログ設定の初期化"""
    log_config = config.get('logging', {})
    logging.basicConfig(
        level=getattr(logging, log_config.get('level', 'INFO').upper()),
        format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
        handlers=[
            logging.FileHandler(log_config.get('file_name', 'speedma_scraping.log'), encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def retry_on_failure(max_retries: int = 3, delay: int = 1):
    """リトライデコレーター"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except httpx.RequestError as e:
                    logging.warning(f"Network error. Retrying {attempt + 1}/{max_retries}: {e}")
                    if attempt == max_retries - 1:
                        logging.error("Max retries reached.")
                        raise
                    time.sleep(delay * (attempt + 1))
            return None
        return wrapper
    return decorator

@retry_on_failure()
def fetch_html(url: str) -> Optional[str]:
    """HTMLコンテンツの取得"""
    timeout = CONFIG.get('scraping', {}).get('timeout', 15)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            return response.text
    except httpx.TimeoutException as e:
        raise httpx.RequestError(f"Timeout occurred: {e}")
    except httpx.HTTPStatusError as e:
        if e.response.status_code >= 500:
            raise httpx.RequestError(f"Server error {e.response.status_code}, retrying...")
        else:
            logging.error(f"HTTP client error (no retry): {e.response.status_code} for url {url}")
            return None
    except Exception as e:
        logging.error(f"Unexpected error fetching {url}: {e}")
        return None

def format_deal_data(raw_deals: List[RawDealData], existing_ids: Set[str]) -> List[FormattedDealData]:
    """生データを整形済みデータに変換し、条件チェックを行う（修正版）"""
    formatted_deals = []
    extraction_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 設定から閾値を取得
    min_revenue = CONFIG.get('speed_ma', {}).get('revenue_threshold', 300000000)  # 3億円
    min_profit = CONFIG.get('speed_ma', {}).get('profit_threshold', 30000000)     # 3,000万円
    
    for raw_deal in raw_deals:
        try:
            unique_id = hashlib.md5(f"{raw_deal.site_name}_{raw_deal.deal_id}".encode()).hexdigest()[:12]
            
            if unique_id in existing_ids:
                logging.info(f"    -> Skipping duplicate deal: {raw_deal.deal_id}")
                continue
            
            # 財務条件チェック
            revenue_value = SpeedMADataConverter.parse_financial_value(raw_deal.revenue_text)
            profit_value = SpeedMADataConverter.parse_financial_value(raw_deal.profit_text)
            
            logging.info(f"    -> Deal {raw_deal.deal_id}: Revenue={revenue_value:,}, Profit={profit_value:,}")
            
            # 売上高チェック（既に一覧ページでフィルタ済みだが再確認）
            if revenue_value > 0 and revenue_value < min_revenue:
                logging.info(f"    -> Skipping deal {raw_deal.deal_id}: Revenue {revenue_value:,} < {min_revenue:,}")
                continue
            
            # 営業利益チェック（データが取得できている場合のみ）
            if profit_value > 0 and profit_value < min_profit:
                logging.info(f"    -> Skipping deal {raw_deal.deal_id}: Profit {profit_value:,} < {min_profit:,}")
                continue
            
            # 営業利益データが取得できない場合は警告だけして通す
            if not raw_deal.profit_text or "**" in raw_deal.profit_text:
                logging.warning(f"    -> Deal {raw_deal.deal_id}: Profit data not available (may require login)")
            
            formatted_deal = FormattedDealData(
                extraction_time=extraction_time,
                site_name=raw_deal.site_name,
                deal_id=raw_deal.deal_id,
                title=raw_deal.title,
                location=raw_deal.location_text or "-",
                revenue=SpeedMADataConverter.format_to_million_yen(raw_deal.revenue_text),
                profit=SpeedMADataConverter.format_to_million_yen(raw_deal.profit_text),
                price=SpeedMADataConverter.format_to_million_yen(raw_deal.price_text),
                features=raw_deal.features_text or "-",
                link=raw_deal.link,
                unique_id=unique_id
            )
            
            formatted_deals.append(formatted_deal)
            logging.info(f"    -> Formatted deal: {raw_deal.deal_id} - {raw_deal.title[:50]}...")
            
        except Exception as e:
            logging.error(f"    -> Error formatting deal {raw_deal.deal_id}: {e}")
            continue
    
    return formatted_deals

def scrape_speed_ma() -> List[RawDealData]:
    """スピードM&Aのスクレイピングを実行（修正版）"""
    logging.info("🔍 Starting scraping for: スピードM&A")
    all_deals = []
    
    try:
        max_pages = CONFIG.get('speed_ma', {}).get('max_pages', 2)
        base_url = CONFIG.get('speed_ma', {}).get('projects_url', 'https://speed-ma.com/projects')
        
        for page_num in range(1, max_pages + 1):
            if page_num == 1:
                url = base_url
            else:
                url = f"{base_url}?p={page_num}"
            
            logging.info(f"  📄 Scraping page {page_num}: {url}")
            
            html_content = fetch_html(url)
            if not html_content:
                logging.error(f"  ❌ Failed to fetch page {page_num}")
                continue
            
            # 一覧ページをパース（売上高フィルタリング済み）
            deals = SpeedMAParser.parse_list_page(html_content)
            all_deals.extend(deals)
            
            # 1ページ目で案件が0件の場合は警告
            if page_num == 1 and len(deals) == 0:
                logging.critical(f"🚨 CRITICAL - スピードM&Aの1ページ目から案件が1件も見つかりませんでした。")
                logging.critical(f"   サイトのHTML構造が変更された可能性があります。")
            
            time.sleep(2)
    
    except Exception as e:
        logging.error(f"❌ Error scraping スピードM&A: {e}")
        logging.debug(traceback.format_exc())
    
    logging.info(f"🎯 Total deals found from スピードM&A (after revenue filtering): {len(all_deals)}")
    return all_deals

def enhance_deals_with_details(raw_deals: List[RawDealData]) -> List[RawDealData]:
    """詳細ページから情報を取得して既存データを拡張"""
    logging.info(f"🔗 Fetching details for {len(raw_deals)} deals from スピードM&A")
    enhanced_deals = []
    
    try:
        anti_blocking = AntiBlockingManager()
        with WebDriverManager(headless=CONFIG.get('debug', {}).get('headless_mode', True), anti_blocking=anti_blocking) as driver:
            scraper = SpeedMADetailScraper(driver, anti_blocking)
            
            for i, deal in enumerate(raw_deals, 1):
                try:
                    logging.info(f"  📖 Processing deal {i}/{len(raw_deals)}: {deal.deal_id}")
                    
                    # 403ブロックが検出されている場合は処理を停止
                    if anti_blocking.blocked_detected:
                        logging.warning(f"  🚫 Blocked state detected. Skipping remaining {len(raw_deals) - i + 1} deals.")
                        enhanced_deals.extend(raw_deals[i-1:])
                        break
                    
                    enhanced_deal = scraper.enhance_deal_with_details(deal)
                    enhanced_deals.append(enhanced_deal)
                    
                    # 人間らしい待機時間
                    delay = anti_blocking.get_human_like_delay(3, 6)
                    logging.info(f"    -> Waiting {delay:.1f} seconds before next request...")
                    time.sleep(delay)
                    
                except Exception as e:
                    logging.error(f"  ❌ Error processing deal {deal.deal_id}: {e}")
                    enhanced_deals.append(deal)
                    continue
    
    except Exception as e:
        logging.error(f"❌ Error initializing WebDriver: {e}")
        return raw_deals
    
    logging.info(f"✅ Enhanced {len(enhanced_deals)} deals with detail information")
    return enhanced_deals

def main():
    """メイン実行関数"""
    try:
        load_config()
        setup_logging(CONFIG)
        
        logging.info("🚀 Starting SpeedM&A deal scraping (FIXED VERSION)")
        logging.info(f"📊 Target criteria: Revenue ≥ {CONFIG.get('speed_ma', {}).get('revenue_threshold', 300000000):,} yen, Profit ≥ {CONFIG.get('speed_ma', {}).get('profit_threshold', 30000000):,} yen")
        
        sheet_connector = GSheetConnector(CONFIG)
        if not sheet_connector.worksheet:
            logging.critical("❌ Cannot proceed without Google Sheets connection")
            return
        
        existing_ids = sheet_connector.get_existing_ids()
        logging.info(f"📋 Found {len(existing_ids)} existing deals in spreadsheet")
        
        # スピードM&Aをスクレイピング（売上高フィルタリング済み）
        raw_deals = scrape_speed_ma()
        
        if not raw_deals:
            logging.warning("⚠️ スピードM&A: No deals extracted (after revenue filtering)")
            return
        
        # 詳細ページから情報を取得
        enhanced_deals = enhance_deals_with_details(raw_deals)
        
        # データをフォーマットし、最終条件でフィルタリング
        formatted_deals = format_deal_data(enhanced_deals, existing_ids)
        
        logging.info(f"✅ スピードM&A: {len(formatted_deals)} new deals after all filtering")
        
        if formatted_deals:
            sheet_connector.write_deals(formatted_deals)
            logging.info(f"🎉 Successfully added {len(formatted_deals)} new deals to spreadsheet")
        else:
            logging.warning("📝 No new deals found that meet all criteria")
        
        logging.info("✨ SpeedM&A scraping process completed successfully")
        
    except Exception as e:
        logging.critical(f"💥 Critical error in main process: {e}")
        logging.debug(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()