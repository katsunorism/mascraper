# main.py (完全版 - 修正済み)
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
    
    # 統一された日本語フィールドマッピング
    JAPANESE_TO_ENGLISH_FIELDS: Dict[str, str] = {
        "案件ID": FIELD_DEAL_ID,
        "案件番号": FIELD_DEAL_ID,
        "案件No": FIELD_DEAL_ID,
        "タイトル": FIELD_TITLE,
        "所在地": FIELD_LOCATION,
        "所在地域": FIELD_LOCATION,
        "エリア": FIELD_LOCATION,
        "地域": FIELD_LOCATION,
        "業種": FIELD_FEATURES,
        "業界": FIELD_FEATURES,
        "売上高": FIELD_REVENUE,
        "概算売上": FIELD_REVENUE,
        "営業利益": FIELD_PROFIT,
        "希望金額": FIELD_PRICE,
        "譲渡希望価格": FIELD_PRICE,
        "特色": FIELD_FEATURES,
        "事業内容": FIELD_FEATURES,
        "事業概要": FIELD_FEATURES,
        "概要": FIELD_FEATURES,
        "特徴・強み": FIELD_FEATURES,
        "リンク": FIELD_LINK,
    }

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

# --- 専門家クラス ---
class DataConverter:
    @staticmethod
    def parse_financial_value(text: str) -> int:
        """財務テキストを数値に変換"""
        if not text or any(keyword in text for keyword in ["非公開", "応相談", "赤字", "N/A", "希望なし", "黒字なし", "損益なし"]):
            return 0
        text = text.translate(str.maketrans('０１２３４５６７８９', '0123456789')).replace(',', '')
        target_text = re.split(r'[〜～-]', text)[-1]
        match = re.search(r'([\d\.]+)', target_text)
        if not match:
            return 0
        try:
            value = float(match.group(1))
        except ValueError:
            return 0
        multipliers = {'億': 100_000_000, '千万': 10_000_000, '百万': 1_000_000, '万': 10_000}
        for unit, multiplier in multipliers.items():
            if unit in text:
                value *= multiplier
                break
        return int(value)
    
    @staticmethod
    def format_financial_text(text: str) -> str:
        """財務テキストを百万円単位に統一フォーマット"""
        if not text or any(keyword in text for keyword in ["非公開", "応相談", "赤字", "N/A", "希望なし", "黒字なし", "損益なし"]):
            return text or "N/A"
        
        def _to_million_format(text_part: str) -> str:
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
            elif '百万円' in text_part:
                return text_part
            elif '万' in text_part and '百万' not in text_part:
                million_value = value / 100
            else:
                return text_part
            
            if million_value >= 1:
                return f"{int(million_value):,}百万円" if million_value == int(million_value) else f"{million_value:.1f}百万円"
            else:
                return text_part
        
        separator = "～" if "～" in text else "〜"
        if separator in text:
            parts = text.split(separator)
            if len(parts) == 2:
                return "～".join([_to_million_format(p.strip()) for p in parts])
        
        return _to_million_format(text)

    @staticmethod
    def convert_strike_revenue_to_million(revenue_text: str) -> str:
        """ストライクの売上高を百万円単位に変換"""
        if not revenue_text:
            return "-"
        
        conversion_map = {
            "5～10億円": "500～1,000百万円",
            "10～50億円": "1,000～5,000百万円", 
            "50～100億円": "5,000～10,000百万円",
            "100億円超": "10,000百万円超"
        }
        
        return conversion_map.get(revenue_text, revenue_text)

class AntiBlockingManager:
    """403ブロック対策を管理するクラス"""
    
    def __init__(self):
        self.blocked_detected = False
        self.retry_count = 0
        self.max_retries = 1
        
    def get_human_like_delay(self, base_min: int = 3, base_max: int = 8) -> float:
        """人間らしいランダム待機時間を生成"""
        return random.uniform(base_min, base_max)
    
    def get_recovery_delay(self) -> float:
        """ブロック後の回復待機時間を生成"""
        return random.uniform(15, 30)
    
    def is_blocked_response(self, html_content: str) -> bool:
        """403エラーページかどうかを判定"""
        if not html_content:
            return False
            
        blocked_indicators = [
            "403 ERROR",
            "The request could not be satisfied",
            "Request blocked",
            "cloudfront",
            "Access Denied",
            "Forbidden"
        ]
        
        # HTMLを小文字に変換して検索
        content_lower = html_content.lower()
        
        for indicator in blocked_indicators:
            if indicator.lower() in content_lower:
                return True
        
        # titleタグの確認
        soup = BeautifulSoup(html_content, 'lxml')
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text().lower()
            if 'error' in title_text or 'blocked' in title_text or 'denied' in title_text:
                return True
        
        return False

class WebDriverManager:
    """WebDriverの管理クラス（403対策強化版）"""
    def __init__(self, headless: bool = True, anti_blocking: AntiBlockingManager = None):
        self.headless = headless
        self.driver: Optional[webdriver.Chrome] = None
        self.anti_blocking = anti_blocking or AntiBlockingManager()

    def __enter__(self) -> webdriver.Chrome:
        logging.info("Initializing Selenium WebDriver with anti-blocking measures...")
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless")
        
        # より人間らしいブラウザ設定
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # User-Agentを設定
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        chrome_options.add_argument(f"--user-agent={user_agent}")
        
        try:
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            
            # WebDriverの自動化検出を回避
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logging.info("✅ WebDriver initialized successfully with anti-blocking measures.")
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

class DetailPageScraper:
    """詳細ページのスクレイピングを専門に行うクラス（403対策強化版）"""
    def __init__(self, driver: webdriver.Chrome, anti_blocking: AntiBlockingManager):
        self.driver = driver
        self.anti_blocking = anti_blocking

    def _format_features_text(self, raw_text_block: str) -> str:
        """特色テキストの整形処理"""
        text_with_header_breaks = re.sub(r'(?<!^)(【[^【】]+】)', r'\n\1', raw_text_block)
        
        # 行頭のマーカーのみを対象とし、文中の「・」は分割しない
        line_start_marker_pattern = r'(?<!^)(?<=。\s*)([・○◆✓●◉▼■])|(?<!^)(?<=、\s*)([・○◆✓●◉▼■])|(?<!^)(?<=\n\s*)([・○◆✓●◉▼■])'
        
        # より慎重な分割：文の終わりや明確な区切りの後のマーカーのみ分割
        if re.search(line_start_marker_pattern, text_with_header_breaks):
            text_with_all_breaks = re.sub(line_start_marker_pattern, r'\n\1\2\3', text_with_header_breaks)
            lines = text_with_all_breaks.splitlines()
        else:
            lines = text_with_header_breaks.splitlines()
        
        final_lines = []
        for line in lines:
            stripped_line = line.strip()
            if stripped_line:
                # マーカー後の不要なスペースを削除
                cleaned_line = re.sub(r'^([・○◆✓●◉▼■　☆★※▲▽])[\s　\t]+', r'\1', stripped_line)
                final_lines.append(cleaned_line)
        
        return "\n".join(final_lines)

    def fetch_features_with_blocking_protection(self, detail_url: str, selectors: Dict[str, Any], referer_url: str = None) -> str:
        """403ブロック対策付きの汎用的な特色抽出メソッド"""
        if not detail_url or detail_url == 'N/A':
            return "-"
        
        try:
            logging.info(f"    -> Accessing detail page: {detail_url}")
            
            # 人間らしい待機時間
            delay = self.anti_blocking.get_human_like_delay()
            logging.info(f"    -> Waiting {delay:.1f} seconds before access...")
            time.sleep(delay)
            
            # ページにアクセス
            self.driver.get(detail_url)
            time.sleep(2.5)  # ページ読み込み待機
            
            html_content = self.driver.page_source
            
            # 403ブロックの検出
            if self.anti_blocking.is_blocked_response(html_content):
                logging.warning(f"    -> 🚫 403 BLOCK DETECTED for URL: {detail_url}")
                
                if not self.anti_blocking.blocked_detected:
                    self.anti_blocking.blocked_detected = True
                    logging.warning("    -> First block detected. Attempting recovery...")
                    
                    # 回復待機時間
                    recovery_delay = self.anti_blocking.get_recovery_delay()
                    logging.info(f"    -> Recovery wait: {recovery_delay:.1f} seconds...")
                    time.sleep(recovery_delay)
                    
                    # リトライ
                    logging.info("    -> Retrying access...")
                    self.driver.get(detail_url)
                    time.sleep(3)
                    
                    retry_html = self.driver.page_source
                    
                    if self.anti_blocking.is_blocked_response(retry_html):
                        logging.error("    -> ❌ Still blocked after retry. Skipping this deal.")
                        return "-"
                    else:
                        logging.info("    -> ✅ Recovery successful!")
                        html_content = retry_html
                        self.anti_blocking.blocked_detected = False
                else:
                    logging.error("    -> ❌ Already in blocked state. Skipping this deal.")
                    return "-"
            
            detail_soup = BeautifulSoup(html_content, 'lxml')
            
            # M&A総合研究所の特別処理
            if 'masouken.com' in detail_url:
                return self._fetch_masouken_features(detail_soup)
            
            # ストライクの特別処理
            if 'strike.co.jp' in detail_url:
                return self._fetch_strike_features_enhanced(detail_soup, detail_url)
            
            # 標準的な特色抽出処理
            return self._fetch_standard_features(detail_soup, selectors)
            
        except WebDriverException as e:
            logging.error(f"    -> WebDriver error on detail page: {detail_url} - {e}")
            return "-"
        except Exception as e:
            logging.error(f"    -> Error parsing detail page: {e}")
            logging.debug(traceback.format_exc())
            return "-"

    def _fetch_strike_features_enhanced(self, detail_soup: BeautifulSoup, detail_url: str) -> str:
        """ストライク専用の特色抽出（完全修正版）"""
        
        # デバッグ用: HTMLファイル保存
        if CONFIG.get('debug', {}).get('save_html_files', False):
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            deal_id = detail_url.split('code=')[-1] if 'code=' in detail_url else 'unknown'
            debug_file = os.path.join("debug", f"debug_strike_detail_{deal_id}_{timestamp}.html")
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(str(detail_soup))
            logging.info(f"Debug: Strike detail HTML saved to {debug_file}")
        
        features_sections = []
        
        # アプローチ1: 標準的なul.detail__listを探す
        detail_list = detail_soup.find('ul', class_='detail__list')
        if detail_list:
            logging.info("    -> Found ul.detail__list using standard approach")
            
            # 事業概要の抽出
            business_overview = self._extract_strike_list_item_enhanced(detail_list, '事業概要')
            if business_overview:
                features_sections.append(f"【事業概要】\n{business_overview}")
                logging.info(f"    -> Found business overview: {business_overview[:50]}...")
            
            # 特徴・強みの抽出
            strengths = self._extract_strike_list_item_enhanced(detail_list, '特徴・強み')
            if strengths:
                features_sections.append(f"【特徴・強み】\n{strengths}")
                logging.info(f"    -> Found strengths: {strengths[:50]}...")
        else:
            logging.warning("    -> ul.detail__list not found, trying alternative approaches")
            
            # アプローチ2: 全体のテキストから抽出
            features_sections = self._extract_strike_features_from_text(detail_soup)
        
        if features_sections:
            result = "\n\n".join(features_sections)
            logging.info(f"    -> Successfully extracted features ({len(result)} chars)")
            return result
        else:
            logging.warning("    -> No features found with any approach")
            return "-"

    def _extract_strike_list_item_enhanced(self, detail_list: Tag, label_text: str) -> str:
        """ストライクのリスト項目からテキストを抽出（完全対応版）"""
        li_items = detail_list.find_all('li')
        
        for li in li_items:
            # アプローチ1: span.labelを探す
            label_span = li.find('span', class_='label')
            if label_span and label_text in label_span.get_text(strip=True):
                
                # 方法1: find_next_siblingでspanタグを直接探す
                value_span = label_span.find_next_sibling('span')
                if value_span:
                    # 【修正】brタグを改行に変換してからテキスト抽出
                    value_span_copy = value_span.__copy__() if hasattr(value_span, '__copy__') else value_span
                    for br in value_span_copy.find_all('br'):
                        br.replace_with('\n')
                    value_text = value_span_copy.get_text(strip=True)
                    if value_text and len(value_text) > 2:
                        # 【修正】改行を保持しながらマーカー処理
                        formatted_value = self._format_strike_text_with_linebreaks(value_text)
                        logging.info(f"    -> Found via next_sibling span: {formatted_value[:30]}...")
                        return formatted_value
                
                # 方法2: li要素内の全テキストから抽出
                # 【修正】brタグを改行に変換
                li_copy = li.__copy__() if hasattr(li, '__copy__') else li
                for br in li_copy.find_all('br'):
                    br.replace_with('\n')
                li_text = li_copy.get_text(separator='\n', strip=True)
                label_full_text = label_span.get_text(strip=True)
                if label_full_text in li_text:
                    remaining_text = li_text.replace(label_full_text, '', 1).strip()
                    if remaining_text and len(remaining_text) > 2:
                        # 【修正】改行を保持しながらマーカー処理
                        formatted_remaining = self._format_strike_text_with_linebreaks(remaining_text)
                        logging.info(f"    -> Found via text extraction: {formatted_remaining[:30]}...")
                        return formatted_remaining
                
                # 方法3: 複数の兄弟要素を順次チェック
                current_sibling = label_span.next_sibling
                collected_text = []
                for attempt in range(5):
                    if current_sibling is None:
                        break
                    
                    if isinstance(current_sibling, str):
                        text_content = current_sibling.strip()
                        if text_content and len(text_content) > 2:
                            collected_text.append(text_content)
                    elif hasattr(current_sibling, 'get_text'):
                        # 【修正】brタグを改行に変換
                        sibling_copy = current_sibling.__copy__() if hasattr(current_sibling, '__copy__') else current_sibling
                        for br in sibling_copy.find_all('br'):
                            br.replace_with('\n')
                        tag_content = sibling_copy.get_text(strip=True)
                        if tag_content and len(tag_content) > 2:
                            collected_text.append(tag_content)
                    
                    current_sibling = current_sibling.next_sibling
                
                if collected_text:
                    combined_text = '\n'.join(collected_text)
                    formatted_combined = self._format_strike_text_with_linebreaks(combined_text)
                    logging.info(f"    -> Found via sibling collection: {formatted_combined[:30]}...")
                    return formatted_combined
            
            # アプローチ2: ラベルが直接テキストに含まれている場合
            # 【修正】brタグを改行に変換
            li_copy = li.__copy__() if hasattr(li, '__copy__') else li
            for br in li_copy.find_all('br'):
                br.replace_with('\n')
            li_text = li_copy.get_text(strip=True)
            if label_text in li_text:
                parts = li_text.split(label_text, 1)
                if len(parts) == 2:
                    remaining = parts[1].strip()
                    remaining = re.sub(r'^[：:\s　]+', '', remaining)
                    if remaining and len(remaining) > 2:
                        formatted_remaining = self._format_strike_text_with_linebreaks(remaining)
                        logging.info(f"    -> Found via text split: {formatted_remaining[:30]}...")
                        return formatted_remaining
        
        logging.warning(f"    -> No content found for label: {label_text}")
        return ""

    def _format_strike_text_with_linebreaks(self, text: str) -> str:
        """ストライクのテキストを改行を保持しながら整形"""
        if not text:
            return ""
        
        lines = []
        
        # 既存の改行で分割
        raw_lines = text.split('\n')
        
        for line in raw_lines:
            line = line.strip()
            if not line:
                continue
            
            # 行頭のマーカーがある場合の処理
            line_start_markers = ['■', '●', '◆', '○', '▼', '◎']  # ←ここに「■」を追加
            has_line_start_marker = any(line.startswith(marker) for marker in line_start_markers)
            
            if has_line_start_marker:
                # 行頭にマーカーがある場合はそのまま追加（マーカー後の空白は削除）
                cleaned_line = re.sub(r'^([■●◆○▼◎])[\s　\t]+', r'\1', line)  # ←ここにも「■」を追加
                lines.append(cleaned_line)
            else:
                # 行頭にマーカーがない場合は、文中の「・」では分割しない
                # ただし、明らかに箇条書きと思われる場合（「・」の後に大文字や改行がある場合）のみ分割
                bullet_pattern = r'([・])(?=\s*[A-Z一-龯])'  # 「・」の後に文字が続く場合のみ
                
                # 文中の「・」（例：「企画・開発」）は分割しない
                # 箇条書きの「・」（例：「・関西地方を中心に」）のみ分割
                if '・' in line:
                    # 「・」が行頭にある、または「・」の前後に十分な文脈がある場合のみ分割
                    parts = re.split(r'(?<=。)\s*・|(?<=、)\s*・|^\s*・', line)
                    
                    if len(parts) > 1:
                        # 分割された場合
                        for i, part in enumerate(parts):
                            part = part.strip()
                            if part:
                                if i == 0 and not part.startswith('・'):
                                    lines.append(part)
                                else:
                                    if not part.startswith('・'):
                                        part = f'・{part}'
                                    # マーカー後の空白を削除
                                    cleaned_part = re.sub(r'^([・])[\s　\t]+', r'\1', part)
                                    lines.append(cleaned_part)
                    else:
                        # 分割されなかった場合（文中の「・」）はそのまま追加
                        lines.append(line)
                else:
                    # 「・」がない場合は「■」での分割処理を追加
                    if '■' in line and not line.startswith('■'):
                        # 「■」で分割（文末の「。」の後の「■」のみ）
                        parts = re.split(r'(?<=。)\s*■', line)
                        
                        if len(parts) > 1:
                            # 最初の部分をそのまま追加
                            if parts[0].strip():
                                lines.append(parts[0].strip())
                            
                            # 残りの部分に「■」を付けて追加
                            for part in parts[1:]:
                                part = part.strip()
                                if part:
                                    cleaned_part = re.sub(r'^[\s　\t]+', '', part)
                                    lines.append(f'■{cleaned_part}')
                        else:
                            lines.append(line)
                    else:
                        # 「■」がない場合、または行頭が「■」の場合はそのまま追加
                        lines.append(line)
        
        # 重複除去と最終クリーンアップ
        final_lines = []
        seen = set()
        
        for line in lines:
            if line and len(line) > 2 and line not in seen:
                final_lines.append(line)
                seen.add(line)
        
        return '\n'.join(final_lines)

    def _extract_strike_features_from_text(self, detail_soup: BeautifulSoup) -> List[str]:
        """ストライクの特色をテキスト全体から抽出（フォールバック）"""
        features_sections = []
        full_text = detail_soup.get_text()
        
        # 事業概要の抽出パターン
        business_patterns = [
            r'事業概要[：:\s]*([^\n]{20,200})',
            r'【事業概要】([^【]{20,200})',
            r'事業内容[：:\s]*([^\n]{20,200})',
        ]
        
        for pattern in business_patterns:
            matches = re.findall(pattern, full_text, re.DOTALL)
            for match in matches:
                clean_match = match.strip()
                if len(clean_match) > 20:
                    features_sections.append(f"【事業概要】\n{clean_match}")
                    break
            if features_sections:
                break
        
        # 特徴・強みの抽出パターン
        strength_patterns = [
            r'特徴・強み[：:\s]*([^\n]{20,200})',
            r'【特徴・強み】([^【]{20,200})',
            r'強み[：:\s]*([^\n]{20,200})',
        ]
        
        for pattern in strength_patterns:
            matches = re.findall(pattern, full_text, re.DOTALL)
            for match in matches:
                clean_match = match.strip()
                if len(clean_match) > 20:
                    features_sections.append(f"【特徴・強み】\n{clean_match}")
                    break
            if len(features_sections) == 2:  # 既に事業概要もある場合
                break
        
        return features_sections

    def _fetch_masouken_features(self, detail_soup: BeautifulSoup) -> str:
        """M&A総合研究所専用の事業詳細と強み抽出"""
        features_sections = []
        
        business_details = self._extract_masouken_business_details(detail_soup)
        if business_details:
            features_sections.append(f"【事業詳細】\n{business_details}")
        
        strengths = self._extract_masouken_strengths(detail_soup)
        if strengths:
            features_sections.append(f"【強み・差別化ポイント】\n{strengths}")
        
        if features_sections:
            return "\n\n".join(features_sections)
        else:
            return "-"

    def _extract_masouken_business_details(self, detail_soup: BeautifulSoup) -> str:
        """M&A総合研究所の事業詳細抽出"""
        business_keywords = ['事業詳細', '事業内容', '事業概要', '概要', 'ビジネスモデル']
        
        for keyword in business_keywords:
            dt_elements = detail_soup.find_all('dt', string=re.compile(keyword))
            for dt in dt_elements:
                dd = dt.find_next_sibling('dd')
                if dd:
                    content = dd.get_text(strip=True)
                    if len(content) > 20:
                        return self._format_masouken_text(content)
        
        for keyword in business_keywords:
            headers = detail_soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5'], 
                                         string=re.compile(keyword))
            
            for header in headers:
                content_elements = []
                current = header
                
                for _ in range(10):
                    current = current.find_next_sibling()
                    if not current or not isinstance(current, Tag):
                        break
                    
                    if current.name in ['h1', 'h2', 'h3', 'h4', 'h5'] and current.get_text().strip():
                        break
                    
                    text = current.get_text(strip=True)
                    if len(text) > 15:
                        content_elements.append(current)
                
                if content_elements:
                    result = self._format_masouken_elements(content_elements)
                    if result and len(result) > 20:
                        return result
        
        return ""

    def _extract_masouken_strengths(self, detail_soup: BeautifulSoup) -> str:
        """M&A総合研究所の強み・差別化ポイント抽出"""
        strength_keywords = ['強み・差別化ポイント', '強み', '差別化ポイント', '特徴', '競合優位性', '優位性']
        
        for keyword in strength_keywords:
            dt_elements = detail_soup.find_all('dt', string=re.compile(keyword))
            for dt in dt_elements:
                dd = dt.find_next_sibling('dd')
                if dd:
                    content = dd.get_text(strip=True)
                    if len(content) > 20:
                        return self._format_masouken_text(content)
        
        for keyword in strength_keywords:
            headers = detail_soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5'], 
                                         string=re.compile(keyword))
            
            for header in headers:
                content_elements = []
                current = header
                
                for _ in range(10):
                    current = current.find_next_sibling()
                    if not current or not isinstance(current, Tag):
                        break
                    
                    if current.name in ['h1', 'h2', 'h3', 'h4', 'h5'] and current.get_text().strip():
                        break
                    
                    text = current.get_text(strip=True)
                    if len(text) > 15:
                        content_elements.append(current)
                
                if content_elements:
                    result = self._format_masouken_elements(content_elements)
                    if result and len(result) > 20:
                        return result
        
        return ""

    def _fetch_standard_features(self, detail_soup: BeautifulSoup, selectors: Dict[str, Any]) -> str:
        """標準的な特色抽出処理"""
        selector_config = selectors.get('features')
        if not selector_config:
            return "-"
        
        start_keywords = selector_config.get('start_keywords', [])
        end_tag = selector_config.get('end_tag')
        target_tags = selector_config.get('target_tags', [])
        
        start_element = None
        for keyword in start_keywords:
            found = detail_soup.find(['h1', 'h2', 'h3', 'h4', 'h5', 'dt'], string=re.compile(keyword))
            if found:
                start_element = found
                break
        
        if not start_element:
            return "-"
        
        collected_elements = []
        for sibling in start_element.find_next_siblings():
            if not isinstance(sibling, Tag):
                continue
            if sibling.name == end_tag:
                break
            if sibling.name in target_tags:
                collected_elements.append(sibling)
            else:
                child_tags = sibling.find_all(target_tags)
                if child_tags:
                    collected_elements.extend(child_tags)
        
        if not collected_elements:
            return "-"
        
        raw_text_block = "\n".join([elem.get_text(strip=True) for elem in collected_elements if elem.get_text(strip=True)])
        return self._format_features_text(raw_text_block) if raw_text_block else "-"

    def _format_masouken_elements(self, elements: List[Tag]) -> str:
        """M&A総合研究所の要素を整形"""
        formatted_items = []
        
        for element in elements:
            text = element.get_text(strip=True)
            
            if element.name in ['ul', 'ol']:
                li_items = element.find_all('li')
                for li in li_items:
                    item_text = li.get_text(strip=True)
                    if len(item_text) > 10:
                        # 【修正】マーカー後の不要なスペースを削除してから追加
                        cleaned_item = re.sub(r'([・○◆✓●◉▼■◎])[\s　\t]+', r'\1', item_text)
                        formatted_items.append(f"・{cleaned_item}")
            elif any(marker in text for marker in ['・', '◆', '▼', '○', '●']):
                for marker in ['・', '◆', '▼', '○', '●']:
                    if marker in text:
                        parts = text.split(marker)
                        for part in parts[1:]:
                            part = part.strip()
                            if len(part) > 10:
                                # 【修正】マーカー後の不要なスペースを削除し、改行で分割
                                cleaned_part = re.sub(r'^[\s　]+', '', part)
                                # 文の区切りで改行を追加
                                sentences = re.split(r'(?<=[。．])\s*(?=[・○◆✓●◉▼■◎])|(?<=[。．])\s*(?=\S)', cleaned_part)
                                for sentence in sentences:
                                    sentence = sentence.strip()
                                    if len(sentence) > 10:
                                        formatted_items.append(f"・{sentence}")
                        break
            elif len(text) > 15:
                # 【修正】文章を自然な区切りで分割
                # まず、マーカーがある場合はそれで分割
                if any(marker in text for marker in ['・', '○', '◆', '✓', '●', '◉', '▼', '■', '◎']):
                    marker_pattern = r'([・○◆✓●◉▼■◎])'
                    parts = re.split(marker_pattern, text)
                    current_item = ""
                    for i, part in enumerate(parts):
                        if part in ['・', '○', '◆', '✓', '●', '◉', '▼', '■', '◎']:
                            if current_item.strip() and len(current_item.strip()) > 10:
                                formatted_items.append(f"・{current_item.strip()}")
                            current_item = ""
                        else:
                            current_item += part
                    if current_item.strip() and len(current_item.strip()) > 10:
                        formatted_items.append(f"・{current_item.strip()}")
                else:
                    # マーカーがない場合は句点で分割
                    sentences = re.split(r'[。．]', text)
                    for sentence in sentences:
                        sentence = sentence.strip()
                        if len(sentence) > 15:
                            if not sentence.endswith('。'):
                                sentence += '。'
                            cleaned_sentence = re.sub(r'^[\s　]+', '', sentence)
                            formatted_items.append(f"・{cleaned_sentence}")
        
        return "\n".join(formatted_items[:8])  # 最大8項目に増加

    def _format_masouken_text(self, text: str) -> str:
        """M&A総合研究所のテキストを整形"""
        if not text or len(text) < 20:
            return ""
        
        cleaned_text = text.strip()
        
        # 【修正】マーカーがある場合は改行を保持
        if any(marker in cleaned_text for marker in ['・', '◆', '▼', '○', '●']):
            lines = []
            # まず改行で分割
            text_lines = cleaned_text.split('\n')
            
            for line in text_lines:
                line = line.strip()
                if not line:
                    continue
                    
                # マーカーで分割しつつ改行も考慮
                found_marker = False
                for marker in ['・', '◆', '▼', '○', '●']:
                    if marker in line:
                        found_marker = True
                        # マーカーで分割
                        parts = line.split(marker)
                        for part in parts[1:]:  # 最初の空の部分をスキップ
                            part = part.strip()
                            if len(part) > 10:
                                cleaned_part = re.sub(r'^[\s　]+', '', part)
                                lines.append(f"・{cleaned_part}")
                        break
                
                if not found_marker and len(line) > 10:
                    lines.append(f"・{line}")
            
            if lines:
                return '\n'.join(lines[:5])  # 最大5項目
        
        # 長すぎる場合は切り詰め
        if len(cleaned_text) > 500:
            sentences = re.split(r'[。．]', cleaned_text)
            truncated_sentences = []
            current_length = 0
            
            for sentence in sentences:
                if current_length + len(sentence) > 500:
                    break
                truncated_sentences.append(sentence.strip())
                current_length += len(sentence)
            
            cleaned_text = '。'.join([s for s in truncated_sentences if s])
            if cleaned_text and not cleaned_text.endswith('。'):
                cleaned_text += '。'
        
        # マーカーがない場合は文で分割
        sentences = re.split(r'[。．]', cleaned_text)
        bullet_points = []
        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) > 10:
                bullet_points.append(f"・{sentence}。")
        
        return "\n".join(bullet_points[:3])

class UniversalParser:
    """統一されたパーサークラス"""
    
    @staticmethod
    def parse_list_page(site_config: Dict[str, Any], html_content: str) -> List[RawDealData]:
        """汎用的な一覧ページパーサー"""
        site_name = site_config['name']
        parser_type = site_config.get('parser_type', 'standard')
        
        # M&A総合研究所の特別処理
        if parser_type == 'text_based' or site_name == "M&A総合研究所":
            return UniversalParser._parse_masouken_text_based(site_config, html_content)
        
        # M&Aキャピタルパートナーズの特別処理
        if site_name == "M&Aキャピタルパートナーズ":
            return UniversalParser._parse_ma_capital_partners(site_config, html_content)
        
        # ストライクの特別処理
        if site_name == "ストライク":
            return UniversalParser._parse_strike(site_config, html_content)
        
        # 標準的なHTMLセレクター処理
        return UniversalParser._parse_selector_based(site_config, html_content)
    
    @staticmethod
    def _parse_strike(site_config: Dict[str, Any], html_content: str) -> List[RawDealData]:
        """ストライク専用パーサー（動的読み込み対応版）"""
        soup = BeautifulSoup(html_content, 'lxml')
        results = []
        
        # デバッグ用: HTMLファイル保存
        if CONFIG.get('debug', {}).get('save_html_files', False):
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            debug_file = os.path.join("debug", f"debug_strike_{timestamp}.html")
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logging.info(f"Debug: HTML saved to {debug_file}")
        
        # 案件アイテムを抽出（より柔軟なセレクター）
        selectors_to_try = [
            'div.search-result__item',
            'div[class*="search-result"]',
            'div[class*="item"]',
            'div[class*="case"]',
            'article',
            'li'
        ]
        
        items = []
        for selector in selectors_to_try:
            items = soup.select(selector)
            if items:
                logging.info(f"Found {len(items)} items using selector: {selector}")
                break
        
        if not items:
            logging.warning("No items found with any selector")
            return []
        
        # 上から52件を対象（14行×3案件/行）
        items_to_process = items[:52]
        
        for i, item in enumerate(items_to_process):
            try:
                logging.info(f"Processing item {i+1}/{len(items_to_process)}")
                
                # 案件番号の抽出（より柔軟に）
                deal_id = UniversalParser._extract_strike_deal_id_flexible(item)
                if not deal_id:
                    logging.info(f"No deal ID found in item {i+1}, skipping")
                    continue
                
                logging.info(f"Found deal ID: {deal_id}")
                
                # 売上高の抽出とフィルタリング
                revenue_text = UniversalParser._extract_strike_revenue_flexible(item)
                
                # 売上高フィルタリング：指定された4パターンのみ詳細ページに進む
                valid_revenues = ["5～10億円", "10～50億円", "50～100億円", "100億円超"]
                if revenue_text not in valid_revenues:
                    logging.info(f"Skipping deal {deal_id}: Revenue '{revenue_text}' doesn't meet criteria")
                    continue
                
                logging.info(f"Revenue meets criteria: {revenue_text}")
                
                # タイトルの抽出（詳細ページで取得するため、ここでは仮のタイトル）
                title = f"ストライク案件_{deal_id}"
                
                # 詳細ページのリンクを構築
                link = f"https://www.strike.co.jp/smart/sell_details.html?code={deal_id}"
                
                # データ作成
                deal_data = RawDealData(
                    site_name=site_config['name'],
                    deal_id=deal_id,
                    title=title,
                    link=link,
                    location_text="",
                    revenue_text=revenue_text,
                    profit_text="-",
                    price_text="-",
                    features_text=""
                )
                
                results.append(deal_data)
                logging.info(f"Successfully extracted deal: {deal_id}")
                
            except Exception as e:
                logging.error(f"Error parsing ストライク item {i+1}: {e}")
                continue
        
        logging.info(f"ストライク: Successfully extracted {len(results)} deals meeting criteria")
        return results

    @staticmethod
    def _extract_strike_deal_id_flexible(item: Tag) -> str:
        """ストライクの案件IDを柔軟に抽出"""
        # アプローチ1: 標準的なセレクター
        selectors_to_try = [
            'p.search-result__num',
            '.search-result__num',
            '[class*="num"]',
            '[class*="id"]'
        ]
        
        for selector in selectors_to_try:
            num_element = item.select_one(selector)
            if num_element:
                text = num_element.get_text(strip=True)
                ss_match = re.search(r'(SS\d+)', text)
                if ss_match:
                    return ss_match.group(1)
        
        # アプローチ2: テキスト全体からSS番号を検索
        item_text = item.get_text()
        ss_match = re.search(r'(SS\d+)', item_text)
        if ss_match:
            return ss_match.group(1)
        
        return ""

    @staticmethod
    def _extract_strike_revenue_flexible(item: Tag) -> str:
        """ストライクの売上高を柔軟に抽出"""
        # アプローチ1: 標準的なセレクター
        selectors_to_try = [
            'span.sales-amount',
            '.sales-amount',
            '[class*="sales"]',
            '[class*="amount"]'
        ]
        
        for selector in selectors_to_try:
            sales_element = item.select_one(selector)
            if sales_element:
                return sales_element.get_text(strip=True)
        
        # アプローチ2: テキスト全体から売上高パターンを検索
        item_text = item.get_text()
        revenue_patterns = [
            r'5～10億円', r'5〜10億円',
            r'10～50億円', r'10〜50億円',
            r'50～100億円', r'50〜100億円',
            r'100億円超'
        ]
        
        for pattern in revenue_patterns:
            if re.search(pattern, item_text):
                # 正規化して返す
                normalized = pattern.replace('〜', '～')
                return normalized
        
        return ""
    
    @staticmethod
    def _parse_ma_capital_partners(site_config: Dict[str, Any], html_content: str) -> List[RawDealData]:
        """M&Aキャピタルパートナーズ専用パーサー（柔軟性向上版）"""
        soup = BeautifulSoup(html_content, 'lxml')
        results = []
        
        # デバッグ用: HTMLファイル保存
        if CONFIG.get('debug', {}).get('save_html_files', False):
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            debug_file = os.path.join("debug", f"debug_ma_capital_{timestamp}.html")
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logging.info(f"Debug: HTML saved to {debug_file}")
        
        # 案件リストを抽出（より柔軟なセレクター）
        selectors_to_try = [
            'article.c-filter-project',
            'article[class*="project"]',
            'div[class*="project"]',
            'div[class*="case"]',
            'article',
            'li[class*="item"]'
        ]
        
        items = []
        for selector in selectors_to_try:
            items = soup.select(selector)
            if items:
                logging.info(f"Found {len(items)} items using selector: {selector}")
                break
        
        if not items:
            logging.warning("No items found with any selector")
            return []
        
        for i, item in enumerate(items):
            try:
                logging.info(f"Processing item {i+1}/{len(items)}")
                
                # 案件番号の抽出（より柔軟に）
                deal_id = UniversalParser._extract_ma_capital_deal_id_flexible(item)
                if not deal_id:
                    logging.info(f"No deal number found in item {i+1}, skipping")
                    continue
                
                logging.info(f"Found deal ID: {deal_id}")
                
                # タイトルの抽出
                title = UniversalParser._extract_ma_capital_title_flexible(item, deal_id)
                
                # リンクの抽出
                link = UniversalParser._extract_ma_capital_link_flexible(item, deal_id)
                
                # 財務情報の抽出
                revenue_text = UniversalParser._extract_ma_capital_dl_data_flexible(item, ['概算売上', '売上高', '売上'])
                profit_text = UniversalParser._extract_ma_capital_dl_data_flexible(item, ['営業利益', '利益'])
                location_text = UniversalParser._extract_ma_capital_dl_data_flexible(item, ['所在地', 'エリア', '地域'])
                price_text = UniversalParser._extract_ma_capital_dl_data_flexible(item, ['希望金額', '譲渡希望価格', '価格'])
                
                logging.info(f"Revenue: {revenue_text}, Profit: {profit_text}")
                
                # 財務条件チェック
                revenue_value = DataConverter.parse_financial_value(revenue_text)
                profit_value = DataConverter.parse_financial_value(profit_text)
                
                min_revenue = CONFIG.get('scraping', {}).get('min_revenue', 300000000)
                min_profit = CONFIG.get('scraping', {}).get('min_profit', 30000000)
                
                if revenue_value < min_revenue or profit_value < min_profit:
                    logging.info(f"Skipping deal {deal_id}: doesn't meet financial criteria")
                    continue
                
                # 事業内容の抽出
                features_text = UniversalParser._extract_ma_capital_business_content_flexible(item)
                
                # データ作成
                deal_data = RawDealData(
                    site_name=site_config['name'],
                    deal_id=deal_id,
                    title=title,
                    link=link,
                    location_text=location_text,
                    revenue_text=revenue_text,
                    profit_text=profit_text,
                    price_text=price_text,
                    features_text=features_text
                )
                
                results.append(deal_data)
                logging.info(f"Successfully extracted deal: {deal_id} - {title[:50]}")
                
            except Exception as e:
                logging.error(f"Error parsing M&Aキャピタルパートナーズ item {i+1}: {e}")
                continue
        
        logging.info(f"M&Aキャピタルパートナーズ: Successfully extracted {len(results)} deals")
        return results
    
    @staticmethod
    def _extract_ma_capital_deal_id_flexible(item: Tag) -> str:
        """M&Aキャピタルパートナーズの案件IDを柔軟に抽出"""
        # アプローチ1: 標準的なセレクター
        selectors_to_try = [
            '.c-filter-project__no',
            '[class*="no"]',
            '[class*="id"]',
            '[class*="num"]'
        ]
        
        for selector in selectors_to_try:
            deal_no_element = item.select_one(selector)
            if deal_no_element:
                deal_no_text = deal_no_element.get_text(strip=True)
                deal_match = re.search(r'案件No[：:\s]*([A-Z0-9-]+)', deal_no_text)
                if deal_match:
                    return deal_match.group(1)
        
        # アプローチ2: テキスト全体から案件番号を検索
        item_text = item.get_text()
        patterns = [
            r'案件No[：:\s]*([A-Z0-9-]+)',
            r'案件番号[：:\s]*([A-Z0-9-]+)',
            r'No[：:\s]*([A-Z0-9-]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, item_text)
            if match:
                return match.group(1)
        
        return ""

    @staticmethod
    def _extract_ma_capital_title_flexible(item: Tag, deal_id: str) -> str:
        """M&Aキャピタルパートナーズのタイトルを柔軟に抽出"""
        selectors_to_try = [
            '.c-filter-project__ttl',
            '[class*="ttl"]',
            '[class*="title"]',
            'h1', 'h2', 'h3'
        ]
        
        for selector in selectors_to_try:
            title_element = item.select_one(selector)
            if title_element:
                title_text = title_element.get_text(strip=True)
                if title_text and len(title_text) > 5:
                    return title_text
        
        return f"M&A案件_{deal_id}"

    @staticmethod
    def _extract_ma_capital_link_flexible(item: Tag, deal_id: str) -> str:
        """M&Aキャピタルパートナーズのリンクを柔軟に抽出"""
        selectors_to_try = [
            'a.c-cta.c-button--arrow',
            'a[class*="cta"]',
            'a[class*="button"]',
            'a[href*="deal"]',
            'a'
        ]
        
        for selector in selectors_to_try:
            link_element = item.select_one(selector)
            if link_element:
                href = link_element.get('href', '')
                if href:
                    link = href if href.startswith('http') else f"https://www.ma-cp.com{href}"
                    return link
        
        return f"https://www.ma-cp.com/deal/{deal_id}/"

    @staticmethod
    def _extract_ma_capital_dl_data_flexible(item: Tag, field_names: List[str]) -> str:
        """M&Aキャピタルパートナーズのdl要素からデータを柔軟に抽出"""
        # アプローチ1: 標準的なdl構造
        dl_elements = item.select('dl.c-filter-project__dataList')
        for dl in dl_elements:
            dt = dl.select_one('dt')
            dd = dl.select_one('dd')
            
            if dt and dd:
                dt_text = dt.get_text(strip=True)
                for field_name in field_names:
                    if field_name in dt_text:
                        return dd.get_text(strip=True)
        
        # アプローチ2: 全てのdl要素を検索
        all_dl_elements = item.select('dl')
        for dl in all_dl_elements:
            dt = dl.select_one('dt')
            dd = dl.select_one('dd')
            
            if dt and dd:
                dt_text = dt.get_text(strip=True)
                for field_name in field_names:
                    if field_name in dt_text:
                        return dd.get_text(strip=True)
        
        # アプローチ3: テキスト全体から抽出
        item_text = item.get_text()
        for field_name in field_names:
            pattern = rf'{field_name}[：:\s]*([^\n]+)'
            match = re.search(pattern, item_text)
            if match:
                return match.group(1).strip()
        
        return ""

    @staticmethod
    def _extract_ma_capital_business_content_flexible(item: Tag) -> str:
        """M&Aキャピタルパートナーズの事業内容を柔軟に抽出"""
        features_sections = []
        
        # アプローチ1: 標準的なブロック構造
        selectors_to_try = [
            'div.c-filter-project__listBlock',
            'div[class*="listBlock"]',
            'div[class*="content"]',
            'div[class*="detail"]'
        ]
        
        business_blocks = []
        for selector in selectors_to_try:
            business_blocks = item.select(selector)
            if business_blocks:
                break
        
        for block in business_blocks:
            # ラベルを確認
            label_selectors = [
                'h5.c-filter-project__label',
                'h5[class*="label"]',
                'h4', 'h5', 'h6',
                '[class*="label"]'
            ]
            
            label_element = None
            for label_selector in label_selectors:
                label_element = block.select_one(label_selector)
                if label_element:
                    break
            
            if not label_element:
                continue
            
            label_text = label_element.get_text(strip=True)
            
            # 事業内容、事業概要、特徴などのセクションを処理
            if any(keyword in label_text for keyword in ['事業内容', '事業概要', '特徴', '概要']):
                # コンテンツを抽出
                content_selectors = [
                    'div.c-filter-project__lists',
                    'div[class*="lists"]',
                    'div[class*="content"]',
                    'p', 'div'
                ]
                
                lists_element = None
                for content_selector in content_selectors:
                    lists_element = block.select_one(content_selector)
                    if lists_element:
                        break
                
                if lists_element:
                    # HTMLの<br>タグを改行に変換
                    html_content = str(lists_element)
                    content_with_breaks = re.sub(r'<br\s*/?>', '\n', html_content)
                    clean_soup = BeautifulSoup(content_with_breaks, 'html.parser')
                    raw_content = clean_soup.get_text()
                    
                    if raw_content:
                        # 最終クレンジング処理を適用
                        cleaned_content = UniversalParser._clean_extracted_text_flexible(raw_content, label_text)
                        
                        if cleaned_content:
                            features_sections.append(cleaned_content)
        
        return '\n\n'.join(features_sections) if features_sections else ""

    @staticmethod
    def _clean_extracted_text_flexible(raw_text: str, label_text: str) -> str:
        """抽出後テキストの柔軟なクレンジング処理"""
        if not raw_text:
            return ""
        
        # 1. 全体の先頭と末尾の空白を完全除去
        cleaned_text = raw_text.strip()
        
        # 2. 各行の先頭と末尾の空白を除去し、空行を除去
        lines = []
        for line in cleaned_text.split('\n'):
            stripped_line = line.strip()
            if stripped_line:  # 空行は除外
                lines.append(stripped_line)
        
        if not lines:
            return ""
        
        # 3. 無関係なデータのフィルタリング（より柔軟に）
        filtered_lines = []
        
        for line in lines:
            # 明らかに無関係なデータを除外
            unwanted_patterns = [
                r'所在地[：:]', r'業種[：:]', r'従業員数[：:]', r'設立[：:]', r'資本金[：:]',
                r'売上高[：:]', r'営業利益[：:]', r'希望金額[：:]', r'案件No[：:]'
            ]
            
            skip_line = False
            for pattern in unwanted_patterns:
                if re.search(pattern, line):
                    skip_line = True
                    break
            
            if not skip_line:
                filtered_lines.append(line)
        
        # 4. サブセクション（【特徴】など）の処理
        final_lines = []
        has_subsections = any('【' in line and '】' in line for line in filtered_lines)
        
        if has_subsections:
            # サブセクションがある場合はそのまま保持
            final_lines = filtered_lines
        else:
            # サブセクションがない場合はラベルを付与
            if label_text and filtered_lines:
                final_lines = [f"【{label_text}】"] + filtered_lines
        
        # 5. マーカー後の空白を完全削除
        cleaned_final_lines = []
        for line in final_lines:
            # マーカー後の全ての空白文字（スペース、タブ、全角スペース）を削除
            cleaned_line = re.sub(r'([◎○◆✓●◉▼■・▲▽☆★※])\s+', r'\1', line)
            cleaned_final_lines.append(cleaned_line)
        
        # 6. 最終的な整形
        if cleaned_final_lines:
            result = '\n'.join(cleaned_final_lines)
            
            # 連続する改行を単一化（3つ以上の改行を2つに）
            result = re.sub(r'\n{3,}', '\n\n', result)
            
            # 最終的な前後の空白除去
            result = result.strip()
            
            return result
        
        return ""
        
    @staticmethod
    def _parse_selector_based(site_config: Dict[str, Any], html_content: str) -> List[RawDealData]:
        """セレクターベースの標準パーサー（柔軟性向上版）"""
        soup = BeautifulSoup(html_content, 'lxml')
        
        # より柔軟なアイテムセレクター
        item_selectors = [
            site_config.get('item_selector', ''),
            'article',
            'div[class*="item"]',
            'li[class*="item"]',
            'div[class*="case"]',
            'div[class*="project"]',
            'tr'
        ]
        
        items = []
        for selector in item_selectors:
            if selector:
                items = soup.select(selector)
                if items:
                    logging.info(f"Found {len(items)} items using selector: {selector}")
                    break
        
        if not items:
            logging.warning("No items found with any selector")
            return []
        
        results = []
        
        for item in items:
            data = {'site_name': site_config['name']}
            
            # 基本データの抽出（より柔軟に）
            for jp_key, selector in site_config.get('data_selectors', {}).items():
                en_key = Constants.JAPANESE_TO_ENGLISH_FIELDS.get(jp_key)
                if not en_key:
                    continue
                
                # 複数のセレクターを試行
                element = None
                selectors_to_try = [selector]
                
                # 代替セレクターを生成
                if '[class*=' in selector:
                    # クラス名の一部マッチングを試行
                    class_part = re.search(r'\[class\*="([^"]+)"\]', selector)
                    if class_part:
                        alt_selector = f'[class*="{class_part.group(1)}"]'
                        selectors_to_try.append(alt_selector)
                
                for try_selector in selectors_to_try:
                    element = item.select_one(try_selector)
                    if element:
                        break
                
                text_content = element.get_text(strip=True) if element else ""
                
                if en_key == Constants.FIELD_LINK and element:
                    href = element.get('href', '')
                    base_url = '/'.join(site_config['url'].split('/')[:3])
                    data[en_key] = href if href.startswith('http') else f"{base_url}{href}"
                elif en_key in [Constants.FIELD_REVENUE, Constants.FIELD_PROFIT, Constants.FIELD_LOCATION, Constants.FIELD_PRICE, Constants.FIELD_FEATURES]:
                    data[f"{en_key}_text"] = text_content
                else:
                    data[en_key] = text_content
            
            # 追加のDL要素処理（M&Aロイヤル用）
            if site_config['name'] == "M&Aロイヤルアドバイザリー":
                UniversalParser._extract_dl_elements_flexible(item, data)
                # 特色の詳細抽出
                enhanced_features = UniversalParser._extract_enhanced_features_flexible(item)
                if enhanced_features and enhanced_features != "-":
                    data['features_text'] = enhanced_features
            
            if data.get('deal_id') and data.get('title') and data.get('link'):
                results.append(RawDealData(**{k: v for k, v in data.items() if k in {f.name for f in fields(RawDealData)}}))
        
        return results

    @staticmethod
    def _parse_masouken_text_based(site_config: Dict[str, Any], html_content: str) -> List[RawDealData]:
        """M&A総合研究所専用の改良版テキストベースパーサー（柔軟性向上版）"""
        results = []
        
        # デバッグ用: HTMLファイル保存
        if CONFIG.get('debug', {}).get('save_html_files', False):
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            debug_file = os.path.join("debug", f"debug_masouken_{timestamp}.html")
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logging.info(f"Debug: HTML saved to {debug_file}")
        
        # BeautifulSoupでパース
        soup = BeautifulSoup(html_content, 'lxml')
        
        # より柔軟なセレクターを試行
        deal_selectors = [
            'li.p-projects-index__item',
            'li[class*="project"]',
            'div[class*="project"]',
            'div[class*="case"]',
            'article',
            'li[class*="item"]',
            'div[class*="item"]'
        ]
        
        deal_items = []
        for selector in deal_selectors:
            deal_items = soup.select(selector)
            if deal_items:
                logging.info(f"Found {len(deal_items)} items using selector: {selector}")
                break
        
        if deal_items:
            for item in deal_items:
                try:
                    # より柔軟な案件ID抽出
                    id_patterns = [
                        r'案件ID[：:\s]*(\d+)',
                        r'案件番号[：:\s]*(\d+)',
                        r'ID[：:\s]*(\d+)',
                        r'No[：:\s]*(\d+)'
                    ]
                    
                    deal_id = None
                    item_text = item.get_text()
                    
                    for pattern in id_patterns:
                        match = re.search(pattern, item_text)
                        if match:
                            deal_id = match.group(1)
                            break
                    
                    if not deal_id:
                        continue
                    
                    # タイトル抽出の改善
                    title_selectors = ['h1', 'h2', 'h3', '.title', '[class*="title"]', '[class*="ttl"]']
                    title = f"M&A案件_{deal_id}"
                    
                    for selector in title_selectors:
                        title_elem = item.select_one(selector)
                        if title_elem:
                            title_text = title_elem.get_text(strip=True)
                            if title_text and len(title_text) > 5:
                                title = title_text
                                break
                    
                    # 財務情報の抽出（より柔軟に）
                    revenue_text = UniversalParser._extract_financial_info_flexible(item_text, ['売上高', '売上'])
                    profit_text = UniversalParser._extract_financial_info_flexible(item_text, ['営業利益', '利益'])
                    
                    # 財務条件チェック
                    revenue_value = DataConverter.parse_financial_value(revenue_text)
                    profit_value = DataConverter.parse_financial_value(profit_text)
                    
                    min_revenue = CONFIG.get('scraping', {}).get('min_revenue', 300000000)
                    min_profit = CONFIG.get('scraping', {}).get('min_profit', 30000000)
                    
                    if revenue_value < min_revenue or profit_value < min_profit:
                        continue
                    
                    # その他の情報
                    location_text = UniversalParser._extract_location_flexible(item_text)
                    price_text = UniversalParser._extract_financial_info_flexible(item_text, ['譲渡希望価格', '希望価格', '価格'])
                    
                    link = f"https://masouken.com/list/{deal_id}"
                    
                    deal_data = RawDealData(
                        site_name=site_config['name'],
                        deal_id=deal_id,
                        title=title,
                        link=link,
                        location_text=location_text,
                        revenue_text=revenue_text,
                        profit_text=profit_text,
                        price_text=price_text,
                        features_text=""
                    )
                    
                    results.append(deal_data)
                    
                except Exception as e:
                    logging.error(f"Error parsing deal item: {e}")
                    continue
        
        # フォールバック処理も改善
        if not results:
            results = UniversalParser._parse_masouken_text_fallback_improved(site_config, html_content)
        
        logging.info(f"M&A総合研究所: Successfully extracted {len(results)} deals")
        return results

    @staticmethod
    def _parse_masouken_text_fallback_improved(site_config: Dict[str, Any], html_content: str) -> List[RawDealData]:
        """M&A総合研究所のフォールバックテキスト抽出（改善版）"""
        results = []
        
        # BeautifulSoupでパース
        soup = BeautifulSoup(html_content, 'lxml')
        
        # アプローチ1: 案件IDパターンでテキスト分割
        content_text = soup.get_text()
        logging.info(f"Total content length: {len(content_text)} characters")
        
        # 案件IDパターンを検索
        deal_id_pattern = r'案件ID[：:\s]*(\d+)'
        deal_matches = list(re.finditer(deal_id_pattern, content_text))
        logging.info(f"Found {len(deal_matches)} deal ID matches")
        
        if deal_matches:
            for i, match in enumerate(deal_matches):
                try:
                    deal_id = match.group(1)
                    start_pos = match.start()
                    
                    # 次の案件IDまでの範囲を取得
                    if i + 1 < len(deal_matches):
                        end_pos = deal_matches[i + 1].start()
                        content = content_text[start_pos:end_pos]
                    else:
                        content = content_text[start_pos:start_pos + 2000]  # 最後の案件は2000文字まで
                    
                    logging.info(f"Processing deal ID: {deal_id}, content length: {len(content)}")
                    
                    # タイトル抽出の改善
                    title = UniversalParser._extract_masouken_title_flexible(content, deal_id)
                    
                    # 所在地抽出の改善
                    location_text = UniversalParser._extract_location_flexible(content)
                    
                    # 財務情報抽出
                    revenue_text = UniversalParser._extract_financial_info_flexible(content, ['売上高', '売上'])
                    profit_text = UniversalParser._extract_financial_info_flexible(content, ['営業利益', '利益'])
                    price_text = UniversalParser._extract_financial_info_flexible(content, ['譲渡希望価格', '希望価格'])
                    
                    # 財務条件チェック
                    revenue_value = DataConverter.parse_financial_value(revenue_text)
                    profit_value = DataConverter.parse_financial_value(profit_text)
                    
                    min_revenue = CONFIG.get('scraping', {}).get('min_revenue', 300000000)
                    min_profit = CONFIG.get('scraping', {}).get('min_profit', 30000000)
                    
                    if revenue_value < min_revenue or profit_value < min_profit:
                        logging.info(f"Skipping deal {deal_id}: doesn't meet financial criteria")
                        continue
                    
                    # リンク生成
                    link = f"https://masouken.com/list/{deal_id}"
                    
                    # データ検証（より柔軟な条件）
                    if deal_id and title:
                        deal_data = RawDealData(
                            site_name=site_config['name'],
                            deal_id=deal_id,
                            title=title,
                            link=link,
                            location_text=location_text,
                            revenue_text=revenue_text,
                            profit_text=profit_text,
                            price_text=price_text,
                            features_text=""
                        )
                        results.append(deal_data)
                        logging.info(f"Successfully extracted deal: {deal_id} - {title[:50]}")
                    else:
                        logging.debug(f"Skipped incomplete deal: ID={deal_id}, Title={title[:30] if title else 'None'}")
                        
                except Exception as e:
                    logging.error(f"Error parsing deal {i + 1}: {e}")
                    continue
        
        return results

    @staticmethod
    def _extract_financial_info_flexible(text: str, keywords: List[str]) -> str:
        """より柔軟な財務情報抽出"""
        for keyword in keywords:
            patterns = [
                rf'{keyword}[：:\s]*([^\n]+)',
                rf'{keyword}\s*([^\n]+)',
                rf'・{keyword}[：:\s]*([^\n]+)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    value = match.group(1).strip()
                    # クリーンアップ
                    value = re.sub(r'^[：:\s・]+', '', value)
                    # 次の項目で切る
                    for stop_word in ['営業利益', '譲渡希望価格', '所在地', '業界']:
                        if stop_word in value and stop_word != keyword:
                            value = value.split(stop_word)[0].strip()
                            break
                    if value and len(value) > 0:
                        return value
        return ""

    @staticmethod
    def _extract_location_flexible(text: str) -> str:
        """より柔軟な所在地抽出"""
        location_keywords = ['所在地', 'エリア', '地域', '所在']
        
        for keyword in location_keywords:
            pattern = rf'{keyword}[：:\s]*([^\n：]+)'
            match = re.search(pattern, text)
            if match:
                location = match.group(1).strip()
                # 都道府県名が含まれているかチェック
                prefectures = ['北海道', '青森', '岩手', '宮城', '秋田', '山形', '福島', '茨城', '栃木', '群馬', '埼玉', '千葉', '東京', '神奈川', '新潟', '富山', '石川', '福井', '山梨', '長野', '岐阜', '静岡', '愛知', '三重', '滋賀', '京都', '大阪', '兵庫', '奈良', '和歌山', '鳥取', '島根', '岡山', '広島', '山口', '徳島', '香川', '愛媛', '高知', '福岡', '佐賀', '長崎', '熊本', '大分', '宮崎', '鹿児島', '沖縄']
                
                for pref in prefectures:
                    if pref in location:
                        return location
        
        return ""

    @staticmethod
    def _extract_masouken_title_flexible(content: str, deal_id: str) -> str:
        """M&A総合研究所のタイトル抽出を改善（柔軟版）"""
        
        # パターン1: 【】で囲まれたタイトルを抽出（より柔軟に）
        bracket_patterns = [
            r'【([^】]{5,100})】',
            r'案件ID[：:\s]*' + deal_id + r'[^\n]*\n[^\n]*【([^】]{5,100})】',
        ]
        
        exclude_keywords = [
            '案件ID', '売上高', '営業利益', '譲渡希望価格', 'URL', 'http',
            '百万円', '万円', '所在地', 'エリア', '地域', '選択してください'
        ]
        
        business_keywords = [
            '事業', 'サービス', '製造', '販売', '工事', '建設', '業', '会社', '企業',
            'レストラン', 'カフェ', '店舗', '広告', 'IT', 'システム', 'ソリューション',
            '化粧品', '塗装', '設計', '開発', '運営', '管理', '商社', '貿易', '輸入',
            '輸出', '卸売', '小売', '医療', '介護', '教育', '学習', '塾', 'スクール',
            '不動産', '建築', '土木', '電気', '機械', '自動車', '部品', '材料',
            'コンサル', 'Web', 'アプリ', 'ソフト', '通販', 'EC', '配送', '物流'
        ]
        
        for pattern in bracket_patterns:
            matches = re.findall(pattern, content, re.DOTALL)
            for match in matches:
                title_candidate = match.strip()
                
                # 長さチェック
                if not (5 <= len(title_candidate) <= 100):
                    continue
                
                # 除外キーワードチェック
                if any(keyword in title_candidate for keyword in exclude_keywords):
                    continue
                
                # 数字のみは除外
                if title_candidate.isdigit():
                    continue
                
                # 事業関連キーワードが含まれているかチェック
                if any(keyword in title_candidate for keyword in business_keywords):
                    logging.info(f"Found title for deal {deal_id}: {title_candidate}")
                    return f"【{title_candidate}】"
        
        # フォールバック: 汎用的なタイトルを生成
        logging.warning(f"Could not extract proper title for deal {deal_id}, using fallback")
        return f"M&A案件_{deal_id}"

    @staticmethod
    def _extract_dl_elements_flexible(item: Tag, data: Dict[str, str]) -> None:
        """DL要素からの詳細情報抽出（柔軟版）"""
        # より多くのdl構造を試行
        dl_selectors = [
            "dl.p-case__dl",
            "dl[class*='case']",
            "div dl",
            "dl"
        ]
        
        dl_tags = []
        for selector in dl_selectors:
            dl_tags = item.select(selector)
            if dl_tags:
                break
        
        for dl in dl_tags:
            dt = dl.find("dt")
            dd = dl.find("dd")
            
            if dt and dd:
                jp_key = dt.get_text(strip=True)
                en_key = Constants.JAPANESE_TO_ENGLISH_FIELDS.get(jp_key)
                
                if en_key and en_key in [Constants.FIELD_REVENUE, Constants.FIELD_PROFIT, 
                                        Constants.FIELD_LOCATION, Constants.FIELD_PRICE]:
                    data[f"{en_key}_text"] = dd.get_text(strip=True)

    @staticmethod
    def _extract_enhanced_features_flexible(item_element: Tag) -> str:
        """M&Aロイヤル用の拡張特色抽出（柔軟版）"""
        try:
            all_text = item_element.get_text()
            
            # 特徴セクションのパターン検索（より多くのパターン）
            feature_patterns = [
                r'【特徴・強み】(.+?)(?=【|■|◆|$)',
                r'【特色】(.+?)(?=【|■|◆|$)',
                r'【事業内容】(.+?)(?=【|■|◆|$)',
                r'特徴・強み[：:\s]*(.+?)(?=【|■|◆|$)',
                r'特色[：:\s]*(.+?)(?=【|■|◆|$)',
                r'事業内容[：:\s]*(.+?)(?=【|■|◆|$)',
            ]
            
            for pattern in feature_patterns:
                match = re.search(pattern, all_text, re.DOTALL)
                if match and len(match.group(1).strip()) > 20:
                    feature_content = match.group(1).strip()
                    
                    # 箇条書きマーカーで抽出
                    bullet_patterns = [
                        (r'✓([^✓\n]+)', '✓'),
                        (r'・([^・\n]+)', '・'),
                        (r'◆([^◆\n]+)', '◆'),
                        (r'○([^○\n]+)', '○'),
                        (r'●([^●\n]+)', '●'),
                    ]
                    
                    for bullet_pattern, marker in bullet_patterns:
                        matches = re.findall(bullet_pattern, feature_content)
                        if matches:
                            business_keywords = ['取引', '実績', '技術', '品質', '顧客', '事業', 'サービス', '製品', '強み']
                            extracted_items = []
                            
                            for match in matches:
                                cleaned_item = match.strip()
                                # 【修正箇所】マーカー後の不要なスペースを削除
                                cleaned_item = re.sub(r'^[\s　]+', '', cleaned_item)
                                if (len(cleaned_item) > 10 and 
                                    any(keyword in cleaned_item for keyword in business_keywords)):
                                    extracted_items.append(f"{marker}{cleaned_item}")
                                    if len(extracted_items) >= 5:
                                        break
                            
                            if extracted_items:
                                return "\n".join(extracted_items)
            
            return "-"
            
        except Exception as e:
            logging.error(f"Error extracting enhanced features: {e}")
            return "-"

class GSheetConnector:
    """Google Sheets接続管理クラス"""
    def __init__(self, config: Dict):
        self.config = config['google_sheets']
        self.worksheet = self._connect()

    def _connect(self):
        logging.info("Connecting to Google Sheets...")
        try:
            # 環境変数から認証情報ファイルのパスを取得
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
            logging.FileHandler(log_config.get('file_name', 'scraping.log'), encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def retry_on_failure(max_retries_key: str = 'max_retries', delay_key: str = 'retry_delay'):
    """リトライデコレーター"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            scraping_config = CONFIG.get('scraping', {})
            max_retries = scraping_config.get(max_retries_key, 3)
            delay = scraping_config.get(delay_key, 1)
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

def diagnose_site_structure(site_config: Dict[str, Any], html_content: str) -> None:
    """サイト構造の診断機能"""
    soup = BeautifulSoup(html_content, 'lxml')
    site_name = site_config['name']
    
    logging.info(f"🔍 Diagnosing {site_name} structure...")
    
    # HTMLの基本情報
    logging.info(f"  HTML length: {len(html_content)} characters")
    logging.info(f"  Title: {soup.title.string if soup.title else 'No title'}")
    
    # 設定されたセレクターの検証
    if 'item_selector' in site_config:
        items = soup.select(site_config['item_selector'])
        logging.info(f"  Items found with '{site_config['item_selector']}': {len(items)}")
        
        if len(items) == 0:
            # 代替セレクターを試行
            alternative_selectors = [
                'article', 'div[class*="item"]', 'li[class*="item"]',
                'div[class*="case"]', 'div[class*="project"]', 
                'tr', 'div[class*="deal"]'
            ]
            
            for alt_selector in alternative_selectors:
                alt_items = soup.select(alt_selector)
                if len(alt_items) > 0:
                    logging.warning(f"  🔄 Alternative selector '{alt_selector}' found {len(alt_items)} items")
    
    # エラーページの検出
    error_indicators = ['404', 'error', 'not found', 'blocked', 'forbidden']
    page_text = soup.get_text().lower()
    
    for indicator in error_indicators:
        if indicator in page_text:
            logging.warning(f"  ⚠️ Possible error page detected: '{indicator}' found in content")

def format_deal_data(raw_deals: List[RawDealData], existing_ids: Set[str]) -> List[FormattedDealData]:
    """生データを整形済みデータに変換し、条件チェックを行う"""
    formatted_deals = []
    extraction_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for raw_deal in raw_deals:
        try:
            unique_id = hashlib.md5(f"{raw_deal.site_name}_{raw_deal.deal_id}".encode()).hexdigest()[:12]
            
            if unique_id in existing_ids:
                logging.info(f"    -> Skipping duplicate deal: {raw_deal.deal_id}")
                continue
            
            # 財務条件チェック（ストライクは一覧ページで既にチェック済み）
            if raw_deal.site_name not in ["M&A総合研究所", "M&Aキャピタルパートナーズ", "ストライク"]:
                revenue_value = DataConverter.parse_financial_value(raw_deal.revenue_text)
                profit_value = DataConverter.parse_financial_value(raw_deal.profit_text)
                
                min_revenue = CONFIG.get('scraping', {}).get('min_revenue', 300000000)
                min_profit = CONFIG.get('scraping', {}).get('min_profit', 30000000)
                
                if revenue_value < min_revenue or profit_value < min_profit:
                    logging.info(f"    -> Skipping deal {raw_deal.deal_id}: doesn't meet financial criteria")
                    continue
            
            # ストライクの売上高を百万円単位に変換
            if raw_deal.site_name == "ストライク":
                revenue_formatted = DataConverter.convert_strike_revenue_to_million(raw_deal.revenue_text)
            else:
                revenue_formatted = DataConverter.format_financial_text(raw_deal.revenue_text)
            
            formatted_deal = FormattedDealData(
                extraction_time=extraction_time,
                site_name=raw_deal.site_name,
                deal_id=raw_deal.deal_id,
                title=raw_deal.title,
                location=raw_deal.location_text or "-",
                revenue=revenue_formatted,
                profit=DataConverter.format_financial_text(raw_deal.profit_text),
                price=DataConverter.format_financial_text(raw_deal.price_text),
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

def scrape_site(site_config: Dict[str, Any]) -> List[RawDealData]:
    """各サイトのスクレイピングを実行（診断機能付き）"""
    if not site_config.get('enabled', False):
        logging.info(f"Site {site_config['name']} is disabled. Skipping.")
        return []
    
    logging.info(f"🔍 Starting scraping for: {site_config['name']}")
    all_deals = []
    
    try:
        max_pages = site_config.get('max_pages', 1)
        base_url = site_config['url']
        
        for page_num in range(1, max_pages + 1):
            if site_config.get('pagination', {}).get('type') == 'query_param':
                param = site_config['pagination']['param']
                url = f"{base_url}?{param}={page_num}"
            elif site_config.get('pagination', {}).get('type') == 'path':
                path_template = site_config['pagination']['path']
                url = f"{base_url}{path_template.format(page_num=page_num)}"
            else:
                url = base_url
            
            logging.info(f"  📄 Scraping page {page_num}: {url}")
            
            # ストライクサイトの動的読み込み対応
            if site_config['name'] == "ストライク":
                html_content = scrape_strike_with_dynamic_loading(url)
            else:
                html_content = fetch_html(url)
            
            if not html_content:
                logging.error(f"  ❌ Failed to fetch page {page_num}")
                continue
            
            # 診断実行
            diagnose_site_structure(site_config, html_content)
            
            # 統一されたパーサーを使用
            deals = UniversalParser.parse_list_page(site_config, html_content)
            all_deals.extend(deals)
            
            # 診断機能：1ページ目で案件が0件の場合は警告
            if page_num == 1 and len(deals) == 0:
                logging.critical(f"🚨 CRITICAL - {site_config['name']}の1ページ目から案件が1件も見つかりませんでした。")
                logging.critical(f"   サイトのHTML構造が変更された可能性があります。")
                logging.critical(f"   config.yamlのCSSセレクタを見直してください。")
                logging.critical(f"   現在のitem_selector: {site_config.get('item_selector')}")
            
            time.sleep(2)
            
            if max_pages == 1:
                break
    
    except Exception as e:
        logging.error(f"❌ Error scraping {site_config['name']}: {e}")
        logging.debug(traceback.format_exc())
    
    logging.info(f"🎯 Total deals found from {site_config['name']}: {len(all_deals)}")
    return all_deals

def scrape_strike_with_dynamic_loading(url: str) -> Optional[str]:
    """ストライク専用の動的読み込み対応スクレイピング"""
    try:
        anti_blocking = AntiBlockingManager()
        with WebDriverManager(headless=CONFIG.get('debug', {}).get('headless_mode', True), anti_blocking=anti_blocking) as driver:
            logging.info(f"  🚀 Loading Strike page with dynamic loading support: {url}")
            driver.get(url)
            
            # 52件の案件アイテムが読み込まれるまで待機（最大30秒）
            wait = WebDriverWait(driver, 30)
            
            logging.info("  ⏳ Waiting for deal items to load...")
            
            # 案件アイテムが読み込まれるまで待機（より柔軟に）
            try:
                # 複数のセレクターを試行
                selectors_to_wait = [
                    'div.search-result__item',
                    'div[class*="search-result"]',
                    'div[class*="item"]'
                ]
                
                items_found = False
                for selector in selectors_to_wait:
                    try:
                        wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, selector)) >= 10)
                        current_items = driver.find_elements(By.CSS_SELECTOR, selector)
                        logging.info(f"  ✅ {len(current_items)} items loaded with selector: {selector}")
                        items_found = True
                        break
                    except TimeoutException:
                        continue
                
                if not items_found:
                    logging.warning("  ⚠️ Timeout waiting for items with all selectors")
                    
            except TimeoutException:
                logging.warning(f"  ⚠️ Timeout waiting for items")
            
            # 追加の待機（JavaScriptの完全な実行完了を確保）
            time.sleep(3)
            
            return driver.page_source
            
    except Exception as e:
        logging.error(f"  ❌ Error in Strike dynamic loading: {e}")
        logging.debug(traceback.format_exc())
        return None

def enhance_deals_with_details(raw_deals: List[RawDealData], site_config: Dict[str, Any]) -> List[RawDealData]:
    """詳細ページから特色情報を取得して既存データを拡張（403対策強化版）"""
    
    # 一覧ページで十分な情報が取得できるサイトは詳細ページアクセスをスキップ
    skip_detail_sites = ["M&Aキャピタルパートナーズ", "M&Aロイヤルアドバイザリー"]
    
    if site_config['name'] in skip_detail_sites:
        logging.info(f"  Skipping detail page scraping for {site_config['name']} (using list page features)")
        return raw_deals
    
    if not site_config.get('detail_page_selectors') and site_config['name'] not in ["M&A総合研究所", "ストライク", "M&Aロイヤルアドバイザリー"]:
        logging.info(f"  No detail page selectors configured for {site_config['name']}")
        return raw_deals
    
    logging.info(f"🔗 Fetching details for {len(raw_deals)} deals from {site_config['name']}")
    enhanced_deals = []
    
    try:
        anti_blocking = AntiBlockingManager()
        with WebDriverManager(headless=CONFIG.get('debug', {}).get('headless_mode', True), anti_blocking=anti_blocking) as driver:
            scraper = DetailPageScraper(driver, anti_blocking)
            
            # 一覧ページのURLをリファラーとして設定
            referer_url = site_config['url']
            
            for i, deal in enumerate(raw_deals, 1):
                try:
                    logging.info(f"  📖 Processing deal {i}/{len(raw_deals)}: {deal.deal_id}")
                    
                    # 403ブロックが検出されている場合は処理を停止
                    if anti_blocking.blocked_detected:
                        logging.warning(f"  🚫 Blocked state detected. Skipping remaining {len(raw_deals) - i + 1} deals.")
                        # 残りの案件もそのまま追加（詳細情報なし）
                        enhanced_deals.extend(raw_deals[i-1:])
                        break
                    
                    # ストライクの詳細ページで追加情報を取得
                    elif site_config['name'] == "ストライク":
                        enhanced_deal = enhance_strike_deal_with_details_protected(deal, scraper, anti_blocking, referer_url)
                        enhanced_deals.append(enhanced_deal)
                    else:
                        # 他のサイトの処理（403対策付き）
                        features = scraper.fetch_features_with_blocking_protection(
                            deal.link, 
                            site_config.get('detail_page_selectors', {}),
                            referer_url
                        )
                        
                        if features and features != "-":
                            if deal.features_text:
                                deal.features_text = f"{deal.features_text}\n{features}"
                            else:
                                deal.features_text = features
                        
                        enhanced_deals.append(deal)
                    
                    # 人間らしい待機時間
                    if site_config['name'] in ["ストライク", "M&Aロイヤルアドバイザリー"]:
                        delay = anti_blocking.get_human_like_delay(4, 6)
                    else:
                        delay = anti_blocking.get_human_like_delay(2, 4)
                    
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

def enhance_maroyal_deal_with_features(deal: RawDealData, scraper: DetailPageScraper, 
    anti_blocking: AntiBlockingManager, referer_url: str) -> RawDealData:
    """M&Aロイヤルアドバイザリーの案件に特徴・強み情報を追加"""
    try:
        logging.info(f"    🔍 Fetching features from M&Aロイヤルアドバイザリー detail page: {deal.link}")
        
        # 詳細ページの特色を取得
        features_text = scraper.fetch_features_with_blocking_protection(
            deal.link, 
            {},  # M&Aロイヤルは独自の抽出ロジックを使用
            referer_url
        )
        
        if features_text and features_text != "-":
            deal.features_text = features_text
            logging.info(f"    ✅ Extracted features: {features_text[:100]}...")
        else:
            logging.warning(f"    ⚠️ No features found for deal {deal.deal_id}")
            
    except Exception as e:
        logging.error(f"    ❌ Error fetching features for deal {deal.deal_id}: {e}")
    
    return deal

def enhance_strike_deal_with_details_protected(deal: RawDealData, scraper: DetailPageScraper, anti_blocking: AntiBlockingManager, referer_url: str) -> RawDealData:
    """ストライクの詳細ページから追加情報を取得（403対策強化版）"""
    try:
        logging.info(f"    -> Accessing Strike detail page: {deal.link}")
        
        # 人間らしい待機時間
        delay = anti_blocking.get_human_like_delay(3, 8)
        logging.info(f"    -> Human-like delay: {delay:.1f} seconds...")
        time.sleep(delay)
        
        # ページにアクセス
        scraper.driver.get(deal.link)
        time.sleep(3)  # ページ読み込み待機
        
        html_content = scraper.driver.page_source
        
        # 403ブロックの検出
        if anti_blocking.is_blocked_response(html_content):
            logging.warning(f"    -> 🚫 403 BLOCK DETECTED for deal: {deal.deal_id}")
            
            if not anti_blocking.blocked_detected:
                anti_blocking.blocked_detected = True
                logging.warning("    -> First Strike block detected. Attempting recovery...")
                
                # 回復待機時間（ストライク専用でより長く）
                recovery_delay = anti_blocking.get_recovery_delay()
                logging.info(f"    -> Recovery wait: {recovery_delay:.1f} seconds...")
                time.sleep(recovery_delay)
                
                # リトライ
                logging.info("    -> Retrying Strike access...")
                scraper.driver.get(deal.link)
                time.sleep(5)  # より長い読み込み待機
                
                retry_html = scraper.driver.page_source
                
                if anti_blocking.is_blocked_response(retry_html):
                    logging.error("    -> ❌ Still blocked after retry. Aborting Strike detail scraping.")
                    anti_blocking.blocked_detected = True
                    return deal
                else:
                    logging.info("    -> ✅ Strike recovery successful!")
                    html_content = retry_html
                    anti_blocking.blocked_detected = False
            else:
                logging.error("    -> ❌ Already in blocked state. Skipping Strike deal.")
                return deal
        
        detail_soup = BeautifulSoup(html_content, 'lxml')
        
        # タイトルの取得
        title = extract_strike_title_enhanced(detail_soup, deal.deal_id)
        if title and title != f"ストライク案件_{deal.deal_id}":
            deal.title = title
        
        # 所在地の取得
        location = extract_strike_location_enhanced(detail_soup)
        if location:
            deal.location_text = location
        
        # 特色の取得
        features = scraper._fetch_strike_features_enhanced(detail_soup, deal.link)
        if features and features != "-":
            deal.features_text = features
        
        logging.info(f"    -> Enhanced Strike deal: {deal.deal_id} - {deal.title[:50]}")
        return deal
        
    except Exception as e:
        logging.error(f"    -> Error enhancing Strike deal {deal.deal_id}: {e}")
        return deal

def extract_strike_title_enhanced(detail_soup: BeautifulSoup, deal_id: str) -> str:
    """ストライクのタイトル抽出（完全対応版）"""
    
    # アプローチ1: 標準的なセレクター
    title_selectors = [
        'h2.section-ttl span',
        'h2.section-ttl',
        'h1.section-ttl span',
        'h1.section-ttl',
        '.section-ttl span',
        '.section-ttl'
    ]
    
    for selector in title_selectors:
        title_element = detail_soup.select_one(selector)
        if title_element:
            title_text = title_element.get_text(strip=True)
            if title_text and len(title_text) > 5 and title_text != deal_id:
                logging.info(f"    -> Found title via selector {selector}: {title_text[:30]}...")
                return title_text
    
    # アプローチ2: h1, h2タグの全探索
    for header_tag in ['h1', 'h2', 'h3']:
        headers = detail_soup.find_all(header_tag)
        for header in headers:
            header_text = header.get_text(strip=True)
            # 案件IDやナビゲーション要素を除外
            exclude_keywords = [
                '案件検索', 'ログイン', 'メニュー', 'ナビ', 'トップ',
                '会員登録', '詳細検索', 'STRIKE', deal_id, 'SS', '売上高'
            ]
            
            if (header_text and len(header_text) > 5 and len(header_text) < 100 and
                not any(keyword in header_text for keyword in exclude_keywords)):
                
                # 事業関連キーワードが含まれているかチェック
                business_keywords = [
                    '事業', 'サービス', '製造', '販売', '工事', '建設', '業', '会社',
                    'レストラン', 'カフェ', '店舗', '広告', 'IT', 'システム',
                    '化粧品', '塗装', '設計', '開発', '運営', '管理', '商社'
                ]
                
                if any(keyword in header_text for keyword in business_keywords):
                    logging.info(f"    -> Found title via header search: {header_text[:30]}...")
                    return header_text
    
    # フォールバック: デフォルトタイトル
    logging.warning(f"    -> Could not extract title for {deal_id}, using default")
    return f"ストライク案件_{deal_id}"

def extract_strike_location_enhanced(detail_soup: BeautifulSoup) -> str:
    """ストライクの所在地抽出（完全対応版）"""
    
    # ストライクの地方表記パターン
    valid_locations = [
        "東北地方", "関東地方", "中部・北陸地方", "関西地方", 
        "中国地方", "四国地方", "九州・沖縄地方", "東日本", "西日本", "海外"
    ]
    
    # アプローチ1: ul.detail__list内の所在地ラベルを探す
    detail_lists = detail_soup.find_all('ul', class_='detail__list')
    
    for ul in detail_lists:
        items = ul.find_all('li')
        
        for item in items:
            # テキスト全体から所在地情報を抽出
            text_content = item.get_text(strip=True)
            
            # 「所在地」で始まる行を探す
            if '所在地' in text_content:
                # 有効な地方表記かチェック
                for valid_location in valid_locations:
                    if valid_location in text_content:
                        logging.info(f"    -> Found location via detail list: {valid_location}")
                        return valid_location
    
    # アプローチ2: ページ全体から地方表記を探す
    full_text = detail_soup.get_text()
    
    for valid_location in valid_locations:
        if valid_location in full_text:
            # 所在地に関連するコンテキストかチェック
            location_pos = full_text.find(valid_location)
            surrounding = full_text[max(0, location_pos-50):location_pos+50]
            
            if '所在地' in surrounding or 'エリア' in surrounding or '地域' in surrounding:
                logging.info(f"    -> Found location via full text search: {valid_location}")
                return valid_location
    
    logging.warning("    -> Could not extract location")
    return ""

def main():
    """メイン実行関数（診断機能付き）"""
    try:
        load_config()
        setup_logging(CONFIG)
        
        logging.info("🚀 Starting M&A deal scraping with diagnostics and anti-blocking measures")
        logging.info(f"📊 Target criteria: Revenue ≥ {CONFIG.get('scraping', {}).get('min_revenue', 300000000):,} yen, Profit ≥ {CONFIG.get('scraping', {}).get('min_profit', 30000000):,} yen")
        
        sheet_connector = GSheetConnector(CONFIG)
        if not sheet_connector.worksheet:
            logging.critical("❌ Cannot proceed without Google Sheets connection")
            return
        
        existing_ids = sheet_connector.get_existing_ids()
        logging.info(f"📋 Found {len(existing_ids)} existing deals in spreadsheet")
        
        all_new_deals = []
        target_sites = ["M&A総合研究所", "M&Aキャピタルパートナーズ", "M&Aロイヤルアドバイザリー", "ストライク"]
        enabled_sites = [site for site in CONFIG['sites'] 
                        if site.get('enabled', False) and site['name'] in target_sites]
        
        for site_config in enabled_sites:
            try:
                logging.info(f"🔍 Processing {site_config['name']}")
                
                raw_deals = scrape_site(site_config)
                
                if not raw_deals:
                    logging.warning(f"⚠️ {site_config['name']}: No deals extracted")
                    continue
                
                enhanced_deals = enhance_deals_with_details(raw_deals, site_config)
                formatted_deals = format_deal_data(enhanced_deals, existing_ids)
                
                logging.info(f"✅ {site_config['name']}: {len(formatted_deals)} new deals after filtering")
                all_new_deals.extend(formatted_deals)
                
            except Exception as e:
                logging.error(f"❌ Failed to process {site_config['name']}: {e}")
                continue
        
        if all_new_deals:
            sheet_connector.write_deals(all_new_deals)
            logging.info(f"🎉 Successfully added {len(all_new_deals)} new deals to spreadsheet")
        else:
            logging.warning("📝 No new deals found across all sites")
        
        logging.info("✨ Scraping process completed successfully with diagnostics and anti-blocking measures")
        
    except Exception as e:
        logging.critical(f"💥 Critical error in main process: {e}")
        logging.debug(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()