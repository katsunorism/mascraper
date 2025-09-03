# scrapers.py
import time
import re
import logging
from bs4 import BeautifulSoup, Tag
from typing import List, Dict, Any

# --- (ここにDataConverter, FinancialValidator, RawDealData, FormattedDealDataを移動) ---

class BaseScraper:
    """全てのスクレイパーの基盤となるクラス"""
    def __init__(self, site_config: Dict[str, Any], driver, existing_ids: set):
        self.config = site_config
        self.driver = driver
        self.existing_ids = existing_ids
        self.new_deals: List[FormattedDealData] = []

    def execute(self) -> List[FormattedDealData]:
        """スクレイピングの実行"""
        logging.info(f"▶️ Processing '{self.config['name']}'...")
        for page_num in range(1, self.config.get('max_pages', 1) + 1):
            target_url = self._build_url_for_page(page_num)
            logging.info(f"  - Scraping page {page_num}: {target_url}")
            
            html = fetch_html(target_url) # fetch_htmlはmain.pyに配置
            if not html:
                logging.warning(f"  - Failed to fetch page {page_num}.")
                continue
            
            raw_deals = self._parse_list_page(html)
            qualified_deals = self._filter_deals(raw_deals)
            self._process_qualified_deals(qualified_deals)
        
        logging.info(f"  - Finally, {len(self.new_deals)} new deals to be added.")
        return self.new_deals

    def _build_url_for_page(self, page_num: int) -> str:
        """ページネーションのURLを構築"""
        if page_num == 1: return self.config['url']
        
        pagination = self.config.get('pagination', {})
        pag_type = pagination.get('type')
        base_url = self.config['url']

        if pag_type == 'query_param':
            return f"{base_url}?{pagination['param']}={page_num}"
        elif pag_type == 'path':
            return f"{base_url.rstrip('/')}/{pagination['path'].format(page_num=page_num)}"
        return base_url

    def _parse_list_page(self, html_content: str) -> List[RawDealData]:
        # ... (main.pyから移植したparse_list_pageロジック) ...
    
    def _filter_deals(self, raw_deals: List[RawDealData]) -> List[RawDealData]:
        # ... (main.pyから移植した絞り込みロジック) ...
    
    def _process_qualified_deals(self, qualified_deals: List[RawDealData]):
        # ... (main.pyから移植した整形・拡充ロジック) ...

    def _fetch_features(self, detail_url: str) -> str:
        """特色情報の抽出（サブクラスでオーバーライド）"""
        raise NotImplementedError("This method should be overridden by subclasses")

class MacpScraper(BaseScraper):
    """M&Aキャピタルパートナーズ専用のスクレイパー"""
    def _fetch_features(self, detail_url: str) -> str:
        # ★★★ あなたのscraper_macp.pyのget_feature_from_detail_pageロジックをここに完全移植 ★★★
        try:
            logging.info(f"    -> Accessing detail page: {detail_url}")
            self.driver.get(detail_url)
            time.sleep(2)
            detail_soup = BeautifulSoup(self.driver.page_source, "lxml")
            
            target_h4 = detail_soup.find("h4", string=lambda t: t and "事業概要" in t)
            if not target_h4: return "特色見出しなし"
            
            collected_text = []
            for next_element in target_h4.find_next_siblings():
                if next_element.name == "h4": break
                if next_element.name in ["p", "ul"]:
                    text = next_element.get_text(strip=True)
                    if text: collected_text.append(text)
            return "\n".join(collected_text)
        except Exception as e:
            logging.error(f"    -> MACP detail page error: {e}")
            return "取得エラー"

class MaroyalScraper(BaseScraper):
    """M&Aロイヤルアドバイザリー専用のスクレイパー"""
    def _fetch_features(self, detail_url: str) -> str:
        # ★★★ あなたのscraper_maroyal.pyの特色抽出ロジックをここに完全移植 ★★★
        # ...
        return "M&Aロイヤルアドバイザリーの特色情報"