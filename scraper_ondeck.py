#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
オンデック社 M&A案件自動抽出スクリプト（デバッグ強化版）
https://www.ondeck.jp/sell から条件に合致する案件を自動抽出
"""

import re
import csv
import time
import logging
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import unicodedata
import json

class OnDeckScraper:
    def __init__(self, debug=True):
        """スクレイパーの初期化"""
        self.base_url = "https://www.ondeck.jp/sell"
        self.results = []
        self.driver = None
        self.debug = debug
        
        # 抽出条件
        self.min_revenue = 300_000_000  # 3億円
        self.min_profit = 30_000_000   # 3千万円
        
        # レンジ区切り文字（文字化け対応含む）
        self.range_separators = ['～', '?', '?', '-', '〜', 'ー', '−', '–', '—']
        
        # デバッグ用ディレクトリ作成
        self.debug_dir = f"debug_{{datetime.now().strftime('%Y%m%d_%H%M%S')}}"
        if self.debug:
            os.makedirs(self.debug_dir, exist_ok=True)
            
        # ログ設定
        self.setup_logging()
        
        # 処理統計
        self.stats = {
            'pages_processed': 0,
            'links_found': 0,
            'cases_processed': 0,
            'cases_with_revenue': 0,
            'cases_with_profit': 0,
            'cases_matching_conditions': 0
        }
        
    def setup_logging(self):
        """ログ設定"""
        # ファイルハンドラー
        log_filename = os.path.join(self.debug_dir, 'scraper.log') if self.debug else 'scraper.log'
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # コンソールハンドラー
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # フォーマット設定
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # ロガー設定
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers.clear()
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def setup_driver(self):
        """Seleniumドライバーの設定"""
        chrome_options = Options()
        if not self.debug:  # デバッグ時は表示モード
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.implicitly_wait(10)
            self.logger.info("Chromeドライバーを起動しました")
        except Exception as e:
            self.logger.error(f"ドライバー起動エラー: {e}")
            raise
    
    def save_html(self, content, filename):
        """HTMLをファイルに保存（デバッグ用）"""
        if not self.debug:
            return
        
        filepath = os.path.join(self.debug_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            self.logger.debug(f"HTMLを保存しました: {filepath}")
        except Exception as e:
            self.logger.error(f"HTML保存エラー: {e}")
    
    def save_debug_info(self, data, filename):
        """デバッグ情報をJSONファイルに保存"""
        if not self.debug:
            return
        
        filepath = os.path.join(self.debug_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.debug(f"デバッグ情報を保存しました: {filepath}")
        except Exception as e:
            self.logger.error(f"デバッグ情報保存エラー: {e}")
    
    def normalize_text(self, text):
        """テキストの正規化（全角→半角変換、空白除去）"""
        if not text:
            return ""
        # Unicode正規化
        text = unicodedata.normalize('NFKC', text)
        # 余分な空白を除去
        text = re.sub(r'\s+', '', text)
        return text
    
    def parse_amount(self, amount_str):
        """
        金額文字列を数値に変換（百万円単位対応）
        例: "約820百万円" → 820000000, "約53百万円" → 53000000
        レンジの場合は最大値を返す
        """
        if not amount_str:
            return 0
            
        original_str = amount_str
        amount_str = self.normalize_text(amount_str)
        self.logger.debug(f"金額解析開始: '{original_str}' → '{amount_str}'")
        
        # 「約」「（直近期実績）」「（修正後）」などを除去
        amount_str = re.sub(r'約|（[^）]*）|直近期実績|修正後', '', amount_str)
        
        # レンジかどうかを判定
        range_match = None
        separator_used = None
        
        for sep in self.range_separators:
            if sep in amount_str:
                parts = amount_str.split(sep)
                if len(parts) == 2:
                    range_match = parts
                    separator_used = sep
                    self.logger.debug(f"レンジ検出: {parts[0]} {sep} {parts[1]}")
                    break
        
        if range_match:
            # レンジの場合、最大値（右側）を使用
            max_amount_str = range_match[1].strip()
            self.logger.debug(f"レンジの最大値を使用: '{max_amount_str}'")
            return self._convert_single_amount(max_amount_str)
        else:
            # 単一値の場合
            return self._convert_single_amount(amount_str)
    
    def _convert_single_amount(self, amount_str):
        """単一の金額文字列を数値に変換（百万円単位対応）"""
        if not amount_str:
            return 0
            
        amount_str = self.normalize_text(amount_str)
        self.logger.debug(f"単一金額変換: '{amount_str}'")
        
        # 「円」を除去
        amount_str = amount_str.replace('円', '')
        
        # カンマを除去
        amount_str = amount_str.replace(',', '')
        
        try:
            # 百万円単位の処理（最優先）
            if '百万' in amount_str:
                match = re.search(r'(\d+(?:\.\d+)?)\s*百万', amount_str)
                if match:
                    value = float(match.group(1)) * 1_000_000
                    self.logger.debug(f"百万円単位変換: {match.group(1)}百万円 → {int(value)}")
                    return int(value)
            
            # 億・万・千の単位処理
            elif '億' in amount_str:
                match = re.search(r'(\d+(?:\.\d+)?)\s*億', amount_str)
                if match:
                    value = float(match.group(1)) * 100_000_000
                    self.logger.debug(f"億単位変換: {match.group(1)}億 → {int(value)}")
                    return int(value)
                    
            elif '万' in amount_str:
                match = re.search(r'(\d+(?:,\d+)*(?:\.\d+)?)\s*万', amount_str)
                if match:
                    num_str = match.group(1).replace(',', '')
                    value = float(num_str) * 10_000
                    self.logger.debug(f"万単位変換: {num_str}万 → {int(value)}")
                    return int(value)
                    
            elif '千' in amount_str:
                match = re.search(r'(\d+(?:,\d+)*(?:\.\d+)?)\s*千', amount_str)
                if match:
                    num_str = match.group(1).replace(',', '')
                    value = float(num_str) * 1_000
                    self.logger.debug(f"千単位変換: {num_str}千 → {int(value)}")
                    return int(value)
            
            # 数値のみの場合
            match = re.search(r'(\d+(?:,\d+)*(?:\.\d+)?)', amount_str)
            if match:
                num_str = match.group(1).replace(',', '')
                value = int(float(num_str))
                self.logger.debug(f"数値のみ変換: {num_str} → {value}")
                return value
                
        except Exception as e:
            self.logger.warning(f"金額変換エラー: '{amount_str}' - {e}")
            
        self.logger.warning(f"金額変換失敗: '{amount_str}'")
        return 0
    
    def extract_case_links(self, page_num=1):
        """案件一覧から詳細ページのリンクを抽出"""
        url = f"{self.base_url}/page/{page_num}" if page_num > 1 else self.base_url
        self.logger.info(f"ページ {page_num} を読み込み中: {url}")
        
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)  # ページ読み込み待機
            
            # HTMLを保存（デバッグ用）
            html_content = self.driver.page_source
            self.save_html(html_content, f"page_{{page_num}}_list.html")
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 全てのリンクを調査（デバッグ）
            all_links = soup.find_all('a')
            link_debug_info = []
            
            for link in all_links:
                href = link.get('href', '')
                text = link.get_text().strip()[:50]  # 最初の50文字
                link_debug_info.append({
                    'href': href,
                    'text': text,
                    'is_sell_case': bool(re.search(r'/sell|案件', href + text))
                })
            
            self.save_debug_info(link_debug_info, f"page_{{page_num}}_all_links.json")
            self.logger.debug(f"ページ {page_num}: 全リンク数 {len(all_links)}")
            
            # 案件リンクを検索（複数パターン）
            link_patterns = [
                r'/sell/[a-zA-Z]+\d+',  # /sell/st1924, /sell/ss1845 等
                r'/sell_cases/\d+',      # /sell_cases/12345 等
                r'/sell/\d+',           # /sell/12345 等
            ]
            
            found_links = []
            for pattern in link_patterns:
                links = soup.find_all('a', href=re.compile(pattern))
                found_links.extend(links)
                self.logger.debug(f"パターン '{pattern}': {len(links)} 個のリンクを発見")
            
            # テキストベースでも検索
            case_links_by_text = soup.find_all('a', text=re.compile(r'案件|詳細|もっと見る'))
            found_links.extend(case_links_by_text)
            self.logger.debug(f"テキストベース検索: {len(case_links_by_text)} 個のリンクを発見")
            
            # 重複を除去してURLリストを作成
            unique_urls = set()
            case_links = []
            
            for link in found_links:
                href = link.get('href')
                if href and href not in unique_urls:
                    if not href.startswith('http'):
                        href = f"https://www.ondeck.jp{href}"
                    unique_urls.add(href)
                    case_links.append(href)
            
            # 見つからない場合の追加調査
            if not case_links:
                self.logger.warning(f"ページ {page_num}: 標準パターンで案件リンクが見つかりません。全リンクを調査します。")
                
                # 'sell' を含む全てのリンクを抽出
                for link in all_links:
                    href = link.get('href', '')
                    if 'sell' in href and href not in unique_urls:
                        if not href.startswith('http'):
                            href = f"https://www.ondeck.jp{href}"
                        unique_urls.add(href)
                        case_links.append(href)
                        self.logger.debug(f"追加発見: {href}")
            
            self.logger.info(f"ページ {page_num}: {len(case_links)}個の案件リンクを発見")
            self.save_debug_info({'case_links': case_links}, f"page_{{page_num}}_case_links.json")
            
            self.stats['links_found'] += len(case_links)
            return case_links
            
        except Exception as e:
            self.logger.error(f"ページ {page_num} の読み込みエラー: {e}")
            return []
    
    def extract_case_info(self, detail_url):
        """案件詳細ページから情報を抽出"""
        self.logger.info(f"詳細ページ確認: {detail_url}")
        self.stats['cases_processed'] += 1
        
        try:
            self.driver.get(detail_url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(2)
            
            # HTMLを保存（デバッグ用）
            html_content = self.driver.page_source
            case_id = detail_url.split('/')[-1]
            self.save_html(html_content, f"case_{{case_id}}_detail.html")
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # ページ全体のテキストを取得
            page_text = soup.get_text()
            
            # デバッグ用に重要な情報を抽出
            debug_info = {
                'url': detail_url,
                'title': soup.title.string if soup.title else '不明',
                'page_text_preview': page_text[:1000],  # 最初の1000文字
                'all_text_lines': [line.strip() for line in page_text.split('\n') if line.strip()][:100]  # 最初の100行
            }
            
            # 案件番号を抽出
            case_no = "不明"
            case_no_match = re.search(r'/sell/([a-zA-Z]+\d+)', detail_url)
            if case_no_match:
                case_no = case_no_match.group(1).upper()
            else:
                # ページ内から案件番号を探す
                case_no_patterns = [
                    r'案件No[.\s]*([A-Z]+\d+)',
                    r'案件番号[.\s]*([A-Z]+\d+)',
                    r'No[.\s]*([A-Z]+\d+)',
                ]
                for pattern in case_no_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        case_no = match.group(1).upper()
                        break
            
            debug_info['case_no'] = case_no
            
            # 業種を抽出（複数パターンで検索）
            industry = "不明"
            
            # パターン1: h1タグから業種を取得
            h1_tags = soup.find_all('h1')
            for h1_tag in h1_tags:
                industry_text = h1_tag.get_text().strip()
                if industry_text and ('業' in industry_text or '企業' in industry_text):
                    industry = industry_text
                    self.logger.debug(f"業種発見 (h1タグ): '{industry}'")
                    break
            
            # パターン2: ページタイトルから業種を取得
            if industry == "不明" and soup.title:
                title_text = soup.title.string or ""
                # "運送業 | Ｍ＆Ａ支援のオンデック" のような形式
                if '|' in title_text:
                    industry_candidate = title_text.split('|')[0].strip()
                    if industry_candidate and ('業' in industry_candidate or '企業' in industry_candidate):
                        industry = industry_candidate
                        self.logger.debug(f"業種発見 (タイトル): '{industry}'")
            
            # パターン3: ページ内テキストから業種を検索
            if industry == "不明":
                industry_patterns = [
                    r'業種[：:\s]*([^\n\r]+業)',
                    r'事業内容[：:\s]*([^\n\r]+業)',
                    r'([^あ-ん\s]+業)\s*（案件No',
                    r'([建設|運送|製造|小売|卸売|不動産|IT|通信|医療|介護|教育|コンサルタント|サービス|飲食|宿泊|物流|倉庫|機械|電気|化学|金融|保険|商社|貿易].*?業)',
                ]
                
                for i, pattern in enumerate(industry_patterns):
                    matches = re.findall(pattern, page_text)
                    if matches:
                        # 最初に見つかった業種を使用
                        candidate = matches[0].strip()
                        if len(candidate) > 1 and len(candidate) < 20:  # 妥当な長さの業種名
                            industry = candidate
                            self.logger.debug(f"業種発見 (パターン{i+1}): '{industry}'")
                            break
            
            # パターン4: よくある業種名のキーワード検索
            if industry == "不明":
                common_industries = [
                    '建設業', '建設コンサルタント業', '運送業', '物流業', '倉庫業',
                    '製造業', '機械製造業', '食品製造業', '化学工業',
                    '小売業', '卸売業', '商社', '貿易業',
                    'IT業', 'システム開発業', '通信業', 'ソフトウェア業',
                    '不動産業', '不動産仲介業', '不動産管理業',
                    '医療業', '介護業', '福祉業',
                    '飲食業', 'レストラン業', '宿泊業', 'ホテル業',
                    'コンサルタント業', '人材派遣業', 'サービス業',
                    '金融業', '保険業', 'リース業'
                ]
                
                for industry_name in common_industries:
                    if industry_name in page_text:
                        industry = industry_name
                        self.logger.debug(f"業種発見 (キーワード): '{industry}'")
                        break
            
            debug_info['industry'] = industry
            
            # 年商を検索
            revenue_text = None
            revenue_patterns = [
                r'年商[^約\n]*約?([^（\n]*(?:百万円|億円|万円))',
                r'年商[：:\s]*([^（\n]*(?:百万円|億円|万円))',
                r'年商.*?(\d+百万円)',
                r'売上[^約\n]*約?([^（\n]*(?:百万円|億円|万円))',
                r'売上高[^約\n]*約?([^（\n]*(?:百万円|億円|万円))',
            ]
            
            revenue_matches = []
            for i, pattern in enumerate(revenue_patterns):
                matches = re.findall(pattern, page_text)
                if matches:
                    revenue_matches.extend([(i, pattern, match) for match in matches])
                    if not revenue_text:  # 最初に見つかったものを使用
                        revenue_text = matches[0].strip()
                        self.logger.debug(f"年商発見 (パターン{i}): '{revenue_text}'")
            
            debug_info['revenue_patterns_found'] = revenue_matches
            debug_info['revenue_text'] = revenue_text
            
            if revenue_text:
                self.stats['cases_with_revenue'] += 1
            
            # 営業利益を検索
            profit_text = None
            profit_patterns = [
                r'営業利益[^約\n]*約?([^（\n]*(?:百万円|億円|万円))',
                r'営業利益[：:\s]*([^（\n]*(?:百万円|億円|万円))',
                r'営業利益.*?(\d+百万円)',
                r'経常利益[^約\n]*約?([^（\n]*(?:百万円|億円|万円))',
                r'純利益[^約\n]*約?([^（\n]*(?:百万円|億円|万円))',
            ]
            
            profit_matches = []
            for i, pattern in enumerate(profit_patterns):
                matches = re.findall(pattern, page_text)
                if matches:
                    profit_matches.extend([(i, pattern, match) for match in matches])
                    if not profit_text:  # 最初に見つかったものを使用
                        profit_text = matches[0].strip()
                        self.logger.debug(f"営業利益発見 (パターン{i}): '{profit_text}'")
            
            debug_info['profit_patterns_found'] = profit_matches
            debug_info['profit_text'] = profit_text
            
            if profit_text:
                self.stats['cases_with_profit'] += 1
            
            # 譲渡希望額を検索
            transfer_price = "未開示"
            transfer_patterns = [
                r'譲渡希望額[^約\n]*約?([^（\n]*(?:百万円|億円|万円))',
                r'譲渡希望額[：:\s]*([^（\n]*(?:百万円|億円|万円))',
                r'希望価格[^約\n]*約?([^（\n]*(?:百万円|億円|万円))',
            ]
            
            for pattern in transfer_patterns:
                match = re.search(pattern, page_text)
                if match:
                    transfer_price = match.group(1).strip()
                    self.logger.debug(f"譲渡希望額発見: '{transfer_price}'")
                    break
            
            debug_info['transfer_price'] = transfer_price
            
            # デバッグ情報を保存
            self.save_debug_info(debug_info, f"case_{{case_id}}_debug.json")
            
            return {
                'industry': industry,
                'case_no': case_no,
                'revenue_text': revenue_text,
                'profit_text': profit_text,
                'transfer_price': transfer_price,
                'detail_url': detail_url
            }
            
        except Exception as e:
            self.logger.error(f"詳細ページ取得エラー {detail_url}: {e}")
            return None
    
    def check_conditions(self, case_info):
        """年商・営業利益の条件をチェック"""
        if not case_info:
            self.logger.debug("case_info が None です")
            return False
            
        self.logger.debug(f"条件チェック開始: {case_info['case_no']}")
        self.logger.debug(f"  年商テキスト: '{case_info['revenue_text']}'")
        self.logger.debug(f"  営業利益テキスト: '{case_info['profit_text']}'")
        
        if not case_info['revenue_text']:
            self.logger.debug(f"年商情報なし: {case_info['case_no']}")
            return False
            
        if not case_info['profit_text']:
            self.logger.debug(f"営業利益情報なし: {case_info['case_no']}")
            return False
        
        # 年商条件チェック
        revenue_value = self.parse_amount(case_info['revenue_text'])
        is_revenue_match = revenue_value >= self.min_revenue
        
        self.logger.info(f"年商判定 [{case_info['case_no']}]: '{case_info['revenue_text']}' → {revenue_value:,}円 "
                        f"({'✓' if is_revenue_match else '✗'} 条件: {self.min_revenue:,}円以上)")
        
        if not is_revenue_match:
            return False
        
        # 営業利益条件チェック
        profit_value = self.parse_amount(case_info['profit_text'])
        is_profit_match = profit_value >= self.min_profit
        
        self.logger.info(f"営業利益判定 [{case_info['case_no']}]: '{case_info['profit_text']}' → {profit_value:,}円 "
                        f"({'✓' if is_profit_match else '✗'} 条件: {self.min_profit:,}円以上)")
        
        if is_profit_match:
            self.logger.info(f"★ 全条件合格案件: {case_info['case_no']} - {case_info['industry']}")
            self.stats['cases_matching_conditions'] += 1
        
        return is_profit_match
    
    def save_to_csv(self, filename='ondeck_ma_cases.csv'):
        """結果をCSVファイルに保存"""
        if not self.results:
            self.logger.warning("保存する結果がありません")
            return
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['業種', '案件No.', '年商', '営業利益', '譲渡希望額', 'リンク']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in self.results:
                writer.writerow(result)
        
        self.logger.info(f"結果をCSVファイルに保存しました: {filename} ({len(self.results)}件)")
    
    def display_results(self):
        """結果をコンソールに表示"""
        print(f"\n{'='*60}")
        print(f"処理統計:")
        print(f"  処理ページ数: {self.stats['pages_processed']}")
        print(f"  発見リンク数: {self.stats['links_found']}")
        print(f"  処理案件数: {self.stats['cases_processed']}")
        print(f"  年商情報有り: {self.stats['cases_with_revenue']}")
        print(f"  営業利益情報有り: {self.stats['cases_with_profit']}")
        print(f"  条件合格案件: {self.stats['cases_matching_conditions']}")
        print(f"{'='*60}")
        
        if not self.results:
            print("\n条件に合致する案件は見つかりませんでした。")
            if self.debug:
                print(f"デバッグファイルを確認してください: {self.debug_dir}/")
            return
        
        print(f"\n抽出結果: {len(self.results)}件")
        print(f"条件: 年商{self.min_revenue:,}円以上 AND 営業利益{self.min_profit:,}円以上")
        
        for i, result in enumerate(self.results, 1):
            print(f"\n【案件 {i}")
            print(f"業種: {result['業種']}")
            print(f"案件No.: {result['案件No.']}")
            print(f"年商: {result['年商']}")
            print(f"営業利益: {result['営業利益']}")
            print(f"譲渡希望額: {result['譲渡希望額']}")
            print(f"リンク: {result['リンク']}")
    
    def run(self, max_pages=5):
        """メイン実行関数"""
        self.logger.info("=" * 60)
        self.logger.info("M&A案件抽出を開始します（デバッグモード）")
        self.logger.info(f"条件: 年商{self.min_revenue:,}円以上 AND 営業利益{self.min_profit:,}円以上")
        self.logger.info(f"デバッグディレクトリ: {self.debug_dir}")
        self.logger.info("=" * 60)
        
        try:
            self.setup_driver()
            
            for page_num in range(1, max_pages + 1):
                self.logger.info(f"\n--- ページ {page_num} 処理開始 ---")
                self.stats['pages_processed'] += 1
                
                # 案件リンクを取得
                case_links = self.extract_case_links(page_num)
                
                if not case_links:
                    self.logger.info(f"ページ {page_num}: 案件リンクが見つかりません")
                    continue
                
                # 各案件の詳細をチェック（最初の5件のみデバッグ用）
                max_cases = len(case_links) if not self.debug else min(5, len(case_links))
                self.logger.info(f"ページ {page_num}: {max_cases}/{len(case_links)} 件の案件を処理します")
                
                for i, case_url in enumerate(case_links[:max_cases]):
                    self.logger.info(f"案件 {i+1}/{max_cases} を処理中...")
                    case_info = self.extract_case_info(case_url)
                    
                    if case_info and self.check_conditions(case_info):
                        # 条件を満たす案件を結果に追加
                        self.results.append({
                            '業種': case_info['industry'],
                            '案件No.': case_info['case_no'],
                            '年商': case_info['revenue_text'],
                            '営業利益': case_info['profit_text'],
                            '譲渡希望額': case_info['transfer_price'],
                            'リンク': case_info['detail_url']
                        })
                
                time.sleep(2)  # ページ間の待機
                
                if self.debug and page_num >= 2:  # デバッグ時は2ページまで
                    self.logger.info("デバッグモード: 2ページで処理を停止します")
                    break
            
            # 統計情報を保存
            self.save_debug_info(self.stats, "processing_stats.json")
            
            self.logger.info(f"\n処理完了")
            
            # 結果表示と保存
            self.display_results()
            self.save_to_csv()
            
        except KeyboardInterrupt:
            self.logger.info("処理が中断されました")
        except Exception as e:
            self.logger.error(f"実行エラー: {e}")
            import traceback
            self.logger.error(f"エラーの詳細:\n{traceback.format_exc()}")
        finally:
            if self.driver:
                self.driver.quit()
                self.logger.info("ブラウザを終了しました")

def main(max_pages=2, debug=True):
    """新しいメイン関数"""
    scraper = OnDeckScraper(debug=debug)
    return scraper.run(max_pages=max_pages)

if __name__ == "__main__":
    print("=" * 60)
    print("オンデック M&A案件抽出ツール (スタンドアロン実行)")
    print("=" * 60)
    
    results = main(max_pages=2, debug=False) # For testing
    
    if results and results['data']:
        print(f"\n抽出結果: {len(results['data'])}件")
        for i, result in enumerate(results['data'][:3], 1):
            print(f"\n【案件 {i}")
            print(f"業種: {result[0]}")
            print(f"案件No.: {result[1]}")
            print(f"年商: {result[2]}")
            print(f"営業利益: {result[3]}")
            print(f"譲渡希望額: {result[4]}")
            print(f"リンク: {result[5]}")
    else:
        print("\n条件に合致する案件は見つかりませんでした。")
