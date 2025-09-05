# main2.py - 日本M&Aセンター＆インテグループ＆NEWOLD CAPITAL＆オンデック専用スクレイピング
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
from functools import wraps
from typing import Optional, Dict, List, Set, Any
from dataclasses import dataclass, fields
from enum import Enum

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
class DataConverter:
    @staticmethod
    def parse_nihon_ma_revenue(revenue_text: str) -> bool:
        """日本M&Aセンターの売上高が5億円以上かチェック"""
        if not revenue_text:
            return False
        
        # 5億円以上のパターン
        valid_patterns = [
            "5億円～10億円",
            "10億円～20億円", 
            "20億円～50億円",
            "50億円～100億円",
            "100億円以上"
        ]
        
        return revenue_text in valid_patterns
    
    @staticmethod
    def convert_nihon_ma_revenue_to_million(revenue_text: str) -> str:
        """日本M&Aセンターの売上高を百万円単位に変換"""
        if not revenue_text:
            return revenue_text
        
        # 億円単位から百万円単位への変換マッピング
        conversion_map = {
            "2億円未満": "～200百万円",
            "2億円～5億円": "200～500百万円",
            "5億円～10億円": "500～1,000百万円",
            "10億円～20億円": "1,000～2,000百万円",
            "20億円～50億円": "2,000～5,000百万円",
            "50億円～100億円": "5,000～10,000百万円",
            "100億円以上": "10,000百万円以上"
        }
        
        return conversion_map.get(revenue_text, revenue_text)
    
    @staticmethod
    def convert_nihon_ma_profit_to_million(profit_text: str) -> str:
        """日本M&Aセンターの実態営業利益を百万円単位に変換"""
        if not profit_text:
            return profit_text
        
        # 既に百万円単位の場合はそのまま返す
        if "百万円" in profit_text:
            return profit_text
        
        # 億円単位の場合の変換処理
        if "億円" in profit_text:
            # レンジパターンの処理 (例: "3億円～8億円")
            range_pattern = r'([\d,]+(?:\.\d+)?)億円～([\d,]+(?:\.\d+)?)億円'
            range_match = re.search(range_pattern, profit_text)
            
            if range_match:
                try:
                    lower_value = float(range_match.group(1).replace(',', ''))
                    upper_value = float(range_match.group(2).replace(',', ''))
                    
                    lower_million = int(lower_value * 100)
                    upper_million = int(upper_value * 100)
                    
                    return f"{lower_million:,}～{upper_million:,}百万円"
                except ValueError:
                    pass
            
            # 単一値パターンの処理 (例: "5億円以上")
            single_over_pattern = r'([\d,]+(?:\.\d+)?)億円以上'
            single_over_match = re.search(single_over_pattern, profit_text)
            
            if single_over_match:
                try:
                    value = float(single_over_match.group(1).replace(',', ''))
                    million_value = int(value * 100)
                    return f"{million_value:,}百万円以上"
                except ValueError:
                    pass
            
            # 単一値パターンの処理 (例: "5億円")
            single_pattern = r'([\d,]+(?:\.\d+)?)億円'
            single_match = re.search(single_pattern, profit_text)
            
            if single_match:
                try:
                    value = float(single_match.group(1).replace(',', ''))
                    million_value = int(value * 100)
                    return f"{million_value:,}百万円"
                except ValueError:
                    pass
        
        # 万円単位の場合の処理
        if "万円" in profit_text and "百万円" not in profit_text:
            # レンジパターンの処理 (例: "3,000万円～8,000万円")
            range_pattern = r'([\d,]+)万円～([\d,]+)万円'
            range_match = re.search(range_pattern, profit_text)
            
            if range_match:
                try:
                    lower_value = int(range_match.group(1).replace(',', ''))
                    upper_value = int(range_match.group(2).replace(',', ''))
                    
                    # 万円を百万円に変換（10で割る）
                    lower_million = lower_value // 10
                    upper_million = upper_value // 10
                    
                    return f"{lower_million:,}～{upper_million:,}百万円"
                except ValueError:
                    pass
            
            # 単一値パターンの処理 (例: "5,000万円以上")
            single_over_pattern = r'([\d,]+)万円以上'
            single_over_match = re.search(single_over_pattern, profit_text)
            
            if single_over_match:
                try:
                    value = int(single_over_match.group(1).replace(',', ''))
                    million_value = value // 10
                    return f"{million_value:,}百万円以上"
                except ValueError:
                    pass
            
            # 単一値パターンの処理 (例: "5,000万円")
            single_pattern = r'([\d,]+)万円'
            single_match = re.search(single_pattern, profit_text)
            
            if single_match:
                try:
                    value = int(single_match.group(1).replace(',', ''))
                    million_value = value // 10
                    return f"{million_value:,}百万円"
                except ValueError:
                    pass
        
        return profit_text
    
    @staticmethod
    def parse_integroup_revenue(revenue_text: str) -> bool:
        """インテグループの売上高が5億円以上かチェック"""
        if not revenue_text:
            return False
        
        # 5億円以上のパターン（除外対象以外）
        exclude_patterns = [
            "～１億円",
            "１～５億円"
        ]
        
        # 除外対象でなければOK
        return revenue_text not in exclude_patterns
    
    @staticmethod
    def convert_integroup_revenue_to_million(revenue_text: str) -> str:
        """インテグループの売上高を百万円単位に変換"""
        if not revenue_text:
            return revenue_text
        
        # 億円単位から百万円単位への変換マッピング
        conversion_map = {
            "～１億円": "～100百万円",
            "１～５億円": "100～500百万円", 
            "５億円以上": "500百万円以上",
            "５～１０億円": "500～1,000百万円",
            "１０～３０億円": "1,000～3,000百万円",
            "３０億円以上": "3,000百万円以上"
        }
        
        return conversion_map.get(revenue_text, revenue_text)
    
    @staticmethod
    def parse_newold_revenue(revenue_text: str) -> bool:
        """NEWOLD CAPITALの売上高が3億円以上かチェック"""
        if not revenue_text:
            return False
        
        # 3億円以上のパターン
        valid_patterns = [
            "3億円～5億円",
            "5億円～10億円"
        ]
        
        return revenue_text in valid_patterns
    
    @staticmethod
    def convert_newold_revenue_to_million(revenue_text: str) -> str:
        """NEWOLD CAPITALの売上高を百万円単位に変換"""
        if not revenue_text:
            return revenue_text
        
        # 億円・万円単位から百万円単位への変換マッピング
        conversion_map = {
            "3,000万円～5,000万円": "30～50百万円",
            "5,000万円～1億円": "50～100百万円",
            "1億円～2億円": "100～200百万円",
            "2億円～3億円": "200～300百万円",
            "3億円～5億円": "300～500百万円",
            "5億円～10億円": "500～1,000百万円"
        }
        
        return conversion_map.get(revenue_text, revenue_text)
    
    @staticmethod
    def convert_newold_profit_to_million(profit_text: str) -> str:
        """NEWOLD CAPITALの営業利益を百万円単位に変換（桁区切りあり）"""
        if not profit_text:
            return profit_text
        
        # 既に百万円単位の場合はそのまま返す
        if "百万円" in profit_text:
            return profit_text
        
        # 万円単位の場合の処理
        if "万円" in profit_text and "百万円" not in profit_text:
            # レンジパターンの処理 (例: "2,000万円～5,000万円")
            range_pattern = r'([\d,]+)万円～([\d,]+)万円'
            range_match = re.search(range_pattern, profit_text)
            
            if range_match:
                try:
                    lower_value = int(range_match.group(1).replace(',', ''))
                    upper_value = int(range_match.group(2).replace(',', ''))
                    
                    # 万円を百万円に変換（10で割る）
                    lower_million = lower_value // 10
                    upper_million = upper_value // 10
                    
                    return f"{lower_million:,}～{upper_million:,}百万円"
                except ValueError:
                    pass
            
            # 単一値パターンの処理 (例: "5,000万円")
            single_pattern = r'([\d,]+)万円'
            single_match = re.search(single_pattern, profit_text)
            
            if single_match:
                try:
                    value = int(single_match.group(1).replace(',', ''))
                    million_value = value // 10
                    return f"{million_value:,}百万円"
                except ValueError:
                    pass
        
        # 億円単位の場合の処理
        if "億円" in profit_text:
            # レンジパターンの処理 (例: "1億円～2億円")
            range_pattern = r'([\d,]+(?:\.\d+)?)億円～([\d,]+(?:\.\d+)?)億円'
            range_match = re.search(range_pattern, profit_text)
            
            if range_match:
                try:
                    lower_value = float(range_match.group(1).replace(',', ''))
                    upper_value = float(range_match.group(2).replace(',', ''))
                    
                    lower_million = int(lower_value * 100)
                    upper_million = int(upper_value * 100)
                    
                    return f"{lower_million:,}～{upper_million:,}百万円"
                except ValueError:
                    pass
            
            # 単一値パターンの処理 (例: "2億円")
            single_pattern = r'([\d,]+(?:\.\d+)?)億円'
            single_match = re.search(single_pattern, profit_text)
            
            if single_match:
                try:
                    value = float(single_match.group(1).replace(',', ''))
                    million_value = int(value * 100)
                    return f"{million_value:,}百万円"
                except ValueError:
                    pass
        
        return profit_text
    
    @staticmethod
    def convert_newold_price_to_million(price_text: str) -> str:
        """NEWOLD CAPITALの譲渡希望額を百万円単位に変換し、不要テキストを除去（桁区切りあり）"""
        if not price_text:
            return price_text
        
        # 不要なテキストを除去（応相談、括弧内など）
        cleaned_text = re.sub(r'[（(][^）)]*[）)]', '', price_text)  # 括弧とその中身を除去
        cleaned_text = cleaned_text.strip()
        
        # 億円単位の場合の処理
        if "億円" in cleaned_text:
            # レンジパターンの処理 (例: "2億5,000万円～3億円")
            range_pattern = r'([\d,]+)億([\d,]*)?万?円?～([\d,]+)億([\d,]*)?万?円?'
            range_match = re.search(range_pattern, cleaned_text)
            
            if range_match:
                try:
                    # 下限値の計算
                    lower_oku = int(range_match.group(1).replace(',', ''))
                    lower_man = int(range_match.group(2).replace(',', '')) if range_match.group(2) else 0
                    lower_total_man = lower_oku * 10000 + lower_man
                    lower_million = lower_total_man // 10
                    
                    # 上限値の計算
                    upper_oku = int(range_match.group(3).replace(',', ''))
                    upper_man = int(range_match.group(4).replace(',', '')) if range_match.group(4) else 0
                    upper_total_man = upper_oku * 10000 + upper_man
                    upper_million = upper_total_man // 10
                    
                    return f"{lower_million:,}～{upper_million:,}百万円"
                except ValueError:
                    pass
            
            # 単一値パターンの処理 (例: "2億5,000万円")
            single_pattern = r'([\d,]+)億([\d,]*)?万?円?'
            single_match = re.search(single_pattern, cleaned_text)
            
            if single_match:
                try:
                    oku_value = int(single_match.group(1).replace(',', ''))
                    man_value = int(single_match.group(2).replace(',', '')) if single_match.group(2) else 0
                    total_man = oku_value * 10000 + man_value
                    million_value = total_man // 10
                    return f"{million_value:,}百万円"
                except ValueError:
                    pass
        
        # 万円のみの場合の処理
        elif "万円" in cleaned_text and "億円" not in cleaned_text:
            # レンジパターンの処理 (例: "5,000万円～8,000万円")
            range_pattern = r'([\d,]+)万円～([\d,]+)万円'
            range_match = re.search(range_pattern, cleaned_text)
            
            if range_match:
                try:
                    lower_value = int(range_match.group(1).replace(',', ''))
                    upper_value = int(range_match.group(2).replace(',', ''))
                    
                    # 万円を百万円に変換（10で割る）
                    lower_million = lower_value // 10
                    upper_million = upper_value // 10
                    
                    return f"{lower_million:,}～{upper_million:,}百万円"
                except ValueError:
                    pass
            
            # 単一値パターンの処理 (例: "5,000万円")
            single_pattern = r'([\d,]+)万円'
            single_match = re.search(single_pattern, cleaned_text)
            
            if single_match:
                try:
                    value = int(single_match.group(1).replace(',', ''))
                    million_value = value // 10
                    return f"{million_value:,}百万円"
                except ValueError:
                    pass
        
        return cleaned_text
    
    @staticmethod
    def parse_newold_profit(profit_text: str) -> bool:
        """NEWOLD CAPITALの営業利益が3,000万円以上かチェック"""
        if not profit_text:
            return False
        
        # レンジの上限値を抽出して判定
        # 例: "2,000万円～5,000万円" → 5,000万円 ≥ 3,000万円 → OK
        
        # まず「万円」単位での抽出を試行
        range_pattern = r'([\d,]+)万円～([\d,]+)万円'
        range_match = re.search(range_pattern, profit_text)
        
        if range_match:
            upper_value_str = range_match.group(2).replace(',', '')
            try:
                upper_value = int(upper_value_str)
                return upper_value >= 3000  # 3,000万円以上
            except ValueError:
                pass
        
        # 「億円」が含まれる場合は確実にOK
        if '億円' in profit_text:
            return True
        
        # 単一値の場合の処理
        single_pattern = r'([\d,]+)万円'
        single_match = re.search(single_pattern, profit_text)
        
        if single_match:
            value_str = single_match.group(1).replace(',', '')
            try:
                value = int(value_str)
                return value >= 3000
            except ValueError:
                pass
        
        return False
    
    @staticmethod
    def parse_nihon_ma_profit(profit_text: str) -> bool:
        """日本M&Aセンターの実態営業利益が5,000万円以上かチェック"""
        if not profit_text:
            return False
        
        # レンジの上限値を抽出して判定
        # 例: "3,000万円～8,000万円" → 8,000万円 ≥ 5,000万円 → OK
        
        # まず「万円」単位での抽出を試行
        range_pattern = r'([\d,]+)万円～([\d,]+)万円'
        range_match = re.search(range_pattern, profit_text)
        
        if range_match:
            upper_value_str = range_match.group(2).replace(',', '')
            try:
                upper_value = int(upper_value_str)
                return upper_value >= 5000  # 5,000万円以上
            except ValueError:
                pass
        
        # 「億円」が含まれる場合は確実にOK
        if '億円' in profit_text:
            return True
        
        # 単一値の場合の処理
        single_pattern = r'([\d,]+)万円'
        single_match = re.search(single_pattern, profit_text)
        
        if single_match:
            value_str = single_match.group(1).replace(',', '')
            try:
                value = int(value_str)
                return value >= 5000
            except ValueError:
                pass
        
        return False
    
    @staticmethod
    def clean_integroup_features(features_text: str) -> str:
        """インテグループの特色テキストから不要な宣伝文を除去"""
        if not features_text:
            return features_text
        
        # 除去対象のパターン（部分一致で検索）
        unwanted_patterns = [
            r'M&Aを成功させるための秘訣.*?を無料で進呈します。',
            r'最新のM&A事例を踏まえて.*?お気軽にお問合せください。',
            r'『中小企業M&Aの真実』.*?を無料で進呈します。',
            r'無料価値算定サービス.*?お気軽にお問合せください。',
            r'M&Aを成功させるための.*?無料で進呈します。',
            r'企業価値を無料で算定.*?お問合せください。'
        ]
        
        cleaned_text = features_text
        
        # パターンマッチングで不要部分を除去
        for pattern in unwanted_patterns:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.DOTALL | re.IGNORECASE)
        
        # より具体的な除去（キーワードベース）
        unwanted_keywords = [
            "M&Aを成功させるための秘訣",
            "中小企業M&Aの真実",
            "無料で進呈します",
            "無料価値算定サービス",
            "企業価値を無料で算定",
            "強引に売却を勧めたり",
            "お気軽にお問合せください"
        ]
        
        # 行ごとに処理して不要行を除去
        lines = cleaned_text.split('\n')
        filtered_lines = []
        
        for line in lines:
            line = line.strip()
            if line and not any(keyword in line for keyword in unwanted_keywords):
                filtered_lines.append(line)
        
        # 空行を除去して再結合
        result = '\n'.join(filtered_lines).strip()
        
        # 最終的な整理
        # 連続する空行を単一の空行に変換
        result = re.sub(r'\n\s*\n\s*\n+', '\n\n', result)
        
        return result
    
    @staticmethod
    def parse_ondeck_revenue(revenue_text: str) -> bool:
        """オンデックの売上高が300百万円以上かチェック（強化版）"""
        if not revenue_text:
            return False
        
        # 「応相談」などの非数値テキストは除外
        if any(keyword in revenue_text for keyword in ['応相談', '非開示', '未開示', '-']):
            return False
        
        try:
            # 「約」や括弧書きなどを除去
            cleaned_text = re.sub(r'約|[（(][^）)]*[）)]', '', revenue_text)
            
            # レンジ表記の場合は下限値で判定
            range_pattern = r'([\d,]+)～([\d,]+)'
            range_match = re.search(range_pattern, cleaned_text)
            
            if range_match:
                lower_value_str = range_match.group(1).replace(',', '')
                lower_value = int(lower_value_str)
                return lower_value >= 300  # 300百万円以上
            
            # 単一値の場合
            single_pattern = r'([\d,]+)'
            single_match = re.search(single_pattern, cleaned_text)
            
            if single_match:
                value_str = single_match.group(1).replace(',', '')
                value = int(value_str)
                return value >= 300
        
        except (ValueError, AttributeError) as e:
            logging.warning(f"Revenue parsing error for '{revenue_text}': {e}")
            return False
        
        return False
    
    @staticmethod
    def parse_ondeck_profit(profit_text: str) -> bool:
        """オンデックの営業利益が30百万円以上かチェック（修正版）"""
        if not profit_text:
            return False
        
        # マイナス値（▲や-が含まれる）は除外
        if '▲' in profit_text or '－' in profit_text or profit_text.strip().startswith('-'):
            logging.info(f"    -> Excluding negative profit: {profit_text}")
            return False
        
        # 「約」や括弧書きなどを無視して数字部分のみを抽出
        # レンジ表記の場合は下限値で判定
        range_pattern = r'([\d,]+)～([\d,]+)'
        range_match = re.search(range_pattern, profit_text)
        
        if range_match:
            lower_value_str = range_match.group(1).replace(',', '')
            try:
                lower_value = int(lower_value_str)
                return lower_value >= 30  # 30百万円以上
            except ValueError:
                pass
        
        # 単一値の場合の処理
        single_pattern = r'([\d,]+)'
        single_match = re.search(single_pattern, profit_text)
        
        if single_match:
            value_str = single_match.group(1).replace(',', '')
            try:
                value = int(value_str)
                return value >= 30
            except ValueError:
                pass
        
        return False
    
    @staticmethod
    def clean_ondeck_revenue(revenue_text: str) -> str:
        """オンデックの売上高テキストをクリーニング"""
        if not revenue_text:
            return revenue_text
        
        # 「約」を除去
        cleaned_text = re.sub(r'^約\s*', '', revenue_text)
        
        # 括弧内のテキストを除去（直近期実績など）
        cleaned_text = re.sub(r'[（(][^）)]*[）)]', '', cleaned_text)
        
        # 余分な空白を除去
        cleaned_text = cleaned_text.strip()
        
        # 桁区切りカンマを追加
        if '～' in cleaned_text:
            # レンジ表記の場合
            parts = cleaned_text.split('～')
            if len(parts) == 2:
                try:
                    lower = int(parts[0].replace('百万円', '').replace(',', ''))
                    upper = int(parts[1].replace('百万円', '').replace(',', ''))
                    return f"{lower:,}～{upper:,}百万円"
                except ValueError:
                    pass
        else:
            # 単一値の場合
            try:
                value = int(cleaned_text.replace('百万円', '').replace(',', ''))
                return f"{value:,}百万円"
            except ValueError:
                pass
        
        return cleaned_text
    
    @staticmethod
    def clean_ondeck_profit(profit_text: str) -> str:
        """オンデックの営業利益テキストをクリーニング"""
        if not profit_text:
            return profit_text
        
        # 「約」を除去
        cleaned_text = re.sub(r'^約\s*', '', profit_text)
        
        # 括弧内のテキストを除去（直近期実績修正後など）
        cleaned_text = re.sub(r'[（(][^）)]*[）)]', '', cleaned_text)
        
        # 余分な空白を除去
        cleaned_text = cleaned_text.strip()
        
        # 桁区切りカンマを追加
        if '～' in cleaned_text:
            # レンジ表記の場合
            parts = cleaned_text.split('～')
            if len(parts) == 2:
                try:
                    lower = int(parts[0].replace('百万円', '').replace(',', ''))
                    upper = int(parts[1].replace('百万円', '').replace(',', ''))
                    return f"{lower:,}～{upper:,}百万円"
                except ValueError:
                    pass
        else:
            # 単一値の場合
            try:
                value = int(cleaned_text.replace('百万円', '').replace(',', ''))
                return f"{value:,}百万円"
            except ValueError:
                pass
        
        return cleaned_text
    
    @staticmethod
    def clean_ondeck_price(price_text: str) -> str:
        """オンデックの譲渡希望額テキストをクリーニング"""
        if not price_text:
            return price_text
        
        # 「応相談」のみの場合はそのまま返す
        if price_text.strip() == "応相談":
            return price_text
        
        # 括弧内の「応相談」を除去
        cleaned_text = re.sub(r'[（(]応相談[）)]', '', price_text)
        
        # 余分な空白を除去
        cleaned_text = cleaned_text.strip()
        
        # 桁区切りカンマを追加
        try:
            value = int(cleaned_text.replace('百万円', '').replace(',', ''))
            return f"{value:,}百万円"
        except ValueError:
            pass
        
        return cleaned_text
    
    @staticmethod
    def format_financial_text(text: str) -> str:
        """財務テキストの整形"""
        return text.strip() if text else "-"

# --- パーサークラス ---
class NihonMACenterParser:
    """日本M&Aセンター専用パーサー"""
    
    @staticmethod
    def parse_list_page(html_content: str) -> List[RawDealData]:
        """一覧ページのパース"""
        soup = BeautifulSoup(html_content, 'lxml')
        results = []
        
        # デバッグ用: HTMLファイル保存
        if CONFIG.get('debug', {}).get('save_html_files', False):
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            debug_file = f"debug/debug_nihon_ma_{timestamp}.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logging.info(f"Debug: HTML saved to {debug_file}")
        
        # 案件アイテムを抽出（実際のHTMLに合わせてセレクターを調整）
        # 推測されるセレクターパターンを複数試行
        possible_selectors = [
            'div.anken_item',
            'div.case_item', 
            'li.anken_list',
            'tr.anken_row',
            'div[class*="anken"]',
            'div[class*="case"]',
            'table tr',
            'tbody tr'
        ]
        
        items = []
        for selector in possible_selectors:
            items = soup.select(selector)
            if items and len(items) > 1:  # ヘッダー行を除く
                logging.info(f"    -> Found {len(items)} items using selector: {selector}")
                break
        
        if not items:
            # フォールバック: aタグでneeds_convey_single.phpを含むリンクを探す
            links = soup.find_all('a', href=re.compile(r'needs_convey_single\.php\?no=\d+'))
            logging.info(f"    -> Fallback: Found {len(links)} detail links")
            
            for link in links:
                try:
                    href = link.get('href', '')
                    deal_id_match = re.search(r'no=(\d+)', href)
                    if not deal_id_match:
                        continue
                    
                    deal_id = f"No.{deal_id_match.group(1)}"
                    
                    # リンク要素の親要素から情報を抽出
                    parent_element = link.parent
                    for _ in range(5):  # 最大5階層上まで遡る
                        if parent_element and parent_element.name != 'body':
                            parent_element = parent_element.parent
                        else:
                            break
                    
                    if parent_element:
                        deal_data = NihonMACenterParser._extract_deal_from_element(parent_element, deal_id, href)
                        if deal_data:
                            results.append(deal_data)
                
                except Exception as e:
                    logging.error(f"    -> Error parsing fallback item: {e}")
                    continue
        else:
            # 通常のパース処理
            for i, item in enumerate(items):
                try:
                    # ヘッダー行をスキップ
                    if i == 0 and item.name == 'tr':
                        item_text = item.get_text().lower()
                        if any(header in item_text for header in ['案件', 'no', '番号', 'タイトル', '売上']):
                            logging.info(f"    -> Skipping header row: {item.get_text()[:50]}")
                            continue
                    
                    deal_data = NihonMACenterParser._parse_item(item)
                    if deal_data:
                        results.append(deal_data)
                
                except Exception as e:
                    logging.error(f"    -> Error parsing item {i+1}: {e}")
                    continue
        
        # 売上高フィルタリング
        filtered_results = []
        for deal in results:
            if DataConverter.parse_nihon_ma_revenue(deal.revenue_text):
                filtered_results.append(deal)
                logging.info(f"    -> Deal {deal.deal_id} meets revenue criteria: {deal.revenue_text}")
            else:
                logging.info(f"    -> Skipping deal {deal.deal_id}: Revenue '{deal.revenue_text}' doesn't meet criteria")
        
        return filtered_results
    
    @staticmethod
    def _parse_item(item: Tag) -> Optional[RawDealData]:
        """個別アイテムのパース"""
        try:
            # 案件番号の抽出
            deal_id = NihonMACenterParser._extract_deal_id(item)
            if not deal_id:
                return None
            
            # タイトルの抽出
            title = NihonMACenterParser._extract_title(item)
            
            # 売上高の抽出
            revenue_text = NihonMACenterParser._extract_revenue(item)
            
            # 詳細ページリンクの抽出
            link = NihonMACenterParser._extract_link(item)
            if not link:
                return None
            
            return RawDealData(
                site_name="日本M&Aセンター",
                deal_id=deal_id,
                title=title,
                link=link,
                revenue_text=revenue_text,
                profit_text="",  # 詳細ページで取得
                location_text="",  # 詳細ページで取得
                price_text="",  # 詳細ページで取得
                features_text=""  # 詳細ページで取得
            )
        
        except Exception as e:
            logging.error(f"    -> Error parsing item: {e}")
            return None
    
    @staticmethod
    def _extract_deal_from_element(element: Tag, deal_id: str, link: str) -> Optional[RawDealData]:
        """要素から案件データを抽出（フォールバック用）"""
        try:
            element_text = element.get_text()
            
            # タイトル抽出（案件番号の後の文字列を推測）
            title_patterns = [
                rf'{deal_id.replace("No.", "")}[^\w]*([^\n]+)',
                r'【([^】]+)】',
                r'■([^■\n]+)',
            ]
            
            title = "案件詳細"  # デフォルト
            for pattern in title_patterns:
                match = re.search(pattern, element_text)
                if match:
                    candidate = match.group(1).strip()
                    if len(candidate) > 2 and len(candidate) < 100:
                        title = candidate
                        break
            
            # 売上高抽出
            revenue_patterns = [
                r'(2億円未満|2億円～5億円|5億円～10億円|10億円～20億円|20億円～50億円|50億円～100億円|100億円以上)'
            ]
            
            revenue_text = ""
            for pattern in revenue_patterns:
                match = re.search(pattern, element_text)
                if match:
                    revenue_text = match.group(1)
                    break
            
            # 完全なURLに変換
            if link.startswith('/'):
                link = f"https://www.nihon-ma.co.jp{link}"
            elif not link.startswith('http'):
                link = f"https://www.nihon-ma.co.jp/anken/{link}"
            
            return RawDealData(
                site_name="日本M&Aセンター",
                deal_id=deal_id,
                title=title,
                link=link,
                revenue_text=revenue_text,
                profit_text="",
                location_text="",
                price_text="",
                features_text=""
            )
        
        except Exception as e:
            logging.error(f"    -> Error extracting from element: {e}")
            return None
    
    @staticmethod
    def _extract_deal_id(item: Tag) -> str:
        """案件IDの抽出"""
        # 複数のパターンを試行
        id_patterns = [
            r'No\.(\d+)',
            r'案件番号[：:\s]*(\d+)',
            r'案件ID[：:\s]*(\d+)',
        ]
        
        item_text = item.get_text()
        for pattern in id_patterns:
            match = re.search(pattern, item_text)
            if match:
                return f"No.{match.group(1)}"
        
        # リンクからIDを抽出
        link_element = item.find('a', href=re.compile(r'no=\d+'))
        if link_element:
            href = link_element.get('href', '')
            match = re.search(r'no=(\d+)', href)
            if match:
                return f"No.{match.group(1)}"
        
        return ""
    
    @staticmethod
    def _extract_title(item: Tag) -> str:
        """タイトルの抽出"""
        # 複数のセレクターパターンを試行
        title_selectors = [
            'h3', 'h4', '.title', '.anken_title', 
            '.case_title', 'strong', 'b'
        ]
        
        for selector in title_selectors:
            element = item.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if len(title) > 2 and len(title) < 100:
                    return title
        
        # フォールバック: テキストから推測
        item_text = item.get_text()
        lines = [line.strip() for line in item_text.split('\n') if line.strip()]
        
        for line in lines:
            if (len(line) > 2 and len(line) < 100 and 
                not re.match(r'^No\.\d+', line) and
                '億円' not in line and '万円' not in line):
                return line
        
        return "案件詳細"
    
    @staticmethod
    def _extract_revenue(item: Tag) -> str:
        """売上高の抽出"""
        revenue_patterns = [
            r'(2億円未満)',
            r'(2億円～5億円)',
            r'(5億円～10億円)',
            r'(10億円～20億円)',
            r'(20億円～50億円)',
            r'(50億円～100億円)',
            r'(100億円以上)'
        ]
        
        item_text = item.get_text()
        for pattern in revenue_patterns:
            match = re.search(pattern, item_text)
            if match:
                return match.group(1)
        
        return ""
    
    @staticmethod
    def _extract_link(item: Tag) -> str:
        """詳細ページリンクの抽出"""
        # needs_convey_single.phpを含むリンクを探す
        link_element = item.find('a', href=re.compile(r'needs_convey_single\.php'))
        if link_element:
            href = link_element.get('href', '')
            if href.startswith('/'):
                return f"https://www.nihon-ma.co.jp{href}"
            elif not href.startswith('http'):
                return f"https://www.nihon-ma.co.jp/anken/{href}"
            else:
                return href
        
        return ""

class IntegroupParser:
    """インテグループ専用パーサー"""
    
    @staticmethod
    def parse_list_page(html_content: str) -> List[RawDealData]:
        """一覧ページのパース"""
        soup = BeautifulSoup(html_content, 'lxml')
        results = []
        
        # デバッグ用: HTMLファイル保存
        if CONFIG.get('debug', {}).get('save_html_files', False):
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            debug_file = f"debug/debug_integroup_{timestamp}.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logging.info(f"Debug: HTML saved to {debug_file}")
        
        # 案件アイテムを抽出
        possible_selectors = [
            'div.sell-item',
            'div.case-item',
            'article.sell',
            'div.sell-card',
            'div[class*="sell"]',
            'div[class*="case"]',
            'li.item',
            'div.item'
        ]
        
        items = []
        for selector in possible_selectors:
            items = soup.select(selector)
            if items:
                logging.info(f"    -> Found {len(items)} items using selector: {selector}")
                break
        
        if not items:
            # フォールバック: 詳細ページリンクから逆算
            links = soup.find_all('a', href=re.compile(r'/sell/\d+\.html'))
            logging.info(f"    -> Fallback: Found {len(links)} detail links")
            
            for link in links:
                try:
                    href = link.get('href', '')
                    
                    # 親要素から情報を抽出
                    parent_element = link.parent
                    for _ in range(5):  # 最大5階層上まで遡る
                        if parent_element and parent_element.name != 'body':
                            parent_element = parent_element.parent
                        else:
                            break
                    
                    if parent_element:
                        deal_data = IntegroupParser._extract_deal_from_element(parent_element, href)
                        if deal_data:
                            results.append(deal_data)
                
                except Exception as e:
                    logging.error(f"    -> Error parsing fallback item: {e}")
                    continue
        else:
            # 通常のパース処理
            for i, item in enumerate(items):
                try:
                    deal_data = IntegroupParser._parse_item(item)
                    if deal_data:
                        results.append(deal_data)
                
                except Exception as e:
                    logging.error(f"    -> Error parsing item {i+1}: {e}")
                    continue
        
        # 売上高フィルタリング
        filtered_results = []
        for deal in results:
            if DataConverter.parse_integroup_revenue(deal.revenue_text):
                # 売上高を百万円単位に変換
                deal.revenue_text = DataConverter.convert_integroup_revenue_to_million(deal.revenue_text)
                filtered_results.append(deal)
                logging.info(f"    -> Deal {deal.deal_id} meets revenue criteria: {deal.revenue_text}")
            else:
                logging.info(f"    -> Skipping deal {deal.deal_id}: Revenue '{deal.revenue_text}' doesn't meet criteria")
        
        return filtered_results
    
    @staticmethod
    def _parse_item(item: Tag) -> Optional[RawDealData]:
        """個別アイテムのパース"""
        try:
            # 案件番号の抽出
            deal_id = IntegroupParser._extract_deal_id(item)
            if not deal_id:
                return None
            
            # タイトルの抽出
            title = IntegroupParser._extract_title(item)
            
            # 売上高の抽出
            revenue_text = IntegroupParser._extract_revenue(item)
            
            # エリア（所在地）の抽出
            location_text = IntegroupParser._extract_location(item)
            
            # 詳細ページリンクの抽出
            link = IntegroupParser._extract_link(item)
            if not link:
                return None
            
            return RawDealData(
                site_name="インテグループ",
                deal_id=deal_id,
                title=title,
                link=link,
                revenue_text=revenue_text,
                profit_text="-",  # インテグループは営業利益記載なし
                location_text=location_text,
                price_text="-",  # インテグループは価格記載なし
                features_text=""  # 詳細ページで取得
            )
        
        except Exception as e:
            logging.error(f"    -> Error parsing item: {e}")
            return None
    
    @staticmethod
    def _extract_deal_from_element(element: Tag, link: str) -> Optional[RawDealData]:
        """要素から案件データを抽出（フォールバック用）"""
        try:
            element_text = element.get_text()
            
            # 案件IDを抽出
            deal_id_patterns = [
                r'(S\d{6})',
                r'案件番号[：:\s]*(S\d{6})',
            ]
            
            deal_id = ""
            for pattern in deal_id_patterns:
                match = re.search(pattern, element_text)
                if match:
                    deal_id = match.group(1)
                    break
            
            if not deal_id:
                return None
            
            # タイトル抽出
            title_lines = [line.strip() for line in element_text.split('\n') if line.strip()]
            title = "案件詳細"
            for line in title_lines:
                if (len(line) > 5 and len(line) < 100 and 
                    deal_id not in line and '億円' not in line):
                    title = line
                    break
            
            # 売上高抽出
            revenue_patterns = [
                r'(～１億円|１～５億円|５億円以上|５～１０億円|１０～３０億円|３０億円以上)'
            ]
            
            revenue_text = ""
            for pattern in revenue_patterns:
                match = re.search(pattern, element_text)
                if match:
                    revenue_text = match.group(1)
                    break
            
            # エリア抽出
            location_patterns = [
                r'(北海道|東北|関東|中部|近畿|中国|四国|九州|沖縄)地方',
                r'(東京都|大阪府|愛知県|神奈川県|埼玉県|千葉県|兵庫県|福岡県|静岡県|茨城県|広島県|京都府|新潟県|宮城県|長野県|岐阜県|栃木県|群馬県|岡山県|三重県|熊本県|鹿児島県|沖縄県|滋賀県|山口県|愛媛県|青森県|岩手県|秋田県|山形県|福島県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)'
            ]
            
            location_text = ""
            for pattern in location_patterns:
                match = re.search(pattern, element_text)
                if match:
                    location_text = match.group(1)
                    break
            
            # 完全なURLに変換
            if link.startswith('/'):
                link = f"https://www.integroup.jp{link}"
            elif not link.startswith('http'):
                link = f"https://www.integroup.jp{link}"
            
            return RawDealData(
                site_name="インテグループ",
                deal_id=deal_id,
                title=title,
                link=link,
                revenue_text=revenue_text,
                profit_text="-",
                location_text=location_text,
                price_text="-",
                features_text=""
            )
        
        except Exception as e:
            logging.error(f"    -> Error extracting from element: {e}")
            return None
    
    @staticmethod
    def _extract_deal_id(item: Tag) -> str:
        """案件IDの抽出（S + 6桁数字）"""
        item_text = item.get_text()
        
        # S + 6桁数字のパターン
        id_patterns = [
            r'(S\d{6})',
            r'案件番号[：:\s]*(S\d{6})',
        ]
        
        for pattern in id_patterns:
            match = re.search(pattern, item_text)
            if match:
                return match.group(1)
        
        return ""
    
    @staticmethod
    def _extract_title(item: Tag) -> str:
        """タイトルの抽出"""
        # 複数のセレクターパターンを試行
        title_selectors = [
            'h2', 'h3', 'h4', '.title', '.sell-title', 
            '.case-title', 'strong', 'b', '.item-title'
        ]
        
        for selector in title_selectors:
            element = item.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if len(title) > 5 and len(title) < 100:
                    return title
        
        # フォールバック: テキストから推測
        item_text = item.get_text()
        lines = [line.strip() for line in item_text.split('\n') if line.strip()]
        
        for line in lines:
            if (len(line) > 5 and len(line) < 100 and 
                not re.match(r'^S\d{6}', line) and
                '億円' not in line and '地方' not in line):
                return line
        
        return "案件詳細"
    
    @staticmethod
    def _extract_revenue(item: Tag) -> str:
        """売上高の抽出"""
        revenue_patterns = [
            r'(～１億円)',
            r'(１～５億円)',
            r'(５億円以上)',
            r'(５～１０億円)',
            r'(１０～３０億円)',
            r'(３０億円以上)'
        ]
        
        item_text = item.get_text()
        for pattern in revenue_patterns:
            match = re.search(pattern, item_text)
            if match:
                return match.group(1)
        
        return ""
    
    @staticmethod
    def _extract_location(item: Tag) -> str:
        """エリア（所在地）の抽出"""
        location_patterns = [
            r'(北海道|東北|関東|中部|近畿|中国|四国|九州|沖縄)地方'
        ]
        
        item_text = item.get_text()
        for pattern in location_patterns:
            match = re.search(pattern, item_text)
            if match:
                return f"{match.group(1)}地方"
        
        return ""
    
    @staticmethod
    def _extract_link(item: Tag) -> str:
        """詳細ページリンクの抽出"""
        # /sell/数字.htmlパターンのリンクを探す
        link_element = item.find('a', href=re.compile(r'/sell/\d+\.html'))
        if link_element:
            href = link_element.get('href', '')
            if href.startswith('/'):
                return f"https://www.integroup.jp{href}"
            elif not href.startswith('http'):
                return f"https://www.integroup.jp/{href}"
            else:
                return href
        
        return ""

class NewoldCapitalParser:
    """NEWOLD CAPITAL専用パーサー"""
    
    @staticmethod
    def parse_list_page(html_content: str) -> List[RawDealData]:
        """一覧ページのパース"""
        soup = BeautifulSoup(html_content, 'lxml')
        results = []
        
        # デバッグ用: HTMLファイル保存
        if CONFIG.get('debug', {}).get('save_html_files', False):
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            debug_file = f"debug/debug_newold_{timestamp}.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logging.info(f"Debug: HTML saved to {debug_file}")
        
        # NEWOLD CAPITAL特有のセレクターを使用
        items = soup.select('a.p-projects-list__item')
        
        if items:
            logging.info(f"    -> Found {len(items)} items using selector: a.p-projects-list__item")
        else:
            # フォールバック: 詳細ページリンクから逆算
            links = soup.find_all('a', href=re.compile(r'/anken-list/\d+/'))
            logging.info(f"    -> Fallback: Found {len(links)} detail links")
            
            for link in links:
                try:
                    href = link.get('href', '')
                    
                    # 親要素から情報を抽出
                    parent_element = link.parent
                    for _ in range(5):  # 最大5階層上まで遡る
                        if parent_element and parent_element.name != 'body':
                            parent_element = parent_element.parent
                        else:
                            break
                    
                    if parent_element:
                        deal_data = NewoldCapitalParser._extract_deal_from_element(parent_element, href)
                        if deal_data:
                            results.append(deal_data)
                
                except Exception as e:
                    logging.error(f"    -> Error parsing fallback item: {e}")
                    continue
        
        # 通常のパース処理
        for i, item in enumerate(items):
            try:
                deal_data = NewoldCapitalParser._parse_item(item)
                if deal_data:
                    results.append(deal_data)
            
            except Exception as e:
                logging.error(f"    -> Error parsing item {i+1}: {e}")
                continue
        
        # 売上高フィルタリング
        filtered_results = []
        for deal in results:
            if DataConverter.parse_newold_revenue(deal.revenue_text):
                # 売上高を百万円単位に変換
                deal.revenue_text = DataConverter.convert_newold_revenue_to_million(deal.revenue_text)
                filtered_results.append(deal)
                logging.info(f"    -> Deal {deal.deal_id} meets revenue criteria: {deal.revenue_text}")
            else:
                logging.info(f"    -> Skipping deal {deal.deal_id}: Revenue '{deal.revenue_text}' doesn't meet criteria")
        
        return filtered_results
    
    @staticmethod
    def _parse_item(item: Tag) -> Optional[RawDealData]:
        """個別アイテムのパース"""
        try:
            # 詳細ページリンクの抽出
            link = item.get('href', '')
            if not link:
                return None
            
            # 案件番号をリンクから抽出
            deal_id_match = re.search(r'/anken-list/(\d+)/', link)
            if not deal_id_match:
                return None
            
            deal_id = deal_id_match.group(1)
            
            # タイトルの抽出（修正箇所）
            title_element = item.select_one('h3.p-projects-list__item__title')
            if title_element:
                title = title_element.get_text(strip=True)
            else:
                # フォールバック: リンク先のページタイトルやテキストから抽出を試行
                title = "案件詳細"
            
            # データリストから売上高とエリアを抽出
            revenue_text = ""
            location_text = ""
            
            data_items = item.select('dl.p-projects-list__item__data > div')
            for data_item in data_items:
                dt = data_item.select_one('dt')
                dd = data_item.select_one('dd')
                
                if dt and dd:
                    dt_text = dt.get_text(strip=True)
                    dd_text = dd.get_text(strip=True)
                    
                    if dt_text == "売上高":
                        revenue_text = dd_text
                    elif dt_text == "エリア":
                        location_text = dd_text
            
            # 完全なURLに変換
            if link.startswith('/'):
                link = f"https://newold.co.jp{link}"
            elif not link.startswith('http'):
                link = f"https://newold.co.jp/{link}"
            
            return RawDealData(
                site_name="NEWOLD CAPITAL",
                deal_id=deal_id,
                title=title,
                link=link,
                revenue_text=revenue_text,
                profit_text="",  # 詳細ページで取得
                location_text=location_text,
                price_text="",  # 詳細ページで取得
                features_text=""  # 詳細ページで取得
            )
        
        except Exception as e:
            logging.error(f"    -> Error parsing item: {e}")
            return None
    
    @staticmethod
    def _extract_deal_from_element(element: Tag, link: str) -> Optional[RawDealData]:
        """要素から案件データを抽出（フォールバック用）"""
        try:
            element_text = element.get_text()
            
            # 案件IDをリンクから抽出
            deal_id_match = re.search(r'/anken-list/(\d+)/', link)
            if not deal_id_match:
                return None
            
            deal_id = deal_id_match.group(1)
            
            # タイトル抽出
            title_lines = [line.strip() for line in element_text.split('\n') if line.strip()]
            title = "案件詳細"
            for line in title_lines:
                if (len(line) > 5 and len(line) < 100 and 
                    deal_id not in line and '億円' not in line and '万円' not in line):
                    title = line
                    break
            
            # 売上高抽出
            revenue_patterns = [
                r'(3,000万円～5,000万円|5,000万円～1億円|1億円～2億円|2億円～3億円|3億円～5億円|5億円～10億円)'
            ]
            
            revenue_text = ""
            for pattern in revenue_patterns:
                match = re.search(pattern, element_text)
                if match:
                    revenue_text = match.group(1)
                    break
            
            # エリア抽出
            location_patterns = [
                r'(北海道・東北|関東|北陸・東海|近畿|中国・四国|九州・沖縄)'
            ]
            
            location_text = ""
            for pattern in location_patterns:
                match = re.search(pattern, element_text)
                if match:
                    location_text = match.group(1)
                    break
            
            # 完全なURLに変換
            if link.startswith('/'):
                link = f"https://newold.co.jp{link}"
            elif not link.startswith('http'):
                link = f"https://newold.co.jp/{link}"
            
            return RawDealData(
                site_name="NEWOLD CAPITAL",
                deal_id=deal_id,
                title=title,
                link=link,
                revenue_text=revenue_text,
                profit_text="",
                location_text=location_text,
                price_text="",
                features_text=""
            )
        
        except Exception as e:
            logging.error(f"    -> Error extracting from element: {e}")
            return None

class OnDeckParser:
    """オンデック専用パーサー（修正版）"""
    
    @staticmethod
    def parse_list_page(html_content: str) -> List[RawDealData]:
        """一覧ページのパース（一次フィルタリング込み）"""
        soup = BeautifulSoup(html_content, 'lxml')
        results = []
        
        # デバッグ用: HTMLファイル保存
        if CONFIG.get('debug', {}).get('save_html_files', False):
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            debug_file = f"debug/debug_ondeck_{timestamp}.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logging.info(f"Debug: HTML saved to {debug_file}")
        
        # 一覧ページから案件情報を抽出
        items = OnDeckParser._extract_items_from_list_page(soup)
        
        for i, item_data in enumerate(items):
            try:
                # 売上高による一次フィルタリング
                if item_data.get('revenue_text') and DataConverter.parse_ondeck_revenue(item_data['revenue_text']):
                    
                    raw_deal = RawDealData(
                        site_name="オンデック",
                        deal_id=item_data.get('deal_id', ''),
                        title=item_data.get('title', '案件詳細'),
                        link=item_data.get('link', ''),
                        revenue_text=item_data.get('revenue_text', ''),
                        profit_text="",  # 詳細ページで取得
                        location_text="",  # 詳細ページで取得
                        price_text="",  # 詳細ページで取得
                        features_text=""  # 詳細ページで取得
                    )
                    
                    results.append(raw_deal)
                    logging.info(f"    -> Deal {item_data.get('deal_id')} meets revenue criteria: {item_data.get('revenue_text')}")
                else:
                    logging.info(f"    -> Skipping deal {item_data.get('deal_id')}: Revenue doesn't meet criteria")
                
            except Exception as e:
                logging.error(f"    -> Error parsing item {i+1}: {e}")
                continue
        
        return results
    
    @staticmethod
    def _extract_items_from_list_page(soup: BeautifulSoup) -> List[Dict[str, str]]:
        """一覧ページから案件の基本情報を抽出"""
        items = []
        
        # オンデックの一覧ページ構造に基づいて案件を抽出
        # テーブル行形式の案件リストを想定
        possible_selectors = [
            'a.table_display__row',
            'tr.table_display__row',
            'div.table_display__row', 
            'a[href*="/sell/"]',
            'tr[data-href*="/sell/"]'
        ]
        
        found_items = []
        for selector in possible_selectors:
            found_items = soup.select(selector)
            if found_items:
                logging.info(f"    -> Found {len(found_items)} items using selector: {selector}")
                break
        
        if not found_items:
            # フォールバック: 詳細ページリンクから逆算
            links = soup.find_all('a', href=re.compile(r'/sell/[a-zA-Z]{2}\d+'))
            logging.info(f"    -> Fallback: Found {len(links)} detail links")
            
            for link in links:
                try:
                    href = link.get('href', '')
                    parent_element = link.parent
                    
                    # 親要素を遡って案件情報を含む要素を探す
                    for _ in range(5):
                        if parent_element and parent_element.name != 'body':
                            parent_element = parent_element.parent
                        else:
                            break
                    
                    if parent_element:
                        item_data = OnDeckParser._extract_item_data_from_element(parent_element, href)
                        if item_data:
                            items.append(item_data)
                
                except Exception as e:
                    logging.error(f"    -> Error parsing fallback item: {e}")
                    continue
        else:
            # 通常のパース処理
            for item_element in found_items:
                try:
                    link = item_element.get('href', '') if item_element.name == 'a' else OnDeckParser._extract_link_from_element(item_element)
                    if link:
                        item_data = OnDeckParser._extract_item_data_from_element(item_element, link)
                        if item_data:
                            items.append(item_data)
                
                except Exception as e:
                    logging.error(f"    -> Error parsing item: {e}")
                    continue
        
        return items
    
    @staticmethod
    def _extract_item_data_from_element(element: Tag, link: str) -> Optional[Dict[str, str]]:
        """要素から案件の基本データを抽出"""
        try:
            # 案件IDをリンクから抽出
            deal_id_match = re.search(r'/sell/([a-zA-Z]{2}\d+)', link)
            if not deal_id_match:
                return None
            
            deal_id = deal_id_match.group(1).upper()
            
            # 完全なURLに変換
            if link.startswith('/'):
                link = f"https://www.ondeck.jp{link}"
            elif not link.startswith('http'):
                link = f"https://www.ondeck.jp/{link}"
            
            # 業種（タイトル）を抽出
            title = OnDeckParser._extract_title_from_element(element, deal_id)
            
            # 年商を抽出
            revenue_text = OnDeckParser._extract_revenue_from_element(element)
            
            return {
                'deal_id': deal_id,
                'title': title,
                'link': link,
                'revenue_text': revenue_text
            }
        
        except Exception as e:
            logging.error(f"    -> Error extracting item data: {e}")
            return None
    
    @staticmethod
    def _extract_link_from_element(element: Tag) -> str:
        """要素から詳細ページリンクを抽出"""
        link_element = element.find('a', href=re.compile(r'/sell/[a-zA-Z]{2}\d+'))
        if link_element:
            return link_element.get('href', '')
        return ""
    
    @staticmethod
    def _extract_title_from_element(element: Tag, deal_id: str) -> str:
        """要素からタイトル（業種）を抽出（修正版）"""
        
        # 方法1: ページタイトルからの抽出（最も確実）
        # ページタイトルは通常 "業種名 | Ｍ＆Ａ支援のオンデック" の形式
        page_title = element.find('title')
        if page_title:
            title_text = page_title.get_text(strip=True)
            # " | Ｍ＆Ａ支援のオンデック" の部分を除去
            if ' | ' in title_text:
                business_type = title_text.split(' | ')[0].strip()
                if business_type and len(business_type) <= 20:
                    logging.info(f"    -> Found title from page title: {business_type}")
                    return business_type
        
        # 方法2: h1またはh2タグの最初の文字列（業種名）を取得
        for heading_tag in ['h1', 'h2']:
            heading = element.find(heading_tag)
            if heading:
                heading_text = heading.get_text(strip=True)
                # 改行や余分な情報を除去して最初の行のみ取得
                first_line = heading_text.split('\n')[0].strip()
                # 案件番号を除去
                cleaned_title = re.sub(r'\（案件No\.\s*[A-Z]{2}\d+\）', '', first_line).strip()
                if cleaned_title and len(cleaned_title) <= 20 and '案件' not in cleaned_title:
                    logging.info(f"    -> Found title from {heading_tag}: {cleaned_title}")
                    return cleaned_title
        
        # 方法3: メタデータからの抽出
        # data-business-type や data-industry などの属性があれば取得
        business_type_element = element.find('[data-business-type]')
        if business_type_element:
            business_type = business_type_element.get('data-business-type', '').strip()
            if business_type:
                logging.info(f"    -> Found title from data attribute: {business_type}")
                return business_type
        
        # 方法4: 業種を含む専用要素の検索
        industry_elements = element.select('span.industry, .business-type, .sector')
        for industry_element in industry_elements:
            title = industry_element.get_text(strip=True)
            if title and len(title) <= 20:
                logging.info(f"    -> Found title from industry element: {title}")
                return title
        
        # 方法5: オンデック専用のdt/dd構造での業種検索
        data_list = element.select_one('dl.p-sell-single__data__list')
        if data_list:
            dt_elements = data_list.find_all('dt')
            dd_elements = data_list.find_all('dd')
            
            # dt要素とdd要素をペアで処理
            for dt, dd in zip(dt_elements, dd_elements):
                dt_text = dt.get_text(strip=True)
                dd_text = dd.get_text(strip=True)
                
                # dtに何らかの業種情報、ddにラベルという逆パターンの場合
                if ('業' in dd_text or 'サービス' in dd_text or '事業' in dd_text) and len(dt_text) <= 20:
                    # dtが業種名の場合
                    if dt_text and dt_text not in ['業種', '事業内容', '業務内容']:
                        logging.info(f"    -> Found title from dt/dd structure: {dt_text}")
                        return dt_text
        
        # 方法6: 正規表現パターンマッチング
        element_text = element.get_text()
        
        # 業種名パターンの検索
        industry_patterns = [
            r'(建設業|運送業|製造業|IT業|サービス業|小売業|卸売業|不動産業|金融業|保険業|医療業|介護事業|教育事業|飲食業|美容業|清掃業|警備業|人材派遣業|コンサルティング業)',
            r'([^\s]{2,10}業)(?:\s|$|（)',  # 「○○業」パターン
            r'([^\s]{2,15}サービス)(?:\s|$|（)',  # 「○○サービス」パターン
        ]
        
        for pattern in industry_patterns:
            matches = re.findall(pattern, element_text)
            if matches:
                # 最も短い（具体的な）マッチを選択
                best_match = min(matches, key=len) if isinstance(matches[0], str) else matches[0]
                if best_match and len(best_match) <= 20:
                    logging.info(f"    -> Found title from pattern matching: {best_match}")
                    return best_match
        
        # 最終フォールバック
        logging.warning(f"    -> Could not determine title for deal {deal_id}, using default")
        return "案件詳細"
    
    @staticmethod
    def _extract_revenue_from_element(element: Tag) -> str:
        """要素から年商を抽出"""
        # data-label="年商" の要素を探す
        revenue_cell = element.select_one('div[data-label="年商"]')
        if revenue_cell:
            revenue_text = revenue_cell.get_text(strip=True)
            # <br>タグによる改行を処理
            revenue_text = re.sub(r'\s+', ' ', revenue_text)
            return revenue_text
        
        # フォールバック: テキストから年商パターンを検索
        element_text = element.get_text()
        revenue_patterns = [
            r'約?([\d,]+(?:～[\d,]+)?)百万円[（(][^）)]*[）)]?'
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, element_text)
            if match:
                return f"約{match.group(1)}百万円"
        
        return ""

# --- 詳細ページスクレイパー ---
class DetailPageScraper:
    """詳細ページのスクレイピング"""
    
    @staticmethod
    def fetch_nihon_ma_details(detail_url: str) -> Dict[str, str]:
        """日本M&Aセンターの詳細ページから情報を取得"""
        if not detail_url:
            return {}
        
        try:
            logging.info(f"    -> Fetching detail page: {detail_url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive'
            }
            
            with httpx.Client(timeout=15, follow_redirects=True) as client:
                response = client.get(detail_url, headers=headers)
                response.raise_for_status()
                
                detail_soup = BeautifulSoup(response.text, 'lxml')
                
                # デバッグ用: 詳細ページHTMLファイル保存
                if CONFIG.get('debug', {}).get('save_html_files', False):
                    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                    deal_id = detail_url.split('no=')[-1] if 'no=' in detail_url else 'unknown'
                    debug_file = f"debug/debug_nihon_ma_detail_{deal_id}_{timestamp}.html"
                    with open(debug_file, 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    logging.info(f"Debug: Detail HTML saved to {debug_file}")
                
                return {
                    'profit': DetailPageScraper._extract_nihon_ma_profit(detail_soup),
                    'features': DetailPageScraper._extract_nihon_ma_features(detail_soup),
                    'location': DetailPageScraper._extract_nihon_ma_location(detail_soup),
                    'price': DetailPageScraper._extract_nihon_ma_price(detail_soup)
                }
        
        except Exception as e:
            logging.error(f"    -> Error fetching detail page: {e}")
            return {}
    
    @staticmethod
    def fetch_integroup_details(detail_url: str) -> Dict[str, str]:
        """インテグループの詳細ページから情報を取得"""
        if not detail_url:
            return {}
        
        try:
            logging.info(f"    -> Fetching detail page: {detail_url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive'
            }
            
            with httpx.Client(timeout=15, follow_redirects=True) as client:
                response = client.get(detail_url, headers=headers)
                response.raise_for_status()
                
                detail_soup = BeautifulSoup(response.text, 'lxml')
                
                # デバッグ用: 詳細ページHTMLファイル保存
                if CONFIG.get('debug', {}).get('save_html_files', False):
                    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                    deal_id = detail_url.split('/')[-1].replace('.html', '') if '.html' in detail_url else 'unknown'
                    debug_file = f"debug/debug_integroup_detail_{deal_id}_{timestamp}.html"
                    with open(debug_file, 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    logging.info(f"Debug: Detail HTML saved to {debug_file}")
                
                # 特色を抽出して不要部分を除去
                raw_features = DetailPageScraper._extract_integroup_features(detail_soup)
                cleaned_features = DataConverter.clean_integroup_features(raw_features)
                
                return {
                    'features': cleaned_features
                }
        
        except Exception as e:
            logging.error(f"    -> Error fetching detail page: {e}")
            return {}
    
    @staticmethod
    def fetch_newold_details(detail_url: str) -> Dict[str, str]:
        """NEWOLD CAPITALの詳細ページから情報を取得（タイトル抽出追加版）"""
        if not detail_url:
            return {}
        
        try:
            logging.info(f"    -> Fetching detail page: {detail_url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive'
            }
            
            with httpx.Client(timeout=15, follow_redirects=True) as client:
                response = client.get(detail_url, headers=headers)
                response.raise_for_status()
                
                detail_soup = BeautifulSoup(response.text, 'lxml')
                
                # デバッグ用: 詳細ページHTMLファイル保存
                if CONFIG.get('debug', {}).get('save_html_files', False):
                    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                    deal_id = detail_url.split('/')[-2] if detail_url.endswith('/') else detail_url.split('/')[-1]
                    debug_file = f"debug/debug_newold_detail_{deal_id}_{timestamp}.html"
                    with open(debug_file, 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    logging.info(f"Debug: Detail HTML saved to {debug_file}")
                
                # タイトル抽出を追加
                title = DetailPageScraper._extract_newold_title_from_detail_page(detail_soup)
                
                return {
                    'title': title,  # 新規追加
                    'profit': DetailPageScraper._extract_newold_profit(detail_soup),
                    'features': DetailPageScraper._extract_newold_features(detail_soup),
                    'price': DetailPageScraper._extract_newold_price(detail_soup)
                }
        
        except Exception as e:
            logging.error(f"    -> Error fetching detail page: {e}")
            return {}
    
    @staticmethod
    def fetch_ondeck_details(detail_url: str) -> Dict[str, str]:
        """オンデックの詳細ページから情報を取得（修正版）"""
        if not detail_url:
            return {}
        
        try:
            logging.info(f"    -> Fetching detail page: {detail_url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive'
            }
            
            with httpx.Client(timeout=15, follow_redirects=True) as client:
                response = client.get(detail_url, headers=headers)
                response.raise_for_status()
                
                # 修正: response.textではなくresponse.contentを使用
                # BeautifulSoupが自動的に文字エンコーディングを判定し、
                # 圧縮されたデータも正しく解凍してくれる
                detail_soup = BeautifulSoup(response.content, 'lxml')
                
                # デバッグ用: 詳細ページHTMLファイル保存
                if CONFIG.get('debug', {}).get('save_html_files', False):
                    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                    deal_id = detail_url.split('/')[-1] if detail_url.split('/')[-1] else detail_url.split('/')[-2]
                    debug_file = f"debug/debug_ondeck_detail_{deal_id}_{timestamp}.html"
                    with open(debug_file, 'w', encoding='utf-8') as f:
                        # デバッグファイル保存時も正しくデコードされたHTMLを使用
                        f.write(str(detail_soup))
                    logging.info(f"Debug: Detail HTML saved to {debug_file}")
                
                return {
                    'profit': DetailPageScraper._extract_ondeck_profit(detail_soup),
                    'features': DetailPageScraper._extract_ondeck_features(detail_soup),
                    'location': DetailPageScraper._extract_ondeck_location(detail_soup),
                    'price': DetailPageScraper._extract_ondeck_price(detail_soup)
                }
        
        except Exception as e:
            logging.error(f"    -> Error fetching detail page: {e}")
            return {}
    
    @staticmethod
    def _extract_nihon_ma_profit(detail_soup: BeautifulSoup) -> str:
        """日本M&Aセンターの実態営業利益の抽出"""
        profit_keywords = ['実態営業利益', '営業利益', '利益']
        
        # dt/dd構造での検索
        for keyword in profit_keywords:
            dt_elements = detail_soup.find_all('dt', string=re.compile(keyword))
            for dt in dt_elements:
                dd = dt.find_next_sibling('dd')
                if dd:
                    profit_text = dd.get_text(strip=True)
                    if profit_text and ('万円' in profit_text or '億円' in profit_text):
                        logging.info(f"    -> Found profit via dt/dd: {profit_text}")
                        return profit_text
        
        # テーブル構造での検索
        for keyword in profit_keywords:
            cells = detail_soup.find_all(['td', 'th'], string=re.compile(keyword))
            for cell in cells:
                next_cell = cell.find_next_sibling(['td', 'th'])
                if next_cell:
                    profit_text = next_cell.get_text(strip=True)
                    if profit_text and ('万円' in profit_text or '億円' in profit_text):
                        logging.info(f"    -> Found profit via table: {profit_text}")
                        return profit_text
        
        # 全体テキストからの抽出
        full_text = detail_soup.get_text()
        profit_patterns = [
            r'実態営業利益[：:\s]*([^\n]+)',
            r'営業利益[：:\s]*([^\n]+)',
        ]
        
        for pattern in profit_patterns:
            match = re.search(pattern, full_text)
            if match:
                candidate = match.group(1).strip()
                if ('万円' in candidate or '億円' in candidate) and len(candidate) < 50:
                    logging.info(f"    -> Found profit via text search: {candidate}")
                    return candidate
        
        logging.warning("    -> No profit information found")
        return ""
    
    @staticmethod
    def _extract_nihon_ma_features(detail_soup: BeautifulSoup) -> str:
        """日本M&Aセンターの特色の抽出"""
        
        # 最優先: 日本M&Aセンター専用のセレクター
        anken_description = detail_soup.select_one('p.anken-description-body__text')
        if anken_description:
            # <br>タグを改行文字に置換
            for br in anken_description.find_all('br'):
                br.replace_with('\n')
            
            features_text = anken_description.get_text(strip=True)
            
            if features_text and len(features_text) > 10:
                # 改行で分割して空行を除去し、再結合
                lines = [line.strip() for line in features_text.split('\n') if line.strip()]
                formatted_features = '\n'.join(lines)
                
                logging.info(f"    -> Found features via anken-description-body__text: {len(lines)} items")
                return formatted_features
        
        # フォールバック: 既存の汎用的な抽出ロジック
        features_keywords = ['特色', '特徴', '強み', '事業内容', '概要']
        features_sections = []
        
        # dt/dd構造での検索
        for keyword in features_keywords:
            dt_elements = detail_soup.find_all('dt', string=re.compile(keyword))
            for dt in dt_elements:
                dd = dt.find_next_sibling('dd')
                if dd:
                    content = dd.get_text(strip=True)
                    if len(content) > 10:
                        features_sections.append(f"【{keyword}】\n{content}")
        
        # リスト構造での検索
        ul_elements = detail_soup.find_all('ul')
        for ul in ul_elements:
            # 前の要素に特色関連のキーワードがあるかチェック
            prev_element = ul.find_previous_sibling(['dt', 'h3', 'h4', 'strong'])
            if prev_element:
                prev_text = prev_element.get_text(strip=True)
                if any(keyword in prev_text for keyword in features_keywords):
                    li_items = ul.find_all('li')
                    if li_items:
                        bullet_points = []
                        for li in li_items:
                            li_text = li.get_text(strip=True)
                            if len(li_text) > 5:
                                bullet_points.append(f"・{li_text}")
                        
                        if bullet_points:
                            features_sections.append(f"【{prev_text}】\n" + '\n'.join(bullet_points))
        
        if features_sections:
            logging.info(f"    -> Found features via fallback methods: {len(features_sections)} sections")
            return '\n\n'.join(features_sections)
        
        logging.warning("    -> No features found with any method")
        return ""
    
    @staticmethod
    def _extract_nihon_ma_location(detail_soup: BeautifulSoup) -> str:
        """日本M&Aセンターの所在地の抽出"""
        location_keywords = ['所在地', '地域', 'エリア']
        
        # dt/dd構造での検索
        for keyword in location_keywords:
            dt_elements = detail_soup.find_all('dt', string=re.compile(keyword))
            for dt in dt_elements:
                dd = dt.find_next_sibling('dd')
                if dd:
                    location_text = dd.get_text(strip=True)
                    if len(location_text) > 1:
                        logging.info(f"    -> Found location via dt/dd: {location_text}")
                        return location_text
        
        # テーブル構造での検索
        for keyword in location_keywords:
            cells = detail_soup.find_all(['td', 'th'], string=re.compile(keyword))
            for cell in cells:
                next_cell = cell.find_next_sibling(['td', 'th'])
                if next_cell:
                    location_text = next_cell.get_text(strip=True)
                    if len(location_text) > 1:
                        logging.info(f"    -> Found location via table: {location_text}")
                        return location_text
        
        logging.warning("    -> No location found")
        return ""
    
    @staticmethod
    def _extract_nihon_ma_price(detail_soup: BeautifulSoup) -> str:
        """日本M&Aセンターの価格の抽出"""
        price_keywords = ['価格', '希望価格', '譲渡価格', '希望金額']
        
        # dt/dd構造での検索
        for keyword in price_keywords:
            dt_elements = detail_soup.find_all('dt', string=re.compile(keyword))
            for dt in dt_elements:
                dd = dt.find_next_sibling('dd')
                if dd:
                    price_text = dd.get_text(strip=True)
                    if len(price_text) > 1:
                        logging.info(f"    -> Found price via dt/dd: {price_text}")
                        return price_text
        
        # テーブル構造での検索
        for keyword in price_keywords:
            cells = detail_soup.find_all(['td', 'th'], string=re.compile(keyword))
            for cell in cells:
                next_cell = cell.find_next_sibling(['td', 'th'])
                if next_cell:
                    price_text = next_cell.get_text(strip=True)
                    if len(price_text) > 1:
                        logging.info(f"    -> Found price via table: {price_text}")
                        return price_text
        
        logging.warning("    -> No price found")
        return ""
    
    @staticmethod
    def _extract_integroup_features(detail_soup: BeautifulSoup) -> str:
        """インテグループの特色の抽出"""
        
        # インテグループ専用のセレクターパターンを試行
        possible_selectors = [
            'div.sell-detail-content',
            'div.content-body',
            'div.detail-description',
            'div.sell-description',
            'div[class*="content"]',
            'div[class*="description"]',
            'div.main-content p',
            'article p'
        ]
        
        for selector in possible_selectors:
            elements = detail_soup.select(selector)
            if elements:
                content_parts = []
                for element in elements:
                    text = element.get_text(strip=True)
                    if len(text) > 20:  # 短すぎるテキストは除外
                        content_parts.append(text)
                
                if content_parts:
                    features_text = '\n\n'.join(content_parts)
                    logging.info(f"    -> Found features via {selector}: {len(content_parts)} parts")
                    return features_text
        
        # フォールバック: 汎用的な抽出
        features_keywords = ['事業内容', '特色', '特徴', '強み', '概要', '詳細']
        features_sections = []
        
        # dt/dd構造での検索
        for keyword in features_keywords:
            dt_elements = detail_soup.find_all('dt', string=re.compile(keyword))
            for dt in dt_elements:
                dd = dt.find_next_sibling('dd')
                if dd:
                    content = dd.get_text(strip=True)
                    if len(content) > 10:
                        features_sections.append(content)
        
        # テーブル構造での検索
        for keyword in features_keywords:
            cells = detail_soup.find_all(['td', 'th'], string=re.compile(keyword))
            for cell in cells:
                next_cell = cell.find_next_sibling(['td', 'th'])
                if next_cell:
                    content = next_cell.get_text(strip=True)
                    if len(content) > 10:
                        features_sections.append(content)
        
        if features_sections:
            logging.info(f"    -> Found features via fallback methods: {len(features_sections)} sections")
            return '\n\n'.join(features_sections)
        
        # 最後の手段: ページ全体から長いテキスト部分を抽出
        all_paragraphs = detail_soup.find_all('p')
        long_texts = []
        for p in all_paragraphs:
            text = p.get_text(strip=True)
            if len(text) > 50:  # 50文字以上のテキスト
                long_texts.append(text)
        
        if long_texts:
            logging.info(f"    -> Found features via paragraph extraction: {len(long_texts)} paragraphs")
            return '\n\n'.join(long_texts[:3])  # 最初の3つのパラグラフのみ
        
        logging.warning("    -> No features found with any method")
        return ""

    @staticmethod
    def _extract_newold_title_from_detail_page(detail_soup: BeautifulSoup) -> str:
        """NEWOLD CAPITALの詳細ページからタイトルを抽出"""
        
        # 方法1: ページタイトルから抽出（最も確実）
        page_title = detail_soup.find('title')
        if page_title:
            title_text = page_title.get_text(strip=True)
            # 「| NEWOLD CAPITAL」などのサイト名を除去
            if ' | ' in title_text:
                business_title = title_text.split(' | ')[0].strip()
                if business_title and len(business_title) > 5:
                    logging.info(f"    -> Found title from page title: {business_title}")
                    return business_title
            # サイト名がない場合はそのまま使用
            elif title_text and len(title_text) > 5 and 'NEWOLD' not in title_text:
                logging.info(f"    -> Found title from page title (no separator): {title_text}")
                return title_text
        
        # 方法2: h1タグから抽出
        h1_element = detail_soup.find('h1')
        if h1_element:
            h1_text = h1_element.get_text(strip=True)
            if h1_text and len(h1_text) > 5 and '案件' not in h1_text:
                logging.info(f"    -> Found title from h1: {h1_text}")
                return h1_text
        
        # 方法3: 事業の内容セクションから抽出
        business_content_heading = detail_soup.find('h3', string=re.compile(r'事業の内容'))
        if business_content_heading:
            next_element = business_content_heading.find_next_sibling(['p', 'div'])
            if next_element:
                content_text = next_element.get_text(strip=True)
                # 最初の行または短い説明文を取得
                first_sentence = content_text.split('。')[0].strip()
                if first_sentence and 10 < len(first_sentence) < 100:
                    logging.info(f"    -> Found title from business content: {first_sentence}")
                    return first_sentence
        
        # 方法4: メタタグのdescriptionから抽出
        meta_description = detail_soup.find('meta', attrs={'name': 'description'})
        if meta_description:
            description = meta_description.get('content', '').strip()
            if description:
                # 最初の文を取得
                first_sentence = description.split('。')[0].strip()
                if first_sentence and 10 < len(first_sentence) < 100:
                    logging.info(f"    -> Found title from meta description: {first_sentence}")
                    return first_sentence
        
        logging.warning("    -> Could not extract title from detail page")
        return "案件詳細"

    @staticmethod
    def _extract_newold_profit(detail_soup: BeautifulSoup) -> str:
        """NEWOLD CAPITALの営業利益の抽出（修正版）"""
        # 財務情報セクションの営業利益を探す
        finance_section = detail_soup.find('h3', string=re.compile(r'財務情報'))
        if finance_section:
            # 財務情報セクション後の anken-single-table を探す
            next_element = finance_section.find_next_sibling()
            while next_element:
                if next_element.name == 'div' and 'anken-single-table' in next_element.get('class', []):
                    # dl要素から営業利益を探す
                    dl_elements = next_element.find_all('dl')
                    for dl in dl_elements:
                        dt = dl.find('dt')
                        dd = dl.find('dd')
                        
                        if dt and dd and dt.get_text(strip=True) == '営業利益':
                            profit_text = dd.get_text(strip=True)
                            logging.info(f"    -> Found profit via finance section: {profit_text}")
                            return profit_text
                    break
                next_element = next_element.find_next_sibling()
        
        # フォールバック: 汎用的な検索
        profit_keywords = ['営業利益', '利益', '経常利益']
        
        # dt/dd構造での検索
        for keyword in profit_keywords:
            dt_elements = detail_soup.find_all('dt', string=re.compile(keyword))
            for dt in dt_elements:
                dd = dt.find_next_sibling('dd')
                if dd:
                    profit_text = dd.get_text(strip=True)
                    if profit_text and ('万円' in profit_text or '億円' in profit_text):
                        logging.info(f"    -> Found profit via dt/dd: {profit_text}")
                        return profit_text
        
        # テーブル構造での検索
        for keyword in profit_keywords:
            cells = detail_soup.find_all(['td', 'th'], string=re.compile(keyword))
            for cell in cells:
                next_cell = cell.find_next_sibling(['td', 'th'])
                if next_cell:
                    profit_text = next_cell.get_text(strip=True)
                    if profit_text and ('万円' in profit_text or '億円' in profit_text):
                        logging.info(f"    -> Found profit via table: {profit_text}")
                        return profit_text
        
        logging.warning("    -> No profit information found")
        return ""
    
    @staticmethod
    def _extract_newold_features(detail_soup: BeautifulSoup) -> str:
        """NEWOLD CAPITALの特徴・強みの抽出（見出し統一版）"""
        
        # ページ全体のテキストを取得
        all_text = detail_soup.get_text()
        lines = [line.strip() for line in all_text.split('\n') if line.strip()]
        
        # 「特徴・強み」の見出しを探す
        for i, line in enumerate(lines):
            if line == '特徴・強み' or '特徴・強み' in line:
                features_list = []
                
                # 「特徴・強み」の次の行から特徴を収集
                for j in range(i + 1, len(lines)):
                    next_line = lines[j]
                    
                    # 次のセクションの見出しに到達したら終了
                    if any(keyword in next_line for keyword in [
                        '設立年', '従業員数', '財務情報', '希望条件', 
                        '業種', '地域', '売上高', '営業利益', '純資産',
                        'スキーム', '譲渡希望額', '譲渡理由'
                    ]):
                        break
                    
                    # 空行や短すぎる行はスキップ
                    if len(next_line) < 3:
                        continue
                    
                    # 特徴として収集する条件
                    should_collect = False
                    
                    # 1. 「・」で始まる項目
                    if next_line.startswith('・'):
                        should_collect = True
                    
                    # 2. 「①②③」などの丸数字で始まる項目
                    elif re.match(r'^[①②③④⑤⑥⑦⑧⑨⑩]', next_line):
                        should_collect = True
                        # 丸数字を「・」に変換
                        next_line = re.sub(r'^[①②③④⑤⑥⑦⑧⑨⑩]', '・', next_line)
                    
                    # 3. 「1.」「2.」などの数字で始まる項目
                    elif re.match(r'^[1-9][.):]', next_line):
                        should_collect = True
                        # 数字を「・」に変換
                        next_line = re.sub(r'^[1-9][.):]?\s*', '・', next_line)
                    
                    # 4. 長い文章で、特徴を示すキーワードを含む場合
                    elif len(next_line) > 15 and any(keyword in next_line for keyword in [
                        '事業', '展開', '取引', '対応', '実現', '中心', 
                        'サービス', '提供', '強み', '特徴', '専門', '技術'
                    ]):
                        should_collect = True
                        # 先頭に「・」を追加
                        next_line = f"・{next_line}"
                    
                    if should_collect:
                        features_list.append(next_line)
                        
                        # 連続して10項目以上は不自然なので制限
                        if len(features_list) >= 10:
                            break
                
                if features_list:
                    features_text = '\n'.join(features_list)
                    logging.info(f"    -> Found features: {len(features_list)} items")
                    return features_text
                break
        
        # HTMLの構造も確認（フォールバック）
        if not features_list:
            # h1〜h6で「特徴・強み」を探す
            for heading_tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                features_heading = detail_soup.find(heading_tag, string=re.compile(r'特徴・強み'))
                
                if features_heading:
                    # 見出しの後の要素を確認
                    current_element = features_heading.parent
                    
                    # 親要素とその兄弟要素を確認
                    for sibling in current_element.find_next_siblings():
                        sibling_text = sibling.get_text(strip=True)
                        
                        if sibling_text:
                            # テキストを行ごとに分析
                            sibling_lines = [line.strip() for line in sibling_text.split('\n') if line.strip()]
                            
                            for line in sibling_lines:
                                if len(line) < 3:
                                    continue
                                    
                                # 特徴項目として収集
                                if (line.startswith('・') or 
                                    re.match(r'^[①②③④⑤⑥⑦⑧⑨⑩1-9]', line) or
                                    (len(line) > 15 and any(keyword in line for keyword in [
                                        '事業', '展開', '取引', '対応', '実現', '中心'
                                    ]))):
                                    
                                    # 記号を統一
                                    clean_line = re.sub(r'^[①②③④⑤⑥⑦⑧⑨⑩1-9][.):]?\s*', '・', line)
                                    if not clean_line.startswith('・'):
                                        clean_line = f"・{clean_line}"
                                    
                                    features_list.append(clean_line)
                                
                                # 他のセクションに到達したら終了
                                elif any(keyword in line for keyword in [
                                    '設立年', '従業員数', '財務情報', '希望条件'
                                ]):
                                    break
                        
                        # 他のセクションの見出しに到達したら終了
                        if sibling.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                            break
                    
                    if features_list:
                        features_text = '\n'.join(features_list)
                        logging.info(f"    -> Found features (HTML structure): {len(features_list)} items")
                        return features_text
                    break
        
        logging.warning("    -> No features found for NEWOLD CAPITAL")
        return ""
    
    @staticmethod
    def _extract_newold_price(detail_soup: BeautifulSoup) -> str:
        """NEWOLD CAPITALの譲渡希望額の抽出（修正版）"""
        # 希望条件セクションの譲渡希望額を探す
        conditions_section = detail_soup.find('h3', string=re.compile(r'希望条件'))
        if conditions_section:
            # 希望条件セクション後の anken-single-table を探す
            next_element = conditions_section.find_next_sibling()
            while next_element:
                if next_element.name == 'div' and 'anken-single-table' in next_element.get('class', []):
                    # dl要素から譲渡希望額を探す
                    dl_elements = next_element.find_all('dl')
                    for dl in dl_elements:
                        dt = dl.find('dt')
                        dd = dl.find('dd')
                        
                        if dt and dd and dt.get_text(strip=True) == '譲渡希望額':
                            price_text = dd.get_text(strip=True)
                            logging.info(f"    -> Found price via conditions section: {price_text}")
                            return price_text
                    break
                next_element = next_element.find_next_sibling()
        
        # フォールバック: 汎用的な検索
        price_keywords = ['価格', '希望価格', '譲渡価格', '希望金額', '売却価格', '譲渡希望額']
        
        # dt/dd構造での検索
        for keyword in price_keywords:
            dt_elements = detail_soup.find_all('dt', string=re.compile(keyword))
            for dt in dt_elements:
                dd = dt.find_next_sibling('dd')
                if dd:
                    price_text = dd.get_text(strip=True)
                    if len(price_text) > 1:
                        logging.info(f"    -> Found price via dt/dd: {price_text}")
                        return price_text
        
        # テーブル構造での検索
        for keyword in price_keywords:
            cells = detail_soup.find_all(['td', 'th'], string=re.compile(keyword))
            for cell in cells:
                next_cell = cell.find_next_sibling(['td', 'th'])
                if next_cell:
                    price_text = next_cell.get_text(strip=True)
                    if len(price_text) > 1:
                        logging.info(f"    -> Found price via table: {price_text}")
                        return price_text
        
        logging.warning("    -> No price found")
        return ""
    
    @staticmethod
    def _extract_ondeck_profit(detail_soup: BeautifulSoup) -> str:
        """オンデックの営業利益の抽出（完全修正版）"""
        # オンデック専用のセレクタで詳細データ領域を直接指定
        data_list = detail_soup.select_one('dl.p-sell-single__data__list')
        if data_list:
            dd_elements = data_list.find_all('dd')
            for dd in dd_elements:
                dd_text = dd.get_text(strip=True)
                # 部分一致で営業利益を検索（空白などに対する耐性向上）
                if '営業利益' in dd_text:
                    dt = dd.find_next_sibling('dt')
                    if dt:
                        profit_text = dt.get_text(strip=True)
                        logging.info(f"    -> Found profit via ondeck selector: {profit_text}")
                        return profit_text
        
        # フォールバック1: より広範囲なセレクタ検索
        all_dd_elements = detail_soup.find_all('dd')
        for dd in all_dd_elements:
            dd_text = dd.get_text(strip=True)
            if '営業利益' in dd_text:
                dt = dd.find_next_sibling('dt')
                if dt:
                    profit_text = dt.get_text(strip=True)
                    if profit_text and '百万円' in profit_text:
                        logging.info(f"    -> Found profit via fallback dd search: {profit_text}")
                        return profit_text
        
        # フォールバック2: テキスト全体からの正規表現検索
        full_text = detail_soup.get_text()
        profit_patterns = [
            r'営業利益[：:\s]*([^\n]+百万円[^\n]*)',
            r'営業利益[：:\s]*約?([0-9,]+(?:～[0-9,]+)?百万円)'
        ]
        
        for pattern in profit_patterns:
            match = re.search(pattern, full_text)
            if match:
                profit_text = match.group(1).strip()
                logging.info(f"    -> Found profit via text search: {profit_text}")
                return profit_text
        
        logging.warning("    -> No profit information found")
        return ""

    @staticmethod
    def _extract_ondeck_features(detail_soup: BeautifulSoup) -> str:
        """オンデックのコメント（特色）の抽出（完全修正版）"""
        # オンデック専用のセレクタで詳細データ領域を直接指定
        data_list = detail_soup.select_one('dl.p-sell-single__data__list')
        if data_list:
            dd_elements = data_list.find_all('dd')
            for dd in dd_elements:
                dd_text = dd.get_text(strip=True)
                # 部分一致でコメントを検索
                if 'コメント' in dd_text:
                    dt = dd.find_next_sibling('dt')
                    if dt:
                        features_text = dt.get_text(strip=True)
                        logging.info(f"    -> Found features via ondeck selector: コメント")
                        return features_text
        
        # フォールバック1: より広範囲なセレクタ検索
        all_dd_elements = detail_soup.find_all('dd')
        for dd in all_dd_elements:
            dd_text = dd.get_text(strip=True)
            if 'コメント' in dd_text:
                dt = dd.find_next_sibling('dt')
                if dt:
                    features_text = dt.get_text(strip=True)
                    if len(features_text) > 10:
                        logging.info(f"    -> Found features via fallback dd search: コメント")
                        return features_text
        
        # フォールバック2: テキスト全体からの検索
        full_text = detail_soup.get_text()
        features_patterns = [
            r'コメント[：:\s]*([^\n]+)',
            r'特色[：:\s]*([^\n]+)',
            r'事業内容[：:\s]*([^\n]+)'
        ]
        
        for pattern in features_patterns:
            match = re.search(pattern, full_text)
            if match:
                features_text = match.group(1).strip()
                if len(features_text) > 10:
                    logging.info(f"    -> Found features via text search")
                    return features_text
        
        logging.warning("    -> No features found")
        return ""

    @staticmethod
    def _extract_ondeck_location(detail_soup: BeautifulSoup) -> str:
        """オンデックの所在地の抽出（完全修正版）"""
        # オンデック専用のセレクタで詳細データ領域を直接指定
        data_list = detail_soup.select_one('dl.p-sell-single__data__list')
        if data_list:
            dd_elements = data_list.find_all('dd')
            for dd in dd_elements:
                dd_text = dd.get_text(strip=True)
                # 部分一致で所在地関連のキーワードを検索
                if any(keyword in dd_text for keyword in ['所在地', '地域', 'エリア']):
                    dt = dd.find_next_sibling('dt')
                    if dt:
                        location_text = dt.get_text(strip=True)
                        logging.info(f"    -> Found location via ondeck selector: {location_text}")
                        return location_text
        
        # フォールバック1: より広範囲なセレクタ検索
        all_dd_elements = detail_soup.find_all('dd')
        for dd in all_dd_elements:
            dd_text = dd.get_text(strip=True)
            if any(keyword in dd_text for keyword in ['所在地', '地域', 'エリア']):
                dt = dd.find_next_sibling('dt')
                if dt:
                    location_text = dt.get_text(strip=True)
                    logging.info(f"    -> Found location via fallback dd search: {location_text}")
                    return location_text
        
        # フォールバック2: テキスト全体からの検索
        full_text = detail_soup.get_text()
        location_patterns = [
            r'所在地[：:\s]*([^\n]+)',
            r'地域[：:\s]*([^\n]+)',
            r'エリア[：:\s]*([^\n]+)'
        ]
        
        for pattern in location_patterns:
            match = re.search(pattern, full_text)
            if match:
                location_text = match.group(1).strip()
                if len(location_text) > 1:
                    logging.info(f"    -> Found location via text search: {location_text}")
                    return location_text
        
        logging.warning("    -> No location found")
        return ""

    @staticmethod
    def _extract_ondeck_price(detail_soup: BeautifulSoup) -> str:
        """オンデックの譲渡希望額の抽出（完全修正版）"""
        # オンデック専用のセレクタで詳細データ領域を直接指定
        data_list = detail_soup.select_one('dl.p-sell-single__data__list')
        if data_list:
            dd_elements = data_list.find_all('dd')
            for dd in dd_elements:
                dd_text = dd.get_text(strip=True)
                # 部分一致で譲渡希望額を検索
                if '譲渡希望額' in dd_text:
                    dt = dd.find_next_sibling('dt')
                    if dt:
                        price_text = dt.get_text(strip=True)
                        logging.info(f"    -> Found price via ondeck selector: {price_text}")
                        return price_text
        
        # フォールバック1: より広範囲なセレクタ検索
        all_dd_elements = detail_soup.find_all('dd')
        for dd in all_dd_elements:
            dd_text = dd.get_text(strip=True)
            if '譲渡希望額' in dd_text:
                dt = dd.find_next_sibling('dt')
                if dt:
                    price_text = dt.get_text(strip=True)
                    logging.info(f"    -> Found price via fallback dd search: {price_text}")
                    return price_text
        
        # フォールバック2: テキスト全体からの検索
        full_text = detail_soup.get_text()
        price_patterns = [
            r'譲渡希望額[：:\s]*([^\n]+)',
            r'希望価格[：:\s]*([^\n]+)',
            r'売却価格[：:\s]*([^\n]+)'
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, full_text)
            if match:
                price_text = match.group(1).strip()
                if len(price_text) > 1:
                    logging.info(f"    -> Found price via text search: {price_text}")
                    return price_text
        
        logging.warning("    -> No price found")
        return ""

# --- Google Sheets接続クラス ---
class GSheetConnector:
    """Google Sheets接続管理クラス"""
    def __init__(self, config: Dict):
        # コンフィグから設定を読み込む（認証情報ファイルは除く）
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
            # 書き込み直前に既存データを再取得（他のプロセスによる更新対応）
            current_existing_ids = self.get_existing_ids()
            
            # 最終的な重複チェック
            final_deals = []
            for deal in new_deals:
                if deal.unique_id not in current_existing_ids:
                    final_deals.append(deal)
                    logging.info(f"    -> Adding deal: {deal.deal_id} (unique_id: {deal.unique_id})")
                else:
                    logging.info(f"    -> Final duplicate check: Skipping {deal.deal_id} (unique_id: {deal.unique_id})")
            
            if not final_deals:
                logging.info("No new deals to write after final duplicate check.")
                return
            
            all_values = self.worksheet.get_all_values()
            headers = [f.name for f in fields(FormattedDealData)]
            
            if not all_values:
                self.worksheet.append_row(headers, value_input_option='USER_ENTERED')
            
            existing_headers = self.worksheet.row_values(1) if all_values else headers
            rows_to_append = [[getattr(deal, key, '') for key in existing_headers] for deal in final_deals]
            
            if rows_to_append:
                self.worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
            
            logging.info(f"✅ Successfully appended {len(final_deals)} rows.")
            
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
            logging.FileHandler(log_config.get('file_name', 'scraping.log'), encoding='utf-8'),
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
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }
    
    try:
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            
            # より堅牢なエンコーディング処理
            try:
                # まずはresponse.textを試す
                content = response.text
            except UnicodeDecodeError:
                # 失敗した場合は複数のエンコーディングを試行
                for encoding in ['utf-8', 'shift_jis', 'euc-jp', 'iso-2022-jp']:
                    try:
                        content = response.content.decode(encoding, errors='ignore')
                        logging.info(f"Successfully decoded with {encoding} for {url}")
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    # 全て失敗した場合はUTF-8で強制デコード
                    content = response.content.decode('utf-8', errors='ignore')
                    logging.warning(f"Forced UTF-8 decode for {url}")
            
            # 取得したコンテンツが空でないことを確認
            if not content or len(content) < 100:
                logging.error(f"Retrieved content is too short or empty for {url}")
                return None
            
            return content
            
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
            logging.FileHandler(log_config.get('file_name', 'scraping.log'), encoding='utf-8'),
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

def format_deal_data(raw_deals: List[RawDealData], existing_ids: Set[str]) -> List[FormattedDealData]:
    """生データを整形済みデータに変換"""
    formatted_deals = []
    extraction_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for raw_deal in raw_deals:
        try:
            # より堅牢なユニークID生成（URLベース）
            if raw_deal.link:
                # URLから案件IDを抽出してユニークIDを生成
                url_based_id = f"{raw_deal.site_name}_{raw_deal.link}"
                unique_id = hashlib.md5(url_based_id.encode()).hexdigest()[:12]
            else:
                # フォールバック: 従来の方法
                unique_id = hashlib.md5(f"{raw_deal.site_name}_{raw_deal.deal_id}".encode()).hexdigest()[:12]
            
            # デバッグ情報をログ出力
            logging.info(f"    -> Generating unique_id for {raw_deal.site_name} {raw_deal.deal_id}")
            logging.info(f"       Link: {raw_deal.link}")
            logging.info(f"       Generated unique_id: {unique_id}")
            
            if unique_id in existing_ids:
                logging.info(f"    -> Skipping duplicate deal: {raw_deal.deal_id} (unique_id: {unique_id})")
                continue
            
            # 処理済みIDセットに追加（同一実行内での重複防止）
            existing_ids.add(unique_id)
            
            # サイト別に売上高と営業利益を百万円単位に変換
            if raw_deal.site_name == "日本M&Aセンター":
                revenue = DataConverter.convert_nihon_ma_revenue_to_million(raw_deal.revenue_text)
                profit = DataConverter.convert_nihon_ma_profit_to_million(raw_deal.profit_text)
                price = DataConverter.format_financial_text(raw_deal.price_text)
            elif raw_deal.site_name == "NEWOLD CAPITAL":
                revenue = DataConverter.format_financial_text(raw_deal.revenue_text)  # 既に変換済み
                profit = DataConverter.convert_newold_profit_to_million(raw_deal.profit_text)
                price = DataConverter.convert_newold_price_to_million(raw_deal.price_text)
            elif raw_deal.site_name == "オンデック":
                revenue = DataConverter.clean_ondeck_revenue(raw_deal.revenue_text)
                profit = DataConverter.clean_ondeck_profit(raw_deal.profit_text)
                price = DataConverter.clean_ondeck_price(raw_deal.price_text)
            else:
                # インテグループの場合は既に変換済み
                revenue = DataConverter.format_financial_text(raw_deal.revenue_text)
                profit = DataConverter.format_financial_text(raw_deal.profit_text)
                price = DataConverter.format_financial_text(raw_deal.price_text)
            
            formatted_deal = FormattedDealData(
                extraction_time=extraction_time,
                site_name=raw_deal.site_name,
                deal_id=raw_deal.deal_id,
                title=raw_deal.title,
                location=raw_deal.location_text or "-",
                revenue=revenue,
                profit=profit,
                price=price,
                features=raw_deal.features_text or "-",
                link=raw_deal.link,
                unique_id=unique_id
            )
            
            formatted_deals.append(formatted_deal)
            logging.info(f"    -> Formatted deal: {raw_deal.deal_id} - {raw_deal.title[:50]}... (unique_id: {unique_id})")
            
        except Exception as e:
            logging.error(f"    -> Error formatting deal {raw_deal.deal_id}: {e}")
            continue
    
    return formatted_deals

def scrape_nihon_ma_center() -> List[RawDealData]:
    """日本M&Aセンターのスクレイピング実行"""
    logging.info("🔍 Starting scraping for: 日本M&Aセンター")
    all_deals = []
    
    try:
        base_url = "https://www.nihon-ma.co.jp/anken/needs_convey.php"
        max_pages = 3
        
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
            
            # 一覧ページのパース（売上高フィルタリング込み）
            deals = NihonMACenterParser.parse_list_page(html_content)
            
            logging.info(f"  ✅ Found {len(deals)} deals meeting revenue criteria on page {page_num}")
            all_deals.extend(deals)
            
            time.sleep(2)  # ページ間の待機時間
    
    except Exception as e:
        logging.error(f"❌ Error scraping 日本M&Aセンター: {e}")
        logging.debug(traceback.format_exc())
    
    logging.info(f"🎯 Total deals found from 日本M&Aセンター: {len(all_deals)}")
    return all_deals

def scrape_integroup() -> List[RawDealData]:
    """インテグループのスクレイピング実行"""
    logging.info("🔍 Starting scraping for: インテグループ")
    all_deals = []
    
    try:
        base_url = "https://www.integroup.jp/sell/"
        max_pages = 3
        
        for page_num in range(1, max_pages + 1):
            if page_num == 1:
                url = base_url
            else:
                url = f"{base_url}page/{page_num}/"
            
            logging.info(f"  📄 Scraping page {page_num}: {url}")
            
            html_content = fetch_html(url)
            if not html_content:
                logging.error(f"  ❌ Failed to fetch page {page_num}")
                continue
            
            # 一覧ページのパース（売上高フィルタリング込み）
            deals = IntegroupParser.parse_list_page(html_content)
            
            logging.info(f"  ✅ Found {len(deals)} deals meeting revenue criteria on page {page_num}")
            all_deals.extend(deals)
            
            time.sleep(2)  # ページ間の待機時間
    
    except Exception as e:
        logging.error(f"❌ Error scraping インテグループ: {e}")
        logging.debug(traceback.format_exc())
    
    logging.info(f"🎯 Total deals found from インテグループ: {len(all_deals)}")
    return all_deals

def scrape_newold_capital() -> List[RawDealData]:
    """NEWOLD CAPITALのスクレイピング実行"""
    logging.info("🔍 Starting scraping for: NEWOLD CAPITAL")
    all_deals = []
    
    try:
        url = "https://newold.co.jp/anken/"
        
        logging.info(f"  📄 Scraping page: {url}")
        
        html_content = fetch_html(url)
        if not html_content:
            logging.error(f"  ❌ Failed to fetch page")
            return all_deals
        
        # 一覧ページのパース（売上高フィルタリング込み）
        deals = NewoldCapitalParser.parse_list_page(html_content)
        
        logging.info(f"  ✅ Found {len(deals)} deals meeting revenue criteria")
        all_deals.extend(deals)
    
    except Exception as e:
        logging.error(f"❌ Error scraping NEWOLD CAPITAL: {e}")
        logging.debug(traceback.format_exc())
    
    logging.info(f"🎯 Total deals found from NEWOLD CAPITAL: {len(all_deals)}")
    return all_deals

def scrape_ondeck() -> List[RawDealData]:
    """オンデックのスクレイピング実行（Selenium統一版）"""
    logging.info("🔍 Starting scraping for: オンデック")
    all_deals = []
    
    try:
        # config.yamlから設定を読み込み
        ondeck_config = None
        for site_config in CONFIG.get('sites', []):
            if site_config.get('name') == 'オンデック':
                ondeck_config = site_config
                break
        
        if not ondeck_config:
            logging.error("オンデックの設定がconfig.yamlに見つかりません")
            return all_deals
        
        base_url = ondeck_config['base_url']
        max_pages = ondeck_config.get('max_pages', 3)
        
        # Seleniumの初期化
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        driver = webdriver.Chrome(options=options)
        
        try:
            # 一覧ページのスクレイピング
            for page_num in range(1, max_pages + 1):
                if page_num == 1:
                    url = base_url.rstrip('/')
                else:
                    pagination_path = ondeck_config.get('pagination', {}).get('path', 'page/{page_num}/')
                    url = f"{base_url.rstrip('/')}/{pagination_path.format(page_num=page_num)}"
                
                logging.info(f"  📄 Scraping page {page_num}: {url}")
                
                try:
                    driver.get(url)
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    time.sleep(3)  # ページ読み込み待機
                    
                    html_content = driver.page_source
                    
                    if not html_content or len(html_content) < 100:
                        logging.error(f"  ❌ Retrieved content is too short for page {page_num}")
                        continue
                    
                    # 一覧ページのパース（売上高フィルタリング込み）
                    deals = OnDeckParser.parse_list_page(html_content)
                    
                    logging.info(f"  ✅ Found {len(deals)} deals meeting revenue criteria on page {page_num}")
                    all_deals.extend(deals)
                    
                    time.sleep(2)  # ページ間の待機時間
                    
                except Exception as e:
                    logging.error(f"  ❌ Failed to fetch page {page_num}: {e}")
                    continue
            
            # 詳細ページの情報取得と二次フィルタリング（Seleniumで統一）
            if all_deals:
                logging.info(f"🔗 Fetching details for {len(all_deals)} deals from オンデック using Selenium")
                enhanced_deals = []
                
                for i, deal in enumerate(all_deals, 1):
                    try:
                        logging.info(f"  📖 Processing deal {i}/{len(all_deals)}: {deal.deal_id}")
                        
                        # Seleniumで詳細ページにアクセス
                        driver.get(deal.link)
                        WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.TAG_NAME, "body"))
                        )
                        time.sleep(2)  # ページ読み込み待機
                        
                        # 完全なHTMLを取得
                        detail_html = driver.page_source
                        detail_soup = BeautifulSoup(detail_html, 'lxml')
                        
                        # デバッグ用: 詳細ページHTMLファイル保存
                        if CONFIG.get('debug', {}).get('save_html_files', False):
                            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                            debug_file = f"debug/debug_ondeck_detail_{deal.deal_id}_{timestamp}.html"
                            with open(debug_file, 'w', encoding='utf-8') as f:
                                f.write(detail_html)
                            logging.info(f"Debug: Detail HTML saved to {debug_file}")
                        
                        # 既存のパーサーメソッドを使用して情報抽出
                        detail_info = {
                            'profit': DetailPageScraper._extract_ondeck_profit(detail_soup),
                            'features': DetailPageScraper._extract_ondeck_features(detail_soup),
                            'location': DetailPageScraper._extract_ondeck_location(detail_soup),
                            'price': DetailPageScraper._extract_ondeck_price(detail_soup)
                        }
                        
                        if detail_info.get('profit'):
                            # 営業利益による二次フィルタリング
                            if DataConverter.parse_ondeck_profit(detail_info['profit']):
                                logging.info(f"    -> Deal {deal.deal_id} meets profit criteria: {detail_info['profit']}")
                                
                                # 詳細情報を設定
                                deal.profit_text = detail_info.get('profit', '')
                                deal.features_text = detail_info.get('features', '')
                                deal.location_text = detail_info.get('location', '')
                                deal.price_text = detail_info.get('price', '')
                                
                                enhanced_deals.append(deal)
                            else:
                                logging.info(f"    -> Skipping deal {deal.deal_id}: Profit '{detail_info['profit']}' doesn't meet criteria")
                        else:
                            logging.warning(f"    -> No profit info found for deal {deal.deal_id}")
                        
                        time.sleep(1)  # リクエスト間の待機時間
                        
                    except Exception as e:
                        logging.error(f"  ❌ Error processing deal {deal.deal_id}: {e}")
                        continue
                
                logging.info(f"✅ Enhanced {len(enhanced_deals)} deals meeting all criteria")
                all_deals = enhanced_deals
        
        finally:
            driver.quit()
    
    except Exception as e:
        logging.error(f"❌ Error scraping オンデック: {e}")
        logging.debug(traceback.format_exc())
    
    logging.info(f"🎯 Total deals found from オンデック: {len(all_deals)}")
    return all_deals

# enhance_ondeck_deals_with_details関数を無効化（不要になったため）
def enhance_ondeck_deals_with_details(raw_deals: List[RawDealData]) -> List[RawDealData]:
    """オンデックは既に詳細情報を含んでいるのでそのまま返す"""
    logging.info(f"✅ オンデック deals already enhanced: {len(raw_deals)} deals")
    return raw_deals

def enhance_nihon_ma_deals_with_details(raw_deals: List[RawDealData]) -> List[RawDealData]:
    """日本M&Aセンターの詳細ページから情報を取得して既存データを拡張"""
    logging.info(f"🔗 Fetching details for {len(raw_deals)} deals from 日本M&Aセンター")
    enhanced_deals = []
    
    for i, deal in enumerate(raw_deals, 1):
        try:
            logging.info(f"  📖 Processing deal {i}/{len(raw_deals)}: {deal.deal_id}")
            
            # 詳細ページから情報取得
            detail_info = DetailPageScraper.fetch_nihon_ma_details(deal.link)
            
            if detail_info.get('profit'):
                # 実態営業利益のフィルタリング
                if DataConverter.parse_nihon_ma_profit(detail_info['profit']):
                    logging.info(f"    -> Deal {deal.deal_id} meets profit criteria: {detail_info['profit']}")
                    
                    # 詳細情報を設定
                    deal.profit_text = detail_info.get('profit', '')
                    deal.features_text = detail_info.get('features', '')
                    deal.location_text = detail_info.get('location', '')
                    deal.price_text = detail_info.get('price', '')
                    
                    enhanced_deals.append(deal)
                else:
                    logging.info(f"    -> Skipping deal {deal.deal_id}: Profit '{detail_info['profit']}' doesn't meet criteria")
            else:
                logging.warning(f"    -> No profit info found for deal {deal.deal_id}")
            
            time.sleep(1)  # リクエスト間の待機時間
            
        except Exception as e:
            logging.error(f"  ❌ Error processing deal {deal.deal_id}: {e}")
            continue
    
    logging.info(f"✅ Enhanced {len(enhanced_deals)} deals meeting all criteria")
    return enhanced_deals

def enhance_integroup_deals_with_details(raw_deals: List[RawDealData]) -> List[RawDealData]:
    """インテグループの詳細ページから情報を取得して既存データを拡張"""
    logging.info(f"🔗 Fetching details for {len(raw_deals)} deals from インテグループ")
    enhanced_deals = []
    
    for i, deal in enumerate(raw_deals, 1):
        try:
            logging.info(f"  📖 Processing deal {i}/{len(raw_deals)}: {deal.deal_id}")
            
            # 詳細ページから情報取得
            detail_info = DetailPageScraper.fetch_integroup_details(deal.link)
            
            # 特色情報を設定（クリーニング済み）
            deal.features_text = detail_info.get('features', '')
            
            enhanced_deals.append(deal)
            logging.info(f"    -> Enhanced deal {deal.deal_id}")
            
            time.sleep(1)  # リクエスト間の待機時間
            
        except Exception as e:
            logging.error(f"  ❌ Error processing deal {deal.deal_id}: {e}")
            continue
    
    logging.info(f"✅ Enhanced {len(enhanced_deals)} deals from インテグループ")
    return enhanced_deals

def enhance_newold_deals_with_details(raw_deals: List[RawDealData]) -> List[RawDealData]:
    """NEWOLD CAPITALの詳細ページから情報を取得して既存データを拡張（タイトル更新追加版）"""
    logging.info(f"🔗 Fetching details for {len(raw_deals)} deals from NEWOLD CAPITAL")
    enhanced_deals = []
    
    for i, deal in enumerate(raw_deals, 1):
        try:
            logging.info(f"  📖 Processing deal {i}/{len(raw_deals)}: {deal.deal_id}")
            
            # 詳細ページから情報取得
            detail_info = DetailPageScraper.fetch_newold_details(deal.link)
            
            # タイトルを更新（重要な修正点）
            if detail_info.get('title'):
                deal.title = detail_info['title']
                logging.info(f"    -> Updated title to: {deal.title}")
            
            if detail_info.get('profit'):
                # 営業利益のフィルタリング
                if DataConverter.parse_newold_profit(detail_info['profit']):
                    logging.info(f"    -> Deal {deal.deal_id} meets profit criteria: {detail_info['profit']}")
                    
                    # 詳細情報を設定
                    deal.profit_text = detail_info.get('profit', '')
                    deal.features_text = detail_info.get('features', '')
                    deal.price_text = detail_info.get('price', '')
                    
                    enhanced_deals.append(deal)
                else:
                    logging.info(f"    -> Skipping deal {deal.deal_id}: Profit '{detail_info['profit']}' doesn't meet criteria")
            else:
                logging.warning(f"    -> No profit info found for deal {deal.deal_id}")
            
            time.sleep(1)  # リクエスト間の待機時間
            
        except Exception as e:
            logging.error(f"  ❌ Error processing deal {deal.deal_id}: {e}")
            continue
    
    logging.info(f"✅ Enhanced {len(enhanced_deals)} deals meeting all criteria")
    return enhanced_deals

def enhance_ondeck_deals_with_details(raw_deals: List[RawDealData]) -> List[RawDealData]:
    """オンデックの詳細ページから情報を取得して二次フィルタリング"""
    logging.info(f"🔗 Fetching details for {len(raw_deals)} deals from オンデック")
    enhanced_deals = []
    
    for i, deal in enumerate(raw_deals, 1):
        try:
            logging.info(f"  📖 Processing deal {i}/{len(raw_deals)}: {deal.deal_id}")
            
            # 詳細ページから情報取得
            detail_info = DetailPageScraper.fetch_ondeck_details(deal.link)
            
            if detail_info.get('profit'):
                # 営業利益による二次フィルタリング
                if DataConverter.parse_ondeck_profit(detail_info['profit']):
                    logging.info(f"    -> Deal {deal.deal_id} meets profit criteria: {detail_info['profit']}")
                    
                    # 詳細情報を設定
                    deal.profit_text = detail_info.get('profit', '')
                    deal.features_text = detail_info.get('features', '')
                    deal.location_text = detail_info.get('location', '')
                    deal.price_text = detail_info.get('price', '')
                    
                    enhanced_deals.append(deal)
                else:
                    logging.info(f"    -> Skipping deal {deal.deal_id}: Profit '{detail_info['profit']}' doesn't meet criteria")
            else:
                logging.warning(f"    -> No profit info found for deal {deal.deal_id}")
            
            time.sleep(1)  # リクエスト間の待機時間
            
        except Exception as e:
            logging.error(f"  ❌ Error processing deal {deal.deal_id}: {e}")
            continue
    
    logging.info(f"✅ Enhanced {len(enhanced_deals)} deals meeting all criteria")
    return enhanced_deals

def main():
    """メイン実行関数"""
    try:
        load_config()
        setup_logging(CONFIG)
        
        logging.info("🚀 Starting M&A deal scraping process")
        logging.info("📊 Target sites: 日本M&Aセンター, インテグループ, NEWOLD CAPITAL, オンデック")
        
        sheet_connector = GSheetConnector(CONFIG)
        if not sheet_connector.worksheet:
            logging.critical("❌ Cannot proceed without Google Sheets connection")
            return
        
        existing_ids = sheet_connector.get_existing_ids()
        logging.info(f"📋 Found {len(existing_ids)} existing deals in spreadsheet")
        
        all_formatted_deals = []
        
        # 日本M&Aセンターのスクレイピング実行
        logging.info("=" * 60)
        logging.info("日本M&Aセンター processing started")
        nihon_ma_raw_deals = scrape_nihon_ma_center()
        
        if nihon_ma_raw_deals:
            # 詳細ページから情報取得＆実態営業利益フィルタリング
            nihon_ma_enhanced_deals = enhance_nihon_ma_deals_with_details(nihon_ma_raw_deals)
            
            # データ整形
            nihon_ma_formatted_deals = format_deal_data(nihon_ma_enhanced_deals, existing_ids)
            all_formatted_deals.extend(nihon_ma_formatted_deals)
            
            logging.info(f"✅ 日本M&Aセンター: {len(nihon_ma_formatted_deals)} new deals after all filtering")
        else:
            logging.info("No deals found from 日本M&Aセンター")
        
        # インテグループのスクレイピング実行
        logging.info("=" * 60)
        logging.info("インテグループ processing started")
        integroup_raw_deals = scrape_integroup()
        
        if integroup_raw_deals:
            # 詳細ページから情報取得
            integroup_enhanced_deals = enhance_integroup_deals_with_details(integroup_raw_deals)
            
            # データ整形
            integroup_formatted_deals = format_deal_data(integroup_enhanced_deals, existing_ids)
            all_formatted_deals.extend(integroup_formatted_deals)
            
            logging.info(f"✅ インテグループ: {len(integroup_formatted_deals)} new deals after all filtering")
        else:
            logging.info("No deals found from インテグループ")
        
        # NEWOLD CAPITALのスクレイピング実行
        logging.info("=" * 60)
        logging.info("NEWOLD CAPITAL processing started")
        newold_raw_deals = scrape_newold_capital()
        
        if newold_raw_deals:
            # 詳細ページから情報取得＆営業利益フィルタリング
            newold_enhanced_deals = enhance_newold_deals_with_details(newold_raw_deals)
            
            # データ整形
            newold_formatted_deals = format_deal_data(newold_enhanced_deals, existing_ids)
            all_formatted_deals.extend(newold_formatted_deals)
            
            logging.info(f"✅ NEWOLD CAPITAL: {len(newold_formatted_deals)} new deals after all filtering")
        else:
            logging.info("No deals found from NEWOLD CAPITAL")
        
        # オンデックのスクレイピング実行（Selenium統一版 - 詳細取得も含む）
        logging.info("=" * 60)
        logging.info("オンデック processing started")
        ondeck_enhanced_deals = scrape_ondeck()  # 既に詳細情報取得とフィルタリング済み
        
        if ondeck_enhanced_deals:
            # データ整形のみ
            ondeck_formatted_deals = format_deal_data(ondeck_enhanced_deals, existing_ids)
            all_formatted_deals.extend(ondeck_formatted_deals)
            
            logging.info(f"✅ オンデック: {len(ondeck_formatted_deals)} new deals after all filtering")
        else:
            logging.info("No deals found from オンデック")
        
        # 結果をスプレッドシートに書き込み
        logging.info("=" * 60)
        logging.info(f"📝 Total new deals to add: {len(all_formatted_deals)}")
        
        if all_formatted_deals:
            sheet_connector.write_deals(all_formatted_deals)
            logging.info(f"🎉 Successfully added {len(all_formatted_deals)} new deals to spreadsheet")
            
            # サイト別の集計情報をログ出力
            site_counts = {}
            for deal in all_formatted_deals:
                site_counts[deal.site_name] = site_counts.get(deal.site_name, 0) + 1
            
            for site_name, count in site_counts.items():
                logging.info(f"  - {site_name}: {count} deals")
        else:
            logging.info("📝 No new deals to add")
        
        logging.info("✨ M&A scraping process completed successfully")
        
    except Exception as e:
        logging.critical(f"💥 Critical error in main process: {e}")
        logging.debug(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()