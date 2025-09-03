import time
import re
import configparser
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from google_sheets_client import GoogleSheetsClient

def load_config():
    """設定ファイル(config.ini)を読み込む"""
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8-sig')
    return config

# --- 設定項目 ---
MIN_REVENUE_THRESHOLD = 3  # 売上高の最低ライン（単位：億円）
MIN_PROFIT_THRESHOLD = 0.3  # 営業利益の最低ライン（単位：億円 = 3千万円）
MAX_PAGES_TO_SCRAPE = 5    # 取得する最大ページ数
# ----------------

def parse_financial_value(text, unit_type="revenue"):
    """
    財務数値を解析して億円単位で返す
    unit_type: "revenue"（億円単位）または "profit