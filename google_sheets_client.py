import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread
from datetime import datetime

class GoogleSheetsClient:
    def __init__(self, credentials_file='credentials.json'):
        """
        Google Sheets クライアントを初期化
        
        Args:
            credentials_file (str): サービスアカウントの認証情報ファイルパス
        """
        self.credentials_file = credentials_file
        self.service = None
        self.gc = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Google Sheets APIクライアントを初期化"""
        try:
            # 認証情報ファイルの存在確認
            if not os.path.exists(self.credentials_file):
                print(f"エラー: 認証情報ファイル '{self.credentials_file}' が見つかりません。")
                print("Google Cloud Consoleからサービスアカウントキーをダウンロードし、credentials.jsonとして保存してください。")
                return False
            
            # スコープを定義
            SCOPES = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            # サービスアカウント認証
            credentials = Credentials.from_service_account_file(
                self.credentials_file, 
                scopes=SCOPES
            )
            
            # Google Sheets API サービスを構築
            self.service = build('sheets', 'v4', credentials=credentials)
            
            # gspreadクライアントも初期化（より簡単な操作用）
            self.gc = gspread.authorize(credentials)
            
            print("Google Sheets APIの認証が完了しました。")
            return True
            
        except FileNotFoundError:
            print(f"認証情報ファイル '{self.credentials_file}' が見つかりません。")
            return False
        except json.JSONDecodeError:
            print("認証情報ファイルの形式が正しくありません。")
            return False
        except Exception as e:
            print(f"Google Sheets APIの初期化に失敗しました: {e}")
            return False
    
    def create_spreadsheet(self, title):
        """
        新しいスプレッドシートを作成
        
        Args:
            title (str): スプレッドシートのタイトル
            
        Returns:
            str: スプレッドシートのID、失敗時はNone
        """
        try:
            spreadsheet = {
                'properties': {
                    'title': title
                }
            }
            
            result = self.service.spreadsheets().create(
                body=spreadsheet,
                fields='spreadsheetId'
            ).execute()
            
            spreadsheet_id = result.get('spreadsheetId')
            print(f"新しいスプレッドシート '{title}' を作成しました。")
            print(f"URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
            
            return spreadsheet_id
            
        except HttpError as error:
            print(f"スプレッドシートの作成に失敗しました: {error}")
            return None
    
    def get_or_create_spreadsheet(self, sheet_name):
        """
        スプレッドシートを取得、存在しなければ作成
        
        Args:
            sheet_name (str): スプレッドシート名
            
        Returns:
            gspread.Spreadsheet: スプレッドシートオブジェクト
        """
        try:
            # 既存のスプレッドシートを検索
            try:
                spreadsheet = self.gc.open(sheet_name)
                print(f"既存のスプレッドシート '{sheet_name}' を開きました。")
                return spreadsheet
            except gspread.SpreadsheetNotFound:
                # 存在しない場合は新規作成
                print(f"スプレッドシート '{sheet_name}' が見つかりません。新規作成します。")
                spreadsheet = self.gc.create(sheet_name)
                
                # 作成したスプレッドシートを誰でも編集可能に設定（オプション）
                # spreadsheet.share('', perm_type='anyone', role='writer')
                
                print(f"新しいスプレッドシート '{sheet_name}' を作成しました。")
                print(f"URL: https://docs.google.com/spreadsheets/d/{spreadsheet.id}")
                
                return spreadsheet
                
        except Exception as e:
            print(f"スプレッドシートの取得/作成に失敗しました: {e}")
            return None
    
    def write_data(self, sheet_name, worksheet_name, headers, data):
        """
        スプレッドシートにデータを書き込み
        
        Args:
            sheet_name (str): スプレッドシート名
            worksheet_name (str): ワークシート名
            headers (list): ヘッダー行のリスト
            data (list): 書き込むデータの2次元リスト
        """
        try:
            if not self.gc:
                print("Google Sheets APIが初期化されていません。")
                return False
            
            # スプレッドシートを取得または作成
            spreadsheet = self.get_or_create_spreadsheet(sheet_name)
            if not spreadsheet:
                return False
            
            # ワークシートを取得または作成
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                print(f"既存のワークシート '{worksheet_name}' を使用します。")
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(
                    title=worksheet_name, 
                    rows=1000, 
                    cols=len(headers)
                )
                print(f"新しいワークシート '{worksheet_name}' を作成しました。")
            
            # 既存のデータをクリア（オプション）
            # worksheet.clear()
            
            # タイムスタンプを追加
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # ヘッダーが存在しない場合は追加
            if not worksheet.get_all_values() or worksheet.row_values(1) != headers:
                worksheet.insert_row(headers, 1)
                print("ヘッダー行を追加しました。")
            
            # データを追加（既存データの下に追加）
            if data:
                # 各行にタイムスタンプを追加（オプション）
                data_with_timestamp = []
                for row in data:
                    new_row = row + [timestamp]  # タイムスタンプを最後の列に追加
                    data_with_timestamp.append(new_row)
                
                # データを一括で追加
                worksheet.append_rows(data_with_timestamp)
                print(f"{len(data)}行のデータを追加しました。")
                
                # スプレッドシートのURLを表示
                print(f"データを確認: https://docs.google.com/spreadsheets/d/{spreadsheet.id}")
                
            return True
            
        except HttpError as error:
            print(f"データの書き込み中にHTTPエラーが発生しました: {error}")
            return False
        except Exception as e:
            print(f"データの書き込み中にエラーが発生しました: {e}")
            return False
    
    def read_data(self, sheet_name, worksheet_name, range_name="A:Z"):
        """
        スプレッドシートからデータを読み取り
        
        Args:
            sheet_name (str): スプレッドシート名
            worksheet_name (str): ワークシート名
            range_name (str): 読み取り範囲
            
        Returns:
            list: データの2次元リスト
        """
        try:
            if not self.gc:
                print("Google Sheets APIが初期化されていません。")
                return None
            
            spreadsheet = self.gc.open(sheet_name)
            worksheet = spreadsheet.worksheet(worksheet_name)
            
            # データを取得
            values = worksheet.get_all_values()
            
            print(f"{len(values)}行のデータを読み取りました。")
            return values
            
        except gspread.SpreadsheetNotFound:
            print(f"スプレッドシート '{sheet_name}' が見つかりません。")
            return None
        except gspread.WorksheetNotFound:
            print(f"ワークシート '{worksheet_name}' が見つかりません。")
            return None
        except Exception as e:
            print(f"データの読み取り中にエラーが発生しました: {e}")
            return None

# テスト用の関数
def test_google_sheets_client():
    """Google Sheets クライアントのテスト"""
    client = GoogleSheetsClient()
    
    if not client.gc:
        print("Google Sheets クライアントの初期化に失敗しました。")
        return
    
    # テストデータ
    test_sheet_name = "テストシート_案件管理"
    test_worksheet_name = "案件リスト"
    test_headers = ['案件ID', 'タイトル', '地域', '売上規模', '営業利益', '価格目線', 'リンク', '更新日時']
    test_data = [
        ['S1001', 'テスト案件1', '東京都', '5億円', '1億円', '応相談', 'https://example.com', ''],
        ['S1002', 'テスト案件2', '大阪府', '3億円', '5000万円', '2億円', 'https://example.com', '']
    ]
    
    # データを書き込み
    success = client.write_data(test_sheet_name, test_worksheet_name, test_headers, test_data)
    
    if success:
        print("テストデータの書き込みが完了しました。")
    else:
        print("テストデータの書き込みに失敗しました。")

if __name__ == "__main__":
    test_google_sheets_client()