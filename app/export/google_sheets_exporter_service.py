"""
Googleスプレッドシートエクスポート機能
検査員割振り結果をGoogleスプレッドシートに出力する
"""

import pandas as pd
from datetime import datetime
from loguru import logger
import re


class GoogleSheetsExporter:
    """Googleスプレッドシートエクスポート機能を提供するクラス"""
    
    def __init__(self, sheets_url=None, credentials_path=None):
        """
        初期化
        
        Args:
            sheets_url: GoogleスプレッドシートのURL
            credentials_path: Google認証情報JSONファイルのパス
        """
        self.sheets_url = sheets_url
        self.credentials_path = credentials_path
        self.client = None
        self.spreadsheet = None
        
    def _get_client(self):
        """gspreadクライアントを取得"""
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            
            if self.client is None:
                if not self.credentials_path:
                    logger.error("Google認証情報のパスが設定されていません")
                    return None
                
                # 認証情報を読み込み
                scope = [
                    'https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive'
                ]
                creds = Credentials.from_service_account_file(
                    self.credentials_path,
                    scopes=scope
                )
                self.client = gspread.authorize(creds)
                
            return self.client
        except ImportError:
            logger.error("gspreadライブラリがインストールされていません。pip install gspread google-auth でインストールしてください")
            return None
        except Exception as e:
            logger.error(f"Google認証エラー: {str(e)}")
            return None
    
    def _get_spreadsheet_id(self, url):
        """スプレッドシートURLからIDを抽出"""
        try:
            # URLからIDを抽出
            # 例: https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit
            match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url)
            if match:
                return match.group(1)
            else:
                logger.error(f"無効なGoogleスプレッドシートURL: {url}")
                return None
        except Exception as e:
            logger.error(f"スプレッドシートID抽出エラー: {str(e)}")
            return None
    
    def _get_spreadsheet(self):
        """スプレッドシートを取得"""
        try:
            if self.spreadsheet is None:
                client = self._get_client()
                if not client:
                    return None
                
                spreadsheet_id = self._get_spreadsheet_id(self.sheets_url)
                if not spreadsheet_id:
                    return None
                
                self.spreadsheet = client.open_by_key(spreadsheet_id)
            
            return self.spreadsheet
        except Exception as e:
            logger.error(f"スプレッドシート取得エラー: {str(e)}")
            return None
    
    def _cell_to_coord(self, cell_ref):
        """
        セル参照（例: 'A4'）を座標（行、列）に変換
        
        Args:
            cell_ref: セル参照文字列（例: 'A4', 'AC4'）
        
        Returns:
            (row, col): 行番号（1始まり）、列番号（1始まり）のタプル
        """
        match = re.match(r'([A-Z]+)(\d+)', cell_ref)
        if not match:
            return None
        
        col_str = match.group(1)
        row = int(match.group(2))
        
        # 列文字列を数値に変換（例: A=1, Z=26, AA=27）
        col = 0
        for char in col_str:
            col = col * 26 + (ord(char) - ord('A') + 1)
        
        return (row, col)
    
    def _coord_to_cell(self, row, col):
        """
        座標（行、列）をセル参照に変換
        
        Args:
            row: 行番号（1始まり）
            col: 列番号（1始まり）
        
        Returns:
            セル参照文字列（例: 'A4'）
        """
        col_str = ""
        col_num = col
        while col_num > 0:
            col_num -= 1
            col_str = chr(ord('A') + (col_num % 26)) + col_str
            col_num //= 26
        return f"{col_str}{row}"
    
    def _parse_range(self, range_str):
        """
        範囲文字列（例: 'A4:J200'）を開始と終了の座標に変換
        
        Args:
            range_str: 範囲文字列
        
        Returns:
            ((start_row, start_col), (end_row, end_col))
        """
        parts = range_str.split(':')
        if len(parts) != 2:
            return None
        
        start = self._cell_to_coord(parts[0])
        end = self._cell_to_coord(parts[1])
        
        if start and end:
            return (start, end)
        return None
    
    def export_inspector_assignment_to_sheets(self, inspector_df, log_callback=None):
        """
        検査員割振り結果をGoogleスプレッドシートにエクスポート
        
        Args:
            inspector_df: 検査員割振り結果のDataFrame
            log_callback: ログ出力用のコールバック関数
        
        Returns:
            bool: 成功した場合True
        """
        def log(msg):
            if log_callback:
                log_callback(msg)
            logger.info(msg)
        
        try:
            if inspector_df is None or inspector_df.empty:
                log("エクスポートする検査員割振りデータがありません")
                return False
            
            if not self.sheets_url:
                log("GoogleスプレッドシートのURLが設定されていません")
                return False
            
            log("Googleスプレッドシートへの出力を開始します")
            
            # スプレッドシートを取得
            spreadsheet = self._get_spreadsheet()
            if not spreadsheet:
                log("スプレッドシートの取得に失敗しました")
                return False
            
            # シート「振分表」を取得（存在しない場合は作成）
            try:
                worksheet = spreadsheet.worksheet("振分表")
            except Exception:
                log("シート「振分表」が存在しないため、作成します")
                worksheet = spreadsheet.add_worksheet(title="振分表", rows=300, cols=100)
            
            # クリアする範囲を定義
            # 注意: A4:A200は削除対象外（保持する必要があるデータ）
            clear_ranges = [
                "B4:J200",  # A列を除外（A4:A200を保持）
                "M4:BB200",
                "A205:A244",
                "M205:Q244"
            ]
            
            log("指定範囲をクリア中...")
            for range_str in clear_ranges:
                try:
                    worksheet.batch_clear([range_str])
                except Exception as e:
                    log(f"警告: 範囲 {range_str} のクリアに失敗しました: {str(e)}")
            
            log("データを書き込み中...")
            
            # デバッグ: 出荷予定日列の値を確認
            if '出荷予定日' in inspector_df.columns:
                cleaning_count = inspector_df['出荷予定日'].astype(str).str.contains('当日洗浄', na=False).sum()
                log(f"デバッグ: 出荷予定日列に当日洗浄品が含まれる行数: {cleaning_count}")
                if cleaning_count > 0:
                    cleaning_values = inspector_df[inspector_df['出荷予定日'].astype(str).str.contains('当日洗浄', na=False)]['出荷予定日'].unique()
                    log(f"デバッグ: 当日洗浄品の値の例: {list(cleaning_values[:5])}")
            
            # データを書き込む
            # 列のマッピング
            column_mapping = {
                '出荷予定日': 'AC4',
                '品番': 'B4',
                '品名': 'C4',
                '客先': 'D4',
                '生産ロットID': 'E4',
                'ロット数量': 'F4',
                '指示日': 'G4',
                '号機': 'H4',
                '現在工程名': 'I4',
                '秒/個': 'J4',
                '検査員1': 'M4',
                '検査員2': 'N4',
                '検査員3': 'O4',
                '検査員4': 'P4',
                '検査員5': 'Q4'
            }
            
            # 各列のデータを準備
            values_to_write = {}
            
            for col_name, start_cell in column_mapping.items():
                if col_name in inspector_df.columns:
                    # 日付列のフォーマット（スラッシュ区切りで書き込む）
                    if col_name in ['出荷予定日', '指示日']:
                        data = []
                        for idx, val in enumerate(inspector_df[col_name]):
                            # NaTや空の値をチェック（fillnaを使わずに直接処理）
                            if pd.isna(val) or val == '' or str(val) == 'nan':
                                data.append('')
                            else:
                                # 値を文字列に変換（型に関わらず）
                                val_str = str(val).strip()  # 前後の空白を除去
                                
                                # 出荷予定日列で「当日洗浄上がり品」の場合は「当日洗浄品」と表示
                                # 文字列型か、または文字列表現が「当日洗浄上がり品」の場合をチェック
                                if col_name == '出荷予定日':
                                    # 文字列として比較（型に関わらず、より確実に判定）
                                    # 元の値が文字列型か、または文字列表現に「当日洗浄」が含まれるかチェック
                                    is_cleaning_product = False
                                    
                                    # 方法1: 文字列として直接比較
                                    if (val_str == "当日洗浄上がり品" or 
                                        val_str == "当日洗浄品"):
                                        is_cleaning_product = True
                                    
                                    # 方法2: 文字列に「当日洗浄」が含まれるかチェック
                                    if not is_cleaning_product and "当日洗浄" in val_str:
                                        is_cleaning_product = True
                                    
                                    # 方法3: 元の値が文字列型で、かつ「当日洗浄」が含まれるかチェック
                                    if not is_cleaning_product and isinstance(val, str) and "当日洗浄" in val:
                                        is_cleaning_product = True
                                    
                                    if is_cleaning_product:
                                        data.append("当日洗浄品")
                                        if log_callback:
                                            log_callback(f"デバッグ: 当日洗浄品を検出しました（行{idx+1}）。値: {repr(val)}, 型: {type(val).__name__}, 文字列: {repr(val_str)}")
                                        continue  # 次のループへ
                                
                                # 日付として変換を試みる（当日洗浄品でない場合のみ）
                                try:
                                    # 日付をyyyy/mm/dd形式に変換
                                    date_value = pd.to_datetime(val, errors='raise')
                                    # NaT（Not a Time）の場合は空文字を追加
                                    if pd.isna(date_value):
                                        data.append('')
                                    else:
                                        # 確実にyyyy/mm/dd形式（ゼロ埋め）で書き込む
                                        formatted_date = date_value.strftime('%Y/%m/%d')
                                        data.append(formatted_date)
                                except (ValueError, TypeError, Exception) as e:
                                    # 日付変換に失敗した場合は元の値をそのまま使用
                                    if log_callback:
                                        log_callback(f"デバッグ: 日付変換失敗（行{idx+1}）。値: {repr(val)}, 型: {type(val).__name__}, エラー: {str(e)}")
                                    data.append(val_str)
                    else:
                        # 日付列以外は通常の処理
                        data = inspector_df[col_name].fillna('').astype(str).tolist()
                    
                    # 検査員列の処理（スキル値の除去）
                    if col_name.startswith('検査員'):
                        data = [
                            str(val).split('(')[0].strip() 
                            if '(' in str(val) and pd.notna(val) and str(val) != 'nan'
                            else (str(val) if pd.notna(val) and str(val) != 'nan' else '')
                            for val in inspector_df[col_name]
                        ]
                    
                    # 開始セルから終了セルまでの範囲を計算
                    start_coord = self._cell_to_coord(start_cell)
                    if start_coord:
                        start_row, start_col = start_coord
                        end_row = start_row + len(data) - 1
                        end_col = start_col
                        
                        # 範囲文字列を作成
                        range_str = f"{self._coord_to_cell(start_row, start_col)}:{self._coord_to_cell(end_row, end_col)}"
                        
                        # データを2次元配列に変換（gspreadの形式）
                        values = [[val] for val in data]
                        
                        values_to_write[range_str] = values
                else:
                    # 列が存在しない場合は空データを書き込む（検査員1～5以外）
                    if col_name.startswith('検査員'):
                        # 検査員列が存在しない場合はスキップ
                        continue
                    else:
                        # その他の列は空データを書き込む
                        start_coord = self._cell_to_coord(start_cell)
                        if start_coord:
                            start_row, start_col = start_coord
                            end_row = start_row + len(inspector_df) - 1
                            end_col = start_col
                            range_str = f"{self._coord_to_cell(start_row, start_col)}:{self._coord_to_cell(end_row, end_col)}"
                            values_to_write[range_str] = [[''] for _ in range(len(inspector_df))]
            
            # 個別に書き込み（gspreadのupdateメソッドを使用）
            if values_to_write:
                success_count = 0
                for range_str, values in values_to_write.items():
                    try:
                        worksheet.update(range_str, values, value_input_option='USER_ENTERED')
                        success_count += 1
                    except Exception as e:
                        log(f"警告: 範囲 {range_str} の書き込みに失敗しました: {str(e)}")
                
                if success_count > 0:
                    log(f"{success_count}/{len(values_to_write)}個の範囲にデータを書き込みました")
            
            log(f"Googleスプレッドシートへの出力が完了しました: {len(inspector_df)}件")
            return True
            
        except ImportError:
            log("gspreadライブラリがインストールされていません。pip install gspread google-auth でインストールしてください")
            return False
        except Exception as e:
            error_msg = f"Googleスプレッドシートエクスポート中にエラーが発生しました: {str(e)}"
            log(error_msg)
            logger.error(error_msg)
            return False

