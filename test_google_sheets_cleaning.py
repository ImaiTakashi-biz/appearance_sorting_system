"""
Googleスプレッドシート「洗浄二次処理依頼」から今日のデータを取得し、
Accessデータベースのt_現品票履歴からロットを抽出するテストスクリプト
"""

import os
import re
import sys
import time
import warnings
import pyodbc
from datetime import datetime, timedelta
from pathlib import Path

# 正規表現パターンの事前コンパイル（高速化）
_MACHINE_PATTERN = re.compile(r'([A-Z]-\d+)')
_DATE_PATTERN = re.compile(r'(\d{1,2}/\d{1,2})')
_LOT_PATTERN = re.compile(r'(\d+)\s*ロット')

# pandasの警告を抑制（最初に実行）
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', message='.*SQLAlchemy.*')
warnings.filterwarnings('ignore', message='.*pandas only supports.*')

# loguruのログ出力を完全に抑制（config.pyが読み込まれる前に実行）
from loguru import logger
logger.remove()
# 何もしないsink関数を追加（すべてのログを無視）
def noop_sink(message):
    pass
logger.add(noop_sink, level="DEBUG")

from app.env_loader import load_env_file
from app.export.google_sheets_exporter_service import GoogleSheetsExporter
from app.config import DatabaseConfig
import pandas as pd


def parse_remarks(remarks):
    """
    詳細・備考から号機、開始日付、日数を抽出（高速化版：事前コンパイル済み正規表現を使用）
    
    Args:
        remarks: 詳細・備考の文字列（例: "E-11　10/26～　9ロット"）
    
    Returns:
        dict: {'machine': 'E-11', 'start_date': '10/26', 'days': 9} または None
    """
    if not remarks or not isinstance(remarks, str):
        return None
    
    result = {}
    
    # 号機の抽出（事前コンパイル済みパターンを使用）
    machine_match = _MACHINE_PATTERN.search(remarks)
    if machine_match:
        result['machine'] = machine_match.group(1)
    
    # 日付の抽出（事前コンパイル済みパターンを使用）
    date_match = _DATE_PATTERN.search(remarks)
    if date_match:
        result['start_date'] = date_match.group(1)
    
    # ロット数の抽出（事前コンパイル済みパターンを使用）
    lot_match = _LOT_PATTERN.search(remarks)
    if lot_match:
        result['days'] = int(lot_match.group(1))
    
    # すべての要素が揃っている場合のみ返す
    if 'machine' in result and 'start_date' in result and 'days' in result:
        return result
    
    return None


def generate_date_range(start_date_str, days):
    """
    開始日付から指定日数分の日付リストを生成
    
    Args:
        start_date_str: 開始日付（MM/DD形式、例: "10/26"）
        days: 日数（例: 9）
    
    Returns:
        list: 日付文字列のリスト（YYYY-MM-DD形式）
    """
    try:
        # 現在の年を取得
        current_year = datetime.now().year
        
        # MM/DD形式を日付オブジェクトに変換
        month, day = map(int, start_date_str.split('/'))
        start_date = datetime(current_year, month, day)
        
        # 日付リストを生成（リスト内包表記で高速化）
        date_list = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)]
        return date_list
    except Exception as e:
        logger.error(f"日付範囲の生成に失敗しました: {str(e)}")
        return []


# テーブル構造のキャッシュ（高速化のため）
_table_structure_cache = None

def _get_table_structure(connection):
    """テーブル構造を取得（キャッシュ機能付き）"""
    global _table_structure_cache
    
    if _table_structure_cache is not None:
        return _table_structure_cache
    
    try:
        columns_query = "SELECT TOP 1 * FROM [t_現品票履歴]"
        sample_df = pd.read_sql(columns_query, connection)
        
        if sample_df.empty:
            logger.warning("t_現品票履歴テーブルにデータが見つかりません")
            _table_structure_cache = {"columns": [], "available": []}
            return _table_structure_cache
        
        actual_columns = sample_df.columns.tolist()
        
        # 取得したい列のリスト
        desired_columns = [
            "品番", "品名", "客先", "数量", "ロット数量", "指示日", "号機", 
            "現在工程番号", "現在工程名", "現在工程二次処理", "生産ロットID", "材料識別"
        ]
        
        # テーブルに存在する列のみを抽出
        available_columns = [col for col in desired_columns if col in actual_columns]
        
        # 必須列が不足している場合は全列を取得
        required_cols = ["品番", "数量", "指示日", "号機", "生産ロットID"]
        missing_required = [col for col in required_cols if col in actual_columns and col not in available_columns]
        
        if missing_required:
            logger.warning(f"必須列が不足しています: {missing_required}。全列を取得します。")
            available_columns = actual_columns
        elif not available_columns:
            logger.warning("希望する列が見つかりません。全列を取得します。")
            available_columns = actual_columns
        
        _table_structure_cache = {
            "columns": actual_columns,
            "available": available_columns
        }
        
        return _table_structure_cache
        
    except Exception as e:
        logger.error(f"テーブル構造の取得に失敗しました: {str(e)}")
        return {"columns": [], "available": []}


def get_lots_from_access(connection, instruction_date=None, machine=None, date_list=None):
    """
    t_現品票履歴からロットを取得（高速化版）
    材料識別が5のレコードのみを対象とする
    
    Args:
        connection: Accessデータベース接続
        instruction_date: 指示日（YYYY-MM-DD形式、またはNone）
        machine: 号機（例: "E-11"、またはNone）
        date_list: 日付リスト（YYYY-MM-DD形式のリスト、またはNone）
    
    Returns:
        pd.DataFrame: 取得したロットデータ
    """
    try:
        # テーブル構造を取得（キャッシュ使用）
        table_info = _get_table_structure(connection)
        available_columns = table_info["available"]
        
        if not available_columns:
            return pd.DataFrame()
        
        columns_str = ", ".join([f"[{col}]" for col in available_columns])
        
        # WHERE条件を構築（高速化のため範囲検索を優先）
        where_conditions = []
        
        # 指示日でフィルタリング
        if instruction_date and "指示日" in available_columns:
            # 日付形式を変換（YYYY-MM-DD → Access形式）
            date_obj = pd.to_datetime(instruction_date)
            date_str = date_obj.strftime('#%Y-%m-%d#')
            where_conditions.append(f"[指示日] = {date_str}")
        
        # 日付リストでフィルタリング（範囲検索を優先）
        elif date_list and "指示日" in available_columns and len(date_list) > 0:
            # 日付リストをソート（文字列形式のまま比較して高速化）
            sorted_dates_str = sorted(date_list)
            start_date_str = sorted_dates_str[0]
            end_date_str = sorted_dates_str[-1]
            
            # 日付オブジェクトに変換（範囲計算のため）
            start_date = pd.to_datetime(start_date_str)
            end_date = pd.to_datetime(end_date_str)
            
            # 連続した日付範囲の場合はBETWEENを使用（高速化）
            expected_days = len(date_list)
            actual_days = (end_date - start_date).days + 1
            
            if expected_days == actual_days:
                # 連続した日付範囲なのでBETWEENを使用
                start_str = start_date.strftime('#%Y-%m-%d#')
                end_str = end_date.strftime('#%Y-%m-%d#')
                where_conditions.append(f"[指示日] >= {start_str} AND [指示日] <= {end_str}")
            else:
                # 連続していない場合はIN句を使用（ただし最大50件まで）
                if len(date_list) <= 50:
                    # 文字列形式のまま使用して変換を削減
                    date_conditions = [f"[指示日] = #{date_str}#" for date_str in date_list]
                    if date_conditions:
                        where_conditions.append(f"({' OR '.join(date_conditions)})")
                else:
                    # 50件を超える場合は範囲検索を使用
                    start_str = start_date.strftime('#%Y-%m-%d#')
                    end_str = end_date.strftime('#%Y-%m-%d#')
                    where_conditions.append(f"[指示日] >= {start_str} AND [指示日] <= {end_str}")
                    logger.info(f"日付リストが大きいため、範囲検索を使用します: {len(date_list)}件")
        
        # 号機でフィルタリング（インデックスが効くように先に配置）
        if machine and "号機" in available_columns:
            # SQLインジェクション対策
            escaped_machine = machine.replace("'", "''")
            where_conditions.insert(0, f"[号機] = '{escaped_machine}'")  # 先頭に配置してインデックスを活用
        
        # 材料識別でフィルタリング（5のみを対象）
        if "材料識別" in available_columns:
            where_conditions.append("[材料識別] = 5")
        
        if not where_conditions:
            logger.warning("フィルタ条件が設定されていません")
            return pd.DataFrame()
        
        where_clause = " AND ".join(where_conditions)
        
        # クエリを実行（高速化のため必要な列のみ取得）
        query = f"""
        SELECT {columns_str}
        FROM [t_現品票履歴]
        WHERE {where_clause}
        ORDER BY [指示日], [号機]
        """
        
        # クエリ実行時間を計測
        start_time = time.time()
        lots_df = pd.read_sql(query, connection)
        elapsed_time = time.time() - start_time
        
        # logger.debug(f"ロット取得完了: {len(lots_df)}件 ({elapsed_time:.2f}秒)")
        
        return lots_df
        
    except Exception as e:
        logger.error(f"t_現品票履歴からのロット取得中にエラーが発生しました: {str(e)}")
        return pd.DataFrame()


def get_lots_from_access_batch(connection, requests):
    """
    複数のリクエストをバッチ処理でまとめて取得（高速化版）
    材料識別が5のレコードのみを対象とする
    
    Args:
        connection: Accessデータベース接続
        requests: リクエストのリスト [{"instruction_date": "...", "machine": "..."}, ...]
    
    Returns:
        pd.DataFrame: 取得したロットデータ
    """
    if not requests:
        return pd.DataFrame()
    
    try:
        table_info = _get_table_structure(connection)
        available_columns = table_info["available"]
        
        if not available_columns:
            return pd.DataFrame()
        
        columns_str = ", ".join([f"[{col}]" for col in available_columns])
        
        # すべての条件をORで結合
        all_conditions = []
        
        for req in requests:
            conditions = []
            
            # 指示日
            if req.get("instruction_date"):
                date_obj = pd.to_datetime(req["instruction_date"])
                date_str = date_obj.strftime('#%Y-%m-%d#')
                conditions.append(f"[指示日] = {date_str}")
            
            # 日付リスト
            if req.get("date_list"):
                date_list = req["date_list"]
                if len(date_list) > 0:
                    # 文字列形式のままソートして高速化
                    sorted_dates_str = sorted(date_list)
                    start_date = pd.to_datetime(sorted_dates_str[0])
                    end_date = pd.to_datetime(sorted_dates_str[-1])
                    start_str = start_date.strftime('#%Y-%m-%d#')
                    end_str = end_date.strftime('#%Y-%m-%d#')
                    conditions.append(f"[指示日] >= {start_str} AND [指示日] <= {end_str}")
            
            # 号機
            if req.get("machine") and "号機" in available_columns:
                escaped_machine = req["machine"].replace("'", "''")
                conditions.append(f"[号機] = '{escaped_machine}'")
            
            if conditions:
                all_conditions.append(f"({' AND '.join(conditions)})")
        
        if not all_conditions:
            return pd.DataFrame()
        
        where_clause = " OR ".join(all_conditions)
        
        # 材料識別でフィルタリング（5のみを対象）
        if "材料識別" in available_columns:
            where_clause = f"({where_clause}) AND [材料識別] = 5"
        
        # バッチクエリを実行
        query = f"""
        SELECT {columns_str}
        FROM [t_現品票履歴]
        WHERE {where_clause}
        ORDER BY [指示日], [号機]
        """
        
        start_time = time.time()
        lots_df = pd.read_sql(query, connection)
        elapsed_time = time.time() - start_time
        
        # logger.debug(f"バッチロット取得完了: {len(lots_df)}件 ({elapsed_time:.2f}秒)")
        
        return lots_df
        
    except Exception as e:
        logger.error(f"バッチロット取得中にエラーが発生しました: {str(e)}")
        return pd.DataFrame()


def get_cleaning_instructions_from_sheets(exporter, sheet_name):
    """
    洗浄指示シートから指定範囲のデータを取得
    - AB列が1の場合：無条件で対象
    - AB列が2または3の場合：AA列が5なら対象
    
    Args:
        exporter: GoogleSheetsExporterインスタンス
        sheet_name: シート名（例: "1106"）
    
    Returns:
        list: 取得したデータのリスト（各要素は{"品番": ..., "品名": ..., "客先名": ..., "号機": ...}）
        エラー時は空リストを返す
    """
    try:
        # スプレッドシートを取得
        spreadsheet = exporter._get_spreadsheet()
        if not spreadsheet:
            logger.error("スプレッドシートの取得に失敗しました")
            return []
        
        # 指定されたシートを取得
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except Exception as e:
            logger.error(f"シート「{sheet_name}」の取得に失敗しました: {str(e)}")
            return []
        
        # A43:AB107の範囲を取得
        range_data = worksheet.get('A43:AB107')
        
        if not range_data:
            logger.info(f"シート「{sheet_name}」の指定範囲にデータがありません")
            return []
        
        # 列インデックス（0始まり）
        # F列 = 6列目 = インデックス5
        # H列 = 8列目 = インデックス7
        # I列 = 9列目 = インデックス8
        # L列 = 12列目 = インデックス11
        # W列 = 23列目 = インデックス22
        # AA列 = 27列目 = インデックス26
        # AB列 = 28列目 = インデックス27
        
        # リスト内包表記で高速化
        result_data = []
        # 事前に条件判定用のヘルパー関数を定義（高速化）
        def get_cell_value(row, idx, default=""):
            """セル値を安全に取得（高速化版）"""
            if len(row) > idx and row[idx]:
                val = row[idx]
                return val.strip() if isinstance(val, str) else str(val).strip()
            return default
        
        for row_index, row in enumerate(range_data, start=43):
            if not row or len(row) < 28:
                continue
            
            # AB列とAA列の値を取得（高速化）
            ab_value = get_cell_value(row, 27)
            aa_value = get_cell_value(row, 26)
            
            # 条件判定（早期リターンで高速化）
            # AB列が1の場合：無条件で対象
            if ab_value == '1':
                pass  # 処理を続行
            # AB列が2または3の場合：AA列が5なら対象
            elif ab_value in ('2', '3'):  # tupleの方が高速
                if aa_value != '5':
                    continue  # 条件を満たさない場合はスキップ
            else:
                # AB列が1, 2, 3以外の場合はスキップ
                continue
            
            # 必要な列の値を取得（一度に取得して高速化）
            result_data.append({
                "号機": get_cell_value(row, 5),
                "客先名": get_cell_value(row, 7),
                "品番": get_cell_value(row, 8),
                "品名": get_cell_value(row, 11),
                "数量": get_cell_value(row, 22),
                "行番号": row_index,
                "AB列": ab_value,
                "AA列": aa_value
            })
        
        # logger.debug(f"シート「{sheet_name}」から {len(result_data)}件のデータを取得しました")
        return result_data
        
    except Exception as e:
        logger.error(f"洗浄指示シートからのデータ取得中にエラーが発生しました: {str(e)}")
        return []


def get_today_requests_from_sheets(exporter, sheet_name="依頼一覧"):
    """
    指定されたシートから今日の日付の行のA列からK列のデータを取得
    
    Args:
        exporter: GoogleSheetsExporterインスタンス
        sheet_name: シート名（デフォルト: "依頼一覧")
    
    Returns:
        list: 今日の日付の行のデータリスト（各要素はA列からK列の値のリスト）
        エラー時は空リストを返す
    """
    try:
        # スプレッドシートを取得
        spreadsheet = exporter._get_spreadsheet()
        if not spreadsheet:
            logger.error("スプレッドシートの取得に失敗しました")
            return []
        
        # 指定されたシートを取得
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except Exception as e:
            logger.error(f"シート「{sheet_name}」の取得に失敗しました: {str(e)}")
            return []
        
        # 全データを取得（A列からK列まで、2行目以降）
        # ヘッダー行は1行目なので、2行目から読み取る
        all_values = worksheet.get('A2:K')
        
        if not all_values:
            logger.info(f"シート「{sheet_name}」にデータがありません")
            return []
        
        # 今日の日付を取得（YYYY/MM/DD形式）
        today = datetime.now().strftime('%Y/%m/%d')
        
        # 今日の日付と一致する行をフィルタリング（高速化）
        today_rows = []
        # 事前に今日の日付をdatetimeオブジェクトに変換（一度だけ）
        today_dt = datetime.now()
        
        for row in all_values:
            if not row or not row[0]:
                continue
            
            date_val = row[0]
            # 文字列変換を最小限に（既に文字列の場合はそのまま使用）
            if isinstance(date_val, str):
                date_str = date_val.strip()
            else:
                date_str = str(date_val).strip()
            
            # 直接一致チェック（最も高速）
            if date_str == today:
                row_data = row[:11] if len(row) >= 11 else row + [''] * (11 - len(row))
                today_rows.append(row_data)
                continue
            
            # 日付解析を試行（エラー時はスキップ）
            try:
                parsed_date = pd.to_datetime(date_str, errors='coerce', format='%Y/%m/%d')
                if pd.notna(parsed_date) and parsed_date.date() == today_dt.date():
                    row_data = row[:11] if len(row) >= 11 else row + [''] * (11 - len(row))
                    today_rows.append(row_data)
            except Exception:
                continue
        
        # logger.debug(f"シート「{sheet_name}」から今日（{today}）のデータを{len(today_rows)}件取得しました")
        return today_rows
        
    except ImportError:
        logger.error("gspreadライブラリがインストールされていません")
        return []
    except Exception as e:
        error_msg = f"シートからのデータ取得中にエラーが発生しました: {str(e)}"
        logger.error(error_msg)
        return []


def main():
    """メイン処理"""
    try:
        # 環境変数ファイルを読み込み
        env_file = "config.env"
        if not Path(env_file).exists():
            print(f"❌ エラー: 設定ファイルが見つかりません: {env_file}")
            return
        
        # 警告を一時的に抑制して設定を読み込み
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            load_env_file(env_file)
        
        # 環境変数から設定を取得
        google_sheets_url_cleaning = os.getenv("GOOGLE_SHEETS_URL_CLEANING")
        google_sheets_url_cleaning_instructions = os.getenv("GOOGLE_SHEETS_URL_CLEANING_INSTRUCTIONS", 
                                                              "https://docs.google.com/spreadsheets/d/1VMLNhPaMauihxSO4NyJNUzFhL6EAcjqfTHnwIcEBL-k/edit?usp=sharing")
        google_sheets_credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
        
        if not google_sheets_url_cleaning:
            print("❌ エラー: GOOGLE_SHEETS_URL_CLEANINGが設定されていません")
            return
        
        if not google_sheets_credentials_path:
            print("❌ エラー: GOOGLE_SHEETS_CREDENTIALS_PATHが設定されていません")
            return
        
        # 今日の日付からシート名を生成（MMDD形式、例: 1106）
        today = datetime.now()
        sheet_name_today = today.strftime('%m%d')  # 月日のみ
        
        # GoogleSheetsExporterを初期化（並列処理のため）
        exporter_cleaning = GoogleSheetsExporter(
            sheets_url=google_sheets_url_cleaning,
            credentials_path=google_sheets_credentials_path
        )
        
        exporter_instructions = GoogleSheetsExporter(
            sheets_url=google_sheets_url_cleaning_instructions,
            credentials_path=google_sheets_credentials_path
        )
        
        # データ取得（並列処理はgspreadの制限により順次実行）
        today_data = get_today_requests_from_sheets(exporter_cleaning, "依頼一覧")
        cleaning_instructions = get_cleaning_instructions_from_sheets(exporter_instructions, sheet_name_today)
        
        if not today_data:
            print("⚠️  今日のデータが見つかりませんでした")
            return
        
        # 列名を定義（ヘッダー行の列名）
        column_names = [
            "期限", "詳細・備考", "依頼者", "品番", "品名", 
            "客先", "指示日", "号機", "数量", "現在工程名", "生産ロットID"
        ]
        
        # Accessデータベース接続
        config = DatabaseConfig()
        connection_string = config.get_connection_string()
        connection = pyodbc.connect(connection_string)
        
        # バッチ処理用にリクエストを準備
        total_start_time = time.time()
        batch_requests = []
        row_info_list = []
        
        # 列名のインデックスを事前に取得（高速化）
        col_idx_map = {name: idx for idx, name in enumerate(column_names)}
        idx_指示日 = col_idx_map.get("指示日", -1)
        idx_号機 = col_idx_map.get("号機", -1)
        idx_詳細備考 = col_idx_map.get("詳細・備考", -1)
        
        for i, row in enumerate(today_data, 1):
            # インデックスで直接アクセス（dict作成を避けて高速化）
            instruction_date = row[idx_指示日].strip() if idx_指示日 >= 0 and len(row) > idx_指示日 and row[idx_指示日] else ""
            machine = row[idx_号機].strip() if idx_号機 >= 0 and len(row) > idx_号機 and row[idx_号機] else ""
            remarks = row[idx_詳細備考].strip() if idx_詳細備考 >= 0 and len(row) > idx_詳細備考 and row[idx_詳細備考] else ""
            
            request = {}
            
            # 指示日と号機が両方ある場合
            if instruction_date and machine:
                try:
                    # 日付形式を直接変換（高速化）
                    # YYYY/MM/DD形式を想定
                    if '/' in instruction_date:
                        parts = instruction_date.split('/')
                        if len(parts) == 3:
                            request["instruction_date"] = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                            request["machine"] = machine
                    else:
                        # その他の形式の場合はpd.to_datetimeを使用
                        instruction_date_obj = pd.to_datetime(instruction_date)
                        request["instruction_date"] = instruction_date_obj.strftime('%Y-%m-%d')
                        request["machine"] = machine
                except Exception:
                    pass
            
            # 指示日または号機が欠落している場合、詳細・備考を解析
            elif remarks:
                parsed = parse_remarks(remarks)
                if parsed:
                    date_list = generate_date_range(parsed['start_date'], parsed['days'])
                    request["date_list"] = date_list
                    request["machine"] = parsed['machine']
            
            if request:
                batch_requests.append(request)
                # row_dictは必要時のみ作成（メモリ節約）
                row_info_list.append({
                    "index": i,
                    "row": row,  # 元のrowを保存（必要時のみdict化）
                    "request": request
                })
        
        # バッチ処理で一括取得（高速化）
        all_lots = []
        if batch_requests:
            batch_start_time = time.time()
            batch_lots_df = get_lots_from_access_batch(connection, batch_requests)
            batch_elapsed = time.time() - batch_start_time
            
            if not batch_lots_df.empty:
                # 日付列を事前に変換してキャッシュ（高速化）
                if "指示日" in batch_lots_df.columns:
                    batch_lots_df['_指示日_parsed'] = pd.to_datetime(batch_lots_df['指示日'], errors='coerce')
                    batch_lots_df['_指示日_str'] = batch_lots_df['_指示日_parsed'].dt.strftime('%Y-%m-%d')
                
                # 各リクエストに対応するロットを分離（高速化）
                for row_info in row_info_list:
                    req = row_info["request"]
                    filtered_df = batch_lots_df
                    
                    # フィルタリング（マスクを使用して高速化）
                    mask = pd.Series([True] * len(filtered_df), index=filtered_df.index)
                    
                    if req.get("instruction_date"):
                        mask = mask & (filtered_df['_指示日_str'] == req["instruction_date"])
                    
                    if req.get("machine") and "号機" in filtered_df.columns:
                        mask = mask & (filtered_df['号機'] == req["machine"])
                    
                    if req.get("date_list"):
                        # setを使用して高速化
                        date_set = set(req["date_list"])  # 既に文字列形式なので変換不要
                        mask = mask & filtered_df['_指示日_str'].isin(date_set)
                    
                    filtered_df = filtered_df[mask].copy()
                    # 一時列を削除
                    filtered_df = filtered_df.drop(columns=['_指示日_parsed', '_指示日_str'], errors='ignore')
                    
                    row_info["lots_df"] = filtered_df
                    if not filtered_df.empty:
                        all_lots.append(filtered_df)
        
        # 洗浄指示からもロットを取得（接続を閉じる前に実行）
        if cleaning_instructions:
            print(f"\n洗浄指示からロットを取得中... ({len(cleaning_instructions)}件の指示)")
            
            # 洗浄指示の号機と品番の組み合わせを収集
            instruction_combinations = []
            for instruction in cleaning_instructions:
                machine = instruction.get("号機", "").strip()
                product_number = instruction.get("品番", "").strip()
                
                if machine and product_number:
                    instruction_combinations.append({
                        "machine": machine,
                        "product_number": product_number
                    })
            
            if instruction_combinations:
                # 号機のリストを取得（重複除去）
                unique_machines = list(set([combo["machine"] for combo in instruction_combinations]))
                
                # 号機でバッチ検索
                machine_requests = [{"machine": machine} for machine in unique_machines]
                instruction_lots_df = get_lots_from_access_batch(connection, machine_requests)
                
                if not instruction_lots_df.empty:
                    # 号機と品番の組み合わせで正確にフィルタリング
                    filtered_lots = []
                    
                    for combo in instruction_combinations:
                        machine = combo["machine"]
                        product_number = combo["product_number"]
                        
                        # 号機と品番の両方でフィルタリング
                        mask = (
                            (instruction_lots_df['号機'] == machine) &
                            (instruction_lots_df['品番'] == product_number)
                        )
                        filtered = instruction_lots_df[mask].copy()
                        
                        if not filtered.empty:
                            filtered_lots.append(filtered)
                    
                    if filtered_lots:
                        instruction_lots_df = pd.concat(filtered_lots, ignore_index=True)
                        # 重複を除去
                        if '生産ロットID' in instruction_lots_df.columns:
                            instruction_lots_df = instruction_lots_df.drop_duplicates(subset=['生産ロットID'], keep='first')
                        
                        all_lots.append(instruction_lots_df)
                        print(f"洗浄指示から {len(instruction_lots_df)}件のロットを取得しました（号機・品番の組み合わせでフィルタリング済み）")
        
        total_elapsed = time.time() - total_start_time
        
        # 接続を閉じる
        connection.close()
        
        # ロット情報を表示
        if all_lots:
            final_lots_df = pd.concat(all_lots, ignore_index=True)
            
            # 重複を除去（生産ロットIDが同じ場合は1件のみ残す）
            if '生産ロットID' in final_lots_df.columns:
                final_lots_df = final_lots_df.drop_duplicates(subset=['生産ロットID'], keep='first')
            
            unique_lots = final_lots_df['生産ロットID'].nunique() if '生産ロットID' in final_lots_df.columns else 0
            
            print(f"\n{'='*80}")
            print(f"ロット取得完了: {len(final_lots_df)}件（ユニーク: {unique_lots}件） - {total_elapsed:.2f}秒")
            print(f"{'='*80}")
            
            # ロット情報を表示（iterrows()を避けて高速化）
            # 列名のインデックスを事前に取得（高速化）
            col_idx = {col: idx for idx, col in enumerate(final_lots_df.columns)}
            get_col = lambda col: col_idx.get(col, -1)
            
            idx_生産ロットID = get_col('生産ロットID')
            idx_品番 = get_col('品番')
            idx_品名 = get_col('品名')
            idx_客先 = get_col('客先')
            idx_指示日 = get_col('指示日')
            idx_号機 = get_col('号機')
            idx_ロット数量 = get_col('ロット数量')
            idx_数量 = get_col('数量')
            idx_現在工程名 = get_col('現在工程名')
            
            # itertuples()の方がiterrows()より約10倍高速（列インデックスでアクセス）
            for idx, lot_tuple in enumerate(final_lots_df.itertuples(index=False), 1):
                lot_id = lot_tuple[idx_生産ロットID] if idx_生産ロットID >= 0 else ''
                product_number = lot_tuple[idx_品番] if idx_品番 >= 0 else ''
                product_name = lot_tuple[idx_品名] if idx_品名 >= 0 else ''
                customer = lot_tuple[idx_客先] if idx_客先 >= 0 else ''
                instruction_date = lot_tuple[idx_指示日] if idx_指示日 >= 0 else ''
                machine = lot_tuple[idx_号機] if idx_号機 >= 0 else ''
                quantity = lot_tuple[idx_ロット数量] if idx_ロット数量 >= 0 else (lot_tuple[idx_数量] if idx_数量 >= 0 else '')
                process = lot_tuple[idx_現在工程名] if idx_現在工程名 >= 0 else ''
                
                print(f"\n【ロット {idx}】")
                print(f"  生産ロットID: {lot_id}")
                print(f"  品番: {product_number}")
                print(f"  品名: {product_name}")
                print(f"  客先: {customer}")
                print(f"  指示日: {instruction_date}")
                print(f"  号機: {machine}")
                print(f"  ロット数量: {quantity}")
                print(f"  現在工程名: {process}")
        else:
            print(f"\n⚠️  ロット未取得 - {total_elapsed:.2f}秒")
        
        # 洗浄指示情報を表示
        if cleaning_instructions:
            print(f"\n{'='*80}")
            print(f"洗浄指示: {len(cleaning_instructions)}件")
            print(f"{'='*80}")
            for i, item in enumerate(cleaning_instructions, 1):
                print(f"\n【洗浄指示 {i}】")
                print(f"  品番: {item['品番']}")
                print(f"  品名: {item['品名']}")
                print(f"  客先名: {item['客先名']}")
                print(f"  号機: {item['号機']}")
                print(f"  数量: {item['数量']}")
                print(f"  行番号: {item['行番号']}, AA列: {item['AA列']}, AB列: {item['AB列']}")
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {str(e)}")
        print(f"❌ エラー: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

