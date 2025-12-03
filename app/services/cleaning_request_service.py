"""
洗浄二次処理依頼からロットを取得するサービス
"""

import os
import re
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
from loguru import logger

from app.export.google_sheets_exporter_service import GoogleSheetsExporter

# 正規表現パターンの事前コンパイル（高速化）
_MACHINE_PATTERN = re.compile(r'([A-Z]-\d+)')
_DATE_PATTERN = re.compile(r'(\d{1,2}/\d{1,2})')
_LOT_PATTERN = re.compile(r'(\d+)\s*ロット')

# テーブル構造のキャッシュ（高速化のため）
_table_structure_cache = None


def _parse_remarks(remarks: str) -> Optional[Dict[str, str]]:
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


def _generate_date_range(start_date_str: str, days: int) -> List[str]:
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
        # 現在工程名はロット情報に含めず、空欄として扱う
        desired_columns = [
            "品番", "品名", "客先", "数量", "ロット数量", "指示日", "号機", 
            "現在工程番号", "現在工程二次処理", "生産ロットID", "材料識別"
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


def _get_lots_from_access_batch(connection, requests: List[Dict]) -> pd.DataFrame:
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
        
        try:
            import time
            logger.info(f"バッチクエリを実行中... (条件数: {len(all_conditions)})")
            start_time = time.time()
            lots_df = pd.read_sql(query, connection)
            elapsed_time = time.time() - start_time
            logger.info(f"バッチクエリ完了: {len(lots_df)}件のロットを取得 ({elapsed_time:.2f}秒)")
            
            # 現在工程名列を空欄として追加（ロット情報に含めないため）
            if '現在工程名' not in lots_df.columns:
                lots_df['現在工程名'] = ''
            
            return lots_df
        except Exception as query_error:
            logger.error(f"バッチクエリ実行中にエラーが発生しました: {str(query_error)}")
            logger.error(f"クエリ: {query[:500]}...")  # クエリの最初の500文字をログに出力
            raise
        
    except Exception as e:
        logger.error(f"バッチロット取得中にエラーが発生しました: {str(e)}", exc_info=True)
        return pd.DataFrame()


def _get_cleaning_instructions_from_sheets(exporter: GoogleSheetsExporter, sheet_name: str) -> List[Dict]:
    """
    洗浄指示シートから指定範囲のデータを取得
    - AB列が1の場合：無条件で対象
    - AB列が2または3の場合：AA列が5なら対象
    
    Args:
        exporter: GoogleSheetsExporterインスタンス
        sheet_name: シート名（例: "1107" = 11月7日）
    
    Returns:
        list: 取得したデータのリスト（各要素に"指示日"が含まれる）
    """
    try:
        spreadsheet = exporter._get_spreadsheet()
        if not spreadsheet:
            logger.error("スプレッドシートの取得に失敗しました")
            return []
        
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except Exception as e:
            logger.error(f"シート「{sheet_name}」の取得に失敗しました: {str(e)}")
            return []
        
        # シート名から指示日を生成（MMDD形式 → YYYY-MM-DD形式）
        instruction_date_str = None
        try:
            if len(sheet_name) == 4:  # MMDD形式（例: "1107"）
                month = int(sheet_name[:2])
                day = int(sheet_name[2:])
                current_year = datetime.now().year
                instruction_date = datetime(current_year, month, day)
                instruction_date_str = instruction_date.strftime('%Y-%m-%d')
        except (ValueError, IndexError) as e:
            logger.warning(f"シート名「{sheet_name}」から指示日を生成できませんでした: {str(e)}")
        
        # A43:AB107の範囲を取得
        range_data = worksheet.get('A43:AB107')
        
        if not range_data:
            logger.info(f"シート「{sheet_name}」の指定範囲にデータがありません")
            return []
        
        result_data = []
        
        def get_cell_value(row, idx, default=""):
            """セル値を安全に取得（高速化版）"""
            if len(row) > idx and row[idx]:
                val = row[idx]
                return val.strip() if isinstance(val, str) else str(val).strip()
            return default
        
        for row_index, row in enumerate(range_data, start=43):
            if not row or len(row) < 28:
                continue
            
            # AB列とAA列の値を取得
            ab_value = get_cell_value(row, 27)
            aa_value = get_cell_value(row, 26)
            
            # 条件判定
            if ab_value == '1':
                pass  # 処理を続行
            elif ab_value in ('2', '3'):
                if aa_value != '5':
                    continue
            else:
                continue
            
            instruction_data = {
                "号機": get_cell_value(row, 5),
                "客先名": get_cell_value(row, 7),
                "品番": get_cell_value(row, 8),
                "品名": get_cell_value(row, 11),
                "数量": get_cell_value(row, 22),
                "行番号": row_index,
                "AB列": ab_value,
                "AA列": aa_value
            }
            
            # 指示日を追加
            if instruction_date_str:
                instruction_data["指示日"] = instruction_date_str
            
            result_data.append(instruction_data)
        
        return result_data
        
    except Exception as e:
        logger.error(f"洗浄指示シートからのデータ取得中にエラーが発生しました: {str(e)}")
        return []


def _get_today_requests_from_sheets(exporter: GoogleSheetsExporter, sheet_name: str = "依頼一覧") -> List[List]:
    """
    指定されたシートから今日の日付の行のA列からK列のデータを取得
    
    Args:
        exporter: GoogleSheetsExporterインスタンス
        sheet_name: シート名（デフォルト: "依頼一覧")
    
    Returns:
        list: 今日の日付の行のデータリスト
    """
    try:
        spreadsheet = exporter._get_spreadsheet()
        if not spreadsheet:
            logger.error("スプレッドシートの取得に失敗しました")
            return []
        
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except Exception as e:
            logger.error(f"シート「{sheet_name}」の取得に失敗しました: {str(e)}")
            return []
        
        all_values = worksheet.get('A2:K')
        
        if not all_values:
            logger.info(f"シート「{sheet_name}」にデータがありません")
            return []
        
        today = datetime.now().strftime('%Y/%m/%d')
        today_rows = []
        today_dt = datetime.now()
        
        for row in all_values:
            if not row or not row[0]:
                continue
            
            date_val = row[0]
            if isinstance(date_val, str):
                date_str = date_val.strip()
            else:
                date_str = str(date_val).strip()
            
            if date_str == today:
                row_data = row[:11] if len(row) >= 11 else row + [''] * (11 - len(row))
                today_rows.append(row_data)
                continue
            
            try:
                parsed_date = pd.to_datetime(date_str, errors='coerce', format='%Y/%m/%d')
                if pd.notna(parsed_date) and parsed_date.date() == today_dt.date():
                    row_data = row[:11] if len(row) >= 11 else row + [''] * (11 - len(row))
                    today_rows.append(row_data)
            except Exception:
                continue
        
        return today_rows
        
    except Exception as e:
        logger.error(f"シートからのデータ取得中にエラーが発生しました: {str(e)}")
        return []


def _load_process_master(process_master_path: str, log_callback: callable) -> Optional[pd.DataFrame]:
    """工程マスタを読み込み、存在しない/失敗時はNoneを返す"""
    if not process_master_path:
        return None
    if not os.path.exists(process_master_path):
        log_callback(f"工程マスタファイルが見つかりません: {process_master_path}")
        return None
    try:
        df = pd.read_excel(process_master_path, engine='openpyxl')
        log_callback(f"工程マスタを読み込みました: {len(df)}件")
        return df
    except Exception as e:
        log_callback(f"工程マスタの読み込みに失敗しました: {str(e)}")
        return None


def _infer_process_info(
    product_number: str,
    process_master_df: pd.DataFrame,
    inspection_target_keywords: List[str],
    log_callback: callable
) -> Tuple[Optional[str], Optional[str]]:
    """工程マスタから品番とキーワードで工程番号と工程名を推定"""
    if not product_number or process_master_df is None or process_master_df.empty:
        return None, None

    keywords = [kw.strip() for kw in inspection_target_keywords if isinstance(kw, str) and kw.strip()]
    if not keywords:
        return None, None

    product_col = process_master_df.columns[0]
    matching_rows = process_master_df[process_master_df[product_col] == product_number]
    if matching_rows.empty:
        return None, None

    row = matching_rows.iloc[0]
    for col_idx in range(1, len(process_master_df.columns)):
        cell_value = row.iloc[col_idx]
        if pd.isna(cell_value):
            continue
        cell_str = str(cell_value).strip()
        if not cell_str:
            continue
        for keyword in keywords:
            if keyword in cell_str:
                process_number = str(process_master_df.columns[col_idx]).strip()
                log_callback(
                    f"工程マスタから工程番号を推定: 品番='{product_number}', "
                    f"工程番号='{process_number}', 工程名='{cell_str}', キーワード='{keyword}'"
                )
                return process_number, cell_str
    return None, None


def _ensure_process_info_for_lots(
    lots_df: pd.DataFrame,
    process_master_df: Optional[pd.DataFrame],
    inspection_target_keywords: List[str],
    log_callback: callable
) -> pd.DataFrame:
    """指定されたロット群に対して工程番号/工程名が欠けていれば補完する"""
    if lots_df.empty or process_master_df is None:
        return lots_df

    keywords = [
        kw.strip() for kw in inspection_target_keywords if isinstance(kw, str) and kw.strip()
    ]
    if not keywords:
        keywords = ["外観"]

    if '現在工程名' not in lots_df.columns:
        lots_df['現在工程名'] = ""
    if '現在工程番号' not in lots_df.columns:
        lots_df['現在工程番号'] = ""

    for idx, row in lots_df.iterrows():
        prod_no = str(row.get("品番", "") or "").strip()
        if not prod_no:
            continue
        current_name = str(row.get("現在工程名", "") or "").strip()
        if current_name:
            continue

        inferred_number, inferred_name = _infer_process_info(
            prod_no,
            process_master_df,
            keywords,
            log_callback
        )
        if inferred_name:
            lots_df.at[idx, "現在工程名"] = inferred_name
        if inferred_number:
            lots_df.at[idx, "現在工程番号"] = inferred_number

    return lots_df


def get_cleaning_lots(
    connection: pyodbc.Connection,
    google_sheets_url_cleaning: str,
    google_sheets_url_cleaning_instructions: str,
    google_sheets_credentials_path: str,
    log_callback: Optional[callable] = None,
    process_master_path: Optional[str] = None,
    inspection_target_keywords: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    ?????????????????????
    
    Args:
        connection: Access????????
        google_sheets_url_cleaning: ?????????????????URL
        google_sheets_url_cleaning_instructions: ?????????????URL
        google_sheets_credentials_path: Google Sheets???????
        log_callback: ?????????????????????
        process_master_path: ??????????????????????????????
        inspection_target_keywords: ?????????????????? ['??']?
    
    Returns:
        pd.DataFrame: ??????????????????"????????"?????????
    """
    def log(msg):
        if log_callback:
            log_callback(msg)
        else:
            logger.info(msg)

    process_master_df = _load_process_master(process_master_path, log) if process_master_path else None
    normalized_keywords = [
        kw.strip() for kw in (inspection_target_keywords or []) if isinstance(kw, str) and kw.strip()
    ]
    if not normalized_keywords:
        normalized_keywords = ["外観"]

    try:
        # GoogleSheetsExporterを初期化
        exporter_cleaning = GoogleSheetsExporter(
            sheets_url=google_sheets_url_cleaning,
            credentials_path=google_sheets_credentials_path
        )
        
        exporter_instructions = GoogleSheetsExporter(
            sheets_url=google_sheets_url_cleaning_instructions,
            credentials_path=google_sheets_credentials_path
        )
        
        # 今日の日付からシート名を生成（MMDD形式）
        today = datetime.now()
        sheet_name_today = today.strftime('%m%d')
        
        # データ取得
        log("洗浄二次処理依頼からデータを取得中...")
        today_data = _get_today_requests_from_sheets(exporter_cleaning, "依頼一覧")
        
        log("洗浄指示からデータを取得中...")
        cleaning_instructions = _get_cleaning_instructions_from_sheets(exporter_instructions, sheet_name_today)
        
        # today_dataが空でも洗浄指示があれば処理を続行
        if not today_data and not cleaning_instructions:
            log("今日の洗浄二次処理依頼データも洗浄指示も見つかりませんでした")
            return pd.DataFrame()
        
        if not today_data:
            log("今日の洗浄二次処理依頼データが見つかりませんでした（洗浄指示のみ処理します）")
        
        # 列名を定義
        column_names = [
            "期限", "詳細・備考", "依頼者", "品番", "品名", 
            "客先", "指示日", "号機", "数量", "現在工程名", "生産ロットID"
        ]
        
        # バッチ処理用にリクエストを準備
        batch_requests = []
        row_info_list = []
        
        col_idx_map = {name: idx for idx, name in enumerate(column_names)}
        idx_指示日 = col_idx_map.get("指示日", -1)
        idx_号機 = col_idx_map.get("号機", -1)
        idx_詳細備考 = col_idx_map.get("詳細・備考", -1)
        
        for i, row in enumerate(today_data, 1):
            instruction_date = row[idx_指示日].strip() if idx_指示日 >= 0 and len(row) > idx_指示日 and row[idx_指示日] else ""
            machine = row[idx_号機].strip() if idx_号機 >= 0 and len(row) > idx_号機 and row[idx_号機] else ""
            remarks = row[idx_詳細備考].strip() if idx_詳細備考 >= 0 and len(row) > idx_詳細備考 and row[idx_詳細備考] else ""
            
            request = {}
            
            # 指示日と号機が両方ある場合
            if instruction_date and machine:
                try:
                    if '/' in instruction_date:
                        parts = instruction_date.split('/')
                        if len(parts) == 3:
                            request["instruction_date"] = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                            request["machine"] = machine
                    else:
                        instruction_date_obj = pd.to_datetime(instruction_date)
                        request["instruction_date"] = instruction_date_obj.strftime('%Y-%m-%d')
                        request["machine"] = machine
                except Exception:
                    pass
            
            # 指示日または号機が欠落している場合、詳細・備考を解析
            elif remarks:
                parsed = _parse_remarks(remarks)
                if parsed:
                    date_list = _generate_date_range(parsed['start_date'], parsed['days'])
                    request["date_list"] = date_list
                    request["machine"] = parsed['machine']
            
            if request:
                batch_requests.append(request)
                row_info_list.append({
                    "index": i,
                    "row": row,
                    "request": request
                })
        
        # バッチ処理で一括取得
        all_lots = []
        if batch_requests:
            log(f"バッチ処理でロットを取得中... ({len(batch_requests)}件のリクエスト)")
            batch_lots_df = _get_lots_from_access_batch(connection, batch_requests)
            log(f"バッチ処理で取得したロット数: {len(batch_lots_df)}件")
            
            if not batch_lots_df.empty:
                # 日付列を事前に変換してキャッシュ
                if "指示日" in batch_lots_df.columns:
                    batch_lots_df['_指示日_parsed'] = pd.to_datetime(batch_lots_df['指示日'], errors='coerce')
                    batch_lots_df['_指示日_str'] = batch_lots_df['_指示日_parsed'].dt.strftime('%Y-%m-%d')
                
                # 各リクエストに対応するロットを分離
                filtered_count = 0
                for row_info in row_info_list:
                    req = row_info["request"]
                    filtered_df = batch_lots_df
                    
                    mask = pd.Series([True] * len(filtered_df), index=filtered_df.index)
                    
                    if req.get("instruction_date"):
                        mask = mask & (filtered_df['_指示日_str'] == req["instruction_date"])
                    
                    if req.get("machine") and "号機" in filtered_df.columns:
                        mask = mask & (filtered_df['号機'] == req["machine"])
                    
                    if req.get("date_list"):
                        date_set = set(req["date_list"])
                        mask = mask & filtered_df['_指示日_str'].isin(date_set)
                    
                    filtered_df = filtered_df[mask].copy()
                    filtered_df = filtered_df.drop(columns=['_指示日_parsed', '_指示日_str'], errors='ignore')
                    
                    if not filtered_df.empty:
                        all_lots.append(filtered_df)
                        filtered_count += len(filtered_df)
                        log(f"  リクエスト {row_info['index']}: {len(filtered_df)}件のロットを追加（累計: {filtered_count}件）")
                
                log(f"通常の在庫ロット（洗浄二次処理依頼）: {filtered_count}件をall_lotsに追加しました")
            else:
                log("バッチ処理で取得したロットが空です")
        else:
            log("バッチリクエストが空のため、通常の在庫ロットは取得されませんでした")
        
        # 洗浄指示からもロットを取得
        # Googleスプレッドシートの洗浄指示から取得したデータをそのままロットとして使用
        # t_現品票履歴からの取得は行わない（生産ロットIDと現在工程名は未記載）
        if cleaning_instructions:
            log(f"洗浄指示からロットを取得中... ({len(cleaning_instructions)}件の指示)")
            
            # 洗浄指示の内容をログに出力（デバッグ用）
            for idx, instruction in enumerate(cleaning_instructions, 1):
                log(f"  洗浄指示 {idx}: 号機={instruction.get('号機', '')}, 品番={instruction.get('品番', '')}, 品名={instruction.get('品名', '')}, 指示日={instruction.get('指示日', '')}")
            
            # 洗浄指示データをDataFrameに変換
            instruction_rows = []
            for instruction in cleaning_instructions:
                machine = instruction.get("号機", "").strip()
                product_number = instruction.get("品番", "").strip()
                
                # 号機と品番が両方存在する場合のみ追加
                if machine and product_number:
                    process_number = ""
                    process_name = ""
                    if process_master_df is not None:
                        inferred_number, inferred_name = _infer_process_info(
                            product_number,
                            process_master_df,
                            normalized_keywords,
                            log
                        )
                        process_number = inferred_number or ""
                        process_name = inferred_name or ""
                    row_data = {
                        "品番": product_number,
                        "品名": instruction.get("品名", "").strip(),
                        "客先": instruction.get("客先名", "").strip(),
                        "指示日": instruction.get("指示日", "").strip(),
                        "号機": machine,
                        "数量": instruction.get("数量", "").strip(),
                        "ロット数量": instruction.get("数量", "").strip(),  # 数量をロット数量にも設定
                        "生産ロットID": "",  # 未記載
                        "現在工程名": process_name,
                        "現在工程番号": process_number,
                        "現在工程二次処理": "",  # 未記載
                    }
                    instruction_rows.append(row_data)
            
            if instruction_rows:
                instruction_lots_df = pd.DataFrame(instruction_rows)
                log(f"洗浄指示から {len(instruction_lots_df)}件のロットを取得しました（Googleスプレッドシートから直接取得）")
                all_lots.append(instruction_lots_df)
        
        if all_lots:
            log(f"all_lotsに含まれるDataFrame数: {len(all_lots)}")
            for idx, df in enumerate(all_lots):
                log(f"  DataFrame {idx+1}: {len(df)}件（生産ロットIDあり: {(df['生産ロットID'].notna() & (df['生産ロットID'] != '')).sum() if '生産ロットID' in df.columns else 0}件）")
            
            final_lots_df = pd.concat(all_lots, ignore_index=True)
            log(f"結合後のロット数: {len(final_lots_df)}件")
            
            # 重複を除去
            # 生産ロットIDが存在し、空でない場合は生産ロットIDで重複除去
            # それ以外（洗浄指示から取得したデータなど）は品番・号機・指示日の組み合わせで重複除去
            # ただし、通常の在庫ロット（生産ロットIDあり）と洗浄指示ロット（生産ロットIDなし）が重複する場合は、通常の在庫ロットを優先
            if '生産ロットID' in final_lots_df.columns:
                before_count = len(final_lots_df)
                
                # 生産ロットIDが空でない行と空の行を分離
                has_lot_id_mask = final_lots_df['生産ロットID'].notna() & (final_lots_df['生産ロットID'] != '')
                has_lot_id_df = final_lots_df[has_lot_id_mask].copy()
                no_lot_id_df = final_lots_df[~has_lot_id_mask].copy()
                
                log(f"重複除去前: 生産ロットIDあり={len(has_lot_id_df)}件, 生産ロットIDなし={len(no_lot_id_df)}件")
                
                # 生産ロットIDが空でない行は生産ロットIDで重複除去
                if not has_lot_id_df.empty:
                    has_lot_id_df = has_lot_id_df.drop_duplicates(subset=['生産ロットID'], keep='first')
                
                # 生産ロットIDが空の行は品番・号機・指示日の組み合わせで重複除去
                if not no_lot_id_df.empty:
                    subset_cols = ['品番', '号機']
                    if '指示日' in no_lot_id_df.columns:
                        subset_cols.append('指示日')
                    no_lot_id_df = no_lot_id_df.drop_duplicates(subset=subset_cols, keep='first')
                    
                    # 通常の在庫ロット（生産ロットIDあり）と重複する洗浄指示ロットを削除
                    # 品番・号機・指示日の組み合わせで重複チェック
                    if not has_lot_id_df.empty:
                        # 通常の在庫ロットの品番・号機・指示日の組み合わせを取得
                        check_cols = ['品番', '号機']
                        if '指示日' in has_lot_id_df.columns and '指示日' in no_lot_id_df.columns:
                            check_cols.append('指示日')
                        
                        # 生産ロットIDあり側の重複判定は itertuples() で高速に走査
                        def _norm_value(val):
                            if pd.isna(val):
                                return None
                            s = str(val).strip()
                            return s or None
                        has_lot_id_keys = []
                        # 速度向上のため、必要列のインデックスを先に取得
                        col_indices = [has_lot_id_df.columns.get_loc(col) for col in check_cols]
                        for row_tuple in has_lot_id_df.itertuples(index=False):
                            key = tuple(_norm_value(row_tuple[i]) for i in col_indices)
                            has_lot_id_keys.append(key)

                        # 生産ロットIDなし側も同様に itertuples() でチェック
                        if has_lot_id_keys:
                            before_no_lot_id_count = len(no_lot_id_df)
                            # 速度向上のため、こちらも列インデックスを先に計算
                            no_lot_id_col_indices = [no_lot_id_df.columns.get_loc(col) for col in check_cols]

                            def _is_duplicate(row_tuple):
                                target_key = tuple(_norm_value(row_tuple[i]) for i in no_lot_id_col_indices)
                                t_prod, t_machine, *rest = target_key
                                t_instr = rest[0] if rest else None
                                for h_key in has_lot_id_keys:
                                    h_prod, h_machine, *h_rest = h_key
                                    h_instr = h_rest[0] if h_rest else None
                                    if t_prod != h_prod:
                                        continue
                                    if h_machine is not None and t_machine not in (None, h_machine):
                                        continue
                                    if h_instr is not None and t_instr not in (None, h_instr):
                                        continue
                                    return True
                                return False

                            mask = []
                            for row_tuple in no_lot_id_df.itertuples(index=False):
                                mask.append(not _is_duplicate(row_tuple))
                            no_lot_id_df = no_lot_id_df[mask].copy()
                            excluded_count = before_no_lot_id_count - len(no_lot_id_df)
                            if excluded_count > 0:
                                log(f"通常の在庫ロットと重複する洗浄指示ロットを {excluded_count}件 除外しました")
                
                # 結合
                if not has_lot_id_df.empty and not no_lot_id_df.empty:
                    final_lots_df = pd.concat([has_lot_id_df, no_lot_id_df], ignore_index=True)
                    log(f"結合後: 生産ロットIDあり={len(has_lot_id_df)}件 + 生産ロットIDなし={len(no_lot_id_df)}件 = 合計{len(final_lots_df)}件")
                elif not has_lot_id_df.empty:
                    final_lots_df = has_lot_id_df
                    log(f"結合後: 生産ロットIDありのみ={len(final_lots_df)}件")
                elif not no_lot_id_df.empty:
                    final_lots_df = no_lot_id_df
                    log(f"結合後: 生産ロットIDなしのみ={len(final_lots_df)}件")
                else:
                    log("警告: 結合後のDataFrameが空です")
                    final_lots_df = pd.DataFrame()
                
                if before_count != len(final_lots_df):
                    log(f"重複を除去: {before_count}件 → {len(final_lots_df)}件")
                else:
                    log(f"重複なし: {before_count}件")
            
            # 出荷予定日列を確実に設定（既存の値があっても上書き）
            # Googleスプレッドシートから取得したロットは全て"当日洗浄上がり品"とする
            final_lots_df['出荷予定日'] = "当日洗浄上がり品"
            final_lots_df = _ensure_process_info_for_lots(
                final_lots_df,
                process_master_df,
                normalized_keywords,
                log
            )
            
            log(f"洗浄二次処理依頼から {len(final_lots_df)}件のロットを取得しました（重複除去後）")
            log(f"出荷予定日: 全て「当日洗浄上がり品」に設定しました")
            return final_lots_df
        else:
            log("洗浄二次処理依頼からロットが見つかりませんでした")
            return pd.DataFrame()
            
    except Exception as e:
        error_msg = f"洗浄二次処理依頼からのロット取得中にエラーが発生しました: {str(e)}"
        log(error_msg)
        logger.error(error_msg)
        return pd.DataFrame()
