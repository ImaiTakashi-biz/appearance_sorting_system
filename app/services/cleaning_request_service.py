"""
洗浄二次処理依頼からロットを取得するサービス
"""

import os
import re
import pyodbc
import pandas as pd
import concurrent.futures
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
from loguru import logger

from app.export.google_sheets_exporter_service import GoogleSheetsExporter
from app.utils.perf import perf_timer

# ログ分類（app_.logの視認性向上）
logger = logger.bind(channel="SVC:CLEAN")

# 正規表現パターンの事前コンパイル（高速化）
_MACHINE_PATTERN = re.compile(r'([A-Z]-\d+)')
_DATE_PATTERN = re.compile(r'(\d{1,2}/\d{1,2})')
_LOT_PATTERN = re.compile(r'(\d+)\s*ロット')
_YMD_PATTERN = re.compile(r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})')

# テーブル構造のキャッシュ（高速化のため）
_table_structure_cache = None


def _normalize_instruction_date(value: object) -> Optional[str]:
    """
    Googleスプレッドシート等から取得した指示日文字列を YYYY-MM-DD に正規化する。
    例: "2025/12/15（完）" -> "2025-12-15"
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    raw = str(value).strip()
    if not raw:
        return None

    # 「（完）」「(完了)」などの注記を除去（全角/半角どちらも）
    raw = re.sub(r"[（(].*?[）)]", "", raw).strip()

    # 年月日を抽出して正規化
    match = _YMD_PATTERN.search(raw)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    # ここまでで拾えない場合はパーサに委譲（失敗時はNone）
    parsed = pd.to_datetime(raw, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).strftime("%Y-%m-%d")


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
        
        # pyodbcカーソルで直接取得（日付パースエラーを回避）
        try:
            cursor = connection.cursor()
            cursor.execute(columns_query)
            column_names = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            
            if row is None:
                logger.warning("t_現品票履歴テーブルにデータが見つかりません")
                _table_structure_cache = {"columns": [], "available": []}
                return _table_structure_cache
            
            # 列名のみを使用（データは不要）
            actual_columns = column_names
        except Exception as e:
            # フォールバック: pd.read_sqlを使用（エラーが発生した場合）
            logger.warning(f"カーソルでの取得に失敗しました。pd.read_sqlを使用します: {str(e)}")
            sample_df = pd.read_sql(columns_query, connection, parse_dates=[])
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
        
        # すべての列を通常通り取得（pyodbcカーソルで直接取得するため、CStrは不要）
        columns_str = ", ".join([f"[{col}]" for col in available_columns])
        
        # AccessはOR条件が多いと遅くなりやすいため、可能なら
        # 「指示日範囲 × 号機ごと」の包絡条件にして取得（後段のpandas側で厳密に絞り込み）
        all_conditions = []

        # 号機ごとに必要な指示日集合を作り、可能なら IN(...) でピンポイントに絞る
        # （範囲指定だと不要日付まで拾ってしまい、Access側の実行が重くなりやすい）
        machine_dates: Dict[str, set] = {}
        no_machine_reqs: List[Dict] = []
        all_dates_set: set[str] = set()

        for req in requests:
            machine = str(req.get("machine", "")).strip()
            date_candidates: List[str] = []

            if req.get("instruction_date"):
                normalized = _normalize_instruction_date(req["instruction_date"])
                if normalized:
                    date_candidates.append(normalized)

            if req.get("date_list"):
                date_list = [d for d in (req.get("date_list") or []) if isinstance(d, str) and d.strip()]
                date_candidates.extend(date_list)

            if not date_candidates:
                continue

            # YYYY-MM-DD に揃えて保持
            normalized_dates = []
            for d in date_candidates:
                nd = _normalize_instruction_date(d)
                if nd:
                    normalized_dates.append(nd)
            if not normalized_dates:
                continue

            all_dates_set.update(normalized_dates)

            if not machine:
                no_machine_reqs.append(req)
                continue

            if machine not in machine_dates:
                machine_dates[machine] = set()
            machine_dates[machine].update(normalized_dates)

        use_envelope = bool(machine_dates) and ("号機" in available_columns)
        if use_envelope:
            per_machine_conditions: List[str] = []
            for machine, dates in sorted(machine_dates.items(), key=lambda x: x[0]):
                dates_sorted = sorted({d for d in dates if isinstance(d, str) and d})
                if not dates_sorted:
                    continue

                escaped_machine = machine.replace("'", "''")

                # Accessは巨大なINリストが遅くなることがあるため、
                # 日付が少ない場合のみINを使い、それ以外は範囲にフォールバックする。
                if len(dates_sorted) <= 7:
                    date_literals = ", ".join([f"#{d}#" for d in dates_sorted])
                    per_machine_conditions.append(f"([号機] = '{escaped_machine}' AND [指示日] IN ({date_literals}))")
                else:
                    start_str = pd.to_datetime(dates_sorted[0]).strftime('#%Y-%m-%d#')
                    end_str = pd.to_datetime(dates_sorted[-1]).strftime('#%Y-%m-%d#')
                    per_machine_conditions.append(f"([号機] = '{escaped_machine}' AND [指示日] >= {start_str} AND [指示日] <= {end_str})")

            # machine無しのものは従来条件で追加（まれ）
            extra_conditions: List[str] = []
            for req in no_machine_reqs:
                conditions = []
                if req.get("instruction_date"):
                    normalized = _normalize_instruction_date(req["instruction_date"])
                    if normalized:
                        try:
                            date_obj = datetime.strptime(normalized, "%Y-%m-%d")
                            date_str = date_obj.strftime('#%Y-%m-%d#')
                            conditions.append(f"[指示日] = {date_str}")
                        except Exception:
                            pass
                if req.get("date_list"):
                    date_list = req.get("date_list") or []
                    if date_list:
                        sorted_dates_str = sorted(date_list)
                        start_date = pd.to_datetime(sorted_dates_str[0])
                        end_date = pd.to_datetime(sorted_dates_str[-1])
                        start_str = start_date.strftime('#%Y-%m-%d#')
                        end_str = end_date.strftime('#%Y-%m-%d#')
                        conditions.append(f"[指示日] >= {start_str} AND [指示日] <= {end_str}")
                if conditions:
                    extra_conditions.append(f"({' AND '.join(conditions)})")

            combined = per_machine_conditions + extra_conditions
            all_conditions = [f"({' OR '.join(combined)})"] if combined else []

        if not all_conditions:
            # 従来方式（OR条件）
            for req in requests:
                conditions = []

                if req.get("instruction_date"):
                    normalized = _normalize_instruction_date(req["instruction_date"])
                    if normalized:
                        try:
                            date_obj = datetime.strptime(normalized, "%Y-%m-%d")
                            date_str = date_obj.strftime('#%Y-%m-%d#')
                            conditions.append(f"[指示日] = {date_str}")
                        except Exception:
                            logger.warning(f"指示日の正規化に失敗したためスキップします: {req['instruction_date']}")

                if req.get("date_list"):
                    date_list = req["date_list"]
                    if len(date_list) > 0:
                        sorted_dates_str = sorted(date_list)
                        start_date = pd.to_datetime(sorted_dates_str[0])
                        end_date = pd.to_datetime(sorted_dates_str[-1])
                        start_str = start_date.strftime('#%Y-%m-%d#')
                        end_str = end_date.strftime('#%Y-%m-%d#')
                        conditions.append(f"[指示日] >= {start_str} AND [指示日] <= {end_str}")

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

        # 全体の指示日範囲で追加絞り込み（結果は不変・Access側の検索範囲を狭める）
        if all_dates_set:
            try:
                dates_sorted = sorted(all_dates_set)
                where_clause = f"({where_clause}) AND [指示日] >= #{dates_sorted[0]}# AND [指示日] <= #{dates_sorted[-1]}#"
            except Exception:
                pass
        
        # バッチクエリを実行
        # CStr([指示日]) はAccess側でインデックスを使いにくくなる場合があるため、
        # 可能な限り生のDate型で取得し、Python側で文字列化して扱う（結果は同一）。
        query = f"""
        SELECT {columns_str}
        FROM [t_現品票履歴]
        WHERE {where_clause}
        ORDER BY [指示日], [号機]
        """
        
        try:
            from time import perf_counter
            import time
            start_time = time.time()
            
            # pyodbcのカーソルを直接使用してデータを取得（日付パースエラーを回避）
            cursor = connection.cursor()

            t0 = perf_counter()
            cursor.execute(query)
            execute_total_ms = (perf_counter() - t0) * 1000.0
            logger.bind(channel="PERF").debug("PERF {}: {:.1f} ms", "cleaning.access.batch.cursor_execute", execute_total_ms)

            # 列名を取得
            column_names = [desc[0] for desc in cursor.description]

            # データを取得（すべて文字列として取得）
            t1 = perf_counter()
            rows = cursor.fetchall()
            fetch_total_ms = (perf_counter() - t1) * 1000.0
            logger.bind(channel="PERF").debug("PERF {}: {:.1f} ms", "cleaning.access.batch.fetchall", fetch_total_ms)
            
            # DataFrameに変換
            if rows:
                # 各行を辞書に変換（すべて文字列として扱う）
                data = []
                for row in rows:
                    row_dict = {}
                    for idx, col_name in enumerate(column_names):
                        value = row[idx]
                        if value is None:
                            row_dict[col_name] = None
                        elif isinstance(value, (datetime, pd.Timestamp)):
                            row_dict[col_name] = str(value)
                        elif isinstance(value, str):
                            row_dict[col_name] = value
                        else:
                            row_dict[col_name] = str(value)
                    data.append(row_dict)
                
                # DataFrameを作成（日付の自動パースを無効化）
                lots_df = pd.DataFrame(data, dtype=object)
            else:
                lots_df = pd.DataFrame(columns=column_names)
            
            elapsed_time = time.time() - start_time
            logger.info(f"バッチクエリ完了: {len(lots_df)}件のロットを取得 ({elapsed_time:.2f}秒)")
            
            # 指示日列が存在する場合、日付文字列から不要な文字を除去してからパース
            if '指示日' in lots_df.columns:
                def clean_date_string(date_val):
                    """日付文字列から不要な文字（「（完）」など）を除去"""
                    if pd.isna(date_val) or date_val is None or date_val == '':
                        return None
                    date_str = str(date_val)
                    # 「（完）」「（完了）」などの文字を除去
                    date_str = re.sub(r'[（(].*?[）)]', '', date_str)
                    # 前後の空白を除去
                    date_str = date_str.strip()
                    return date_str if date_str else None
                
                # 日付文字列をクリーニングしてパース
                lots_df['指示日'] = lots_df['指示日'].apply(clean_date_string)
                lots_df['指示日'] = pd.to_datetime(lots_df['指示日'], errors='coerce')
            
            # 現在工程名列を空欄として追加（ロット情報に含めないため）
            if '現在工程名' not in lots_df.columns:
                lots_df['現在工程名'] = ''

            return lots_df
        except Exception as query_error:
            logger.error(f"バッチクエリ実行中にエラーが発生しました: {str(query_error)}")
            try:
                logger.error(f"クエリ: {query[:500]}...")  # クエリの最初の500文字をログに出力
            except Exception:
                pass
            import traceback
            logger.error(f"トレースバック: {traceback.format_exc()}")
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


def _get_today_requests_from_sheets(exporter: GoogleSheetsExporter, sheet_name: str = "依頼一覧", log_callback: Optional[callable] = None) -> List[List]:
    """
    指定されたシートから今日の日付の行のA列からK列のデータを取得
    
    Args:
        exporter: GoogleSheetsExporterインスタンス
        sheet_name: シート名（デフォルト: "依頼一覧")
        log_callback: ログ出力用のコールバック関数（オプション）
    
    Returns:
        list: 今日の日付の行のデータリスト
    """
    def log(msg):
        if log_callback:
            log_callback(msg)
        else:
            logger.info(msg)
    
    try:
        log(f"スプレッドシート「{sheet_name}」からデータを取得中...")
        spreadsheet = exporter._get_spreadsheet()
        if not spreadsheet:
            log("スプレッドシートの取得に失敗しました")
            return []
        
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            log(f"シート「{sheet_name}」を取得しました")
        except Exception as e:
            log(f"シート「{sheet_name}」の取得に失敗しました: {str(e)}")
            return []
        
        all_values = worksheet.get('A2:K')
        
        if not all_values:
            log(f"シート「{sheet_name}」にデータがありません")
            return []
        
        log(f"シート「{sheet_name}」から {len(all_values)}行のデータを取得しました")
        
        today_dt = datetime.now()
        today = today_dt.strftime('%Y/%m/%d')
        today_rows = []
        parse_failed_count = 0

        for row_idx, row in enumerate(all_values, start=2):
            if not row or not row[0]:
                continue
            
            date_val = row[0]
            if isinstance(date_val, str):
                date_str = date_val.strip()
            else:
                date_str = str(date_val).strip()
            
            if not date_str:
                continue
            
            # 日付の比較（複数の形式に対応）
            matched = False
            
            # 1. 完全一致（%Y/%m/%d形式）
            if date_str == today:
                matched = True
            else:
                # 2. よくある形式はdatetimeで高速に判定し、失敗時のみpandasに委譲
                parsed_dt = None
                for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                    try:
                        parsed_dt = datetime.strptime(date_str, fmt)
                        break
                    except Exception:
                        continue

                if parsed_dt is not None:
                    if parsed_dt.date() == today_dt.date():
                        matched = True
                else:
                    try:
                        parsed_date = pd.to_datetime(date_str, errors='coerce')
                    except Exception:
                        parsed_date = pd.NaT

                    if pd.notna(parsed_date) and isinstance(parsed_date, pd.Timestamp):
                        if parsed_date.date() == today_dt.date():
                            matched = True
                    elif pd.isna(parsed_date):
                        parse_failed_count += 1
            
            if matched:
                row_data = row[:11] if len(row) >= 11 else row + [''] * (11 - len(row))
                today_rows.append(row_data)

        log(f"今日の日付と一致する行: {len(today_rows)}件")
        if parse_failed_count:
            logger.debug(f"日付パース失敗: {parse_failed_count}件（先頭列）")
        
        return today_rows
        
    except Exception as e:
        error_msg = f"シートからのデータ取得中にエラーが発生しました: {str(e)}"
        log(error_msg)
        logger.error(error_msg, exc_info=True)
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

    if process_master_path:
        with perf_timer(logger, "cleaning.process_master.load"):
            process_master_df = _load_process_master(process_master_path, log)
    else:
        process_master_df = None
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
        
        # データ取得（独立処理なので並列化して待ち時間を短縮）
        def _fetch_today_requests():
            log("洗浄二次処理依頼からデータを取得中...")
            with perf_timer(logger, "cleaning.sheets.today_requests"):
                return _get_today_requests_from_sheets(exporter_cleaning, "依頼一覧", log_callback=log)

        def _fetch_instructions():
            log("洗浄指示からデータを取得中...")
            with perf_timer(logger, "cleaning.sheets.instructions"):
                return _get_cleaning_instructions_from_sheets(exporter_instructions, sheet_name_today)

        with perf_timer(logger, "cleaning.sheets.parallel_total"):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                future_today = executor.submit(_fetch_today_requests)
                future_instructions = executor.submit(_fetch_instructions)
                today_data = future_today.result()
                cleaning_instructions = future_instructions.result()
        
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
                normalized = _normalize_instruction_date(instruction_date)
                if normalized:
                    request["instruction_date"] = normalized
                    request["machine"] = machine
            
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
            with perf_timer(logger, "cleaning.access.batch_lots"):
                batch_lots_df = _get_lots_from_access_batch(connection, batch_requests)
            log(f"バッチ処理で取得したロット数: {len(batch_lots_df)}件")
            
            if not batch_lots_df.empty:
                # 指示日列を正規化（YYYY-MM-DD形式に統一）
                if "指示日" in batch_lots_df.columns:
                    batch_lots_df['_指示日_normalized'] = batch_lots_df['指示日'].apply(_normalize_instruction_date)
                
                # 各リクエストに対応するロットを分離
                filtered_count = 0
                for row_info in row_info_list:
                    req = row_info["request"]
                    filtered_df = batch_lots_df
                    
                    mask = pd.Series([True] * len(filtered_df), index=filtered_df.index)
                    
                    if req.get("instruction_date"):
                        normalized_req_date = _normalize_instruction_date(req["instruction_date"])
                        if normalized_req_date:
                            mask = mask & (filtered_df['_指示日_normalized'] == normalized_req_date)
                    
                    if req.get("machine") and "号機" in filtered_df.columns:
                        mask = mask & (filtered_df['号機'] == req["machine"])
                    
                    if req.get("date_list"):
                        normalized_date_list = [_normalize_instruction_date(d) for d in req["date_list"]]
                        normalized_date_set = {d for d in normalized_date_list if d}
                        if normalized_date_set:
                            mask = mask & filtered_df['_指示日_normalized'].isin(normalized_date_set)
                    
                    filtered_df = filtered_df[mask].copy()
                    filtered_df = filtered_df.drop(columns=['_指示日_normalized'], errors='ignore')
                    
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

                has_lot_id_mask = final_lots_df['生産ロットID'].notna() & (final_lots_df['生産ロットID'] != '')
                has_lot_id_df = final_lots_df[has_lot_id_mask].copy()
                no_lot_id_df = final_lots_df[~has_lot_id_mask].copy()

                log(f"重複除去前: 生産ロットIDあり={len(has_lot_id_df)}件, 生産ロットIDなし={len(no_lot_id_df)}件")

                if not has_lot_id_df.empty:
                    has_lot_id_df = has_lot_id_df.drop_duplicates(subset=['生産ロットID'], keep='first')

                if not no_lot_id_df.empty:
                    subset_cols = ['品番', '号機']
                    if '指示日' in no_lot_id_df.columns:
                        subset_cols.append('指示日')
                    no_lot_id_df = no_lot_id_df.drop_duplicates(subset=subset_cols, keep='first')

                    if not has_lot_id_df.empty:
                        check_cols = ['品番', '号機']
                        if '指示日' in has_lot_id_df.columns and '指示日' in no_lot_id_df.columns:
                            check_cols.append('指示日')

                        def _normalize_value(val):
                            if pd.isna(val):
                                return None
                            s = str(val).strip()
                            return s or None

                        has_key_indices = [has_lot_id_df.columns.get_loc(col) for col in check_cols]
                        no_key_indices = [no_lot_id_df.columns.get_loc(col) for col in check_cols]

                        normalized_has_keys = {
                            tuple(_normalize_value(row_tuple[i]) for i in has_key_indices)
                            for row_tuple in has_lot_id_df.itertuples(index=False)
                        }

                        if normalized_has_keys:
                            before_no_lot_id_count = len(no_lot_id_df)
                            mask = []
                            for row_tuple in no_lot_id_df.itertuples(index=False):
                                key = tuple(_normalize_value(row_tuple[i]) for i in no_key_indices)
                                mask.append(key not in normalized_has_keys)

                            no_lot_id_df = no_lot_id_df[mask].copy()
                            excluded_count = before_no_lot_id_count - len(no_lot_id_df)
                            if excluded_count > 0:
                                log(f"通常の在庫ロットと重複する洗浄指示ロットを {excluded_count}件 除外しました")

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
            final_lots_df['__from_cleaning_sheet'] = True
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
