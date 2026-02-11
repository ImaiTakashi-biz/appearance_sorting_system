"""
洗浄二次処理依頼からロットを取得するサービス
"""

import os
import re
import unicodedata
import time
import copy
import pyodbc
import pandas as pd
import concurrent.futures
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
from loguru import logger

from app.export.google_sheets_exporter_service import GoogleSheetsExporter
from app.utils.perf import perf_timer
from time import perf_counter

# ログ分類（app_.logの視認性向上）
logger = logger.bind(channel="SVC:CLEAN")

# 正規表現パターンの事前コンパイル（高速化）
_MACHINE_PATTERN = re.compile(r'([A-Z]-\d+)')
_DATE_PATTERN = re.compile(r'(\d{1,2}/\d{1,2})')
_LOT_PATTERN = re.compile(r'(\d+)\s*(?:ロット|ﾛｯﾄ|LOT|lot)')
_YMD_PATTERN = re.compile(r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})')
_REMARKS_SEGMENT_SPLIT_PATTERN = re.compile(r"[、､,，;\n]+")

# テーブル構造のキャッシュ（高速化のため）
_table_structure_cache = None
_process_infer_logged: set = set()
_CLEANING_SHEETS_CACHE: dict[tuple[str, str, str], tuple[float, object]] = {}


def _get_cleaning_sheets_cache_ttl_seconds() -> float:
    """
    洗浄関連のGoogle Sheets取得結果をメモリで再利用するTTL（秒）。
    デフォルトは0（キャッシュ無効）で、結果不変の運用を維持する。
    """
    raw = str(os.getenv("CLEANING_SHEETS_CACHE_TTL_SECONDS", "0")).strip()
    try:
        return max(0.0, float(raw))
    except Exception:
        return 0.0


def _cleaning_cache_get(key: tuple[str, str, str]) -> object | None:
    ttl = _get_cleaning_sheets_cache_ttl_seconds()
    if ttl <= 0:
        return None
    item = _CLEANING_SHEETS_CACHE.get(key)
    if not item:
        return None
    ts, payload = item
    if (time.monotonic() - ts) > ttl:
        _CLEANING_SHEETS_CACHE.pop(key, None)
        return None
    return copy.deepcopy(payload)


def _cleaning_cache_set(key: tuple[str, str, str], payload: object) -> None:
    ttl = _get_cleaning_sheets_cache_ttl_seconds()
    if ttl <= 0:
        return
    _CLEANING_SHEETS_CACHE[key] = (time.monotonic(), copy.deepcopy(payload))


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

    # 年が無い MM/DD 形式は当年として扱う（年跨ぎの誤判定を防ぐ）
    md_match = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})", raw)
    if md_match:
        month = int(md_match.group(1))
        day = int(md_match.group(2))
        today = datetime.now().date()
        year = today.year
        if month == 12 and today.month < 12:
            year -= 1
        return f"{year}-{month:02d}-{day:02d}"

    # ここまでで拾えない場合はパーサに委譲（失敗時はNone）
    parsed = pd.to_datetime(raw, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).strftime("%Y-%m-%d")


def _normalize_key_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return unicodedata.normalize("NFKC", text)


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


def _parse_remarks_multi(remarks: str) -> List[Dict[str, str]]:
    """
    詳細・備考から複数の「号機 + 開始日付 + ロット数」指定を抽出する。
    例: "E-11 12/12～ 5ロット、F-12 12/13～ 5ロット" -> 2件
    """
    if not remarks or not isinstance(remarks, str):
        return []

    # まず区切りで分割して、それぞれを既存ロジックで解析する（挙動を崩しにくい）
    segments = [seg.strip() for seg in _REMARKS_SEGMENT_SPLIT_PATTERN.split(remarks) if seg and seg.strip()]
    if not segments:
        return []

    parsed_list: List[Dict[str, str]] = []
    for seg in segments:
        parsed = _parse_remarks(seg)
        if parsed:
            parsed_list.append(parsed)
    return parsed_list


def _generate_date_range(start_date_str: str, days: int) -> List[str]:
    """
    開始日付から指定日数分の日付リストを生成
    年をまたぐ場合、指定ロット数に達しない場合は翌年の日付も含める
    
    Args:
        start_date_str: 開始日付（MM/DD形式、例: "10/26"）
        days: 日数（例: 9）
    
    Returns:
        list: 日付文字列のリスト（YYYY-MM-DD形式）
    """
    try:
        # 現在の年月を取得
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        
        # MM/DD形式を日付オブジェクトに変換
        month, day = map(int, start_date_str.split('/'))
        
        # 年の決定ロジック：
        # 12月の日付が現在の月（1月など）より前の場合は、前年として解釈
        # 例: 現在が2026年1月で、12/20が指定された場合 → 2025年12月20日
        if month == 12 and current_month <= 2:
            # 年初（1-2月）に12月指定が来た場合のみ前年として扱う
            year = current_year - 1
        else:
            year = current_year
        
        start_date = datetime(year, month, day)
        
        # 日付リストを生成（リスト内包表記で高速化）
        date_list = []
        for i in range(days):
            date = start_date + timedelta(days=i)
            date_list.append(date.strftime('%Y-%m-%d'))
        
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


def _get_lots_from_access_instruction_date(connection, requests: List[Dict]) -> pd.DataFrame:
    """
    instruction_dateのみのリクエストを個別の日付で直接取得
    材料識別が5のレコードのみを対象とする
    
    Args:
        connection: Accessデータベース接続
        requests: リクエストのリスト [{"instruction_date": "...", "machine": "...", "product_number": "..."}, ...]
    
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
        
        # すべての列を通常通り取得
        columns_str = ", ".join([f"[{col}]" for col in available_columns])
        
        # 各リクエストの条件を構築（個別の日付で直接取得）
        conditions = []
        for req in requests:
            machine = str(req.get("machine", "")).strip()
            instruction_date = req.get("instruction_date")
            
            if not instruction_date or not machine:
                continue
            
            normalized_date = _normalize_instruction_date(instruction_date)
            if not normalized_date:
                continue
            
            # 個別の日付で直接取得
            escaped_machine = machine.replace("'", "''")
            date_str = f"#{normalized_date}#"
            condition = f"([号機] = '{escaped_machine}' AND [指示日] = {date_str})"
            
            # 品番が指定されている場合は追加
            if req.get("product_number") and "品番" in available_columns:
                product_number = str(req.get("product_number", "")).strip()
                if product_number:
                    escaped_product = product_number.replace("'", "''")
                    condition = f"({condition} AND [品番] = '{escaped_product}')"
            
            conditions.append(condition)
        
        if not conditions:
            return pd.DataFrame()
        
        # OR条件で結合
        where_clause = " OR ".join(conditions)
        
        # 材料識別でフィルタリング（5のみを対象）
        if "材料識別" in available_columns:
            where_clause = f"({where_clause}) AND [材料識別] = 5"
        
        query = f"""
        SELECT {columns_str}
        FROM [t_現品票履歴]
        WHERE {where_clause}
        """
        
        start_time = time.time()
        cursor = connection.cursor()
        
        t0 = perf_counter()
        cursor.execute(query)
        execute_total_ms = (perf_counter() - t0) * 1000.0
        logger.bind(channel="PERF").debug("PERF {}: {:.1f} ms", "cleaning.access.instruction_date.cursor_execute", execute_total_ms)
        
        # 列名を取得
        column_names = [desc[0] for desc in cursor.description]
        
        # データを取得（すべて文字列として取得）
        t1 = perf_counter()
        rows = cursor.fetchall()
        fetch_total_ms = (perf_counter() - t1) * 1000.0
        logger.bind(channel="PERF").debug("PERF {}: {:.1f} ms", "cleaning.access.instruction_date.fetchall", fetch_total_ms)
        
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
        logger.info(f"instruction_date指定のクエリ完了: {len(lots_df)}件のロットを取得 ({elapsed_time:.2f}秒)")
        
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
            
            # 日付文字列をクリーニング
            lots_df['指示日'] = lots_df['指示日'].apply(clean_date_string)
            
            # パース処理: 明示的なフォーマット指定で変換を試みる
            def parse_date_safe(date_str):
                """日付文字列を安全にパースする"""
                if date_str is None or pd.isna(date_str):
                    return pd.NaT
                try:
                    # まず明示的なフォーマットで試す
                    from datetime import datetime
                    # YYYY-MM-DD HH:MM:SS 形式
                    if isinstance(date_str, str) and len(date_str) >= 10:
                        # 日付部分のみを抽出（YYYY-MM-DD）
                        date_part = date_str[:10]
                        return pd.to_datetime(date_part, format='%Y-%m-%d', errors='coerce')
                    # フォールバック: 通常のパース
                    return pd.to_datetime(date_str, errors='coerce')
                except Exception:
                    return pd.NaT
            
            lots_df['指示日'] = lots_df['指示日'].apply(parse_date_safe)
        
        # 現在工程名列を空欄として追加（ロット情報に含めないため）
        if '現在工程名' not in lots_df.columns:
            lots_df['現在工程名'] = ''
        
        return lots_df
    except Exception as query_error:
        logger.error(f"instruction_date指定のクエリ実行中にエラーが発生しました: {str(query_error)}")
        try:
            logger.error(f"クエリ: {query[:500]}...")  # クエリの最初の500文字をログに出力
        except Exception:
            pass
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()


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
            
            # デバッグ: リクエストごとの指示日をログ出力（品番指定時）
            if req.get("product_number") == "1J841R3-4":
                logger.debug(f"リクエスト処理: 品番={req.get('product_number')}, 号機={machine}, 指示日候補={date_candidates}, 正規化後={normalized_dates}")

            if not machine:
                no_machine_reqs.append(req)
                continue

            if machine not in machine_dates:
                machine_dates[machine] = set()
            machine_dates[machine].update(normalized_dates)

        # Accessは巨大なOR条件で極端に遅くなることがあるため、
        # 号機IN + 指示日範囲の「包絡条件」で広めに取得して、後段（呼び出し側）で厳密に絞る。
        # ※呼び出し側で _指示日_normalized / 号機 で必ず絞り込んでいるため、結果は不変。
        # instruction_dateが指定されている場合でも、範囲指定で取得し、後段のフィルタリングで厳密に絞り込む
        use_simple_range_query = (
            bool(machine_dates)
            and not no_machine_reqs
            and ("号機" in available_columns)
            and ("指示日" in available_columns)
            and bool(all_dates_set)
        )

        if use_simple_range_query:
            machines_sorted = sorted({str(m).strip() for m in machine_dates.keys() if str(m).strip()})
            dates_sorted = sorted(all_dates_set)
            if not machines_sorted or not dates_sorted:
                use_simple_range_query = False

        if use_simple_range_query:
            escaped_machines: List[str] = []
            for machine in machines_sorted:
                escaped = str(machine).replace("'", "''")
                escaped_machines.append(f"'{escaped}'")
            machine_literals = ", ".join(escaped_machines)
            # Accessの日付比較では、<= #2026-01-05# は 2026-01-05 00:00:00 までしか含まないため、
            # 2026-01-05 のロットが取得されない可能性がある。
            # そのため、終了日を1日後にして < #2026-01-06# とする
            from datetime import datetime, timedelta
            try:
                end_date = datetime.strptime(dates_sorted[-1], "%Y-%m-%d")
                end_date_next = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")
                where_clause = f"[号機] IN ({machine_literals}) AND [指示日] >= #{dates_sorted[0]}# AND [指示日] < #{end_date_next}#"
            except Exception:
                # 日付パースに失敗した場合は従来通り
                where_clause = f"[号機] IN ({machine_literals}) AND [指示日] >= #{dates_sorted[0]}# AND [指示日] <= #{dates_sorted[-1]}#"
            # デバッグ: バッチクエリの日付範囲をログ出力（品番指定時）
            if any(req.get("product_number") == "1J841R3-4" for req in requests):
                logger.info(f"バッチクエリ日付範囲: {dates_sorted[0]} ～ {dates_sorted[-1]} (全指示日: {sorted(all_dates_set)})")

            # 材料識別でフィルタリング（5のみを対象）
            if "材料識別" in available_columns:
                where_clause = f"({where_clause}) AND [材料識別] = 5"

            query = f"""
            SELECT {columns_str}
            FROM [t_現品票履歴]
            WHERE {where_clause}
            """
        else:
            use_envelope = bool(machine_dates) and ("号機" in available_columns)

            if use_envelope:
                per_machine_conditions: List[str] = []
                for machine, dates in sorted(machine_dates.items(), key=lambda x: x[0]):
                    dates_sorted = sorted({d for d in dates if isinstance(d, str) and d})
                    if not dates_sorted:
                        continue

                escaped_machine = machine.replace("'", "''")

                # 範囲指定（>= / <=）は余計な日付もヒットして取得行数が増えやすく、
                # かえって遅くなるケースがあるため、INで「必要な日付だけ」に限定する。
                # ただしINが巨大になりすぎないよう、分割して OR で連結する。
                chunk_size = 14
                in_clauses: List[str] = []
                for i in range(0, len(dates_sorted), chunk_size):
                    chunk = dates_sorted[i:i + chunk_size]
                    date_literals = ", ".join([f"#{d}#" for d in chunk])
                    in_clauses.append(f"[指示日] IN ({date_literals})")
                per_machine_conditions.append(
                    f"([号機] = '{escaped_machine}' AND ({' OR '.join(in_clauses)}))"
                )

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

                # Access側の ORDER BY を避け、同等の安定ソートをpandas側で実施（結果の順序を維持）
                sort_cols = []
                if "指示日" in lots_df.columns:
                    sort_cols.append("指示日")
                if "号機" in lots_df.columns:
                    sort_cols.append("号機")
                if sort_cols and not lots_df.empty:
                    lots_df = lots_df.sort_values(sort_cols, na_position="last", kind="mergesort")
            
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
        spreadsheet = exporter._get_spreadsheet()
        if not spreadsheet:
            log("スプレッドシートの取得に失敗しました")
            return []
        
        spreadsheet_title = spreadsheet.title if hasattr(spreadsheet, 'title') else "洗浄二次処理依頼"
        log(f"スプレッドシート「{spreadsheet_title}」のシート「{sheet_name}」からデータを取得中...")
        
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
                # 同一品番で何度もログが出ると冗長になるため、初回のみINFOで出す
                try:
                    key = (str(product_number), str(process_number), str(keyword))
                    if key not in _process_infer_logged:
                        _process_infer_logged.add(key)
                        log_callback(
                            f"工程マスタから工程番号を推定: 品番='{product_number}', "
                            f"工程番号='{process_number}', 工程名='{cell_str}', キーワード='{keyword}'"
                        )
                except Exception:
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
        # 今日の日付からシート名を生成（MMDD形式）
        today = datetime.now()
        sheet_name_today = today.strftime('%m%d')

        today_key = (str(google_sheets_url_cleaning or ""), "依頼一覧", today.strftime("%Y-%m-%d"))
        instructions_key = (str(google_sheets_url_cleaning_instructions or ""), sheet_name_today, today.strftime("%Y-%m-%d"))

        cached_today = _cleaning_cache_get(today_key)
        cached_instructions = _cleaning_cache_get(instructions_key)
        if cached_today is not None and cached_instructions is not None:
            logger.bind(channel="PERF").debug("PERF cleaning.sheets.today_requests: cache_hit")
            logger.bind(channel="PERF").debug("PERF cleaning.sheets.instructions: cache_hit")
            today_data = cached_today
            cleaning_instructions = cached_instructions
        else:
            # GoogleSheetsExporterを初期化（キャッシュミス時のみ）
            exporter_cleaning = GoogleSheetsExporter(
                sheets_url=google_sheets_url_cleaning,
                credentials_path=google_sheets_credentials_path
            )
            
            exporter_instructions = GoogleSheetsExporter(
                sheets_url=google_sheets_url_cleaning_instructions,
                credentials_path=google_sheets_credentials_path
            )

            # データ取得（独立処理なので並列化して待ち時間を短縮）
            def _fetch_today_requests():
                cached = _cleaning_cache_get(today_key)
                if cached is not None:
                    logger.bind(channel="PERF").debug("PERF cleaning.sheets.today_requests: cache_hit")
                    return cached
                log("洗浄二次処理依頼からデータを取得中...")
                with perf_timer(logger, "cleaning.sheets.today_requests"):
                    data = _get_today_requests_from_sheets(exporter_cleaning, "依頼一覧", log_callback=log)
                _cleaning_cache_set(today_key, data)
                return data

            def _fetch_instructions():
                cached = _cleaning_cache_get(instructions_key)
                if cached is not None:
                    logger.bind(channel="PERF").debug("PERF cleaning.sheets.instructions: cache_hit")
                    return cached
                log("洗浄指示からデータを取得中...")
                with perf_timer(logger, "cleaning.sheets.instructions"):
                    data = _get_cleaning_instructions_from_sheets(exporter_instructions, sheet_name_today)
                _cleaning_cache_set(instructions_key, data)
                return data

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
        idx_品番 = col_idx_map.get("品番", -1)
        
        for i, row in enumerate(today_data, 1):
            instruction_date = row[idx_指示日].strip() if idx_指示日 >= 0 and len(row) > idx_指示日 and row[idx_指示日] else ""
            machine = row[idx_号機].strip() if idx_号機 >= 0 and len(row) > idx_号機 and row[idx_号機] else ""
            remarks = row[idx_詳細備考].strip() if idx_詳細備考 >= 0 and len(row) > idx_詳細備考 and row[idx_詳細備考] else ""
            product_number = row[idx_品番].strip() if idx_品番 >= 0 and len(row) > idx_品番 and row[idx_品番] else ""
            
            requests: List[Dict[str, object]] = []
            
            # 指示日と号機が両方ある場合
            if instruction_date and machine:
                normalized = _normalize_instruction_date(instruction_date)
                if normalized:
                    request_dict = {
                        "instruction_date": normalized,
                        "machine": machine,
                    }
                    # 品番が指定されている場合は追加
                    if product_number:
                        request_dict["product_number"] = product_number
                    requests.append(request_dict)
                    # デバッグ: リクエスト作成をログ出力
                    log(f"  行 {i}: リクエスト作成（品番: {product_number}, 号機: {machine}, 指示日: {instruction_date} → {normalized}）")
                else:
                    log(f"  行 {i}: 指示日の正規化に失敗（指示日: {instruction_date}）")
            
            # 指示日または号機が欠落している場合、詳細・備考を解析
            elif remarks:
                parsed_list = _parse_remarks_multi(remarks)
                for parsed in parsed_list:
                    date_list = _generate_date_range(parsed['start_date'], parsed['days'])
                    request_dict = {
                        "date_list": date_list,
                        "machine": parsed['machine'],
                        "required_lots": parsed['days'],  # 必要なロット数を保存
                        "original_start_date": parsed['start_date'],  # 元の開始日付を保存
                    }
                    # 品番が指定されている場合は追加
                    if product_number:
                        request_dict["product_number"] = product_number
                    requests.append(request_dict)
            
            for request in requests:
                batch_requests.append(request)
                row_info_list.append({
                    "index": i,
                    "row": row,
                    "request": request,
                })
        
        # リクエストを2つのグループに分ける：
        # 1. instruction_dateのみのリクエスト（個別の日付で直接取得）
        # 2. date_listのリクエスト（範囲指定で取得）
        instruction_date_requests = []
        date_list_requests = []
        instruction_date_row_info_list = []
        date_list_row_info_list = []
        
        for row_info in row_info_list:
            req = row_info["request"]
            # instruction_dateのみでdate_listがない場合は、個別処理
            if req.get("instruction_date") and not req.get("date_list"):
                instruction_date_requests.append(req)
                instruction_date_row_info_list.append(row_info)
            else:
                # date_listがある場合は、範囲指定で処理
                date_list_requests.append(req)
                date_list_row_info_list.append(row_info)
        
        # バッチ処理で一括取得
        all_lots = []
        instruction_date_lots_df = pd.DataFrame()
        date_list_lots_df = pd.DataFrame()
        
        # 1. instruction_dateのみのリクエストを個別の日付で取得
        if instruction_date_requests:
            log(f"instruction_date指定のリクエストを処理中... ({len(instruction_date_requests)}件)")
            with perf_timer(logger, "cleaning.access.instruction_date_lots"):
                instruction_date_lots_df = _get_lots_from_access_instruction_date(connection, instruction_date_requests)
            log(f"instruction_date指定で取得したロット数: {len(instruction_date_lots_df)}件")
            if not instruction_date_lots_df.empty:
                # 指示日列を正規化（YYYY-MM-DD形式に統一）
                if "指示日" in instruction_date_lots_df.columns:
                    instruction_date_lots_df['_指示日_normalized'] = instruction_date_lots_df['指示日'].apply(_normalize_instruction_date)
        
        # 2. date_listのリクエストを範囲指定で取得
        if date_list_requests:
            log(f"date_list指定のリクエストを処理中... ({len(date_list_requests)}件)")
            with perf_timer(logger, "cleaning.access.batch_lots"):
                date_list_lots_df = _get_lots_from_access_batch(connection, date_list_requests)
            log(f"date_list指定で取得したロット数: {len(date_list_lots_df)}件")
            if not date_list_lots_df.empty:
                # 指示日列を正規化（YYYY-MM-DD形式に統一）
                if "指示日" in date_list_lots_df.columns:
                    date_list_lots_df['_指示日_normalized'] = date_list_lots_df['指示日'].apply(_normalize_instruction_date)
        
        # デバッグ: 取得したロットの指示日を確認
        if not instruction_date_lots_df.empty:
            if "指示日" in instruction_date_lots_df.columns and "品番" in instruction_date_lots_df.columns and "号機" in instruction_date_lots_df.columns:
                for product in instruction_date_lots_df["品番"].dropna().unique():
                    for machine in instruction_date_lots_df[instruction_date_lots_df["品番"] == product]["号機"].dropna().unique():
                        mask = (instruction_date_lots_df["品番"] == product) & (instruction_date_lots_df["号機"] == machine)
                        if mask.any():
                            dates = instruction_date_lots_df[mask]["_指示日_normalized"].dropna().unique().tolist()
                            # デバッグログはDEBUGレベルに変更（本番環境では出力されない）
                            # log(f"  デバッグ: instruction_date指定で取得したロット（品番 '{product}', 号機 '{machine}'）の指示日: {sorted(dates)}")
        
        if not date_list_lots_df.empty:
            # デバッグ: date_list指定で取得した全ロットの指示日を確認
            all_dates = date_list_lots_df["_指示日_normalized"].dropna().unique().tolist()
            # デバッグログはDEBUGレベルに変更（本番環境では出力されない）
            # log(f"  デバッグ: date_list指定で取得した全ロットの指示日: {sorted(all_dates)}")
            if "品番" in date_list_lots_df.columns and "号機" in date_list_lots_df.columns:
                # 品番と号機の組み合わせごとの指示日を確認
                for product in date_list_lots_df["品番"].dropna().unique():
                    for machine in date_list_lots_df[date_list_lots_df["品番"] == product]["号機"].dropna().unique():
                        mask = (date_list_lots_df["品番"] == product) & (date_list_lots_df["号機"] == machine)
                        if mask.any():
                            dates = date_list_lots_df[mask]["_指示日_normalized"].dropna().unique().tolist()
                            # デバッグログはDEBUGレベルに変更（本番環境では出力されない）
                            # log(f"  デバッグ: date_list指定で取得したロット（品番 '{product}', 号機 '{machine}'）の指示日: {sorted(dates)}")
        
        # 各リクエストに対応するロットを分離
        filtered_count = 0
        for row_info in row_info_list:
            req = row_info["request"]
            
            # instruction_dateのみのリクエストは、instruction_date_lots_dfから直接フィルタリング
            # date_listのリクエストは、date_list_lots_dfからフィルタリング
            if req.get("instruction_date") and not req.get("date_list"):
                # instruction_dateのみのリクエスト
                filtered_df = instruction_date_lots_df.copy() if not instruction_date_lots_df.empty else pd.DataFrame()
            else:
                # date_listのリクエスト
                filtered_df = date_list_lots_df.copy() if not date_list_lots_df.empty else pd.DataFrame()
            
            if filtered_df.empty:
                # ロットが取得できていない場合はスキップ
                if req.get("product_number"):
                    product_debug = req.get("product_number", "")
                    machine_debug = req.get("machine", "不明")
                    date_debug = req.get("instruction_date") or (req.get("date_list", [])[0] if req.get("date_list") else "N/A")
                    log(f"  リクエスト {row_info['index']}: ⚠️ フィルタリング結果が0件です（品番: {product_debug}, 号機: {machine_debug}, 指示日: {date_debug}）")
                continue
            
            mask = pd.Series([True] * len(filtered_df), index=filtered_df.index)
            
            # 品番が指定されている場合は、品番でフィルタリング（空白/全角差を吸収）
            if req.get("product_number") and "品番" in filtered_df.columns:
                product_number_req = _normalize_key_text(req["product_number"])
                if product_number_req:
                    product_series = filtered_df["品番"].astype(str).map(_normalize_key_text)
                    mask = mask & (product_series == product_number_req)
            
            # 指示日が指定されている場合は、指示日でフィルタリング
            if req.get("instruction_date"):
                normalized_req_date = _normalize_instruction_date(req["instruction_date"])
                if normalized_req_date:
                    mask = mask & (filtered_df['_指示日_normalized'] == normalized_req_date)
            
            # 号機が指定されている場合は、号機でフィルタリング（品番が指定されている場合でも必須）
            if req.get("machine") and "号機" in filtered_df.columns:
                machine_req = _normalize_key_text(req["machine"])
                if machine_req:
                    machine_series = filtered_df["号機"].astype(str).map(_normalize_key_text)
                    mask = mask & (machine_series == machine_req)
            
            if req.get("date_list"):
                normalized_date_list = [_normalize_instruction_date(d) for d in req["date_list"]]
                normalized_date_set = {d for d in normalized_date_list if d}
                if normalized_date_set:
                    mask = mask & filtered_df['_指示日_normalized'].isin(normalized_date_set)
            
            filtered_df = filtered_df[mask].copy()
            filtered_df = filtered_df.drop(columns=['_指示日_normalized'], errors='ignore')
            
            # デバッグ: フィルタリング結果をログ出力（品番指定時）
            if req.get("product_number"):
                product_debug = req.get("product_number", "")
                machine_debug = req.get("machine", "不明")
                date_debug = req.get("instruction_date") or (req.get("date_list", [])[0] if req.get("date_list") else "N/A")
                log(f"  リクエスト {row_info['index']}: フィルタリング結果（品番: {product_debug}, 号機: {machine_debug}, 指示日: {date_debug}）: {len(filtered_df)}件")
                if len(filtered_df) > 0 and "生産ロットID" in filtered_df.columns:
                    lot_ids = filtered_df["生産ロットID"].dropna().unique().tolist()
                    log(f"  リクエスト {row_info['index']}: 取得した生産ロットID: {lot_ids}")
                elif len(filtered_df) == 0:
                    log(f"  リクエスト {row_info['index']}: ⚠️ フィルタリング結果が0件です（品番: {product_debug}, 号機: {machine_debug}, 指示日: {date_debug}）")
            
            # 指定されたロット数に達しない場合、翌年のロットを追加で取得
            required_lots = req.get("required_lots")
            if required_lots and len(filtered_df) < required_lots:
                missing_lots = required_lots - len(filtered_df)
                machine = req.get("machine")
                
                if machine:
                    try:
                                # 元のリクエストの開始日付から次の年の1月1日を計算
                                original_start_date = req.get("original_start_date")
                                next_year = None
                                
                                if original_start_date:
                                    # original_start_dateがdatetimeオブジェクトまたは文字列の場合に対応
                                    if isinstance(original_start_date, str):
                                        try:
                                            # まず '%Y-%m-%d' 形式で試す
                                            original_start = datetime.strptime(original_start_date, '%Y-%m-%d')
                                            next_year = original_start.year + 1
                                        except ValueError:
                                            try:
                                                # 'MM/DD' 形式の場合、_generate_date_rangeと同じロジックで年を決定
                                                if '/' in original_start_date:
                                                    month, day = map(int, original_start_date.split('/'))
                                                    now = datetime.now()
                                                    # 12月の日付が現在の月（1月など）より前の場合は、前年として解釈
                                                    if month == 12 and now.month < 12:
                                                        year = now.year - 1
                                                    else:
                                                        year = now.year
                                                    original_start = datetime(year, month, day)
                                                    next_year = original_start.year + 1
                                                else:
                                                    # その他の形式の場合は、現在の年から次の年を計算
                                                    now = datetime.now()
                                                    next_year = now.year + 1
                                            except (ValueError, AttributeError) as e:
                                                log(f"  リクエスト {row_info['index']}: original_start_dateのパースに失敗しました: {original_start_date}, エラー: {e}")
                                                # エラーが発生した場合は、現在の年から次の年を計算
                                                now = datetime.now()
                                                next_year = now.year + 1
                                    else:
                                        # datetimeオブジェクトの場合
                                        original_start = original_start_date
                                        next_year = original_start.year + 1
                                else:
                                    # original_start_dateがない場合は、現在の年から次の年を計算
                                    now = datetime.now()
                                    next_year = now.year + 1
                                
                                if next_year is None:
                                    # next_yearが設定されていない場合は、現在の年から次の年を計算
                                    now = datetime.now()
                                    next_year = now.year + 1
                                
                                next_year_start = datetime(next_year, 1, 1)
                                next_year_end = datetime(next_year, 12, 31)
                                
                                # 翌年の1月1日から12月31日までの日付リストを生成（範囲指定用）
                                # 範囲指定で取得するため、最初と最後の日付だけを指定すれば良いが、
                                # 中間の日付も含めることで確実に取得できるようにする
                                next_year_date_list = []
                                current_date = next_year_start
                                # 翌年の1月1日から12月31日までの全365日を生成
                                while current_date <= next_year_end:
                                    next_year_date_list.append(current_date.strftime('%Y-%m-%d'))
                                    current_date += timedelta(days=1)
                                
                                # 翌年のロットを取得
                                next_year_req = {
                                    "date_list": next_year_date_list,
                                    "machine": machine,
                                }
                                # 品番が指定されている場合は追加
                                if req.get("product_number"):
                                    next_year_req["product_number"] = req["product_number"]
                                next_year_lots_df = _get_lots_from_access_batch(connection, [next_year_req])
                                
                                if not next_year_lots_df.empty and "指示日" in next_year_lots_df.columns:
                                    # 指示日列を正規化
                                    next_year_lots_df['_指示日_normalized'] = next_year_lots_df['指示日'].apply(_normalize_instruction_date)
                                    
                                    # 品番、号機、日付でフィルタリング
                                    next_year_mask = pd.Series([True] * len(next_year_lots_df), index=next_year_lots_df.index)
                                    
                                    # 品番が指定されている場合は、品番でフィルタリング
                                    if req.get("product_number") and "品番" in next_year_lots_df.columns:
                                        product_number_req = req["product_number"].strip()
                                        if product_number_req:
                                            next_year_mask = next_year_mask & (next_year_lots_df['品番'] == product_number_req)
                                    
                                    # 号機が指定されている場合は、号機でフィルタリング（品番が指定されている場合でも必須）
                                    if "号機" in next_year_lots_df.columns:
                                        next_year_mask = next_year_mask & (next_year_lots_df['号機'] == machine)
                                    
                                    # 翌年の1月1日以降であることを確認
                                    next_year_start_normalized = _normalize_instruction_date(next_year_start.strftime('%Y-%m-%d'))
                                    if next_year_start_normalized:
                                        next_year_mask = next_year_mask & (next_year_lots_df['_指示日_normalized'] >= next_year_start_normalized)
                                    
                                    next_year_filtered_df = next_year_lots_df[next_year_mask].copy()
                                    
                                    if not next_year_filtered_df.empty:
                                        # 指示日が古い順にソートして、必要な数だけ取得
                                        next_year_filtered_df = next_year_filtered_df.sort_values(by="指示日").head(missing_lots)
                                        next_year_filtered_df = next_year_filtered_df.drop(columns=['_指示日_normalized'], errors='ignore')
                                        
                                        filtered_df = pd.concat([filtered_df, next_year_filtered_df], ignore_index=True)
                                        
                                        # 取得ロット数がまだ不足している場合、警告を出力
                                        if len(filtered_df) < required_lots:
                                            log(f"  リクエスト {row_info['index']}: ⚠️ 警告: ロット数がまだ不足しています（取得: {len(filtered_df)}件 / 必要: {required_lots}件、号機: {machine}）")
                    except Exception as e:
                        log(f"  リクエスト {row_info['index']}: 翌年のロット取得処理中にエラーが発生しました: {e}")
                        # エラーが発生しても処理を続行
            
            if not filtered_df.empty:
                        all_lots.append(filtered_df)
                        filtered_count += len(filtered_df)
                        machine_info = req.get("machine", "不明")
                        required_info = f" / 必要: {req.get('required_lots', 'N/A')}件" if req.get("required_lots") else ""
                        log(f"  リクエスト {row_info['index']}: {len(filtered_df)}件のロットを追加（号機: {machine_info}{required_info}, 累計: {filtered_count}件）")
                        
                        # 取得ロット数が不足している場合、警告を出力
                        if req.get("required_lots") and len(filtered_df) < req.get("required_lots"):
                            log(f"  リクエスト {row_info['index']}: ⚠️ 警告: 最終的にロット数が不足しています（取得: {len(filtered_df)}件 / 必要: {req.get('required_lots')}件、号機: {machine_info}）")
        
        if filtered_count > 0:
            log(f"通常の在庫ロット（洗浄二次処理依頼）: {filtered_count}件をall_lotsに追加しました")
        elif not instruction_date_lots_df.empty or not date_list_lots_df.empty:
            log("バッチ処理で取得したロットが空です")
        else:
            log("バッチリクエストが空のため、通常の在庫ロットは取得されませんでした")
        
        # 洗浄指示からもロットを取得
        # Googleスプレッドシートの洗浄指示から取得したデータをそのままロットとして使用
        # ===== 洗浄指示（GOOGLE_SHEETS_URL_CLEANING_INSTRUCTIONS）の処理 =====
        # この部分は洗浄指示専用の処理で、original_start_dateのパース処理は含まれません
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
                        "洗浄指示_行番号": instruction.get("行番号", ""),
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
                machine_info = ""
                if '号機' in df.columns:
                    machines = df['号機'].unique()
                    machine_info = f", 号機: {', '.join(map(str, machines[:3]))}" + (f" 他{len(machines)-3}件" if len(machines) > 3 else "")
                log(f"  DataFrame {idx+1}: {len(df)}件（生産ロットIDあり: {(df['生産ロットID'].notna() & (df['生産ロットID'] != '')).sum() if '生産ロットID' in df.columns else 0}件{machine_info}）")
            
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
                    before_has_lot_id_count = len(has_lot_id_df)
                    has_lot_id_df = has_lot_id_df.drop_duplicates(subset=['生産ロットID'], keep='first')
                    if before_has_lot_id_count != len(has_lot_id_df):
                        log(f"重複除去: 生産ロットIDありのロット {before_has_lot_id_count}件 → {len(has_lot_id_df)}件（{before_has_lot_id_count - len(has_lot_id_df)}件の重複を除去）")

                if not no_lot_id_df.empty:
                    subset_cols = ['品番', '号機']
                    if '指示日' in no_lot_id_df.columns:
                        subset_cols.append('指示日')
                    # 洗浄指示シート由来の行は、同一キーが複数存在し得るため行番号も重複判定に含める
                    if '洗浄指示_行番号' in no_lot_id_df.columns:
                        subset_cols.append('洗浄指示_行番号')
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
                    removed_count = before_count - len(final_lots_df)
                    log(f"重複を除去: {before_count}件 → {len(final_lots_df)}件（{removed_count}件の重複を除去）")
                    # 重複除去の詳細をログ出力（号機別）
                    if '号機' in final_lots_df.columns:
                        machine_counts = final_lots_df['号機'].value_counts()
                        log(f"重複除去後の号機別ロット数: {dict(machine_counts)}")
                else:
                    log(f"重複なし: {before_count}件")

            # ロット数量が欠損/0の行は、数量を優先的に補完（洗浄二次処理依頼の欠損対策）
            if 'ロット数量' in final_lots_df.columns and '数量' in final_lots_df.columns and not final_lots_df.empty:
                lot_qty = pd.to_numeric(final_lots_df['ロット数量'], errors='coerce')
                qty = pd.to_numeric(final_lots_df['数量'], errors='coerce')
                fill_mask = (lot_qty.isna() | (lot_qty <= 0)) & qty.notna() & (qty > 0)
                fill_count = int(fill_mask.sum())
                if fill_count > 0:
                    final_lots_df.loc[fill_mask, 'ロット数量'] = qty[fill_mask]
                    log(f"洗浄二次処理依頼: ロット数量が0/NaNの{fill_count}件を数量で補完しました")
            
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
            
            # 最終結果の号機別ロット数をログ出力（デバッグ用）
            if '号機' in final_lots_df.columns:
                machine_final_counts = final_lots_df['号機'].value_counts().to_dict()
                log(f"最終結果の号機別ロット数: {machine_final_counts}")
            
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
