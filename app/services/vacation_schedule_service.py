"""
休暇予定スプレッドシート読み込みサービス
Googleスプレッドシートから休暇予定を読み込む
"""

import os
from datetime import datetime, date
from pathlib import Path
from loguru import logger
from app.env_loader import load_env_file
from app.export.google_sheets_exporter_service import GoogleSheetsExporter

# 例外勤務定義
VACATION_DEFINITIONS = {
    "": {
        "interpretation": "例外勤務はなし",
        "work_status": None,
        "start_time": None,
        "end_time": None,
        "remarks": ""
    },
    "休": {
        "interpretation": "終日休み",
        "work_status": "休み",
        "start_time": None,
        "end_time": None,
        "remarks": ""
    },
    "AM": {
        "interpretation": "午前中休暇",
        "work_status": "休み",
        "start_time": "8:30",
        "end_time": "13:00",
        "remarks": ""
    },
    "PM": {
        "interpretation": "午後休暇",
        "work_status": "休み",
        "start_time": "13:00",
        "end_time": "17:30",
        "remarks": ""
    },
    "早": {
        "interpretation": "早退",
        "work_status": "休み",
        "start_time": "15:00",
        "end_time": "17:30",
        "remarks": ""
    },
    "遅": {
        "interpretation": "遅刻",
        "work_status": "休み",
        "start_time": "8:30",
        "end_time": "10:00",
        "remarks": ""
    },
    "中": {
        "interpretation": "例外勤務はなし",
        "work_status": None,
        "start_time": None,
        "end_time": None,
        "remarks": ""
    },
    "出": {
        "interpretation": "終日不在",
        "work_status": "休み",
        "start_time": None,
        "end_time": None,
        "remarks": "出張などの予定で不在な場合の想定"
    },
    "当": {
        "interpretation": "当日休暇",
        "work_status": "休み",
        "start_time": None,
        "end_time": None,
        "remarks": ""
    }
}


def get_vacation_info(vacation_code: str) -> dict:
    """
    休暇コードから詳細情報を取得する
    
    Args:
        vacation_code: 休暇コード（休、AM、PM、早、遅、出、当など）
    
    Returns:
        dict: 休暇情報の辞書
    """
    code = vacation_code.strip()
    if code in VACATION_DEFINITIONS:
        info = VACATION_DEFINITIONS[code].copy()
        info["code"] = code
        return info
    else:
        return {
            "code": code,
            "interpretation": "未定義のコード",
            "work_status": None,
            "start_time": None,
            "end_time": None,
            "remarks": ""
        }


def get_current_month_sheet_name(year: int = None, month: int = None) -> str:
    """
    年月からシート名を生成する
    
    Args:
        year: 年（Noneの場合は現在の年）
        month: 月（Noneの場合は現在の月）
    
    Returns:
        str: シート名（例：「2025.11」「2026.1」）
    """
    if year is None or month is None:
        now = datetime.now()
        year = year or now.year
        month = month or now.month
    return f"{year}.{month}"


def load_vacation_schedule(sheets_url: str, credentials_path: str, sheet_name: str = None, year: int = None, month: int = None):
    """
    休暇予定スプレッドシートからデータを読み込む
    
    Args:
        sheets_url: GoogleスプレッドシートのURL
        credentials_path: Google認証情報JSONファイルのパス
        sheet_name: 読み込むシート名（Noneの場合は現在の年月のシートを使用）
        year: 年（シート名から自動判定する場合はNone）
        month: 月（シート名から自動判定する場合はNone）
    
    Returns:
        dict: {従業員名: {日付: 休暇情報}} の形式の辞書
    """
    try:
        logger.info("休暇予定スプレッドシートへの接続を開始します")
        
        # GoogleSheetsExporterを初期化
        exporter = GoogleSheetsExporter(
            sheets_url=sheets_url,
            credentials_path=credentials_path
        )
        
        # スプレッドシートを取得
        spreadsheet = exporter._get_spreadsheet()
        if not spreadsheet:
            logger.error("スプレッドシートの取得に失敗しました")
            return {}
        
        logger.info(f"スプレッドシート名: {spreadsheet.title}")
        
        # シート名が指定されていない場合は現在の年月のシート名を生成
        if sheet_name is None:
            sheet_name = get_current_month_sheet_name(year, month)
            logger.info(f"シート名を自動生成: {sheet_name}")
        
        # 年月をシート名から取得
        if year is None or month is None:
            try:
                parts = sheet_name.split('.')
                if len(parts) == 2:
                    year = int(parts[0])
                    month = int(parts[1])
                    logger.info(f"シート名から年月を取得: {year}年{month}月")
            except (ValueError, IndexError):
                logger.warning(f"シート名から年月を取得できませんでした: {sheet_name}")
                now = datetime.now()
                year = year or now.year
                month = month or now.month
        
        # シートを取得
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            logger.info(f"シート「{sheet_name}」を取得しました")
        except Exception as e:
            logger.error(f"シート「{sheet_name}」の取得に失敗しました: {str(e)}")
            logger.info("利用可能なシート一覧:")
            for sheet in spreadsheet.worksheets():
                logger.info(f"  - {sheet.title}")
            return {}
        
        # 全データを取得
        # 必要な範囲のみ取得して処理負荷を削減
        all_values = worksheet.get('A1:AG100')
        
        if not all_values or len(all_values) < 3:
            logger.warning("データが存在しないか、フォーマットが正しくありません")
            return {}
        
        # 3行目（インデックス2）から日付ヘッダーを取得
        date_row = all_values[2]
        dates = []
        for i, cell_value in enumerate(date_row[1:], start=1):
            if cell_value.strip():
                try:
                    day = int(cell_value.strip())
                    if 1 <= day <= 31:
                        dates.append((i, day))
                except ValueError:
                    continue
        
        logger.info(f"日付ヘッダーを {len(dates)} 件取得しました")
        
        # 4行目以降から従業員名と休暇情報を取得
        vacation_data = {}
        for row in all_values[3:]:
            if not row or not row[0]:
                continue
            
            employee_name = row[0].strip()
            if not employee_name:
                continue
            
            # 各日の休暇情報を取得
            employee_schedule = {}
            for col_idx, day in dates:
                if col_idx < len(row):
                    vacation_code = row[col_idx].strip()
                    if vacation_code:
                        try:
                            date_str = f"{year}-{month:02d}-{day:02d}"
                            vacation_info = get_vacation_info(vacation_code)
                            employee_schedule[date_str] = vacation_info
                        except Exception as e:
                            logger.warning(f"日付生成エラー: {year}年{month}月{day}日 - {str(e)}")
                            continue
            
            if employee_schedule:
                vacation_data[employee_name] = employee_schedule
        
        logger.info(f"休暇予定データを {len(vacation_data)} 名分読み込みました")
        return vacation_data
        
    except Exception as e:
        logger.error(f"休暇予定の読み込み中にエラーが発生しました: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {}


def get_vacation_for_date(vacation_data: dict, target_date: date) -> dict:
    """
    特定日付の休暇情報を取得する
    
    Args:
        vacation_data: {従業員名: {日付: 休暇情報辞書}} の形式の辞書
        target_date: 対象日付
    
    Returns:
        dict: {従業員名: 休暇情報辞書} の形式の辞書
    """
    date_str = target_date.strftime("%Y-%m-%d")
    result = {}
    
    for employee_name, schedule in vacation_data.items():
        if date_str in schedule:
            result[employee_name] = schedule[date_str]
    
    return result


def calculate_vacation_absence_hours(vacation_info: dict, base_start_time: str, base_end_time: str) -> float:
    """
    休暇情報に基づいて不在時間を計算する
    
    Args:
        vacation_info: 休暇情報辞書
        base_start_time: 基本勤務開始時刻（"HH:MM"形式）
        base_end_time: 基本勤務終了時刻（"HH:MM"形式）
    
    Returns:
        float: 不在時間（時間単位）
    """
    if not vacation_info or not isinstance(vacation_info, dict):
        return 0.0
    
    work_status = vacation_info.get("work_status")
    if work_status != "休み":
        return 0.0
    
    code = vacation_info.get("code", "")
    
    # 終日休みの場合は基本勤務時間全体が不在
    if code in ["休", "出", "当"]:
        # 基本勤務時間を計算
        try:
            base_start_hour = float(base_start_time.split(':')[0]) + float(base_start_time.split(':')[1]) / 60.0
            base_end_hour = float(base_end_time.split(':')[0]) + float(base_end_time.split(':')[1]) / 60.0
            base_hours = base_end_hour - base_start_hour
            # 休憩時間を含む場合は1時間を差し引く
            if base_start_hour <= 12.25 and base_end_hour >= 13.0:
                base_hours -= 1.0
            return max(0.0, base_hours)
        except:
            return 8.0  # デフォルト
    
    # 部分的な休暇の場合
    vacation_start_time = vacation_info.get("start_time")
    vacation_end_time = vacation_info.get("end_time")
    
    if not vacation_start_time or not vacation_end_time:
        return 0.0
    
    try:
        # 休暇の開始時刻と終了時刻を時間に変換
        vac_start_hour = float(vacation_start_time.split(':')[0]) + float(vacation_start_time.split(':')[1]) / 60.0
        vac_end_hour = float(vacation_end_time.split(':')[0]) + float(vacation_end_time.split(':')[1]) / 60.0
        
        # 基本勤務時間を計算
        base_start_hour = float(base_start_time.split(':')[0]) + float(base_start_time.split(':')[1]) / 60.0
        base_end_hour = float(base_end_time.split(':')[0]) + float(base_end_time.split(':')[1]) / 60.0
        
        # 休暇時間と基本勤務時間の重複部分を計算
        overlap_start = max(base_start_hour, vac_start_hour)
        overlap_end = min(base_end_hour, vac_end_hour)
        
        if overlap_start < overlap_end:
            absence_hours = overlap_end - overlap_start
            return absence_hours
        
        return 0.0
    except Exception as e:
        logger.warning(f"不在時間の計算に失敗しました: {str(e)}")
        return 0.0

