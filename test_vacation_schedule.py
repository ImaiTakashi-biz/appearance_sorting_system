"""
休暇予定スプレッドシート読み込みテストスクリプト
Googleスプレッドシートから休暇予定を読み込むテスト用スクリプト
"""

import os
import sys
import pandas as pd
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
        dict: 休暇情報の辞書（定義にない場合はNoneを返す）
    """
    code = vacation_code.strip()
    if code in VACATION_DEFINITIONS:
        info = VACATION_DEFINITIONS[code].copy()
        info["code"] = code
        return info
    else:
        # 未定義のコードの場合
        return {
            "code": code,
            "interpretation": "未定義のコード",
            "work_status": None,
            "start_time": None,
            "end_time": None,
            "remarks": ""
        }


def setup_logging():
    """ログ設定"""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )


def get_current_month_sheet_name():
    """
    現在の年月からシート名を生成する
    
    Returns:
        str: シート名（例：「2025.11」「2026.1」）
    """
    now = datetime.now()
    return f"{now.year}.{now.month}"


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
            sheet_name = get_current_month_sheet_name()
            logger.info(f"現在の年月からシート名を自動生成: {sheet_name}")
        
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
                # 現在の年月を使用
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
        all_values = worksheet.get_all_values()
        
        if not all_values or len(all_values) < 3:
            logger.warning("データが存在しないか、フォーマットが正しくありません")
            return {}
        
        # 3行目（インデックス2）から日付ヘッダーを取得
        date_row = all_values[2]  # 3行目（0-indexedなので2）
        dates = []
        for i, cell_value in enumerate(date_row[1:], start=1):  # B列から開始（列インデックス1）
            if cell_value.strip():
                try:
                    day = int(cell_value.strip())
                    if 1 <= day <= 31:
                        dates.append((i, day))  # (列インデックス, 日) - B列はインデックス1
                except ValueError:
                    # 日付でないセルはスキップ（例：「累積休暇日数」など）
                    continue
        
        logger.info(f"日付ヘッダーを {len(dates)} 件取得しました: {[d[1] for d in dates]}")
        
        # 4行目以降から従業員名と休暇情報を取得
        vacation_data = {}
        for row_idx, row in enumerate(all_values[3:], start=4):  # 4行目から開始（0-indexedなので3）
            if not row or not row[0]:  # A列が空の場合はスキップ
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
                        # 日付を生成（YYYY-MM-DD形式）
                        try:
                            date_str = f"{year}-{month:02d}-{day:02d}"
                            # 例外勤務定義から詳細情報を取得
                            vacation_info = get_vacation_info(vacation_code)
                            employee_schedule[date_str] = vacation_info
                            
                            # 未定義のコードの場合は警告を出力
                            if vacation_info["interpretation"] == "未定義のコード":
                                logger.warning(f"未定義の休暇コードを検出: {employee_name} - {date_str} - '{vacation_code}'")
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


def load_inspector_master(inspector_master_path: str) -> list:
    """
    検査員マスタファイルから検査員名のリストを取得する
    
    Args:
        inspector_master_path: 検査員マスタファイルのパス
    
    Returns:
        list: 検査員名のリスト
    """
    try:
        if not inspector_master_path or not Path(inspector_master_path).exists():
            logger.warning(f"検査員マスタファイルが見つかりません: {inspector_master_path}")
            return []
        
        # CSVファイルを読み込み
        df = pd.read_csv(
            inspector_master_path,
            encoding='utf-8-sig',
            header=None,
            low_memory=False
        )
        
        # 2行目をヘッダーとして使用
        if len(df) > 1:
            new_header = df.iloc[1]
            df = df[2:]  # 2行目以降のデータのみ残す
            df.columns = new_header
            df = df.reset_index(drop=True)
        
        # 氏名列が存在するかチェック
        if '#氏名' not in df.columns:
            logger.warning("検査員マスタに「#氏名」列が見つかりません")
            return []
        
        # 検査員名のリストを取得（空の値を除外）
        inspector_names = df['#氏名'].dropna().astype(str).str.strip()
        inspector_names = inspector_names[inspector_names != ''].tolist()
        
        logger.info(f"検査員マスタから {len(inspector_names)} 名の検査員を読み込みました")
        return inspector_names
        
    except Exception as e:
        logger.error(f"検査員マスタの読み込みに失敗しました: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []


def display_vacation_for_date(vacation_data: dict, target_date: date, inspector_names: list = None, detailed: bool = True):
    """
    特定日付の休暇情報を表示する
    
    Args:
        vacation_data: {従業員名: {日付: 休暇情報辞書}} の形式の辞書
        target_date: 対象日付
        inspector_names: 検査員名のリスト（指定された場合はその検査員のみ表示）
        detailed: Trueの場合は詳細情報（解釈、時刻など）も表示
    """
    date_str = target_date.strftime("%Y-%m-%d")
    date_str_jp = target_date.strftime("%Y年%m月%d日")
    
    logger.info("=" * 80)
    logger.info(f"{date_str_jp} ({date_str}) の休暇情報")
    logger.info("=" * 80)
    
    # 対象日の休暇情報を取得
    date_vacation = get_vacation_for_date(vacation_data, target_date)
    
    if not date_vacation:
        logger.info("対象日に休暇予定はありません")
        return
    
    # 検査員リストが指定されている場合はフィルタリング
    if inspector_names:
        # 検査員マスタに存在する検査員のみ表示
        filtered_vacation = {
            name: info for name, info in date_vacation.items() 
            if name in inspector_names
        }
        
        # 検査員マスタに存在するが休暇予定がない検査員も表示
        for inspector_name in inspector_names:
            if inspector_name not in filtered_vacation:
                filtered_vacation[inspector_name] = None
        
        date_vacation = filtered_vacation
    
    # 休暇ありの検査員
    vacation_employees = {name: info for name, info in date_vacation.items() if info is not None}
    # 休暇なしの検査員
    no_vacation_employees = [name for name, info in date_vacation.items() if info is None]
    
    if vacation_employees:
        logger.info(f"\n【休暇予定がある検査員 ({len(vacation_employees)} 名)】")
        for employee_name, vacation_info in sorted(vacation_employees.items()):
            if detailed and isinstance(vacation_info, dict):
                code = vacation_info.get("code", "")
                interpretation = vacation_info.get("interpretation", "")
                work_status = vacation_info.get("work_status", "")
                start_time = vacation_info.get("start_time", "")
                end_time = vacation_info.get("end_time", "")
                
                info_parts = [f"{code}"]
                if interpretation:
                    info_parts.append(f"({interpretation})")
                if work_status:
                    info_parts.append(f"[{work_status}]")
                if start_time and end_time:
                    info_parts.append(f"{start_time}～{end_time}")
                elif start_time:
                    info_parts.append(f"{start_time}～")
                elif end_time:
                    info_parts.append(f"～{end_time}")
                
                logger.info(f"  {employee_name}: {' '.join(info_parts)}")
            else:
                logger.info(f"  {employee_name}: {vacation_info}")
    
    if no_vacation_employees:
        logger.info(f"\n【休暇予定がない検査員 ({len(no_vacation_employees)} 名)】")
        for employee_name in sorted(no_vacation_employees):
            logger.info(f"  {employee_name}: 通常勤務")
    
    # 統計情報
    logger.info(f"\n統計: 対象日 {date_str_jp}")
    logger.info(f"  休暇予定あり: {len(vacation_employees)} 名")
    logger.info(f"  休暇予定なし: {len(no_vacation_employees)} 名")
    
    # 休暇コード別の集計
    if vacation_employees:
        code_counts = {}
        for vacation_info in vacation_employees.values():
            if isinstance(vacation_info, dict):
                code = vacation_info.get("code", "")
                code_counts[code] = code_counts.get(code, 0) + 1
        
        if code_counts:
            logger.info("  休暇コード別集計:")
            for code, count in sorted(code_counts.items()):
                interpretation = VACATION_DEFINITIONS.get(code, {}).get("interpretation", "未定義")
                logger.info(f"    {code} ({interpretation}): {count} 名")


def display_vacation_schedule(vacation_data: dict, limit: int = 20, detailed: bool = True):
    """
    休暇予定データを表示する
    
    Args:
        vacation_data: {従業員名: {日付: 休暇情報辞書}} の形式の辞書
        limit: 表示する最大件数（従業員数）
        detailed: Trueの場合は詳細情報（解釈、時刻など）も表示
    """
    if not vacation_data:
        logger.info("表示するデータがありません")
        return
    
    logger.info("=" * 80)
    logger.info("休暇予定データ")
    logger.info("=" * 80)
    
    # 従業員ごとに表示
    employee_list = list(vacation_data.keys())[:limit]
    
    for employee_name in employee_list:
        schedule = vacation_data[employee_name]
        logger.info(f"\n【{employee_name}】")
        
        # 日付順にソートして表示
        sorted_dates = sorted(schedule.keys())
        for date_str in sorted_dates:
            vacation_info = schedule[date_str]
            
            if detailed and isinstance(vacation_info, dict):
                # 構造化データの場合
                code = vacation_info.get("code", "")
                interpretation = vacation_info.get("interpretation", "")
                work_status = vacation_info.get("work_status", "")
                start_time = vacation_info.get("start_time", "")
                end_time = vacation_info.get("end_time", "")
                
                # 基本情報を表示
                info_parts = [f"{code}"]
                if interpretation:
                    info_parts.append(f"({interpretation})")
                if work_status:
                    info_parts.append(f"[{work_status}]")
                if start_time and end_time:
                    info_parts.append(f"{start_time}～{end_time}")
                elif start_time:
                    info_parts.append(f"{start_time}～")
                elif end_time:
                    info_parts.append(f"～{end_time}")
                
                logger.info(f"  {date_str}: {' '.join(info_parts)}")
            else:
                # 文字列の場合（後方互換性）
                logger.info(f"  {date_str}: {vacation_info}")
        
        if len(schedule) == 0:
            logger.info("  （休暇予定なし）")
    
    if len(vacation_data) > limit:
        logger.info(f"\n... 他 {len(vacation_data) - limit} 名")
    
    # 統計情報を表示
    total_employees = len(vacation_data)
    total_vacation_days = sum(len(schedule) for schedule in vacation_data.values())
    
    # 休暇コード別の集計
    code_counts = {}
    for schedule in vacation_data.values():
        for vacation_info in schedule.values():
            if isinstance(vacation_info, dict):
                code = vacation_info.get("code", "")
                code_counts[code] = code_counts.get(code, 0) + 1
    
    logger.info(f"\n統計: 従業員数 {total_employees} 名、休暇予定総数 {total_vacation_days} 件")
    if code_counts:
        logger.info("休暇コード別集計:")
        for code, count in sorted(code_counts.items()):
            interpretation = VACATION_DEFINITIONS.get(code, {}).get("interpretation", "未定義")
            logger.info(f"  {code} ({interpretation}): {count} 件")


def parse_date(date_str: str) -> date:
    """
    日付文字列をdateオブジェクトに変換する
    
    Args:
        date_str: 日付文字列（YYYY-MM-DD形式またはYYYY/MM/DD形式）
    
    Returns:
        date: dateオブジェクト
    """
    try:
        # YYYY-MM-DD形式
        if '-' in date_str:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        # YYYY/MM/DD形式
        elif '/' in date_str:
            return datetime.strptime(date_str, "%Y/%m/%d").date()
        else:
            raise ValueError(f"無効な日付形式: {date_str}")
    except ValueError as e:
        logger.error(f"日付の解析に失敗しました: {str(e)}")
        raise


def main():
    """メイン処理"""
    setup_logging()
    
    try:
        # コマンドライン引数から日付を取得（指定されていない場合は今日の日付）
        target_date = None
        if len(sys.argv) > 1:
            try:
                target_date = parse_date(sys.argv[1])
                logger.info(f"コマンドライン引数から日付を取得: {target_date.strftime('%Y年%m月%d日')}")
            except Exception as e:
                logger.error(f"日付の解析に失敗しました: {str(e)}")
                logger.info("使用方法: python test_vacation_schedule.py [YYYY-MM-DD]")
                logger.info("例: python test_vacation_schedule.py 2025-11-12")
                return
        else:
            target_date = date.today()
            logger.info(f"日付が指定されていないため、今日の日付を使用: {target_date.strftime('%Y年%m月%d日')}")
        
        # config.envを読み込み
        env_file_path = "config.env"
        if not Path(env_file_path).exists():
            logger.error(f"設定ファイルが見つかりません: {env_file_path}")
            return
        
        load_env_file(env_file_path)
        
        # 設定値を取得
        vacation_sheets_url = os.getenv("GOOGLE_SHEETS_URL_VACATION")
        credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
        inspector_master_path = os.getenv("INSPECTOR_MASTER_PATH")
        
        # 設定の確認
        if not vacation_sheets_url:
            logger.error("GOOGLE_SHEETS_URL_VACATIONが設定されていません")
            logger.info("config.envに以下の設定を追加してください:")
            logger.info("GOOGLE_SHEETS_URL_VACATION=https://docs.google.com/spreadsheets/d/...")
            return
        
        if not credentials_path:
            logger.error("GOOGLE_SHEETS_CREDENTIALS_PATHが設定されていません")
            return
        
        if not Path(credentials_path).exists():
            logger.error(f"認証情報ファイルが見つかりません: {credentials_path}")
            return
        
        # 対象日からシート名を生成
        target_sheet_name = f"{target_date.year}.{target_date.month}"
        
        logger.info("=" * 80)
        logger.info("休暇予定スプレッドシート読み込みテスト")
        logger.info("=" * 80)
        logger.info(f"対象日: {target_date.strftime('%Y年%m月%d日')} ({target_date.strftime('%Y-%m-%d')})")
        logger.info(f"スプレッドシートURL: {vacation_sheets_url}")
        logger.info(f"認証情報ファイル: {credentials_path}")
        logger.info(f"対象シート名（自動生成）: {target_sheet_name}")
        logger.info("=" * 80)
        
        # 休暇予定を読み込む
        vacation_data = load_vacation_schedule(
            sheets_url=vacation_sheets_url,
            credentials_path=credentials_path,
            sheet_name=target_sheet_name
        )
        
        # 検査員マスタから検査員リストを取得
        inspector_names = None
        if inspector_master_path:
            inspector_names = load_inspector_master(inspector_master_path)
        
        # 対象日の休暇情報を表示
        display_vacation_for_date(
            vacation_data=vacation_data,
            target_date=target_date,
            inspector_names=inspector_names,
            detailed=True
        )
        
        logger.info("=" * 80)
        logger.info("テスト完了")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()

