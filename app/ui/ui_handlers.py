"""
外観検査振分支援システム - メインUI
近未来的なデザインで出荷予定日を指定してデータを抽出する
"""

import os
import sys
import hashlib
from pathlib import Path
from collections import defaultdict, deque
import warnings  # 警告抑制のため
import webbrowser
from typing import Deque, Dict, List, Optional, Tuple, Any

# pandasのUserWarningを抑制（SQLAlchemy接続の推奨警告）
warnings.filterwarnings('ignore', category=UserWarning, message='.*pandas only supports SQLAlchemy.*')

# 直接実行時のパス解決（モジュールとして実行される場合の対応）
# スクリプトとして直接実行されている場合のみパスを追加
if __package__ is None:
    # プロジェクトルートをパスに追加
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

import customtkinter as ctk
import tkinter as tk
from tkinter import messagebox, filedialog, ttk
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime, date, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import re
from loguru import logger

# ログ分類（app_.logの視認性向上）
logger = logger.bind(channel="UI")

# 検査員列の最大数（UI/出力と一致させる）
MAX_INSPECTORS_PER_LOT = 10
from version import APP_NAME, APP_VERSION, BUILD_DATE


from app.utils.perf import perf_timer
from app.config import DatabaseConfig
import calendar
import locale
from app.export.google_sheets_exporter_service import GoogleSheetsExporter
from app.assignment.inspector_assignment_service import InspectorAssignmentManager
from app.services.cleaning_request_service import get_cleaning_lots
from app.config_manager import AppConfigManager
from app.utils.path_resolver import resolve_resource_path

from app.seat_ui import (
    SEATING_JSON_PATH,
    SEATING_HTML_PATH,
    build_initial_seating_chart,
    attach_lots_to_chart,
    load_seating_chart,
    save_seating_chart,
    generate_html,
)
from app.seat_ui_server import SeatChartServer
from PIL import Image


class ModernDataExtractorUI:
    """近未来的なデザインのデータ抽出UI"""
    
    # キャッシュ設定定数
    TABLE_STRUCTURE_CACHE_TTL = 3600  # 1時間（秒）
    MASTER_CACHE_TTL_MINUTES = 5  # 5分
    ACCESS_LOTS_CACHE_TTL_SECONDS = 300  # 5分（秒）
    
    # UI設定定数（最小サイズのみ指定して柔軟な拡張を許容）
    MIN_WINDOW_WIDTH = 900
    MIN_WINDOW_HEIGHT = 600
    
    # シート出力用の未割当ロットキー
    UNASSIGNED_LOTS_KEY = "__UNASSIGNED_LOTS__"

    @staticmethod
    def _hash_dataframe_v1(
        df: pd.DataFrame,
        *,
        sort_keys: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        order_invariant: bool = False,
    ) -> str:
        """
        DataFrameの内容を比較用にハッシュ化する（個人名などをログに出さずに差分検知するため）
        """
        if df is None:
            return "none"
        if df.empty:
            return "empty"

        work_df = df
        if include_columns:
            cols = [c for c in include_columns if c in work_df.columns]
            if cols:
                work_df = work_df[cols]

        if order_invariant:
            keys = [k for k in (sort_keys or []) if k in work_df.columns]
            if keys:
                try:
                    work_df = work_df.sort_values(by=keys, kind="mergesort", na_position="last")
                except Exception:
                    pass

        try:
            h = pd.util.hash_pandas_object(work_df, index=True)
            return hashlib.sha256(h.values.tobytes()).hexdigest()
        except Exception:
            try:
                payload = work_df.to_csv(index=True).encode("utf-8", errors="replace")
                return hashlib.sha256(payload).hexdigest()
            except Exception:
                return "error"

    def _log_df_signature(
        self,
        label: str,
        df: pd.DataFrame,
        *,
        sort_keys: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
    ) -> None:
        """
        実行結果が不変かどうかをログから機械的に比較できるよう、DataFrameのハッシュを出力する。
        """
        if os.environ.get("DEBUG_SIGNATURE_LOG_ENABLED", "1") != "1":
            return
        try:
            rows = int(len(df)) if df is not None else 0
            cols = int(len(df.columns)) if df is not None and hasattr(df, "columns") else 0
            h_os = self._hash_dataframe_v1(df, include_columns=include_columns, order_invariant=False)
            h_oi = self._hash_dataframe_v1(
                df,
                sort_keys=sort_keys,
                include_columns=include_columns,
                order_invariant=True,
            )
            self.log_message(f"[HASH v1] {label}: rows={rows} cols={cols} order_sensitive={h_os} order_invariant={h_oi}")
        except Exception:
            pass

    @staticmethod
    def _normalize_key_value(val: Any) -> str:
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return ""
        try:
            if isinstance(val, pd.Timestamp):
                if pd.isna(val):
                    return ""
                return val.isoformat()
        except Exception:
            pass
        try:
            return str(val).strip()
        except Exception:
            return ""

    def _build_lot_key_set(self, df: pd.DataFrame) -> Tuple[str, List[str]]:
        """
        ロットを識別するためのキー集合を作る（ログ比較用）。
        """
        if df is None or df.empty:
            return ("empty", [])

        # 優先: 生産ロットIDがある場合はそれをキーに含める
        cols = set(df.columns)
        has_lot_id = "生産ロットID" in cols
        has_product = "品番" in cols
        has_ship = "出荷予定日" in cols

        keys: List[str] = []
        try:
            for row in df.itertuples(index=False):
                row_dict = row._asdict() if hasattr(row, "_asdict") else {}
                lot_id = self._normalize_key_value(row_dict.get("生産ロットID")) if has_lot_id else ""
                prod = self._normalize_key_value(row_dict.get("品番")) if has_product else ""
                ship = self._normalize_key_value(row_dict.get("出荷予定日")) if has_ship else ""
                keys.append(f"{lot_id}|{prod}|{ship}")
        except Exception:
            # フォールバック（遅いが確実）
            for _, row in df.iterrows():
                lot_id = self._normalize_key_value(row.get("生産ロットID")) if has_lot_id else ""
                prod = self._normalize_key_value(row.get("品番")) if has_product else ""
                ship = self._normalize_key_value(row.get("出荷予定日")) if has_ship else ""
                keys.append(f"{lot_id}|{prod}|{ship}")

        keys = sorted(set(keys))
        schema = "生産ロットID|品番|出荷予定日" if has_lot_id else "品番|出荷予定日"
        return (schema, keys)

    def _save_and_log_snapshot(self, snapshot_label: str, df: pd.DataFrame) -> None:
        """
        直前実行との差分（増減ロット）をログに出すために、キー一覧をローカルに保存し比較する。
        """
        if os.environ.get("DEBUG_SNAPSHOT_DIFF_ENABLED", "1") != "1":
            return
        try:
            schema, keys = self._build_lot_key_set(df)
            max_keys_env = os.environ.get("DEBUG_SNAPSHOT_KEYS_MAX", "")
            try:
                max_keys = int(max_keys_env) if max_keys_env else 0
            except Exception:
                max_keys = 0

            if max_keys and len(keys) > max_keys:
                self.log_message(f"[DIFF] {snapshot_label}: skipped (keys={len(keys)} exceeds DEBUG_SNAPSHOT_KEYS_MAX={max_keys})")
                return

            self._save_and_log_snapshot_keys(
                snapshot_label,
                schema=schema,
                keys=keys,
                rows=int(len(df)) if df is not None else 0,
            )
        except Exception:
            pass

    def _save_and_log_snapshot_keys(self, snapshot_label: str, *, schema: str, keys: List[str], rows: int) -> None:
        base_dir = Path(os.environ.get("LOCALAPPDATA", ".")) / "appearance_sorting_system" / "debug_snapshots"
        base_dir.mkdir(parents=True, exist_ok=True)
        path = base_dir / f"last_{snapshot_label}.json"

        prev_keys: Optional[set] = None
        prev_schema: Optional[str] = None
        if path.exists():
            try:
                prev = json.loads(path.read_text(encoding="utf-8"))
                prev_schema = prev.get("schema")
                prev_keys = set(prev.get("keys") or [])
            except Exception:
                prev_keys = None

        payload = {
            "label": snapshot_label,
            "schema": schema,
            "rows": rows,
            "keys": keys,
            "saved_at": datetime.now().isoformat(timespec="seconds"),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        if prev_keys is not None and prev_schema == schema:
            current = set(keys)
            removed = sorted(prev_keys - current)
            added = sorted(current - prev_keys)
            if removed or added:
                self.log_message(f"[DIFF] {snapshot_label}: removed={len(removed)} added={len(added)} (schema={schema})")
                try:
                    max_lines = int(os.environ.get("DEBUG_SNAPSHOT_DIFF_MAX_LINES", "20"))
                except Exception:
                    max_lines = 20
                for k in removed[:max_lines]:
                    self.log_message(f"[DIFF] removed: {k}")
                for k in added[:max_lines]:
                    self.log_message(f"[DIFF] added: {k}")
            else:
                self.log_message(f"[DIFF] {snapshot_label}: no_change (schema={schema})")
        else:
            self.log_message(f"[DIFF] {snapshot_label}: baseline_saved (schema={schema})")

    @staticmethod
    def _hash_token_v1(text: str) -> str:
        if not text:
            return ""
        return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()[:12]

    def _save_and_log_assignment_diff_snapshot(self, snapshot_label: str, df: pd.DataFrame) -> None:
        """
        振分結果の差分（どのロットで割当が変わったか）を、個人名を出さずに比較できる形で保存・出力する。
        """
        if os.environ.get("DEBUG_ASSIGNMENT_DIFF_ENABLED", "1") != "1":
            return
        if os.environ.get("DEBUG_SNAPSHOT_DIFF_ENABLED", "1") != "1":
            return
        if df is None or df.empty:
            return

        cols = set(df.columns)
        required = {"生産ロットID", "品番", "出荷予定日"}
        if not required.issubset(cols):
            return

        inspector_cols = [
            f"検査員{i}" for i in range(1, MAX_INSPECTORS_PER_LOT + 1)
            if f"検査員{i}" in cols
        ]
        if not inspector_cols:
            return

        keys: List[str] = []
        for row in df.itertuples(index=False):
            row_dict = row._asdict() if hasattr(row, "_asdict") else {}
            lot_id = self._normalize_key_value(row_dict.get("生産ロットID"))
            prod = self._normalize_key_value(row_dict.get("品番"))
            ship = self._normalize_key_value(row_dict.get("出荷予定日"))
            tokens: List[str] = []
            for c in inspector_cols:
                raw = self._normalize_key_value(row_dict.get(c))
                if "(" in raw:
                    raw = raw.split("(")[0].strip()
                tokens.append(self._hash_token_v1(raw))
            keys.append(f"{lot_id}|{prod}|{ship}|" + ",".join(tokens))

        keys = sorted(set(keys))
        schema = "生産ロットID|品番|出荷予定日|検査員hash1..5"
        self._save_and_log_snapshot_keys(snapshot_label, schema=schema, keys=keys, rows=int(len(df)))
    
    # クラス変数としてテーブル構造をキャッシュ（高速化）
    _table_structure_cache = None
    _table_structure_cache_timestamp = None
    _table_structure_cache_ttl = TABLE_STRUCTURE_CACHE_TTL
    
    def __init__(self):
        """UIの初期化"""
        # 日本語ロケール設定
        try:
            locale.setlocale(locale.LC_TIME, 'ja_JP.UTF-8')
        except:
            try:
                locale.setlocale(locale.LC_TIME, 'Japanese_Japan.932')
            except:
                pass  # ロケール設定に失敗した場合はデフォルトを使用
        
        # CustomTkinterのテーマ設定
        ctk.set_appearance_mode("light")  # ライトモード
        ctk.set_default_color_theme("blue")  # ブルーテーマ
        
        # メインウィンドウの作成
        self.root = ctk.CTk()
        self.root.title(f"{APP_NAME}  {APP_VERSION}")
        self.root.minsize(self.MIN_WINDOW_WIDTH, self.MIN_WINDOW_HEIGHT)
        
        # ウィンドウの背景色を白に設定
        self.root.configure(fg_color=("white", "white"))
        
        # ウィンドウの閉じるボタン（×）のイベントを設定
        self.root.protocol("WM_DELETE_WINDOW", self.quit_application)
        
        # 変数の初期化
        # 設定を先に読み込む（registered_products_pathを使用するため）
        self.config = DatabaseConfig()
        # アプリケーション設定管理の初期化
        self.app_config_manager = AppConfigManager()
        self.extractor = None
        self.is_extracting = False
        self.selected_start_date = None
        self.selected_end_date = None
        
        # 当日検査品入力用の変数
        self.product_code_entry = None  # 品番入力フィールド
        self.process_name_entry = None  # 工程名入力
        self.inspectable_lots_entry = None  # 検査可能ロット数／日入力フィールド
        self.register_button = None  # 登録確定ボタン
        self.registered_products = []  # 登録された品番のリスト [{品番, ロット数}, ...]
        self.registered_products_frame = None  # 登録リスト表示フレーム
        self.registered_list_container = None  # 登録リストコンテナ
        
        # 登録済み品番リストの保存ファイルパス（exe化対応・NAS共有対応）
        if self.config.registered_products_path:
            # config.envで設定されている場合はそれを使用（NAS共有対応）
            self.registered_products_file = Path(self.config.registered_products_path)
        elif getattr(sys, 'frozen', False):
            # exe化されている場合、exeファイルと同じディレクトリに保存
            self.registered_products_file = Path(sys.executable).parent / "registered_products.json"
        else:
            # 開発環境の場合、プロジェクトルートに保存
            self.registered_products_file = Path(__file__).parent.parent.parent / "registered_products.json"

        # 抽出対象外（品番）マスタ
        self.excluded_products: List[Dict[str, str]] = []  # [{品番, メモ}, ...]
        if getattr(self.config, "extract_exclude_products_path", None):
            self.excluded_products_file = Path(self.config.extract_exclude_products_path)
        elif getattr(sys, 'frozen', False):
            self.excluded_products_file = Path(sys.executable).parent / "extract_exclude_products.json"
        else:
            self.excluded_products_file = Path(__file__).parent.parent.parent / "extract_exclude_products.json"
        
        # カレンダー用の変数初期化
        today = date.today()
        self.current_year = today.year
        self.current_month = today.month
        
        # Googleスプレッドシートエクスポーターの初期化（設定読み込み後に更新）
        self.google_sheets_exporter = None
        
        # 検査員割当てマネージャーの初期化（設定値を渡す）
        self.inspector_manager = InspectorAssignmentManager(
            log_callback=self.log_message,
            product_limit_hard_threshold=self.app_config_manager.get_product_limit_hard_threshold(),
            required_inspectors_threshold=self.app_config_manager.get_required_inspectors_threshold()
        )
        
        # 休暇情報テーブル用の変数
        self.vacation_info_frame = None
        
        # データ保存用変数
        self.current_main_data = None
        self.current_assignment_data = None
        self.current_inspector_data = None
        self._seat_chart_server = SeatChartServer()
        
        # 【追加】検査対象外ロット情報保存用
        self.non_inspection_lots_df = pd.DataFrame()
        self._non_inspection_confirm_window = None
        self._auto_open_non_inspection_window_done = False

        # 進捗表示（スレッド処理中も「止まって見えない」ように補助する）
        self._progress_value: float = 0.0
        self._progress_message: str = ""
        self._progress_monotonic_lock: bool = False
        self._progress_pulse_job = None
        self._progress_pulse_active: bool = False
        self._progress_pulse_end: float = 0.0
        self._progress_pulse_step: float = 0.001
        self._progress_pulse_interval_ms: int = 120
        # 表示側の進捗配分（内部の0.0-0.1/0.1-0.4/0.4-0.9/0.9-1.0 は維持し、見た目だけ調整）
        # 直近のperf計測結果（不足集計/抽出とロット取得が支配的、検査員フェーズは短め）に合わせたデフォルト値
        # 目安: 抽出 0.0-0.45 / ロット 0.45-0.8 / 検査員 0.8-0.9 / 表示 0.9-1.0
        # Access VBA（不足集計）が長く見えるため、抽出側を厚めにして体感整合を取る
        self._progress_display_phase_extract_end: float = 0.45
        self._progress_display_phase_lot_end: float = 0.80
        self._progress_display_phase_inspector_end: float = 0.90
        self._progress_display_mapping_enabled: bool = True
        
        # スキル表示状態管理
        self.original_inspector_data = None  # 元のデータを保持
        
        # 品番予測検索用の変数
        self.product_code_autocomplete_list = []  # 重複除去済み品番リスト
        self.autocomplete_dropdown = None  # ドロップダウンリスト
        self.autocomplete_search_job = None  # 遅延実行用のジョブID
        self.autocomplete_hide_job = None  # 非表示処理用のジョブID
        self.autocomplete_mouse_inside = False  # マウスがドロップダウンフレーム内にあるか
        self.min_search_length = 2  # 検索開始最小文字数
        self.max_display_items = 20  # 最大表示件数
        
        # マスタデータ保存用変数
        self.inspector_master_data = None
        self.skill_master_data = None
        self.inspection_target_keywords = []  # 検査対象.csvのA列の文字列リスト
        
        # マスタデータキャッシュ機能
        self.master_cache = {}
        self.cache_timestamps = {}
        self.cache_file_mtimes = {}  # ファイル更新時刻を保存（高速化）
        self.cache_ttl = timedelta(minutes=self.MASTER_CACHE_TTL_MINUTES)

        # Accessデータ取得キャッシュ
        self._access_lots_cache: Dict[Tuple[str, Tuple[str, ...], Tuple[str, ...]], pd.DataFrame] = {}
        self._access_lots_cache_timestamp: Dict[Tuple[str, Tuple[str, ...], Tuple[str, ...]], datetime] = {}

        # 在庫ロット（t_現品票履歴）のテーブル構造キャッシュ
        self._inventory_table_structure_cache = None
        self._inventory_table_structure_timestamp = None
        
        # 現在表示中のテーブル
        self.current_display_table = None
        self.inspector_column_map_for_seating: Dict[str, str] = {}
        self.seating_flow_prompt_label: Optional[ctk.CTkLabel] = None

        # メインスクロールのバインド状態
        self._main_scroll_bound = False

        # UIの構築
        self.setup_ui()
        
        # メニューバーの作成
        self.create_menu_bar()
        
        # ログ設定
        self.setup_logging()
        
        # ウィンドウのアイコンを設定（シンプルで確実な方法にリセット）
        try:
            icon_path = self._get_icon_path("appearance_sorting_system.ico")
            if icon_path and Path(icon_path).exists():
                # 方法1: iconbitmapを使用（Tkinterの標準的な方法）
                try:
                    self.root.iconbitmap(icon_path)
                    logger.debug(f"ウィンドウアイコンを設定しました（iconbitmap）: {icon_path}")
                except Exception as iconbitmap_error:
                    # 方法2: Windows APIを使用（フォールバック）
                    try:
                        import ctypes
                        hwnd = self.root.winfo_id()
                        if hwnd:
                            LR_LOADFROMFILE = 0x0010
                            IMAGE_ICON = 1
                            ICON_SMALL = 0
                            ICON_BIG = 1
                            WM_SETICON = 0x0080
                            
                            # アイコンを読み込む
                            hicon_small = ctypes.windll.user32.LoadImageW(
                                None,
                                str(icon_path),
                                IMAGE_ICON,
                                16, 16,
                                LR_LOADFROMFILE
                            )
                            hicon_big = ctypes.windll.user32.LoadImageW(
                                None,
                                str(icon_path),
                                IMAGE_ICON,
                                32, 32,
                                LR_LOADFROMFILE
                            )
                            
                            if hicon_small:
                                ctypes.windll.user32.SendMessageW(
                                    hwnd,
                                    WM_SETICON,
                                    ICON_SMALL,
                                    hicon_small
                                )
                            if hicon_big:
                                ctypes.windll.user32.SendMessageW(
                                    hwnd,
                                    WM_SETICON,
                                    ICON_BIG,
                                    hicon_big
                                )
                            logger.debug(f"ウィンドウアイコンを設定しました（Windows API）: {icon_path}")
                    except Exception as api_error:
                        logger.warning(f"アイコン設定に失敗しました: {api_error}")
            else:
                logger.debug(f"アイコンファイルが見つかりませんでした: {icon_path}")
        except Exception as e:
            logger.warning(f"ウィンドウアイコンの設定に失敗しました: {e}", exc_info=True)
        
        # 設定の読み込み
        self.load_config()
        
        # 登録済み品番リストの読み込み
        self.load_registered_products()

        # 抽出対象外（品番）マスタの読み込み
        self.load_excluded_products()
        
        # UI構築後に全画面表示を設定
        self.root.after(200, self.set_fullscreen)  # UI完全構築後に全画面表示
    
    def set_fullscreen(self):
        """全画面表示を設定"""
        try:
            self.root.state('zoomed')  # 全画面表示（Windows）
        except Exception as e:
            logger.error(f"全画面表示の設定に失敗しました: {e}")
    
    def center_window(self):
        """ウィンドウを画面中央に配置"""
        # ウィンドウサイズを明示的に指定（初期化時はwinfo_width/heightが0になるため）
        window_width = 1200
        window_height = 800
        
        # 画面サイズを取得
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        
        # 中央位置を計算
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2
        
        # ウィンドウを中央に配置
        self.root.geometry(f"{window_width}x{window_height}+{x}+{y}")
    
    def bind_main_scroll(self):
        """メイン画面のスクロールをバインド"""
        if getattr(self, "_main_scroll_bound", False):
            return

        try:
            def on_main_mousewheel(event):
                delta = event.delta
                if not delta:
                    return "break"

                base_steps = -int(delta / 120) if abs(delta) >= 120 else (-1 if delta < 0 else 1)
                scroll_steps = base_steps * 50  # スクロールを速くする
                target = getattr(self.main_scroll_frame, "_parent_canvas", self.main_scroll_frame)
                try:
                    target.yview_scroll(scroll_steps, "units")
                except AttributeError:
                    pass

                return "break"

            self.root.bind_all("<MouseWheel>", on_main_mousewheel)
            self._main_scroll_bound = True

        except Exception as e:
            logger.error(f"メインスクロールバインド中にエラーが発生しました: {str(e)}")
    
    def setup_logging(self, execution_id: str = None, use_existing_file: bool = False):
        """ログ設定
        
        Args:
            execution_id: 実行ID（指定された場合、そのIDを含むファイル名でログを作成）
            use_existing_file: Trueの場合、既存のログファイルを使用（データ抽出時の統合用）
        """
        from pathlib import Path
        from datetime import datetime
        import sys
        
        # ログディレクトリの決定（NAS共有対応）
        if self.config and self.config.log_dir_path:
            # config.envで設定されている場合はそれを使用（NAS共有対応）
            log_dir = Path(self.config.log_dir_path)
        elif getattr(sys, 'frozen', False):
            # exe化されている場合：exeファイルの場所を基準にする
            application_path = Path(sys.executable).parent
            log_dir = application_path / "logs"
        else:
            # 通常のPython実行の場合：スクリプトの場所を基準にする
            application_path = Path(__file__).parent.parent.parent
            log_dir = application_path / "logs"
        
        # ログディレクトリを作成
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # ログファイルのパス
        if use_existing_file and hasattr(self, 'current_log_file') and self.current_log_file:
            # 既存のログファイルを使用（データ抽出時の統合用）
            log_file = self.current_log_file
        elif execution_id:
            # 実行ごとにファイルを作成（日時を含む）
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = log_dir / f"app_{timestamp}_{execution_id}.log"
        else:
            # 起動時は日時付きのファイル名を使用（毎回新規作成）
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = log_dir / f"app_{timestamp}.log"
        
        # 起動時に作成されたログファイルのパスを保存（データ抽出時の統合用）
        if not hasattr(self, 'current_log_file') or not use_existing_file:
            self.current_log_file = log_file

        # 既に同一のログ構成で初期化済みなら何もしない（重複出力/ハンドラー再設定を抑制）
        try:
            perf_log_file = log_file.with_name(f"{log_file.stem}_perf.log")
            config_key = (str(log_file), str(perf_log_file))
            if getattr(self, "_logging_config_key", None) == config_key:
                return
        except Exception:
            pass

        logger.remove()  # デフォルトのハンドラーを削除
        logger.configure(extra={"channel": "-"})  # 既定の分類（ログの見やすさ向上）
        
        # GUIアプリのためコンソール出力は行わない
        
        # ログファイルの冒頭にユーザーアカウント名を記載（ファイル作成前に書き込む）
        try:
            import getpass
            # ユーザーアカウント名を取得（複数の方法を試行）
            user_name = None
            # 方法1: Windows環境変数USERNAME（最も確実）
            user_name = os.getenv('USERNAME') or os.environ.get('USERNAME')
            # 方法2: getpass.getuser()（Python標準ライブラリ）
            if not user_name:
                try:
                    user_name = getpass.getuser()
                except Exception:
                    pass
            # 方法3: USER環境変数（一部の環境で使用可能）
            if not user_name:
                user_name = os.getenv('USER') or os.environ.get('USER')
            
            # ユーザー名が取得できない場合はフォールバック
            if not user_name:
                user_name = "Unknown"
            
            import codecs

            # 文字化け防止のため、UTF-8 BOM を先頭に付与（既存ファイルも含む）
            # Windows の既定ツールで開いたときに UTF-8 として解釈されやすくする。
            if log_file.exists():
                try:
                    raw = log_file.read_bytes()
                    if raw and not raw.startswith(codecs.BOM_UTF8):
                        log_file.write_bytes(codecs.BOM_UTF8 + raw)
                except Exception:
                    pass

            # ログファイルが存在しない場合は作成し、ユーザー名を書き込む（BOM付きで作成）
            if not log_file.exists():
                with open(log_file, 'w', encoding='utf-8-sig') as f:
                    f.write(f"user : {user_name}\n")
            else:
                # 既存ファイルの場合は、既にユーザー名が記載されているかチェック
                with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                    first_line = f.readline().strip()
                    # 既にユーザー名が記載されている場合は追加しない（重複を防ぐ）
                    if not first_line.startswith('user :'):
                        # ユーザー名が記載されていない場合のみ追加
                        with open(log_file, 'r+', encoding='utf-8', errors='replace') as f2:
                            content = f2.read()
                            f2.seek(0, 0)
                            f2.write(f"user : {user_name}\n")
                            f2.write(content)
        except Exception as e:
            # ユーザー名の取得・書き込みに失敗した場合はエラーを無視（ログ機能は継続）
            pass
        
        # PERFログ出力（計測）は環境変数でOFF可能（デフォルトON）
        perf_log_enabled = str(os.getenv("PERF_LOG_ENABLED", "1")).strip().lower() not in {
            "0",
            "false",
            "off",
            "no",
        }

        def _main_filter(record):
            if perf_log_enabled:
                return True
            try:
                return record.get("extra", {}).get("channel") != "PERF"
            except Exception:
                return True

        # ファイル出力（すべてのログを1つのファイルに統一）
        # ERROR時はスタックトレースも含める
        # エラーハンドリングを改善（ログ書き込みエラーを抑制）
        # ログファイルのパスを検証（無効な文字が含まれていないことを確認）
        try:
            log_file_str = str(log_file)
            # ログファイルのパスが有効か確認
            if len(log_file_str) > 260:  # Windowsのパス長制限
                # パスが長すぎる場合は短縮
                log_file = log_dir / "app.log"
                log_file_str = str(log_file)
        except Exception:
            pass
        
        try:
            logger.add(
                log_file_str,
                level="INFO",  # INFO以上をファイルに記録
                format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[channel]} | {name}:{line} | {message}",
                rotation="5 MB",  # 5MBごとにローテーション（ログ肥大化を抑制）
                retention="30 days",  # 30日間保持
                compression="zip",  # ローテーション後に圧縮して容量削減
                encoding="utf-8",
                backtrace=True,  # ERROR時はスタックトレースを出力
                diagnose=False,  # 容量が大きくなりやすい詳細診断は抑制
                enqueue=True,  # スレッドセーフな出力
                catch=True,  # ログ出力中のエラーをキャッチ（エラーメッセージは表示されるが、アプリは継続）
                filter=_main_filter,
            )
        except Exception:
            # ログ設定に失敗した場合は無視（アプリケーションの動作を妨げない）
            pass

        logger.bind(channel="SYS").info(f"ログファイル: {log_file.absolute()}")
        try:
            self._logging_config_key = (str(log_file), str(log_file.with_name(f"{log_file.stem}_perf.log")))
        except Exception:
            pass

        # パフォーマンス計測ログ（DEBUGのみ）を別ファイルへ出力
        if perf_log_enabled:
            try:
                perf_log_file = log_file.with_name(f"{log_file.stem}_perf.log")

                def _perf_filter(record):
                    return record.get("extra", {}).get("channel") == "PERF"

                logger.add(
                    perf_log_file,
                    level="DEBUG",
                    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[channel]} | {name}:{line} | {message}",
                    rotation="1 MB",
                    retention="30 days",
                    compression="zip",
                    encoding="utf-8",
                    enqueue=True,
                    catch=True,
                    filter=_perf_filter,
                )
                logger.bind(channel="SYS").info(f"PERFログファイル: {perf_log_file.absolute()}")
            except Exception:
                # 計測ログの設定に失敗しても本処理は継続
                pass
    
    def load_config(self):
        """設定の読み込み"""
        try:
            self.config = DatabaseConfig()
            if self.config.validate_config():
                logger.bind(channel="SYS").info("設定の読み込みが完了しました")
                
                # Googleスプレッドシートエクスポーターを初期化
                if self.config.google_sheets_url and self.config.google_sheets_credentials_path:
                    try:
                        self.google_sheets_exporter = GoogleSheetsExporter(
                            sheets_url=self.config.google_sheets_url,
                            credentials_path=self.config.google_sheets_credentials_path
                        )
                        self.log_message("Googleスプレッドシートエクスポーターを初期化しました")
                    except Exception as e:
                        self.log_message(f"警告: Googleスプレッドシートエクスポーターの初期化に失敗しました: {str(e)}")
                        self.google_sheets_exporter = None
                else:
                    self.log_message("Googleスプレッドシートの設定がありません。スプレッドシートへの自動出力は無効です。")
                    self.google_sheets_exporter = None
            else:
                logger.error("設定の検証に失敗しました")
        except Exception as e:
            logger.error(f"設定の読み込みに失敗しました: {e}")
    
    def setup_ui(self):
        """UIの構築"""
        # メインスクロールフレーム
        self.main_scroll_frame = ctk.CTkScrollableFrame(self.root, fg_color="white", corner_radius=0)
        self.main_scroll_frame.pack(fill="both", expand=True, padx=0, pady=0)
        
        # タイトルセクション
        self.create_title_section(self.main_scroll_frame)
        
        
        # 日付選択セクション
        self.create_date_section(self.main_scroll_frame)
        
        # 当日検査品追加セクション
        self.create_same_day_inspection_section(self.main_scroll_frame)
        
        # ボタンセクション
        self.create_button_section(self.main_scroll_frame)
        
        # 進捗セクション
        self.create_progress_section(self.main_scroll_frame)
        
        # データ表示セクションは選択式表示のため削除
        # self.create_data_display_section(self.main_scroll_frame)
        
        # ログセクションは削除
        
        # メインスクロールをバインド
        self.bind_main_scroll()
    
    def _get_icon_path(self, icon_filename: str) -> str:
        """
        アイコンファイルのパスを解決（exe化対応）
        
        Args:
            icon_filename: アイコンファイル名
            
        Returns:
            解決されたアイコンファイルのパス
        """
        script_dir = Path(__file__).parent.parent.parent
        resolved_path = resolve_resource_path(icon_filename, base_dir=script_dir)
        
        if resolved_path != icon_filename:
            logger.debug(f"アイコンファイルを読み込みました: {resolved_path}")
        else:
            logger.debug(f"アイコンファイルが見つかりませんでした: {icon_filename}")
        
        return resolved_path
    
    def _get_image_path(self, image_filename: str) -> str:
        """
        画像ファイルのパスを解決（exe化対応）
        
        Args:
            image_filename: 画像ファイル名
            
        Returns:
            解決された画像ファイルのパス
        """
        script_dir = Path(__file__).parent.parent.parent
        return resolve_resource_path(image_filename, base_dir=script_dir)
    
    def create_title_section(self, parent):
        """タイトルセクションの作成"""
        title_frame = ctk.CTkFrame(parent, height=70, fg_color="white", corner_radius=0)
        title_frame.pack(fill="x", pady=(5, 15))  # 上部の余白を5pxに削減
        title_frame.pack_propagate(False)
        
        # タイトルと画像を中央配置するコンテナ
        title_container = ctk.CTkFrame(title_frame, fg_color="white", corner_radius=0)
        title_container.place(relx=0.5, rely=0.5, anchor="center")  # 中央配置
        
        # 画像を読み込む（存在するファイル名に修正）
        image_filename = "ChatGPT Image 2025年11月19日 13_13_22.png"
        image_path = self._get_image_path(image_filename)
        
        try:
            # 画像ファイルの存在確認（高速化のため）
            if Path(image_path).exists():
                # 画像を読み込んでリサイズ（サイズを大きく）
                pil_image = Image.open(image_path)
                # タイトルに合わせたサイズにリサイズ（高さ50pxに拡大）
                pil_image = pil_image.resize((50, 50), Image.Resampling.LANCZOS)
                ctk_image = ctk.CTkImage(light_image=pil_image, dark_image=pil_image, size=(50, 50))
                
                # 画像ラベル
                image_label = ctk.CTkLabel(
                    title_container,
                    image=ctk_image,
                    text=""  # テキストなし
                )
                image_label.pack(side="left", padx=(0, 12))  # 画像とテキストの間隔を調整
        except Exception:
            # 画像が読み込めない場合は警告を出さずに画像なしで続行（高速化）
            return
        
        # メインタイトル（サイズを大きく、中央配置）
        title_label = ctk.CTkLabel(
            title_container,
            text="外観検査振分支援システム",
            font=ctk.CTkFont(family="Yu Gothic", size=32, weight="bold"),  # 28から32に拡大
            text_color="#1E3A8A"  # 濃い青
        )
        title_label.pack(side="left", pady=0)

        
    
    def create_date_section(self, parent):
        """日付選択セクションの作成"""
        date_frame = ctk.CTkFrame(parent, fg_color="#EFF6FF", corner_radius=12)
        date_frame.pack(fill="x", pady=(0, 10), padx=20)
        
        # セクションタイトル
        date_title = ctk.CTkLabel(
            date_frame,
            text="出荷予定日選択",
            font=ctk.CTkFont(family="Yu Gothic", size=20, weight="bold"),
            text_color="#1E3A8A"
        )
        date_title.pack(pady=(10, 8))
        
        # 期間選択フレーム
        period_frame = ctk.CTkFrame(date_frame, fg_color="white", corner_radius=8, border_width=1, border_color="#DBEAFE")
        period_frame.pack(fill="x", padx=15, pady=(0, 10))
        
        # 期間選択UIを作成
        self.create_period_selector(period_frame)
    
    def create_same_day_inspection_section(self, parent):
        """当日検査品追加セクションの作成"""
        # メインフレーム
        inspection_frame = ctk.CTkFrame(parent, fg_color="#EFF6FF", corner_radius=12)
        inspection_frame.pack(fill="x", pady=(0, 10), padx=20)
        
        # セクションタイトル
        inspection_title = ctk.CTkLabel(
            inspection_frame,
            text="<追加>　当日先行検査品",
            font=ctk.CTkFont(family="Yu Gothic", size=20, weight="bold"),
            text_color="#1E3A8A"
        )
        inspection_title.pack(pady=(8, 4))
        
        # 入力フォームフレーム
        input_frame = ctk.CTkFrame(inspection_frame, fg_color="white", corner_radius=8, border_width=1, border_color="#DBEAFE")
        input_frame.pack(fill="x", padx=10, pady=(0, 10))  # 出荷予定日選択のperiod_frameと同じ余白に設定
        
        # 入力フィールドを横並びに配置するフレーム
        fields_frame = ctk.CTkFrame(input_frame, fg_color="transparent")
        fields_frame.pack(fill="x", padx=10, pady=(8, 8))  # 下部余白を追加
        
        # 品番入力セクション
        product_code_frame = ctk.CTkFrame(fields_frame, fg_color="transparent")
        product_code_frame.pack(side="left", fill="x", expand=True, padx=(0, 8))
        
        # 予測検索用のコンテナフレーム（相対位置指定のため）
        self.product_code_container = ctk.CTkFrame(product_code_frame, fg_color="transparent")
        self.product_code_container.pack(fill="x")
        
        product_code_label = ctk.CTkLabel(
            self.product_code_container,
            text="品番（製品マスタと完全一致）",
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            text_color="#374151"
        )
        product_code_label.pack(anchor="w", pady=(0, 4))
        
        self.product_code_entry = ctk.CTkEntry(
            self.product_code_container,
            placeholder_text="品番を入力（2文字以上で検索）",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=40,
            border_width=1,
            fg_color="white",
            text_color="#374151"
        )
        self.product_code_entry.pack(fill="x")
        
        # 予測検索のイベントバインディング
        self.product_code_entry.bind("<KeyRelease>", self.on_product_code_key_release)
        self.product_code_entry.bind("<FocusIn>", self.on_product_code_focus_in)
        self.product_code_entry.bind("<FocusOut>", self.on_product_code_focus_out)
        
        # 予測検索ドロップダウンフレーム（初期状態は非表示）
        self.autocomplete_dropdown = None
        
        # 品番リストの初期化（バックグラウンドで読み込み）
        self.initialize_product_code_list()
        
        # 検査可能ロット数／日入力セクション
        # 工程名入力
        process_frame = ctk.CTkFrame(fields_frame, fg_color="transparent")
        process_frame.pack(side="left", fill="x", expand=True, padx=(0, 8))

        process_label = ctk.CTkLabel(
            process_frame,
            text="工程名　※未記載の場合は仕掛の現在工程に設定される",
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            text_color="#374151"
        )
        process_label.pack(anchor="w", pady=(0, 4))

        self.process_name_entry = ctk.CTkEntry(
            process_frame,
            placeholder_text="例: 外観、顕微鏡、PG",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=40,
            border_width=1,
            fg_color="white",
            text_color="#374151"
        )
        self.process_name_entry.pack(fill="x")

        lots_frame = ctk.CTkFrame(fields_frame, fg_color="transparent")
        lots_frame.pack(side="left", fill="x", expand=True, padx=(8, 0))
        
        lots_label = ctk.CTkLabel(
            lots_frame,
            text="検査可能ロット数／日",
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            text_color="#374151"
        )
        lots_label.pack(anchor="w", pady=(0, 4))
        
        self.inspectable_lots_entry = ctk.CTkEntry(
            lots_frame,
            placeholder_text="ロット数を入力",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=40,
            border_width=1,
            fg_color="white",
            text_color="#374151"
        )
        self.inspectable_lots_entry.pack(fill="x")
        
        # 入力フィールドの変更を監視してボタンの表示/非表示を制御
        # 品番入力フィールドのイベントはon_product_code_key_releaseで処理（予測検索も含む）
        self.inspectable_lots_entry.bind("<KeyRelease>", self.check_input_fields)
        self.inspectable_lots_entry.bind("<FocusOut>", self.check_input_fields)
        
        # 登録確定ボタン（初期状態は非表示）
        self.button_frame = ctk.CTkFrame(input_frame, fg_color="transparent")
        # 初期状態ではボタンフレーム自体も非表示にする
        self.button_frame.pack_forget()
        
        self.register_button = ctk.CTkButton(
            self.button_frame,
            text="登録確定",
            command=self.register_product,
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            height=40,
            fg_color="#3B82F6",
            hover_color="#2563EB",
            text_color="white"
        )
        # 初期状態では非表示
        self.register_button.pack_forget()
        
        # 登録リスト表示フレーム
        self.registered_products_frame = ctk.CTkFrame(inspection_frame, fg_color="white", corner_radius=8, border_width=1, border_color="#DBEAFE")
        self.registered_products_frame.pack(fill="x", padx=10, pady=(8, 8))  # 上部に8pxの余白を追加
        
        # 登録リストのタイトル
        list_title = ctk.CTkLabel(
            self.registered_products_frame,
            text="登録済み品番",
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            text_color="#374151"
        )
        list_title.pack(pady=(8, 5))
        
        # 登録リストコンテナ（スクロールバーなし、リスト分のみ表示）
        self.registered_list_container = ctk.CTkFrame(
            self.registered_products_frame,
            fg_color="transparent"
        )
        self.registered_list_container.pack(fill="x", padx=10, pady=(0, 8))
        
        # 初期状態では登録リストを非表示
        self.registered_products_frame.pack_forget()
        
        # 既に読み込まれた登録済み品番があれば表示
        if self.registered_products:
            self.update_registered_list()
    
    def check_input_fields(self, event=None):
        """入力フィールドの状態をチェックして登録確定ボタンの表示/非表示を制御"""
        product_code = self.product_code_entry.get().strip()
        lots = self.inspectable_lots_entry.get().strip()
        
        # 両方のフィールドが入力されている場合はボタンを表示
        if product_code and lots:
            # ボタンフレームを表示
            self.button_frame.pack(fill="x", padx=10, pady=(0, 8))
            self.register_button.pack(pady=(5, 0))
        else:
            self.register_button.pack_forget()
            # ボタンフレームも非表示にする
            self.button_frame.pack_forget()
    
    def register_product(self):
        """品番を登録リストに追加"""
        product_code = self.product_code_entry.get().strip()
        process_name = self.process_name_entry.get().strip()
        lots = self.inspectable_lots_entry.get().strip()
        
        # 入力チェック
        if not product_code or not lots:
            return
        
        # 登録済みか確認（工程名が異なれば別項目）
        for item in self.registered_products:
            existing_code = item.get('品番', '')
            existing_process = item.get('工程名', '').strip()
            if existing_code == product_code and existing_process == process_name:
                item['品番'] = product_code
                item['ロット数'] = lots
                item['工程名'] = process_name
                if '固定検査員' not in item:
                    item['固定検査員'] = []
                self.update_registered_list()
                self.save_registered_products()
                self.product_code_entry.delete(0, "end")
                self.inspectable_lots_entry.delete(0, "end")
                self.process_name_entry.delete(0, "end")
                self.check_input_fields()
                return
        
        # 新規登録
        self.registered_products.append({
            '品番': product_code,
            'ロット数': lots,
            '工程名': process_name,
            '固定検査員': []
        })
        
        # リストとファイル更新
        self.update_registered_list()
        self.save_registered_products()
        self.product_code_entry.delete(0, "end")
        self.inspectable_lots_entry.delete(0, "end")
        self.process_name_entry.delete(0, "end")
        self.check_input_fields()

    def update_registered_list(self):
        """登録リストを更新して表示"""
        # 既存のウィジェットを削除
        for widget in self.registered_list_container.winfo_children():
            widget.destroy()
        
        # 登録がない場合は非表示
        if not self.registered_products:
            self.registered_products_frame.pack_forget()
            return
        
        # 登録リストを表示
        self.registered_products_frame.pack(fill="x", padx=10, pady=(8, 8))
        
        # 各登録項目を表示
        for idx, item in enumerate(self.registered_products):
            # 検査員情報がない場合は初期化
            if '固定検査員' not in item:
                item['固定検査員'] = []
            
            default_row_color = "#F3F4F6"
            item_frame = ctk.CTkFrame(self.registered_list_container, fg_color=default_row_color, corner_radius=6)
            item_frame.pack(fill="x", pady=(0, 4), padx=5)

            item_frame.grid_columnconfigure(0, weight=1)
            item_frame.grid_columnconfigure(1, weight=0)
            item_frame.grid_rowconfigure(0, weight=1)

            info_column = ctk.CTkFrame(item_frame, fg_color="transparent")
            info_column.grid(row=0, column=0, sticky="nsew", padx=(2, 0), pady=6)
            info_column.grid_columnconfigure(0, weight=1)
            
            # 情報表示フレーム（一行で表示）
            info_frame = ctk.CTkFrame(info_column, fg_color="transparent")
            info_frame.pack(side="left", fill="x", expand=True, padx=0, pady=0)
            
            # 一行で表示するフレーム
            single_row = ctk.CTkFrame(info_frame, fg_color="transparent")
            single_row.pack(fill="x", anchor="w")
            
            # 品番ラベル
            product_label = ctk.CTkLabel(
                single_row,
                text="品番：",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151",
                anchor="w"
            )
            product_label.pack(side="left", padx=(0, 2))
            
            # 品番の値（固定幅で位置を揃える）
            product_value = ctk.CTkLabel(
                single_row,
                text=item['品番'],
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151",
                width=150,
                anchor="w"
            )
            product_value.pack(side="left", padx=(0, 2))
            
            # 検査可能ロット数／日のラベル
            lots_label = ctk.CTkLabel(
                single_row,
                text="検査可能ロット数／日：",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151",
                anchor="w"
            )
            lots_label.pack(side="left", padx=(0, 2))
            
            # ロット数の値
            lots_value = ctk.CTkLabel(
                single_row,
                text=f"{item['ロット数']}ロット",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151",
                anchor="w"
            )
            lots_value.pack(side="left", padx=(0, 2))
            # 工程名
            process_label = ctk.CTkLabel(
                single_row,
                text="工程名",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151",
                anchor="w"
            )
            process_label.pack(side="left", padx=(20, 2))

            process_value = ctk.CTkLabel(
                single_row,
                text=item.get('工程名', '') or "未指定",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151",
                anchor="w"
            )
            process_value.pack(side="left", padx=(0, 2))

            
            # 固定検査員の表示
            fixed_inspectors_label = ctk.CTkLabel(
                single_row,
                text="固定検査員：",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151",
                anchor="w"
            )
            fixed_inspectors_label.pack(side="left", padx=(20, 2))
            
            # 固定検査員の値（3件まで表示し、残りは+N人で省略）
            visible_limit = 6
            inspectors = item['固定検査員']
            if inspectors:
                displayed_names = inspectors[:visible_limit]
                fixed_inspectors_text = ", ".join(displayed_names)
                remaining_count = len(inspectors) - len(displayed_names)
                if remaining_count > 0:
                    fixed_inspectors_text += f" +{remaining_count}人"
            else:
                fixed_inspectors_text = "未設定"
            
            fixed_inspectors_value = ctk.CTkLabel(
                single_row,
                text=fixed_inspectors_text,
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#059669" if item['固定検査員'] else "#6B7280",
                anchor="w"
            )
            fixed_inspectors_value.pack(side="left", fill="x", expand=True, padx=(0, 2))
            if len(inspectors) > visible_limit:
                fixed_inspectors_value.configure(cursor="hand2")
                fixed_inspectors_value.bind("<Button-1>", lambda event, names=inspectors: self.show_fixed_inspector_list(names))
            
            button_column = ctk.CTkFrame(item_frame, fg_color="transparent", width=220)
            button_column.grid(row=0, column=1, sticky="ne", padx=(8, 5), pady=6)
            button_column.grid_propagate(False)

            button_frame = ctk.CTkFrame(button_column, fg_color="transparent")
            button_frame.pack(anchor="e")
            button_frame.grid_columnconfigure(0, weight=1)
            button_frame.grid_columnconfigure(1, weight=1)

            inspector_button = ctk.CTkButton(
                button_frame,
                text="検査員固定",
                command=lambda idx=idx: self.fix_inspectors_for_product(idx),
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                height=32,
                width=90,
                fg_color="#10B981" if item['固定検査員'] else "#6B7280",
                hover_color="#059669" if item['固定検査員'] else "#4B5563",
                text_color="white"
            )
            inspector_button.grid(row=0, column=0, sticky="ew", padx=(0, 5))
            
            modify_button = ctk.CTkButton(
                button_frame,
                text="登録変更",
                command=lambda idx=idx: self.modify_registered_product(idx),
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                height=32,
                width=90,
                fg_color="#3B82F6",
                hover_color="#2563EB",
                text_color="white"
            )
            modify_button.grid(row=0, column=1, sticky="ew")
    
    def show_fixed_inspector_list(self, inspector_names):
        """固定検査員一覧をモーダル表示"""
        if not inspector_names:
            return

        dialog = ctk.CTkToplevel(self.root)
        dialog.title("固定検査員一覧")
        dialog.geometry("320x360")
        dialog.transient(self.root)
        dialog.grab_set()

        label = ctk.CTkLabel(
            dialog,
            text="\n".join(inspector_names),
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            text_color="#111827",
            justify="left",
            anchor="w"
        )
        label.pack(fill="both", expand=True, padx=20, pady=(20, 10))

        close_button = ctk.CTkButton(
            dialog,
            text="閉じる",
            command=dialog.destroy,
            width=100,
            height=32
        )
        close_button.pack(pady=(0, 20))

    def delete_registered_product(self, index):
        """登録された品番を削除（後方互換性のため残す）"""
        if 0 <= index < len(self.registered_products):
            self.registered_products.pop(index)
            self.update_registered_list()
            # ファイルに保存
            self.save_registered_products()
    
    def modify_registered_product(self, index):
        """登録された品番の変更ダイアログを表示（ロット数変更・削除）"""
        try:
            if index < 0 or index >= len(self.registered_products):
                return
            
            item = self.registered_products[index]
            product_number = item['品番']
            current_lots = item['ロット数']
            
            # 変更ダイアログを作成
            dialog = ctk.CTkToplevel(self.root)
            dialog.title(f"登録変更 - {product_number}")
            dialog.geometry("450x300")
            dialog.transient(self.root)
            dialog.grab_set()
            
            # タイトルラベル
            title_label = ctk.CTkLabel(
                dialog,
                text=f"品番「{product_number}」の登録変更",
                font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold")
            )
            title_label.pack(pady=15)
            
            # 現在のロット数表示
            current_label = ctk.CTkLabel(
                dialog,
                text=f"現在のロット数: {current_lots}ロット",
                font=ctk.CTkFont(family="Yu Gothic", size=14),
                text_color="#6B7280"
            )
            current_label.pack(pady=5)
            
            # ロット数入力フレーム
            lots_frame = ctk.CTkFrame(dialog, fg_color="transparent")
            lots_frame.pack(pady=20, padx=30, fill="x")
            
            lots_label = ctk.CTkLabel(
                lots_frame,
                text="新しいロット数:",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold")
            )
            lots_label.pack(side="left", padx=(0, 10))
            
            lots_entry = ctk.CTkEntry(
                lots_frame,
                placeholder_text="ロット数を入力",
                font=ctk.CTkFont(family="Yu Gothic", size=14),
                width=150
            )
            lots_entry.pack(side="left")
            lots_entry.insert(0, str(current_lots))  # 現在の値を初期値として設定
            
            # ボタンフレーム
            button_frame = ctk.CTkFrame(dialog, fg_color="transparent")
            button_frame.pack(pady=20)
            
            def update_lots():
                """ロット数を更新"""
                new_lots = lots_entry.get().strip()
                if not new_lots:
                    return
                
                # ロット数を更新
                item['ロット数'] = new_lots
                self.update_registered_list()
                self.save_registered_products()
                self.log_message(f"品番「{product_number}」のロット数を「{new_lots}ロット」に変更しました")
                dialog.destroy()
            
            def delete_product():
                """登録を削除"""
                # 確認ダイアログ
                confirm_dialog = ctk.CTkToplevel(dialog)
                confirm_dialog.title("確認")
                confirm_dialog.geometry("400x150")
                confirm_dialog.transient(dialog)
                confirm_dialog.grab_set()
                
                confirm_label = ctk.CTkLabel(
                    confirm_dialog,
                    text=f"品番「{product_number}」を登録から削除しますか？",
                    font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold")
                )
                confirm_label.pack(pady=20)
                
                confirm_button_frame = ctk.CTkFrame(confirm_dialog, fg_color="transparent")
                confirm_button_frame.pack(pady=10)
                
                def confirm_delete():
                    if 0 <= index < len(self.registered_products):
                        self.registered_products.pop(index)
                        self.update_registered_list()
                        self.save_registered_products()
                        self.log_message(f"品番「{product_number}」を登録から削除しました")
                    confirm_dialog.destroy()
                    dialog.destroy()
                
                def cancel_delete():
                    confirm_dialog.destroy()
                
                confirm_yes_button = ctk.CTkButton(
                    confirm_button_frame,
                    text="削除",
                    command=confirm_delete,
                    font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                    width=100,
                    height=32,
                    fg_color="#EF4444",
                    hover_color="#DC2626",
                    text_color="white"
                )
                confirm_yes_button.pack(side="left", padx=10)
                
                confirm_no_button = ctk.CTkButton(
                    confirm_button_frame,
                    text="キャンセル",
                    command=cancel_delete,
                    font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                    width=100,
                    height=32,
                    fg_color="#6B7280",
                    hover_color="#4B5563",
                    text_color="white"
                )
                confirm_no_button.pack(side="left", padx=10)
            
            # ロット数変更ボタン
            update_button = ctk.CTkButton(
                button_frame,
                text="ロット数変更",
                command=update_lots,
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                width=120,
                height=35,
                fg_color="#10B981",
                hover_color="#059669",
                text_color="white"
            )
            update_button.pack(side="left", padx=10)
            
            # 登録削除ボタン
            delete_button = ctk.CTkButton(
                button_frame,
                text="登録削除",
                command=delete_product,
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                width=120,
                height=35,
                fg_color="#EF4444",
                hover_color="#DC2626",
                text_color="white"
            )
            delete_button.pack(side="left", padx=10)
            
            # キャンセルボタン
            cancel_button = ctk.CTkButton(
                button_frame,
                text="キャンセル",
                command=dialog.destroy,
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                width=120,
                height=35,
                fg_color="#6B7280",
                hover_color="#4B5563",
                text_color="white"
            )
            cancel_button.pack(side="left", padx=10)
            
            # Enterキーでロット数変更
            lots_entry.bind("<Return>", lambda e: update_lots())
            lots_entry.focus_set()
            
        except Exception as e:
            logger.error(f"登録変更ダイアログの表示に失敗しました: {str(e)}")
            self.log_message(f"エラー: 登録変更ダイアログの表示に失敗しました")
    
    def fix_inspectors_for_product(self, index):
        """品番に対する検査員を固定するダイアログを表示"""
        try:
            if index < 0 or index >= len(self.registered_products):
                return
            
            item = self.registered_products[index]
            product_number = item['品番']
            
            # 検査員マスタを読み込む（キャッシュを活用）
            inspector_master_df = self.load_inspector_master_cached()
            if inspector_master_df is None or inspector_master_df.empty:
                self.log_message("エラー: 検査員マスタを読み込めません")
                return
            
            # 検査員選択ダイアログを作成
            dialog = ctk.CTkToplevel(self.root)
            dialog.title(f"検査員固定 - {product_number}")
            dialog.geometry("500x600")
            dialog.transient(self.root)
            dialog.grab_set()
            
            # ラベル
            label = ctk.CTkLabel(
                dialog,
                text=f"品番「{product_number}」の固定検査員を選択してください",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold")
            )
            label.pack(pady=10)
            
            # 現在の固定検査員を表示
            current_fixed = item.get('固定検査員', [])
            if current_fixed:
                current_label = ctk.CTkLabel(
                    dialog,
                    text=f"現在: {', '.join(current_fixed)}",
                    font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                    text_color="#6B7280"
                )
                current_label.pack(pady=5)
            
            # スクロール可能なフレーム
            scroll_frame = ctk.CTkScrollableFrame(dialog)
            scroll_frame.pack(fill="both", expand=True, padx=20, pady=10)
            
            # マウスホイールイベントのバインド（CTkScrollableFrame用）
            def on_scroll_mousewheel(event):
                # スクロール量を計算（速度を上げるため10倍にする）
                scroll_amount = int(-1 * (event.delta / 120)) * 10
                # CTkScrollableFrameの正しいスクロールメソッドを使用
                if hasattr(scroll_frame, 'yview_scroll'):
                    scroll_frame.yview_scroll(scroll_amount, "units")
                else:
                    # CTkScrollableFrameの場合は内部のCanvasを直接操作
                    canvas = scroll_frame._parent_canvas
                    if canvas:
                        canvas.yview_scroll(scroll_amount, "units")
                return "break"
            
            # スクロールフレームにマウスホイールイベントをバインド
            scroll_frame.bind("<MouseWheel>", on_scroll_mousewheel)
            # ダイアログ全体にもバインド（フォーカスが外れている場合でも動作するように）
            dialog.bind("<MouseWheel>", on_scroll_mousewheel)
            
            # 選択された検査員を保持（セットで管理）
            selected_inspectors = set(current_fixed)
            
            # 検査員リストを作成
            inspector_names = inspector_master_df['#氏名'].dropna().astype(str).str.strip()
            inspector_names = inspector_names[inspector_names != ''].unique().tolist()
            
            # 各検査員にチェックボックスを作成
            inspector_checkboxes = {}
            for inspector_name in sorted(inspector_names):
                # チェックボックスを作成
                checkbox_var = tk.BooleanVar(value=inspector_name in selected_inspectors)
                checkbox = ctk.CTkCheckBox(
                    scroll_frame,
                    text=inspector_name,
                    variable=checkbox_var,
                    command=lambda name=inspector_name, var=checkbox_var: self._update_selected_inspectors(name, var, selected_inspectors),
                    font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold")
                )
                checkbox.pack(anchor="w", pady=2)
                inspector_checkboxes[inspector_name] = checkbox_var
            
            # ボタンフレーム
            button_frame = ctk.CTkFrame(dialog, fg_color="transparent")
            button_frame.pack(pady=10)
            
            def on_ok():
                # 固定検査員を更新
                item['固定検査員'] = sorted(list(selected_inspectors))
                self.update_registered_list()
                self.save_registered_products()
                self.log_message(f"品番「{product_number}」の固定検査員を設定しました: {', '.join(item['固定検査員']) if item['固定検査員'] else 'なし'}")
                dialog.destroy()
            
            def on_cancel():
                dialog.destroy()
            
            def on_clear():
                selected_inspectors.clear()
                for var in inspector_checkboxes.values():
                    var.set(False)
            
            ok_button = ctk.CTkButton(
                button_frame,
                text="OK",
                command=on_ok,
                width=100,
                height=30
            )
            ok_button.pack(side="left", padx=5)
            
            clear_button = ctk.CTkButton(
                button_frame,
                text="クリア",
                command=on_clear,
                width=100,
                height=30,
                fg_color="#F59E0B",
                hover_color="#D97706"
            )
            clear_button.pack(side="left", padx=5)
            
            cancel_button = ctk.CTkButton(
                button_frame,
                text="キャンセル",
                command=on_cancel,
                width=100,
                height=30,
                fg_color="#6B7280",
                hover_color="#4B5563"
            )
            cancel_button.pack(side="left", padx=5)
            
            # ダイアログを中央に配置
            dialog.update_idletasks()
            x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
            y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
            dialog.geometry(f"+{x}+{y}")
            
        except Exception as e:
            self.log_message(f"検査員固定ダイアログの表示に失敗しました: {str(e)}")
            logger.error(f"検査員固定ダイアログの表示に失敗しました: {str(e)}", exc_info=True)
    
    def _update_selected_inspectors(self, name, var, selected_set):
        """選択された検査員を更新"""
        if var.get():
            selected_set.add(name)
        else:
            selected_set.discard(name)
    
    def _update_selected_inspectors_for_change(self, name, code, var, selected_dict):
        """検査員変更ダイアログ用：選択された検査員を更新（辞書形式）"""
        if var.get():
            selected_dict[name] = code
        else:
            selected_dict.pop(name, None)
    
    def _set_fixed_inspectors_to_manager(self):
        """登録済み品番の固定検査員情報をInspectorAssignmentManagerに設定"""
        try:
            if not hasattr(self, 'inspector_manager') or self.inspector_manager is None:
                return
            
            # 固定検査員情報を辞書形式で構築
            fixed_inspectors_dict: Dict[str, List[Dict[str, Any]]] = {}
            for item in self.registered_products:
                product_number = str(item.get('品番', '')).strip()
                process_name = str(item.get('工程名', '') or '').strip()
                fixed_inspectors = [
                    str(name).strip()
                    for name in item.get('固定検査員', [])
                    if name and str(name).strip()
                ]
                if not product_number or not fixed_inspectors:
                    continue
                unique_inspectors = list(dict.fromkeys(fixed_inspectors))
                fixed_inspectors_dict.setdefault(product_number, []).append({
                    'process': process_name,
                    'inspectors': unique_inspectors
                })
            
            # InspectorAssignmentManagerに設定
            self.inspector_manager.fixed_inspectors_by_product = fixed_inspectors_dict
            
            if fixed_inspectors_dict:
                self.log_message(f"固定検査員情報を設定しました: {len(fixed_inspectors_dict)}品番")
                for product, entries in fixed_inspectors_dict.items():
                    for entry in entries:
                        process_text = entry.get('process') or '全工程'
                        inspectors = entry.get('inspectors', [])
                        inspectors_text = ', '.join(inspectors)
                        self.log_message(f"  品番 '{product}' (工程: {process_text}) → {inspectors_text}")
            else:
                self.log_message("固定検査員情報は設定されていません")
                
        except Exception as e:
            self.log_message(f"固定検査員情報の設定に失敗しました: {str(e)}")
            logger.error(f"固定検査員情報の設定に失敗しました: {str(e)}", exc_info=True)
    
    def load_registered_products(self):
        """登録済み品番リストをファイルから読み込む"""
        try:
            if self.registered_products_file.exists():
                raw_bytes = self.registered_products_file.read_bytes()
                last_error: Optional[Exception] = None
                for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis"):
                    try:
                        self.registered_products = json.loads(raw_bytes.decode(enc))
                        last_error = None
                        break
                    except Exception as e:
                        last_error = e
                if last_error is not None:
                    raise last_error

                # 旧データのキー名救済（文字化け/旧バージョン互換）
                legacy_key_map = {
                    "?i??": "品番",
                    "???b?g??": "ロット数",
                    "???????": "固定検査員",
                }
                # 後方互換性: 検査員情報がない場合は初期化
                for item in self.registered_products:
                    if '固定検査員' not in item:
                        item['固定検査員'] = []

                    # 旧キーを正規キーへ移し替え
                    for legacy_key, normalized_key in legacy_key_map.items():
                        if normalized_key not in item and legacy_key in item:
                            item[normalized_key] = item[legacy_key]
                    item.setdefault('工程名', '')

                    # 廃止キー/旧キーを削除（保存時に共有マスター側もクリーンにする）
                    item.pop('same_day_priority', None)
                    for legacy_key in legacy_key_map.keys():
                        item.pop(legacy_key, None)
                # UIが構築されている場合はリストを更新
                if self.registered_list_container is not None:
                    self.update_registered_list()
                logger.bind(channel="SYS").info(f"登録済み品番リストを読み込みました: {len(self.registered_products)}件")
        except Exception as e:
            logger.error(f"登録済み品番リストの読み込みに失敗しました: {str(e)}")
            self.registered_products = []
    
    def save_registered_products(self):
        """登録済み品番リストをファイルに保存"""
        try:
            legacy_keys_to_remove = {"?i??", "???b?g??", "???????"}
            for item in self.registered_products:
                if isinstance(item, dict):
                    item.pop('same_day_priority', None)
                    for legacy_key in legacy_keys_to_remove:
                        item.pop(legacy_key, None)
            # 共有パスも含めてUTF-8(BOM)で統一して文字化けを防ぐ（Excel/メモ帳でも崩れにくい）
            tmp_path = self.registered_products_file.with_suffix(self.registered_products_file.suffix + ".tmp")
            with open(tmp_path, 'w', encoding='utf-8-sig') as f:
                json.dump(self.registered_products, f, ensure_ascii=False, indent=2)
            tmp_path.replace(self.registered_products_file)
            logger.debug(f"登録済み品番リストを保存しました: {len(self.registered_products)}件")
        except Exception as e:
            logger.error(f"登録済み品番リストの保存に失敗しました: {str(e)}")

    def _normalize_product_number(self, value: Any) -> str:
        return str(value or "").strip()

    def get_excluded_product_numbers_set(self) -> set[str]:
        """抽出対象外（品番）のセットを返す（空白は除外）。"""
        result: set[str] = set()
        for item in (self.excluded_products or []):
            if isinstance(item, dict):
                pn = self._normalize_product_number(item.get("品番", ""))
            else:
                pn = self._normalize_product_number(item)
            if pn:
                result.add(pn)
        return result

    def load_excluded_products(self) -> None:
        """抽出対象外（品番）マスタを読み込む"""
        try:
            self.excluded_products = []
            if not self.excluded_products_file:
                return

            if not self.excluded_products_file.exists():
                # 初回用に空ファイルを作成（失敗しても動作は継続）
                try:
                    self.excluded_products_file.parent.mkdir(parents=True, exist_ok=True)
                    tmp_path = self.excluded_products_file.with_suffix(self.excluded_products_file.suffix + ".tmp")
                    with open(tmp_path, "w", encoding="utf-8-sig") as f:
                        json.dump([], f, ensure_ascii=False, indent=2)
                    tmp_path.replace(self.excluded_products_file)
                except Exception:
                    pass
                return

            raw_bytes = self.excluded_products_file.read_bytes()
            payload = None
            last_error: Optional[Exception] = None
            for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis"):
                try:
                    payload = json.loads(raw_bytes.decode(enc))
                    last_error = None
                    break
                except Exception as e:
                    last_error = e
            if last_error is not None:
                raise last_error

            items: list[dict[str, str]] = []
            if isinstance(payload, list):
                for entry in payload:
                    if isinstance(entry, dict):
                        pn = self._normalize_product_number(entry.get("品番", ""))
                        memo = self._normalize_product_number(entry.get("メモ", ""))
                        if pn:
                            items.append({"品番": pn, "メモ": memo})
                    else:
                        pn = self._normalize_product_number(entry)
                        if pn:
                            items.append({"品番": pn, "メモ": ""})

            # 重複排除（順序維持）
            seen: set[str] = set()
            deduped: list[dict[str, str]] = []
            for item in items:
                pn = item.get("品番", "")
                if pn and pn not in seen:
                    seen.add(pn)
                    deduped.append(item)

            self.excluded_products = deduped
            logger.bind(channel="SYS").info(f"抽出対象外（品番）マスタを読み込みました: {len(self.excluded_products)}件")
        except Exception as e:
            logger.error(f"抽出対象外（品番）マスタの読み込みに失敗しました: {str(e)}")
            self.excluded_products = []

    def save_excluded_products(self) -> None:
        """抽出対象外（品番）マスタを保存"""
        try:
            if not self.excluded_products_file:
                return
            self.excluded_products_file.parent.mkdir(parents=True, exist_ok=True)

            # 正規化＋重複排除
            items: list[dict[str, str]] = []
            seen: set[str] = set()
            for entry in (self.excluded_products or []):
                if isinstance(entry, dict):
                    pn = self._normalize_product_number(entry.get("品番", ""))
                    memo = self._normalize_product_number(entry.get("メモ", ""))
                else:
                    pn = self._normalize_product_number(entry)
                    memo = ""
                if not pn or pn in seen:
                    continue
                seen.add(pn)
                items.append({"品番": pn, "メモ": memo})

            tmp_path = self.excluded_products_file.with_suffix(self.excluded_products_file.suffix + ".tmp")
            with open(tmp_path, "w", encoding="utf-8-sig") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
            tmp_path.replace(self.excluded_products_file)
            self.excluded_products = items
            logger.debug(f"抽出対象外（品番）マスタを保存しました: {len(self.excluded_products)}件")
        except Exception as e:
            logger.error(f"抽出対象外（品番）マスタの保存に失敗しました: {str(e)}")
    
    def create_period_selector(self, parent):
        """期間選択UIの作成"""
        # 出荷予定日ラベル
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.pack(fill="x", padx=15, pady=(8, 4))
        
        date_label = ctk.CTkLabel(
            label_frame,
            text="出荷予定日",
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            text_color="#374151"
        )
        date_label.pack(side="left")
        
        # 日付入力フレーム
        date_input_frame = ctk.CTkFrame(parent, fg_color="transparent")
        date_input_frame.pack(fill="x", padx=15, pady=(0, 8))
        
        # 開始日入力
        start_date_frame = ctk.CTkFrame(date_input_frame, fg_color="white", corner_radius=8)
        start_date_frame.pack(side="left", fill="x", expand=True, padx=(0, 5))
        
        self.start_date_entry = ctk.CTkEntry(
            start_date_frame,
            placeholder_text="YYYY/MM/DD　検査日当日を入力のこと（休暇情報取得のため）",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=40,
            border_width=1,
            fg_color="white",
            text_color="#374151"
        )
        self.start_date_entry.pack(side="left", fill="x", expand=True, padx=10, pady=5)
        
        # 開始日カレンダーボタン
        start_calendar_button = ctk.CTkButton(
            start_date_frame,
            text="📅",
            command=lambda: self.show_calendar_popup("start"),
            width=32,
            height=32,
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            fg_color="transparent",
            hover_color="#F3F4F6",
            text_color="#6B7280"
        )
        start_calendar_button.pack(side="right", padx=(0, 8), pady=5)
        
        # ～ セパレーター
        separator_label = ctk.CTkLabel(
            date_input_frame,
            text="～",
            font=ctk.CTkFont(family="Yu Gothic", size=20, weight="bold"),
            text_color="#6B7280"
        )
        separator_label.pack(side="left", padx=10)
        
        # 終了日入力
        end_date_frame = ctk.CTkFrame(date_input_frame, fg_color="white", corner_radius=8)
        end_date_frame.pack(side="right", fill="x", expand=True, padx=(5, 0))
        
        self.end_date_entry = ctk.CTkEntry(
            end_date_frame,
            placeholder_text="YYYY/MM/DD",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=40,
            border_width=1,
            fg_color="white",
            text_color="#374151"
        )
        self.end_date_entry.pack(side="left", fill="x", expand=True, padx=10, pady=5)
        
        # 終了日カレンダーボタン
        end_calendar_button = ctk.CTkButton(
            end_date_frame,
            text="📅",
            command=lambda: self.show_calendar_popup("end"),
            width=32,
            height=32,
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            fg_color="transparent",
            hover_color="#F3F4F6",
            text_color="#6B7280"
        )
        end_calendar_button.pack(side="right", padx=(0, 8), pady=5)
        
        # カレンダーポップアップ用の変数
        self.calendar_popup = None
        self.calendar_window = None
        self.current_date_type = None  # "start" or "end"
    
    def show_calendar_popup(self, date_type):
        """カレンダーポップアップを表示"""
        if self.calendar_window is not None:
            self.calendar_window.destroy()
        
        # 日付タイプを設定
        self.current_date_type = date_type
        
        # ポップアップウィンドウを作成
        self.calendar_window = ctk.CTkToplevel(self.root)
        self.calendar_window.title(f"{'開始日' if date_type == 'start' else '終了日'}を選択")
        self.calendar_window.geometry("420x580")  # コンパクトなデザインに合わせてサイズを調整
        self.calendar_window.resizable(False, False)
        
        # ウィンドウを中央に配置
        self.calendar_window.transient(self.root)
        self.calendar_window.grab_set()
        
        # カレンダーウィジェットを作成
        self.create_calendar_popup(self.calendar_window)
    
    def create_calendar_popup(self, parent):
        """カレンダーポップアップの作成"""
        # メインフレーム
        main_frame = ctk.CTkFrame(parent, fg_color="white")
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # タイトル
        title_text = f"{'開始日' if self.current_date_type == 'start' else '終了日'}を選択してください"
        title_label = ctk.CTkLabel(
            main_frame,
            text=title_text,
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),  # 20→16に縮小
            text_color="#1E3A8A"
        )
        title_label.pack(pady=(15, 10))  # パディングも縮小
        
        # カレンダーヘッダー
        header_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        header_frame.pack(fill="x", padx=15, pady=(0, 10))
        
        # 前月ボタン
        prev_button = ctk.CTkButton(
            header_frame,
            text="◀",
            width=28,  # 32→28に縮小
            height=28,  # 32→28に縮小
            font=ctk.CTkFont(family="Yu Gothic", size=10, weight="bold"),  # 14→10に縮小
            fg_color="#3B82F6",
            hover_color="#2563EB",
            command=self.prev_month_popup
        )
        prev_button.pack(side="left")
        
        # 年月表示
        self.month_year_label_popup = ctk.CTkLabel(
            header_frame,
            text=f"{self.current_year}年 {self.current_month}月",
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),  # 18→14に縮小
            text_color="#1E3A8A"
        )
        self.month_year_label_popup.pack(side="left", expand=True)
        
        # 次月ボタン
        next_button = ctk.CTkButton(
            header_frame,
            text="▶",
            width=28,  # 32→28に縮小
            height=28,  # 32→28に縮小
            font=ctk.CTkFont(family="Yu Gothic", size=10, weight="bold"),  # 14→10に縮小
            fg_color="#3B82F6",
            hover_color="#2563EB",
            command=self.next_month_popup
        )
        next_button.pack(side="right")
        
        # 今日ボタン
        today_button = ctk.CTkButton(
            header_frame,
            text="今日",
            width=40,  # 50→40に縮小
            height=28,  # 32→28に縮小
            font=ctk.CTkFont(family="Yu Gothic", size=10, weight="bold"),  # 12→10に縮小
            fg_color="#10B981",
            hover_color="#059669",
            command=self.go_to_today_popup
        )
        today_button.pack(side="right", padx=(0, 8))
        
        # 曜日ヘッダー（日曜スタート）
        weekdays = ["日", "月", "火", "水", "木", "金", "土"]
        weekday_colors = ["#DC2626", "#6B7280", "#6B7280", "#6B7280", "#6B7280", "#6B7280", "#2563EB"]  # 日曜日:赤、土曜日:青
        weekday_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        weekday_frame.pack(fill="x", padx=15, pady=(0, 5))
        
        for i, day in enumerate(weekdays):
            label = ctk.CTkLabel(
                weekday_frame,
                text=day,
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),  # 14→12に縮小
                text_color=weekday_colors[i],
                width=35  # 40→35に縮小
            )
            label.grid(row=0, column=i, padx=1)  # padx=2→1に縮小
        
        # カレンダーグリッド
        self.calendar_frame_popup = ctk.CTkFrame(main_frame, fg_color="transparent")
        self.calendar_frame_popup.pack(fill="x", padx=15, pady=(0, 15))
        
        # 選択された日付の表示
        self.selected_dates_frame_popup = ctk.CTkFrame(main_frame, fg_color="#EFF6FF", corner_radius=8)
        self.selected_dates_frame_popup.pack(fill="x", padx=15, pady=(0, 15))
        
        self.selected_dates_label_popup = ctk.CTkLabel(
            self.selected_dates_frame_popup,
            text=f"{'開始日' if self.current_date_type == 'start' else '終了日'}を選択してください",
            font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),  # 14→12に縮小
            text_color="#1E3A8A"
        )
        self.selected_dates_label_popup.pack(pady=8)  # 10→8に縮小
        
        # ボタンフレーム
        button_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        button_frame.pack(fill="x", padx=15, pady=(15, 20))
        
        # 確定ボタン
        confirm_button = ctk.CTkButton(
            button_frame,
            text="確定",
            command=self.confirm_period_selection,
            font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),  # 14→12に縮小
            width=70,  # 80→70に縮小
            height=35,  # 40→35に縮小
            fg_color="#3B82F6",
            hover_color="#2563EB",
            corner_radius=8
        )
        confirm_button.pack(side="left", padx=(0, 6))  # 8→6に縮小
        
        # キャンセルボタン
        cancel_button = ctk.CTkButton(
            button_frame,
            text="キャンセル",
            command=self.cancel_period_selection,
            font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),  # 14→12に縮小
            width=70,  # 80→70に縮小
            height=35,  # 40→35に縮小
            fg_color="#6B7280",
            hover_color="#4B5563",
            corner_radius=8
        )
        cancel_button.pack(side="right", padx=(6, 0))  # 8→6に縮小
        
        # カレンダーを更新
        self.update_calendar_popup()
    
    def prev_month_popup(self):
        """前月に移動（ポップアップ）"""
        if self.current_month == 1:
            self.current_month = 12
            self.current_year -= 1
        else:
            self.current_month -= 1
        self.update_calendar_popup()
    
    def next_month_popup(self):
        """次月に移動（ポップアップ）"""
        if self.current_month == 12:
            self.current_month = 1
            self.current_year += 1
        else:
            self.current_month += 1
        self.update_calendar_popup()
    
    def go_to_today_popup(self):
        """今日の日付に移動（ポップアップ）"""
        today = date.today()
        self.current_year = today.year
        self.current_month = today.month
        
        # 今日の日付を自動選択
        if self.current_date_type == "start":
            self.selected_start_date = today
        else:
            self.selected_end_date = today
        
        # カレンダーを更新
        self.update_calendar_popup()
        
        # 選択された日付の表示を更新
        self.update_selected_dates_display_popup()
    
    def update_calendar_popup(self):
        """カレンダーを更新（ポップアップ）"""
        # 既存のカレンダーをクリア
        for widget in self.calendar_frame_popup.winfo_children():
            widget.destroy()
        
        # 年月ラベルを更新
        self.month_year_label_popup.configure(text=f"{self.current_year}年 {self.current_month}月")
        
        # 週初めを日曜日に設定
        calendar.setfirstweekday(6)  # 6 = 日曜日
        # カレンダーを生成
        cal = calendar.monthcalendar(self.current_year, self.current_month)
        
        for week_num, week in enumerate(cal):
            for day_num, day in enumerate(week):
                if day == 0:
                    # 空のセル
                    label = ctk.CTkLabel(
                        self.calendar_frame_popup,
                        text="",
                        width=35,  # 40→35に縮小
                        height=35  # 40→35に縮小
                    )
                    label.grid(row=week_num, column=day_num, padx=1, pady=1)  # padx, pady=2→1に縮小
                else:
                    # 日付ボタン
                    # 土曜日と日曜日の色を設定
                    if day_num == 6:  # 土曜日（日曜スタートなので6番目）
                        text_color = "#2563EB"  # 青
                    elif day_num == 0:  # 日曜日（日曜スタートなので0番目）
                        text_color = "#DC2626"  # 赤
                    else:
                        text_color = "#374151"  # 通常のグレー
                    
                    button = ctk.CTkButton(
                        self.calendar_frame_popup,
                        text=str(day),
                        width=35,  # 40→35に縮小
                        height=35,  # 40→35に縮小
                        font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),  # 14→12に縮小
                        fg_color="white",
                        hover_color="#F3F4F6",
                        text_color=text_color,
                        command=lambda d=day: self.select_date_popup(d)
                    )
                    button.grid(row=week_num, column=day_num, padx=1, pady=1)  # padx, pady=2→1に縮小
                    
                    # 今日の日付をハイライト
                    today = date.today()
                    if (self.current_year == today.year and 
                        self.current_month == today.month and 
                        day == today.day):
                        button.configure(fg_color="#DBEAFE", text_color="#1E3A8A")
                    
                    # 選択された日付をハイライト
                    selected_date = date(self.current_year, self.current_month, day)
                    if (self.selected_start_date and selected_date == self.selected_start_date):
                        button.configure(fg_color="#3B82F6", text_color="white")
                    elif (self.selected_end_date and selected_date == self.selected_end_date):
                        button.configure(fg_color="#3B82F6", text_color="white")
                    elif (self.selected_start_date and self.selected_end_date and 
                          self.selected_start_date <= selected_date <= self.selected_end_date):
                        button.configure(fg_color="#93C5FD", text_color="white")
    
    def select_date_popup(self, day):
        """日付を選択（ポップアップ）"""
        selected_date = date(self.current_year, self.current_month, day)
        
        if self.current_date_type == "start":
            self.selected_start_date = selected_date
        else:
            self.selected_end_date = selected_date
        
        # 選択された日付の表示を更新
        self.update_selected_dates_display_popup()
        
        # カレンダーを更新
        self.update_calendar_popup()
    
    def update_selected_dates_display_popup(self):
        """選択された日付の表示を更新（ポップアップ）"""
        if self.current_date_type == "start" and self.selected_start_date:
            self.selected_dates_label_popup.configure(
                text=f"選択された開始日: {self.selected_start_date.strftime('%Y/%m/%d')}"
            )
        elif self.current_date_type == "end" and self.selected_end_date:
            self.selected_dates_label_popup.configure(
                text=f"選択された終了日: {self.selected_end_date.strftime('%Y/%m/%d')}"
            )
        else:
            self.selected_dates_label_popup.configure(
                text=f"{'開始日' if self.current_date_type == 'start' else '終了日'}を選択してください"
            )
    
    def confirm_period_selection(self):
        """期間選択を確定"""
        if self.current_date_type == "start" and self.selected_start_date:
            # 開始日を入力フィールドに設定
            self.start_date_entry.delete(0, "end")
            self.start_date_entry.insert(0, self.selected_start_date.strftime("%Y/%m/%d"))
            # ポップアップを閉じる
            self.calendar_window.destroy()
            self.calendar_window = None
        elif self.current_date_type == "end" and self.selected_end_date:
            # 終了日を入力フィールドに設定
            self.end_date_entry.delete(0, "end")
            self.end_date_entry.insert(0, self.selected_end_date.strftime("%Y/%m/%d"))
            # ポップアップを閉じる
            self.calendar_window.destroy()
            self.calendar_window = None
        else:
            messagebox.showwarning("警告", f"{'開始日' if self.current_date_type == 'start' else '終了日'}を選択してください")
    
    def cancel_period_selection(self):
        """期間選択をキャンセル"""
        self.calendar_window.destroy()
        self.calendar_window = None
    
    def create_calendar_widget(self, parent):
        """カレンダーウィジェットの作成"""
        # 現在の日付を取得
        today = date.today()
        self.current_year = today.year
        self.current_month = today.month
        
        # カレンダーヘッダー
        header_frame = ctk.CTkFrame(parent, fg_color="transparent")
        header_frame.pack(fill="x", padx=15, pady=(15, 10))
        
        # 前月ボタン
        prev_button = ctk.CTkButton(
            header_frame,
            text="◀",
            width=40,
            height=40,
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            fg_color="#3B82F6",
            hover_color="#2563EB",
            command=self.prev_month
        )
        prev_button.pack(side="left")
        
        # 年月表示
        self.month_year_label = ctk.CTkLabel(
            header_frame,
            text=f"{self.current_year}年 {self.current_month}月",
            font=ctk.CTkFont(family="Yu Gothic", size=18, weight="bold"),
            text_color="#1E3A8A"
        )
        self.month_year_label.pack(side="left", expand=True)
        
        # 次月ボタン
        next_button = ctk.CTkButton(
            header_frame,
            text="▶",
            width=40,
            height=40,
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            fg_color="#3B82F6",
            hover_color="#2563EB",
            command=self.next_month
        )
        next_button.pack(side="right")
        
        # 今日ボタン
        today_button = ctk.CTkButton(
            header_frame,
            text="今日",
            width=60,
            height=40,
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            fg_color="#10B981",
            hover_color="#059669",
            command=self.go_to_today
        )
        today_button.pack(side="right", padx=(0, 10))
        
        # 曜日ヘッダー
        weekdays = ["月", "火", "水", "木", "金", "土", "日"]
        weekday_colors = ["#6B7280", "#6B7280", "#6B7280", "#6B7280", "#6B7280", "#2563EB", "#DC2626"]  # 土曜日:青、日曜日:赤
        weekday_frame = ctk.CTkFrame(parent, fg_color="transparent")
        weekday_frame.pack(fill="x", padx=15, pady=(0, 5))
        
        for i, day in enumerate(weekdays):
            label = ctk.CTkLabel(
                weekday_frame,
                text=day,
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color=weekday_colors[i],
                width=40
            )
            label.grid(row=0, column=i, padx=2)
        
        # カレンダーグリッド
        self.calendar_frame = ctk.CTkFrame(parent, fg_color="transparent")
        self.calendar_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        # 選択された日付の表示
        self.selected_dates_frame = ctk.CTkFrame(parent, fg_color="#EFF6FF", corner_radius=8)
        self.selected_dates_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        self.selected_dates_label = ctk.CTkLabel(
            self.selected_dates_frame,
            text="開始日と終了日を選択してください",
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            text_color="#1E3A8A"
        )
        self.selected_dates_label.pack(pady=10)
        
        # カレンダーを更新
        self.update_calendar()
    
    def prev_month(self):
        """前月に移動"""
        if self.current_month == 1:
            self.current_month = 12
            self.current_year -= 1
        else:
            self.current_month -= 1
        self.update_calendar()
    
    def next_month(self):
        """次月に移動"""
        if self.current_month == 12:
            self.current_month = 1
            self.current_year += 1
        else:
            self.current_month += 1
        self.update_calendar()
    
    def go_to_today(self):
        """今日の日付に移動"""
        today = date.today()
        self.current_year = today.year
        self.current_month = today.month
        self.update_calendar()
    
    def update_calendar(self):
        """カレンダーを更新"""
        # 既存のカレンダーをクリア
        for widget in self.calendar_frame.winfo_children():
            widget.destroy()
        
        # 年月ラベルを更新
        self.month_year_label.configure(text=f"{self.current_year}年 {self.current_month}月")
        
        # カレンダーを生成
        cal = calendar.monthcalendar(self.current_year, self.current_month)
        
        for week_num, week in enumerate(cal):
            for day_num, day in enumerate(week):
                if day == 0:
                    # 空のセル
                    label = ctk.CTkLabel(
                        self.calendar_frame,
                        text="",
                        width=40,
                        height=40
                    )
                    label.grid(row=week_num, column=day_num, padx=2, pady=2)
                else:
                    # 日付ボタン
                    # 土曜日と日曜日の色を設定
                    if day_num == 5:  # 土曜日（月曜スタートなので5番目）
                        text_color = "#2563EB"  # 青
                    elif day_num == 6:  # 日曜日（月曜スタートなので6番目）
                        text_color = "#DC2626"  # 赤
                    else:
                        text_color = "#374151"  # 通常のグレー
                    
                    button = ctk.CTkButton(
                        self.calendar_frame,
                        text=str(day),
                        width=40,
                        height=40,
                        font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                        fg_color="white",
                        hover_color="#F3F4F6",
                        text_color=text_color,
                        command=lambda d=day: self.select_date(d)
                    )
                    button.grid(row=week_num, column=day_num, padx=2, pady=2)
                    
                    # 今日の日付をハイライト
                    today = date.today()
                    if (self.current_year == today.year and 
                        self.current_month == today.month and 
                        day == today.day):
                        button.configure(fg_color="#DBEAFE", text_color="#1E3A8A")
                    
                    # 選択された日付をハイライト
                    selected_date = date(self.current_year, self.current_month, day)
                    if (self.selected_start_date and selected_date == self.selected_start_date):
                        button.configure(fg_color="#3B82F6", text_color="white")
                    elif (self.selected_end_date and selected_date == self.selected_end_date):
                        button.configure(fg_color="#3B82F6", text_color="white")
                    elif (self.selected_start_date and self.selected_end_date and 
                          self.selected_start_date <= selected_date <= self.selected_end_date):
                        button.configure(fg_color="#93C5FD", text_color="white")
    
    def select_date(self, day):
        """日付を選択"""
        selected_date = date(self.current_year, self.current_month, day)
        
        if not self.selected_start_date:
            # 開始日を選択
            self.selected_start_date = selected_date
            self.selected_end_date = None
        elif not self.selected_end_date:
            # 終了日を選択
            if selected_date >= self.selected_start_date:
                self.selected_end_date = selected_date
            else:
                # 開始日より前の日付が選択された場合は開始日を更新
                self.selected_start_date = selected_date
                self.selected_end_date = None
        else:
            # 新しい開始日を選択
            self.selected_start_date = selected_date
            self.selected_end_date = None
        
        # 選択された日付の表示を更新
        self.update_selected_dates_display()
        
        # カレンダーを更新
        self.update_calendar()
    
    def update_selected_dates_display(self):
        """選択された日付の表示を更新"""
        if self.selected_start_date and self.selected_end_date:
            self.selected_dates_label.configure(
                text=f"選択期間: {self.selected_start_date.strftime('%Y年%m月%d日')} ～ {self.selected_end_date.strftime('%Y年%m月%d日')}"
            )
        elif self.selected_start_date:
            self.selected_dates_label.configure(
                text=f"開始日: {self.selected_start_date.strftime('%Y年%m月%d日')} (終了日を選択してください)"
            )
        else:
            self.selected_dates_label.configure(
                text="開始日と終了日を選択してください"
            )
    
    
    def create_button_section(self, parent):
        """ボタンセクションの作成"""
        button_frame = ctk.CTkFrame(parent, fg_color="white", corner_radius=0)
        button_frame.pack(fill="x", pady=(5, 5), padx=20)
        
        buttons_frame = ctk.CTkFrame(button_frame, fg_color="transparent")
        buttons_frame.pack(expand=True, fill="x", pady=5)

        # 左側のボタングループ（主要操作）
        left_buttons_frame = ctk.CTkFrame(buttons_frame, fg_color="transparent")
        left_buttons_frame.pack(side="left", expand=True, fill="x")

        # データ抽出ボタン（左側）
        self.extract_button = ctk.CTkButton(
            left_buttons_frame,
            text="データ抽出開始",
            command=self.start_extraction,
            font=ctk.CTkFont(family="Yu Gothic", size=15, weight="bold"),
            height=45,
            width=160,
            fg_color="#3B82F6",
            hover_color="#2563EB",
            corner_radius=10,
            border_width=0,
            text_color="white"
        )
        self.extract_button.pack(side="left", padx=(0, 15))
        
        # 設定リロードボタン（左側）
        self.reload_button = ctk.CTkButton(
            left_buttons_frame,
            text="設定リロード",
            command=self.reload_config,
            font=ctk.CTkFont(family="Yu Gothic", size=15, weight="bold"),
            height=45,
            width=140,
            fg_color="#6B7280",
            hover_color="#4B5563",
            corner_radius=10,
            border_width=0,
            text_color="white"
        )
        self.reload_button.pack(side="left", padx=(0, 15))
        
        # 【追加】右側のボタングループ（補助操作）
        right_buttons_frame = ctk.CTkFrame(buttons_frame, fg_color="transparent")
        right_buttons_frame.pack(side="right")
        
        # ARAICHAT送信ボタン（右側）（白基調で青と黒の配色、視認性を改善）
        self.send_araichat_button = ctk.CTkButton(
            right_buttons_frame,
            text="検査対象外ロットをARAICHATに送信",
            command=self.show_non_inspection_lots_confirmation,
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            height=45,
            width=280,
            fg_color="#FFFFFF",  # 白背景
            hover_color="#E5E7EB",  # ホバー時は薄いグレー（他のボタンと同じパターン）
            corner_radius=10,
            border_width=3,
            border_color="#3B82F6",  # 青のボーダー（太く）
            text_color="#1E3A8A"  # 濃い青のテキスト
        )
        self.send_araichat_button.pack(side="right", padx=(15, 0))
        
        # ボタンの初期状態を設定（データがない場合は無効化）
        self._update_araichat_button_state()
    
    def create_progress_section(self, parent):
        """進捗セクションの作成"""
        progress_frame = ctk.CTkFrame(parent, fg_color="#EFF6FF", corner_radius=12)
        progress_frame.pack(fill="x", pady=(0, 10), padx=20)
        
        # 進捗ラベル
        self.progress_label = ctk.CTkLabel(
            progress_frame,
            text="待機中...",
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            text_color="#1E3A8A"
        )
        self.progress_label.pack(pady=(10, 8))
        
        # プログレスバー
        self.progress_bar = ctk.CTkProgressBar(
            progress_frame,
            height=24,
            progress_color="#3B82F6",
            fg_color="#E5E7EB"
        )
        self.progress_bar.pack(fill="x", padx=20, pady=(0, 10))
        self.progress_bar.set(0)
    
    def create_data_display_section(self, parent):
        """データ表示セクションの作成"""
        data_frame = ctk.CTkFrame(parent, fg_color="#EFF6FF", corner_radius=12)
        data_frame.pack(fill="both", expand=True, pady=(0, 20), padx=20)
        
        # データ表示タイトル
        data_title = ctk.CTkLabel(
            data_frame,
            text="抽出データ",
            font=ctk.CTkFont(family="Yu Gothic", size=20, weight="bold"),
            text_color="#1E3A8A"
        )
        data_title.pack(pady=(20, 15))
        
        # データ表示エリア
        self.data_display_frame = ctk.CTkFrame(data_frame, fg_color="white", corner_radius=8)
        self.data_display_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        
        # データ表示用のテーブル（Treeview）
        self.data_tree = None
        self.create_data_table()
        
        # 初期メッセージ
        self.show_initial_message()
    
    def _set_seating_flow_prompt(self, text: Optional[str]) -> None:
        """座席操作後の案内メッセージを更新"""
        if self.seating_flow_prompt_label is None:
            return
        display_text = text or ""
        self.seating_flow_prompt_label.configure(text=display_text)

    def create_data_table(self):
        """データ表示用のテーブルを作成"""
        # テーブルとスクロールバー用のフレーム
        table_container = tk.Frame(self.data_display_frame, bg="white")
        table_container.pack(fill="both", expand=True, padx=15, pady=15)
        
        # Treeviewの作成（高さを動的に調整）
        self.data_tree = ttk.Treeview(
            table_container,
            show="headings",
            height=20  # 高さを増加
        )
        
        # スクロールバーの追加
        v_scrollbar = ttk.Scrollbar(table_container, orient="vertical", command=self.data_tree.yview)
        h_scrollbar = ttk.Scrollbar(table_container, orient="horizontal", command=self.data_tree.xview)
        
        self.data_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        # グリッド配置でスクロールバーを適切に配置
        self.data_tree.grid(row=0, column=0, sticky="nsew")
        v_scrollbar.grid(row=0, column=1, sticky="ns")
        h_scrollbar.grid(row=1, column=0, sticky="ew")
        
        # グリッドの重み設定
        table_container.grid_rowconfigure(0, weight=1)
        table_container.grid_columnconfigure(0, weight=1)
        
        # デフォルトのスクロール動作を使用（カスタムスクロールを削除）
        
        # スタイル設定
        style = ttk.Style()
        style.configure("Treeview", 
                       background="white",
                       foreground="#374151",
                       fieldbackground="white",
                       font=("MS Gothic", 10, "bold"))
        style.map("Treeview",
                 background=[('selected', '#3B82F6')],
                 foreground=[('selected', 'white')])
        
        # マイナス値用のスタイル設定
        style.configure("Treeview", 
                       background="white",
                       foreground="#374151",
                       fieldbackground="white",
                       font=("MS Gothic", 10, "bold"))
        style.configure("Treeview.Negative", 
                       background="#FEE2E2",
                       foreground="#DC2626",
                       fieldbackground="#FEE2E2",
                       font=("MS Gothic", 10, "bold"))
        
        # タグの設定
        self.data_tree.tag_configure("negative", background="#FEE2E2", foreground="#DC2626")
    
    def show_initial_message(self):
        """初期メッセージを表示"""
        if self.data_tree:
            # テーブルをクリア
            for item in self.data_tree.get_children():
                self.data_tree.delete(item)
            
            # 初期メッセージ用の列を設定
            self.data_tree["columns"] = ("message",)
            self.data_tree.column("message", width=400, anchor="center")
            self.data_tree.heading("message", text="データを抽出すると、ここに結果が表示されます。")
            
            # メッセージを挿入
            self.data_tree.insert("", "end", values=("",))
    
    # ログセクションは削除
    
    
    def show_settings_dialog(self):
        """設定ダイアログを表示"""
        dialog = ctk.CTkToplevel(self.root)
        dialog.title("割り当てルール設定")
        dialog.geometry("550x480")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # メインフレーム
        main_frame = ctk.CTkFrame(dialog)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # タイトル
        title_label = ctk.CTkLabel(
            main_frame,
            text="割り当てルール設定",
            font=ctk.CTkFont(family="Yu Gothic", size=18, weight="bold")
        )
        title_label.pack(pady=(0, 20))
        
        # 同一品番の4時間上限設定
        limit_frame = ctk.CTkFrame(main_frame)
        limit_frame.pack(fill="x", pady=10)
        
        limit_label = ctk.CTkLabel(
            limit_frame,
            text="同一品番の時間上限（時間）:",
            font=ctk.CTkFont(family="Yu Gothic", size=14)
        )
        limit_label.pack(side="left", padx=10, pady=10)
        
        limit_entry = ctk.CTkEntry(
            limit_frame,
            width=100,
            font=ctk.CTkFont(family="Yu Gothic", size=14)
        )
        limit_entry.insert(0, str(self.app_config_manager.get_product_limit_hard_threshold()))
        limit_entry.pack(side="left", padx=10, pady=10)
        
        limit_default_label = ctk.CTkLabel(
            limit_frame,
            text=f"（デフォルト: {AppConfigManager.DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD}時間）",
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            text_color="gray"
        )
        limit_default_label.pack(side="left", padx=5, pady=10)
        
        # 必要人数計算の3時間基準設定
        threshold_frame = ctk.CTkFrame(main_frame)
        threshold_frame.pack(fill="x", pady=10)
        
        threshold_label = ctk.CTkLabel(
            threshold_frame,
            text="必要人数計算の時間基準（時間）:",
            font=ctk.CTkFont(family="Yu Gothic", size=14)
        )
        threshold_label.pack(side="left", padx=10, pady=10)
        
        threshold_entry = ctk.CTkEntry(
            threshold_frame,
            width=100,
            font=ctk.CTkFont(family="Yu Gothic", size=14)
        )
        threshold_entry.insert(0, str(self.app_config_manager.get_required_inspectors_threshold()))
        threshold_entry.pack(side="left", padx=10, pady=10)
        
        threshold_default_label = ctk.CTkLabel(
            threshold_frame,
            text=f"（デフォルト: {AppConfigManager.DEFAULT_REQUIRED_INSPECTORS_THRESHOLD}時間）",
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            text_color="gray"
        )
        threshold_default_label.pack(side="left", padx=5, pady=10)
        
        # 説明ラベル
        info_label = ctk.CTkLabel(
            main_frame,
            text="※ 設定を変更した場合、次回の割り当て処理から反映されます。",
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            text_color="gray"
        )
        info_label.pack(pady=10)
        
        # ARAICHATルームID設定ボタン（管理者用）
        araichat_frame = ctk.CTkFrame(main_frame)
        araichat_frame.pack(fill="x", pady=10)
        
        araichat_label = ctk.CTkLabel(
            araichat_frame,
            text="ARAICHATルームID設定（管理者のみ）:",
            font=ctk.CTkFont(family="Yu Gothic", size=14)
        )
        araichat_label.pack(side="left", padx=10, pady=10)
        
        araichat_button = ctk.CTkButton(
            araichat_frame,
            text="設定を開く",
            command=self.show_araichat_room_settings,
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            width=120,
            fg_color="#6B7280",
            hover_color="#4B5563"
        )
        araichat_button.pack(side="left", padx=10, pady=10)
        
        # ボタンフレーム
        button_frame = ctk.CTkFrame(main_frame)
        button_frame.pack(fill="x", pady=20)
        
        def save_settings():
            """設定を保存"""
            try:
                limit_value = float(limit_entry.get())
                threshold_value = float(threshold_entry.get())
                
                if limit_value <= 0 or threshold_value <= 0:
                    messagebox.showerror("エラー", "設定値は0より大きい値である必要があります")
                    return
                
                self.app_config_manager.update_product_limit_hard_threshold(limit_value)
                self.app_config_manager.update_required_inspectors_threshold(threshold_value)
                
                # InspectorAssignmentManagerを再初期化して設定値を反映
                self.inspector_manager = InspectorAssignmentManager(
                    log_callback=self.log_message,
                    product_limit_hard_threshold=self.app_config_manager.get_product_limit_hard_threshold(),
                    required_inspectors_threshold=self.app_config_manager.get_required_inspectors_threshold()
                )
                
                messagebox.showinfo("完了", "設定を保存しました。\n次回の割り当て処理から反映されます。")
                dialog.destroy()
            except ValueError:
                messagebox.showerror("エラー", "数値を入力してください")
            except Exception as e:
                messagebox.showerror("エラー", f"設定の保存に失敗しました: {str(e)}")
        
        def reset_to_default():
            """デフォルト値にリセット"""
            result = messagebox.askyesno(
                "確認",
                "設定をデフォルト値に戻しますか？\n"
                f"同一品番の時間上限: {AppConfigManager.DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD}時間\n"
                f"必要人数計算の時間基準: {AppConfigManager.DEFAULT_REQUIRED_INSPECTORS_THRESHOLD}時間"
            )
            if result:
                self.app_config_manager.reset_to_default()
                limit_entry.delete(0, tk.END)
                limit_entry.insert(0, str(self.app_config_manager.get_product_limit_hard_threshold()))
                threshold_entry.delete(0, tk.END)
                threshold_entry.insert(0, str(self.app_config_manager.get_required_inspectors_threshold()))
                
                # InspectorAssignmentManagerを再初期化して設定値を反映
                self.inspector_manager = InspectorAssignmentManager(
                    log_callback=self.log_message,
                    product_limit_hard_threshold=self.app_config_manager.get_product_limit_hard_threshold(),
                    required_inspectors_threshold=self.app_config_manager.get_required_inspectors_threshold()
                )
                
                messagebox.showinfo("完了", "設定をデフォルト値に戻しました")
        
        save_button = ctk.CTkButton(
            button_frame,
            text="保存",
            command=save_settings,
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            width=100,
            fg_color="#3B82F6",
            hover_color="#2563EB"
        )
        save_button.pack(side="left", padx=10)
        
        reset_button = ctk.CTkButton(
            button_frame,
            text="デフォルトに戻す",
            command=reset_to_default,
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            width=140,
            fg_color="#6B7280",
            hover_color="#4B5563"
        )
        reset_button.pack(side="left", padx=10)
        
        cancel_button = ctk.CTkButton(
            button_frame,
            text="キャンセル",
            command=dialog.destroy,
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            width=100,
            fg_color="#EF4444",
            hover_color="#DC2626"
        )
        cancel_button.pack(side="right", padx=10)
    
    def show_araichat_room_settings(self):
        """ARAICHATルームID設定ダイアログ（パスワード認証付き）"""
        # パスワード認証ダイアログ
        password_dialog = ctk.CTkToplevel(self.root)
        password_dialog.title("管理者認証")
        password_dialog.geometry("400x200")
        password_dialog.transient(self.root)
        password_dialog.grab_set()
        password_dialog.resizable(False, False)
        
        # パスワードを環境変数から取得（デフォルトは"admin"）
        admin_password = os.getenv("ARAICHAT_ADMIN_PASSWORD", "admin")
        
        main_frame = ctk.CTkFrame(password_dialog)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        title_label = ctk.CTkLabel(
            main_frame,
            text="管理者認証",
            font=ctk.CTkFont(family="Yu Gothic", size=18, weight="bold")
        )
        title_label.pack(pady=(0, 20))
        
        password_label = ctk.CTkLabel(
            main_frame,
            text="パスワード:",
            font=ctk.CTkFont(family="Yu Gothic", size=14)
        )
        password_label.pack(pady=10)
        
        password_entry = ctk.CTkEntry(
            main_frame,
            width=250,
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            show="*"  # パスワードを隠す
        )
        password_entry.pack(pady=10)
        password_entry.focus()
        
        def verify_password():
            """パスワードを確認"""
            entered_password = password_entry.get()
            if entered_password == admin_password:
                password_dialog.destroy()
                self._show_araichat_room_settings_dialog()
            else:
                messagebox.showerror("認証エラー", "パスワードが正しくありません")
                password_entry.delete(0, tk.END)
                password_entry.focus()
        
        def on_enter_key(event):
            """Enterキーで認証"""
            verify_password()
        
        password_entry.bind("<Return>", on_enter_key)
        
        button_frame = ctk.CTkFrame(main_frame)
        button_frame.pack(pady=20)
        
        ok_button = ctk.CTkButton(
            button_frame,
            text="認証",
            command=verify_password,
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            width=100,
            fg_color="#3B82F6",
            hover_color="#2563EB"
        )
        ok_button.pack(side="left", padx=10)
        
        cancel_button = ctk.CTkButton(
            button_frame,
            text="キャンセル",
            command=password_dialog.destroy,
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            width=100,
            fg_color="#6B7280",
            hover_color="#4B5563"
        )
        cancel_button.pack(side="left", padx=10)
    
    def _show_araichat_room_settings_dialog(self):
        """ARAICHATルームID設定ダイアログ（認証後）"""
        dialog = ctk.CTkToplevel(self.root)
        dialog.title("ARAICHATルームID設定")
        dialog.geometry("500x300")
        dialog.transient(self.root)
        dialog.grab_set()
        dialog.resizable(False, False)
        
        # メインフレーム
        main_frame = ctk.CTkFrame(dialog)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # タイトル
        title_label = ctk.CTkLabel(
            main_frame,
            text="ARAICHATルームID設定",
            font=ctk.CTkFont(family="Yu Gothic", size=18, weight="bold")
        )
        title_label.pack(pady=(0, 20))
        
        # 現在のルームIDを読み込み
        current_room_id = "142"  # デフォルト値
        room_config_path = self._get_araichat_room_config_path()
        
        # まずconfig.envで指定されたパスを確認
        if self.config and self.config.araichat_room_config_path:
            config_env_path = Path(self.config.araichat_room_config_path)
            if config_env_path.exists():
                try:
                    with open(config_env_path, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                        current_room_id = config.get('default_room_id', '142')
                        room_config_path = str(config_env_path)  # config.envのパスを使用
                except Exception as e:
                    logger.warning(f"ARAICHATルーム設定の読み込みに失敗（config.env）: {e}")
        
        # config.envのパスが存在しない場合は、実行ファイルと同じディレクトリを確認
        if current_room_id == "142" and room_config_path and Path(room_config_path).exists():
            try:
                with open(room_config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    current_room_id = config.get('default_room_id', '142')
            except Exception as e:
                logger.warning(f"ARAICHATルーム設定の読み込みに失敗: {e}")
        
        # ルームID設定フレーム
        room_frame = ctk.CTkFrame(main_frame)
        room_frame.pack(fill="x", pady=10)
        
        room_label = ctk.CTkLabel(
            room_frame,
            text="ルームID:",
            font=ctk.CTkFont(family="Yu Gothic", size=14)
        )
        room_label.pack(side="left", padx=10, pady=10)
        
        room_entry = ctk.CTkEntry(
            room_frame,
            width=150,
            font=ctk.CTkFont(family="Yu Gothic", size=14)
        )
        room_entry.insert(0, str(current_room_id))
        room_entry.pack(side="left", padx=10, pady=10)
        
        # 説明ラベル
        info_label = ctk.CTkLabel(
            main_frame,
            text="※ すべての工程から同じルームにメッセージが送信されます。",
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            text_color="gray"
        )
        info_label.pack(pady=10)
        
        # ボタンフレーム
        button_frame = ctk.CTkFrame(main_frame)
        button_frame.pack(fill="x", pady=20)
        
        def save_room_id():
            """ルームIDを保存"""
            try:
                room_id = room_entry.get().strip()
                if not room_id:
                    messagebox.showerror("エラー", "ルームIDを入力してください")
                    return
                
                # 数値かどうか確認（オプション）
                if not room_id.isdigit():
                    result = messagebox.askyesno(
                        "確認",
                        f"ルームID '{room_id}' は数値ではありません。\nこのまま保存しますか？"
                    )
                    if not result:
                        return
                
                # 設定を保存（oneFile化を考慮して実行ファイルと同じディレクトリに保存）
                config = {
                    "default_room_id": room_id
                }
                
                # 保存先は実行ファイルと同じディレクトリ（oneFile化対応）
                save_config_path = Path(self._get_araichat_room_config_path())
                # ディレクトリが存在しない場合は作成
                save_config_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(save_config_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)
                
                logger.info(f"ARAICHATルームIDを保存しました: {room_id} (パス: {save_config_path})")
                
                # configのパスも更新（次回の送信時に新しい設定が読み込まれるように）
                if self.config:
                    self.config.araichat_room_config_path = str(save_config_path)
                
                messagebox.showinfo("完了", f"ルームIDを '{room_id}' に設定しました。\n次回の送信から反映されます。")
                dialog.destroy()
                
            except Exception as e:
                error_msg = f"ルームIDの保存に失敗しました: {str(e)}"
                logger.error(error_msg, exc_info=True)
                messagebox.showerror("エラー", error_msg)
        
        save_button = ctk.CTkButton(
            button_frame,
            text="保存",
            command=save_room_id,
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            width=100,
            fg_color="#3B82F6",
            hover_color="#2563EB"
        )
        save_button.pack(side="left", padx=10)
        
        cancel_button = ctk.CTkButton(
            button_frame,
            text="キャンセル",
            command=dialog.destroy,
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            width=100,
            fg_color="#6B7280",
            hover_color="#4B5563"
        )
        cancel_button.pack(side="right", padx=10)
    
    def _get_araichat_room_config_path(self) -> str:
        """ARAICHATルーム設定ファイルのパスを取得（oneFile化対応）"""
        # 実行ファイルのディレクトリを取得
        if getattr(sys, 'frozen', False):
            # exe化されている場合
            base_dir = Path(sys.executable).parent
        else:
            # 通常のPython実行の場合
            base_dir = Path(__file__).parent.parent.parent
        
        # 設定ファイルのパス
        config_path = base_dir / "araichat_room_config.json"
        return str(config_path)
    
    def reload_config(self):
        """設定のリロード"""
        self.log_message("設定をリロードしています...")
        self.load_config()
        
        if self.config and self.config.validate_config():
            self.log_message("設定のリロードが完了しました")
        else:
            self.log_message("設定のリロードに失敗しました")
    
    def validate_dates(self):
        """日付の妥当性を検証"""
        try:
            # 入力フィールドから日付を取得
            start_date_str = self.start_date_entry.get().strip()
            end_date_str = self.end_date_entry.get().strip()
            
            if not start_date_str or not end_date_str:
                raise ValueError("開始日と終了日を入力してください")
            
            # 日付形式を解析
            start_date = datetime.strptime(start_date_str, "%Y/%m/%d").date()
            end_date = datetime.strptime(end_date_str, "%Y/%m/%d").date()
            
            if start_date > end_date:
                raise ValueError("開始日は終了日より前である必要があります")
            
            return start_date, end_date
            
        except ValueError as e:
            messagebox.showerror("日付エラー", str(e))
            return None, None
    
    def start_extraction(self):
        """データ抽出の開始"""
        if self.is_extracting:
            messagebox.showwarning("警告", "既にデータ抽出が実行中です")
            return
        
        # 設定の確認
        if not self.config or not self.config.validate_config():
            messagebox.showerror("エラー", "データベース設定が無効です。設定を確認してください。")
            return
        
        # 日付の検証
        start_date, end_date = self.validate_dates()
        if start_date is None or end_date is None:
            return
        
        # 出力は不要（開発段階ではアプリ上に表示）
        
        # バックグラウンドでデータ抽出を実行
        self.is_extracting = True
        self._auto_open_non_inspection_window_done = False
        self._progress_monotonic_lock = True
        self._refresh_progress_display_mapping()
        self.extract_button.configure(state="disabled", text="抽出中...")
        self.progress_bar.set(0)
        self.progress_label.configure(text="データベースに接続中...")
        
        # スレッドでデータ抽出を実行
        thread = threading.Thread(
            target=self.extract_data_thread,
            args=(start_date, end_date)
        )
        thread.daemon = True
        thread.start()

    @staticmethod
    def _format_access_vba_date(value: date) -> str:
        # 例: 2025/12/24, 2026/01/31（ゼロ埋めあり）
        return f"{value.year}/{value.month:02d}/{value.day:02d}"

    def _run_access_vba_shortage_aggregate(self, start_date: date, end_date: date) -> None:
        """
        Access の VBA マクロを実行し、T_出荷予定集計 を更新する。

        実行: Application.Run("ShukkahusokuCalculate", start_date_str, end_date_str)
        """
        if os.environ.get("ACCESS_VBA_ENABLED", "1").strip() != "1":
            self.log_message("Access VBA実行は無効化されています（ACCESS_VBA_ENABLED!=1）")
            return

        access_path = os.environ.get("ACCESS_VBA_FILE_PATH", "").strip()
        macro_name = os.environ.get("ACCESS_VBA_MACRO_NAME", "ShukkahusokuCalculate").strip()
        if not access_path:
            raise RuntimeError("ACCESS_VBA_FILE_PATH が未設定です")
        if not macro_name:
            raise RuntimeError("ACCESS_VBA_MACRO_NAME が未設定です")

        # 共有上の accdb を直接更新
        start_str = self._format_access_vba_date(start_date)
        end_str = self._format_access_vba_date(end_date)

        self.log_message(f"Access VBAを実行します: {macro_name}({start_str}, {end_str})")
        self.log_message(f"Access VBA対象DB: {access_path}")

        try:
            import pythoncom  # type: ignore
            import win32com.client  # type: ignore
        except Exception as e:
            raise RuntimeError(f"Access VBAの実行に必要なモジュールが読み込めませんでした: {e}")

        access_app = None
        pythoncom.CoInitialize()
        try:
            # 既存のAccessプロセスへアタッチすると、別用途でDBが既に開かれており
            # OpenCurrentDatabase が「既にこのデータベースは開いています」で失敗することがある。
            # そのため基本は新規インスタンス（DispatchEx）を優先する。
            try:
                access_app = win32com.client.DispatchEx("Access.Application")
            except Exception:
                access_app = win32com.client.Dispatch("Access.Application")
            visible_enabled = os.environ.get("ACCESS_VBA_VISIBLE", "0").strip() == "1"
            try:
                access_app.Visible = bool(visible_enabled)
            except Exception:
                pass
            try:
                access_app.DisplayAlerts = False
            except Exception:
                pass
            # マクロ実行のセキュリティ（環境によっては存在しないため失敗は無視）
            try:
                # msoAutomationSecurityLow = 1
                access_app.AutomationSecurity = 1
            except Exception:
                pass

            opened = False
            try:
                access_app.OpenCurrentDatabase(access_path)
                opened = True
            except Exception as e:
                # 「既にこのデータベースは開いています。」は、同一インスタンス内で既に開かれている場合に発生する。
                # CloseCurrentDatabase → 再Open を試し、それでもダメなら「開いている前提」でマクロ実行を試みる。
                msg = str(e)
                if "既にこのデータベースは開いています" in msg:
                    try:
                        access_app.CloseCurrentDatabase()
                    except Exception:
                        pass
                    try:
                        access_app.OpenCurrentDatabase(access_path)
                        opened = True
                    except Exception:
                        opened = False
                else:
                    raise RuntimeError(f"Access.OpenCurrentDatabase に失敗しました: {e}") from e

            try:
                access_app.run(macro_name, start_str, end_str)
            except Exception as e:
                # COM例外の詳細をできるだけログへ（Access側のVBAエラー解析用）
                try:
                    import pywintypes  # type: ignore

                    if isinstance(e, pywintypes.com_error):
                        hresult = getattr(e, "hresult", None)
                        excepinfo = getattr(e, "excepinfo", None)
                        scode = None
                        try:
                            if excepinfo and len(excepinfo) >= 6:
                                scode = excepinfo[5]
                        except Exception:
                            scode = None

                        def _hex(val: Any) -> str:
                            try:
                                return hex(int(val) & 0xFFFFFFFF)
                            except Exception:
                                return ""

                        self.log_message(
                            "Access VBA COM例外: "
                            f"hresult={hresult}({_hex(hresult)}), scode={scode}({_hex(scode)}), excepinfo={excepinfo}",
                            level="error",
                        )

                        # DISP_E_BADPARAMCOUNT (0x8002000E) の場合は、マクロ側の引数不一致が濃厚
                        try:
                            if (int(scode) & 0xFFFFFFFF) == 0x8002000E:
                                self.log_message(
                                    "Access VBA: 引数数が一致しません（0x8002000E）。"
                                    " Access側のプロシージャが (開始日, 終了日) の2引数を受け取る Public Sub/Function になっているか、"
                                    " または ACCESS_VBA_MACRO_NAME を 'ModuleName.ShukkahusokuCalculate' 形式にする必要があります。",
                                    level="error",
                                )
                        except Exception:
                            pass
                except Exception:
                    pass
                raise RuntimeError(f"Access.Application.run に失敗しました: {e}") from e
            try:
                if opened:
                    access_app.CloseCurrentDatabase()
            except Exception:
                pass
        finally:
            try:
                if access_app is not None:
                    try:
                        access_app.Quit()
                    except Exception:
                        pass
            finally:
                pythoncom.CoUninitialize()
    
    def extract_data_thread(self, start_date, end_date):
        """データ抽出のスレッド処理"""
        connection = None
        success = False  # 成功フラグを追加
        try:
            # 既存のログファイルに追記（起動時のログファイルに統合）
            with perf_timer(logger, "logging.setup"):
                self.setup_logging(use_existing_file=True)
            
            self.log_message(f"データ抽出を開始します")
            self.log_message(f"抽出期間: {start_date} ～ {end_date}")

            # データ抽出開始直後に、Access側VBAで不足集計（T_出荷予定集計）を更新
            try:
                self.update_progress(0.005, "不足集計（Access VBA）を実行中...")
                # Accessマクロは数十秒かかることがあるため、進捗が止まって見えないようにパルス表示
                access_target_seconds = float(os.getenv("ACCESS_VBA_PROGRESS_TARGET_SECONDS", "60").strip() or "60")
                self.start_progress_pulse(
                    0.005,
                    0.06,
                    "不足集計（Access）を実行中...",
                    target_seconds=access_target_seconds,
                )
                # パルス開始を確実に反映させるため、短時間だけUIスレッドへ実行権を譲る
                try:
                    import time
                    time.sleep(0.3)
                except Exception:
                    pass
                with perf_timer(logger, "access.vba.run"):
                    self._run_access_vba_shortage_aggregate(start_date, end_date)
                    self.stop_progress_pulse(final_value=0.06, message="不足集計（Access）が完了しました")
                self.log_message("Access VBAの実行が完了しました")
            except Exception as e:
                self.log_message(f"Access VBAの実行に失敗しました: {e}", level="error")
                # 失敗時は抽出を中断（更新前提のため）
                raise
             
            # 【追加】休暇予定を取得（データ抽出開始日付を使用）
            # 進捗配分: 抽出 0.0-0.3 / ロット 0.3-0.6 / 検査員 0.6-0.9 / 表示 0.9-1.0
            self.update_progress(0.01, "休暇予定を取得中...")
            # ネットワーク状況で待ちが出るため、進捗が止まって見えないようにパルス表示
            self.start_progress_pulse(0.01, 0.02, "休暇予定を取得中...")
            from app.services.vacation_schedule_service import load_vacation_schedule, get_vacation_for_date
            from datetime import date as date_type
            
            # データ抽出開始日付を取得
            extraction_date = start_date if isinstance(start_date, date_type) else pd.to_datetime(start_date).date()
            
            # インスタンス変数として保存（休暇情報テーブル表示用）
            self.current_extraction_date = extraction_date
            
            vacation_sheets_url = os.getenv("GOOGLE_SHEETS_URL_VACATION")
            # config.pyで解決されたパスを使用（exe化対応）
            credentials_path = self.config.google_sheets_credentials_path
            
            vacation_data_for_date = {}  # 初期化
            inspector_master_df = None  # 初期化
            
            if vacation_sheets_url and credentials_path:
                try:
                    # 月全体の休暇予定を読み込む
                    with perf_timer(logger, "vacation.load"):
                        vacation_data = load_vacation_schedule(
                            sheets_url=vacation_sheets_url,
                            credentials_path=credentials_path,
                            year=extraction_date.year,
                            month=extraction_date.month
                        )
                    
                    # 対象日の休暇情報を取得
                    with perf_timer(logger, "vacation.filter_for_date"):
                        vacation_data_for_date = get_vacation_for_date(vacation_data, extraction_date)
                    
                    self.log_message(f"休暇予定を取得しました: {len(vacation_data_for_date)}名")
                    
                    # 検査員マスタを読み込む（休暇情報のフィルタリング用）
                    if inspector_master_df is None:
                        try:
                            with perf_timer(logger, "inspector_master.load_cached"):
                                inspector_master_df = self.load_inspector_master_cached()
                        except Exception as e:
                            self.log_message(f"警告: 検査員マスタの読み込みに失敗しました: {str(e)}")
                    
                    # 休暇情報テーブルを表示（検査員マスタと日付を渡す）
                    self.root.after(0, lambda vd=vacation_data_for_date, ed=extraction_date, imd=inspector_master_df: self.display_vacation_info_table(vd, ed, imd))
                except Exception as e:
                    self.log_message(f"警告: 休暇予定の取得に失敗しました: {str(e)}")
                    # エラー時も空のテーブルを表示
                    if inspector_master_df is None:
                        try:
                            inspector_master_df = self.load_inspector_master_cached()
                        except:
                            pass
                    self.root.after(0, lambda ed=extraction_date, imd=inspector_master_df: self.display_vacation_info_table({}, ed, imd))
            else:
                self.log_message("休暇予定スプレッドシートの設定がありません")
                # 設定がない場合も空のテーブルを表示
                if inspector_master_df is None:
                    try:
                        inspector_master_df = self.load_inspector_master_cached()
                    except:
                        pass
                self.root.after(0, lambda ed=extraction_date, imd=inspector_master_df: self.display_vacation_info_table({}, ed, imd))
            
            # 検査員マスタを読み込む（休暇情報のマッピング用）
            # 既に読み込まれている場合は再利用
            if inspector_master_df is None:
                try:
                    inspector_master_df = self.load_inspector_master_cached()
                except Exception as e:
                    self.log_message(f"警告: 検査員マスタの読み込みに失敗しました: {str(e)}")
            
            # 検査員割当てマネージャーに休暇情報を設定
            self.inspector_manager.set_vacation_data(
                vacation_data_for_date, 
                extraction_date,
                inspector_master_df=inspector_master_df
            )
            
            # データベース接続
            self.update_progress(0.02, "データベースに接続中...")
            # UNC→ローカルキャッシュ作成で時間がかかることがあるためパルス表示
            self.start_progress_pulse(0.02, 0.03, "データベースに接続中...")
            with perf_timer(logger, "db.connect"):
                connection = self.config.get_connection()
            self.log_message("データベース接続が完了しました")

            # 実際に参照しているAccessファイル（UNC→ローカルスナップショット含む）をログに出す
            try:
                from datetime import datetime
                from app.config import DatabaseConfig
                import os as _os

                effective_access_path = DatabaseConfig.get_last_effective_access_path() or ""
                if effective_access_path and _os.path.exists(effective_access_path):
                    stat = _os.stat(effective_access_path)
                    mtime = datetime.fromtimestamp(getattr(stat, "st_mtime", 0))
                    size = int(getattr(stat, "st_size", 0))
                    self.log_message(
                        f"Accessファイル: {effective_access_path} (更新: {mtime:%Y-%m-%d %H:%M:%S}, サイズ: {size:,} bytes)"
                    )
            except Exception:
                pass
            
            # 検査対象.csvを読み込む（キャッシュ機能を使用）
            self.update_progress(0.03, "検査対象CSVを読み込み中...")
            with perf_timer(logger, "inspection_target_csv.load_cached"):
                self.inspection_target_keywords = self.load_inspection_target_csv_cached()
            
            # テーブル構造を確認（キャッシュ機能付き・高速化）
            import time
            actual_columns = None
            
            # キャッシュが有効な場合は再利用
            if (ModernDataExtractorUI._table_structure_cache is not None and 
                ModernDataExtractorUI._table_structure_cache_timestamp is not None):
                elapsed = time.time() - ModernDataExtractorUI._table_structure_cache_timestamp
                if elapsed < ModernDataExtractorUI.TABLE_STRUCTURE_CACHE_TTL:
                    actual_columns = ModernDataExtractorUI._table_structure_cache
                    # ログ出力を削減（高速化）
            
            # キャッシュが無効な場合は取得
            if actual_columns is None:
                self.update_progress(0.04, "テーブル構造を確認中...")
                columns_query = f"SELECT TOP 1 * FROM [{self.config.access_table_name}]"
                with perf_timer(logger, "access.table_structure.read_sql"):
                    sample_df = pd.read_sql(columns_query, connection)
                
                if sample_df.empty:
                    self.log_message("テーブルにデータが見つかりません")
                    self.update_progress(1.0, "完了（データなし）")
                    success = True  # データなしも完了として扱う
                    return
                
                # 実際の列名を取得してキャッシュに保存
                actual_columns = sample_df.columns.tolist()
                ModernDataExtractorUI._table_structure_cache = actual_columns
                ModernDataExtractorUI._table_structure_cache_timestamp = time.time()
            
            # T_出荷予定集計テーブルから直接必要なデータを取得
            # 必要な列: 品番, 品名, 客先, 出荷予定日, 出荷数量, 在庫数, 不足数
            # 「処理」列と「注文ID」列は不要のため除外
            # テーブルの列名は「出荷数量」「在庫数」のため、それを使用
            required_columns = ["品番", "品名", "客先", "出荷予定日", "出荷数量", "在庫数", "不足数", "梱包完了合計"]
            available_columns = [col for col in required_columns if col in actual_columns]
            
            if not available_columns:
                # ログ出力を削減（高速化）
                available_columns = actual_columns
            
            # 利用可能な列のみでクエリを作成（SQL側で日付フィルタリングを実行して高速化）
            columns_str = ", ".join([f"[{col}]" for col in available_columns])
            
            # 出荷予定日でフィルタリングをSQL側で実行（高速化）
            if '出荷予定日' in available_columns:
                # Accessの日付形式に変換
                start_date_str = pd.to_datetime(start_date).strftime('#%Y-%m-%d#')
                end_date_str = pd.to_datetime(end_date).strftime('#%Y-%m-%d#')
                query = f"SELECT {columns_str} FROM [{self.config.access_table_name}] WHERE [出荷予定日] >= {start_date_str} AND [出荷予定日] <= {end_date_str} ORDER BY [出荷予定日]"
            else:
                query = f"SELECT {columns_str} FROM [{self.config.access_table_name}]"
            
            # データの抽出（読み込み中に進捗が止まって見えないようにパルス表示）
            self.update_progress(0.05, "データを抽出中...")
            self.start_progress_pulse(0.05, 0.07, "データを抽出中...")
            with perf_timer(logger, "access.main_query.read_sql"):
                df = pd.read_sql(query, connection)
            self.stop_progress_pulse(final_value=0.07, message=f"データ抽出完了: {len(df)}件")
            self.log_message(f"データ抽出完了: {len(df)}件")
            
            # 列名をリネーム（出荷数量→出荷数）
            if '出荷数量' in df.columns:
                df = df.rename(columns={'出荷数量': '出荷数'})

            # 抽出対象外（品番）を除外（通常品：出荷予定日抽出の結果のみ）
            try:
                excluded_products = self.get_excluded_product_numbers_set()
                if excluded_products and '品番' in df.columns:
                    before = len(df)
                    df = df[~df['品番'].fillna('').astype(str).str.strip().isin(excluded_products)].copy()
                    removed = before - len(df)
                    if removed > 0:
                        self.log_message(f"抽出対象外（品番）で除外: {removed}件（{len(excluded_products)}品番）")
            except Exception as e:
                logger.debug(f"抽出対象外（品番）の除外に失敗: {e}")
            
            # 出荷数・在庫数・不足数が存在しない場合は0を設定（後方互換性のため）
            if '出荷数' not in df.columns:
                df['出荷数'] = 0
            if '在庫数' not in df.columns:
                df['在庫数'] = 0
            if '不足数' not in df.columns:
                df['不足数'] = 0
                self.log_message("不足数列が見つかりませんでした。0を設定しました。")
            
            # 在梱包数（梱包・完了）の取得
            # 優先: T_出荷予定集計の「梱包完了合計」（不足数が既に加味済みの場合、二重計上を避けるため）
            if '梱包完了合計' in df.columns:
                df['梱包・完了'] = pd.to_numeric(df['梱包完了合計'], errors='coerce').fillna(0).astype(int)
                self.log_message("在梱包数はT_出荷予定集計の「梱包完了合計」を使用しました")
            else:
                # フォールバック: t_現品票履歴から梱包工程の数量を集計して付与
                self.update_progress(0.075, "梱包工程データを取得中...")
                with perf_timer(logger, "packaging_quantities"):
                    packaging_data = self.get_packaging_quantities(connection, df)

                # 梱包数量をメインデータに結合
                self.update_progress(0.085, "データを処理中...")
                if not packaging_data.empty and '品番' in df.columns:
                    df = df.merge(packaging_data, on='品番', how='left')
                    # 梱包数量が存在しない場合は0を設定
                    df['梱包・完了'] = df['梱包・完了'].fillna(0)
                    self.log_message(f"梱包工程データを結合しました: {len(packaging_data)}件")
                else:
                    df['梱包・完了'] = 0
                    self.log_message("梱包工程データが見つかりませんでした")
            
            # 梱包・完了を数値型に変換してから整数に変換
            df['梱包・完了'] = pd.to_numeric(df['梱包・完了'], errors='coerce').fillna(0).astype(int)
            
            # 出荷数・在庫数・不足数を数値型に変換（不足数は直接取得した値を使用）
            self.update_progress(0.095, "データを数値型に変換中...")
            if '出荷数' in df.columns:
                df['出荷数'] = pd.to_numeric(df['出荷数'], errors='coerce').fillna(0)
            if '在庫数' in df.columns:
                df['在庫数'] = pd.to_numeric(df['在庫数'], errors='coerce').fillna(0)
            if '不足数' in df.columns:
                # 不足数は直接取得した値を使用（計算しない）
                df['不足数'] = pd.to_numeric(df['不足数'], errors='coerce').fillna(0)
                self.log_message("不足数は直接取得した値を使用しました")
            else:
                df['不足数'] = 0
                self.log_message("不足数列が見つかりませんでした。0を設定しました。")
            
            # 出荷予定日をdatetime型に変換（既にSQL側でソート済み）
            if not df.empty and '出荷予定日' in df.columns:
                df['出荷予定日'] = pd.to_datetime(df['出荷予定日'], errors='coerce')
                
                # 異常な日付（例: 1677年など）をチェックして修正
                try:
                    abnormal_dates_mask = df['出荷予定日'].notna() & (
                        (df['出荷予定日'].dt.year < 1900) | (df['出荷予定日'].dt.year > 2100)
                    )
                    if abnormal_dates_mask.any():
                        abnormal_count = abnormal_dates_mask.sum()
                        self.log_message(
                            f"警告: 出荷予定日に異常な日付が {abnormal_count}件検出されました（1900年～2100年の範囲外）。"
                            "これらを無効な日付として処理します。",
                            level='warning'
                        )
                        # 異常な日付をNaTに設定
                        df.loc[abnormal_dates_mask, '出荷予定日'] = pd.NaT
                except Exception as e:
                    self.log_message(f"出荷予定日の異常値チェック中にエラーが発生しました: {e}", level='warning')

                # 指定期間に対してデータの最大日付が大きく手前の場合、Accessファイル未更新/古いコピーの可能性を通知
                try:
                    max_date = df['出荷予定日'].max()
                    end_date_dt = pd.to_datetime(end_date)
                    if pd.notna(max_date) and pd.notna(end_date_dt):
                        # 2日以上手前なら警告（夜間更新・午前更新の揺れを考慮して緩め）
                        if max_date < (end_date_dt - pd.Timedelta(days=2)):
                            self.log_message(
                                f"注意: 抽出データの出荷予定日最大値({max_date:%Y-%m-%d})が指定終了日({end_date_dt:%Y-%m-%d})より前です。"
                                "Accessファイルが未更新、または古いローカルコピーを参照している可能性があります。"
                            )
                except Exception:
                    pass
            
            # 出荷予定日からデータが無い場合でも、先行検査品と洗浄品の処理を続行
            if df is None or df.empty:
                self.log_message("指定された期間に出荷予定日からのデータが見つかりませんでした")
                self.log_message("先行検査品と洗浄品の処理を続行します...")
                # 空のDataFrameを作成（必要な列を含む）
                df = pd.DataFrame(columns=['品番', '品名', '客先', '出荷予定日', '出荷数', '在庫数', '梱包・完了', '不足数'])
            else:
                self.log_message(f"抽出完了: {len(df)}件のレコード")
            
            # データを保存（エクスポート用）
            self.current_main_data = df

            # 抽出結果の不変性チェック用（内容は出さずハッシュのみ出力）
            self._log_df_signature(
                "extract.main_df",
                df,
                sort_keys=["品番", "出荷予定日", "客先", "品名"],
            )
            # 差分（前回実行比）をログに出すためのスナップショット
            self._save_and_log_snapshot("extract.main_df", df)
             
            # 不足数がマイナスの品番に対してロット割り当てを実行
            # 出荷予定日からデータが無い場合でも、先行検査品と洗浄品の処理を実行
            self.update_progress(0.10, "ロット割り当て処理中...")
            with perf_timer(logger, "lot_assignment"):
                self.process_lot_assignment(connection, df, start_progress=0.10)
            
            # 表示（0.9-1.0）
            self.update_progress(0.95, "表示を更新中...")
            self.update_progress(1.0, "データ抽出が完了しました")
            if df.empty:
                self.log_message(f"処理完了! 出荷予定日からのデータはありませんでしたが、先行検査品と洗浄品の処理を実行しました")
            else:
                self.log_message(f"処理完了! {len(df)}件のデータを表示しました")
            
            # テーブルは選択式表示のため、自動表示しない
            # self.show_table("main")
            
            # 成功メッセージ
            extraction_count = len(df) if not df.empty else 0
            assignment_count = len(self.current_assignment_data) if self.current_assignment_data is not None else 0
            inspector_count = len(self.current_inspector_data) if self.current_inspector_data is not None else 0
            
            if df.empty:
                message = (
                    f"処理が完了しました!\n\n"
                    f"出荷予定日からのデータ: 0件\n"
                    f"ロット割り当て: {assignment_count}件\n"
                    f"検査員割振り: {inspector_count}件\n\n"
                    f"先行検査品と洗浄品の処理を実行しました。\n"
                    f"検査員割振り結果を自動表示しました"
                )
            else:
                message = (
                    f"データ抽出が完了しました!\n\n"
                    f"抽出件数: {extraction_count}件\n"
                    f"ロット割り当て: {assignment_count}件\n"
                    f"検査員割振り: {inspector_count}件\n\n"
                    f"検査員割振り結果を自動表示しました"
                )
            
            self.root.after(0, lambda msg=message: self._on_extraction_complete(msg))
            
            success = True  # 成功フラグを設定
            
        except Exception as e:
            error_msg = f"データ抽出中にエラーが発生しました: {str(e)}"
            self.log_message(f"エラー: {error_msg}")
            self.update_progress(0, "エラーが発生しました")
            
            # エラーメッセージ
            self.root.after(0, lambda: messagebox.showerror("エラー", error_msg))
            
        finally:
            # データベース接続を確実に切断
            if connection:
                try:
                    connection.close()
                    logger.debug("データベース接続をクローズしました")
                except Exception as e:
                    logger.warning(f"データベース接続のクローズでエラー: {e}")
                finally:
                    connection = None  # 参照をクリア
            
            # UIの状態をリセット（エラー時のみ）
            if not success:
                self.root.after(0, self.reset_ui_state)
            else:
                # 成功時はボタンのみ有効化（ステータスバーは維持）
                self.root.after(0, lambda: self.extract_button.configure(state="normal", text="データ抽出開始"))
                self.root.after(0, lambda: setattr(self, 'is_extracting', False))
                self.root.after(0, lambda: setattr(self, '_progress_monotonic_lock', False))
    
    def update_progress(self, value: float, message: str) -> None:
        """
        進捗の更新
        
        Args:
            value: 進捗値（0.0～1.0）
            message: 進捗メッセージ
        """
        # 明示更新が来たら、進捗パルス（疑似的な徐々に進む表示）は停止する
        self.root.after(0, self._stop_progress_pulse)
        next_value = float(value) if value is not None else 0.0
        # 抽出中は進捗が「戻る」表示を防ぐ（パルスが先行→明示更新で後退、等）
        if self._progress_monotonic_lock and next_value < self._progress_value:
            next_value = self._progress_value

        self._progress_value = next_value
        self._progress_message = str(message) if message is not None else ""
        self.root.after(0, lambda v=self._progress_value: self.progress_bar.set(self._map_progress_for_display(v)))
        self.root.after(0, lambda m=self._progress_message: self.progress_label.configure(text=m))

    def _refresh_progress_display_mapping(self) -> None:
        """
        進捗バーの表示配分を環境変数から読み込み（必要なら）更新する。

        - 既存処理の progress 値（0.0-1.0）そのものは変更せず、表示だけを線形変換する。
        - デフォルトは __init__ で設定した値（環境変数未設定時）を使う。
        """
        enabled = str(os.getenv("PROGRESS_DISPLAY_MAPPING_ENABLED", "1")).strip().lower() not in {
            "0",
            "false",
            "off",
            "no",
        }
        self._progress_display_mapping_enabled = bool(enabled)

        extract_default = float(getattr(self, "_progress_display_phase_extract_end", 0.10))
        lot_default = float(getattr(self, "_progress_display_phase_lot_end", 0.40))
        inspector_default = float(getattr(self, "_progress_display_phase_inspector_end", 0.90))

        def _f(key: str, default: float) -> float:
            raw = os.getenv(key, "")
            if raw is None or not str(raw).strip():
                return float(default)
            try:
                return float(str(raw).strip())
            except Exception:
                return float(default)

        # 表示側のフェーズ境界（0-1）。単調増加になるように補正する。
        extract_end = _f("PROGRESS_PHASE_EXTRACT_END", extract_default)
        lot_end = _f("PROGRESS_PHASE_LOT_END", lot_default)
        inspector_end = _f("PROGRESS_PHASE_INSPECTOR_END", inspector_default)

        extract_end = max(0.01, min(0.95, extract_end))
        lot_end = max(extract_end + 0.01, min(0.98, lot_end))
        inspector_end = max(lot_end + 0.01, min(0.99, inspector_end))

        self._progress_display_phase_extract_end = float(extract_end)
        self._progress_display_phase_lot_end = float(lot_end)
        self._progress_display_phase_inspector_end = float(inspector_end)

    def _map_progress_for_display(self, raw_value: float) -> float:
        """
        進捗の「表示値」への線形変換。

        raw は既存の進捗配分:
        - 抽出:   0.00-0.10
        - ロット: 0.10-0.40
        - 検査員: 0.40-0.90
        - 表示:   0.90-1.00
        """
        try:
            raw = float(raw_value)
        except Exception:
            raw = 0.0
        raw = max(0.0, min(1.0, raw))
        if not getattr(self, "_progress_display_mapping_enabled", True):
            return raw

        e_end = float(getattr(self, "_progress_display_phase_extract_end", 0.10))
        l_end = float(getattr(self, "_progress_display_phase_lot_end", 0.40))
        i_end = float(getattr(self, "_progress_display_phase_inspector_end", 0.90))

        r0, r1, r2, r3, r4 = 0.0, 0.10, 0.40, 0.90, 1.0
        d0, d1, d2, d3, d4 = 0.0, e_end, l_end, i_end, 1.0

        def _lerp(x: float, a0: float, a1: float, b0: float, b1: float) -> float:
            if a1 <= a0:
                return b1
            t = (x - a0) / (a1 - a0)
            t = max(0.0, min(1.0, t))
            return b0 + (b1 - b0) * t

        if raw <= r1:
            return _lerp(raw, r0, r1, d0, d1)
        if raw <= r2:
            return _lerp(raw, r1, r2, d1, d2)
        if raw <= r3:
            return _lerp(raw, r2, r3, d2, d3)
        return _lerp(raw, r3, r4, d3, d4)

    def start_progress_pulse(
        self,
        start_value: float,
        end_value: float,
        message: str,
        *,
        target_seconds: Optional[float] = None,
    ) -> None:
        """長時間処理中、進捗が止まって見えないように段階的に進める（UIスレッドで動作）。"""
        self.root.after(
            0,
            lambda: self._start_progress_pulse(
                float(start_value),
                float(end_value),
                str(message),
                target_seconds=target_seconds,
            ),
        )

    def stop_progress_pulse(self, final_value: Optional[float] = None, message: Optional[str] = None) -> None:
        """進捗パルスを停止し、必要なら最終値をセットする（UIスレッドで動作）。"""
        self.root.after(0, lambda: self._stop_progress_pulse(final_value=final_value, message=message))

    def _start_progress_pulse(
        self,
        start_value: float,
        end_value: float,
        message: str,
        *,
        target_seconds: Optional[float] = None,
    ) -> None:
        self._stop_progress_pulse()

        start = max(0.0, min(1.0, float(start_value)))
        end = max(0.0, min(1.0, float(end_value)))
        if end <= start:
            end = min(1.0, start + 0.02)

        # 既に進捗が進んでいる場合は巻き戻さない
        current = max(self._progress_value, start)
        # end まで到達しきらない（終端は明示更新で合わせる）
        end_cap = max(start, min(end, end - 0.002))
        # 範囲が現在値より手前の場合は「後退」しないように少し先に伸ばす
        if end_cap <= current:
            if end > current:
                end_cap = min(end, max(current + 0.01, end - 0.002))
            else:
                end_cap = min(1.0, current + 0.02)

        # target_seconds（未指定は約30秒）で end_cap 付近まで進む速度
        span = max(0.01, end_cap - current)
        try:
            seconds = float(target_seconds) if target_seconds is not None else 30.0
        except Exception:
            seconds = 30.0
        seconds = max(5.0, min(180.0, seconds))
        steps = max(50.0, (seconds * 1000.0) / float(self._progress_pulse_interval_ms))
        # target_seconds を優先して小さな step も許容（上限到達で「止まって見える」のを防ぐ）
        step = max(0.00002, span / steps)

        self._progress_pulse_active = True
        self._progress_pulse_end = end_cap
        self._progress_pulse_step = step
        self._progress_pulse_interval_ms = 120

        self._progress_value = current
        self._progress_message = message
        try:
            self.progress_bar.set(self._map_progress_for_display(current))
            self.progress_label.configure(text=message)
        except Exception:
            pass

        self._progress_pulse_job = self.root.after(self._progress_pulse_interval_ms, self._progress_pulse_tick)

    def _progress_pulse_tick(self) -> None:
        if not self._progress_pulse_active:
            return

        next_value = min(self._progress_value + self._progress_pulse_step, self._progress_pulse_end)
        if next_value <= self._progress_value:
            next_value = min(self._progress_pulse_end, self._progress_value + 0.0003)

        self._progress_value = next_value
        try:
            self.progress_bar.set(self._map_progress_for_display(next_value))
        except Exception:
            pass

        if self._progress_value >= self._progress_pulse_end:
            # end_cap に到達したら少し待機（明示更新待ち）
            self._progress_pulse_job = self.root.after(300, self._progress_pulse_tick)
            return

        self._progress_pulse_job = self.root.after(self._progress_pulse_interval_ms, self._progress_pulse_tick)

    def _stop_progress_pulse(self, final_value: Optional[float] = None, message: Optional[str] = None) -> None:
        self._progress_pulse_active = False
        if self._progress_pulse_job is not None:
            try:
                self.root.after_cancel(self._progress_pulse_job)
            except Exception:
                pass
            self._progress_pulse_job = None

        if final_value is not None or message is not None:
            if final_value is not None:
                self._progress_value = max(0.0, min(1.0, float(final_value)))
                try:
                    self.progress_bar.set(self._map_progress_for_display(self._progress_value))
                except Exception:
                    pass
            if message is not None:
                self._progress_message = str(message)
                try:
                    self.progress_label.configure(text=self._progress_message)
                except Exception:
                    pass

    def _on_extraction_complete(self, message: str) -> None:
        """抽出完了の通知を表示する。"""
        try:
            messagebox.showinfo("完了", message)
        except Exception:
            pass
    
    def log_message(self, message: str, level: str = "info", channel: str = "UI") -> None:
        """
        ログメッセージの追加（loguruのみ使用）
        
        Args:
            message: ログメッセージ
        """
        # print文を削除してloguruのみを使用（高速化）
        level_normalized = (level or "info").lower().strip()
        loguru_logger = logger.bind(channel=channel) if channel else logger
        if level_normalized == "warning":
            loguru_logger.warning(message)
        elif level_normalized == "error":
            loguru_logger.error(message)
        elif level_normalized == "debug":
            loguru_logger.debug(message)
        else:
            loguru_logger.info(message)
    
    def calculate_column_widths(self, df, columns, min_width=0, max_width=600):
        """
        データに基づいて列幅を自動計算
        
        Args:
            df: DataFrame（Excel出力時の全データを使用）
            columns: 対象となる列名のリスト
            min_width: 最小列幅（ピクセル、デフォルトは0でデータに合わせる）
            max_width: 最大列幅（ピクセル）
        
        Returns:
            dict: 列名をキー、列幅を値とする辞書
        """
        column_widths = {}
        
        for col in columns:
            if col not in df.columns:
                # 列が存在しない場合はデフォルト値を使用
                column_widths[col] = 100
                continue
            
            # ヘッダーの実際の文字幅を測定（日本語文字は幅が広い）
            header_str = str(col)
            header_effective_width = 0
            for char in header_str:
                if ord(char) > 127:  # 日本語文字
                    header_effective_width += 2
                else:  # 英数字・記号
                    header_effective_width += 1
            
            # データの最大実効幅を計算
            max_effective_width = header_effective_width
            for value in df[col]:
                if pd.notna(value):
                    value_str = str(value)
                    effective_width = 0
                    for char in value_str:
                        if ord(char) > 127:  # 日本語文字
                            effective_width += 2
                        else:  # 英数字・記号
                            effective_width += 1
                    max_effective_width = max(max_effective_width, effective_width)
            
            # 列幅を計算（余白を最小限に）
            # 1文字あたり約6.5ピクセル + 最小余白8ピクセル
            # 実際のTreeviewでの表示を考慮して、少し余裕を持たせる
            column_width = max_effective_width * 6.5 + 8
            
            # 最小幅と最大幅を設定（min_widthが0の場合はデータに合わせる）
            if min_width > 0:
                column_width = max(min_width, min(column_width, max_width))
            else:
                column_width = min(column_width, max_width)
            
            column_widths[col] = int(column_width)
        
        return column_widths
    
    def configure_table_style(self, tree, style_name="Modern.Treeview"):
        """
        テーブルのスタイルを統一して設定
        
        Args:
            tree: ttk.Treeviewインスタンス
            style_name: スタイル名
        """
        style = ttk.Style()
        
        # 基本スタイル設定
        style.configure(
            style_name,
            background="#FFFFFF",
            foreground="#1F2937",
            fieldbackground="#FFFFFF",
            font=("Yu Gothic UI", 10),
            rowheight=30,  # 行の高さを少し増やして見やすく
            borderwidth=0,
            relief="flat"
        )
        
        # ヘッダースタイルはデフォルトの設定を使用（元の設定に戻す）
        
        # 選択時のスタイル
        style.map(
            style_name,
            background=[('selected', '#3B82F6')],
            foreground=[('selected', '#FFFFFF')]
        )
        
        # スタイルを適用
        tree.configure(style=style_name)
    
    def apply_striped_rows(self, tree, even_color="#F9FAFB", odd_color="#FFFFFF"):
        """
        交互の行色を適用（ストライプ表示）
        
        Args:
            tree: ttk.Treeviewインスタンス
            even_color: 偶数行の背景色
            odd_color: 奇数行の背景色
        """
        # タグ設定
        tree.tag_configure("even", background=even_color)
        tree.tag_configure("odd", background=odd_color)
        
        # 既存のアイテムにタグを適用
        children = tree.get_children()
        for idx, item in enumerate(children):
            tag = "even" if idx % 2 == 0 else "odd"
            current_tags = list(tree.item(item, "tags"))
            # 既存のタグを保持しつつ追加
            if "negative" not in current_tags:
                tree.item(item, tags=(tag,))
            else:
                # negativeタグがある場合は両方適用
                tree.item(item, tags=(tag, "negative"))
    
    def display_data(self, df):
        """データをテーブル形式で表示"""
        try:
            # 既存のテーブルセクションを削除
            self.hide_current_table()
            
            # 抽出データセクションを作成
            data_frame = ctk.CTkFrame(self.main_scroll_frame, fg_color="#EFF6FF", corner_radius=12)
            data_frame.table_section = True
            data_frame.pack(fill="x", padx=20, pady=(10, 20))
            
            # タイトル
            data_title = ctk.CTkLabel(
                data_frame,
                text="抽出データ",
                font=ctk.CTkFont(family="Yu Gothic", size=18, weight="bold"),
                text_color="#1E3A8A"
            )
            data_title.pack(pady=(15, 10))
            
            # テーブルフレーム
            table_frame = ctk.CTkFrame(data_frame, fg_color="white", corner_radius=8)
            table_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
            
            # テーブルとスクロールバー用のフレーム
            table_container = tk.Frame(table_frame)
            table_container.pack(fill="both", expand=True, padx=10, pady=10)
            
            # Treeviewの作成
            data_tree = ttk.Treeview(table_container, show="headings", height=20)
            
            # スタイルを適用
            self.configure_table_style(data_tree, "Data.Treeview")
            
            # スクロールバーの追加
            v_scrollbar = ttk.Scrollbar(table_container, orient="vertical", command=data_tree.yview)
            h_scrollbar = ttk.Scrollbar(table_container, orient="horizontal", command=data_tree.xview)
            
            data_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
            
            # グリッド配置
            data_tree.grid(row=0, column=0, sticky="nsew")
            v_scrollbar.grid(row=0, column=1, sticky="ns")
            h_scrollbar.grid(row=1, column=0, sticky="ew")
            
            # グリッドの重み設定
            table_container.grid_rowconfigure(0, weight=1)
            table_container.grid_columnconfigure(0, weight=1)
            
            # テーブルをクリア
            for item in data_tree.get_children():
                data_tree.delete(item)
            
            # 列の設定
            columns = df.columns.tolist()
            data_tree["columns"] = columns
            
            # 列幅を自動計算（Excel出力時の全データを使用）
            # current_main_dataが存在する場合はそれを使用、ない場合は表示用のdfを使用
            width_df = self.current_main_data if self.current_main_data is not None and not self.current_main_data.empty else df
            column_widths = self.calculate_column_widths(width_df, columns)
            
            # 右詰めにする数値列
            numeric_columns = ["出荷数", "在庫数", "梱包・完了", "不足数"]
            
            for col in columns:
                width = column_widths.get(col, 120)
                # 数値列は右詰め、その他は左詰め
                anchor = "e" if col in numeric_columns else "w"
                data_tree.column(col, width=width, anchor=anchor)
                data_tree.heading(col, text=col, anchor="center")
            
            # データの挿入（最初の100件まで）
            display_limit = min(100, len(df))
            row_index = 0
            # 列名から列インデックスへのマッピングを作成（高速化：itertuples()を使用）
            col_idx_map = {col: df.head(display_limit).columns.get_loc(col) for col in columns}
            
            for row_tuple in df.head(display_limit).itertuples(index=True):
                index = row_tuple[0]  # インデックス
                row = df.loc[index] if index in df.index else pd.Series(dtype=object)
                values = []
                item_id = None
                for col in columns:
                    col_idx = col_idx_map[col]
                    # itertuples(index=True)では、row_tuple[0]がインデックス、row_tuple[1]以降が列の値
                    # 列インデックスは0始まりなので、col_idx + 1でアクセス
                    if col_idx + 1 < len(row_tuple):
                        col_value = row_tuple[col_idx + 1]
                    else:
                        col_value = None
                    if pd.notna(col_value):
                        # 出荷予定日の場合は日付形式で表示
                        if col == '出荷予定日':
                            try:
                                date_value = pd.to_datetime(col_value)
                                values.append(date_value.strftime('%Y/%m/%d'))
                            except:
                                values.append(str(col_value))
                        # 数値列は整数で表示
                        elif col in ['出荷数', '在庫数', '梱包・完了', '不足数']:
                            try:
                                values.append(str(int(col_value)))
                            except:
                                values.append(str(col_value))
                        else:
                            values.append(str(col_value))
                    else:
                        values.append("")
                
                # 行のタグを決定（交互色 + マイナス値の場合は警告色）
                tags = []
                
                # 不足数がマイナスの場合は警告タグを追加（交互色は適用しない）
                is_negative = False
                if '不足数' in columns:
                    shortage_idx = col_idx_map['不足数']
                    shortage_value = row_tuple[shortage_idx + 1] if shortage_idx < len(row_tuple) - 1 else None
                    if pd.notna(shortage_value):
                        try:
                            shortage = float(shortage_value)
                            if shortage < 0:
                                tags.append("negative")
                                is_negative = True
                        except:
                            pass
                
                # 不足数がマイナスでない場合のみ交互色を適用
                if not is_negative:
                    tag = "even" if row_index % 2 == 0 else "odd"
                    tags.append(tag)
                
                # データを挿入
                item_id = data_tree.insert("", "end", values=values, tags=tuple(tags))
                
                # 不足数がマイナスの場合は値を更新
                if '不足数' in columns and pd.notna(row['不足数']):
                    try:
                        shortage = float(row['不足数'])
                        if shortage < 0:
                            data_tree.set(item_id, '不足数', str(int(shortage)))
                    except:
                        pass
                
                row_index += 1
            
            # 件数制限の表示
            if len(df) > 100:
                tag = "even" if row_index % 2 == 0 else "odd"
                data_tree.insert("", "end", values=["... 他 " + str(len(df) - 100) + "件のデータがあります"] + [""] * (len(columns) - 1), tags=(tag,))
            
            # タグの設定（交互行色と警告色）
            data_tree.tag_configure("even", background="#F9FAFB")
            data_tree.tag_configure("odd", background="#FFFFFF")
            data_tree.tag_configure("negative", background="#FEE2E2", foreground="#DC2626")
            
            # マウスホイールイベントのバインド
            def on_data_mousewheel(event):
                data_tree.yview_scroll(int(-1 * (event.delta / 120)), "units")
                return "break"
            
            data_tree.bind("<MouseWheel>", on_data_mousewheel)
            
            # テーブルに入ったときと出たときのイベント（精度向上のため、コンテナフレームにも追加）
            # 注意: unbind_allは使わず、テーブル専用のスクロールを優先的に処理
            def on_data_enter(event):
                # テーブル内ではテーブルのスクロールを優先（メインスクロールは無効化しない）
                pass
            
            def on_data_leave(event):
                # テーブルから出たときはメインスクロールを再バインド（念のため）
                self.bind_main_scroll()
            
            data_tree.bind("<Enter>", on_data_enter)
            data_tree.bind("<Leave>", on_data_leave)
            table_container.bind("<Enter>", on_data_enter)
            table_container.bind("<Leave>", on_data_leave)
            
            # テーブルの先頭にスクロール
            if data_tree.get_children():
                data_tree.see(data_tree.get_children()[0])
            
        except Exception as e:
            error_msg = f"データ表示中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
    
    def get_packaging_quantities(self, connection, main_df):
        """t_現品票履歴から梱包工程の数量を取得"""
        try:
            # メインデータから品番リストを取得
            if '品番' not in main_df.columns or main_df.empty:
                self.log_message("品番列が見つからないか、データが空です")
                return pd.DataFrame()
            
            product_numbers = sorted({
                str(pn).strip()
                for pn in main_df['品番'].dropna().unique().tolist()
                if str(pn).strip()
            })
            if not product_numbers:
                self.log_message("品番データが見つかりません")
                return pd.DataFrame()
            
            self.log_message(f"梱包工程データを検索中: {len(product_numbers)}件の品番")

            cache_key = self._build_access_cache_key(
                "packaging",
                product_numbers,
                ["%梱包%"],
            )
            cached_packaging = self._try_get_access_cache(cache_key)
            if cached_packaging is not None:
                self.log_message("Accessの梱包工程データをキャッシュから再利用しました")
                return cached_packaging

            # ローカルディスクキャッシュ（実行を跨いで再利用）
            # Accessファイルの更新状態（mtime/size）に紐づけているため、結果は不変のまま高速化できる。
            disk_cache_path = None
            try:
                from app.config import DatabaseConfig
                import hashlib
                import os
                import pickle
                from pathlib import Path

                effective_access_path = DatabaseConfig.get_last_effective_access_path() or ""
                if effective_access_path:
                    stat = os.stat(effective_access_path)
                    sig = f"{int(getattr(stat, 'st_mtime', 0))}_{int(getattr(stat, 'st_size', 0))}"

                    base_dir = os.getenv("LOCALAPPDATA", "").strip() or os.getenv("TEMP", "").strip()
                    if base_dir:
                        cache_dir = Path(base_dir) / "appearance_sorting_system" / "query_cache"
                        cache_dir.mkdir(parents=True, exist_ok=True)

                        key_material = effective_access_path + "|" + ",".join(product_numbers)
                        key_hash = hashlib.sha1(key_material.encode("utf-8", errors="ignore")).hexdigest()[:12]
                        disk_cache_path = cache_dir / f"packaging_{key_hash}_{sig}.pkl"

                        if disk_cache_path.exists():
                            with open(disk_cache_path, "rb") as f:
                                cached_df = pickle.load(f)
                            if isinstance(cached_df, pd.DataFrame):
                                logger.bind(channel="PERF").debug("PERF {}: cache_hit", "access.packaging.disk_cache")
                                self._store_access_cache(cache_key, cached_df)
                                return cached_df.copy()
            except Exception:
                disk_cache_path = None
            
            # Accessに集計を任せて転送量を削減（高速化）
            placeholders = ", ".join("?" for _ in product_numbers)
            packaging_query = f"""
            SELECT 品番, SUM(数量) AS 梱包・完了
            FROM [t_現品票履歴]
            WHERE 品番 IN ({placeholders})
              AND InStr(現在工程名, ?) > 0
            GROUP BY 品番
            """
            params = list(product_numbers) + ["梱包"]

            with perf_timer(logger, "access.packaging.read_sql"):
                packaging_df = pd.read_sql(packaging_query, connection, params=params)
            
            if packaging_df.empty:
                self.log_message("梱包工程のデータが見つかりませんでした")
                return pd.DataFrame()

            self.log_message(f"梱包工程データを取得しました: {len(packaging_df)}件")
            self._store_access_cache(cache_key, packaging_df)

            # ディスクキャッシュへ保存（失敗しても無視）
            if disk_cache_path is not None:
                try:
                    import pickle
                    with open(disk_cache_path, "wb") as f:
                        pickle.dump(packaging_df, f, protocol=4)

                    # 古いキャッシュの間引き（失敗しても無視）
                    try:
                        self._prune_query_cache_files(disk_cache_path.parent, "packaging_", keep=30)
                    except Exception:
                        pass
                except Exception:
                    pass
            return packaging_df
            
        except Exception as e:
            self.log_message(f"梱包工程データの取得中にエラーが発生しました: {str(e)}")
            return pd.DataFrame()

    def get_shipping_stock_quantities(self, connection, main_df):
        """別テーブルから出荷数・在庫数を取得（注文IDで結合）"""
        try:
            if not self.config.shipping_stock_table_name:
                self.log_message("出荷数・在庫数テーブル名が設定されていません")
                return pd.DataFrame()
            
            # メインデータから注文IDリストを取得
            if '注文ID' not in main_df.columns or main_df.empty:
                self.log_message("注文ID列が見つからないか、データが空です")
                return pd.DataFrame()
            
            order_ids = main_df['注文ID'].dropna().unique().tolist()
            if not order_ids:
                self.log_message("注文IDデータが見つかりません")
                return pd.DataFrame()
            
            self.log_message(f"出荷数・在庫数データを検索中: {len(order_ids)}件の注文ID")
            
            # AccessのODBCドライバーの制限を回避するため、pyodbcのカーソルを使って直接取得
            try:
                # まずテーブルの構造を確認
                self.log_message("  テーブル構造を確認中...")
                cursor = connection.cursor()
                
                # テーブル全体を取得（カーソルを使用）
                self.log_message("  テーブル全体を取得中...")
                table_name = self.config.shipping_stock_table_name
                query = f"SELECT * FROM [{table_name}]"
                
                try:
                    cursor.execute(query)
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()
                    
                    if not rows:
                        self.log_message("  テーブルにデータが見つかりませんでした")
                        cursor.close()
                        return pd.DataFrame()
                    
                    self.log_message(f"  テーブルから {len(rows)}件のデータを取得しました")
                    
                    # DataFrameに変換
                    shipping_stock_df = pd.DataFrame.from_records(rows, columns=columns)
                    
                    # 必要な列が存在するか確認
                    required_columns = ['注文ID', '出荷数', '在庫数']
                    missing_columns = [col for col in required_columns if col not in shipping_stock_df.columns]
                    if missing_columns:
                        self.log_message(f"  警告: 必要な列が見つかりません: {missing_columns}")
                        self.log_message(f"  利用可能な列: {list(shipping_stock_df.columns)}")
                        cursor.close()
                        return pd.DataFrame()
                    
                    # 注文IDのセットを作成（高速検索のため）
                    order_ids_set = set(str(oid) for oid in order_ids)
                    
                    # Python側でフィルタリング
                    shipping_stock_df['注文ID_str'] = shipping_stock_df['注文ID'].astype(str)
                    filtered_df = shipping_stock_df[shipping_stock_df['注文ID_str'].isin(order_ids_set)].copy()
                    filtered_df = filtered_df.drop(columns=['注文ID_str'])
                    
                    # 必要な列のみを抽出
                    filtered_df = filtered_df[required_columns].copy()
                    
                    if filtered_df.empty:
                        self.log_message("  フィルタリング後、該当するデータが見つかりませんでした")
                        cursor.close()
                        return pd.DataFrame()
                    
                    self.log_message(f"  フィルタリング後: {len(filtered_df)}件のデータ")
                    cursor.close()
                    
                except Exception as query_error:
                    cursor.close()
                    raise query_error
                
            except Exception as e:
                self.log_message(f"  テーブル取得中にエラー: {str(e)}")
                import traceback
                self.log_message(f"  エラー詳細: {traceback.format_exc()}")
                return pd.DataFrame()
            
            shipping_stock_df = filtered_df
            
            # 注文IDごとに集計（複数レコードがある場合は合計）
            if len(shipping_stock_df) > len(order_ids):
                # 複数レコードがある場合は注文IDごとに合計
                shipping_stock_summary = shipping_stock_df.groupby('注文ID').agg({
                    '出荷数': 'sum',
                    '在庫数': 'sum'
                }).reset_index()
            else:
                shipping_stock_summary = shipping_stock_df.copy()
            
            self.log_message(f"出荷数・在庫数データを取得しました: {len(shipping_stock_summary)}件")
            
            return shipping_stock_summary
            
        except Exception as e:
            self.log_message(f"出荷数・在庫数データの取得中にエラーが発生しました: {str(e)}")
            return pd.DataFrame()

    def _get_inventory_table_structure(self, connection):
        """t_現品票履歴テーブルの列情報をキャッシュ"""
        import time

        now = time.time()
        cache_valid = (
            self._inventory_table_structure_timestamp is not None and
            (now - self._inventory_table_structure_timestamp) < self.TABLE_STRUCTURE_CACHE_TTL
        )

        if cache_valid and self._inventory_table_structure_cache:
            cached = self._inventory_table_structure_cache
            return cached.get("columns", []), cached.get("has_rows", False)

        columns_query = "SELECT TOP 1 * FROM [t_現品票履歴]"
        try:
            sample_df = pd.read_sql(columns_query, connection)
        except Exception as e:
            self.log_message(f"t_現品票履歴の構造取得に失敗しました: {str(e)}")
            return [], False

        has_rows = not sample_df.empty
        columns = sample_df.columns.tolist()

        self._inventory_table_structure_cache = {
            "columns": columns,
            "has_rows": has_rows,
        }
        self._inventory_table_structure_timestamp = now
        return columns, has_rows

    def _build_access_cache_key(
        self,
        scope: str,
        identifiers: List[str],
        keywords: Optional[List[str]] = None
    ) -> Tuple[str, Tuple[str, ...], Tuple[str, ...]]:
        cleaned_ids = tuple(sorted({str(identifier).strip() for identifier in identifiers if str(identifier).strip()}))
        keyword_tuple = tuple(keywords) if keywords else ()
        return (scope, cleaned_ids, keyword_tuple)

    def _try_get_access_cache(self, key: Tuple[str, Tuple[str, ...], Tuple[str, ...]]) -> Optional[pd.DataFrame]:
        timestamp = self._access_lots_cache_timestamp.get(key)
        if timestamp and (datetime.now() - timestamp).total_seconds() < self.ACCESS_LOTS_CACHE_TTL_SECONDS:
            cached = self._access_lots_cache.get(key)
            if cached is not None:
                return cached.copy()
        self._access_lots_cache.pop(key, None)
        self._access_lots_cache_timestamp.pop(key, None)
        return None

    def _store_access_cache(self, key: Tuple[str, Tuple[str, ...], Tuple[str, ...]], df: pd.DataFrame) -> None:
        self._access_lots_cache[key] = df.copy()
        self._access_lots_cache_timestamp[key] = datetime.now()

    def _prune_query_cache_files(self, cache_dir: "Path", prefix: str, keep: int = 30) -> None:
        """
        ディスクキャッシュ肥大化防止用の間引き。
        - prefix一致のpklのみ対象
        - 新しいものから keep 個だけ残す
        """
        try:
            from pathlib import Path

            if not isinstance(cache_dir, Path):
                cache_dir = Path(str(cache_dir))
            if keep <= 0:
                keep = 1
            if not cache_dir.exists():
                return

            candidates = [
                p for p in cache_dir.iterdir()
                if p.is_file() and p.name.startswith(prefix) and p.suffix.lower() == ".pkl"
            ]
            if len(candidates) <= keep:
                return

            candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            for p in candidates[keep:]:
                try:
                    p.unlink(missing_ok=True)
                except Exception:
                    pass
        except Exception:
            pass

    def get_available_lots_for_shortage(self, connection, shortage_df):
        """不足数がマイナスの品番に対して利用可能なロットを取得"""
        try:
            if shortage_df.empty:
                self.log_message("不足数がマイナスのデータがありません")
                return pd.DataFrame()

            shortage_products = shortage_df[shortage_df['不足数'] < 0]['品番'].dropna().unique().tolist()
            if not shortage_products:
                self.log_message("不足数がマイナスの品番が見つかりません")
                return pd.DataFrame()
            self.log_message(f"不足数がマイナスの品番: {len(shortage_products)}件")

            cache_key = self._build_access_cache_key(
                "shortage",
                shortage_products,
                self.inspection_target_keywords
            )
            cached_lots = self._try_get_access_cache(cache_key)
            if cached_lots is not None:
                self.log_message("Accessのロットデータをキャッシュから再利用しました")
                return cached_lots

            # 対象外ロット（参考情報）は必須なので、同一クエリ結果から派生させてAccess二重クエリを避ける
            non_inspection_cache_key = self._build_access_cache_key(
                "non_inspection",
                shortage_products,
                self.inspection_target_keywords,
            )

            # ローカルディスクキャッシュ（実行を跨いで再利用）
            # Accessファイルの更新状態（mtime/size）に紐づけているため、結果は不変のまま揺れを低減できる。
            disk_cache_path = None
            disk_cached_payload = None

            registered_product_numbers = []
            if self.registered_products:
                registered_product_numbers = [
                    item.get('品番', '')
                    for item in self.registered_products
                    if item.get('品番', '')
                ]
                registered_product_numbers = sorted({str(pn).strip() for pn in registered_product_numbers if str(pn).strip()})

            actual_columns, has_rows = self._get_inventory_table_structure(connection)
            if not has_rows:
                self.log_message("t_現品票履歴テーブルにデータが見つかりません")
                return pd.DataFrame()

            # 対象外ロット（参考情報）も同一クエリから生成するため、表示に必要な列も可能なら同時取得する
            available_columns = [col for col in actual_columns if col in [
                "品番", "品名", "客先", "数量", "ロット数量", "指示日", "号機",
                "現在工程番号", "現在工程名", "現在工程二次処理", "生産ロットID"
            ]]
            if not available_columns:
                self.log_message("必要な列が見つかりません。全列を取得します。")
                available_columns = actual_columns

            # ディスクキャッシュ読み込み（表構造確定後にキー生成）
            try:
                from app.config import DatabaseConfig
                import hashlib
                import os
                import pickle
                from pathlib import Path

                effective_access_path = DatabaseConfig.get_last_effective_access_path() or ""
                if effective_access_path:
                    stat = os.stat(effective_access_path)
                    sig = f"{int(getattr(stat, 'st_mtime', 0))}_{int(getattr(stat, 'st_size', 0))}"

                    base_dir = os.getenv("LOCALAPPDATA", "").strip() or os.getenv("TEMP", "").strip()
                    if base_dir:
                        cache_dir = Path(base_dir) / "appearance_sorting_system" / "query_cache"
                        cache_dir.mkdir(parents=True, exist_ok=True)

                        key_material = (
                            effective_access_path
                            + "|cols=" + ",".join(available_columns)
                            + "|shortage=" + ",".join(sorted(shortage_products))
                            + "|registered=" + ",".join(sorted(registered_product_numbers))
                            + "|keywords=" + ",".join([str(k).strip() for k in (self.inspection_target_keywords or []) if str(k).strip()])
                        )
                        key_hash = hashlib.sha1(key_material.encode("utf-8", errors="ignore")).hexdigest()[:12]
                        disk_cache_path = cache_dir / f"lots_shortage_{key_hash}_{sig}.pkl"

                        if disk_cache_path.exists():
                            with open(disk_cache_path, "rb") as f:
                                disk_cached_payload = pickle.load(f)
            except Exception:
                disk_cache_path = None
                disk_cached_payload = None

            if isinstance(disk_cached_payload, dict):
                try:
                    cached_shortage_df = disk_cached_payload.get("shortage_lots_df")
                    cached_registered_df = disk_cached_payload.get("registered_lots_df")
                    cached_non_inspection_df = disk_cached_payload.get("non_inspection_lots_df")

                    if isinstance(cached_shortage_df, pd.DataFrame):
                        logger.bind(channel="PERF").debug("PERF {}: cache_hit", "access.lots_for_shortage.disk_cache")
                        self._store_access_cache(cache_key, cached_shortage_df)
                        if isinstance(cached_non_inspection_df, pd.DataFrame):
                            self._store_access_cache(non_inspection_cache_key, cached_non_inspection_df)

                        if registered_product_numbers and isinstance(cached_registered_df, pd.DataFrame):
                            registered_cache_key = self._build_access_cache_key(
                                "registered",
                                registered_product_numbers,
                            )
                            self._store_access_cache(registered_cache_key, cached_registered_df)

                        return cached_shortage_df.copy()
                except Exception:
                    pass

            columns_str = ", ".join([f"[{col}]" for col in available_columns])

            # Access側のOR条件は最適化されにくいことがあるため、品番は不足品番＋登録済み品番の集合でINに統合する
            all_product_numbers = list(shortage_products)
            if registered_product_numbers:
                all_product_numbers.extend(registered_product_numbers)
            all_product_numbers = sorted({str(pn).strip() for pn in all_product_numbers if str(pn).strip()})

            placeholders = ", ".join("?" for _ in all_product_numbers)
            base_product_clause = f"品番 IN ({placeholders})"
            params = list(all_product_numbers)

            if "現在工程名" in available_columns:
                # NULL の場合に NOT LIKE が NULL となり除外されてしまうため、NULL は許容して後段で扱う
                base_conditions = [
                    "(現在工程名 IS NULL OR 現在工程名 NOT LIKE '%完了%')",
                    "(現在工程名 IS NULL OR 現在工程名 NOT LIKE '%梱包%')",
                ]

                # キーワード絞り込み（OR/LIKE）はAccess側で遅くなりやすいので、
                # まず品番IN中心で取得し、後段で不足品番側のみpandasで絞り込む（結果は同一）
                if self.inspection_target_keywords:
                    self.log_message(f"検査対象キーワードでフィルタリング: {len(self.inspection_target_keywords)}件のキーワード")
                else:
                    self.log_message("検査対象キーワードが設定されていません。全てのロットを対象とします。")
            else:
                base_conditions = []

            where_conditions = [base_product_clause] + base_conditions
            where_clause = " AND ".join(where_conditions)

            lots_query = f"""
            SELECT {columns_str}
            FROM [t_現品票履歴]
            WHERE {where_clause}
            """

            def _read_sql_via_cursor(query_text: str, query_params: list[str]) -> pd.DataFrame:
                cursor = connection.cursor()
                cursor.execute(query_text, query_params)
                column_names = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                if not rows:
                    return pd.DataFrame(columns=column_names)
                return pd.DataFrame.from_records(rows, columns=column_names)

            with perf_timer(logger, "access.lots_for_shortage.read_sql"):
                # pd.read_sql は環境によって型推論/パースが重くなるため、pyodbcカーソルで直接取得して高速化する
                lots_df = _read_sql_via_cursor(lots_query, params)

            # Access側の ORDER BY を避け、同等の安定ソートをpandas側で実施（結果の選択順序を維持）
            sort_cols = []
            if "品番" in lots_df.columns:
                sort_cols.append("品番")
            if "指示日" in lots_df.columns:
                sort_cols.append("指示日")
            elif "号機" in lots_df.columns:
                sort_cols.append("号機")
            if sort_cols:
                lots_df = lots_df.sort_values(sort_cols, na_position="last", kind="mergesort")

            if lots_df.empty:
                self.log_message("利用可能なロットが見つかりませんでした")
                return pd.DataFrame()

            # 対象外ロット（参考情報）を同一lots_dfから派生（結果は同一・Accessクエリ回数を削減）
            non_inspection_lots_df = pd.DataFrame()
            try:
                shortage_all_lots_df = lots_df[lots_df["品番"].isin(shortage_products)].copy()
                if (
                    not shortage_all_lots_df.empty
                    and self.inspection_target_keywords
                    and "現在工程名" in shortage_all_lots_df.columns
                ):
                    with perf_timer(logger, "lots.non_inspection.keyword_filter"):
                        process_series = shortage_all_lots_df["現在工程名"].astype(str)
                        keyword_mask = pd.Series(False, index=shortage_all_lots_df.index, dtype=bool)
                        for keyword in self.inspection_target_keywords:
                            if not isinstance(keyword, str) or not keyword.strip():
                                continue
                            keyword_mask |= process_series.str.contains(keyword.strip(), na=False, regex=False)
                        non_inspection_lots_df = shortage_all_lots_df[~keyword_mask].copy()
            except Exception:
                non_inspection_lots_df = pd.DataFrame()

            # 不足品番だけ返す（登録済み品番分はキャッシュに保持して二重クエリを回避）
            shortage_lots_df = lots_df[lots_df["品番"].isin(shortage_products)].copy()

            # 不足品番側のみ、検査対象キーワードで絞り込み（Access側のOR/LIKEを避ける）
            if (
                not shortage_lots_df.empty
                and self.inspection_target_keywords
                and "現在工程名" in shortage_lots_df.columns
            ):
                with perf_timer(logger, "lots.shortage.keyword_filter"):
                    process_series = shortage_lots_df["現在工程名"].astype(str)
                    keyword_mask = pd.Series(False, index=shortage_lots_df.index, dtype=bool)
                    for keyword in self.inspection_target_keywords:
                        if not isinstance(keyword, str) or not keyword.strip():
                            continue
                        keyword_mask |= process_series.str.contains(keyword.strip(), na=False, regex=False)
                    shortage_lots_df = shortage_lots_df[keyword_mask].copy()

            self._store_access_cache(cache_key, shortage_lots_df)
            if isinstance(non_inspection_lots_df, pd.DataFrame) and not non_inspection_lots_df.empty:
                self._store_access_cache(non_inspection_cache_key, non_inspection_lots_df)

            if registered_product_numbers:
                registered_cache_key = self._build_access_cache_key(
                    "registered",
                    registered_product_numbers
                )
                registered_lots_df = lots_df[ lots_df["品番"].isin(registered_product_numbers) ].copy()
                if not registered_lots_df.empty:
                    self._store_access_cache(registered_cache_key, registered_lots_df)

            # ディスクキャッシュへ保存（失敗しても無視）
            if disk_cache_path is not None:
                try:
                    import pickle
                    payload = {
                        "shortage_lots_df": shortage_lots_df,
                        "registered_lots_df": registered_lots_df if registered_product_numbers else pd.DataFrame(),
                        "non_inspection_lots_df": non_inspection_lots_df if isinstance(non_inspection_lots_df, pd.DataFrame) else pd.DataFrame(),
                    }
                    with open(disk_cache_path, "wb") as f:
                        pickle.dump(payload, f, protocol=4)

                    # 古いキャッシュの間引き（失敗しても無視）
                    try:
                        self._prune_query_cache_files(disk_cache_path.parent, "lots_shortage_", keep=30)
                    except Exception:
                        pass
                except Exception:
                    pass

            self.log_message(f"利用可能なロットを取得しました: {len(shortage_lots_df)}件")
            return shortage_lots_df

        except Exception as e:
            self.log_message(f"利用可能ロットの取得中にエラーが発生しました: {str(e)}")
            return pd.DataFrame()
    
    def get_non_inspection_target_lots_for_shortage(self, connection, shortage_df):
        """
        不足品番の検査対象外ロットを取得（前後工程情報付き）
        
        Args:
            connection: Accessデータベース接続
            shortage_df: 不足数がマイナスのデータ
        
        Returns:
            検査対象外ロットのDataFrame（前後工程情報を含む）
        """
        try:
            if shortage_df.empty:
                return pd.DataFrame()
             
            shortage_products = shortage_df[shortage_df['不足数'] < 0]['品番'].dropna().unique().tolist()
            if not shortage_products:
                return pd.DataFrame()
             
            self.log_message(f"検査対象外ロットを取得中: {len(shortage_products)}品番")

            # get_available_lots_for_shortage() と同一クエリ結果から派生したキャッシュがあれば再利用（Access二重クエリ回避）
            cache_key = self._build_access_cache_key(
                "non_inspection",
                shortage_products,
                self.inspection_target_keywords,
            )
            cached = self._try_get_access_cache(cache_key)
            if cached is not None:
                logger.bind(channel="PERF").debug("PERF {}: cache_hit", "access.non_inspection_lots.memory_cache")
                non_inspection_lots_df = cached

                # 出荷予定日をshortage_dfからマージ（品番で結合、高速化）
                if not shortage_df.empty and '出荷予定日' in shortage_df.columns and '品番' in shortage_df.columns:
                    shipping_date_map = shortage_df.groupby('品番')['出荷予定日'].first().to_dict()
                    if shipping_date_map and '品番' in non_inspection_lots_df.columns:
                        non_inspection_lots_df['出荷予定日'] = non_inspection_lots_df['品番'].map(shipping_date_map)

                return non_inspection_lots_df

            # テーブル構造を取得
            actual_columns, has_rows = self._get_inventory_table_structure(connection)
            if not has_rows:
                return pd.DataFrame()
            
            available_columns = [col for col in actual_columns if col in [
                "品番", "品名", "客先", "数量", "ロット数量", "指示日", "号機", 
                "現在工程番号", "現在工程名", "現在工程二次処理", "生産ロットID"
            ]]
            
            if not available_columns:
                available_columns = actual_columns
            
            columns_str = ", ".join([f"[{col}]" for col in available_columns])
            shortage_placeholders = ", ".join("?" for _ in shortage_products)
            
            # 検査対象外のロットを取得（検査対象キーワードに一致しないもの）
            # 高速化: WHERE条件を最適化
            process_subconditions = [
                "現在工程名 NOT LIKE '%完了%'",
                "現在工程名 NOT LIKE '%梱包%'",
            ]
            
            # 検査対象キーワードに一致しないロットを取得（高速化: 条件を統合）
            if self.inspection_target_keywords and "現在工程名" in available_columns:
                valid_keywords = [kw.strip() for kw in self.inspection_target_keywords if isinstance(kw, str) and kw.strip()]
                if valid_keywords:
                    # LIKE条件を統合して高速化（' をエスケープしてクエリ失敗を抑制）
                    for kw in valid_keywords:
                        kw_escaped = kw.replace("'", "''")
                        process_subconditions.append(f"現在工程名 NOT LIKE '%{kw_escaped}%'")
            
            where_conditions = [
                f"品番 IN ({shortage_placeholders})",
                f"(現在工程名 IS NULL OR ({' AND '.join(process_subconditions)}))",
            ]
            
            where_clause = " AND ".join(where_conditions)
            
            query = f"""
            SELECT {columns_str}
            FROM [t_現品票履歴]
            WHERE {where_clause}
            """
            
            with perf_timer(logger, "access.non_inspection_lots.read_sql"):
                # 高速化: chunksizeを指定してメモリ効率を向上
                non_inspection_lots_df = pd.read_sql(query, connection, params=shortage_products)
            
            if non_inspection_lots_df.empty:
                return pd.DataFrame()
            
            # 前後工程情報の取得は不要（削除）
            
            # 出荷予定日をshortage_dfからマージ（品番で結合、高速化）
            if not shortage_df.empty and '出荷予定日' in shortage_df.columns and '品番' in shortage_df.columns:
                # 品番ごとの出荷予定日を取得（最初の値を使用、高速化）
                shipping_date_map = shortage_df.groupby('品番')['出荷予定日'].first().to_dict()
                if shipping_date_map:
                    # mapを使用（高速、大量データでも効率的）
                    non_inspection_lots_df['出荷予定日'] = non_inspection_lots_df['品番'].map(shipping_date_map)
            
            return non_inspection_lots_df
            
        except Exception as e:
            self.log_message(f"検査対象外ロットの取得中にエラーが発生しました: {str(e)}")
            logger.error(f"検査対象外ロット取得エラー: {e}", exc_info=True)
            return pd.DataFrame()
    
    def _add_adjacent_process_info(self, lots_df: pd.DataFrame, process_master_path: str) -> pd.DataFrame:
        """
        前後工程情報をロットデータに追加（高速化版）
        
        Args:
            lots_df: ロットデータ
            process_master_path: 工程マスタのパス
        
        Returns:
            前後工程情報が追加されたDataFrame
        """
        try:
            from pathlib import Path
            import os
            
            # 工程マスタをキャッシュから読み込み（高速化）
            cache_key = 'process_master'
            process_master_df = None
            
            # キャッシュチェック
            if cache_key in self.master_cache:
                try:
                    current_mtime = os.path.getmtime(process_master_path)
                    cached_mtime = self.cache_file_mtimes.get(cache_key, 0)
                    if current_mtime == cached_mtime:
                        process_master_df = self.master_cache[cache_key]
                        logger.debug("工程マスタをキャッシュから読み込みました（ファイル未変更）")
                except (OSError, AttributeError):
                    pass
            
            # キャッシュミスの場合は読み込み
            if process_master_df is None:
                process_master_path_obj = Path(process_master_path)
                if not process_master_path_obj.exists():
                    return lots_df
                
                try:
                    process_master_df = pd.read_excel(process_master_path, header=None, engine='openpyxl')
                except Exception as e:
                    self.log_message(f"工程マスタの読み込みに失敗しました: {str(e)}")
                    return lots_df
                
                if process_master_df.empty:
                    return lots_df
                
                # キャッシュに保存
                try:
                    self.master_cache[cache_key] = process_master_df
                    self.cache_file_mtimes[cache_key] = os.path.getmtime(process_master_path)
                    self.cache_timestamps[cache_key] = datetime.now()
                except Exception:
                    pass
            
            # 前後工程情報の列を追加（初期化）
            lots_df = lots_df.copy()
            lots_df['前工程番号'] = ''
            lots_df['前工程名'] = ''
            lots_df['後工程番号'] = ''
            lots_df['後工程名'] = ''
            lots_df['工程情報'] = ''
            
            # 必要な列が存在するか確認
            if '品番' not in lots_df.columns or '現在工程番号' not in lots_df.columns:
                return lots_df
            
            # 工程マスタの列名を事前に準備（高速化）
            product_col = process_master_df.columns[0]
            process_master_df[product_col] = process_master_df[product_col].astype(str).str.strip()
            
            # 工程番号と列インデックスのマッピングを事前作成（高速化）
            process_number_to_col_idx = {}
            for col_idx in range(1, len(process_master_df.columns)):
                col_name = str(process_master_df.columns[col_idx]).strip()
                if col_name:
                    process_number_to_col_idx[col_name] = col_idx
            
            # 品番と工程番号の組み合わせで一括処理（高速化）
            # 品番と現在工程番号の組み合わせを取得
            lots_df['品番_clean'] = lots_df['品番'].astype(str).str.strip()
            lots_df['現在工程番号_clean'] = lots_df['現在工程番号'].astype(str).str.strip()
            
            # 有効な行のみをフィルタリング
            valid_mask = (lots_df['品番_clean'] != '') & (lots_df['品番_clean'] != 'nan') & \
                        (lots_df['現在工程番号_clean'] != '') & (lots_df['現在工程番号_clean'] != 'nan')
            
            if not valid_mask.any():
                lots_df = lots_df.drop(columns=['品番_clean', '現在工程番号_clean'])
                return lots_df
            
            # 工程マスタから該当品番の行を一括取得（高速化）
            unique_products = lots_df[valid_mask]['品番_clean'].unique()
            matching_rows_dict = {}
            
            for product in unique_products:
                matching_rows = process_master_df[process_master_df[product_col] == product]
                if not matching_rows.empty:
                    matching_rows_dict[product] = matching_rows.iloc[0]
            
            # ベクトル化された処理（高速化）
            def get_process_info(row):
                product = row['品番_clean']
                current_process = row['現在工程番号_clean']
                
                if product not in matching_rows_dict or current_process not in process_number_to_col_idx:
                    return pd.Series({
                        '前工程番号': '',
                        '前工程名': '',
                        '後工程番号': '',
                        '後工程名': ''
                    })
                
                matching_row = matching_rows_dict[product]
                current_col_idx = process_number_to_col_idx[current_process]
                
                # 前工程
                prev_process_number = ''
                prev_process_name = ''
                if current_col_idx > 1:
                    prev_col_idx = current_col_idx - 1
                    prev_process_number = str(process_master_df.columns[prev_col_idx]).strip()
                    prev_value = matching_row.iloc[prev_col_idx]
                    prev_process_name = str(prev_value).strip() if pd.notna(prev_value) else ''
                
                # 後工程
                next_process_number = ''
                next_process_name = ''
                if current_col_idx < len(process_master_df.columns) - 1:
                    next_col_idx = current_col_idx + 1
                    next_process_number = str(process_master_df.columns[next_col_idx]).strip()
                    next_value = matching_row.iloc[next_col_idx]
                    next_process_name = str(next_value).strip() if pd.notna(next_value) else ''
                
                return pd.Series({
                    '前工程番号': prev_process_number,
                    '前工程名': prev_process_name,
                    '後工程番号': next_process_number,
                    '後工程名': next_process_name
                })
            
            # 一括処理（高速化）
            process_info_df = lots_df[valid_mask].apply(get_process_info, axis=1)
            lots_df.loc[valid_mask, '前工程番号'] = process_info_df['前工程番号']
            lots_df.loc[valid_mask, '前工程名'] = process_info_df['前工程名']
            lots_df.loc[valid_mask, '後工程番号'] = process_info_df['後工程番号']
            lots_df.loc[valid_mask, '後工程名'] = process_info_df['後工程名']
            
            # 工程情報をまとめる（ベクトル化）
            def build_process_info(row):
                parts = []
                if row.get('前工程名', ''):
                    parts.append(f"前: {row['前工程名']}")
                if row.get('現在工程名', ''):
                    parts.append(f"現在: {row['現在工程名']}")
                if row.get('後工程名', ''):
                    parts.append(f"後: {row['後工程名']}")
                return " / ".join(parts) if parts else ''
            
            lots_df['工程情報'] = lots_df.apply(build_process_info, axis=1)
            
            # 一時列を削除
            lots_df = lots_df.drop(columns=['品番_clean', '現在工程番号_clean'])
            
            return lots_df
            
        except Exception as e:
            self.log_message(f"前後工程情報の追加中にエラーが発生しました: {str(e)}")
            logger.error(f"前後工程情報追加エラー: {e}", exc_info=True)
            return lots_df
    
    def log_non_inspection_lots_info(self, connection, shortage_df):
        """
        検査対象外ロットの情報をログに出力（データを保存してボタンで送信可能にする）
        
        Args:
            connection: Accessデータベース接続
            shortage_df: 不足数がマイナスのデータ
        """
        try:
            if shortage_df.empty:
                self.non_inspection_lots_df = pd.DataFrame()
                self._update_araichat_button_state()
                return
            
            # 検査対象外ロットを取得
            non_inspection_lots_df = self.get_non_inspection_target_lots_for_shortage(connection, shortage_df)
            
            # ソート処理（出荷予定日 → 品番 → 指示日の順で昇順）
            non_inspection_lots_df = self._sort_non_inspection_lots_df(non_inspection_lots_df)
            
            # インスタンス変数に保存（ボタン送信用）
            self.non_inspection_lots_df = non_inspection_lots_df.copy() if not non_inspection_lots_df.empty else pd.DataFrame()
            
            # ボタンの状態を更新
            self._update_araichat_button_state()
            
            if non_inspection_lots_df.empty:
                self.log_message("検査対象外ロットは見つかりませんでした")
                return

            # 取得完了時点で確認ウィンドウを自動表示（1回のみ）
            if not self._auto_open_non_inspection_window_done:
                self._auto_open_non_inspection_window_done = True
                self.root.after(0, self.show_non_inspection_lots_confirmation)
             
            # ログに出力
            self.log_message(f"【検査対象外ロット（参考情報）】合計: {len(non_inspection_lots_df)}件")
            
            # 工程別の詳細はログに出さない（冗長になるため）
            
            # （ログは最小化）
            
        except Exception as e:
            self.log_message(f"検査対象外ロット情報の出力中にエラーが発生しました: {str(e)}")
            logger.error(f"検査対象外ロット情報出力エラー: {e}", exc_info=True)
            self.non_inspection_lots_df = pd.DataFrame()
            self._update_araichat_button_state()

    def _sort_non_inspection_lots_df(self, lots_df: pd.DataFrame) -> pd.DataFrame:
        """検査対象外ロットのソート（出荷予定日 → 品番 → 指示日、NaNは最後）"""
        if lots_df is None or lots_df.empty:
            return pd.DataFrame() if lots_df is None else lots_df

        sort_df = lots_df.copy()

        if '出荷予定日' in sort_df.columns:
            sort_df['_sort_出荷予定日'] = pd.to_datetime(sort_df['出荷予定日'], errors='coerce')
        else:
            sort_df['_sort_出荷予定日'] = pd.NaT

        if '指示日' in sort_df.columns:
            sort_df['_sort_指示日'] = pd.to_datetime(sort_df['指示日'], errors='coerce')
        else:
            sort_df['_sort_指示日'] = pd.NaT

        if '品番' in sort_df.columns:
            sort_df['_sort_品番'] = sort_df['品番'].fillna('').astype(str)
        else:
            sort_df['_sort_品番'] = ''

        return (
            sort_df.sort_values(
                by=['_sort_出荷予定日', '_sort_品番', '_sort_指示日'],
                ascending=[True, True, True],
                na_position='last'
            )
            .drop(columns=['_sort_出荷予定日', '_sort_品番', '_sort_指示日'])
            .reset_index(drop=True)
        )
    
    def get_registered_products_lots(self, connection):
        """登録済み品番のロットをt_現品票履歴から取得"""
        try:
            if not self.registered_products:
                return pd.DataFrame()
            
            # 登録済み品番のリストを取得
            registered_product_numbers = [item['品番'] for item in self.registered_products]
            if not registered_product_numbers:
                return pd.DataFrame()
            
            self.log_message(f"登録済み品番のロットを取得中: {len(registered_product_numbers)}件の品番")

            # 登録済み品番は「設定した品番・工程」で取り置きする用途のため、
            # 検査対象キーワード（inspection_target_keywords）では絞り込まない。
            cache_key = self._build_access_cache_key(
                "registered",
                registered_product_numbers
            )
            cached_lots = self._try_get_access_cache(cache_key)
            if cached_lots is not None:
                self.log_message("Accessのロットデータ（登録済み品番）をキャッシュから再利用しました")
                return cached_lots

            # テーブル構造を確認
            actual_columns, has_rows = self._get_inventory_table_structure(connection)
            if not has_rows:
                self.log_message("t_現品票履歴テーブルにデータが見つかりません")
                return pd.DataFrame()

            available_columns = [col for col in actual_columns if col in [
                "品番", "品名", "客先", "数量", "指示日", "号機", "現在工程番号", "現在工程名", 
                "現在工程二次処理", "生産ロットID"
            ]]
            if not available_columns:
                available_columns = actual_columns

            columns_str = ", ".join([f"[{col}]" for col in available_columns])
            placeholders = ", ".join("?" for _ in registered_product_numbers)
            where_conditions = [f"品番 IN ({placeholders})"]
            params = list(registered_product_numbers)

            if "現在工程名" in available_columns:
                # NULL の場合に NOT LIKE が NULL となり除外されてしまうため、NULL は許容して後段で扱う
                where_conditions.append("(現在工程名 IS NULL OR 現在工程名 NOT LIKE '%完了%')")
                where_conditions.append("(現在工程名 IS NULL OR 現在工程名 NOT LIKE '%梱包%')")
            where_clause = " AND ".join(where_conditions)
            lots_query = f"""
            SELECT {columns_str}
            FROM [t_現品票履歴]
            WHERE {where_clause}
            """
            with perf_timer(logger, "access.lots_for_registered.read_sql"):
                lots_df = pd.read_sql(lots_query, connection, params=params)

            # Access側の ORDER BY を避け、同等の安定ソートをpandas側で実施（結果の選択順序を維持）
            sort_cols = []
            if "品番" in lots_df.columns:
                sort_cols.append("品番")
            if "指示日" in lots_df.columns:
                sort_cols.append("指示日")
            elif "号機" in lots_df.columns:
                sort_cols.append("号機")
            if sort_cols:
                lots_df = lots_df.sort_values(sort_cols, na_position="last", kind="mergesort")
            
            if lots_df.empty:
                self.log_message("登録済み品番のロットが見つかりませんでした")
                return pd.DataFrame()

            self._store_access_cache(cache_key, lots_df)

            self.log_message(f"登録済み品番のロットを取得しました: {len(lots_df)}件")
            
            return lots_df
            
        except Exception as e:
            self.log_message(f"登録済み品番のロット取得中にエラーが発生しました: {str(e)}")
            return pd.DataFrame()
    
    def assign_registered_products_lots(self, connection, main_df, assignment_df):
        """登録済み品番のロットを割り当て"""
        try:
            if not self.registered_products:
                return assignment_df
            
            # 登録済み品番のロットを取得
            with perf_timer(logger, "lots.registered.get_registered_products_lots"):
                registered_lots_df = self.get_registered_products_lots(connection)
            
            if registered_lots_df.empty:
                return assignment_df
            
            # 登録済み品番ごとに処理
            additional_assignments = []

            main_row_by_product = {}
            if main_df is not None and not main_df.empty and '品番' in main_df.columns:
                try:
                    main_row_by_product = main_df.dropna(subset=['品番']).drop_duplicates('品番').set_index('品番')
                except Exception:
                    main_row_by_product = {}
            
            for registered_item in self.registered_products:
                product_number = registered_item.get('品番', '')
                max_lots_per_day = int(registered_item.get('ロット数', 0))
                
                # 該当品番のロットを取得
                product_lots = registered_lots_df[registered_lots_df['品番'] == product_number].copy()
                
                if product_lots.empty:
                    continue

                lots_before_filter = len(product_lots)
                
                # 指示日順でソート（生産日の古い順）
                process_filter = registered_item.get('工程名', '').strip()
                if process_filter:
                    with perf_timer(logger, f"lots.registered.process_filter[{product_number}]"):
                        process_keywords = [
                            keyword.strip()
                            for keyword in re.split(r'[／/]', process_filter)
                            if keyword.strip()
                        ]
                        if not process_keywords:
                            process_keywords = [process_filter]
                        process_columns = [col for col in ['現在工程名', '現在工程二次処理', '工程名'] if col in product_lots.columns]
                        if process_columns:
                            has_process_data = any(
                                product_lots[col].astype(str).str.strip().ne('').any()
                                for col in process_columns
                            )
                            if has_process_data:
                                mask = pd.Series(False, index=product_lots.index, dtype=bool)
                                for col in process_columns:
                                    column_data = product_lots[col].astype(str)
                                    column_mask = pd.Series(False, index=product_lots.index, dtype=bool)
                                    for keyword in process_keywords:
                                        column_mask |= column_data.str.contains(keyword, na=False, regex=False)
                                    mask |= column_mask
                                if not mask.any():
                                    self.log_message(
                                        f"工程名「{process_filter}」に一致するロットが見つかりません: {product_number}"
                                    )
                                    continue
                                product_lots = product_lots[mask].copy()
                            else:
                                self.log_message(
                                    f"工程名「{process_filter}」を指定しましたが、現在工程名が未記載のため割当をスキップします: {product_number}"
                                )
                                continue
                        else:
                            self.log_message(
                                f"工程名「{process_filter}」を指定しましたが、照合可能な工程名列がありません（割当をスキップ）: {product_number}"
                            )
                            continue
                if product_lots.empty:
                    continue
                if process_filter:
                    self.log_message(
                        f"登録済み品番 {product_number}: 工程フィルタ適用 {lots_before_filter}件 → {len(product_lots)}件（工程名: {process_filter}）"
                    )

                if '指示日' in product_lots.columns:
                    with perf_timer(logger, f"lots.registered.sort[{product_number}]"):
                        product_lots = product_lots.copy()
                        product_lots['_指示日_ソート用'] = product_lots['指示日'].apply(
                            lambda x: str(x) if pd.notna(x) else ''
                        )
                        product_lots = product_lots.sort_values('_指示日_ソート用', na_position='last')
                        product_lots = product_lots.drop(columns=['_指示日_ソート用'])
                
                # 検査可能ロット数／日を考慮してロットを割り当て
                assigned_count = 0
                lot_cols = {col: idx for idx, col in enumerate(product_lots.columns)}

                main_row = None
                if isinstance(main_row_by_product, pd.DataFrame) and product_number in main_row_by_product.index:
                    try:
                        main_row = main_row_by_product.loc[product_number]
                    except Exception:
                        main_row = None
                
                with perf_timer(logger, f"lots.registered.build_assignments[{product_number}]"):
                    for lot in product_lots.itertuples(index=False):
                        if assigned_count >= max_lots_per_day:
                            break
                        
                        lot_quantity = int(lot[lot_cols['数量']]) if pd.notna(lot[lot_cols['数量']]) else 0
                        
                        # 出荷予定日は「先行検査」とする
                        shipping_date = "先行検査"
                        
                        # 品名と客先を取得（main_dfから取得できない場合はロットデータから取得）
                        product_name = (
                            main_row.get('品名', '') if main_row is not None else 
                            (lot[lot_cols.get('品名', -1)] if '品名' in lot_cols and pd.notna(lot[lot_cols.get('品名', -1)]) else '')
                        )
                        customer_name = (
                            main_row.get('客先', '') if main_row is not None else 
                            (lot[lot_cols.get('客先', -1)] if '客先' in lot_cols and pd.notna(lot[lot_cols.get('客先', -1)]) else '')
                        )
                        
                        assignment_result = {
                            '出荷予定日': shipping_date,
                            '品番': product_number,
                            '品名': product_name,
                            '客先': customer_name,
                            '出荷数': int(main_row.get('出荷数', 0)) if main_row is not None else 0,
                            '在庫数': int(main_row.get('在庫数', 0)) if main_row is not None else 0,
                            '在梱包数': int(main_row.get('梱包・完了', 0)) if main_row is not None else 0,
                            '不足数': 0,  # 登録済み品番は不足数0として扱う
                            'ロット数量': lot_quantity,
                            '指示日': lot[lot_cols.get('指示日', -1)] if '指示日' in lot_cols and pd.notna(lot[lot_cols['指示日']]) else '',
                            '号機': lot[lot_cols.get('号機', -1)] if '号機' in lot_cols and pd.notna(lot[lot_cols['号機']]) else '',
                            '現在工程番号': lot[lot_cols.get('現在工程番号', -1)] if '現在工程番号' in lot_cols and pd.notna(lot[lot_cols['現在工程番号']]) else '',
                            '現在工程名': lot[lot_cols.get('現在工程名', -1)] if '現在工程名' in lot_cols and pd.notna(lot[lot_cols['現在工程名']]) else '',
                            '現在工程二次処理': lot[lot_cols.get('現在工程二次処理', -1)] if '現在工程二次処理' in lot_cols and pd.notna(lot[lot_cols['現在工程二次処理']]) else '',
                            '生産ロットID': lot[lot_cols.get('生産ロットID', -1)] if '生産ロットID' in lot_cols and pd.notna(lot[lot_cols['生産ロットID']]) else ''
                        }
                        
                        additional_assignments.append(assignment_result)
                        assigned_count += 1
                
                self.log_message(f"登録済み品番 {product_number}: {assigned_count}ロットを割り当てました（最大: {max_lots_per_day}ロット/日）")
            
            # assignment_dfに追加
            if additional_assignments:
                registered_df = pd.DataFrame(additional_assignments)
                if assignment_df.empty:
                    assignment_df = registered_df
                else:
                    assignment_df = pd.concat([assignment_df, registered_df], ignore_index=True)
                self.log_message(f"登録済み品番のロット {len(registered_df)}件を割り当てました")
            
            return assignment_df
            
        except Exception as e:
            self.log_message(f"登録済み品番のロット割り当て中にエラーが発生しました: {str(e)}")
            return assignment_df
    
    def assign_lots_to_shortage(self, shortage_df, lots_df):
        """不足数に対してロットを割り当て"""
        try:
            if shortage_df.empty or lots_df.empty:
                return pd.DataFrame()

            if '数量' not in lots_df.columns:
                self.log_message("ロットデータに数量列がありません")
                return pd.DataFrame()

            negative_shortage = shortage_df[shortage_df['不足数'] < 0].copy()
            if negative_shortage.empty:
                return pd.DataFrame()

            grouped_shortage = (
                negative_shortage.groupby('品番', sort=False, as_index=False)
                .first()
            )
            grouped_shortage['required_qty'] = grouped_shortage['不足数'].abs()
            grouped_shortage = grouped_shortage[grouped_shortage['required_qty'] > 0]
            if grouped_shortage.empty:
                return pd.DataFrame()

            grouped_shortage = grouped_shortage.set_index('品番', drop=False)
            grouped_shortage = grouped_shortage.rename(columns={'不足数': 'initial_shortage'})
            shortage_products = grouped_shortage.index.tolist()

            filtered_lots = lots_df[lots_df['品番'].isin(shortage_products)].copy()
            if filtered_lots.empty:
                return pd.DataFrame()

            if '指示日' in filtered_lots.columns:
                filtered_lots['_sort_value'] = filtered_lots['指示日'].apply(
                    lambda x: str(x) if pd.notna(x) else ''
                )
                filtered_lots = filtered_lots.sort_values(['品番', '_sort_value'], na_position='last')
                filtered_lots = filtered_lots.drop(columns=['_sort_value'])
            else:
                filtered_lots = filtered_lots.sort_values('品番')

            filtered_lots['lot_quantity'] = pd.to_numeric(filtered_lots['数量'], errors='coerce').fillna(0)
            filtered_lots['cum_qty'] = filtered_lots.groupby('品番')['lot_quantity'].cumsum()
            filtered_lots['prev_cum_qty'] = filtered_lots['cum_qty'] - filtered_lots['lot_quantity']

            filtered_lots = filtered_lots.merge(
                grouped_shortage[['initial_shortage', 'required_qty']],
                left_on='品番',
                right_index=True,
                how='inner'
            )

            filtered_lots = filtered_lots[filtered_lots['required_qty'].notna() & (filtered_lots['required_qty'] > 0)]
            if filtered_lots.empty:
                self.log_message("ロット割り当て対象の不足品番が見つかりません")
                return pd.DataFrame()

            selected_mask = filtered_lots['prev_cum_qty'] < filtered_lots['required_qty']
            selected_lots = filtered_lots[selected_mask].copy()
            if selected_lots.empty:
                self.log_message("ロット割り当て結果がありません")
                return pd.DataFrame()

            selected_lots['不足数'] = selected_lots['initial_shortage'] + selected_lots['prev_cum_qty']

            if '出荷予定日' in selected_lots.columns:
                shipping_series = selected_lots['出荷予定日'].fillna('')
            else:
                shipping_series = pd.Series('', index=selected_lots.index)
            default_shipping = grouped_shortage['出荷予定日'].fillna('')
            shipping_series = shipping_series.where(
                shipping_series != '',
                selected_lots['品番'].map(default_shipping)
            )
            shipping_series = shipping_series.fillna('')

            def _safe_int(value):
                try:
                    return int(value)
                except Exception:
                    return 0

            def _get_column_series(col_name):
                if col_name in selected_lots.columns:
                    return selected_lots[col_name].fillna('')
                return pd.Series([''] * len(selected_lots), index=selected_lots.index)

            def _map_shortage_field(col_name):
                if col_name in grouped_shortage.columns:
                    mapping = grouped_shortage[col_name]
                    return selected_lots['品番'].map(mapping).fillna('')
                return pd.Series([''] * len(selected_lots), index=selected_lots.index)

            def _map_shortage_int(col_name):
                return _map_shortage_field(col_name).apply(_safe_int)

            assigned_counts = selected_lots['品番'].value_counts()
            for product_number, lot_count in assigned_counts.items():
                self.log_message(f"品番 {product_number} に {lot_count}件のロットを割り当てました")

            result_df = pd.DataFrame({
                '出荷予定日': shipping_series.values,
                '品番': selected_lots['品番'].values,
                '品名': _map_shortage_field('品名'),
                '客先': _map_shortage_field('客先'),
                '出荷数': _map_shortage_int('出荷数'),
                '在庫数': _map_shortage_int('在庫数'),
                '在梱包数': _map_shortage_int('梱包・完了'),
                '不足数': selected_lots['不足数'].values,
                'ロット数量': selected_lots['lot_quantity'].round(0).astype(int).values,
                '指示日': _get_column_series('指示日').values,
                '号機': _get_column_series('号機').values,
                '洗浄指示_行番号': _get_column_series('洗浄指示_行番号').values,
                '現在工程番号': _get_column_series('現在工程番号').values,
                '現在工程名': _get_column_series('現在工程名').values,
                '現在工程二次処理': _get_column_series('現在工程二次処理').values,
                '生産ロットID': _get_column_series('生産ロットID').values,
            })
            self.log_message(f"ロット割り当て完了: {len(result_df)}件")
            return result_df

        except Exception as e:
            self.log_message(f"ロット割り当て中にエラーが発生しました: {str(e)}")
            return pd.DataFrame()
    
    def remove_duplicate_lot_ids(self, assignment_df: pd.DataFrame) -> pd.DataFrame:
        """
        ロットIDの重複を削除（出荷予定日の優先順位に基づいて残す方を決定）
        
        - 生産ロットIDがある場合: 生産ロットIDで重複チェック
        - 生産ロットIDがない場合: 品番・号機・指示日の組み合わせで重複チェック
        
        Args:
            assignment_df: ロット割り当て結果のDataFrame
            
        Returns:
            重複を削除したDataFrame
        """
        try:
            if assignment_df.empty:
                return assignment_df
            
            # 現在日付を取得
            current_date = pd.Timestamp.now().date()
            
            def get_next_business_day(date_val):
                """翌営業日を取得（金曜日の場合は翌週の月曜日）"""
                weekday = date_val.weekday()  # 0=月曜日, 4=金曜日
                if weekday == 4:  # 金曜日
                    return date_val + timedelta(days=3)  # 翌週の月曜日
                else:
                    return date_val + timedelta(days=1)  # 翌日
            
            next_business_day = get_next_business_day(current_date)
            
            def get_shipping_date_priority(shipping_date_val):
                """
                出荷予定日の優先度を取得（数値が小さいほど優先度が高い）
                
                Returns:
                    (優先度, ソート用の値) のタプル
                """
                if pd.isna(shipping_date_val):
                    return (5, None)  # 最後に
                
                val_str = str(shipping_date_val).strip()
                
                # 1. 当日の日付（優先度0）
                try:
                    date_val = pd.to_datetime(shipping_date_val, errors='coerce')
                    if pd.notna(date_val):
                        date_date = date_val.date()
                        if date_date == current_date:
                            return (0, date_val)
                except:
                    pass
                
                # 2. 当日洗浄上がり品（優先度1）
                if (val_str == "当日洗浄上がり品" or
                    val_str == "当日洗浄品" or
                    "当日洗浄" in val_str):
                    return (1, val_str)
                
                # 3. 先行検査品（優先度2）
                if (val_str == "先行検査" or
                    val_str == "当日先行検査"):
                    return (2, val_str)
                
                # 4. 翌日または翌営業日（優先度3）
                try:
                    date_val = pd.to_datetime(shipping_date_val, errors='coerce')
                    if pd.notna(date_val):
                        date_date = date_val.date()
                        if date_date == next_business_day:
                            return (3, date_val)
                except:
                    pass
                
                # 5. それ以降の日付（優先度4）
                try:
                    date_val = pd.to_datetime(shipping_date_val, errors='coerce')
                    if pd.notna(date_val):
                        return (4, date_val)
                except:
                    pass
                
                return (5, val_str)  # その他文字列
            
            # 出荷予定日列が存在するか確認
            has_shipping_date_col = '出荷予定日' in assignment_df.columns
            
            # 生産ロットIDがある行とない行を分離
            has_lot_id_mask = pd.Series([False] * len(assignment_df), index=assignment_df.index)
            if '生産ロットID' in assignment_df.columns:
                has_lot_id_mask = assignment_df['生産ロットID'].notna() & (assignment_df['生産ロットID'] != '')
            
            has_lot_id_df = assignment_df[has_lot_id_mask].copy()
            no_lot_id_df = assignment_df[~has_lot_id_mask].copy()
            
            result_dfs = []
            total_removed = 0
            
            # 1. 生産ロットIDがある行の重複削除
            if not has_lot_id_df.empty:
                before_count = len(has_lot_id_df)
                
                # 重複を検出（ログ出力用）
                if '生産ロットID' in has_lot_id_df.columns:
                    duplicates = has_lot_id_df[has_lot_id_df.duplicated(subset=['生産ロットID'], keep=False)]
                    if not duplicates.empty:
                        duplicate_lot_ids = duplicates['生産ロットID'].unique()
                        self.log_message(f"【重複検出】生産ロットIDで重複: {len(duplicate_lot_ids)}件のロットIDに重複があります")
                
                if has_shipping_date_col:
                    # 優先度を計算してソートキーを追加
                    priority_tuples = has_lot_id_df['出荷予定日'].apply(
                        lambda x: get_shipping_date_priority(x)
                    )
                    # タプルの最初の要素（優先度）のみを抽出してソートキーとする
                    has_lot_id_df['_priority'] = priority_tuples.apply(lambda x: x[0] if isinstance(x, tuple) else 5)
                    
                    # 優先度でソート（優先度が小さい順 = 優先度の高いものが先に来る）
                    has_lot_id_df = has_lot_id_df.sort_values('_priority', na_position='last')
                    
                    # 生産ロットIDで重複を削除（優先度の高い方を残す = keep='first'）
                    has_lot_id_df = has_lot_id_df.drop_duplicates(subset=['生産ロットID'], keep='first')
                    
                    # ソートキーを削除
                    has_lot_id_df = has_lot_id_df.drop(columns=['_priority'], errors='ignore')
                else:
                    # 出荷予定日がない場合は、最初に見つかった行を残す
                    has_lot_id_df = has_lot_id_df.drop_duplicates(subset=['生産ロットID'], keep='first')
                
                removed_count = before_count - len(has_lot_id_df)
                total_removed += removed_count
                if removed_count > 0:
                    self.log_message(f"【重複削除】生産ロットID: {removed_count}件を削除しました（残り: {len(has_lot_id_df)}件）")
                
                result_dfs.append(has_lot_id_df)
            
            # 2. 生産ロットIDがない行の重複削除（品番・号機・指示日で重複チェック）
            if not no_lot_id_df.empty:
                # 品番・号機・指示日の列が存在するか確認
                required_cols = ['品番', '号機', '指示日']
                available_cols = [col for col in required_cols if col in no_lot_id_df.columns]
                if '洗浄指示_行番号' in no_lot_id_df.columns:
                    available_cols.append('洗浄指示_行番号')
                
                if len(available_cols) >= 2:  # 最低2つの列があれば重複チェック可能
                    before_count = len(no_lot_id_df)
                    
                    # 重複を検出（ログ出力用）
                    duplicates = no_lot_id_df[no_lot_id_df.duplicated(subset=available_cols, keep=False)]
                    if not duplicates.empty:
                        duplicate_groups = no_lot_id_df.groupby(available_cols)
                        duplicate_count = 0
                        for key, group in duplicate_groups:
                            if len(group) > 1:
                                duplicate_count += 1
                        self.log_message(f"【重複検出】生産ロットIDなし（{', '.join(available_cols)}）: {duplicate_count}件の組み合わせに重複があります")
                    
                    if has_shipping_date_col:
                        # 優先度を計算してソートキーを追加
                        priority_tuples = no_lot_id_df['出荷予定日'].apply(
                            lambda x: get_shipping_date_priority(x)
                        )
                        # タプルの最初の要素（優先度）のみを抽出してソートキーとする
                        no_lot_id_df['_priority'] = priority_tuples.apply(lambda x: x[0] if isinstance(x, tuple) else 5)
                        
                        # 優先度でソート（優先度が小さい順 = 優先度の高いものが先に来る）
                        no_lot_id_df = no_lot_id_df.sort_values('_priority', na_position='last')
                        
                        # 品番・号機・指示日の組み合わせで重複を削除（優先度の高い方を残す = keep='first'）
                        no_lot_id_df = no_lot_id_df.drop_duplicates(subset=available_cols, keep='first')
                        
                        # ソートキーを削除
                        no_lot_id_df = no_lot_id_df.drop(columns=['_priority'], errors='ignore')
                    else:
                        # 出荷予定日がない場合は、最初に見つかった行を残す
                        no_lot_id_df = no_lot_id_df.drop_duplicates(subset=available_cols, keep='first')
                    
                    removed_count = before_count - len(no_lot_id_df)
                    total_removed += removed_count
                    if removed_count > 0:
                        self.log_message(f"【重複削除】生産ロットIDなし: {removed_count}件を削除しました（残り: {len(no_lot_id_df)}件）")
                
                result_dfs.append(no_lot_id_df)
            
            # 結果を結合
            if result_dfs:
                result_df = pd.concat(result_dfs, ignore_index=True)
                
                # 3. 品番・号機・指示日の組み合わせで当日洗浄品・先行検査品・通常品の重複を処理
                # （同じ品番・号機・指示日で「当日洗浄品」と「先行検査品」、または「当日洗浄品」と「通常品」、または「先行検査品」と「通常品」の両方が存在する場合に重複削除）
                # 注意: この処理は、生産ロットIDがない行に対してのみ適用される
                # （生産ロットIDがある行は、すでに1段階目で重複削除されているため）
                if not result_df.empty and '品番' in result_df.columns and has_shipping_date_col:
                    # 生産ロットIDがない行を全て抽出（当日洗浄品・先行検査品・通常品を含む全ての行）
                    no_lot_id_mask = pd.Series([True] * len(result_df), index=result_df.index)
                    if '生産ロットID' in result_df.columns:
                        no_lot_id_mask = result_df['生産ロットID'].isna() | (result_df['生産ロットID'] == '')
                    
                    target_df = result_df[no_lot_id_mask].copy()
                    other_result_df = result_df[~no_lot_id_mask].copy()
                    
                    if not target_df.empty:
                        before_special_count = len(target_df)
                        self.log_message(f"【ステージ3】当日洗浄品・先行検査品・通常品の処理対象: {before_special_count}件")
                        
                        # 品番・号機・指示日の組み合わせで重複チェック用の列を準備
                        check_cols = ['品番']
                        if '号機' in target_df.columns:
                            check_cols.append('号機')
                        if '指示日' in target_df.columns:
                            check_cols.append('指示日')
                        
                        # 最低1つの列（品番）があれば重複チェック可能（号機・指示日が欠損している場合でも品番でチェック）
                        if len(check_cols) >= 1:
                            def is_normal_date(shipping_date_val):
                                """通常の日付（通常品）かどうかを判定"""
                                if pd.isna(shipping_date_val):
                                    return False
                                val_str = str(shipping_date_val).strip()
                                # 「当日洗浄」や「先行検査」を含まない文字列は通常品ではない
                                if "当日洗浄" in val_str or "先行検査" in val_str:
                                    return False
                                # 日付型に変換可能な場合は通常品
                                try:
                                    date_val = pd.to_datetime(shipping_date_val, errors='coerce')
                                    if pd.notna(date_val):
                                        return True
                                except:
                                    pass
                                return False
                            
                            # 重複を検出（ログ出力用）
                            product_groups = target_df.groupby(check_cols)
                            duplicate_count = 0
                            detailed_logs = []
                            
                            for key, group in product_groups:
                                if len(group) > 1:
                                    # 出荷予定日を確認
                                    shipping_dates = group['出荷予定日'].tolist()
                                    has_cleaning = any("当日洗浄" in str(sd) for sd in shipping_dates)
                                    has_pre_inspection = any("先行検査" in str(sd) for sd in shipping_dates)
                                    has_normal_date = any(is_normal_date(sd) for sd in shipping_dates)
                                    
                                    # 重複として扱うケース：
                                    # 1. 当日洗浄品と先行検査品の両方が存在する場合
                                    # 2. 当日洗浄品と通常品の両方が存在する場合
                                    # 3. 先行検査品と通常品の両方が存在する場合
                                    is_duplicate_combination = False
                                    duplicate_type = []
                                    
                                    if has_cleaning and has_pre_inspection:
                                        is_duplicate_combination = True
                                        duplicate_type.append("当日洗浄品+先行検査品")
                                    if has_cleaning and has_normal_date:
                                        is_duplicate_combination = True
                                        duplicate_type.append("当日洗浄品+通常品")
                                    if has_pre_inspection and has_normal_date:
                                        is_duplicate_combination = True
                                        duplicate_type.append("先行検査品+通常品")
                                    
                                    if is_duplicate_combination:
                                        duplicate_count += 1
                                        # 号機・指示日の情報も取得
                                        machine_info = []
                                        instruction_info = []
                                        if '号機' in group.columns:
                                            machine_info = group['号機'].dropna().unique().tolist()
                                        if '指示日' in group.columns:
                                            instruction_info = group['指示日'].dropna().unique().tolist()
                                        
                                        key_str = ", ".join([f"{col}='{val}'" for col, val in zip(check_cols, key)]) if isinstance(key, tuple) else f"品番='{key}'"
                                        detailed_logs.append({
                                            'key': key_str,
                                            'count': len(group),
                                            'shipping_dates': shipping_dates,
                                            'machines': machine_info,
                                            'instructions': instruction_info,
                                            'type': duplicate_type
                                        })
                            
                            if duplicate_count > 0:
                                self.log_message(f"【重複検出】当日洗浄品・先行検査品・通常品（{', '.join(check_cols)}）: {duplicate_count}件の組み合わせに重複があります")
                                # 詳細ログを出力（最初の10件）
                            
                            # 優先度を計算してソートキーを追加（高速化：一度のapplyで処理）
                            def get_priority(x):
                                result = get_shipping_date_priority(x)
                                return result[0] if isinstance(result, tuple) else 5
                            target_df['_priority'] = target_df['出荷予定日'].apply(get_priority)
                            
                            # 優先度でソート（優先度が小さい順 = 優先度の高いものが先に来る）
                            target_df = target_df.sort_values('_priority', na_position='last')
                            
                            # 重複を削除: 品番（および号機・指示日）ごとにグループ化して、重複が存在する場合のみ削除
                            def should_remove_duplicate(group):
                                """
                                重複として扱うケース：
                                1. 当日洗浄品と先行検査品の両方が存在する場合
                                2. 当日洗浄品と通常品の両方が存在する場合
                                3. 先行検査品と通常品の両方が存在する場合
                                """
                                shipping_dates = group['出荷予定日'].tolist()
                                has_cleaning = any("当日洗浄" in str(sd) for sd in shipping_dates)
                                has_pre_inspection = any("先行検査" in str(sd) for sd in shipping_dates)
                                has_normal_date = any(is_normal_date(sd) for sd in shipping_dates)
                                
                                # 当日洗浄品と先行検査品の両方が存在する場合、または
                                # 当日洗浄品と通常品の両方が存在する場合、または
                                # 先行検査品と通常品の両方が存在する場合
                                return (has_cleaning and has_pre_inspection) or (has_cleaning and has_normal_date) or (has_pre_inspection and has_normal_date)
                            
                            # 品番（および号機・指示日）ごとにグループ化
                            product_groups = target_df.groupby(check_cols)
                            
                            rows_to_keep = []
                            removed_in_stage3 = 0
                            
                            for key, group in product_groups:
                                if len(group) > 1 and should_remove_duplicate(group):
                                    # 重複が存在する場合、優先度の高い方のみ残す
                                    # 既に優先度順にソート済みなので、最初の1件のみ残す
                                    rows_to_keep.append(group.iloc[0:1].drop(columns=['_priority'], errors='ignore'))
                                    removed_in_stage3 += len(group) - 1
                                else:
                                    # 重複がない場合は全て残す
                                    rows_to_keep.append(group.drop(columns=['_priority'], errors='ignore'))
                            
                            if rows_to_keep:
                                target_df = pd.concat(rows_to_keep, ignore_index=True)
                            
                            removed_special_count = before_special_count - len(target_df)
                            total_removed += removed_special_count
                            if removed_special_count > 0:
                                self.log_message(f"【重複削除】当日洗浄品・先行検査品・通常品: {removed_special_count}件を削除しました（残り: {len(target_df)}件）")
                            elif removed_in_stage3 == 0:
                                self.log_message(f"【ステージ3】当日洗浄品・先行検査品・通常品の重複は検出されませんでした")
                        
                        # 生産ロットIDがない行（処理済み）と生産ロットIDがある行を結合
                        if not other_result_df.empty:
                            result_df = pd.concat([other_result_df, target_df], ignore_index=True)
                        else:
                            result_df = target_df
                
                # 4. 全行（生産ロットIDの有無に関わらず）で当日洗浄品・先行検査品・通常品の重複を処理
                # （生産ロットIDがある行でも、同じ品番で「当日洗浄品」と「先行検査品」などが混在する場合は重複として扱う）
                if not result_df.empty and '品番' in result_df.columns and has_shipping_date_col:
                    before_stage4_count = len(result_df)
                    self.log_message(f"【ステージ4】全行での当日洗浄品・先行検査品・通常品の処理対象: {before_stage4_count}件")
                    
                    def is_normal_date(shipping_date_val):
                        """通常の日付（通常品）かどうかを判定"""
                        if pd.isna(shipping_date_val):
                            return False
                        val_str = str(shipping_date_val).strip()
                        # 「当日洗浄」や「先行検査」を含まない文字列は通常品ではない
                        if "当日洗浄" in val_str or "先行検査" in val_str:
                            return False
                        # 日付型に変換可能な場合は通常品
                        try:
                            date_val = pd.to_datetime(shipping_date_val, errors='coerce')
                            if pd.notna(date_val):
                                return True
                        except:
                            pass
                        return False
                    
                    # Stage 4では「品番」のみでグループ化
                    # （号機や指示日が異なる場合でも、同じ品番で「当日洗浄品」と「先行検査品」などが混在する場合は重複として扱う）
                    check_cols = ['品番']
                    
                    if len(check_cols) >= 1:
                        # 重複を検出（ログ出力用）
                        product_groups = result_df.groupby(check_cols)
                        duplicate_count = 0
                        detailed_logs = []
                        
                        for key, group in product_groups:
                            if len(group) > 1:
                                # 出荷予定日を確認
                                shipping_dates = group['出荷予定日'].tolist()
                                has_cleaning = any("当日洗浄" in str(sd) for sd in shipping_dates)
                                has_pre_inspection = any("先行検査" in str(sd) for sd in shipping_dates)
                                has_normal_date = any(is_normal_date(sd) for sd in shipping_dates)
                                
                                # 重複として扱うケース：
                                # 1. 当日洗浄品と先行検査品の両方が存在する場合
                                # 2. 当日洗浄品と通常品の両方が存在する場合
                                # 3. 先行検査品と通常品の両方が存在する場合
                                is_duplicate_combination = False
                                duplicate_type = []
                                
                                if has_cleaning and has_pre_inspection:
                                    is_duplicate_combination = True
                                    duplicate_type.append("当日洗浄品+先行検査品")
                                if has_cleaning and has_normal_date:
                                    is_duplicate_combination = True
                                    duplicate_type.append("当日洗浄品+通常品")
                                if has_pre_inspection and has_normal_date:
                                    is_duplicate_combination = True
                                    duplicate_type.append("先行検査品+通常品")
                                
                                if is_duplicate_combination:
                                    duplicate_count += 1
                                    # 号機・指示日の情報も取得
                                    machine_info = []
                                    instruction_info = []
                                    lot_id_info = []
                                    if '号機' in group.columns:
                                        machine_info = group['号機'].dropna().unique().tolist()
                                    if '指示日' in group.columns:
                                        instruction_info = group['指示日'].dropna().unique().tolist()
                                    if '生産ロットID' in group.columns:
                                        lot_id_info = group['生産ロットID'].dropna().unique().tolist()
                                    
                                    key_str = ", ".join([f"{col}='{val}'" for col, val in zip(check_cols, key)]) if isinstance(key, tuple) else f"品番='{key}'"
                                    detailed_logs.append({
                                        'key': key_str,
                                        'count': len(group),
                                        'shipping_dates': shipping_dates,
                                        'machines': machine_info,
                                        'instructions': instruction_info,
                                        'lot_ids': lot_id_info,
                                        'type': duplicate_type
                                    })
                        
                        if duplicate_count > 0:
                            self.log_message(f"【重複検出】全行での当日洗浄品・先行検査品・通常品（品番のみでグループ化）: {duplicate_count}件の組み合わせに重複があります")
                        
                        # 優先度を計算してソートキーを追加
                        priority_tuples = result_df['出荷予定日'].apply(get_shipping_date_priority)
                        result_df['_priority'] = priority_tuples.apply(
                            lambda x: x[0] if isinstance(x, tuple) else 5
                        )
                        
                        # 優先度でソート（優先度が小さい順 = 優先度の高いものが先に来る）
                        result_df = result_df.sort_values('_priority', na_position='last')
                        
                        # 重複を削除: 品番のみでグループ化して、重複が存在する場合のみ削除
                        # （号機や指示日が異なる場合でも、同じ品番で「当日洗浄品」と「先行検査品」などが混在する場合は重複として扱う）
                        def should_remove_duplicate(group):
                            """
                            重複として扱うケース：
                            1. 当日洗浄品と先行検査品の両方が存在する場合
                            2. 当日洗浄品と通常品の両方が存在する場合
                            3. 先行検査品と通常品の両方が存在する場合
                            
                            ただし、以下の場合は重複として扱わない（区別要因がある）：
                            - 有効な生産ロットIDが異なる場合
                            - 号機が異なる場合（号機が存在し、かつ全ての行で有効な値がある場合）
                            - 指示日が異なる場合（指示日が存在し、かつ全ての行で有効な値がある場合）
                            
                            注意: Stage 4では「品番」のみでグループ化しているため、
                            同じ品番で出荷予定日の種類が混在する場合でも、区別要因があれば重複として扱わない
                            """
                            shipping_dates = group['出荷予定日'].tolist()
                            has_cleaning = any("当日洗浄" in str(sd) for sd in shipping_dates)
                            has_pre_inspection = any("先行検査" in str(sd) for sd in shipping_dates)
                            has_normal_date = any(is_normal_date(sd) for sd in shipping_dates)
                            
                            # 当日洗浄品と先行検査品の両方が存在する場合、または
                            # 当日洗浄品と通常品の両方が存在する場合、または
                            # 先行検査品と通常品の両方が存在する場合
                            has_duplicate_combination = (has_cleaning and has_pre_inspection) or (has_cleaning and has_normal_date) or (has_pre_inspection and has_normal_date)
                            
                            if not has_duplicate_combination:
                                return False
                            
                            # 区別要因をチェック
                            # 1. 生産ロットIDが異なる場合は重複として扱わない
                            if '生産ロットID' in group.columns:
                                # NaNと空文字列を除外して、有効な生産ロットIDのみを取得
                                valid_lot_ids = group['生産ロットID'].apply(
                                    lambda x: x if pd.notna(x) and str(x).strip() != '' else None
                                ).dropna().unique()
                                if len(valid_lot_ids) > 1:
                                    # 有効な生産ロットIDが複数存在する場合は重複として扱わない
                                    return False
                            
                            # 2. 号機が異なる場合は重複として扱わない（号機が存在し、かつ全ての行で有効な値がある場合）
                            if '号機' in group.columns:
                                # NaNと空文字列を除外して、有効な号機のみを取得
                                valid_machines = group['号機'].apply(
                                    lambda x: x if pd.notna(x) and str(x).strip() != '' else None
                                ).dropna().unique()
                                # 全ての行に有効な号機がある場合のみ、号機の違いを区別要因とする
                                if len(valid_machines) > 1 and len(valid_machines) == len(group):
                                    # 有効な号機が複数存在し、かつ全ての行に有効な号機がある場合は重複として扱わない
                                    return False
                            
                            # 3. 指示日が異なる場合は重複として扱わない（指示日が存在し、かつ全ての行で有効な値がある場合）
                            if '指示日' in group.columns:
                                # NaNと空文字列を除外して、有効な指示日のみを取得
                                valid_instructions = group['指示日'].apply(
                                    lambda x: x if pd.notna(x) and str(x).strip() != '' else None
                                ).dropna().unique()
                                # 全ての行に有効な指示日がある場合のみ、指示日の違いを区別要因とする
                                if len(valid_instructions) > 1 and len(valid_instructions) == len(group):
                                    # 有効な指示日が複数存在し、かつ全ての行に有効な指示日がある場合は重複として扱わない
                                    return False
                            
                            # 区別要因がない、または全て同じ場合は重複として扱う
                            return True
                        
                        # 品番のみでグループ化（Stage 4では品番のみでグループ化）
                        # ただし、重複削除は号機・指示日・生産ロットIDの組み合わせごとに行う
                        product_groups = result_df.groupby(check_cols)
                        
                        rows_to_keep = []
                        removed_in_stage4 = 0
                        detailed_removal_logs = []
                        
                        for key, product_group in product_groups:
                            if len(product_group) > 1:
                                # 詳細ログ用の情報を取得
                                shipping_dates_all = product_group['出荷予定日'].tolist()
                                key_str = ", ".join([f"{col}='{val}'" for col, val in zip(check_cols, key)]) if isinstance(key, tuple) else f"品番='{key}'"
                                
                                # 品番グループ内で、号機・指示日・生産ロットIDの組み合わせでさらにグループ化
                                # 各組み合わせ内で重複をチェック
                                sub_group_cols = []
                                if '号機' in product_group.columns:
                                    sub_group_cols.append('号機')
                                if '指示日' in product_group.columns:
                                    sub_group_cols.append('指示日')
                                if '生産ロットID' in product_group.columns:
                                    sub_group_cols.append('生産ロットID')
                                
                                # サブグループがない場合は、品番グループ全体を1つのサブグループとして扱う
                                if not sub_group_cols:
                                    sub_groups = [(None, product_group)]
                                else:
                                    # サブグループを作成（有効な値のみを使用）
                                    def get_sub_group_key(row):
                                        """サブグループのキーを取得（有効な値のみを使用）"""
                                        key_parts = []
                                        for col in sub_group_cols:
                                            val = row[col]
                                            if pd.notna(val) and str(val).strip() != '':
                                                key_parts.append(str(val).strip())
                                            else:
                                                key_parts.append('__EMPTY__')
                                        return tuple(key_parts)
                                    
                                    product_group['_sub_key'] = product_group.apply(get_sub_group_key, axis=1)
                                    sub_groups = list(product_group.groupby('_sub_key'))
                                
                                # 各サブグループで重複をチェック
                                for sub_key, sub_group in sub_groups:
                                    if len(sub_group) > 1:
                                        # サブグループ内で重複をチェック
                                        should_remove = should_remove_duplicate(sub_group)
                                        
                                        if should_remove:
                                            # 重複が存在する場合、優先度の高い方のみ残す
                                            # 既に優先度順にソート済みなので、最初の1件のみ残す
                                            rows_to_keep.append(sub_group.iloc[0:1].drop(columns=['_priority', '_sub_key'], errors='ignore'))
                                            removed_count = len(sub_group) - 1
                                            removed_in_stage4 += removed_count
                                            
                                            # 削除された行の詳細を記録
                                            sub_shipping_dates = sub_group['出荷予定日'].tolist()
                                            sub_priorities = sub_group['_priority'].tolist()
                                            sub_machines = sub_group['号機'].tolist() if '号機' in sub_group.columns else []
                                            sub_instructions = sub_group['指示日'].tolist() if '指示日' in sub_group.columns else []
                                            sub_lot_ids = sub_group['生産ロットID'].tolist() if '生産ロットID' in sub_group.columns else []
                                            
                                            detailed_removal_logs.append({
                                                'key': key_str,
                                                'total': len(sub_group),
                                                'kept': 1,
                                                'removed': removed_count,
                                                'shipping_dates': sub_shipping_dates,
                                                'priorities': sub_priorities,
                                                'machines': sub_machines,
                                                'instructions': sub_instructions,
                                                'lot_ids': sub_lot_ids
                                            })
                                        else:
                                            # 重複がない場合は全て残す
                                            rows_to_keep.append(sub_group.drop(columns=['_priority', '_sub_key'], errors='ignore'))
                                    else:
                                        # 1件のみの場合はそのまま残す
                                        rows_to_keep.append(sub_group.drop(columns=['_priority', '_sub_key'], errors='ignore'))
                            else:
                                # 1件のみの場合はそのまま残す
                                rows_to_keep.append(product_group.drop(columns=['_priority', '_sub_key'], errors='ignore'))
                        
                        if rows_to_keep:
                            result_df = pd.concat(rows_to_keep, ignore_index=True)
                        
                        
                        removed_stage4_count = before_stage4_count - len(result_df)
                        total_removed += removed_stage4_count
                        if removed_stage4_count > 0:
                            self.log_message(f"【重複削除】全行での当日洗浄品・先行検査品・通常品: {removed_stage4_count}件を削除しました（残り: {len(result_df)}件）")
                        elif removed_in_stage4 == 0:
                            self.log_message(f"【ステージ4】全行での当日洗浄品・先行検査品・通常品の重複は検出されませんでした")
                
                if total_removed > 0:
                    self.log_message(f"ロットID重複削除: {total_removed}件の重複ロットを削除しました（残り: {len(result_df)}件）")
                
                return result_df
            else:
                return assignment_df
            
        except Exception as e:
            self.log_message(f"ロットID重複削除中にエラーが発生しました: {str(e)}")
            logger.error(f"ロットID重複削除エラー: {str(e)}", exc_info=True)
            # エラーが発生した場合は元のDataFrameを返す
            return assignment_df
    
    def process_lot_assignment(self, connection, main_df, start_progress=0.65):
        """ロット割り当て処理のメイン処理"""
        try:
            cleaning_lots_df = pd.DataFrame()
            # 不足数がマイナスのデータを抽出
            self.update_progress(start_progress + 0.03, "不足データを抽出中...")
            # main_dfが空の場合でも処理を続行できるようにする
            if main_df.empty or '不足数' not in main_df.columns:
                shortage_df = pd.DataFrame()
                self.log_message("出荷予定日からのデータがありません。先行検査品と洗浄品の処理を続行します...")
            else:
                with perf_timer(logger, "lot_assignment.shortage.extract"):
                    shortage_df = main_df[main_df['不足数'] < 0].copy()
            
            if shortage_df.empty:
                self.log_message("不足数がマイナスのデータがありません。先行検査品と洗浄品の処理を続行します...")
                # 不足数がマイナスのデータが無い場合でも、先行検査品と洗浄品の処理を続行
                lots_df = pd.DataFrame()
            else:
                self.log_message(f"不足数がマイナスのデータ: {len(shortage_df)}件")
                
                # 通常の在庫ロットを取得（取得中に進捗が止まって見えないようにパルス表示）
                self.update_progress(start_progress + 0.08, "利用可能なロットを取得中...")
                self.start_progress_pulse(start_progress + 0.08, start_progress + 0.22, "利用可能なロットを取得中...")

                with perf_timer(logger, "lots.get_available_for_shortage"):
                    lots_df = self.get_available_lots_for_shortage(connection, shortage_df)

                self.stop_progress_pulse(
                    final_value=start_progress + 0.22,
                    message=f"利用可能なロット取得完了: {len(lots_df)}件",
                )
                 
                # 【追加】検査対象外ロット情報を取得（参考情報として）
                try:
                    self.update_progress(start_progress + 0.22, "検査対象外ロット情報を取得中...")
                    self.start_progress_pulse(start_progress + 0.22, start_progress + 0.25, "検査対象外ロット情報を取得中...")
                    with perf_timer(logger, "lots.get_non_inspection_target"):
                        self.log_non_inspection_lots_info(connection, shortage_df)
                    self.stop_progress_pulse(final_value=start_progress + 0.25, message="検査対象外ロット情報の取得が完了しました")
                except Exception as e:
                    self.log_message(f"検査対象外ロット情報の取得中にエラーが発生しました: {str(e)}")
                    logger.error(f"検査対象外ロット情報取得エラー: {e}", exc_info=True)
             
            # 洗浄二次処理依頼からロットを取得（追加で取得）
            if (
                cleaning_lots_df.empty
                and self.config.google_sheets_url_cleaning
                and self.config.google_sheets_url_cleaning_instructions
                and self.config.google_sheets_credentials_path
            ):
                try:
                    self.update_progress(start_progress + 0.25, "洗浄二次処理依頼からロットを取得中...")
                    self.start_progress_pulse(start_progress + 0.25, start_progress + 0.29, "洗浄二次処理依頼からロットを取得中...")
                    with perf_timer(logger, "lots.get_cleaning_lots"):
                        cleaning_lots_df = get_cleaning_lots(
                            connection,
                            self.config.google_sheets_url_cleaning,
                            self.config.google_sheets_url_cleaning_instructions,
                            self.config.google_sheets_credentials_path,
                            log_callback=self.log_message,
                            process_master_path=self.config.process_master_path if self.config else None,
                            inspection_target_keywords=self.inspection_target_keywords
                        )
                    self.stop_progress_pulse(final_value=start_progress + 0.29, message="洗浄二次処理依頼ロットの取得が完了しました")
                    if not cleaning_lots_df.empty:
                        self.log_message(f"洗浄二次処理依頼から {len(cleaning_lots_df)}件のロットを取得しました")
                    else:
                        self.log_message("洗浄二次処理依頼からロットが取得できませんでした（データが空です）")
                except Exception as e:
                    self.log_message(f"洗浄二次処理依頼からのロット取得中にエラーが発生しました: {str(e)}")
                    import traceback
                    self.log_message(f"エラー詳細: {traceback.format_exc()}")
                    cleaning_lots_df = pd.DataFrame()
            
            # 洗浄二次処理依頼のロットを統合
            # 注意: 通常の在庫ロットの情報（出荷予定日を含む）は一切変更しない
            if not cleaning_lots_df.empty:
                # 洗浄関連のロットのみに出荷予定日を設定（通常の在庫ロットには影響しない）
                if '出荷予定日' not in cleaning_lots_df.columns:
                    cleaning_lots_df['出荷予定日'] = "当日洗浄上がり品"
                else:
                    # 洗浄関連のロットのみに出荷予定日を設定
                    cleaning_lots_df['出荷予定日'] = "当日洗浄上がり品"
                
                if lots_df.empty:
                    lots_df = cleaning_lots_df
                    self.log_message(f"洗浄二次処理依頼のロット {len(cleaning_lots_df)}件を振分け対象として設定しました")
                else:
                    # 統合前の通常ロット数を記録
                    normal_lots_count = len(lots_df)
                    
                    # 統合前に、通常の在庫ロットの生産ロットIDと出荷予定日を記録（出荷予定日を保護するため）
                    normal_lot_ids = set()
                    normal_lot_shipping_dates = {}
                    if '生産ロットID' in lots_df.columns:
                        normal_lot_ids = set(lots_df['生産ロットID'].dropna())
                        # 通常の在庫ロットの出荷予定日を記録（存在する場合）
                        if '出荷予定日' in lots_df.columns:
                            # iterrows()を避けて高速化
                            lot_id_col = lots_df['生産ロットID']
                            shipping_date_col = lots_df['出荷予定日']
                            for lot_id in normal_lot_ids:
                                mask = lot_id_col == lot_id
                                if mask.any():
                                    shipping_date = shipping_date_col[mask].iloc[0]
                                    normal_lot_shipping_dates[lot_id] = shipping_date
                    
                    # 既存のロットと統合（重複を避ける）
                    if '生産ロットID' in lots_df.columns and '生産ロットID' in cleaning_lots_df.columns:
                        # 既存のロットIDを除外
                        existing_lot_ids = set(lots_df['生産ロットID'].dropna())
                        cleaning_lots_df_filtered = cleaning_lots_df[
                            ~cleaning_lots_df['生産ロットID'].isin(existing_lot_ids)
                        ]
                        if not cleaning_lots_df_filtered.empty:
                            # 洗浄二次処理依頼から取得したロットの生産ロットIDを記録（出荷予定日を保護するため）
                            cleaning_lot_ids = set(cleaning_lots_df_filtered['生産ロットID'].dropna())
                            
                            # 統合（通常の在庫ロットの出荷予定日は変更しない）
                            lots_df = pd.concat([lots_df, cleaning_lots_df_filtered], ignore_index=True)
                            
                            # 統合後、通常の在庫ロットの出荷予定日を復元
                            if '出荷予定日' in lots_df.columns and '生産ロットID' in lots_df.columns:
                                # 通常の在庫ロットの出荷予定日を復元（記録した値またはNone）
                                normal_lots_mask = lots_df['生産ロットID'].isin(normal_lot_ids)
                                if normal_lots_mask.any():
                                    # 記録した出荷予定日を一括で復元
                                    for lot_id, shipping_date in normal_lot_shipping_dates.items():
                                        lot_mask = (lots_df['生産ロットID'] == lot_id) & normal_lots_mask
                                        if lot_mask.any():
                                            lots_df.loc[lot_mask, '出荷予定日'] = shipping_date
                                    
                                    # 記録がない通常の在庫ロットの出荷予定日をNoneに設定
                                    recorded_lot_ids = set(normal_lot_shipping_dates.keys())
                                    unrecorded_mask = normal_lots_mask & ~lots_df['生産ロットID'].isin(recorded_lot_ids)
                                    if unrecorded_mask.any():
                                        lots_df.loc[unrecorded_mask, '出荷予定日'] = None
                                
                                # 洗浄二次処理依頼から取得したロットの出荷予定日を「当日洗浄上がり品」に確実に設定
                                cleaning_lots_mask = lots_df['生産ロットID'].isin(cleaning_lot_ids)
                                if cleaning_lots_mask.any():
                                    lots_df.loc[cleaning_lots_mask, '出荷予定日'] = "当日洗浄上がり品"
                            
                            self.log_message(f"洗浄二次処理依頼のロット {len(cleaning_lots_df_filtered)}件を統合しました（通常ロット: {normal_lots_count}件、合計: {len(lots_df)}件）")
                        else:
                            self.log_message(f"洗浄二次処理依頼のロットは全て重複していたため追加しませんでした（通常ロット: {normal_lots_count}件）")
                    else:
                        # 洗浄二次処理依頼から取得したロットの生産ロットIDを記録（出荷予定日を保護するため）
                        cleaning_lot_ids = set()
                        if '生産ロットID' in cleaning_lots_df.columns:
                            cleaning_lot_ids = set(cleaning_lots_df['生産ロットID'].dropna())
                        
                        # 統合（通常の在庫ロットの出荷予定日は変更しない）
                        lots_df = pd.concat([lots_df, cleaning_lots_df], ignore_index=True)
                        
                        # 統合後、通常の在庫ロットの出荷予定日を復元
                        if '出荷予定日' in lots_df.columns and '生産ロットID' in lots_df.columns and normal_lot_ids:
                            # 通常の在庫ロットの出荷予定日を復元（記録した値またはNone）
                            normal_lots_mask = lots_df['生産ロットID'].isin(normal_lot_ids)
                            if normal_lots_mask.any():
                                # 記録した出荷予定日を一括で復元
                                for lot_id, shipping_date in normal_lot_shipping_dates.items():
                                    lot_mask = (lots_df['生産ロットID'] == lot_id) & normal_lots_mask
                                    if lot_mask.any():
                                        lots_df.loc[lot_mask, '出荷予定日'] = shipping_date
                                
                                # 記録がない通常の在庫ロットの出荷予定日をNoneに設定
                                recorded_lot_ids = set(normal_lot_shipping_dates.keys())
                                unrecorded_mask = normal_lots_mask & ~lots_df['生産ロットID'].isin(recorded_lot_ids)
                                if unrecorded_mask.any():
                                    lots_df.loc[unrecorded_mask, '出荷予定日'] = None
                        
                        # 洗浄二次処理依頼から取得したロットの出荷予定日を「当日洗浄上がり品」に確実に設定
                        if '出荷予定日' in lots_df.columns and '生産ロットID' in lots_df.columns and cleaning_lot_ids:
                            cleaning_lots_mask = lots_df['生産ロットID'].isin(cleaning_lot_ids)
                            if cleaning_lots_mask.any():
                                lots_df.loc[cleaning_lots_mask, '出荷予定日'] = "当日洗浄上がり品"
                        
                        self.log_message(f"洗浄二次処理依頼のロット {len(cleaning_lots_df)}件を統合しました（通常ロット: {normal_lots_count}件、合計: {len(lots_df)}件）")
            
            # ロット割り当てを実行（不足数がマイナスのデータがある場合のみ）
            assignment_df = pd.DataFrame()
            if not shortage_df.empty and not lots_df.empty:
                self.update_progress(start_progress + 0.28, "ロットを割り当て中...")
                with perf_timer(logger, "lot_assignment.assign_lots_to_shortage"):
                    assignment_df = self.assign_lots_to_shortage(shortage_df, lots_df)
            elif lots_df.empty and shortage_df.empty:
                # 出荷予定日からのデータが無い場合、assignment_dfを空のDataFrameで初期化
                assignment_df = pd.DataFrame()
                self.log_message("出荷予定日からのデータが無いため、先行検査品と洗浄品の処理を続行します...")
            
            # 登録済み品番のロットを割り当て（追加）
            if self.registered_products:
                self.update_progress(start_progress + 0.30, "登録済み品番のロットを割り当て中...")
                with perf_timer(logger, "lots.assign_registered_products"):
                    assignment_df = self.assign_registered_products_lots(connection, main_df, assignment_df)
            
            # 洗浄二次処理依頼のロットを追加（不足数がマイナスの品番と一致するものも含む）
            if not cleaning_lots_df.empty:
                # 不足数がマイナスの品番リストを取得（shortage_dfが空の場合は空のセット）
                shortage_product_numbers = set(shortage_df['品番'].unique()) if not shortage_df.empty else set()
                
                # 洗浄二次処理依頼のロットで、不足数がマイナスの品番と一致しないものを抽出
                cleaning_lots_not_in_shortage = cleaning_lots_df[
                    ~cleaning_lots_df['品番'].isin(shortage_product_numbers)
                ].copy()
                
                # 洗浄二次処理依頼のロットで、不足数がマイナスの品番と一致するものを抽出
                # assign_lots_to_shortageで処理されなかったロットを追加するため
                cleaning_lots_in_shortage = cleaning_lots_df[
                    cleaning_lots_df['品番'].isin(shortage_product_numbers)
                ].copy()
                
                # assign_lots_to_shortageで既に割り当てられたロットIDを取得
                assigned_lot_ids = set()
                if not assignment_df.empty and '生産ロットID' in assignment_df.columns:
                    lot_ids = assignment_df['生産ロットID'].dropna().astype(str).map(str.strip)
                    assigned_lot_ids = set(lot_ids[lot_ids != ''].unique())

                assigned_cleaning_row_ids = set()
                if not assignment_df.empty and '洗浄指示_行番号' in assignment_df.columns:
                    row_ids = assignment_df['洗浄指示_行番号'].dropna().astype(str).map(str.strip)
                    assigned_cleaning_row_ids = set(row_ids[row_ids != ''].unique())
                
                # 不足数がマイナスの品番と一致するが、まだ割り当てられていないロットを抽出
                cleaning_lots_in_shortage_not_assigned = cleaning_lots_in_shortage.copy()
                if not cleaning_lots_in_shortage_not_assigned.empty:
                    keep_mask = pd.Series([True] * len(cleaning_lots_in_shortage_not_assigned), index=cleaning_lots_in_shortage_not_assigned.index)

                    if '生産ロットID' in cleaning_lots_in_shortage_not_assigned.columns:
                        lot_id_series = (
                            cleaning_lots_in_shortage_not_assigned['生産ロットID']
                            .fillna('')
                            .astype(str)
                            .map(str.strip)
                        )
                        has_lot_id_mask = lot_id_series != ''
                        if assigned_lot_ids:
                            keep_mask.loc[has_lot_id_mask] = ~lot_id_series[has_lot_id_mask].isin(assigned_lot_ids)

                        if '洗浄指示_行番号' in cleaning_lots_in_shortage_not_assigned.columns and assigned_cleaning_row_ids:
                            row_id_series = (
                                cleaning_lots_in_shortage_not_assigned['洗浄指示_行番号']
                                .fillna('')
                                .astype(str)
                                .map(str.strip)
                            )
                            has_row_id_mask = row_id_series != ''
                            no_lot_id_has_row_id = (~has_lot_id_mask) & has_row_id_mask
                            keep_mask.loc[no_lot_id_has_row_id] = ~row_id_series[no_lot_id_has_row_id].isin(assigned_cleaning_row_ids)
                    elif '洗浄指示_行番号' in cleaning_lots_in_shortage_not_assigned.columns and assigned_cleaning_row_ids:
                        row_id_series = (
                            cleaning_lots_in_shortage_not_assigned['洗浄指示_行番号']
                            .fillna('')
                            .astype(str)
                            .map(str.strip)
                        )
                        has_row_id_mask = row_id_series != ''
                        keep_mask.loc[has_row_id_mask] = ~row_id_series[has_row_id_mask].isin(assigned_cleaning_row_ids)

                    cleaning_lots_in_shortage_not_assigned = cleaning_lots_in_shortage_not_assigned[keep_mask].copy()
                
                # 不足数がマイナスの品番と一致しないものと、一致するが未割当のものを統合
                all_additional_cleaning_lots = pd.DataFrame()
                if not cleaning_lots_not_in_shortage.empty and not cleaning_lots_in_shortage_not_assigned.empty:
                    all_additional_cleaning_lots = pd.concat([
                        cleaning_lots_not_in_shortage,
                        cleaning_lots_in_shortage_not_assigned
                    ], ignore_index=True)
                elif not cleaning_lots_not_in_shortage.empty:
                    all_additional_cleaning_lots = cleaning_lots_not_in_shortage
                elif not cleaning_lots_in_shortage_not_assigned.empty:
                    all_additional_cleaning_lots = cleaning_lots_in_shortage_not_assigned
                
                if not all_additional_cleaning_lots.empty:
                    # 洗浄二次処理依頼から取得したロットの出荷予定日を「当日洗浄上がり品」に確実に設定
                    if '出荷予定日' in all_additional_cleaning_lots.columns:
                        all_additional_cleaning_lots['出荷予定日'] = "当日洗浄上がり品"
                    else:
                        all_additional_cleaning_lots['出荷予定日'] = "当日洗浄上がり品"
                    
                    # これらのロットを独立したロットとして追加
                    additional_assignments = []
                    # 列名から列インデックスへのマッピングを作成（高速化：itertuples()を使用）
                    lot_col_idx_map = {col: all_additional_cleaning_lots.columns.get_loc(col) for col in all_additional_cleaning_lots.columns}
                    
                    with perf_timer(logger, "lot_assignment.add_additional_cleaning_lots"):
                        for row_tuple in all_additional_cleaning_lots.itertuples(index=True):
                            lot_row_idx = row_tuple[0]  # インデックス
                            lot_row = all_additional_cleaning_lots.loc[lot_row_idx]  # Seriesとして扱うために元の行を取得
                        
                            # 品番がmain_dfに存在するか確認（main_dfが空の場合でもエラーが発生しないようにする）
                            product_in_main = pd.DataFrame()
                            if not main_df.empty and '品番' in main_df.columns:
                                product_in_main = main_df[main_df['品番'] == lot_row['品番']]
                            if not product_in_main.empty:
                                # main_dfから該当品番の最初の行を取得
                                main_row = product_in_main.iloc[0]
                                additional_assignment = {
                                    '出荷予定日': "当日洗浄上がり品",  # 洗浄二次処理依頼から取得したロットは常に「当日洗浄上がり品」
                                    '品番': lot_row['品番'],
                                    '品名': lot_row.get('品名', main_row.get('品名', '')),
                                    '客先': lot_row.get('客先', main_row.get('客先', '')),
                                    '出荷数': int(main_row.get('出荷数', 0)),
                                    '在庫数': int(main_row.get('在庫数', 0)),
                                    '在梱包数': int(main_row.get('梱包・完了', 0)),
                                    '不足数': 0,  # 不足数がマイナスでない場合は0
                                    'ロット数量': int(lot_row.get('数量', lot_row.get('ロット数量', 0))),
                                    '指示日': lot_row.get('指示日', ''),
                                    '号機': lot_row.get('号機', ''),
                                    '洗浄指示_行番号': lot_row.get('洗浄指示_行番号', ''),
                                    '現在工程番号': lot_row.get('現在工程番号', ''),
                                    '現在工程名': lot_row.get('現在工程名', ''),
                                    '現在工程二次処理': lot_row.get('現在工程二次処理', ''),
                                    '生産ロットID': lot_row.get('生産ロットID', ''),
                                    '__from_cleaning_sheet': True
                                }
                                additional_assignments.append(additional_assignment)
                            else:
                                # main_dfに存在しない場合は、ロットの情報のみを使用
                                additional_assignment = {
                                    '出荷予定日': "当日洗浄上がり品",  # 洗浄二次処理依頼から取得したロットは常に「当日洗浄上がり品」
                                    '品番': lot_row['品番'],
                                    '品名': lot_row.get('品名', ''),
                                    '客先': lot_row.get('客先', ''),
                                    '出荷数': 0,
                                    '在庫数': 0,
                                    '在梱包数': 0,
                                    '不足数': 0,
                                    'ロット数量': int(lot_row.get('数量', lot_row.get('ロット数量', 0))),
                                    '指示日': lot_row.get('指示日', ''),
                                    '号機': lot_row.get('号機', ''),
                                    '洗浄指示_行番号': lot_row.get('洗浄指示_行番号', ''),
                                    '現在工程番号': lot_row.get('現在工程番号', ''),
                                    '現在工程名': lot_row.get('現在工程名', ''),
                                    '現在工程二次処理': lot_row.get('現在工程二次処理', ''),
                                    '生産ロットID': lot_row.get('生産ロットID', ''),
                                    '__from_cleaning_sheet': True
                                }
                                additional_assignments.append(additional_assignment)
                    
                    if additional_assignments:
                        additional_df = pd.DataFrame(additional_assignments)
                        if assignment_df.empty:
                            assignment_df = additional_df
                        else:
                            assignment_df = pd.concat([assignment_df, additional_df], ignore_index=True)
                        not_in_shortage_count = len(cleaning_lots_not_in_shortage) if not cleaning_lots_not_in_shortage.empty else 0
                        in_shortage_not_assigned_count = len(cleaning_lots_in_shortage_not_assigned) if not cleaning_lots_in_shortage_not_assigned.empty else 0
                        self.log_message(f"洗浄二次処理依頼のロット {len(additional_df)}件を追加しました（不足数マイナス以外: {not_in_shortage_count}件、不足数マイナスで未割当: {in_shortage_not_assigned_count}件）")
            
            if not assignment_df.empty:
                # ロットIDの重複を削除（出荷予定日の優先順位に基づいて）
                with perf_timer(logger, "lot_assignment.remove_duplicate_lot_ids"):
                    assignment_df = self.remove_duplicate_lot_ids(assignment_df)
                
                # ロット割り当て結果は選択式表示のため、ここでは表示しない
                # self.display_lot_assignment_table(assignment_df)
                
                # ロット割り当てデータを保存（エクスポート用）
                self.current_assignment_data = assignment_df

                # ロット抽出（割当）結果の不変性チェック用
                self._log_df_signature(
                    "lot_assignment.assignment_df",
                    assignment_df,
                    sort_keys=["生産ロットID", "品番", "出荷予定日"],
                )
                self._save_and_log_snapshot("lot_assignment.assignment_df", assignment_df)
                 
                # 検査員割振り処理を実行（進捗は連続させる）
                # ロット割り当て: start_progress〜start_progress+0.20
                # 検査員割振り: 0.40〜0.90
                with perf_timer(logger, "inspector_assignment"):
                    self.update_progress(0.40, "検査員割振り処理中...")
                    self.process_inspector_assignment(assignment_df, start_progress=0.40)
            else:
                self.log_message("ロット割り当て結果がありません")
                
        except Exception as e:
            self.log_message(f"ロット割り当て処理中にエラーが発生しました: {str(e)}")
    
    def process_inspector_assignment(self, assignment_df, start_progress=0.1):
        """検査員割振り処理を実行"""
        try:
            if assignment_df.empty:
                self.log_message("ロット割り当て結果がありません")
                return
            
            # 進捗配分（検査員割当 0.40-0.90）
            phase_start = float(start_progress)
            phase_end = 0.90
            if phase_end <= phase_start:
                phase_end = min(1.0, phase_start + 0.01)

            master_end = min(phase_end, phase_start + 0.10)  # 0.40-0.50
            table_end = min(phase_end, master_end + 0.05)    # 0.50-0.55
            assign_start = table_end                          # 0.55-
            assign_end = phase_end                            # -0.90

            # マスタファイルを並列で読み込み（高速化）
            progress_base = phase_start
            progress_range_master = max(0.01, master_end - phase_start)

            self.update_progress(progress_base, "マスタファイルを読み込み中...")
            self.start_progress_pulse(progress_base, master_end - 0.001, "マスタファイルを読み込み中...")
            with perf_timer(logger, "masters.load_parallel"):
                masters = self.load_masters_parallel(progress_base=progress_base, progress_range=progress_range_master)
            self.stop_progress_pulse()
            
            product_master_df = masters.get('product')
            inspector_master_df = masters.get('inspector')
            skill_master_df = masters.get('skill')
            
            # 必須マスタファイルの読み込み失敗をチェック
            missing_masters = []
            if product_master_df is None:
                missing_masters.append("製品マスタ")
                self.log_message("製品マスタの読み込みに失敗しました")
            
            if inspector_master_df is None:
                missing_masters.append("検査員マスタ")
                self.log_message("検査員マスタの読み込みに失敗しました")
            
            if skill_master_df is None:
                missing_masters.append("スキルマスタ")
                self.log_message("スキルマスタの読み込みに失敗しました")
            
            # 必須マスタファイルが読み込めなかった場合、ユーザーに通知
            if missing_masters:
                error_msg = (
                    "必須マスタファイルの読み込みに失敗しました。\n\n"
                    f"読み込み失敗: {', '.join(missing_masters)}\n\n"
                    "以下の点を確認してください：\n"
                    "1. config.envファイルのパス設定が正しいか\n"
                    "2. マスタファイルが存在し、アクセス可能か\n"
                    "3. ファイルが他のアプリケーションで開かれていないか\n"
                    "4. ネットワークパスの場合、接続が確立されているか\n\n"
                    "ログファイルを確認してください。"
                )
                self.root.after(0, lambda: messagebox.showerror("マスタファイル読み込みエラー", error_msg))
                return
            
            # マスタデータを保存
            self.inspector_master_data = inspector_master_df
            self.skill_master_data = skill_master_df
            
            # 検査員割振りテーブルを作成（製品マスタパスを渡す）
            self.update_progress(master_end, "検査員割振りテーブルを作成中...")
            self.start_progress_pulse(master_end, table_end - 0.001, "検査員割振りテーブルを作成中...")
            product_master_path = self.config.product_master_path if self.config else None
            process_master_path = self.config.process_master_path if self.config else None
            with perf_timer(logger, "inspection_target_csv.load"):
                inspection_target_keywords = self.load_inspection_target_csv()
            
            with perf_timer(logger, "inspector_assignment.create_table"):
                inspector_df = self.inspector_manager.create_inspector_assignment_table(
                    assignment_df,
                    product_master_df,
                    product_master_path=product_master_path,
                    process_master_path=process_master_path,
                    inspection_target_keywords=inspection_target_keywords
                )
            if inspector_df is None:
                self.log_message("検査員割振りテーブルの作成に失敗しました")
                return
            self.stop_progress_pulse(final_value=table_end, message="検査員割振りテーブルの作成が完了しました")
            
            # 製品マスタが更新された場合は再読み込み
            if product_master_path and Path(product_master_path).exists():
                # 再読み込みは次の処理で行うため、ここではログのみ
                pass
            
            # 工程マスタを読み込む（検査員割当て用）
            process_master_df = None
            if process_master_path:
                process_master_df = self.inspector_manager.load_process_master(process_master_path)
            
            # 固定検査員情報を設定
            self._set_fixed_inspectors_to_manager()
            
            # 検査員を割り当て（スキル値付きで保存）
            self.update_progress(assign_start, "検査員を割り当て中...")
            self.start_progress_pulse(assign_start, assign_end - 0.01, "検査員を割り当て中...")
            with perf_timer(logger, "inspector_assignment.assign_inspectors"):
                inspector_df_with_skills = self.inspector_manager.assign_inspectors(
                    inspector_df, 
                    inspector_master_df, 
                    skill_master_df, 
                    show_skill_values=True,
                    process_master_df=process_master_df,
                    inspection_target_keywords=inspection_target_keywords
                )
            self.stop_progress_pulse()

            # 振分結果（スキル値付き）の不変性チェック用
            self._log_df_signature(
                "inspector_assignment.result_with_skills",
                inspector_df_with_skills,
                sort_keys=["生産ロットID", "品番", "出荷予定日"],
                include_columns=[
                    "生産ロットID",
                    "品番",
                    "品名",
                    "客先",
                    "出荷予定日",
                    "検査時間",
                    "分割検査時間",
                    *[f"検査員{i}" for i in range(1, MAX_INSPECTORS_PER_LOT + 1)],
                    "検査員人数",
                    "remaining_work_hours",
                    "assignability_status",
                    "チーム情報",
                ],
            )
            self._save_and_log_snapshot("inspector_assignment.result_with_skills", inspector_df_with_skills)
            # 振分結果（検査員割当の差分）をロット単位で特定できるようにスナップショット化（氏名はハッシュ化）
            self._save_and_log_assignment_diff_snapshot(
                "inspector_assignment.assignment_diff_with_skills",
                inspector_df_with_skills,
            )
             
            # 表示用のデータは氏名のみ
            with perf_timer(logger, "inspector_assignment.display_name_strip"):
                inspector_df = inspector_df_with_skills.copy()
                for col in inspector_df.columns:
                    if col.startswith('検査員'):
                        inspector_df[col] = inspector_df[col].astype(str).apply(
                            lambda x: x.split('(')[0].strip() if '(' in x and ')' in x else x
                        )

            # 振分結果（表示用）の不変性チェック用
            self._log_df_signature(
                "inspector_assignment.result_display",
                inspector_df,
                sort_keys=["生産ロットID", "品番", "出荷予定日"],
                include_columns=[
                    "生産ロットID",
                    "品番",
                    "品名",
                    "客先",
                    "出荷予定日",
                    "検査時間",
                    "分割検査時間",
                    *[f"検査員{i}" for i in range(1, MAX_INSPECTORS_PER_LOT + 1)],
                    "検査員人数",
                    "remaining_work_hours",
                    "assignability_status",
                    "チーム情報",
                ],
            )
            self._save_and_log_snapshot("inspector_assignment.result_display", inspector_df)
            self._save_and_log_assignment_diff_snapshot(
                "inspector_assignment.assignment_diff_display",
                inspector_df,
            )
            
            # 検査員割振りデータを保存（エクスポート用）
            self.current_inspector_data = inspector_df
            self.original_inspector_data = inspector_df_with_skills.copy()  # スキル値付きの元データを保持
            
            # 表示フェーズへ（0.90-1.00）は呼び出し元側で進める
            self.update_progress(assign_end, "検査員割振り処理が完了しました")
            self.log_message(f"検査員割振り処理が完了しました: {len(inspector_df)}件")
            
            # メインスレッドでテーブル表示を指示
            self.root.after(0, self._refresh_inspector_table_post_assignment)
            
        except Exception as e:
            self.log_message(f"検査員割振り処理中にエラーが発生しました: {str(e)}")

    def _refresh_inspector_table_post_assignment(self):
        """検査員割振り後にメインスレッドでテーブルを表示"""
        try:
            if self.current_inspector_data is None or self.current_inspector_data.empty:
                return

            self.display_inspector_assignment_table(self.current_inspector_data)
            self.current_display_table = "inspector"
            if hasattr(self, "inspector_button"):
                self.update_button_states("inspector")
        except Exception as e:
            self.log_message(f"検査員テーブル表示中にエラーが発生しました: {str(e)}")
            logger.error(f"検査員テーブル表示中にエラーが発生しました: {str(e)}")
    
    def calculate_cumulative_shortage(self, assignment_df):
        """同一品番の連続行で不足数を累積計算"""
        try:
            if assignment_df.empty:
                return assignment_df
            
            # 出荷予定日昇順、同一品番は指示日古い順でソート（型を統一してからソート）
            # 出荷予定日を文字列に統一してからソート（None/NaNは最後に）
            assignment_df = assignment_df.copy()
            assignment_df['_出荷予定日_ソート用'] = assignment_df['出荷予定日'].apply(
                lambda x: str(x) if pd.notna(x) else ''
            )
            # 指示日も文字列に統一
            if '指示日' in assignment_df.columns:
                assignment_df['_指示日_ソート用'] = assignment_df['指示日'].apply(
                    lambda x: str(x) if pd.notna(x) else ''
                )
                assignment_df = assignment_df.sort_values(['_出荷予定日_ソート用', '品番', '_指示日_ソート用'], na_position='last').reset_index(drop=True)
                assignment_df = assignment_df.drop(columns=['_出荷予定日_ソート用', '_指示日_ソート用'])
            else:
                assignment_df = assignment_df.sort_values(['_出荷予定日_ソート用', '品番'], na_position='last').reset_index(drop=True)
                assignment_df = assignment_df.drop(columns=['_出荷予定日_ソート用'])
            
            # 不足数を再計算（ベクトル化処理で高速化）
            # 品番ごとにグループ化して累積計算
            def calculate_cumulative(group):
                """同一品番グループ内で不足数を累積計算"""
                result = group['不足数'].copy()
                for i in range(1, len(group)):
                    result.iloc[i] = result.iloc[i-1] + group['ロット数量'].iloc[i-1]
                return result
            
            assignment_df['不足数'] = assignment_df.groupby('品番', group_keys=False).apply(calculate_cumulative).reset_index(drop=True)
            
            return assignment_df
            
        except Exception as e:
            self.log_message(f"不足数計算中にエラーが発生しました: {str(e)}")
            return assignment_df
    
    def display_lot_assignment_table(self, assignment_df):
        """ロット割り当て結果テーブルを表示"""
        try:
            if assignment_df.empty:
                return
            
            # 既存のテーブルセクションを削除
            self.hide_current_table()
            
            # ロット割り当て結果セクションを作成
            self.create_lot_assignment_section(assignment_df)
            
        except Exception as e:
            self.log_message(f"ロット割り当てテーブル表示中にエラーが発生しました: {str(e)}")
    
    def create_lot_assignment_section(self, assignment_df):
        """ロット割り当て結果セクションを作成"""
        try:
            # ロット割り当て結果フレーム
            lot_frame = ctk.CTkFrame(self.main_scroll_frame, fg_color="#EFF6FF", corner_radius=12)
            lot_frame.table_section = True
            lot_frame.pack(fill="x", padx=20, pady=(10, 20))
            
            # タイトル
            lot_title = ctk.CTkLabel(
                lot_frame,
                text="ロット割り当て結果",
                font=ctk.CTkFont(family="Yu Gothic", size=18, weight="bold"),
                text_color="#1E3A8A"
            )
            lot_title.pack(pady=(15, 10))
            
            # テーブルフレーム
            lot_table_frame = ctk.CTkFrame(lot_frame, fg_color="white", corner_radius=8)
            lot_table_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
            
            # ロット割り当てテーブルを作成
            self.create_lot_assignment_table(lot_table_frame, assignment_df)
            
            # 検査員割振りボタンは削除（テーブル選択ボタンで操作）
            
        except Exception as e:
            self.log_message(f"ロット割り当てセクション作成中にエラーが発生しました: {str(e)}")
    
    def create_lot_assignment_table(self, parent, assignment_df):
        """ロット割り当て結果テーブルを作成"""
        try:
            # テーブルとスクロールバー用のフレーム
            lot_table_container = tk.Frame(parent, bg="white")
            lot_table_container.pack(fill="both", expand=True, padx=15, pady=15)
            
            # テーブル作成（高さを動的に調整）
            lot_tree = ttk.Treeview(
                lot_table_container,
                show="headings",
                height=20  # 他のテーブルと統一
            )
            
            # スタイルを適用
            self.configure_table_style(lot_tree, "Lot.Treeview")
            
            # スクロールバー
            lot_v_scrollbar = ttk.Scrollbar(lot_table_container, orient="vertical", command=lot_tree.yview)
            lot_h_scrollbar = ttk.Scrollbar(lot_table_container, orient="horizontal", command=lot_tree.xview)
            lot_tree.configure(yscrollcommand=lot_v_scrollbar.set, xscrollcommand=lot_h_scrollbar.set)
            
            # グリッド配置でスクロールバーを適切に配置
            lot_tree.grid(row=0, column=0, sticky="nsew")
            lot_v_scrollbar.grid(row=0, column=1, sticky="ns")
            lot_h_scrollbar.grid(row=1, column=0, sticky="ew")
            
            # グリッドの重み設定
            lot_table_container.grid_rowconfigure(0, weight=1)
            lot_table_container.grid_columnconfigure(0, weight=1)
            
            # 列の定義（画像で要求されているプロパティを含む）
            lot_columns = [
                "出荷予定日", "品番", "品名", "客先", "出荷数", "在庫数", "在梱包数", "不足数",
                "生産ロットID", "ロット数量", "指示日", "号機", "現在工程番号", "現在工程名", "現在工程二次処理"
            ]
            lot_tree["columns"] = lot_columns
            
            # 列幅を自動計算（Excel出力時の全データを使用）
            # current_assignment_dataが存在する場合はそれを使用、ない場合は表示用のassignment_dfを使用
            width_df = self.current_assignment_data if self.current_assignment_data is not None and not self.current_assignment_data.empty else assignment_df
            lot_column_widths = self.calculate_column_widths(width_df, lot_columns)
            
            # 右詰めにする数値列
            lot_numeric_columns = ["出荷数", "在庫数", "在梱包数", "不足数", "ロット数量"]
            
            for col in lot_columns:
                width = lot_column_widths.get(col, 120)
                anchor = "e" if col in lot_numeric_columns else "w"
                lot_tree.column(col, width=width, anchor=anchor)
                lot_tree.heading(col, text=col, anchor="center")
            
            # データの挿入
            row_index = 0
            # 列名から列インデックスへのマッピングを作成（高速化：itertuples()を使用）
            lot_col_idx_map = {col: assignment_df.columns.get_loc(col) for col in lot_columns}
            
            for row_tuple in assignment_df.itertuples(index=True):
                index = row_tuple[0]  # インデックス
                values = []
                for col in lot_columns:
                    col_idx = lot_col_idx_map[col]
                    # itertuples(index=True)では、row_tuple[0]がインデックス、row_tuple[1]以降が列の値
                    # 列インデックスは0始まりなので、col_idx + 1でアクセス
                    if col_idx + 1 < len(row_tuple):
                        col_value = row_tuple[col_idx + 1]
                    else:
                        col_value = None
                    if pd.notna(col_value):
                        if col == '出荷予定日' or col == '指示日':
                            try:
                                date_value = pd.to_datetime(col_value)
                                values.append(date_value.strftime('%Y/%m/%d'))
                            except:
                                values.append(str(col_value))
                        elif col in lot_numeric_columns:
                            try:
                                values.append(str(int(col_value)))
                            except:
                                values.append(str(col_value))
                        else:
                            values.append(str(col_value))
                    else:
                        values.append("")
                
                # 交互行色を適用
                tag = "even" if row_index % 2 == 0 else "odd"
                lot_tree.insert("", "end", values=values, tags=(tag,))
                row_index += 1
            
            # タグの設定（交互行色）
            lot_tree.tag_configure("even", background="#F9FAFB")
            lot_tree.tag_configure("odd", background="#FFFFFF")
            
            # マウスホイールイベントのバインド
            def on_lot_mousewheel(event):
                lot_tree.yview_scroll(int(-1 * (event.delta / 120)), "units")
                return "break"
            
            lot_tree.bind("<MouseWheel>", on_lot_mousewheel)
            
            # テーブルに入ったときと出たときのイベント（精度向上のため、コンテナフレームにも追加）
            # 注意: unbind_allは使わず、テーブル専用のスクロールを優先的に処理
            def on_lot_enter(event):
                # テーブル内ではテーブルのスクロールを優先（メインスクロールは無効化しない）
                pass
            
            def on_lot_leave(event):
                # テーブルから出たときはメインスクロールを再バインド（念のため）
                self.bind_main_scroll()
            
            lot_tree.bind("<Enter>", on_lot_enter)
            lot_tree.bind("<Leave>", on_lot_leave)
            lot_table_container.bind("<Enter>", on_lot_enter)
            lot_table_container.bind("<Leave>", on_lot_leave)
            
        except Exception as e:
            self.log_message(f"ロット割り当てテーブル作成中にエラーが発生しました: {str(e)}")
    
    def reset_ui_state(self):
        """UIの状態をリセット"""
        self.is_extracting = False
        self.extract_button.configure(state="normal", text="データ抽出開始")
        self.root.after(0, self._stop_progress_pulse)
        self._progress_monotonic_lock = False
        self.progress_bar.set(0)
        self.progress_label.configure(text="待機中...")
    
    def run(self):
        """アプリケーションの実行"""
        try:
            self.log_message("外観検査振分支援システムを起動しました")
            self.log_message("設定を確認してください")
            
            # 設定情報の表示
            if self.config and self.config.validate_config():
                pass  # 設定は正常に読み込まれている
            
            # mainloopを実行
            self.root.mainloop()
            
        except KeyboardInterrupt:
            # Ctrl+Cで中断された場合の処理
            logger.info("アプリケーションが中断されました（KeyboardInterrupt）")
            self.quit_application()
        except Exception as e:
            logger.error(f"アプリケーション実行中にエラーが発生しました: {e}", exc_info=True)
            try:
                self.quit_application()
            except:
                import os
                os._exit(1)
        finally:
            # リソースのクリーンアップ
            self.cleanup_resources()
    
    def load_masters_parallel(self, progress_base=0.1, progress_range=0.8):
        """マスタファイルを並列で読み込む（高速化、エラー時は順次処理にフォールバック）"""
        try:
            self.log_message("マスタファイルの並列読み込みを開始します...")
            # 進捗は呼び出し元で設定済みのため、ここでは更新しない
            
            # 独立したラッパー関数（インスタンス変数を事前に取得）
            product_path = self.config.product_master_path if self.config else None
            inspector_path = self.config.inspector_master_path if self.config else None
            skill_path = self.config.skill_master_path if self.config else None
            inspection_target_path = self.config.inspection_target_csv_path if self.config else None
            
            def load_product():
                """製品マスタ読み込み（独立関数）"""
                try:
                    return self.load_product_master_cached()
                except Exception as e:
                    logger.error(f"製品マスタの読み込みエラー: {str(e)}", exc_info=True)
                    return None
            
            def load_inspector():
                """検査員マスタ読み込み（独立関数）"""
                try:
                    return self.load_inspector_master_cached()
                except Exception as e:
                    logger.error(f"検査員マスタの読み込みエラー: {str(e)}", exc_info=True)
                    return None
            
            def load_skill():
                """スキルマスタ読み込み（独立関数）"""
                try:
                    return self.load_skill_master_cached()
                except Exception as e:
                    logger.error(f"スキルマスタの読み込みエラー: {str(e)}", exc_info=True)
                    return None
            
            def load_inspection_target():
                """検査対象CSV読み込み（独立関数）"""
                try:
                    return self.load_inspection_target_csv_cached()
                except Exception as e:
                    logger.error(f"検査対象CSVの読み込みエラー: {str(e)}", exc_info=True)
                    return None
            
            try:
                with ThreadPoolExecutor(max_workers=4) as executor:
                    # 並列実行タスクを定義
                    futures = {
                        'product': executor.submit(load_product),
                        'inspector': executor.submit(load_inspector),
                        'skill': executor.submit(load_skill),
                        'inspection_target': executor.submit(load_inspection_target)
                    }
                    
                    results = {}
                    total_files = len(futures)
                    completed_files = 0
                    # 進捗範囲は引数で受け取る（デフォルト: 0.1から0.9まで）
                    
                    # futureからkeyを逆引きするためのマッピングを作成
                    future_to_key = {future: key for key, future in futures.items()}
                    
                    for future in as_completed(futures.values()):
                        key = future_to_key[future]
                        try:
                            result = future.result(timeout=60)  # タイムアウトを設定
                            results[key] = result
                            completed_files += 1
                            
                            # 進捗を更新（各ファイル完了時に段階的に更新）
                            progress = progress_base + (progress_range * completed_files / total_files)
                            file_name_map = {
                                'product': '製品マスタ',
                                'inspector': '検査員マスタ',
                                'skill': 'スキルマスタ',
                                'inspection_target': '検査対象CSV'
                            }
                            file_name = file_name_map.get(key, key)
                            
                            if result is not None:
                                self.log_message(f"{file_name}の読み込みが完了しました")
                                # 進捗更新を最後の1回のみに最適化（パフォーマンス向上）
                                if completed_files == total_files:
                                    self.update_progress(progress, f"{file_name}の読み込み完了")
                            else:
                                self.log_message(f"{file_name}の読み込みに失敗しました")
                                # 進捗更新を最後の1回のみに最適化
                                if completed_files == total_files:
                                    self.update_progress(progress, f"{file_name}の読み込み失敗")
                        except Exception as e:
                            completed_files += 1
                            progress = progress_base + (progress_range * completed_files / total_files)
                            error_msg = f"{key}マスタの読み込み中にエラーが発生しました: {str(e)}"
                            self.log_message(error_msg)
                            logger.error(error_msg, exc_info=True)
                            results[key] = None
                            # 進捗更新を最後の1回のみに最適化
                            if completed_files == total_files:
                                self.update_progress(progress, f"{key}マスタの読み込みエラー")
                    
                    end_progress = progress_base + progress_range
                    self.update_progress(end_progress, "マスタファイルの並列読み込みが完了しました")
                    self.log_message("マスタファイルの並列読み込みが完了しました")
                    return results
            except Exception as parallel_error:
                # 並列処理でエラーが発生した場合は順次処理にフォールバック
                error_msg = f"並列処理でエラーが発生しました。順次処理に切り替えます: {str(parallel_error)}"
                self.log_message(error_msg)
                logger.warning(error_msg, exc_info=True)
                self.update_progress(progress_base, "順次処理に切り替え中...")
                return self.load_masters_sequential(progress_base=progress_base, progress_range=progress_range)
                
        except Exception as e:
            error_msg = f"マスタファイルの読み込み中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg, exc_info=True)
            self.update_progress(progress_base, "順次処理に切り替え中...")
            # 順次処理にフォールバック
            return self.load_masters_sequential(progress_base=progress_base, progress_range=progress_range)
    
    def load_masters_sequential(self, progress_base=0.1, progress_range=0.8):
        """マスタファイルを順次で読み込む（フォールバック用）"""
        try:
            self.log_message("マスタファイルの順次読み込みを開始します...")
            # 進捗は呼び出し元で設定済みのため、開始時は更新しない
            
            results = {}
            total_files = 4
            completed_files = 0
            # 進捗範囲は引数で受け取る（デフォルト: 0.1から0.9まで）
            
            # 製品マスタ
            try:
                self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                   "製品マスタを読み込み中...")
                results['product'] = self.load_product_master_cached()
                completed_files += 1
                if results['product'] is not None:
                    self.log_message("製品マスタの読み込みが完了しました")
                    self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                       f"製品マスタの読み込み完了 ({completed_files}/{total_files})")
                else:
                    self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                       f"製品マスタの読み込み失敗 ({completed_files}/{total_files})")
            except Exception as e:
                completed_files += 1
                logger.error(f"製品マスタの読み込みエラー: {str(e)}", exc_info=True)
                results['product'] = None
                self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                   f"製品マスタの読み込みエラー ({completed_files}/{total_files})")
            
            # 検査員マスタ
            try:
                self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                   "検査員マスタを読み込み中...")
                results['inspector'] = self.load_inspector_master_cached()
                completed_files += 1
                if results['inspector'] is not None:
                    self.log_message("検査員マスタの読み込みが完了しました")
                    self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                       f"検査員マスタの読み込み完了 ({completed_files}/{total_files})")
                else:
                    self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                       f"検査員マスタの読み込み失敗 ({completed_files}/{total_files})")
            except Exception as e:
                completed_files += 1
                logger.error(f"検査員マスタの読み込みエラー: {str(e)}", exc_info=True)
                results['inspector'] = None
                self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                   f"検査員マスタの読み込みエラー ({completed_files}/{total_files})")
            
            # スキルマスタ
            try:
                self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                   "スキルマスタを読み込み中...")
                results['skill'] = self.load_skill_master_cached()
                completed_files += 1
                if results['skill'] is not None:
                    self.log_message("スキルマスタの読み込みが完了しました")
                    self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                       f"スキルマスタの読み込み完了 ({completed_files}/{total_files})")
                else:
                    self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                       f"スキルマスタの読み込み失敗 ({completed_files}/{total_files})")
            except Exception as e:
                completed_files += 1
                logger.error(f"スキルマスタの読み込みエラー: {str(e)}", exc_info=True)
                results['skill'] = None
                self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                   f"スキルマスタの読み込みエラー ({completed_files}/{total_files})")
            
            # 検査対象CSV
            try:
                self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                   "検査対象CSVを読み込み中...")
                results['inspection_target'] = self.load_inspection_target_csv_cached()
                completed_files += 1
                if results['inspection_target'] is not None:
                    self.log_message("検査対象CSVの読み込みが完了しました")
                    self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                       f"検査対象CSVの読み込み完了 ({completed_files}/{total_files})")
                else:
                    self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                       f"検査対象CSVの読み込み失敗 ({completed_files}/{total_files})")
            except Exception as e:
                completed_files += 1
                logger.error(f"検査対象CSVの読み込みエラー: {str(e)}", exc_info=True)
                results['inspection_target'] = None
                self.update_progress(progress_base + (progress_range * completed_files / total_files), 
                                   f"検査対象CSVの読み込みエラー ({completed_files}/{total_files})")
            
            end_progress = progress_base + progress_range
            self.update_progress(end_progress, "マスタファイルの順次読み込みが完了しました")
            self.log_message("マスタファイルの順次読み込みが完了しました")
            return results
            
        except Exception as e:
            error_msg = f"マスタファイルの順次読み込み中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg, exc_info=True)
            return {
                'product': None,
                'inspector': None,
                'skill': None,
                'inspection_target': None
            }
    
    def _load_master_cached(self, cache_key: str, file_path_attr: str, load_func: callable, log_name: str):
        """共通キャッシュ付きマスタ読み込み（ファイル更新時刻チェック対応）"""
        # ファイルパスを取得
        file_path = getattr(self.config, file_path_attr, None) if self.config else None
        if not file_path or not os.path.exists(file_path):
            return load_func()
        
        # キャッシュチェック（ファイル更新時刻も確認）
        try:
            if cache_key in self.master_cache:
                if datetime.now() - self.cache_timestamps[cache_key] < self.cache_ttl:
                    try:
                        current_mtime = os.path.getmtime(file_path)
                        cached_mtime = self.cache_file_mtimes.get(cache_key, 0)
                        if current_mtime == cached_mtime:
                            logger.debug(f"{log_name}をキャッシュから読み込みました（ファイル未変更）")
                            return self.master_cache[cache_key]
                    except (OSError, AttributeError):
                        pass
        except Exception:
            pass
        
        # キャッシュミスの場合は通常読み込み
        result = load_func()
        if result is not None:
            try:
                self.master_cache[cache_key] = result
                self.cache_timestamps[cache_key] = datetime.now()
                try:
                    self.cache_file_mtimes[cache_key] = os.path.getmtime(file_path)
                except (OSError, AttributeError):
                    pass
            except Exception:
                pass
        
        return result
    
    def load_product_master_cached(self):
        """キャッシュ付き製品マスタ読み込み（ファイル更新時刻チェック対応）"""
        return self._load_master_cached(
            'product_master', 'product_master_path', 
            self.load_product_master, '製品マスタ'
        )
    
    def initialize_product_code_list(self):
        """製品マスタから重複除去済み品番リストを初期化（バックグラウンド処理）"""
        def load_in_background():
            try:
                product_master_df = self.load_product_master_cached()
                if product_master_df is not None and '品番' in product_master_df.columns:
                    # 重複を除去して一意の品番リストを作成
                    unique_products = product_master_df['品番'].dropna().astype(str).unique().tolist()
                    # 空文字列を除外
                    unique_products = [p for p in unique_products if p.strip()]
                    # ソート
                    unique_products.sort()
                    self.product_code_autocomplete_list = unique_products
                else:
                    logger.warning("製品マスタが読み込めないか、'品番'列が存在しません")
                    self.product_code_autocomplete_list = []
            except Exception as e:
                logger.error(f"品番リストの初期化に失敗しました: {e}", exc_info=True)
                self.product_code_autocomplete_list = []
        
        # バックグラウンドスレッドで実行（UIをブロックしない）
        threading.Thread(target=load_in_background, daemon=True).start()
    
    def on_product_code_key_release(self, event):
        """品番入力フィールドのキーリリースイベント"""
        # 既存のcheck_input_fieldsも呼び出す
        self.check_input_fields(event)
        
        # 予測検索の処理
        try:
            # product_code_entryが初期化されているか確認
            if self.product_code_entry is None:
                return
            
            current_text = self.product_code_entry.get().strip()
            
            # 既存の遅延実行ジョブをキャンセル
            if self.autocomplete_search_job is not None:
                self.root.after_cancel(self.autocomplete_search_job)
                self.autocomplete_search_job = None
            
            # 最小文字数未満の場合はドロップダウンを非表示
            if len(current_text) < self.min_search_length:
                self.hide_autocomplete_dropdown()
                return
            
            # 遅延実行で検索（300ms後）
            self.autocomplete_search_job = self.root.after(300, lambda text=current_text: self.search_product_codes(text))
        except Exception as e:
            logger.error(f"品番入力イベント処理エラー: {e}", exc_info=True)
    
    def on_product_code_focus_in(self, event):
        """品番入力フィールドにフォーカスが入った時"""
        current_text = self.product_code_entry.get().strip()
        if len(current_text) >= self.min_search_length:
            # 既存の遅延実行ジョブをキャンセル
            if self.autocomplete_search_job is not None:
                self.root.after_cancel(self.autocomplete_search_job)
                self.autocomplete_search_job = None
            # 即座に検索
            self.search_product_codes(current_text)
    
    def on_product_code_focus_out(self, event):
        """品番入力フィールドからフォーカスが外れた時"""
        # 既存のcheck_input_fieldsも呼び出す
        self.check_input_fields(event)
        
        # マウスがドロップダウンフレーム内にある場合は非表示にしない
        if self.autocomplete_mouse_inside:
            return
        
        # 入力フィールドにフォーカスがある場合は非表示にしない
        try:
            if self.product_code_entry.focus_get() == self.product_code_entry:
                return
        except:
            pass
        
        # 既存の非表示処理ジョブをキャンセル
        if self.autocomplete_hide_job is not None:
            self.root.after_cancel(self.autocomplete_hide_job)
            self.autocomplete_hide_job = None
        
        # 少し遅延させてから非表示（ドロップダウンをクリックする時間を確保）
        self.autocomplete_hide_job = self.root.after(300, self.hide_autocomplete_dropdown)
    
    def search_product_codes(self, search_text: str):
        """品番を検索してドロップダウンを表示"""
        try:
            if not self.product_code_autocomplete_list:
                # リストがまだ初期化されていない場合は再試行
                self.initialize_product_code_list()
                # 少し待ってから再検索
                self.root.after(500, lambda text=search_text: self.search_product_codes(text))
                return
            
            # 大文字小文字を区別しない部分一致検索
            search_text_lower = search_text.lower()
            matches = [
                product for product in self.product_code_autocomplete_list
                if search_text_lower in product.lower()
            ]
            
            # 最大表示件数で制限
            matches = matches[:self.max_display_items]
            
            if matches:
                self.show_autocomplete_dropdown(matches, search_text)
            else:
                self.hide_autocomplete_dropdown()
        except Exception as e:
            logger.error(f"品番検索エラー: {e}", exc_info=True)
    
    def show_autocomplete_dropdown(self, matches: list, current_text: str):
        """予測検索ドロップダウンを表示"""
        # 既存のドロップダウンを強制的に削除（新しいドロップダウンを表示するため）
        self.force_hide_autocomplete_dropdown()
        
        if not matches:
            return
        
        try:
            # コンテナフレームを取得
            if not hasattr(self, 'product_code_container') or self.product_code_container is None:
                logger.error("product_code_containerが初期化されていません")
                return
            container = self.product_code_container
            
            # ドロップダウンフレームを作成（コンテナフレームに配置）
            self.autocomplete_dropdown = ctk.CTkFrame(
                container,
                fg_color="white",
                corner_radius=8,
                border_width=1,
                border_color="#DBEAFE"
            )
            
            # スクロール可能なフレームを作成
            max_height = min(len(matches) * 35 + 10, 200)  # 最大200pxの高さ
            scrollable_frame = ctk.CTkScrollableFrame(
                self.autocomplete_dropdown,
                fg_color="white",
                height=max_height
            )
            scrollable_frame.pack(fill="both", expand=True, padx=2, pady=2)
            
            # マウスホイールイベントを処理する関数
            def on_autocomplete_mousewheel(event):
                """ドロップダウンリストのマウスホイールイベント処理"""
                # CTkScrollableFrameの内部Canvasを直接操作
                canvas = scrollable_frame._parent_canvas
                if canvas:
                    # WindowsとLinux/Macでイベントの形式が異なる
                    if event.delta:
                        # Windows（スクロール速度を20倍に）
                        scroll_amount = int(-event.delta / 120) * 20
                    else:
                        # Linux/Mac（スクロール速度を20倍に）
                        scroll_amount = -20 if event.num == 4 else 20
                    canvas.yview_scroll(scroll_amount, "units")
                return "break"
            
            # マウスホイールイベントをバインド
            scrollable_frame.bind("<MouseWheel>", on_autocomplete_mousewheel)
            self.autocomplete_dropdown.bind("<MouseWheel>", on_autocomplete_mousewheel)
            
            # ドロップダウンフレームとその子ウィジェットにマウスホイールイベントを再帰的にバインド
            def bind_mousewheel_to_children(widget):
                """再帰的に子ウィジェットにマウスホイールイベントをバインド"""
                try:
                    widget.bind("<MouseWheel>", on_autocomplete_mousewheel)
                    for child in widget.winfo_children():
                        bind_mousewheel_to_children(child)
                except:
                    pass
            
            # ドロップダウンフレームの子ウィジェットにバインド（初期状態のみ）
            # ボタン作成後にも再度呼び出す必要があるため、ここでは呼ばない
            
            # ドロップダウンフレームに入ったときにフォーカスを維持
            def on_enter_dropdown(event):
                """ドロップダウンフレームに入ったとき"""
                # マウスがドロップダウンフレーム内にあることを記録
                self.autocomplete_mouse_inside = True
                # 非表示処理をキャンセル
                if self.autocomplete_hide_job is not None:
                    self.root.after_cancel(self.autocomplete_hide_job)
                    self.autocomplete_hide_job = None
                # マウスホイールイベントが確実に動作するように、フォーカスを設定
                try:
                    scrollable_frame.focus_set()
                except:
                    pass
            
            def on_leave_dropdown(event):
                """ドロップダウンフレームから出たとき"""
                # イベントのwidgetを確認
                try:
                    widget = event.widget
                    # マウスが実際にドロップダウンフレーム外に出たか確認
                    # 子ウィジェット間の移動の場合は無視
                    if widget == self.autocomplete_dropdown or widget == scrollable_frame:
                        # ドロップダウンフレーム自体から出た場合のみ処理
                        # 少し遅延させて、実際に外に出たか確認
                        def check_leave():
                            try:
                                # マウスの現在位置を確認
                                x, y = self.root.winfo_pointerxy()
                                widget_x = self.autocomplete_dropdown.winfo_rootx()
                                widget_y = self.autocomplete_dropdown.winfo_rooty()
                                widget_width = self.autocomplete_dropdown.winfo_width()
                                widget_height = self.autocomplete_dropdown.winfo_height()
                                
                                # マウスがドロップダウンフレーム内にあるか確認
                                if (widget_x <= x <= widget_x + widget_width and 
                                    widget_y <= y <= widget_y + widget_height):
                                    # まだフレーム内にあるので非表示にしない
                                    return
                                
                                # 入力フィールドに戻っているかチェック
                                entry_x = self.product_code_entry.winfo_rootx()
                                entry_y = self.product_code_entry.winfo_rooty()
                                entry_width = self.product_code_entry.winfo_width()
                                entry_height = self.product_code_entry.winfo_height()
                                
                                if (entry_x <= x <= entry_x + entry_width and 
                                    entry_y <= y <= entry_y + entry_height):
                                    # 入力フィールドに戻っているので非表示にしない
                                    return
                                
                                # 実際に外に出た場合のみ非表示
                                self.autocomplete_mouse_inside = False
                                if self.autocomplete_hide_job is not None:
                                    self.root.after_cancel(self.autocomplete_hide_job)
                                self.autocomplete_hide_job = self.root.after(300, self.hide_autocomplete_dropdown)
                            except:
                                # エラーが発生した場合は安全のため非表示にしない
                                pass
                        
                        # 少し遅延させて確認（子ウィジェット間の移動を除外）
                        self.root.after(100, check_leave)
                    else:
                        # 子ウィジェットからのLeaveイベントは無視（親のLeaveイベントで処理）
                        pass
                except:
                    # エラーが発生した場合は非表示にしない
                    pass
            
            # 各候補をボタンとして表示
            for product_code in matches:
                # ボタン用のフレームを作成（ホバー効果を確実に表示するため）
                button_frame = ctk.CTkFrame(
                    scrollable_frame,
                    fg_color="#F9FAFB",
                    corner_radius=4,
                    height=32
                )
                button_frame.pack(fill="x", padx=2, pady=1)
                
                # ラベルを作成（クリック可能な領域）
                item_label = ctk.CTkLabel(
                    button_frame,
                    text=product_code,
                    font=ctk.CTkFont(family="Yu Gothic", size=13),
                    fg_color="transparent",
                    text_color="#374151",
                    anchor="w",
                    height=32
                )
                item_label.pack(fill="x", padx=8, pady=0)
                
                # クリックイベントをバインド
                def on_item_click(event, code=product_code):
                    """品番を選択"""
                    # イベントの伝播を止める
                    event.widget.focus_set()
                    # 少し遅延させてから選択処理を実行（イベント処理が完了してから）
                    self.root.after(10, lambda: self.select_product_code(code))
                    return "break"
                
                # Enter/Leaveイベントで背景色を変更
                def on_frame_enter(event, frame=button_frame, label=item_label):
                    """フレームにマウスが入ったとき"""
                    try:
                        frame.configure(fg_color="#3B82F6")
                        label.configure(text_color="white")
                    except:
                        pass
                    # ドロップダウンフレームのEnterイベントも呼び出す
                    on_enter_dropdown(event)
                
                def on_frame_leave(event, frame=button_frame, label=item_label):
                    """フレームからマウスが出たとき"""
                    try:
                        frame.configure(fg_color="#F9FAFB")
                        label.configure(text_color="#374151")
                    except:
                        pass
                    # ドロップダウンフレームのLeaveイベントは呼ばない（親のLeaveで処理）
                
                # フレームとラベルの両方にイベントをバインド（優先度を高く）
                button_frame.bind("<Enter>", on_frame_enter, add="+")
                button_frame.bind("<Leave>", on_frame_leave, add="+")
                button_frame.bind("<Button-1>", on_item_click)
                item_label.bind("<Enter>", on_frame_enter, add="+")
                item_label.bind("<Leave>", on_frame_leave, add="+")
                item_label.bind("<Button-1>", on_item_click)
                
                # マウスホイールイベントもバインド（フレームとラベルに）
                button_frame.bind("<MouseWheel>", on_autocomplete_mousewheel)
                item_label.bind("<MouseWheel>", on_autocomplete_mousewheel)
            
            # ドロップダウンをpackで配置（入力フィールドの直下）
            self.autocomplete_dropdown.pack(fill="x", pady=(2, 0))
            
            # ボタン作成後にマウスホイールイベントを再帰的にバインド（作成されたボタンにもバインド）
            bind_mousewheel_to_children(self.autocomplete_dropdown)
            
            # Enter/Leaveイベントをバインド（ボタン作成後に実行）
            self.autocomplete_dropdown.bind("<Enter>", on_enter_dropdown)
            self.autocomplete_dropdown.bind("<Leave>", on_leave_dropdown)
            scrollable_frame.bind("<Enter>", on_enter_dropdown)
            scrollable_frame.bind("<Leave>", on_leave_dropdown)
            
            # 各ボタンにもEnter/Leaveイベントをバインド（再帰的に、ボタン作成後に実行）
            # ただし、フレームとラベルには既にバインド済みなので、スクロール可能フレームのみ
            def bind_enter_leave_to_children(widget):
                """再帰的に子ウィジェットにEnter/Leaveイベントをバインド"""
                try:
                    # フレームとラベルは既に個別にバインド済みなのでスキップ
                    # スクロール可能フレームとその他のウィジェットのみバインド
                    widget_type = type(widget).__name__
                    # フレームとラベルでない場合、またはスクロール可能フレームの場合のみバインド
                    if widget == scrollable_frame or widget_type not in ['CTkFrame', 'CTkLabel']:
                        widget.bind("<Enter>", on_enter_dropdown, add="+")
                        widget.bind("<Leave>", on_leave_dropdown, add="+")
                    for child in widget.winfo_children():
                        bind_enter_leave_to_children(child)
                except:
                    pass
            
            # ドロップダウンフレームの子ウィジェットにバインド（ボタン作成後に実行）
            # フレームとラベルは既に個別にバインド済みなので、それ以外のみ
            bind_enter_leave_to_children(self.autocomplete_dropdown)
            
        except Exception as e:
            logger.error(f"ドロップダウンの表示に失敗しました: {e}", exc_info=True)
    
    def force_hide_autocomplete_dropdown(self):
        """予測検索ドロップダウンを強制的に非表示（新しいドロップダウンを表示する前など）"""
        if self.autocomplete_dropdown is not None:
            try:
                self.autocomplete_dropdown.destroy()
            except:
                pass
            self.autocomplete_dropdown = None
        # 非表示処理ジョブをクリア
        if self.autocomplete_hide_job is not None:
            self.root.after_cancel(self.autocomplete_hide_job)
            self.autocomplete_hide_job = None
        # マウス位置フラグをリセット
        self.autocomplete_mouse_inside = False
    
    def hide_autocomplete_dropdown(self):
        """予測検索ドロップダウンを非表示"""
        # マウスがドロップダウンフレーム内にある場合は非表示にしない
        if self.autocomplete_mouse_inside:
            return
        
        # 入力フィールドにフォーカスがある場合は非表示にしない
        try:
            if self.product_code_entry and self.product_code_entry.focus_get() == self.product_code_entry:
                return
        except:
            pass
        
        # 強制削除を呼び出す
        self.force_hide_autocomplete_dropdown()
    
    def select_product_code(self, product_code: str):
        """品番を選択して入力フィールドに設定"""
        # マウス位置フラグを先にリセット（Enterイベントが発火しないように）
        self.autocomplete_mouse_inside = False
        
        # 非表示処理ジョブをキャンセル
        if self.autocomplete_hide_job is not None:
            try:
                self.root.after_cancel(self.autocomplete_hide_job)
            except:
                pass
            self.autocomplete_hide_job = None
        
        # 検索処理ジョブもキャンセル
        if self.autocomplete_search_job is not None:
            try:
                self.root.after_cancel(self.autocomplete_search_job)
            except:
                pass
            self.autocomplete_search_job = None
        
        # ドロップダウンを確実に削除する関数
        def hide_dropdown_immediately():
            """ドロップダウンを即座に非表示にする"""
            if self.autocomplete_dropdown is not None:
                try:
                    # まず、pack_forgetで非表示にする
                    try:
                        self.autocomplete_dropdown.pack_forget()
                    except:
                        pass
                    
                    # その後、destroyで削除
                    try:
                        self.autocomplete_dropdown.destroy()
                    except:
                        pass
                except:
                    pass
                finally:
                    self.autocomplete_dropdown = None
                    self.autocomplete_mouse_inside = False
            
            # UIを更新して確実に非表示にする
            try:
                self.root.update_idletasks()
            except:
                pass
        
        # 即座にドロップダウンを非表示（選択確定時）
        hide_dropdown_immediately()
        
        # 念のため、少し遅延させて再度確認（イベント処理が完了してから）
        self.root.after(50, hide_dropdown_immediately)
        
        # 入力フィールドに品番を設定
        self.product_code_entry.delete(0, "end")
        self.product_code_entry.insert(0, product_code)
        
        # フォーカスを入力フィールドに戻す
        self.product_code_entry.focus_set()
        
        # 入力フィールドのチェックも実行
        self.check_input_fields(None)
    
    def load_inspector_master_cached(self):
        """キャッシュ付き検査員マスタ読み込み（ファイル更新時刻チェック対応）"""
        return self._load_master_cached(
            'inspector_master', 'inspector_master_path',
            self.load_inspector_master, '検査員マスタ'
        )
    
    def load_skill_master_cached(self):
        """キャッシュ付きスキルマスタ読み込み（ファイル更新時刻チェック対応）"""
        return self._load_master_cached(
            'skill_master', 'skill_master_path',
            self.load_skill_master, 'スキルマスタ'
        )
    
    def load_inspection_target_csv_cached(self):
        """キャッシュ付き検査対象CSV読み込み（ファイル更新時刻チェック対応）"""
        return self._load_master_cached(
            'inspection_target_csv', 'inspection_target_csv_path',
            self.load_inspection_target_csv, '検査対象CSV'
        )
    
    def load_product_master(self):
        """製品マスタファイルを読み込む"""
        try:
            # 設定ファイルから製品マスタファイルのパスを取得
            if self.config and self.config.product_master_path:
                file_path = self.config.product_master_path
                if not os.path.exists(file_path):
                    self.log_message(f"製品マスタファイルが見つかりません: {file_path}")
                    return None
            else:
                # ファイル選択ダイアログを表示
                file_path = filedialog.askopenfilename(
                    title="製品マスタファイルを選択",
                    filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                    initialdir=os.path.expanduser("~/Desktop")
                )
                
                if not file_path:
                    return None
            
            # Excelファイルを読み込み（最適化: engine指定のみ、型推測を高速化）
            # usecolsやdtype指定はエラー処理のオーバーヘッドがあるため、シンプルに読み込む
            df = pd.read_excel(file_path, engine='openpyxl')
            
            # 必要な列が存在するかチェック
            required_columns = ['品番', '工程番号', '検査時間']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                # 列名のマッピングを試行
                column_mapping = {}
                for col in df.columns:
                    if '品番' in str(col):
                        column_mapping[col] = '品番'
                    elif '工程' in str(col) and '番号' in str(col):
                        column_mapping[col] = '工程番号'
                    elif '検査' in str(col) and '時間' in str(col):
                        column_mapping[col] = '検査時間'
                
                if len(column_mapping) >= 2:  # 品番と検査時間は最低限必要
                    df = df.rename(columns=column_mapping)
                else:
                    self.log_message(f"必要な列が見つかりません: {missing_columns}")
                    return None
            
            return df
            
        except Exception as e:
            error_msg = f"製品マスタの読み込みに失敗しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            return None
    
    def display_vacation_info_table(self, vacation_data, extraction_date, inspector_master_df=None):
        """休暇情報テーブルを表示"""
        try:
            # 既存の休暇情報テーブルがあれば削除
            if hasattr(self, 'vacation_info_frame') and self.vacation_info_frame:
                try:
                    self.vacation_info_frame.destroy()
                except:
                    pass
            
            # 休暇情報セクションを作成（左寄せのためfill="none"に変更、背景色を赤系に変更）
            vacation_frame = ctk.CTkFrame(self.main_scroll_frame, fg_color="#FEE2E2", corner_radius=12)
            vacation_frame.table_section = True
            vacation_frame.vacation_section = True  # 休暇情報テーブルのマーカー
            vacation_frame.pack(fill="none", anchor="w", padx=20, pady=(10, 10))  # 左寄せに変更
            self.vacation_info_frame = vacation_frame
            
            # タイトルフレーム
            title_frame = ctk.CTkFrame(vacation_frame, fg_color="transparent")
            title_frame.pack(fill="x", padx=15, pady=(15, 5))
            
            # タイトル
            title_label = ctk.CTkLabel(
                title_frame,
                text="休暇情報",
                font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold")
            )
            title_label.pack(side="left")
            
            # 対象日表示フレーム
            date_frame = ctk.CTkFrame(vacation_frame, fg_color="transparent")
            date_frame.pack(fill="x", padx=15, pady=(0, 10))
            
            # 対象日表示（日付の処理を改善）
            # extraction_dateがNoneの場合は、インスタンス変数から取得を試みる
            if extraction_date is None and hasattr(self, 'current_extraction_date'):
                extraction_date = self.current_extraction_date
            
            # extraction_dateがNoneの場合は今日の日付を使用
            if extraction_date is None:
                extraction_date = date.today()
            
            date_str = ""
            if extraction_date is not None:
                try:
                    # date型の場合
                    if hasattr(extraction_date, 'strftime'):
                        date_str = extraction_date.strftime('%Y/%m/%d')
                    # 文字列の場合
                    elif isinstance(extraction_date, str):
                        date_obj = pd.to_datetime(extraction_date).date()
                        date_str = date_obj.strftime('%Y/%m/%d')
                    # datetime型の場合
                    elif hasattr(extraction_date, 'date'):
                        date_obj = extraction_date.date()
                        date_str = date_obj.strftime('%Y/%m/%d')
                    else:
                        # その他の型の場合は文字列に変換してから処理
                        try:
                            date_obj = pd.to_datetime(str(extraction_date)).date()
                            date_str = date_obj.strftime('%Y/%m/%d')
                        except:
                            date_str = str(extraction_date)
                            logger.debug(f"extraction_dateの型が不明です: {type(extraction_date)}, 値: {extraction_date}")
                except Exception as e:
                    logger.error(f"日付のフォーマット処理でエラーが発生しました: {str(e)}, extraction_date: {extraction_date}, 型: {type(extraction_date)}")
                    # エラー時も今日の日付を表示
                    date_str = date.today().strftime('%Y/%m/%d')
            else:
                # 念のため、今日の日付を表示
                date_str = date.today().strftime('%Y/%m/%d')
            
            date_label = ctk.CTkLabel(
                date_frame,
                text=f"対象日: {date_str}",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151"
            )
            date_label.pack(side="left")
            
            # 検査員マスタから検査員名のリストを取得
            inspector_names_set = set()
            if inspector_master_df is not None and '#氏名' in inspector_master_df.columns:
                inspector_names_set = set(inspector_master_df['#氏名'].dropna().astype(str).str.strip())
                inspector_names_set = {name for name in inspector_names_set if name}  # 空文字列を除外
            
            # 検査員マスタに存在する検査員のみをフィルタリング
            filtered_vacation_data = {}
            if vacation_data and inspector_names_set:
                for employee_name, vacation_info in vacation_data.items():
                    # 検査員マスタに存在する検査員のみを追加
                    if employee_name in inspector_names_set:
                        filtered_vacation_data[employee_name] = vacation_info
            elif vacation_data:
                # 検査員マスタが読み込めない場合は全員表示
                filtered_vacation_data = vacation_data
            
            # テーブルフレーム（左寄せ、内容に応じた幅に調整）
            table_frame = tk.Frame(vacation_frame)
            table_frame.pack(fill="none", anchor="w", padx=15, pady=(0, 15))  # 左寄せに変更
            
            # 列の定義（検査員名と休暇内容）
            vacation_columns = ["検査員名", "休暇内容"]
            
            # Treeviewの作成
            row_count = len(filtered_vacation_data) if filtered_vacation_data else 1
            vacation_tree = ttk.Treeview(table_frame, columns=vacation_columns, show="headings", height=min(10, max(3, row_count)))
            
            # スタイルを適用
            style = ttk.Style()
            style.configure("Vacation.Treeview", 
                           background="white",
                           foreground="#374151",
                           fieldbackground="white",
                           font=("MS Gothic", 10, "bold"))
            style.map("Vacation.Treeview",
                     background=[('selected', '#3B82F6')],
                     foreground=[('selected', 'white')])
            
            # 列の設定（内容に応じた幅に最適化）
            vacation_tree.heading("検査員名", text="検査員名", anchor="center")
            vacation_tree.heading("休暇内容", text="休暇内容", anchor="center")
            
            # 列幅を内容に応じて最適化（窮屈にならないように適度な余白を確保）
            # 検査員名: 最大文字数に応じて調整（日本語1文字=約10ピクセル、余白20ピクセル）
            if filtered_vacation_data:
                max_name_length = max([len(name) for name in filtered_vacation_data.keys()] + [len("検査員名")])
            else:
                max_name_length = len("検査員名")
            name_width = min(max(max_name_length * 10 + 20, 120), 250)  # 最小120、最大250
            
            # 休暇内容: 最大文字数に応じて調整
            if filtered_vacation_data:
                max_content_length = max([len(str(v.get('interpretation', v.get('code', '')))) for v in filtered_vacation_data.values()] + [len("休暇内容")])
            else:
                max_content_length = len("休暇内容")
            content_width = min(max(max_content_length * 10 + 20, 150), 300)  # 最小150、最大300
            
            vacation_tree.column("検査員名", width=int(name_width), anchor="w", minwidth=120)
            vacation_tree.column("休暇内容", width=int(content_width), anchor="w", minwidth=150)
            
            # データの挿入
            if filtered_vacation_data:
                for idx, (inspector_name, vacation_info) in enumerate(sorted(filtered_vacation_data.items())):
                    # 休暇内容を取得
                    vacation_content = vacation_info.get('interpretation', '')
                    if not vacation_content:
                        vacation_content = vacation_info.get('code', '')
                    
                    tag = "even" if idx % 2 == 0 else "odd"
                    vacation_tree.insert("", "end", values=(inspector_name, vacation_content), tags=(tag,))
            else:
                vacation_tree.insert("", "end", values=("休暇予定なし", ""))
            
            # タグの設定（交互行色）
            vacation_tree.tag_configure("even", background="#F9FAFB")
            vacation_tree.tag_configure("odd", background="#FFFFFF")
            
            # スクロールバー
            # スクロールバーは不要のため削除
            vacation_tree.grid(row=0, column=0, sticky="nsew")
            
            # テーブルフレームのサイズを内容に合わせる（横方向は自動調整）
            table_frame.grid_rowconfigure(0, weight=1)
            table_frame.grid_columnconfigure(0, weight=0)  # 横方向は自動サイズに変更
            
            # マウスホイールイベントのバインド（メイン画面のスクロールを有効化）
            def on_vacation_mousewheel(event):
                # テーブル内ではメインスクロールを使用
                if hasattr(self.main_scroll_frame, '_parent_canvas'):
                    canvas = self.main_scroll_frame._parent_canvas
                    if canvas:
                        scroll_amount = int(-1 * (event.delta / 120)) * 14
                        canvas.yview_scroll(scroll_amount, "units")
                return "break"
            
            vacation_tree.bind("<MouseWheel>", on_vacation_mousewheel)
            table_frame.bind("<MouseWheel>", on_vacation_mousewheel)
            
            self.log_message(f"休暇情報テーブルを表示しました: {len(filtered_vacation_data)}名")
            
        except Exception as e:
            error_msg = f"休暇情報テーブルの表示に失敗しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
    
    
    def display_inspector_assignment_table(self, inspector_df, preserve_scroll_position=False, target_row_index=None):
        """検査員割振りテーブルを表示
        
        Args:
            inspector_df: 表示するデータフレーム
            preserve_scroll_position: スクロール位置を保持するかどうか
            target_row_index: 選択・表示する行のインデックス（DataFrameのインデックス）
        """
        try:
            # 既存のテーブルがある場合、スクロール位置を保存
            saved_scroll_position = None
            saved_first_visible_row_data = None  # 最初に表示されている行のデータを保存
            saved_main_scroll_position = None
            
            if preserve_scroll_position and hasattr(self, 'current_inspector_tree') and self.current_inspector_tree:
                try:
                    # メインスクロールフレームの位置を先に保存（テーブル削除前に）
                    try:
                        if hasattr(self.main_scroll_frame, '_parent_canvas'):
                            canvas = self.main_scroll_frame._parent_canvas
                            if canvas:
                                saved_main_scroll_position = canvas.yview()
                    except:
                        pass
                    
                    # テーブル内のスクロール位置を取得
                    saved_scroll_position = self.current_inspector_tree.yview()
                    
                    # 表示されている最初の行のデータを保存（より確実な方法）
                    try:
                        visible_items = self.current_inspector_tree.get_children()
                        if visible_items:
                            scroll_top = saved_scroll_position[0]
                            total_items = len(visible_items)
                            if total_items > 0:
                                # スクロール位置から最初に表示される行のインデックスを計算
                                first_visible_index = int(scroll_top * total_items)
                                if first_visible_index < len(visible_items):
                                    first_item = visible_items[first_visible_index]
                                    # その行のデータを取得（品番とロットIDを保存）
                                    item_values = self.current_inspector_tree.item(first_item, 'values')
                                    if item_values and len(item_values) > 4:
                                        # 列の順序: 出荷予定日(0), 品番(1), 品名(2), 客先(3), 生産ロットID(4), ...
                                        saved_first_visible_row_data = {
                                            'product_number': item_values[1] if len(item_values) > 1 else None,  # 品番
                                            'lot_id': item_values[4] if len(item_values) > 4 else None,  # 生産ロットID
                                            'scroll_pos': saved_scroll_position[0]
                                        }
                    except Exception as e:
                        logger.debug(f"最初の行データの保存に失敗: {str(e)}")
                except Exception as e:
                    logger.debug(f"スクロール位置の保存に失敗: {str(e)}")
            
            # 既存のテーブルセクションを削除（検査員割振りテーブルのみ）
            self.hide_current_table()
            
            # 検査員割振りセクションを作成
            inspector_frame = ctk.CTkFrame(self.main_scroll_frame, fg_color="#EFF6FF", corner_radius=12)
            inspector_frame.table_section = True
            inspector_frame.inspector_section = True  # 検査員割振りテーブルのマーカー
            inspector_frame.pack(fill="x", padx=20, pady=(10, 20))  # 休暇情報テーブルの下に表示
            
            # タイトルとスキル表示切り替えボタンのフレーム
            title_frame = ctk.CTkFrame(inspector_frame, fg_color="transparent")
            title_frame.pack(fill="x", padx=15, pady=(15, 10))
            
            # タイトル
            title_label = ctk.CTkLabel(
                title_frame,
                text="検査員割振り結果",
                font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold")
            )
            title_label.pack(side="left")
            
            # ボタンフレーム
            button_frame = ctk.CTkFrame(title_frame, fg_color="transparent")
            button_frame.pack(side="right")
            
            action_flow_frame = ctk.CTkFrame(button_frame, fg_color="transparent")
            action_flow_frame.pack(side="right", padx=(0, 25))

            def append_arrow():
                arrow_label = ctk.CTkLabel(
                    action_flow_frame,
                    text="→",
                    font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
                    text_color="#1F2937"
                )
                arrow_label.pack(side="left", padx=6)

            self.seating_view_button = ctk.CTkButton(
                action_flow_frame,
                text="座席表",
                command=self.open_seating_chart,
                width=110,
                height=30,
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                fg_color="#1F7AEF",
                hover_color="#2563EB",
                corner_radius=10,
                border_width=0,
                text_color="white"
            )
            self.seating_view_button.pack(side="left")

            append_arrow()

            self.seating_reflect_button = ctk.CTkButton(
                action_flow_frame,
                text="ロット振分変更反映",
                command=self.apply_seating_chart_results,
                width=160,
                height=30,
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                fg_color="#F97316",
                hover_color="#EA580C",
                corner_radius=10,
                border_width=0,
                text_color="white"
            )
            self.seating_reflect_button.pack(side="left")

            append_arrow()

            self.google_sheets_button = ctk.CTkButton(
                action_flow_frame,
                text="Googleスプレッドシートへ出力",
                command=self.export_to_google_sheets,
                width=220,
                height=30,
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                fg_color="#10B981",
                hover_color="#059669",
                corner_radius=10,
                border_width=0,
                text_color="white"
            )
            self.google_sheets_button.pack(side="left")

            append_arrow()

            self.app_exit_button = ctk.CTkButton(
                action_flow_frame,
                text="アプリ終了",
                command=self.quit_application,
                width=130,
                height=30,
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                fg_color="#EF4444",
                hover_color="#DC2626",
                corner_radius=10,
                border_width=0,
                text_color="white"
            )
            self.app_exit_button.pack(side="left")
            
            self.seating_flow_prompt_label = ctk.CTkLabel(
                inspector_frame,
                text="",
                font=ctk.CTkFont(family="Yu Gothic", size=11, weight="bold"),
                text_color="#1F7AEF",
                anchor="w"
            )
            self.seating_flow_prompt_label.pack(fill="x", padx=15, pady=(0, 10))

            # テーブルフレーム
            table_frame = tk.Frame(inspector_frame)
            table_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
            
            # 列の定義
            inspector_columns = [
                "出荷予定日", "品番", "品名", "客先", "生産ロットID", "ロット数量", 
                "指示日", "号機", "現在工程名", "秒/個", "検査時間",
                "検査員人数", "分割検査時間",
            ] + [f"検査員{i}" for i in range(1, MAX_INSPECTORS_PER_LOT + 1)]
            
            # Treeviewの作成
            inspector_tree = ttk.Treeview(table_frame, columns=inspector_columns, show="headings", height=20)
            
            # スタイルを適用
            self.configure_table_style(inspector_tree, "Inspector.Treeview")
            
            # 列幅を自動計算（Excel出力時の全データを使用）
            # current_inspector_dataが存在する場合はそれを使用、ない場合は表示用のinspector_dfを使用
            width_df = self.current_inspector_data if self.current_inspector_data is not None and not self.current_inspector_data.empty else inspector_df
            inspector_column_widths = self.calculate_column_widths(width_df, inspector_columns)
            
            # 右詰めにする数値列
            inspector_numeric_columns = ["ロット数量", "秒/個", "検査時間", "検査員人数", "分割検査時間"]
            
            for col in inspector_columns:
                width = inspector_column_widths.get(col, 100)
                anchor = "e" if col in inspector_numeric_columns else "w"
                inspector_tree.heading(col, text=col, anchor="center")
                inspector_tree.column(col, width=width, anchor=anchor)
            
            # スクロールバーの追加
            inspector_v_scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=inspector_tree.yview)
            inspector_h_scrollbar = ttk.Scrollbar(table_frame, orient="horizontal", command=inspector_tree.xview)
            inspector_tree.configure(yscrollcommand=inspector_v_scrollbar.set, xscrollcommand=inspector_h_scrollbar.set)
            
            # グリッドレイアウト
            inspector_tree.grid(row=0, column=0, sticky="nsew")
            inspector_v_scrollbar.grid(row=0, column=1, sticky="ns")
            inspector_h_scrollbar.grid(row=1, column=0, sticky="ew")
            
            table_frame.grid_rowconfigure(0, weight=1)
            table_frame.grid_columnconfigure(0, weight=1)
            
            # データの挿入
            row_index = 0
            target_tree_item = None  # 選択する行のTreeviewアイテム
            # 列名から列インデックスへのマッピングを作成（高速化：itertuples()を使用）
            inspector_col_idx_map = {col: inspector_df.columns.get_loc(col) for col in inspector_columns if col in inspector_df.columns}
            
            for row_tuple in inspector_df.itertuples(index=True):
                row_idx = row_tuple[0]  # インデックス
                row = inspector_df.loc[row_idx]  # Seriesとして扱うために元の行を取得
                values = []
                for col in inspector_columns:
                    # 列が存在しない場合は空文字を表示
                    if col not in inspector_df.columns:
                        values.append('')
                        continue
                    
                    col_idx = inspector_col_idx_map.get(col)
                    if col_idx is not None:
                        # itertuples(index=True)では、row_tuple[0]がインデックス、row_tuple[1]以降が列の値
                        # 列インデックスは0始まりなので、col_idx + 1でアクセス
                        if col_idx + 1 < len(row_tuple):
                            col_value = row_tuple[col_idx + 1]
                        else:
                            col_value = None
                    else:
                        col_value = None
                    
                    if col == '出荷予定日' or col == '指示日':
                        try:
                            date_value = pd.to_datetime(col_value) if pd.notna(col_value) else None
                            values.append(date_value.strftime('%Y/%m/%d') if date_value is not None else '')
                        except:
                            values.append(str(col_value) if pd.notna(col_value) else '')
                    elif col.startswith('検査員'):
                        inspector_name = self._strip_skill_annotation(col_value if pd.notna(col_value) else None)
                        values.append(inspector_name)
                    else:
                        values.append(str(col_value) if pd.notna(col_value) else '')
                
                # 交互行色を適用
                tag = "even" if row_index % 2 == 0 else "odd"
                tree_item = inspector_tree.insert("", "end", values=values, tags=(tag,))
                
                # 対象行を記録
                if target_row_index is not None and row_idx == target_row_index:
                    target_tree_item = tree_item
                
                row_index += 1
            
            # タグの設定（交互行色）
            inspector_tree.tag_configure("even", background="#F9FAFB")
            inspector_tree.tag_configure("odd", background="#FFFFFF")
            
            # マウスホイールイベントのバインド（テーブルとフレーム全体にバインド）
            def on_inspector_mousewheel(event):
                # スクロール量を計算（元の速度に戻す）
                scroll_amount = int(-1 * (event.delta / 120))
                inspector_tree.yview_scroll(scroll_amount, "units")
                return "break"  # イベントの伝播を止める
            
            # テーブルとフレーム全体にマウスホイールイベントをバインド
            inspector_tree.bind("<MouseWheel>", on_inspector_mousewheel)
            table_frame.bind("<MouseWheel>", on_inspector_mousewheel)
            inspector_frame.bind("<MouseWheel>", on_inspector_mousewheel)
            
            # テーブルに入ったときと出たときのイベント（メインスクロールのbind_allを一時的に解除）
            def on_inspector_enter(event):
                # テーブル内ではメインスクロールのbind_allを一時的に解除
                # これにより、テーブルのマウスホイールイベントが優先される
                try:
                    self.root.unbind_all("<MouseWheel>")
                    # フラグをリセット（再バインド可能にするため）
                    self._main_scroll_bound = False
                except:
                    pass
            
            def on_inspector_leave(event):
                # テーブルから出たときはメインスクロールを再バインド
                self.bind_main_scroll()
            
            inspector_tree.bind("<Enter>", on_inspector_enter)
            inspector_tree.bind("<Leave>", on_inspector_leave)
            table_frame.bind("<Enter>", on_inspector_enter)
            table_frame.bind("<Leave>", on_inspector_leave)
            inspector_frame.bind("<Enter>", on_inspector_enter)
            inspector_frame.bind("<Leave>", on_inspector_leave)
            
            # 【追加】右クリックメニューの実装
            def show_inspector_context_menu(event):
                """検査員列の右クリックメニューを表示"""
                try:
                    # クリックされた位置のアイテムと列を取得
                    item = inspector_tree.identify_row(event.y)
                    column = inspector_tree.identify_column(event.x)
                    
                    if not item or not column:
                        return
                    
                    # 列名を取得（列番号から列名に変換）
                    col_index = int(column.replace('#', '')) - 1
                    if col_index < 0 or col_index >= len(inspector_columns):
                        return
                    
                    col_name = inspector_columns[col_index]
                    
                    # 検査員列（検査員1～10）の場合のみメニューを表示
                    if not col_name.startswith('検査員'):
                        return
                    
                    # 現在の値を取得
                    item_values = inspector_tree.item(item, 'values')
                    current_inspector = item_values[col_index] if col_index < len(item_values) else ''
                    
                    # 行インデックスを取得（テーブルの行番号）
                    row_index_in_tree = inspector_tree.index(item)
                    
                    # メニューを作成
                    context_menu = tk.Menu(self.root, tearoff=0)
                    context_menu.add_command(
                        label=f"検査員を変更（現在: {current_inspector if current_inspector else '未割当'}）",
                        command=lambda: self.change_inspector_dialog(row_index_in_tree, col_name, col_index, current_inspector, inspector_df)
                    )
                    context_menu.add_separator()
                    context_menu.add_command(
                        label="検査員を削除",
                        command=lambda: self.remove_inspector_from_table(row_index_in_tree, col_name, col_index, inspector_df)
                    )
                    
                    # メニューを表示
                    try:
                        context_menu.tk_popup(event.x_root, event.y_root)
                    finally:
                        context_menu.grab_release()
                
                except Exception as e:
                    self.log_message(f"右クリックメニューの表示に失敗しました: {str(e)}")
                    logger.error(f"右クリックメニューの表示に失敗しました: {str(e)}", exc_info=True)
            
            inspector_tree.bind("<Button-3>", show_inspector_context_menu)  # 右クリック
            
            # スクロール位置を復元（選択行の表示より優先）
            if preserve_scroll_position and (saved_scroll_position or saved_first_visible_row_data):
                try:
                    saved_pos = saved_scroll_position[0] if saved_scroll_position else None
                    saved_row_data = saved_first_visible_row_data
                    saved_main_pos = saved_main_scroll_position[0] if saved_main_scroll_position else None
                    target_item = target_tree_item  # クロージャで使用するため変数に保存
                    
                    # 少し遅延を入れてからスクロール位置を復元（テーブルが完全に描画された後）
                    def restore_scroll():
                        try:
                            # メインスクロールフレームの位置を先に復元
                            if saved_main_pos is not None:
                                try:
                                    if hasattr(self.main_scroll_frame, '_parent_canvas'):
                                        canvas = self.main_scroll_frame._parent_canvas
                                        if canvas:
                                            canvas.yview_moveto(saved_main_pos)
                                except:
                                    pass
                            
                            # テーブル内のスクロール位置を復元
                            if saved_row_data:
                                # 保存した行のデータから該当行を探す
                                try:
                                    all_items = inspector_tree.get_children()
                                    target_item_found = None
                                    
                                    # 品番とロットIDで一致する行を検索
                                    for item in all_items:
                                        item_values = inspector_tree.item(item, 'values')
                                        if len(item_values) > 4:
                                            product_match = (saved_row_data['product_number'] and 
                                                           item_values[1] == saved_row_data['product_number'])
                                            lot_match = (saved_row_data['lot_id'] and 
                                                        item_values[4] == saved_row_data['lot_id'])
                                            
                                            # 品番またはロットIDが一致する場合
                                            if product_match or lot_match:
                                                target_item_found = item
                                                break
                                    
                                    if target_item_found:
                                        # 保存したスクロール位置を直接使用
                                        if saved_pos is not None:
                                            inspector_tree.yview_moveto(saved_pos)
                                        else:
                                            # スクロール位置が保存されていない場合は、行の位置から計算
                                            item_index = inspector_tree.index(target_item_found)
                                            total_items = len(all_items)
                                            if total_items > 0:
                                                target_scroll_pos = max(0.0, min(1.0, item_index / total_items))
                                                inspector_tree.yview_moveto(target_scroll_pos)
                                except Exception as e:
                                    logger.debug(f"行データからのスクロール位置復元に失敗: {str(e)}")
                                    # フォールバック: 保存したスクロール位置を使用
                                    if saved_pos is not None:
                                        inspector_tree.yview_moveto(saved_pos)
                            elif saved_pos is not None:
                                # 行データがない場合は、保存したスクロール位置を直接使用
                                inspector_tree.yview_moveto(saved_pos)
                            
                            # 対象行を選択（スクロール位置は変更しない）
                            if target_item:
                                inspector_tree.selection_set(target_item)
                                inspector_tree.focus(target_item)
                                # see()は呼ばない（スクロール位置を保持するため）
                            
                            # 復元後に再確認して、必要に応じて再試行
                            if saved_pos is not None:
                                self.root.after(50, lambda: self._verify_and_restore_scroll(inspector_tree, saved_pos))
                        except Exception as e:
                            logger.debug(f"スクロール位置の復元に失敗: {str(e)}")
                    # テーブルが完全に描画されるまで待つ（遅延を増やす）
                    self.root.after(250, restore_scroll)  # 遅延を250msに増やす
                except:
                    pass
            else:
                # スクロール位置を保持しない場合は、対象行を表示
                if target_tree_item:
                    try:
                        def select_target_row():
                            try:
                                inspector_tree.selection_set(target_tree_item)
                                inspector_tree.focus(target_tree_item)
                                # 行が見えるようにスクロール
                                inspector_tree.see(target_tree_item)
                            except:
                                pass
                        self.root.after(20, select_target_row)
                    except:
                        pass
            
            # テーブルとデータフレームを保持（後で更新するため）
            self.current_inspector_tree = inspector_tree
            self.current_inspector_df = inspector_df
            self.current_display_table = "inspector"
            if hasattr(self, "inspector_button"):
                self.update_button_states("inspector")
            
            self.log_message(f"検査員割振りテーブルを表示しました: {len(inspector_df)}件")
            
        except Exception as e:
            error_msg = f"検査員割振りテーブルの表示に失敗しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
    
    def _verify_and_restore_scroll(self, tree, target_pos, retry_count=0):
        """スクロール位置が正しく復元されたか確認し、必要に応じて再試行"""
        try:
            current_pos = tree.yview()[0]
            if abs(current_pos - target_pos) > 0.01 and retry_count < 3:  # 0.01以上の差がある場合、最大3回再試行
                tree.yview_moveto(target_pos)
                self.root.after(20, lambda: self._verify_and_restore_scroll(tree, target_pos, retry_count + 1))
        except:
            pass

    @staticmethod
    def _normalize_inspector_column_name(name: Optional[str]) -> str:
        if not name:
            return ''
        normalized = ''.join(name.split())
        return normalized.lower()

    @staticmethod
    def _normalize_seating_row_key(value: object) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            numeric = float(text)
            if numeric.is_integer():
                return str(int(numeric))
            normalized = str(numeric).rstrip('0').rstrip('.')
            return normalized if normalized else text
        except (ValueError, TypeError):
            return text

    @staticmethod
    def _strip_skill_annotation(inspector_name: Optional[str]) -> str:
        """括弧付きスキル表記を取り除いて検査員名のみを返す"""
        if not inspector_name:
            return ""
        name = str(inspector_name).strip()
        if not name:
            return ""
        if "(" in name and ")" in name:
            open_idx = name.find("(")
            return name[:open_idx].strip()
        return name
    
    def open_seating_chart(self):
        """Export current lot assignments to the seating UI."""
        if self.current_inspector_data is None or self.current_inspector_data.empty:
            messagebox.showwarning("Seat chart", "Inspector assignment data is not available.")
            return
        lots_by_inspector = self._serialize_inspector_lots_for_seating()
        logger.bind(channel="UI:SEAT").debug(
            "serialize_inspector_lots_for_seating inspectors={}",
            len(lots_by_inspector),
        )
        if not lots_by_inspector:
            messagebox.showinfo("Seat chart", "No lot data is available for seating layout export.")
            return
        unassigned_lots = lots_by_inspector.pop(self.UNASSIGNED_LOTS_KEY, [])
        inspector_names = self._resolve_inspector_names_for_seating()
        if not inspector_names:
            inspector_names = list(lots_by_inspector.keys())
        chart = None
        if os.path.exists(SEATING_JSON_PATH):
            try:
                chart = load_seating_chart(SEATING_JSON_PATH)
                if not chart.get("seats"):
                    chart = None
            except Exception:
                chart = None
        if chart is None:
            chart = build_initial_seating_chart(inspector_names)
        chart = attach_lots_to_chart(chart, lots_by_inspector)
        chart["unassigned_lots"] = unassigned_lots
        chart["inspector_column_map"] = self.inspector_column_map_for_seating.copy()
        try:
            save_seating_chart(SEATING_JSON_PATH, chart)
            generate_html(chart, SEATING_HTML_PATH, inspector_candidates=inspector_names)
            self._open_seating_chart_html(SEATING_HTML_PATH)
            self.log_message(f"Seat chart generated: {SEATING_HTML_PATH}")
            self._set_seating_flow_prompt("座席表で割当を変更したら「ロット振分変更反映」を押してください。")
        except Exception as exc:
            messagebox.showerror("Seat chart", f"Failed to generate seat chart: {exc}")
            logger.error("Seat chart export failed", exc_info=True)

    def _open_seating_chart_html(self, html_path: str) -> None:
        """ネットワーク共有先の HTML をブラウザで開くためのラッパー"""
        try:
            seating_path = Path(html_path)
            if not seating_path.exists():
                logger.warning("Seat chart HTMLが見つかりません: {}", html_path)
                messagebox.showwarning("Seat chart", f"座席表HTMLが存在しません: {html_path}")
                return
            file_url = ""
            try:
                file_url = seating_path.as_uri()
            except ValueError:
                file_url = seating_path.resolve().as_uri()
            server_url = None
            try:
                self._seat_chart_server.start()
                server_url = self._seat_chart_server.get_html_url(html_path)
            except Exception as exc:
                logger.debug("Seat chart server の起動に失敗しました: {}", exc)
            target_url = server_url or file_url
            opened = False
            try:
                opened = webbrowser.open(target_url)
            except Exception:
                opened = False
            if not opened and os.name == "nt" and hasattr(os, "startfile"):
                try:
                    os.startfile(str(seating_path))
                    opened = True
                except Exception:
                    logger.debug("os.startfile による座席表 HTML の起動に失敗しました", exc_info=True)
            if opened:
                logger.bind(channel="UI:SEAT").debug("Seat chart opened: {}", target_url)
            else:
                logger.warning("Seat chart HTML を自動的に開けませんでした: {}", html_path)
                messagebox.showwarning("Seat chart", f"座席表HTMLを開くことができませんでした。\n{html_path}")
        except Exception as exc:
            logger.error("Seat chart HTML を開けませんでした", exc_info=True)
            messagebox.showerror("Seat chart", f"座席表を開く処理でエラーが発生しました: {exc}")

    def apply_seating_chart_results(self):
        """Update the assignment table from the seating_chart.json file."""
        if self.current_inspector_data is None or self.current_inspector_data.empty:
            messagebox.showwarning("Seat chart sync", "Inspector assignment table is empty.")
            return
        if not os.path.exists(SEATING_JSON_PATH):
            messagebox.showwarning("Seat chart sync", f"JSON file not found: {SEATING_JSON_PATH}")
            return
        try:
            chart = load_seating_chart(SEATING_JSON_PATH)
        except Exception as exc:
            messagebox.showerror("Seat chart sync", f"Failed to load seating JSON: {exc}")
            logger.error("Seat chart load failed", exc_info=True)
            return
        inspector_cols = [col for col in self.current_inspector_data.columns if col.startswith("検査員")]
        normalized_columns: List[Tuple[str, str]] = []
        seen_norms = set()
        for col in inspector_cols:
            normalized_col = self._normalize_inspector_column_name(col)
            if normalized_col and normalized_col not in seen_norms:
                normalized_columns.append((normalized_col, col))
                seen_norms.add(normalized_col)

        rowcol_to_inspector: Dict[Tuple[str, str], str] = {}
        product_code_candidates = ["品番", "製品番号", "製品コード", "製品CD", "品目コード"]
        lot_key_to_inspector: Dict[str, Deque[str]] = {}
        # lot_keyからsource_inspector_colを取得するマッピング
        lot_key_to_source_col: Dict[str, str] = {}
        seen_rowcol_keys = set()
        seen_lot_keys = set()
        # 座席表のロット順番を保持するためのマッピング
        # {lot_key: (inspector_name, order_index, global_order)} の形式
        # global_order: 座席表全体での順序（全座席を通した順序）
        lot_key_to_order: Dict[str, Tuple[str, int, int]] = {}
        global_order_counter = 0
        for seat in chart.get("seats", []):
            inspector_name = (seat.get("name") or "").strip()
            if not inspector_name:
                continue
            lots = seat.get("lots", [])
            for order_index, lot in enumerate(lots):
                source_row = lot.get("source_row_index")
                source_row_key = lot.get("source_row_key")
                source_col = lot.get("source_inspector_col")
                normalized_row = (
                    source_row_key if source_row_key else self._normalize_seating_row_key(source_row)
                )
                normalized_col = self._normalize_inspector_column_name(source_col)
                if normalized_row and normalized_col:
                    rowcol_key = (normalized_row, normalized_col)
                    if rowcol_key not in seen_rowcol_keys:
                        rowcol_to_inspector[rowcol_key] = inspector_name
                        seen_rowcol_keys.add(rowcol_key)
                lot_key = lot.get("lot_key")
                if lot_key:
                    if lot_key not in seen_lot_keys:
                        lot_key_to_inspector.setdefault(lot_key, deque()).append(inspector_name)
                        seen_lot_keys.add(lot_key)
                    # lot_keyからsource_inspector_colを取得できるようにする
                    if source_col:
                        lot_key_to_source_col[lot_key] = source_col
                    # ロット順番を記録（座席表の検査員名、座席内での順序、全体での順序）
                    # 同じlot_keyが複数回出現する場合、最後の出現位置を優先（座席表での最新の状態を反映）
                    lot_key_to_order[lot_key] = (inspector_name, order_index, global_order_counter)
                    global_order_counter += 1
        
        # 未割当ロットの処理: 未割当ロットに対応する行の検査員列をクリア
        unassigned_lots = chart.get("unassigned_lots", [])
        unassigned_lot_keys = set()
        unassigned_rowcol_keys = set()
        unassigned_row_keys = set()  # source_inspector_colが空の場合、行全体をクリア
        for lot in unassigned_lots:
            lot_key = lot.get("lot_key")
            if lot_key:
                unassigned_lot_keys.add(lot_key)
            source_row = lot.get("source_row_index")
            source_row_key = lot.get("source_row_key")
            source_col = lot.get("source_inspector_col")
            normalized_row = (
                source_row_key if source_row_key else self._normalize_seating_row_key(source_row)
            )
            if normalized_row:
                if source_col and source_col.strip():
                    # source_inspector_colが指定されている場合、特定の列をクリア
                    normalized_col = self._normalize_inspector_column_name(source_col)
                    if normalized_col:
                        unassigned_rowcol_keys.add((normalized_row, normalized_col))
                else:
                    # source_inspector_colが空の場合、行全体をクリア対象とする
                    unassigned_row_keys.add(normalized_row)

        if not rowcol_to_inspector and not lot_key_to_inspector and not unassigned_lot_keys and not unassigned_rowcol_keys:
            logger.info("Seat chart sync: rowcol_to_inspector is empty")
            messagebox.showinfo("Seat chart sync", "Seating chart has no lot assignments.")
            return
        logger.info(
            "Seat chart sync: rowcol_to_inspector entries={}, lot_key entries={}, unassigned_lots={}",
            len(rowcol_to_inspector),
            len(lot_key_to_inspector),
            len(unassigned_lots),
        )

        df = self.current_inspector_data.copy()
        updated = 0
        matched_by_rowcol = 0
        matched_by_lot_key = 0
        matched_by_lot_key_no_col = 0
        for row_index, row in df.iterrows():
            row_key = self._normalize_seating_row_key(row_index)
            if not row_key:
                continue
            lot_key = self._derive_lot_key(row, row_index, product_code_candidates)
            row_modified = False
            assigned = None
            target_col = None
            # まずrowcolでマッチングを試みる
            for normalized_col, actual_col in normalized_columns:
                assigned = rowcol_to_inspector.get((row_key, normalized_col))
                if assigned:
                    target_col = actual_col
                    matched_by_rowcol += 1
                    break
            # rowcolでマッチングできなかった場合、lot_keyでマッチングを試みる
            if not assigned and lot_key:
                inspectors_queue = lot_key_to_inspector.get(lot_key)
                if inspectors_queue:
                    assigned = inspectors_queue.popleft()
                    if not inspectors_queue:
                        lot_key_to_inspector.pop(lot_key, None)
                    # lot_keyでマッチングした場合、source_inspector_colから対応する列を特定
                    source_col = lot_key_to_source_col.get(lot_key)
                    if source_col:
                        normalized_source_col = self._normalize_inspector_column_name(source_col)
                        for norm_col, act_col in normalized_columns:
                            if norm_col == normalized_source_col:
                                target_col = act_col
                                matched_by_lot_key += 1
                                break
                        if not target_col:
                            matched_by_lot_key_no_col += 1
                            logger.debug(
                                "lot_key matched but target_col not found: lot_key={}, source_col={}, normalized_source_col={}, available_normalized_columns={}",
                                lot_key,
                                source_col,
                                normalized_source_col,
                                [nc for nc, _ in normalized_columns],
                            )
                    else:
                        matched_by_lot_key_no_col += 1
                        logger.debug(
                            "lot_key matched but source_col not found: lot_key={}",
                            lot_key,
                        )
            # マッチングできた場合、対応する検査員列を更新
            if assigned:
                if target_col:
                    # 特定の列が特定されている場合、その列のみを更新
                    current_value = df.at[row_index, target_col]
                    current_value_str = str(current_value).strip() if pd.notna(current_value) else ""
                    assigned_str = str(assigned).strip()
                    # デバッグ: 最初の10件のみ詳細ログを出力
                    if matched_by_rowcol <= 10:
                        will_update = pd.isna(current_value) or current_value_str != assigned_str
                        logger.info(
                            "Seat chart sync update check: row_index={}, target_col={}, current_value='{}', assigned='{}', will_update={}",
                            row_index,
                            target_col,
                            current_value_str,
                            assigned_str,
                            will_update,
                        )
                    if pd.isna(current_value) or current_value_str != assigned_str:
                        # 他の列から同じ検査員を削除
                        for norm_col, act_col in normalized_columns:
                            if act_col != target_col:
                                other_value = df.at[row_index, act_col]
                                if pd.notna(other_value) and str(other_value).strip() == assigned_str:
                                    df.at[row_index, act_col] = ""
                        # 新しい検査員を設定
                        df.at[row_index, target_col] = assigned_str
                        updated += 1
                        row_modified = True
                    else:
                        # 既に同じ値が設定されている場合でも、他の列に同じ検査員がいる場合は削除
                        for norm_col, act_col in normalized_columns:
                            if act_col != target_col:
                                other_value = df.at[row_index, act_col]
                                if pd.notna(other_value) and str(other_value).strip() == assigned_str:
                                    df.at[row_index, act_col] = ""
                                    updated += 1
                                    row_modified = True
                else:
                    # 列が特定できない場合、最初の空いている列に設定
                    for normalized_col, actual_col in normalized_columns:
                        current_value = df.at[row_index, actual_col]
                        if pd.isna(current_value) or not str(current_value).strip():
                            df.at[row_index, actual_col] = assigned
                            updated += 1
                            row_modified = True
                            break
                    # すべての列が埋まっている場合、最初の列を上書き
                    if not row_modified and normalized_columns:
                        first_col = normalized_columns[0][1]
                        current_value = df.at[row_index, first_col]
                        if pd.isna(current_value) or str(current_value).strip() != assigned:
                            df.at[row_index, first_col] = assigned
                            updated += 1
                            row_modified = True
            if row_modified:
                self._recalculate_inspector_count_and_divided_time(df, row_index)
        
        # 未割当ロットの処理: 対応する行の検査員列をクリア
        unassigned_cleared = 0
        unassigned_matched_by_lot_key = 0
        unassigned_matched_by_rowcol = 0
        unassigned_matched_by_row = 0
        for row_index, row in df.iterrows():
            row_key = self._normalize_seating_row_key(row_index)
            if not row_key:
                continue
            lot_key = self._derive_lot_key(row, row_index, product_code_candidates)
            row_modified = False
            
            # lot_keyで未割当かどうかを確認（最優先）
            if lot_key and lot_key in unassigned_lot_keys:
                # 未割当ロットの場合、すべての検査員列をクリア
                for normalized_col, actual_col in normalized_columns:
                    current_value = df.at[row_index, actual_col]
                    if pd.notna(current_value) and str(current_value).strip():
                        logger.debug(
                            "Unassigned lot cleared by lot_key: row_index={}, lot_key={}, col={}, value='{}'",
                            row_index,
                            lot_key,
                            actual_col,
                            str(current_value).strip(),
                        )
                        df.at[row_index, actual_col] = ""
                        unassigned_cleared += 1
                        row_modified = True
                if row_modified:
                    unassigned_matched_by_lot_key += 1
            # rowcolで未割当かどうかを確認
            elif row_key:
                matched_by_rowcol = False
                for normalized_col, actual_col in normalized_columns:
                    rowcol_key = (row_key, normalized_col)
                    if rowcol_key in unassigned_rowcol_keys:
                        # 未割当ロットの場合、該当する検査員列をクリア
                        current_value = df.at[row_index, actual_col]
                        if pd.notna(current_value) and str(current_value).strip():
                            logger.debug(
                                "Unassigned lot cleared by rowcol: row_index={}, rowcol_key={}, col={}, value='{}'",
                                row_index,
                                rowcol_key,
                                actual_col,
                                str(current_value).strip(),
                            )
                            df.at[row_index, actual_col] = ""
                            unassigned_cleared += 1
                            row_modified = True
                            matched_by_rowcol = True
                if matched_by_rowcol:
                    unassigned_matched_by_rowcol += 1
                # row_keyで未割当かどうかを確認（source_inspector_colが空の場合）
                elif row_key in unassigned_row_keys:
                    # 未割当ロットの場合、すべての検査員列をクリア
                    for normalized_col, actual_col in normalized_columns:
                        current_value = df.at[row_index, actual_col]
                        if pd.notna(current_value) and str(current_value).strip():
                            logger.debug(
                                "Unassigned lot cleared by row_key: row_index={}, row_key={}, col={}, value='{}'",
                                row_index,
                                row_key,
                                actual_col,
                                str(current_value).strip(),
                            )
                            df.at[row_index, actual_col] = ""
                            unassigned_cleared += 1
                            row_modified = True
                    if row_modified:
                        unassigned_matched_by_row += 1
            
            if row_modified:
                self._recalculate_inspector_count_and_divided_time(df, row_index)
        
        logger.info(
            "Seat chart sync: updated={}, matched_by_rowcol={}, matched_by_lot_key={}, matched_by_lot_key_no_col={}, unassigned_cleared={} (matched_by_lot_key={}, matched_by_rowcol={}, matched_by_row={})",
            updated,
            matched_by_rowcol,
            matched_by_lot_key,
            matched_by_lot_key_no_col,
            unassigned_cleared,
            unassigned_matched_by_lot_key,
            unassigned_matched_by_rowcol,
            unassigned_matched_by_row,
        )
        if matched_by_rowcol > 0 and updated == 0:
            logger.warning(
                "Seat chart sync: matched_by_rowcol={} but updated=0. This may indicate that values are already correct or comparison logic has issues.",
                matched_by_rowcol,
            )
        if updated == 0:
            messagebox.showinfo("Seat chart sync", "No matching lots were updated.")
            return

        self.current_inspector_data = df
        self.current_display_table = "inspector"
        if hasattr(self, 'inspector_button'):
            self.update_button_states("inspector")
        self.display_inspector_assignment_table(df, preserve_scroll_position=True)
        # 強制的に GUI を更新して、視覚的な反映を促す
        try:
            self.root.update_idletasks()
        except Exception:
            pass
        self.original_inspector_data = df.copy()
        # 座席表のロット順番を保存（Googleスプレッドシート出力時に使用）
        self.seating_chart_lot_order = lot_key_to_order.copy()
        self._set_seating_flow_prompt("変更が反映されました。次に「Googleスプレッドシートへ出力」を押してください。")
        self.log_message(f"Applied seating results to {updated} lots.")

    def _serialize_inspector_lots_for_seating(self):
        """Collect lots keyed by the inspector who owns the first assignment column."""
        df = self.current_inspector_data
        if df is None or df.empty:
            return {}
        logger.bind(channel="UI:SEAT").debug("serialize_inspector_lots_for_seating columns={}", df.columns.tolist())
        if not df.empty:
            logger.bind(channel="UI:SEAT").debug("sample row: {}", df.iloc[0].to_dict())
        inspector_cols = [
            col for col in df.columns
            if col.startswith("検査員") and col[len("検査員"):].isdigit()
        ]
        inspector_column_map: Dict[str, str] = {}
        lot_id_candidates = ["生産ロットID", "ロットID", "LotID"]
        product_code_candidates = ["品番", "製品番号", "製品コード", "製品CD", "品目コード"]
        product_name_candidates = ["品名", "製品名", "製品名称", "品目名", "品目名称"]
        lots = defaultdict(list)
        unassigned_lots = []
        def format_shipping_date(value):
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return ""
            if isinstance(value, (datetime, date)):
                return value.strftime("%Y-%m-%d")
            if isinstance(value, pd.Timestamp):
                return value.strftime("%Y-%m-%d")
            text = str(value).strip()
            return text

        def normalize_inspection_time(value):
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        def resolve_inspection_time(divided_value, normal_value, assigned_flag):
            divided = normalize_inspection_time(divided_value)
            normal = normalize_inspection_time(normal_value)
            if assigned_flag:
                if divided is not None and divided > 0:
                    return divided
                if normal is not None:
                    return normal
                return divided if divided is not None else 0.0
            # unassigned: prefer normal time when available
            if normal is not None:
                return normal
            if divided is not None:
                return divided
            return 0.0

        for row_index, row in df.iterrows():
            lot_id = ""
            lot_key = self._derive_lot_key(row, row_index, product_code_candidates)
            for candidate in lot_id_candidates:
                if candidate in df.columns:
                    value = row.get(candidate)
                    if pd.notna(value):
                        candidate_id = str(value).strip()
                        if candidate_id:
                            lot_id = candidate_id
                            break
            if not lot_id:
                lot_id = self._derive_lot_key(row, row_index, product_code_candidates)
            if not lot_id:
                lot_id = f"lot-{row_index}"
            product_code = ""
            for candidate in product_code_candidates:
                if candidate in df.columns:
                    value = row.get(candidate)
                    if pd.notna(value):
                        cleaned = str(value).strip()
                        if cleaned:
                            product_code = cleaned
                            break
            product_name = ""
            for candidate in product_name_candidates:
                if candidate in df.columns:
                    value = row.get(candidate)
                    if pd.notna(value):
                        cleaned = str(value).strip()
                        if cleaned:
                            product_name = cleaned
                            break
            process_name = ""
            if "現在工程名" in df.columns:
                value = row.get("現在工程名")
                if pd.notna(value):
                    process_name = str(value).strip()
            divided_time_value = row.get("分割検査時間") if "分割検査時間" in df.columns else None
            normal_time_value = row.get("検査時間") if "検査時間" in df.columns else None
            shipping_date_value = ""
            if "出荷予定日" in df.columns:
                shipping_date_value = row.get("出荷予定日")
            shipping_date_text = format_shipping_date(shipping_date_value)
            row_key = self._normalize_seating_row_key(row_index)
            lot_base = {
                "lot_id": lot_id,
                "product_name": product_name,
                "product_code": product_code,
                "sec_per_piece": 0.0,
                "inspection_time": 0.0,
                "source_row_index": str(row_index),
                "source_row_key": row_key,
                "lot_key": lot_key,
                "shipping_date": shipping_date_text,
                "process_name": process_name,
            }
            assigned = False
            for inspector_col in inspector_cols:
                name_value = row.get(inspector_col)
                if not (pd.notna(name_value) and str(name_value).strip()):
                    continue
                inspector_name = str(name_value).strip()
                lot_entry = lot_base.copy()
                lot_entry["source_inspector_col"] = inspector_col
                inspector_column_map.setdefault(inspector_name, inspector_col)
                lots[inspector_name].append(lot_entry)
                inspection_time = resolve_inspection_time(divided_time_value, normal_time_value, True)
                lot_entry["inspection_time"] = inspection_time
                lot_entry["sec_per_piece"] = inspection_time * 3600.0
                assigned = True
            if not assigned:
                unassigned_entry = lot_base.copy()
                unassigned_entry["source_inspector_col"] = ""
                inspection_time = resolve_inspection_time(divided_time_value, normal_time_value, False)
                unassigned_entry["inspection_time"] = inspection_time
                unassigned_entry["sec_per_piece"] = inspection_time * 3600.0
                unassigned_lots.append(unassigned_entry)
        if unassigned_lots:
            lots[self.UNASSIGNED_LOTS_KEY] = unassigned_lots
        self.inspector_column_map_for_seating = inspector_column_map.copy()
        return dict(lots)

    def _derive_lot_key(self, row, row_index, product_code_candidates):
        """品番・ロット数量・指示日から代替の lot_id を構築"""
        parts: List[str] = []
        for candidate in product_code_candidates:
            value = row.get(candidate)
            if pd.notna(value):
                clean = str(value).strip()
                if clean:
                    parts.append(clean)
                    break
        if "ロット数量" in row.index:
            value = row.get("ロット数量")
            if pd.notna(value):
                parts.append(str(value).strip())
        if "指示日" in row.index:
            value = row.get("指示日")
            if pd.notna(value):
                parts.append(str(value).strip())
        key_components = []
        key_components.append(f"idx{row_index}")
        if parts:
            key_components.extend(parts)
        return "_".join(key_components)
    
    def _sort_dataframe_by_seating_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        座席表のロット順番に基づいてDataFrameを並び替える
        
        Args:
            df: 並び替えるDataFrame
            
        Returns:
            並び替えられたDataFrame
        """
        if not hasattr(self, 'seating_chart_lot_order') or not self.seating_chart_lot_order:
            return df
        
        product_code_candidates = ["品番", "製品番号", "製品コード", "製品CD", "品目コード"]
        inspector_cols = [col for col in df.columns if col.startswith("検査員")]
        
        # 各行のロットキーを計算し、座席表の順番を取得
        def get_sort_key(row):
            row_index = row.name
            lot_key = self._derive_lot_key(row, row_index, product_code_candidates)
            
            # 割り当てられている検査員を取得
            assigned_inspector = None
            for col in inspector_cols:
                value = row.get(col)
                if pd.notna(value) and str(value).strip():
                    assigned_inspector = str(value).strip().split('(')[0].strip()
                    break
            
            if lot_key in self.seating_chart_lot_order:
                # 座席表の順序情報を取得
                # 形式: (inspector_name, order_index, global_order) または (inspector_name, order_index)
                order_info = self.seating_chart_lot_order[lot_key]
                if len(order_info) >= 3:
                    inspector_name, order_index, global_order = order_info
                else:
                    inspector_name, order_index = order_info
                    global_order = 999999  # 後方互換性のため
                
                # 座席表の検査員名と一致する場合
                if assigned_inspector == inspector_name:
                    # 検査員名でグループ化し、その中で座席表の順番でソート
                    return (0, inspector_name or "", order_index, global_order)
                else:
                    # 座席表の検査員名と一致しない場合でも、lot_keyが一致すれば順序を使用
                    # これは、座席表でロットが移動された場合でも順序を保持するため
                    # 割り当てられている検査員でグループ化し、座席表の順序を使用
                    return (1, assigned_inspector or "", global_order, order_index)
            else:
                # 座席表にないロットは最後に配置（割り当てられている検査員でグループ化）
                # 元の行インデックスで安定ソートを保証
                return (2, assigned_inspector or "", 999999, row_index)
        
        # ソートキーを計算
        sort_keys = df.apply(get_sort_key, axis=1)
        df_sorted = df.iloc[sort_keys.argsort()].copy()
        
        return df_sorted
    def _resolve_inspector_names_for_seating(self):
        """Return inspector names derived from the master or current table."""
        names = []
        if self.inspector_master_data is not None and "#氏名" in self.inspector_master_data.columns:
            seen = set()
            for raw in self.inspector_master_data["#氏名"].dropna().astype(str):
                candidate = raw.strip()
                if candidate and candidate not in seen:
                    seen.add(candidate)
                    names.append(candidate)
            return names
        if self.current_inspector_data is not None:
            inspector_cols = [col for col in self.current_inspector_data.columns if col.startswith("検査員")]
            seen = set()
            for col in inspector_cols:
                for raw in self.current_inspector_data[col].dropna().astype(str):
                    candidate = raw.strip()
                    if candidate and candidate not in seen:
                        seen.add(candidate)
                        names.append(candidate)
        return names

    def change_inspector_dialog(self, row_index_in_tree, col_name, col_index, current_inspector, inspector_df):
        """検査員を変更するダイアログを表示"""
        try:
            # 元のDataFrameのインデックスを取得
            if inspector_df is None or inspector_df.empty:
                self.log_message("エラー: 検査員割当てデータが見つかりません")
                return
            
            # 行インデックスを取得
            if row_index_in_tree >= len(inspector_df):
                self.log_message(f"エラー: 行インデックスが範囲外です: {row_index_in_tree}")
                return
            
            original_index = inspector_df.index[row_index_in_tree]
            row = inspector_df.iloc[original_index]
            
            # 検査員マスタを読み込む（キャッシュを活用）
            inspector_master_df = self.load_inspector_master_cached()
            if inspector_master_df is None or inspector_master_df.empty:
                self.log_message("エラー: 検査員マスタを読み込めません")
                return
            
            # 検査員選択ダイアログを作成
            dialog = ctk.CTkToplevel(self.root)
            dialog.title("検査員を選択")
            dialog.geometry("400x500")
            dialog.transient(self.root)
            dialog.grab_set()
            
            # ラベル
            label = ctk.CTkLabel(
                dialog,
                text=f"検査員列「{col_name}」の検査員を選択してください",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold")
            )
            label.pack(pady=10)
            
            # 現在の検査員を表示
            if current_inspector:
                current_label = ctk.CTkLabel(
                    dialog,
                    text=f"現在: {current_inspector}",
                    font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                    text_color="#6B7280"
                )
                current_label.pack(pady=5)
            
            # スクロール可能なフレーム
            scroll_frame = ctk.CTkScrollableFrame(dialog)
            scroll_frame.pack(fill="both", expand=True, padx=20, pady=10)
            
            # マウスホイールイベントのバインド（CTkScrollableFrame用）
            def on_scroll_mousewheel(event):
                # スクロール量を計算（速度を上げるため10倍にする）
                scroll_amount = int(-1 * (event.delta / 120)) * 10
                # CTkScrollableFrameの正しいスクロールメソッドを使用
                if hasattr(scroll_frame, 'yview_scroll'):
                    scroll_frame.yview_scroll(scroll_amount, "units")
                else:
                    # CTkScrollableFrameの場合は内部のCanvasを直接操作
                    canvas = scroll_frame._parent_canvas
                    if canvas:
                        canvas.yview_scroll(scroll_amount, "units")
                return "break"
            
            # スクロールフレームにマウスホイールイベントをバインド
            scroll_frame.bind("<MouseWheel>", on_scroll_mousewheel)
            # ダイアログ全体にもバインド（フォーカスが外れている場合でも動作するように）
            dialog.bind("<MouseWheel>", on_scroll_mousewheel)
            
            # 選択された検査員を保持（複数選択対応：辞書形式で名前とコードを保持）
            selected_inspectors = {}  # {name: code}
            
            # 現在の検査員を初期選択状態にする
            if current_inspector:
                current_name_clean = current_inspector.split('(')[0].strip()
                inspector_info = inspector_master_df[inspector_master_df['#氏名'] == current_name_clean]
                if not inspector_info.empty:
                    inspector_code = inspector_info.iloc[0]['#ID']
                    selected_inspectors[current_name_clean] = inspector_code
            
            # 検査員リストを作成
            inspector_names = inspector_master_df['#氏名'].dropna().astype(str).str.strip()
            inspector_names = inspector_names[inspector_names != ''].unique().tolist()
            
            # 各検査員にチェックボックスを作成
            inspector_checkboxes = {}
            for inspector_name in sorted(inspector_names):
                # 検査員コードを取得
                inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                if inspector_info.empty:
                    continue
                
                inspector_code = inspector_info.iloc[0]['#ID']
                
                # チェックボックスを作成
                checkbox_var = tk.BooleanVar(value=inspector_name in selected_inspectors)
                checkbox = ctk.CTkCheckBox(
                    scroll_frame,
                    text=inspector_name,
                    variable=checkbox_var,
                    command=lambda name=inspector_name, code=inspector_code, var=checkbox_var: self._update_selected_inspectors_for_change(name, code, var, selected_inspectors),
                    font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold")
                )
                checkbox.pack(anchor="w", pady=2)
                inspector_checkboxes[inspector_name] = checkbox_var
            
            # ボタンフレーム
            button_frame = ctk.CTkFrame(dialog, fg_color="transparent")
            button_frame.pack(pady=10)
            
            def on_ok():
                if selected_inspectors:
                    # 複数の検査員を変更（col_nameは最初の列名として使用）
                    self.update_inspector_assignment_multiple(
                        original_index, col_name, col_index,
                        selected_inspectors,
                        current_inspector, row, inspector_df
                    )
                else:
                    # 選択が空の場合は未割当にする
                    self.update_inspector_assignment(
                        original_index, col_name, col_index,
                        None, None,
                        current_inspector, row, inspector_df
                    )
                dialog.destroy()
            
            def on_cancel():
                dialog.destroy()
            
            ok_button = ctk.CTkButton(
                button_frame,
                text="OK",
                command=on_ok,
                width=100,
                height=30
            )
            ok_button.pack(side="left", padx=5)
            
            cancel_button = ctk.CTkButton(
                button_frame,
                text="キャンセル",
                command=on_cancel,
                width=100,
                height=30,
                fg_color="#6B7280",
                hover_color="#4B5563"
            )
            cancel_button.pack(side="left", padx=5)
            
            # ダイアログを中央に配置
            dialog.update_idletasks()
            x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
            y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
            dialog.geometry(f"+{x}+{y}")
            
        except Exception as e:
            self.log_message(f"検査員選択ダイアログの表示に失敗しました: {str(e)}")
            logger.error(f"検査員選択ダイアログの表示に失敗しました: {str(e)}", exc_info=True)
    
    def update_inspector_assignment(self, original_index, col_name, col_index, new_inspector_name, new_inspector_code, old_inspector_name, row, inspector_df):
        """検査員割当てを更新"""
        try:
            from datetime import date as date_type
            
            if inspector_df is None:
                self.log_message("エラー: 検査員割当てデータが見つかりません")
                return
            
            # データフレームの行を取得
            df = inspector_df.copy()
            divided_time = row.get('分割検査時間', 0.0)
            product_number = row.get('品番', '')
            current_date = pd.Timestamp.now().date()
            
            # 検査員マスタを読み込む（キャッシュを活用）
            inspector_master_df = self.load_inspector_master_cached()
            if inspector_master_df is None:
                self.log_message("エラー: 検査員マスタを読み込めません")
                return
            
            # 旧検査員のコードを取得
            old_inspector_code = None
            if old_inspector_name:
                old_name_clean = old_inspector_name.split('(')[0].strip()
                old_info = inspector_master_df[inspector_master_df['#氏名'] == old_name_clean]
                if not old_info.empty:
                    old_inspector_code = old_info.iloc[0]['#ID']
            
            # 新検査員が空の場合は削除処理のみ実行
            if not new_inspector_name or not new_inspector_code:
                # 検査員を削除（未割当にする）処理
                # 旧検査員から時間を引く
                if old_inspector_code:
                    if old_inspector_code in self.inspector_manager.inspector_daily_assignments:
                        if current_date in self.inspector_manager.inspector_daily_assignments[old_inspector_code]:
                            self.inspector_manager.inspector_daily_assignments[old_inspector_code][current_date] = max(
                                0.0,
                                self.inspector_manager.inspector_daily_assignments[old_inspector_code][current_date] - divided_time
                            )
                    
                    if old_inspector_code in self.inspector_manager.inspector_work_hours:
                        self.inspector_manager.inspector_work_hours[old_inspector_code] = max(
                            0.0,
                            self.inspector_manager.inspector_work_hours[old_inspector_code] - divided_time
                        )
                    
                    # 品番別累計時間も更新
                    if old_inspector_code in self.inspector_manager.inspector_product_hours:
                        if product_number in self.inspector_manager.inspector_product_hours[old_inspector_code]:
                            self.inspector_manager.inspector_product_hours[old_inspector_code][product_number] = max(
                                0.0,
                                self.inspector_manager.inspector_product_hours[old_inspector_code][product_number] - divided_time
                            )
                
                # データフレームを更新（空文字列を設定）
                df.at[original_index, col_name] = ''
                
                # 検査員人数と分割検査時間を再計算
                self._recalculate_inspector_count_and_divided_time(df, original_index)
                
                # 当日洗浄上がり品の制約を更新
                shipping_date_str = str(row.get('出荷予定日', '')).strip()
                is_same_day_cleaning = (
                    shipping_date_str == "当日洗浄上がり品" or
                    shipping_date_str == "当日洗浄品" or
                    "当日洗浄" in shipping_date_str or
                    shipping_date_str == "先行検査" or
                    shipping_date_str == "当日先行検査"
                )
                
                if is_same_day_cleaning and old_inspector_code:
                    # 旧検査員を削除
                    if product_number in self.inspector_manager.same_day_cleaning_inspectors:
                        self.inspector_manager.same_day_cleaning_inspectors[product_number].discard(old_inspector_code)
                
                # データフレームを更新
                self.current_inspector_data = df
                
                # テーブルを再描画（スクロール位置と選択行を保持）
                self.display_inspector_assignment_table(df, preserve_scroll_position=True, target_row_index=original_index)
                
                self.log_message(
                    f"検査員を削除しました: {old_inspector_name} → 未割当 "
                    f"(品番: {product_number}, {col_name})"
                )
                return
            
            # 新検査員の情報を取得
            new_info = inspector_master_df[inspector_master_df['#ID'] == new_inspector_code]
            if new_info.empty:
                self.log_message(f"エラー: 検査員コード {new_inspector_code} が見つかりません")
                return
            
            # 制約チェック（簡易版）
            # 1. 勤務時間チェック
            max_hours = self.inspector_manager.get_inspector_max_hours(new_inspector_code, inspector_master_df)
            daily_hours = self.inspector_manager.inspector_daily_assignments.get(new_inspector_code, {}).get(current_date, 0.0)
            
            if daily_hours + divided_time > max_hours:
                self.log_message(
                    f"警告: 検査員 '{new_inspector_name}' の勤務時間が超過します "
                    f"({daily_hours:.1f}h + {divided_time:.1f}h > {max_hours:.1f}h)。"
                    f"変更を続行します。",
                    level='warning'
                )
            
            # 2. 同一品番4時間上限チェック
            product_hours = self.inspector_manager.inspector_product_hours.get(new_inspector_code, {}).get(product_number, 0.0)
            if product_hours + divided_time > 4.0:
                self.log_message(
                    f"警告: 検査員 '{new_inspector_name}' の同一品番累計時間が4時間を超過します "
                    f"({product_hours:.1f}h + {divided_time:.1f}h = {product_hours + divided_time:.1f}h)。"
                    f"変更を続行します。",
                    level='warning'
                )
            
            # 旧検査員から時間を引く
            if old_inspector_code:
                if old_inspector_code in self.inspector_manager.inspector_daily_assignments:
                    if current_date in self.inspector_manager.inspector_daily_assignments[old_inspector_code]:
                        self.inspector_manager.inspector_daily_assignments[old_inspector_code][current_date] = max(
                            0.0,
                            self.inspector_manager.inspector_daily_assignments[old_inspector_code][current_date] - divided_time
                        )
                
                if old_inspector_code in self.inspector_manager.inspector_work_hours:
                    self.inspector_manager.inspector_work_hours[old_inspector_code] = max(
                        0.0,
                        self.inspector_manager.inspector_work_hours[old_inspector_code] - divided_time
                    )
                
                # 品番別累計時間も更新
                if old_inspector_code in self.inspector_manager.inspector_product_hours:
                    if product_number in self.inspector_manager.inspector_product_hours[old_inspector_code]:
                        self.inspector_manager.inspector_product_hours[old_inspector_code][product_number] = max(
                            0.0,
                            self.inspector_manager.inspector_product_hours[old_inspector_code][product_number] - divided_time
                        )
            
            # 新検査員に時間を追加
            if new_inspector_code not in self.inspector_manager.inspector_daily_assignments:
                self.inspector_manager.inspector_daily_assignments[new_inspector_code] = {}
            if current_date not in self.inspector_manager.inspector_daily_assignments[new_inspector_code]:
                self.inspector_manager.inspector_daily_assignments[new_inspector_code][current_date] = 0.0
            
            self.inspector_manager.inspector_daily_assignments[new_inspector_code][current_date] += divided_time
            
            if new_inspector_code not in self.inspector_manager.inspector_work_hours:
                self.inspector_manager.inspector_work_hours[new_inspector_code] = 0.0
            self.inspector_manager.inspector_work_hours[new_inspector_code] += divided_time
            
            # 品番別累計時間も更新
            if new_inspector_code not in self.inspector_manager.inspector_product_hours:
                self.inspector_manager.inspector_product_hours[new_inspector_code] = {}
            self.inspector_manager.inspector_product_hours[new_inspector_code][product_number] = (
                self.inspector_manager.inspector_product_hours[new_inspector_code].get(product_number, 0.0) + divided_time
            )
            
            # データフレームを更新（氏名のみ）
            new_inspector_display = new_inspector_name
            
            df.at[original_index, col_name] = new_inspector_display
            
            # 検査員人数と分割検査時間を再計算
            self._recalculate_inspector_count_and_divided_time(df, original_index)
            
            # 当日洗浄上がり品の制約を更新
            shipping_date_str = str(row.get('出荷予定日', '')).strip()
            is_same_day_cleaning = (
                shipping_date_str == "当日洗浄上がり品" or
                shipping_date_str == "当日洗浄品" or
                "当日洗浄" in shipping_date_str or
                shipping_date_str == "先行検査" or
                shipping_date_str == "当日先行検査"
            )
            
            if is_same_day_cleaning:
                # 旧検査員を削除
                if old_inspector_code and product_number in self.inspector_manager.same_day_cleaning_inspectors:
                    self.inspector_manager.same_day_cleaning_inspectors[product_number].discard(old_inspector_code)
                
                # 新検査員を追加
                self.inspector_manager.same_day_cleaning_inspectors.setdefault(product_number, set()).add(new_inspector_code)
            
            # データフレームを更新
            self.current_inspector_data = df
            
            # テーブルを再描画（スクロール位置と選択行を保持）
            self.display_inspector_assignment_table(df, preserve_scroll_position=True, target_row_index=original_index)
            
            self.log_message(
                f"検査員を変更しました: {old_inspector_name if old_inspector_name else '未割当'} → {new_inspector_name} "
                f"(品番: {product_number}, {col_name})"
            )
            
        except Exception as e:
            self.log_message(f"検査員割当ての更新に失敗しました: {str(e)}")
            logger.error(f"検査員割当ての更新に失敗しました: {str(e)}", exc_info=True)
    
    def update_inspector_assignment_multiple(self, original_index, col_name, col_index, selected_inspectors_dict, old_inspector_name, row, inspector_df):
        """複数の検査員を割り当てる（検査員変更ダイアログ用）"""
        try:
            from datetime import date as date_type
            
            if inspector_df is None:
                self.log_message("エラー: 検査員割当てデータが見つかりません")
                return
            
            # データフレームの行を取得
            df = inspector_df.copy()
            inspection_time = row.get('検査時間', 0.0)
            product_number = row.get('品番', '')
            current_date = pd.Timestamp.now().date()
            
            # 検査員マスタを読み込む（キャッシュを活用）
            inspector_master_df = self.load_inspector_master_cached()
            if inspector_master_df is None:
                self.log_message("エラー: 検査員マスタを読み込めません")
                return
            
            # 選択された検査員のリストを取得（最大5人まで）
            selected_names = list(selected_inspectors_dict.keys())[:5]
            selected_codes = [selected_inspectors_dict[name] for name in selected_names]
            
            if not selected_names:
                # すべての検査員を削除
                self.update_inspector_assignment(
                    original_index, col_name, col_index,
                    None, None,
                    old_inspector_name, row, inspector_df
                )
                return
            
            # 旧検査員のコードを取得
            old_inspector_codes = []
            if old_inspector_name:
                old_name_clean = old_inspector_name.split('(')[0].strip()
                old_info = inspector_master_df[inspector_master_df['#氏名'] == old_name_clean]
                if not old_info.empty:
                    old_inspector_codes.append(old_info.iloc[0]['#ID'])
            
            # 現在の検査員列（検査員1～10）から旧検査員のコードを取得
            for i in range(1, MAX_INSPECTORS_PER_LOT + 1):
                inspector_col = f'検査員{i}'
                if inspector_col in df.columns:
                    inspector_value = row.get(inspector_col, '')
                    if pd.notna(inspector_value) and str(inspector_value).strip() != '':
                        inspector_name_clean = str(inspector_value).split('(')[0].strip()
                        inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name_clean]
                        if not inspector_info.empty:
                            inspector_code = inspector_info.iloc[0]['#ID']
                            if inspector_code not in old_inspector_codes:
                                old_inspector_codes.append(inspector_code)
            
            # 分割検査時間を計算
            divided_time = inspection_time / len(selected_names) if len(selected_names) > 0 else 0.0
            
            # 旧検査員から時間を引く
            for old_code in old_inspector_codes:
                if old_code in self.inspector_manager.inspector_daily_assignments:
                    if current_date in self.inspector_manager.inspector_daily_assignments[old_code]:
                        self.inspector_manager.inspector_daily_assignments[old_code][current_date] = max(
                            0.0,
                            self.inspector_manager.inspector_daily_assignments[old_code][current_date] - divided_time
                        )
                
                if old_code in self.inspector_manager.inspector_work_hours:
                    self.inspector_manager.inspector_work_hours[old_code] = max(
                        0.0,
                        self.inspector_manager.inspector_work_hours[old_code] - divided_time
                    )
                
                # 品番別累計時間も更新
                if old_code in self.inspector_manager.inspector_product_hours:
                    if product_number in self.inspector_manager.inspector_product_hours[old_code]:
                        self.inspector_manager.inspector_product_hours[old_code][product_number] = max(
                            0.0,
                            self.inspector_manager.inspector_product_hours[old_code][product_number] - divided_time
                        )
            
            # 新検査員に時間を追加
            for new_code in selected_codes:
                if new_code not in self.inspector_manager.inspector_daily_assignments:
                    self.inspector_manager.inspector_daily_assignments[new_code] = {}
                if current_date not in self.inspector_manager.inspector_daily_assignments[new_code]:
                    self.inspector_manager.inspector_daily_assignments[new_code][current_date] = 0.0
                
                self.inspector_manager.inspector_daily_assignments[new_code][current_date] += divided_time
                
                if new_code not in self.inspector_manager.inspector_work_hours:
                    self.inspector_manager.inspector_work_hours[new_code] = 0.0
                self.inspector_manager.inspector_work_hours[new_code] += divided_time
                
                # 品番別累計時間も更新
                if new_code not in self.inspector_manager.inspector_product_hours:
                    self.inspector_manager.inspector_product_hours[new_code] = {}
                self.inspector_manager.inspector_product_hours[new_code][product_number] = (
                    self.inspector_manager.inspector_product_hours[new_code].get(product_number, 0.0) + divided_time
                )
            
            # データフレームを更新（検査員1～10に設定）
            for i in range(1, MAX_INSPECTORS_PER_LOT + 1):
                inspector_col = f'検査員{i}'
                if inspector_col in df.columns:
                    if i <= len(selected_names):
                        df.at[original_index, inspector_col] = selected_names[i - 1]
                    else:
                        df.at[original_index, inspector_col] = ''
            
            # 検査員人数と分割検査時間を再計算
            self._recalculate_inspector_count_and_divided_time(df, original_index)
            
            # 当日洗浄上がり品の制約を更新
            shipping_date_str = str(row.get('出荷予定日', '')).strip()
            is_same_day_cleaning = (
                shipping_date_str == "当日洗浄上がり品" or
                shipping_date_str == "当日洗浄品" or
                "当日洗浄" in shipping_date_str or
                shipping_date_str == "先行検査" or
                shipping_date_str == "当日先行検査"
            )
            
            if is_same_day_cleaning:
                # 旧検査員を削除
                for old_code in old_inspector_codes:
                    if product_number in self.inspector_manager.same_day_cleaning_inspectors:
                        self.inspector_manager.same_day_cleaning_inspectors[product_number].discard(old_code)
                
                # 新検査員を追加
                for new_code in selected_codes:
                    self.inspector_manager.same_day_cleaning_inspectors.setdefault(product_number, set()).add(new_code)
            
            # データフレームを更新
            self.current_inspector_data = df
            
            # テーブルを再描画（スクロール位置と選択行を保持）
            self.display_inspector_assignment_table(df, preserve_scroll_position=True, target_row_index=original_index)
            
            selected_names_str = ', '.join(selected_names)
            self.log_message(
                f"検査員を変更しました: {old_inspector_name if old_inspector_name else '未割当'} → {selected_names_str} "
                f"(品番: {product_number}, {col_name})"
            )
            
        except Exception as e:
            self.log_message(f"検査員割当ての更新に失敗しました: {str(e)}")
            logger.error(f"検査員割当ての更新に失敗しました: {str(e)}", exc_info=True)
    
    def _recalculate_inspector_count_and_divided_time(self, df, row_index):
        """検査員人数と分割検査時間を再計算"""
        try:
            row = df.loc[row_index]
            
            # 検査員1～10の列を確認して、実際に割り当てられている検査員数をカウント
            inspector_count = 0
            for i in range(1, MAX_INSPECTORS_PER_LOT + 1):
                inspector_col = f'検査員{i}'
                if inspector_col in df.columns:
                    inspector_value = row.get(inspector_col, '')
                    if pd.notna(inspector_value) and str(inspector_value).strip() != '':
                        inspector_count += 1
            
            # 検査員人数を更新
            if '検査員人数' in df.columns:
                df.at[row_index, '検査員人数'] = inspector_count
            
            # 分割検査時間を再計算
            if '分割検査時間' in df.columns and '検査時間' in df.columns:
                inspection_time = row.get('検査時間', 0.0)
                if pd.notna(inspection_time) and inspector_count > 0:
                    # 検査時間を検査員人数で割る
                    divided_time = inspection_time / inspector_count
                    df.at[row_index, '分割検査時間'] = round(divided_time, 1)
                else:
                    df.at[row_index, '分割検査時間'] = 0.0
            
        except Exception as e:
            logger.debug(f"検査員人数と分割検査時間の再計算に失敗: {str(e)}")
    
    def remove_inspector_from_table(self, row_index_in_tree, col_name, col_index, inspector_df):
        """検査員を削除（未割当にする）"""
        try:
            if inspector_df is None or inspector_df.empty:
                return
            
            if row_index_in_tree >= len(inspector_df):
                self.log_message(f"エラー: 行インデックスが範囲外です: {row_index_in_tree}")
                return
            
            original_index = inspector_df.index[row_index_in_tree]
            row = inspector_df.iloc[original_index]
            
            # 現在の検査員名を取得
            current_inspector = row.get(col_name, '')
            if not current_inspector or pd.isna(current_inspector):
                self.log_message("既に未割当です")
                return
            
            # 確認ダイアログ
            result = messagebox.askyesno(
                "確認",
                f"検査員列「{col_name}」の検査員「{current_inspector}」を削除（未割当にする）してもよろしいですか？"
            )
            
            if not result:
                return
            
            # 検査員を削除（未割当にする）
            self.update_inspector_assignment(
                original_index, col_name, col_index,
                '', '',  # 新検査員なし
                current_inspector, row, inspector_df
            )
            
        except Exception as e:
            self.log_message(f"検査員の削除に失敗しました: {str(e)}")
            logger.error(f"検査員の削除に失敗しました: {str(e)}", exc_info=True)
    
    def export_to_google_sheets(self):
        """Googleスプレッドシートに手動で出力"""
        try:
            # 検査員割振りデータが存在するか確認
            if self.current_inspector_data is None or self.current_inspector_data.empty:
                messagebox.showwarning(
                    "警告",
                    "出力する検査員割振りデータがありません。\n先にデータ抽出と検査員割振りを実行してください。"
                )
                return
            
            # Googleスプレッドシートエクスポーターが初期化されているか確認
            if not self.google_sheets_exporter:
                messagebox.showerror(
                    "エラー",
                    "Googleスプレッドシートエクスポーターが初期化されていません。\n"
                    "config.envにGOOGLE_SHEETS_URLとGOOGLE_SHEETS_CREDENTIALS_PATHが設定されているか確認してください。"
                )
                return
            
            # 確認ダイアログを表示
            response = messagebox.askyesno(
                "確認",
                "Googleスプレッドシートに出力しますか？\n\n"
                f"出力件数: {len(self.current_inspector_data)}件\n\n"
                "※既存のデータは上書きされます。"
            )
            
            if not response:
                return
            
            # スキル値付きのデータを使用（検査員名のみを抽出して出力）
            inspector_df = self.original_inspector_data if hasattr(self, 'original_inspector_data') and self.original_inspector_data is not None else self.current_inspector_data
            
            # 座席表のロット順番に基づいてDataFrameを並び替え
            if hasattr(self, 'seating_chart_lot_order') and self.seating_chart_lot_order:
                inspector_df = self._sort_dataframe_by_seating_order(inspector_df)
            
            self.log_message("Googleスプレッドシートへの出力を開始します")
            success = self.google_sheets_exporter.export_inspector_assignment_to_sheets(
                inspector_df,
                log_callback=self.log_message
            )
            
            if success:
                messagebox.showinfo(
                    "完了",
                    f"Googleスプレッドシートへの出力が完了しました。\n\n"
                    f"出力件数: {len(self.current_inspector_data)}件"
                )
                self.log_message("Googleスプレッドシートへの出力が完了しました")
                self._set_seating_flow_prompt("")
            else:
                messagebox.showerror(
                    "エラー",
                    "Googleスプレッドシートへの出力に失敗しました。\n"
                    "ログを確認してください。"
                )
                self.log_message("警告: Googleスプレッドシートへの出力に失敗しました")
                
        except Exception as e:
            error_msg = f"Googleスプレッドシートへの出力中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            messagebox.showerror("エラー", error_msg)
    
    def cleanup_resources(self):
        """リソースのクリーンアップ"""
        if getattr(self, "_cleanup_done", False):
            return
        if getattr(self, "_cleanup_in_progress", False):
            return

        self._cleanup_in_progress = True
        try:
            logger.info("リソースをクリーンアップしています...")
            
            # データベース接続を閉じる（リソース解放）
            try:
                DatabaseConfig.close_all_connections()
            except Exception as e:
                logger.debug(f"データベース接続のクローズでエラー（無視）: {e}")
            
            # カレンダーウィンドウを閉じる
            if hasattr(self, 'calendar_window') and self.calendar_window is not None:
                try:
                    self.calendar_window.destroy()
                except (AttributeError, tk.TclError) as e:
                    logger.debug(f"カレンダーウィンドウの破棄でエラー（無視）: {e}")
                self.calendar_window = None
            
            # Seat chart server を停止（起動していれば）
            try:
                self._seat_chart_server.stop()
            except Exception as e:
                logger.debug(f"Seat chart server の停止でエラー（無視）: {e}")
            
            logger.info("リソースのクリーンアップが完了しました")
            self._cleanup_done = True
            
        except Exception as e:
            logger.error(f"リソースクリーンアップ中にエラーが発生しました: {e}")
        finally:
            self._cleanup_in_progress = False
    
    def quit_application(self):
        """アプリケーションを完全に終了する"""
        try:
            # ログ出力
            logger.info("アプリケーションを終了しています...")
            
            # 【高速化】ログバッファをフラッシュ（終了時）
            if hasattr(self.inspector_manager, 'log_batch_enabled') and self.inspector_manager.log_batch_enabled:
                try:
                    self.inspector_manager._flush_log_buffer()
                except Exception as e:
                    logger.debug(f"ログバッファのフラッシュでエラー（無視）: {e}")
            
            # リソースのクリーンアップ
            self.cleanup_resources()
            
            # メインウィンドウを破棄
            if hasattr(self, 'root') and self.root is not None:
                try:
                    # mainloopを終了
                    self.root.quit()
                    # ウィンドウを破棄
                    self.root.destroy()
                except:
                    pass
            
            logger.info("アプリケーションを正常に終了しました")
            
        except Exception as e:
            logger.error(f"アプリケーション終了中にエラーが発生しました: {e}")
            # エラーが発生した場合のみ強制終了
            try:
                if hasattr(self, 'root') and self.root is not None:
                    self.root.quit()
                    self.root.destroy()
            except:
                import os
                os._exit(0)
    
    
    def start_inspector_assignment(self):
        """検査員割振りを開始"""
        try:
            if self.current_assignment_data is None or self.current_assignment_data.empty:
                messagebox.showwarning("警告", "ロット割り当て結果がありません。\n先にデータを抽出してください。")
                return
            
            # 製品マスタファイルを読み込み（キャッシュを活用）
            product_master_df = self.load_product_master_cached()
            if product_master_df is None:
                return
            
            # 固定検査員情報を設定
            self._set_fixed_inspectors_to_manager()
            
            # 検査員割振りテーブルを作成（製品マスタパスを渡す）
            product_master_path = self.config.product_master_path if self.config else None
            process_master_path = self.config.process_master_path if self.config else None
            inspection_target_keywords = self.load_inspection_target_csv()
            inspector_df = self.inspector_manager.create_inspector_assignment_table(
                self.current_assignment_data, 
                product_master_df, 
                product_master_path=product_master_path,
                process_master_path=process_master_path,
                inspection_target_keywords=inspection_target_keywords
            )
            if inspector_df is None:
                return
            
            # データを保存（エクスポート用）
            self.current_inspector_data = inspector_df
            self._refresh_inspector_table_post_assignment()
            
        except Exception as e:
            error_msg = f"検査員割振りの開始に失敗しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            messagebox.showerror("エラー", error_msg)
    
    def show_table(self, table_type):
        """選択されたテーブルを表示"""
        try:
            # 現在表示中のテーブルを非表示にする
            self.hide_current_table()
            
            # 選択されたテーブルを表示
            if table_type == "main" and self.current_main_data is not None:
                self.display_data(self.current_main_data)
                self.current_display_table = "main"
                # テーブル選択ボタンが存在する場合のみ状態を更新
                if hasattr(self, 'main_data_button'):
                    self.update_button_states("main")
            elif table_type == "assignment" and self.current_assignment_data is not None:
                self.display_lot_assignment_table(self.current_assignment_data)
                self.current_display_table = "assignment"
                if hasattr(self, 'assignment_button'):
                    self.update_button_states("assignment")
            elif table_type == "inspector" and self.current_inspector_data is not None:
                self.display_inspector_assignment_table(self.current_inspector_data)
                self.current_display_table = "inspector"
                if hasattr(self, 'inspector_button'):
                    self.update_button_states("inspector")
            else:
                self.log_message(f"{table_type}テーブルのデータがありません")
                
        except Exception as e:
            error_msg = f"テーブル表示中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
    
    def hide_current_table(self):
        """現在表示中のテーブルを非表示にする（検査員割振りテーブルのみ）"""
        try:
            # 既存の検査員割振りテーブルセクションのみを削除（休暇情報テーブルは保持）
            for widget in self.main_scroll_frame.winfo_children():
                if hasattr(widget, 'table_section') and hasattr(widget, 'inspector_section'):
                    widget.destroy()
        except Exception as e:
            logger.error(f"テーブル非表示中にエラーが発生しました: {str(e)}")
    
    def update_button_states(self, active_table):
        """テーブル選択ボタンの状態を更新"""
        try:
            # ボタンが存在しない場合は処理をスキップ
            if not (hasattr(self, 'main_data_button') and hasattr(self, 'assignment_button') and hasattr(self, 'inspector_button')):
                return
                
            # すべてのボタンを非アクティブ状態に
            self.main_data_button.configure(fg_color="#6B7280", hover_color="#4B5563")
            self.assignment_button.configure(fg_color="#6B7280", hover_color="#4B5563")
            self.inspector_button.configure(fg_color="#6B7280", hover_color="#4B5563")
            
            # アクティブなボタンをハイライト
            if active_table == "main":
                self.main_data_button.configure(fg_color="#3B82F6", hover_color="#2563EB")
            elif active_table == "assignment":
                self.assignment_button.configure(fg_color="#3B82F6", hover_color="#2563EB")
            elif active_table == "inspector":
                self.inspector_button.configure(fg_color="#3B82F6", hover_color="#2563EB")
                
        except Exception as e:
            logger.error(f"ボタン状態更新中にエラーが発生しました: {str(e)}")
    
    def load_inspector_master(self):
        """検査員マスタファイルを読み込む"""
        try:
            file_path = self.config.inspector_master_path
            
            if not file_path:
                self.log_message("検査員マスタファイルのパスが設定されていません")
                return None
            
            if not os.path.exists(file_path):
                self.log_message(f"検査員マスタファイルが見つかりません: {file_path}")
                return None
            
            # CSVファイルを読み込み（ヘッダーなし、最適化）
            # dtype指定はエラー処理のオーバーヘッドがあるため、シンプルに読み込む
            df = pd.read_csv(
                file_path,
                encoding='utf-8-sig',
                header=None,
                low_memory=False  # メモリ使用量を増やして高速化
            )
            
            # 列名を確認
            self.log_message(f"検査員マスタの元の列数: {len(df.columns)}")
            
            # 1行目（#0,1,2,3,4,5,6,7,）をスキップし、2行目（#ID,#氏名,...）をヘッダーとして使用
            if len(df) > 1:
                # 2行目をヘッダーとして使用
                new_header = df.iloc[1]
                df = df[2:]  # 2行目以降のデータのみ残す
                df.columns = new_header
                df = df.reset_index(drop=True)
                self.log_message(f"ヘッダーを修正しました: {df.columns.tolist()}")
            
            # 必要な列が存在するかチェック
            required_columns = ['#氏名', '開始時刻', '終了時刻']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                # 列名のマッピングを試行
                column_mapping = {}
                for col in df.columns:
                    col_str = str(col)
                    if '氏名' in col_str or '名前' in col_str:
                        column_mapping[col] = '#氏名'
                    elif ('開始' in col_str and '時刻' in col_str) or '開始時間' in col_str:
                        column_mapping[col] = '開始時刻'
                    elif ('終了' in col_str and '時刻' in col_str) or '終了時間' in col_str:
                        column_mapping[col] = '終了時刻'
                
                if len(column_mapping) >= 3:
                    df = df.rename(columns=column_mapping)
                    self.log_message(f"列名をマッピングしました: {column_mapping}")
                else:
                    # デフォルトの列名を試行（B列=氏名、D列=開始時刻、E列=終了時刻）
                    if len(df.columns) >= 5:
                        df = df.rename(columns={
                            df.columns[1]: '#氏名',  # B列
                            df.columns[3]: '開始時刻',  # D列
                            df.columns[4]: '終了時刻'   # E列
                        })
                        self.log_message("デフォルト列名（B列=氏名、D列=開始時刻、E列=終了時刻）を使用しました")
                    else:
                        self.log_message(f"必要な列が見つかりません: {missing_columns}")
                        self.log_message(f"利用可能な列: {df.columns.tolist()}")
                        return None
            
            # 就業時間を計算（終了時刻 - 開始時刻 - 1時間休憩）
            try:
                # 時刻フォーマットを試行（formatを指定して警告を回避・高速化）
                # 文字列の時刻をdatetime型に変換（同じ日付として扱う）
                base_date = pd.Timestamp('1900-01-01')
                start_datetime = pd.to_datetime(base_date.strftime('%Y-%m-%d') + ' ' + df['開始時刻'].astype(str), format='%Y-%m-%d %H:%M', errors='coerce')
                end_datetime = pd.to_datetime(base_date.strftime('%Y-%m-%d') + ' ' + df['終了時刻'].astype(str), format='%Y-%m-%d %H:%M', errors='coerce')
                
                # 就業時間を計算（formatを指定して警告を回避・高速化）
                df['就業時間'] = (end_datetime - start_datetime).dt.total_seconds() / 3600 - 1  # 休憩1時間を引く
                
            except Exception as e:
                self.log_message(f"時刻フォーマット処理でエラー: {str(e)}")
                # フォールバック: 文字列として処理
                try:
                    df['就業時間'] = 8.0  # デフォルト8時間
                    self.log_message("デフォルト就業時間（8時間）を使用しました")
                except:
                    df['就業時間'] = 8.0
            
            self.log_message(f"検査員マスタを読み込みました: {len(df)}件")
            return df
            
        except Exception as e:
            error_msg = f"検査員マスタの読み込みに失敗しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            return None
    
    def load_skill_master(self):
        """スキルマスタファイルを読み込む"""
        try:
            file_path = self.config.skill_master_path
            
            if not file_path:
                self.log_message("スキルマスタファイルのパスが設定されていません")
                return None
            
            if not os.path.exists(file_path):
                self.log_message(f"スキルマスタファイルが見つかりません: {file_path}")
                return None
            
            # CSVファイルを読み込み（ヘッダーなし、最適化）
            df = pd.read_csv(
                file_path,
                encoding='utf-8-sig',
                header=None,
                low_memory=False  # メモリ使用量を増やして高速化
            )
            
            # 列名を確認
            self.log_message(f"スキルマスタの元の列数: {len(df.columns)}")
            
            # 1行目（品番, 工程, V002, V004, ...）を列名として使用
            if len(df) > 1:
                # 1行目を列名として設定
                df.columns = df.iloc[0]
                # 1行目と2行目（検査員名の行）を削除
                df = df[2:]  # 2行目以降のデータのみ残す
                df = df.reset_index(drop=True)
                self.log_message(f"スキルマスタの列名: {df.columns.tolist()[:10]}...")  # 最初の10列のみ表示
            
            self.log_message(f"スキルマスタを読み込みました: {len(df)}件")
            return df
            
        except Exception as e:
            error_msg = f"スキルマスタの読み込みに失敗しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            return None
    
    def load_inspection_target_csv(self):
        """検査対象.csvファイルを読み込み、A列の文字列リストを取得"""
        try:
            if not self.config:
                self.log_message("設定が読み込まれていません")
                return []
            
            file_path = self.config.inspection_target_csv_path
            
            # パスが設定されていない場合は空リストを返す（フィルタリングなし）
            if not file_path:
                self.log_message("検査対象CSVファイルのパスが設定されていません。全てのロットを対象とします。")
                return []
            
            if not os.path.exists(file_path):
                self.log_message(f"検査対象CSVファイルが見つかりません: {file_path}。全てのロットを対象とします。")
                return []
            
            # CSVファイルを読み込み（A列のみ、最適化）
            df = pd.read_csv(
                file_path,
                encoding='utf-8-sig',
                header=None,
                usecols=[0],  # A列のみ読み込む
                low_memory=False
            )
            
            # A列の値を取得（空のセルやNaNを除外）
            keywords = df.iloc[:, 0].dropna().astype(str).str.strip()
            keywords = keywords[keywords != ''].tolist()
            
            self.log_message(f"検査対象CSVを読み込みました: {len(keywords)}件のキーワード")
            
            return keywords
            
        except Exception as e:
            error_msg = f"検査対象CSVの読み込みに失敗しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            # エラー時も空リストを返して処理を継続
            return []
    
    
    
    
    
    def create_menu_bar(self):
        """メニューバーを作成"""
        try:
            # CustomTkinterでも標準のtkinterメニューバーを使用可能
            menubar = tk.Menu(self.root)
            self.root.config(menu=menubar)
            
            # マスタファイルメニュー
            master_menu = tk.Menu(menubar, tearoff=0)
            menubar.add_cascade(label="🗂️ マスタファイル", menu=master_menu)
            
            # 各マスタファイルを開くメニュー項目
            master_menu.add_command(
                label="製品マスタを開く",
                command=self.open_product_master_file
            )
            master_menu.add_command(
                label="検査員マスタを開く",
                command=self.open_inspector_master_file
            )
            master_menu.add_command(
                label="スキルマスタを開く",
                command=self.open_skill_master_file
            )
            master_menu.add_command(
                label="工程マスタを開く",
                command=self.open_process_master_file
            )
            master_menu.add_separator()
            master_menu.add_command(
                label="検査対象CSVを開く",
                command=self.open_inspection_target_csv_file
            )
            master_menu.add_command(
                label="抽出対象外（品番）マスタを編集",
                command=self.show_excluded_products_dialog
            )

            # 設定メニュー
            menubar.add_command(label="⚙️ 設定", command=self.show_settings_dialog)

            # ガイドメニュー
            menubar.add_command(label="📘 ガイド", command=self.open_assignment_rules_guide)

            # ヘルプメニュー
            help_menu = tk.Menu(menubar, tearoff=0)
            menubar.add_cascade(label="ヘルプ", menu=help_menu)
            help_menu.add_command(label="バージョン情報", command=self.show_about_dialog)
             
        except Exception as e:
            logger.error(f"メニューバーの作成に失敗しました: {e}")

    def show_about_dialog(self) -> None:
        """ヘルプ → バージョン情報"""
        try:
            messagebox.showinfo(
                "バージョン情報",
                f"{APP_NAME}\n\nバージョン: {APP_VERSION}\nビルド日: {BUILD_DATE}",
            )
        except Exception:
            pass
     
    def open_product_master_file(self):
        """製品マスタファイルを開く"""
        try:
            if self.config and self.config.product_master_path:
                file_path = self.config.product_master_path
                if os.path.exists(file_path):
                    os.startfile(file_path)
                    self.log_message(f"製品マスタファイルを開きました: {file_path}")
                else:
                    messagebox.showerror("エラー", f"製品マスタファイルが見つかりません:\n{file_path}")
            else:
                messagebox.showinfo("情報", "製品マスタファイルのパスが設定されていません。")
        except Exception as e:
            error_msg = f"製品マスタファイルを開く際にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            messagebox.showerror("エラー", error_msg)
    
    def open_inspector_master_file(self):
        """検査員マスタファイルを開く"""
        try:
            if self.config and self.config.inspector_master_path:
                file_path = self.config.inspector_master_path
                if os.path.exists(file_path):
                    os.startfile(file_path)
                    self.log_message(f"検査員マスタファイルを開きました: {file_path}")
                else:
                    messagebox.showerror("エラー", f"検査員マスタファイルが見つかりません:\n{file_path}")
            else:
                messagebox.showinfo("情報", "検査員マスタファイルのパスが設定されていません。")
        except Exception as e:
            error_msg = f"検査員マスタファイルを開く際にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            messagebox.showerror("エラー", error_msg)

    def open_assignment_rules_guide(self):
        """ガイド（HTML）を開く"""
        try:
            # exe化対応のパス解決を使用
            guide_path_str = resolve_resource_path("inspector_assignment_rules_help.html")
            guide_path = Path(guide_path_str)
            if not guide_path.exists():
                messagebox.showerror("エラー", f"ガイドファイルが見つかりません:\n{guide_path}")
                return

            try:
                # UNCパスや特殊なパスを扱うため、Windowsでは os.startfile を使う
                if os.name == "nt":
                    os.startfile(guide_path_str)
                else:
                    webbrowser.open(guide_path.as_uri())
            except OSError:
                # os.startfile が使えない場合は URI で開く（クロスプラットフォーム対応）
                webbrowser.open(guide_path.as_uri())

            self.log_message(f"ガイドを開きました: {guide_path}")
        except Exception as e:
            error_msg = f"ガイドを開く際にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            messagebox.showerror("エラー", error_msg)
    
    def open_skill_master_file(self):
        """スキルマスタファイルを開く"""
        try:
            if self.config and self.config.skill_master_path:
                file_path = self.config.skill_master_path
                if os.path.exists(file_path):
                    os.startfile(file_path)
                    self.log_message(f"スキルマスタファイルを開きました: {file_path}")
                else:
                    messagebox.showerror("エラー", f"スキルマスタファイルが見つかりません:\n{file_path}")
            else:
                messagebox.showinfo("情報", "スキルマスタファイルのパスが設定されていません。")
        except Exception as e:
            error_msg = f"スキルマスタファイルを開く際にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            messagebox.showerror("エラー", error_msg)
    
    def open_process_master_file(self):
        """工程マスタファイルを開く"""
        try:
            if self.config and self.config.process_master_path:
                file_path = self.config.process_master_path
                if os.path.exists(file_path):
                    os.startfile(file_path)
                    self.log_message(f"工程マスタファイルを開きました: {file_path}")
                else:
                    messagebox.showerror("エラー", f"工程マスタファイルが見つかりません:\n{file_path}")
            else:
                messagebox.showinfo("情報", "工程マスタファイルのパスが設定されていません。")
        except Exception as e:
            error_msg = f"工程マスタファイルを開く際にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            messagebox.showerror("エラー", error_msg)
    
    def open_inspection_target_csv_file(self):
        """検査対象CSVファイルを開く"""
        try:
            if self.config and self.config.inspection_target_csv_path:
                file_path = self.config.inspection_target_csv_path
                if os.path.exists(file_path):
                    os.startfile(file_path)
                    self.log_message(f"検査対象CSVファイルを開きました: {file_path}")
                else:
                    messagebox.showerror("エラー", f"検査対象CSVファイルが見つかりません:\n{file_path}")
            else:
                messagebox.showinfo("情報", "検査対象CSVファイルのパスが設定されていません。")
        except Exception as e:
            error_msg = f"検査対象CSVファイルを開く際にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            messagebox.showerror("エラー", error_msg)

    def show_excluded_products_dialog(self):
        """抽出対象外（品番）マスタ編集ダイアログ"""
        dialog = ctk.CTkToplevel(self.root)
        dialog.title("抽出対象外（品番）マスタ")
        dialog.geometry("650x520")
        dialog.transient(self.root)
        dialog.grab_set()

        container = ctk.CTkFrame(dialog, fg_color="white")
        container.pack(fill="both", expand=True, padx=16, pady=16)

        title = ctk.CTkLabel(
            container,
            text="抽出対象外（品番）",
            font=ctk.CTkFont(family="Yu Gothic", size=18, weight="bold"),
            text_color="#111827",
        )
        title.pack(anchor="w", pady=(0, 8))

        desc = ctk.CTkLabel(
            container,
            text="ここで登録した品番は、Accessからの抽出結果（出荷予定集計・不足・ロット取得）から除外されます。",
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            text_color="#6B7280",
            wraplength=610,
            justify="left",
        )
        desc.pack(anchor="w", pady=(0, 12))

        input_frame = ctk.CTkFrame(container, fg_color="#F9FAFB", corner_radius=10)
        input_frame.pack(fill="x", pady=(0, 12))

        product_entry = ctk.CTkEntry(
            input_frame,
            placeholder_text="品番を入力（例: 3D025-G4960）",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=36,
        )
        product_entry.pack(side="left", fill="x", expand=True, padx=(12, 8), pady=12)

        memo_entry = ctk.CTkEntry(
            input_frame,
            placeholder_text="メモ（任意）",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=36,
            width=220,
        )
        memo_entry.pack(side="left", padx=(0, 8), pady=12)

        def refresh_list():
            for w in list_frame.winfo_children():
                w.destroy()

            items = sorted(self.excluded_products, key=lambda x: (x.get("品番", ""), x.get("メモ", "")))
            self.excluded_products = items

            if not items:
                empty = ctk.CTkLabel(
                    list_frame,
                    text="（未登録）",
                    font=ctk.CTkFont(family="Yu Gothic", size=13),
                    text_color="#6B7280",
                )
                empty.pack(anchor="w", padx=8, pady=8)
                count_label.configure(text="登録数: 0")
                return

            count_label.configure(text=f"登録数: {len(items)}")
            for item in items:
                pn = item.get("品番", "")
                memo = item.get("メモ", "")
                row = ctk.CTkFrame(list_frame, fg_color="white", corner_radius=8, border_width=1, border_color="#E5E7EB")
                row.pack(fill="x", padx=6, pady=4)

                text = pn if not memo else f"{pn}  ({memo})"
                label = ctk.CTkLabel(
                    row,
                    text=text,
                    font=ctk.CTkFont(family="Yu Gothic", size=13),
                    text_color="#111827",
                    anchor="w",
                )
                label.pack(side="left", fill="x", expand=True, padx=10, pady=8)

                def _remove(p=pn):
                    self.excluded_products = [x for x in self.excluded_products if x.get("品番") != p]
                    self.save_excluded_products()
                    refresh_list()

                btn = ctk.CTkButton(
                    row,
                    text="削除",
                    width=70,
                    height=28,
                    fg_color="#EF4444",
                    hover_color="#DC2626",
                    font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                    command=_remove,
                )
                btn.pack(side="right", padx=10, pady=6)

        def add_item():
            pn = self._normalize_product_number(product_entry.get())
            memo = self._normalize_product_number(memo_entry.get())
            if not pn:
                messagebox.showwarning("入力", "品番を入力してください。")
                return
            current = self.get_excluded_product_numbers_set()
            if pn in current:
                messagebox.showinfo("情報", f"既に登録されています: {pn}")
                return
            self.excluded_products.append({"品番": pn, "メモ": memo})
            self.save_excluded_products()
            product_entry.delete(0, tk.END)
            memo_entry.delete(0, tk.END)
            refresh_list()

        add_button = ctk.CTkButton(
            input_frame,
            text="追加",
            width=80,
            height=36,
            fg_color="#3B82F6",
            hover_color="#2563EB",
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            command=add_item,
        )
        add_button.pack(side="left", padx=(0, 12), pady=12)

        product_entry.bind("<Return>", lambda _e: add_item())

        header = ctk.CTkFrame(container, fg_color="transparent")
        header.pack(fill="x", pady=(0, 6))
        count_label = ctk.CTkLabel(
            header,
            text="登録数: 0",
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            text_color="#6B7280",
        )
        count_label.pack(side="left")

        list_frame = ctk.CTkScrollableFrame(container, fg_color="#F9FAFB", corner_radius=10)
        list_frame.pack(fill="both", expand=True)

        footer = ctk.CTkFrame(container, fg_color="transparent")
        footer.pack(fill="x", pady=(12, 0))

        close_button = ctk.CTkButton(
            footer,
            text="閉じる",
            width=120,
            fg_color="#111827",
            hover_color="#374151",
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            command=dialog.destroy,
        )
        close_button.pack(side="right")

        refresh_list()
    
    def _update_araichat_button_state(self):
        """ARAICHAT送信ボタンの有効/無効状態を更新"""
        if hasattr(self, 'send_araichat_button'):
            if self.non_inspection_lots_df is not None and not self.non_inspection_lots_df.empty:
                # 有効時: 白基調で青と黒の配色（他のボタンと同じホバーパターン）
                self.send_araichat_button.configure(
                    state="normal", 
                    fg_color="#FFFFFF",  # 白背景
                    hover_color="#E5E7EB",  # ホバー時は薄いグレー（他のボタンと同じパターン）
                    border_color="#3B82F6",  # 青のボーダー
                    border_width=3,  # ボーダーを太く
                    text_color="#1E3A8A"  # 濃い青のテキスト
                )
            else:
                # 無効時: グレー
                self.send_araichat_button.configure(
                    state="disabled", 
                    fg_color="#9CA3AF", 
                    hover_color="#9CA3AF",
                    border_color="#9CA3AF",
                    border_width=2,
                    text_color="#6B7280"
                )
    
    def show_non_inspection_lots_confirmation(self):
        """検査対象外ロット情報の確認ウィンドウを表示"""
        try:
            # データの確認
            if self.non_inspection_lots_df is None or self.non_inspection_lots_df.empty:
                messagebox.showwarning(
                    "送信エラー",
                    "検査対象外ロット情報がありません。\n\nデータ抽出を実行してから送信してください。"
                )
                return
            
            # 既に開いている場合は前面化
            try:
                if self._non_inspection_confirm_window is not None and self._non_inspection_confirm_window.winfo_exists():
                    self._non_inspection_confirm_window.lift()
                    self._non_inspection_confirm_window.focus_set()
                    return
            except Exception:
                self._non_inspection_confirm_window = None

            # 確認ウィンドウを作成（サイズは後で動的に調整）
            confirm_window = ctk.CTkToplevel(self.root)
            self._non_inspection_confirm_window = confirm_window
            confirm_window.title("検査対象外ロット情報 - 送信確認")
            confirm_window.transient(self.root)
            confirm_window.grab_set()
            confirm_window.focus_set()
            
            # モーダルウィンドウのスクロール問題を修正
            # 親ウィンドウのスクロールイベントを無効化
            def disable_parent_scroll(event):
                # モーダルウィンドウが開いている間は親ウィンドウのスクロールを無効化
                if confirm_window.winfo_exists():
                    return "break"
                return None
            
            # 親ウィンドウとその子ウィジェットのスクロールイベントを一時的に無効化
            scroll_bindings = []
            def disable_scroll_on_widget(widget):
                try:
                    # 既存のバインディングを保存
                    bindings = widget.bind("<MouseWheel>")
                    if bindings:
                        scroll_bindings.append((widget, "<MouseWheel>", bindings))
                    widget.bind("<MouseWheel>", disable_parent_scroll, add="+")
                    
                    # Linux用
                    bindings4 = widget.bind("<Button-4>")
                    if bindings4:
                        scroll_bindings.append((widget, "<Button-4>", bindings4))
                    widget.bind("<Button-4>", disable_parent_scroll, add="+")
                    
                    bindings5 = widget.bind("<Button-5>")
                    if bindings5:
                        scroll_bindings.append((widget, "<Button-5>", bindings5))
                    widget.bind("<Button-5>", disable_parent_scroll, add="+")
                except:
                    pass
            
            # 親ウィンドウとその子ウィジェットに適用
            disable_scroll_on_widget(self.root)
            for child in self.root.winfo_children():
                try:
                    disable_scroll_on_widget(child)
                except:
                    pass
            
            # ウィンドウが閉じられた時にイベントを復元
            def on_window_close():
                # 保存したバインディングを復元
                for widget, event, binding in scroll_bindings:
                    try:
                        if binding:
                            widget.bind(event, binding)
                        else:
                            widget.unbind(event)
                    except:
                        pass
                try:
                    self._non_inspection_confirm_window = None
                except Exception:
                    pass
                confirm_window.destroy()
            
            confirm_window.protocol("WM_DELETE_WINDOW", on_window_close)
            
            # メインフレーム（メインUIと同じスタイル）
            main_frame = ctk.CTkFrame(confirm_window, fg_color="#EFF6FF", corner_radius=12)
            main_frame.pack(fill="both", expand=True, padx=20, pady=20)
            
            # タイトル（メインUIと同じスタイル）
            title_label = ctk.CTkLabel(
                main_frame,
                text="検査対象外ロット情報 - 送信確認",
                font=ctk.CTkFont(family="Yu Gothic", size=20, weight="bold"),
                text_color="#1E3A8A"
            )
            title_label.pack(pady=(15, 10))
            
            # 説明ラベル（メインUIと同じスタイル）
            info_label = ctk.CTkLabel(
                main_frame,
                text=f"以下の {len(self.non_inspection_lots_df)}件 の検査対象外ロット情報があります。\n送信する行にチェックを入れてから送信してください。",
                font=ctk.CTkFont(family="Yu Gothic", size=14),
                text_color="#374151",
                justify="left"
            )
            info_label.pack(pady=(0, 15))
            
            # テーブル表示用のフレーム（メインUIと同じスタイル）
            table_frame = ctk.CTkFrame(main_frame, fg_color="white", corner_radius=8, border_width=1, border_color="#DBEAFE")
            table_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
            
            # Treeview用の内部フレーム（ttk.Treeviewは通常のtk.Frameが必要）
            tree_container = tk.Frame(table_frame, bg="white")
            tree_container.pack(fill="both", expand=True, padx=10, pady=10)
            
            # 表示する列を選択（工程情報、前工程名、後工程名を削除）
            # 出荷予定日を送信の右に配置するため、最初に配置
            # 品名の右に客先を追加
            display_columns = ['出荷予定日', '品番', '品名', '客先', 'ロット数量', '現在工程名', '指示日', '号機', '生産ロットID']
            available_columns = [col for col in display_columns if col in self.non_inspection_lots_df.columns]
            
            # チェックボックス列を最初に追加、その次に出荷予定日
            tree_columns = ['送信'] + available_columns
            
            # Treeviewで表示
            tree = ttk.Treeview(tree_container, columns=tree_columns, show='headings', height=20)
            
            # 列の設定
            column_widths = {
                '送信': 50,
                '品番': 120,
                '品名': 150,
                '客先': 150,
                'ロット数量': 100,
                '現在工程名': 150,
                '指示日': 120,
                '号機': 80,
                '生産ロットID': 120,
                '出荷予定日': 120
            }
            
            # チェックボックス列の設定
            tree.heading('送信', text='送信')
            tree.column('送信', width=column_widths.get('送信', 50), anchor='center')
            
            for col in available_columns:
                tree.heading(col, text=col)
                tree.column(col, width=column_widths.get(col, 120), anchor='w')
            
            # チェック状態を管理する辞書（デフォルトはFalse）
            check_states = {}
            
            # スクロールバー
            v_scrollbar = ttk.Scrollbar(tree_container, orient="vertical", command=tree.yview)
            h_scrollbar = ttk.Scrollbar(tree_container, orient="horizontal", command=tree.xview)
            tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
            
            # グリッド配置
            tree.grid(row=0, column=0, sticky="nsew")
            v_scrollbar.grid(row=0, column=1, sticky="ns")
            h_scrollbar.grid(row=1, column=0, sticky="ew")
            
            tree_container.grid_rowconfigure(0, weight=1)
            tree_container.grid_columnconfigure(0, weight=1)
            
            # スタイル設定（他のUIと統一：Yu Gothicフォントを使用）
            style = ttk.Style()
            style.configure("Treeview", 
                          background="white",
                          foreground="#374151",
                          fieldbackground="white",
                          font=("Yu Gothic", 10))
            # 選択時のハイライトを無効化（背景色を白のまま）
            style.map("Treeview",
                     background=[('selected', 'white')],
                     foreground=[('selected', '#374151')])
            
            # チェックありの行のタグスタイル（1段階強い色）
            tree.tag_configure('checked', background='#BFDBFE')  # より濃い青の背景
            
            # マウスオーバー時のハイライト用タグ（1段階強い色）
            tree.tag_configure('hover', background='#E5E7EB')  # より濃いグレーの背景
            
            # 現在マウスオーバーしている行を追跡
            current_hover_item = None
            
            # マウスオーバー時のハイライト処理
            def on_mouse_enter(event):
                nonlocal current_hover_item
                item = tree.identify_row(event.y)
                if item and item != current_hover_item:
                    # 前の行のハイライトを解除
                    if current_hover_item:
                        prev_tags = list(tree.item(current_hover_item, 'tags'))
                        if 'hover' in prev_tags:
                            prev_tags.remove('hover')
                        # チェック状態を維持
                        if check_states.get(current_hover_item, False):
                            if 'checked' not in prev_tags:
                                prev_tags.append('checked')
                        tree.item(current_hover_item, tags=tuple(prev_tags) if prev_tags else ())
                    
                    # 新しい行をハイライト
                    current_hover_item = item
                    current_tags = list(tree.item(item, 'tags'))
                    if 'hover' not in current_tags:
                        current_tags.append('hover')
                    tree.item(item, tags=tuple(current_tags))
            
            def on_mouse_leave(event):
                nonlocal current_hover_item
                if current_hover_item:
                    tags = list(tree.item(current_hover_item, 'tags'))
                    if 'hover' in tags:
                        tags.remove('hover')
                    # チェック状態を維持
                    if check_states.get(current_hover_item, False):
                        if 'checked' not in tags:
                            tags.append('checked')
                    tree.item(current_hover_item, tags=tuple(tags) if tags else ())
                    current_hover_item = None
            
            # マウス移動時のハイライト処理
            def on_mouse_motion(event):
                on_mouse_enter(event)
            
            # チェックボックスのトグル関数（行全体のクリックに対応）
            def toggle_check(event):
                # クリック位置を確認
                region = tree.identify_region(event.x, event.y)
                if region not in ("cell", "tree"):
                    return
                
                item = tree.identify_row(event.y)
                if not item:
                    return
                
                # 行全体のクリックでチェック状態をトグル
                current_state = check_states.get(item, False)
                new_state = not current_state
                check_states[item] = new_state
                
                # チェックボックスの表示を更新
                check_symbol = "☑" if new_state else "☐"
                tree.set(item, '送信', check_symbol)
                
                # ハイライト表示を更新（タグを使用）
                current_tags = list(tree.item(item, 'tags'))
                # hoverタグを維持
                has_hover = 'hover' in current_tags
                if new_state:
                    if 'checked' not in current_tags:
                        current_tags.append('checked')
                else:
                    if 'checked' in current_tags:
                        current_tags.remove('checked')
                
                # hoverタグを再追加
                if has_hover and 'hover' not in current_tags:
                    current_tags.append('hover')
                
                tree.item(item, tags=tuple(current_tags) if current_tags else ())
            
            # 行全体のクリックイベントをバインド
            def on_tree_click(event):
                toggle_check(event)
                # デフォルトの選択動作を防ぐ
                return "break"
            
            tree.bind("<Button-1>", on_tree_click)
            tree.bind("<Double-1>", on_tree_click)
            tree.bind("<Motion>", on_mouse_motion)
            tree.bind("<Leave>", on_mouse_leave)
            
            # データを挿入（日付形式をyyyy/mm/ddに変換）
            # TreeviewのアイテムIDとDataFrameのインデックスの対応を保持
            item_to_df_index = {}
            def _format_date_series_for_tree(series: pd.Series) -> pd.Series:
                original = series.astype(object)
                original_str = original.where(pd.notna(original), '').astype(str)
                parsed = pd.to_datetime(original, errors='coerce')
                formatted = parsed.dt.strftime('%Y/%m/%d')
                mask = parsed.notna()
                return original_str.where(~mask, formatted)

            tree_df = self.non_inspection_lots_df.loc[:, available_columns].copy()
            for col in available_columns:
                if col in ['指示日', '出荷予定日'] and col in tree_df.columns:
                    tree_df[col] = _format_date_series_for_tree(tree_df[col])
                else:
                    series = tree_df[col].astype(object)
                    tree_df[col] = series.where(pd.notna(series), '').astype(str)

            for row_tuple in tree_df.itertuples(index=True, name=None):
                idx = row_tuple[0]
                values = ['☐', *row_tuple[1:]]  # チェックボックス列（デフォルトは未チェック）
                item_id = tree.insert("", "end", values=values)
                # チェック状態を初期化（デフォルトはFalse）
                check_states[item_id] = False
                # TreeviewのアイテムIDとDataFrameのインデックスの対応を保存
                item_to_df_index[item_id] = idx
            
            # ウィンドウサイズを動的に計算（列幅の合計 + 余白 + スクロールバー）
            # 実際に設定された列幅の合計を計算
            total_column_width = column_widths.get('送信', 50)  # チェックボックス列
            for col in available_columns:
                total_column_width += column_widths.get(col, 120)
            
            # 余白とスクロールバーを考慮
            # メインフレームのパディング（左右20px × 2 = 40px）
            # テーブルフレームのパディング（左右15px × 2 = 30px）
            # ツリーコンテナのパディング（左右10px × 2 = 20px）
            # 縦スクロールバー（約20px）
            # ウィンドウの装飾（約20px）
            total_padding = 40 + 30 + 20 + 20 + 20
            window_width = total_column_width + total_padding
            
            # 画面サイズを取得して、最大幅を制限（画面幅の95%を超えないように）
            screen_width = confirm_window.winfo_screenwidth()
            max_width = int(screen_width * 0.95)
            window_width = min(window_width, max_width)
            
            # ウィンドウの高さはデータ行数に応じて調整
            row_count = len(self.non_inspection_lots_df)
            # 1行あたり約25px、ヘッダーとボタンエリアで約250px
            calculated_height = max(400, min(800, row_count * 25 + 250))
            # 画面高さの90%を超えないように
            screen_height = confirm_window.winfo_screenheight()
            max_height = int(screen_height * 0.9)
            calculated_height = min(calculated_height, max_height)
            
            # ウィンドウサイズを設定（update_idletasks()を呼んでから計算を正確にする）
            confirm_window.update_idletasks()
            confirm_window.geometry(f"{window_width}x{calculated_height}")
            
            # ウィンドウを中央に配置
            x = (screen_width - window_width) // 2
            y = (screen_height - calculated_height) // 2
            confirm_window.geometry(f"{window_width}x{calculated_height}+{x}+{y}")
            
            # ボタンフレーム（メインUIと同じスタイル）
            button_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
            button_frame.pack(pady=(0, 15))
            
            # 送信ボタン（チェックありの行のみを送信）
            def send_checked_items():
                # チェックありの行のインデックスを取得
                checked_indices = []
                for item_id, is_checked in check_states.items():
                    if is_checked:
                        # TreeviewのアイテムIDから元のDataFrameのインデックスを取得
                        if item_id in item_to_df_index:
                            checked_indices.append(item_to_df_index[item_id])
                
                if not checked_indices:
                    messagebox.showwarning(
                        "送信エラー",
                        "送信対象が選択されていません。\n\nチェックボックスで送信する行を選択してください。"
                    )
                    return
                
                # 送信前の最終確認
                checked_count = len(checked_indices)
                confirm_result = messagebox.askyesno(
                    "送信確認",
                    f"選択された{checked_count}件のロット情報をARAICHATに送信しますか？\n\n"
                    "この操作は取り消せません。"
                )
                
                if not confirm_result:
                    return  # ユーザーが「いいえ」を選択した場合は送信をキャンセル
                
                # チェックありの行のみのDataFrameを作成
                checked_df = self.non_inspection_lots_df.loc[checked_indices].copy()
                
                # ウィンドウを閉じて送信処理を開始
                confirm_window.destroy()
                self.send_non_inspection_lots_to_araichat_with_df(checked_df)
            
            send_button = ctk.CTkButton(
                button_frame,
                text="ARAICHATに送信",
                command=send_checked_items,
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                width=150,
                height=40,
                fg_color="#FFFFFF",  # 白背景
                hover_color="#E5E7EB",  # ホバー時は薄いグレー（他のボタンと同じパターン）
                corner_radius=10,
                border_width=3,
                border_color="#3B82F6",  # 青のボーダー（太く）
                text_color="#1E3A8A"  # 濃い青のテキスト
            )
            send_button.pack(side="left", padx=10)
            
            # キャンセルボタン
            cancel_button = ctk.CTkButton(
                button_frame,
                text="キャンセル",
                command=on_window_close,
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                width=150,
                height=40,
                fg_color="#6B7280",
                hover_color="#4B5563",
                text_color="white"
            )
            cancel_button.pack(side="left", padx=10)

            # ボタンが画面下に隠れるケースがあるため、生成後に最終サイズで再調整
            try:
                confirm_window.update_idletasks()
                required_height = main_frame.winfo_reqheight() + 40
                target_height = min(max(calculated_height, required_height), max_height)
                if target_height != calculated_height:
                    calculated_height = target_height
                    y = (screen_height - calculated_height) // 2
                    confirm_window.geometry(f"{window_width}x{calculated_height}+{x}+{y}")
            except Exception:
                pass
            
        except Exception as e:
            self.log_message(f"確認ウィンドウの表示中にエラーが発生しました: {str(e)}")
            logger.error(f"確認ウィンドウ表示エラー: {e}", exc_info=True)
            messagebox.showerror("エラー", f"確認ウィンドウの表示に失敗しました:\n{str(e)}")
    
    def _confirm_and_send(self, confirm_window):
        """確認ウィンドウを閉じて送信処理を開始（後方互換性のため残す）"""
        confirm_window.destroy()
        self.send_non_inspection_lots_to_araichat()
    
    def send_non_inspection_lots_to_araichat_with_df(self, target_df):
        """指定されたDataFrameをARAICHATに送信"""
        try:
            # 送信処理を別スレッドで実行（UIをブロックしない）
            if hasattr(self, 'send_araichat_button'):
                self.send_araichat_button.configure(state="disabled", text="送信中...")
            threading.Thread(
                target=self._send_non_inspection_lots_to_araichat_thread_with_df,
                args=(target_df,),
                daemon=True
            ).start()
            
        except Exception as e:
            self.log_message(f"ARAICHAT送信処理の開始中にエラーが発生しました: {str(e)}")
            logger.error(f"ARAICHAT送信処理開始エラー: {e}", exc_info=True)
            messagebox.showerror("エラー", f"送信処理の開始に失敗しました:\n{str(e)}")
            if hasattr(self, 'send_araichat_button'):
                self.send_araichat_button.configure(state="normal", text="検査対象外ロットをARAICHATに送信")
    
    def send_non_inspection_lots_to_araichat(self):
        """検査対象外ロット情報をARAICHATに送信（手動送信）"""
        try:
            # 送信処理を別スレッドで実行（UIをブロックしない）
            self.send_araichat_button.configure(state="disabled", text="送信中...")
            threading.Thread(
                target=self._send_non_inspection_lots_to_araichat_thread,
                daemon=True
            ).start()
            
        except Exception as e:
            self.log_message(f"ARAICHAT送信処理の開始中にエラーが発生しました: {str(e)}")
            logger.error(f"ARAICHAT送信処理開始エラー: {e}", exc_info=True)
            messagebox.showerror("エラー", f"送信処理の開始に失敗しました:\n{str(e)}")
            if hasattr(self, 'send_araichat_button'):
                self.send_araichat_button.configure(state="normal", text="検査対象外ロットをARAICHATに送信")
    
    def _send_non_inspection_lots_to_araichat_thread_with_df(self, target_df):
        """ARAICHAT送信処理（スレッド実行、指定されたDataFrameを使用）"""
        try:
            # ARAICHAT通知サービスを初期化
            if (not self.config or 
                not self.config.araichat_base_url or 
                not self.config.araichat_api_key):
                self.log_message("ARAICHAT設定が不完全です。config.envを確認してください。")
                messagebox.showwarning(
                    "設定エラー",
                    "ARAICHAT設定が不完全です。\n\n"
                    "config.envに以下を設定してください：\n"
                    "- ARAICHAT_BASE_URL\n"
                    "- ARAICHAT_API_KEY\n"
                    "- ARAICHAT_ROOM_CONFIG_PATH（オプション）"
                )
                return
            
            from app.services.chat_notification_service import ChatNotificationService
            chat_service = ChatNotificationService(
                base_url=self.config.araichat_base_url,
                api_key=self.config.araichat_api_key,
                room_config_path=self.config.araichat_room_config_path
            )
            
            # 全体を1回で送信（ソート順を維持）
            # ソート順を維持（出荷予定日 → 品番 → 指示日）
            target_df_sorted = self._sort_non_inspection_lots_df(target_df)
            
            # 送信先の工程名を決定（最初に見つかった工程名、またはNone）
            process_name_for_send = None
            if '現在工程名' in target_df_sorted.columns:
                # 最初の非空の工程名を取得
                for process_name in target_df_sorted['現在工程名'].dropna().unique():
                    if pd.notna(process_name) and str(process_name).strip():
                        process_name_for_send = str(process_name).strip()
                        break
            
            try:
                success = chat_service.send_non_inspection_lots_notification(
                    target_df_sorted,
                    process_name_for_send
                )
                
                if success:
                    self.log_message(f"ARAICHAT通知を送信しました（{len(target_df_sorted)}件のロット）")
                    messagebox.showinfo("送信完了", f"送信が完了しました。\n\n送信件数: {len(target_df_sorted)}件")
                else:
                    self.log_message("ARAICHAT通知の送信に失敗しました")
                    messagebox.showerror("送信失敗", "ARAICHAT通知の送信に失敗しました。\n\nログを確認してください。")
            except Exception as e:
                error_msg = f"ARAICHAT通知送信中にエラーが発生しました: {str(e)}"
                self.log_message(error_msg)
                logger.error(f"ARAICHAT通知送信エラー: {e}", exc_info=True)
                messagebox.showerror("送信エラー", error_msg)
            
        except Exception as e:
            error_msg = f"ARAICHAT送信処理中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(f"ARAICHAT送信処理エラー: {e}", exc_info=True)
            messagebox.showerror("エラー", error_msg)
        finally:
            # ボタンの状態を復元
            if hasattr(self, 'send_araichat_button'):
                self.send_araichat_button.configure(state="normal", text="検査対象外ロットをARAICHATに送信")
                self._update_araichat_button_state()
    
    def _send_non_inspection_lots_to_araichat_thread(self):
        """ARAICHAT送信処理（スレッド実行）"""
        try:
            # ARAICHAT通知サービスを初期化
            if (not self.config or 
                not self.config.araichat_base_url or 
                not self.config.araichat_api_key):
                self.log_message("ARAICHAT設定が不完全です。config.envを確認してください。")
                messagebox.showwarning(
                    "設定エラー",
                    "ARAICHAT設定が不完全です。\n\n"
                    "config.envに以下を設定してください：\n"
                    "- ARAICHAT_BASE_URL\n"
                    "- ARAICHAT_API_KEY\n"
                    "- ARAICHAT_ROOM_CONFIG_PATH（オプション）"
                )
                return
            
            from app.services.chat_notification_service import ChatNotificationService
            chat_service = ChatNotificationService(
                base_url=self.config.araichat_base_url,
                api_key=self.config.araichat_api_key,
                room_config_path=self.config.araichat_room_config_path
            )
            
            # 全体を1回で送信（ソート順を維持）
            # ソート順を維持（出荷予定日 → 品番 → 指示日）
            target_df_sorted = self._sort_non_inspection_lots_df(self.non_inspection_lots_df)
            
            # 送信先の工程名を決定（最初に見つかった工程名、またはNone）
            process_name_for_send = None
            if '現在工程名' in target_df_sorted.columns:
                # 最初の非空の工程名を取得
                for process_name in target_df_sorted['現在工程名'].dropna().unique():
                    if pd.notna(process_name) and str(process_name).strip():
                        process_name_for_send = str(process_name).strip()
                        break
            
            try:
                success = chat_service.send_non_inspection_lots_notification(
                    target_df_sorted,
                    process_name_for_send
                )
                
                if success:
                    self.log_message(f"ARAICHAT通知を送信しました（{len(target_df_sorted)}件のロット）")
                    messagebox.showinfo("送信完了", f"送信が完了しました。\n\n送信件数: {len(target_df_sorted)}件")
                else:
                    self.log_message("ARAICHAT通知の送信に失敗しました")
                    messagebox.showerror("送信失敗", "ARAICHAT通知の送信に失敗しました。\n\nログを確認してください。")
            except Exception as e:
                error_msg = f"ARAICHAT通知送信中にエラーが発生しました: {str(e)}"
                self.log_message(error_msg)
                logger.error(f"ARAICHAT通知送信エラー: {e}", exc_info=True)
                messagebox.showerror("送信エラー", error_msg)
            
        except Exception as e:
            error_msg = f"ARAICHAT送信処理中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(f"ARAICHAT送信処理エラー: {e}", exc_info=True)
            messagebox.showerror("エラー", error_msg)
        finally:
            # ボタンの状態を復元
            if hasattr(self, 'send_araichat_button'):
                self.send_araichat_button.configure(state="normal", text="検査対象外ロットをARAICHATに送信")
                self._update_araichat_button_state()
