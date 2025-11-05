"""
出荷検査データ抽出システム - メインUI
近未来的なデザインで出荷予定日を指定してデータを抽出する
"""

import customtkinter as ctk
import tkinter as tk
from tkinter import messagebox, filedialog, ttk
import pandas as pd
import pyodbc
from datetime import datetime, date, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from pathlib import Path
from loguru import logger
from app.config import DatabaseConfig
import calendar
import locale
from app.export.excel_exporter_service import ExcelExporter
from app.export.google_sheets_exporter_service import GoogleSheetsExporter
from app.assignment.inspector_assignment_service import InspectorAssignmentManager
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.font_manager as fm


class ModernDataExtractorUI:
    """近未来的なデザインのデータ抽出UI"""
    
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
        self.root.title("出荷検査データ抽出システム")
        self.root.geometry("1200x800")  # 初期サイズを設定
        self.root.minsize(1000, 700)
        
        # ウィンドウの背景色を白に設定
        self.root.configure(fg_color=("white", "white"))
        
        # ウィンドウの閉じるボタン（×）のイベントを設定
        self.root.protocol("WM_DELETE_WINDOW", self.quit_application)
        
        # 変数の初期化
        self.config = None
        self.extractor = None
        self.is_extracting = False
        self.selected_start_date = None
        self.selected_end_date = None
        
        # カレンダー用の変数初期化
        today = date.today()
        self.current_year = today.year
        self.current_month = today.month
        
        # Excelエクスポーターの初期化
        self.excel_exporter = ExcelExporter()
        
        # Googleスプレッドシートエクスポーターの初期化（設定読み込み後に更新）
        self.google_sheets_exporter = None
        
        # 検査員割当てマネージャーの初期化
        self.inspector_manager = InspectorAssignmentManager(log_callback=self.log_message)
        
        # データ保存用変数
        self.current_main_data = None
        self.current_assignment_data = None
        self.current_inspector_data = None
        
        # スキル表示状態管理
        self.show_skill_values = False
        self.original_inspector_data = None  # 元のデータを保持
        
        # グラフ表示状態管理
        self.show_graph = False
        self.graph_frame = None
        
        # マスタデータ保存用変数
        self.inspector_master_data = None
        self.skill_master_data = None
        self.inspection_target_keywords = []  # 検査対象.csvのA列の文字列リスト
        
        # マスタデータキャッシュ機能
        self.master_cache = {}
        self.cache_timestamps = {}
        self.cache_ttl = timedelta(minutes=5)  # 5分間キャッシュ
        
        # 現在表示中のテーブル
        self.current_display_table = None
        
        # UIの構築
        self.setup_ui()
        
        # ログ設定
        self.setup_logging()
        
        # 設定の読み込み
        self.load_config()
        
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
        try:
            # メインスクロールフレームにマウスホイールイベントをバインド
            def on_main_mousewheel(event):
                # CTkScrollableFrameのスクロール速度を他のテーブルと同等にするため、スクロール量を14倍に設定
                scroll_amount = int(-1 * (event.delta / 120)) * 14
                # CTkScrollableFrameの正しいスクロールメソッドを使用
                if hasattr(self.main_scroll_frame, 'yview_scroll'):
                    self.main_scroll_frame.yview_scroll(scroll_amount, "units")
                else:
                    # CTkScrollableFrameの場合は内部のCanvasを直接操作
                    canvas = self.main_scroll_frame._parent_canvas
                    if canvas:
                        canvas.yview_scroll(scroll_amount, "units")
                return "break"
            
            # 既存のバインドを解除してから新しいバインドを追加
            self.main_scroll_frame.unbind_all("<MouseWheel>")
            self.main_scroll_frame.bind("<MouseWheel>", on_main_mousewheel)
            
            # メインスクロールフレーム内のすべての子ウィジェットにもバインド（タイトル部分、ボタン部分など）
            def bind_to_children(widget):
                """再帰的に子ウィジェットにマウスホイールイベントをバインド"""
                try:
                    # テーブル以外のウィジェットにバインド
                    if not isinstance(widget, (ttk.Treeview, ttk.Scrollbar)):
                        widget.bind("<MouseWheel>", on_main_mousewheel)
                    
                    # 子ウィジェットを再帰的に処理
                    for child in widget.winfo_children():
                        bind_to_children(child)
                except:
                    pass
            
            # メインスクロールフレームの子ウィジェットにバインド
            bind_to_children(self.main_scroll_frame)
            
        except Exception as e:
            logger.error(f"メインスクロールバインド中にエラーが発生しました: {str(e)}")
    
    def setup_logging(self):
        """ログ設定"""
        from pathlib import Path
        from datetime import datetime
        
        logger.remove()  # デフォルトのハンドラーを削除
        
        # ログディレクトリを作成
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # ログファイルのパス（日付ごとにファイルを分ける）
        log_file = log_dir / f"app_{datetime.now().strftime('%Y%m%d')}.log"
        
        # コンソール出力用のフィルタ関数
        def console_filter(record):
            """重要なログのみコンソールに出力"""
            message = record["message"]
            level = record["level"].name
            # WARNING以上、または重要なマーカーを含むメッセージのみ
            return (level in ["WARNING", "ERROR", "CRITICAL"] or 
                   "⚠️" in message or 
                   "❌" in message or 
                   "📊" in message)
        
        # コンソール出力（重要なログのみ）
        def _safe_console_output(message: str) -> None:
            import sys
            try:
                print(message, end="")
            except UnicodeEncodeError:
                encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
                safe_message = message.encode(encoding, errors="replace").decode(encoding, errors="replace")
                print(safe_message, end="")

        logger.add(
            _safe_console_output,
            level="INFO",
            format="{time:HH:mm:ss} | {level} | {message}",
            filter=console_filter
        )
        
        # ファイル出力（すべてのログ）
        logger.add(
            log_file,
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
            rotation="10 MB",  # 10MBごとにローテーション
            retention="30 days",  # 30日間保持
            encoding="utf-8",
            backtrace=True,
            diagnose=True
        )
        
        logger.info(f"ログファイル: {log_file.absolute()}")
        print(f"📝 ログファイル: {log_file.absolute()}")
    
    def load_config(self):
        """設定の読み込み"""
        try:
            self.config = DatabaseConfig()
            if self.config.validate_config():
                logger.info("設定の読み込みが完了しました")
                
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
        
        # ボタンセクション
        self.create_button_section(self.main_scroll_frame)
        
        # 進捗セクション
        self.create_progress_section(self.main_scroll_frame)
        
        # データ表示セクションは選択式表示のため削除
        # self.create_data_display_section(self.main_scroll_frame)
        
        # ログセクションは削除
        
        # メインスクロールをバインド
        self.bind_main_scroll()
    
    def create_title_section(self, parent):
        """タイトルセクションの作成"""
        title_frame = ctk.CTkFrame(parent, height=60, fg_color="white", corner_radius=0)
        title_frame.pack(fill="x", pady=(10, 15))
        title_frame.pack_propagate(False)
        
        # メインタイトル
        title_label = ctk.CTkLabel(
            title_frame,
            text="出荷検査データ抽出システム",
            font=ctk.CTkFont(family="Yu Gothic", size=28, weight="bold"),
            text_color="#1E3A8A"  # 濃い青
        )
        title_label.pack(pady=(10, 5))
        
    
    def create_date_section(self, parent):
        """日付選択セクションの作成"""
        date_frame = ctk.CTkFrame(parent, fg_color="#F8FAFC", corner_radius=12)
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
        period_frame = ctk.CTkFrame(date_frame, fg_color="white", corner_radius=8)
        period_frame.pack(fill="x", padx=15, pady=(0, 10))
        
        # 期間選択UIを作成
        self.create_period_selector(period_frame)
    
    def create_period_selector(self, parent):
        """期間選択UIの作成"""
        # 出荷予定日ラベル
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.pack(fill="x", padx=15, pady=(15, 8))
        
        date_label = ctk.CTkLabel(
            label_frame,
            text="出荷予定日",
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            text_color="#374151"
        )
        date_label.pack(side="left")
        
        # 日付入力フレーム
        date_input_frame = ctk.CTkFrame(parent, fg_color="transparent")
        date_input_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        # 開始日入力
        start_date_frame = ctk.CTkFrame(date_input_frame, fg_color="white", corner_radius=8)
        start_date_frame.pack(side="left", fill="x", expand=True, padx=(0, 5))
        
        self.start_date_entry = ctk.CTkEntry(
            start_date_frame,
            placeholder_text="YYYY/MM/DD",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=40,
            border_width=1,
            fg_color="white",
            text_color="#374151"
        )
        self.start_date_entry.pack(side="left", fill="x", expand=True, padx=10, pady=10)
        
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
        start_calendar_button.pack(side="right", padx=(0, 8), pady=8)
        
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
        self.end_date_entry.pack(side="left", fill="x", expand=True, padx=10, pady=10)
        
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
        end_calendar_button.pack(side="right", padx=(0, 8), pady=8)
        
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
            font=ctk.CTkFont(family="Yu Gothic", size=12),  # 14→12に縮小
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
                        font=ctk.CTkFont(family="Yu Gothic", size=12),  # 14→12に縮小
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
            font=ctk.CTkFont(family="Yu Gothic", size=14),
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
                        font=ctk.CTkFont(family="Yu Gothic", size=14),
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
        button_frame.pack(fill="x", pady=(10, 10), padx=20)
        
        # ボタンフレーム
        buttons_frame = ctk.CTkFrame(button_frame, fg_color="transparent")
        buttons_frame.pack(fill="x", pady=10)
        
        # データ抽出ボタン
        self.extract_button = ctk.CTkButton(
            buttons_frame,
            text="データ抽出開始",
            command=self.start_extraction,
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            height=35,
            width=120,
            fg_color="#3B82F6",
            hover_color="#2563EB",
            corner_radius=6
        )
        self.extract_button.pack(side="left", padx=(0, 10))
        
        # 設定リロードボタン
        self.reload_button = ctk.CTkButton(
            buttons_frame,
            text="設定リロード",
            command=self.reload_config,
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            height=35,
            width=100,
            fg_color="#6B7280",
            hover_color="#4B5563",
            corner_radius=6
        )
        self.reload_button.pack(side="left", padx=(0, 10))
        
        # Excel出力ボタン
        self.export_button = ctk.CTkButton(
            buttons_frame,
            text="Excel出力",
            command=self.export_selected_table,
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            height=35,
            width=80,
            fg_color="#10B981",
            hover_color="#059669",
            corner_radius=6
        )
        self.export_button.pack(side="left", padx=(0, 10))
        
        # テーブル選択フレーム
        table_selection_frame = ctk.CTkFrame(buttons_frame, fg_color="transparent")
        table_selection_frame.pack(side="left", padx=(20, 10))
        
        # テーブル選択ラベル
        table_label = ctk.CTkLabel(
            table_selection_frame,
            text="表示テーブル:",
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            text_color="#1E3A8A"
        )
        table_label.pack(side="left", padx=(0, 5))
        
        # テーブル選択ボタン
        self.main_data_button = ctk.CTkButton(
            table_selection_frame,
            text="抽出データ",
            command=lambda: self.show_table("main"),
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            height=35,
            width=80,
            fg_color="#6B7280",
            hover_color="#4B5563",
            corner_radius=6
        )
        self.main_data_button.pack(side="left", padx=(0, 5))
        
        self.assignment_button = ctk.CTkButton(
            table_selection_frame,
            text="ロット割当",
            command=lambda: self.show_table("assignment"),
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            height=35,
            width=80,
            fg_color="#6B7280",
            hover_color="#4B5563",
            corner_radius=6
        )
        self.assignment_button.pack(side="left", padx=(0, 5))
        
        self.inspector_button = ctk.CTkButton(
            table_selection_frame,
            text="検査員割振",
            command=lambda: self.show_table("inspector"),
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            height=35,
            width=80,
            fg_color="#6B7280",
            hover_color="#4B5563",
            corner_radius=6
        )
        self.inspector_button.pack(side="left", padx=(0, 5))
        
        # 終了ボタン
        self.exit_button = ctk.CTkButton(
            buttons_frame,
            text="終了",
            command=self.quit_application,
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            height=35,
            width=80,
            fg_color="#EF4444",
            hover_color="#DC2626",
            corner_radius=6
        )
        self.exit_button.pack(side="right")
        
        # Googleスプレッドシート出力ボタン
        self.google_sheets_button = ctk.CTkButton(
            buttons_frame,
            text="Googleスプレッドシートへ出力",
            command=self.export_to_google_sheets,
            font=ctk.CTkFont(family="Yu Gothic", size=12),
            height=35,
            width=200,
            fg_color="#10B981",
            hover_color="#059669",
            corner_radius=6
        )
        self.google_sheets_button.pack(side="right", padx=(0, 20))
    
    def create_progress_section(self, parent):
        """進捗セクションの作成"""
        progress_frame = ctk.CTkFrame(parent, fg_color="#F8FAFC", corner_radius=12)
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
        data_frame = ctk.CTkFrame(parent, fg_color="#F8FAFC", corner_radius=12)
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
                       font=("MS Gothic", 10))
        style.map("Treeview",
                 background=[('selected', '#3B82F6')],
                 foreground=[('selected', 'white')])
        
        # マイナス値用のスタイル設定
        style.configure("Treeview", 
                       background="white",
                       foreground="#374151",
                       fieldbackground="white",
                       font=("MS Gothic", 10))
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
    
    
    def reload_config(self):
        """設定のリロード"""
        self.log_message("設定をリロードしています...")
        self.load_config()
        
        if self.config and self.config.validate_config():
            self.log_message("設定のリロードが完了しました")
        else:
            self.log_message("設定のリロードに失敗しました")
    
    def export_to_excel(self):
        """Excelエクスポート機能"""
        try:
            # ロット割り当てデータがあるかチェック
            if self.current_assignment_data is not None and not self.current_assignment_data.empty:
                # ロット割り当て結果をエクスポート
                success = self.excel_exporter.export_lot_assignment_to_excel(
                    self.current_assignment_data, 
                    self.root
                )
                if success:
                    logger.info("ロット割り当て結果のExcelエクスポートが完了しました")
            else:
                # メインデータをエクスポート
                if self.current_main_data is not None and not self.current_main_data.empty:
                    success = self.excel_exporter.export_main_data_to_excel(
                        self.current_main_data, 
                        self.root
                    )
                    if success:
                        logger.info("抽出データのExcelエクスポートが完了しました")
                else:
                    messagebox.showwarning(
                        "警告", 
                        "エクスポートするデータがありません。\n先にデータを抽出してください。"
                    )
        except Exception as e:
            error_msg = f"Excelエクスポート中にエラーが発生しました: {str(e)}"
            logger.error(error_msg)
            messagebox.showerror("エクスポートエラー", error_msg)
    
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
    
    def extract_data_thread(self, start_date, end_date):
        """データ抽出のスレッド処理"""
        connection = None
        try:
            self.log_message(f"データ抽出を開始します")
            self.log_message(f"抽出期間: {start_date} ～ {end_date}")
            
            # データベース接続
            self.update_progress(0.02, "データベースに接続中...")
            connection_string = self.config.get_connection_string()
            connection = pyodbc.connect(connection_string)
            self.log_message("データベース接続が完了しました")
            
            # 検査対象.csvを読み込む（キャッシュ機能を使用）
            self.update_progress(0.05, "検査対象CSVを読み込み中...")
            self.inspection_target_keywords = self.load_inspection_target_csv_cached()
            
            # テーブル構造を確認
            self.update_progress(0.08, "テーブル構造を確認中...")
            self.log_message("テーブル構造を確認中...")
            columns_query = f"SELECT TOP 1 * FROM [{self.config.access_table_name}]"
            sample_df = pd.read_sql(columns_query, connection)
            
            if sample_df.empty:
                self.log_message("テーブルにデータが見つかりません")
                self.update_progress(1.0, "完了（データなし）")
                return
            
            # 実際の列名を取得
            actual_columns = sample_df.columns.tolist()
            self.log_message(f"テーブルの列: {actual_columns}")
            
            # 指定された列が存在するかチェック（梱包・完了は後で追加するため除外）
            required_columns = ["品番", "品名", "客先", "出荷予定日", "出荷数", "在庫数", "不足数", "処理"]
            available_columns = [col for col in required_columns if col in actual_columns]
            
            if not available_columns:
                self.log_message("指定された列が見つかりません。全列を取得します。")
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
            
            # データの抽出
            self.update_progress(0.15, "データを抽出中...")
            df = pd.read_sql(query, connection)
            self.log_message(f"データ抽出完了: {len(df)}件")
            
            # t_現品票履歴から梱包工程の数量を取得
            self.update_progress(0.35, "梱包工程データを取得中...")
            packaging_data = self.get_packaging_quantities(connection, df)
            
            # 梱包数量をメインデータに結合
            self.update_progress(0.45, "データを処理中...")
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
            
            # 不足数を計算: (在庫数+梱包・完了)-出荷数
            self.update_progress(0.55, "不足数を計算中...")
            if all(col in df.columns for col in ['出荷数', '在庫数', '梱包・完了']):
                # 数値列を数値型に変換（梱包・完了は既に変換済み）
                df['出荷数'] = pd.to_numeric(df['出荷数'], errors='coerce').fillna(0)
                df['在庫数'] = pd.to_numeric(df['在庫数'], errors='coerce').fillna(0)
                
                # 不足数を計算: (在庫数+梱包・完了)-出荷数
                df['不足数'] = (df['在庫数'] + df['梱包・完了']) - df['出荷数']
                self.log_message("不足数を計算しました")
            else:
                df['不足数'] = 0
                self.log_message("不足数の計算に必要な列が見つかりませんでした")
            
            # 出荷予定日をdatetime型に変換（既にSQL側でソート済み）
            if not df.empty and '出荷予定日' in df.columns:
                df['出荷予定日'] = pd.to_datetime(df['出荷予定日'], errors='coerce')
            
            if df is None or df.empty:
                self.log_message("指定された期間にデータが見つかりませんでした")
                self.update_progress(1.0, "完了（データなし）")
                return
            
            self.log_message(f"抽出完了: {len(df)}件のレコード")
            
            # データを保存（エクスポート用）
            self.current_main_data = df
            
            # 不足数がマイナスの品番に対してロット割り当てを実行
            self.update_progress(0.65, "ロット割り当て処理中...")
            self.process_lot_assignment(connection, df, start_progress=0.65)
            
            # 完了
            self.update_progress(1.0, "データ抽出が完了しました")
            self.log_message(f"処理完了! {len(df)}件のデータを表示しました")
            
            # テーブルは選択式表示のため、自動表示しない
            # self.show_table("main")
            
            # 成功メッセージ
            self.root.after(0, lambda: messagebox.showinfo(
                "完了", 
                f"データ抽出が完了しました!\n\n"
                f"抽出件数: {len(df)}件\n"
                f"ロット割り当て: {len(self.current_assignment_data) if self.current_assignment_data is not None else 0}件\n"
                f"検査員割振り: {len(self.current_inspector_data) if self.current_inspector_data is not None else 0}件\n\n"
                f"「表示テーブル」ボタンでテーブルを選択してください"
            ))
            
        except Exception as e:
            error_msg = f"データ抽出中にエラーが発生しました: {str(e)}"
            self.log_message(f"エラー: {error_msg}")
            self.update_progress(0, "エラーが発生しました")
            
            # エラーメッセージ
            self.root.after(0, lambda: messagebox.showerror("エラー", error_msg))
            
        finally:
            # データベース接続を切断
            if connection:
                connection.close()
            
            # UIの状態をリセット
            self.root.after(0, self.reset_ui_state)
    
    def update_progress(self, value, message):
        """進捗の更新"""
        self.root.after(0, lambda: self.progress_bar.set(value))
        self.root.after(0, lambda: self.progress_label.configure(text=message))
    
    def log_message(self, message):
        """ログメッセージの追加（コンソール出力のみ）"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"{timestamp} | {message}"
        try:
            print(log_entry)  # コンソール出力のみ
        except UnicodeEncodeError:
            import sys
            encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
            safe_entry = log_entry.encode(encoding, errors="replace").decode(encoding, errors="replace")
            print(safe_entry)
        logger.info(message)
    
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
            data_frame = ctk.CTkFrame(self.main_scroll_frame, fg_color="#F8FAFC", corner_radius=12)
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
            for index, row in df.head(display_limit).iterrows():
                values = []
                item_id = None
                for col in columns:
                    if pd.notna(row[col]):
                        # 出荷予定日の場合は日付形式で表示
                        if col == '出荷予定日':
                            try:
                                date_value = pd.to_datetime(row[col])
                                values.append(date_value.strftime('%Y/%m/%d'))
                            except:
                                values.append(str(row[col]))
                        # 数値列は整数で表示
                        elif col in ['出荷数', '在庫数', '梱包・完了', '不足数']:
                            try:
                                values.append(str(int(row[col])))
                            except:
                                values.append(str(row[col]))
                        else:
                            values.append(str(row[col]))
                    else:
                        values.append("")
                
                # 行のタグを決定（交互色 + マイナス値の場合は警告色）
                tags = []
                
                # 不足数がマイナスの場合は警告タグを追加（交互色は適用しない）
                is_negative = False
                if '不足数' in columns and pd.notna(row['不足数']):
                    try:
                        shortage = float(row['不足数'])
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
            
            product_numbers = main_df['品番'].dropna().unique().tolist()
            if not product_numbers:
                self.log_message("品番データが見つかりません")
                return pd.DataFrame()
            
            self.log_message(f"梱包工程データを検索中: {len(product_numbers)}件の品番")
            
            # 品番のリストをSQL用の文字列に変換
            product_numbers_str = "', '".join([str(pn) for pn in product_numbers])
            
            # t_現品票履歴から梱包工程のデータを取得
            packaging_query = f"""
            SELECT 品番, 数量
            FROM [t_現品票履歴]
            WHERE 品番 IN ('{product_numbers_str}')
            AND 現在工程名 LIKE '%梱包%'
            """
            
            packaging_df = pd.read_sql(packaging_query, connection)
            
            if packaging_df.empty:
                self.log_message("梱包工程のデータが見つかりませんでした")
                return pd.DataFrame()
            
            # 品番ごとに数量を合計
            packaging_summary = packaging_df.groupby('品番')['数量'].sum().reset_index()
            packaging_summary.columns = ['品番', '梱包・完了']
            
            self.log_message(f"梱包工程データを取得しました: {len(packaging_summary)}件")
            
            return packaging_summary
            
        except Exception as e:
            self.log_message(f"梱包工程データの取得中にエラーが発生しました: {str(e)}")
            return pd.DataFrame()
    
    def get_available_lots_for_shortage(self, connection, shortage_df):
        """不足数がマイナスの品番に対して利用可能なロットを取得"""
        try:
            if shortage_df.empty:
                self.log_message("不足数がマイナスのデータがありません")
                return pd.DataFrame()
            
            # 不足数がマイナスの品番を取得
            shortage_products = shortage_df[shortage_df['不足数'] < 0]['品番'].dropna().unique().tolist()
            if not shortage_products:
                self.log_message("不足数がマイナスの品番が見つかりません")
                return pd.DataFrame()
            
            self.log_message(f"不足数がマイナスの品番: {len(shortage_products)}件")
            
            # まずテーブル構造を確認
            self.log_message("t_現品票履歴テーブル構造を確認中...")
            columns_query = f"SELECT TOP 1 * FROM [t_現品票履歴]"
            sample_df = pd.read_sql(columns_query, connection)
            
            if sample_df.empty:
                self.log_message("t_現品票履歴テーブルにデータが見つかりません")
                return pd.DataFrame()
            
            # 実際の列名を取得
            actual_columns = sample_df.columns.tolist()
            self.log_message(f"t_現品票履歴テーブルの列: {actual_columns}")
            
            # 利用可能な列のみでクエリを作成
            available_columns = [col for col in actual_columns if col in [
                "品番", "数量", "指示日", "号機", "現在工程番号", "現在工程名", "現在工程二次処理", "生産ロットID"
            ]]
            
            if not available_columns:
                self.log_message("必要な列が見つかりません。全列を取得します。")
                available_columns = actual_columns
            
            # 利用可能な列のみでクエリを作成
            columns_str = ", ".join([f"[{col}]" for col in available_columns])
            
            # 品番のリストをSQL用の文字列に変換
            product_numbers_str = "', '".join([str(pn) for pn in shortage_products])
            
            # WHERE条件を動的に構築
            where_conditions = [f"品番 IN ('{product_numbers_str}')"]
            
            # 現在工程名が存在する場合のみ条件を追加
            if "現在工程名" in available_columns:
                where_conditions.append("現在工程名 NOT LIKE '%完了%'")
                where_conditions.append("現在工程名 NOT LIKE '%梱包%'")
                
                # 検査対象.csvのキーワードでフィルタリング
                if self.inspection_target_keywords:
                    # キーワードのいずれかが現在工程名に含まれる条件を追加
                    keyword_conditions = []
                    for keyword in self.inspection_target_keywords:
                        # SQLインジェクション対策: キーワードをエスケープ
                        escaped_keyword = keyword.replace("'", "''").replace("%", "[%]").replace("_", "[_]")
                        keyword_conditions.append(f"現在工程名 LIKE '%{escaped_keyword}%'")
                    
                    if keyword_conditions:
                        # OR条件でキーワードのいずれかに一致する条件を追加
                        keyword_filter = "(" + " OR ".join(keyword_conditions) + ")"
                        where_conditions.append(keyword_filter)
                        self.log_message(f"検査対象キーワードでフィルタリング: {len(self.inspection_target_keywords)}件のキーワード")
                else:
                    self.log_message("検査対象キーワードが設定されていません。全てのロットを対象とします。")
            
            where_clause = " AND ".join(where_conditions)
            
            # ORDER BY条件を動的に構築
            order_conditions = ["品番"]
            if "指示日" in available_columns:
                order_conditions.append("指示日 ASC")
            elif "号機" in available_columns:
                order_conditions.append("号機 ASC")
            
            order_clause = ", ".join(order_conditions)
            
            # 完了・梱包以外の工程のロットを取得
            lots_query = f"""
            SELECT {columns_str}
            FROM [t_現品票履歴]
            WHERE {where_clause}
            ORDER BY {order_clause}
            """
            
            lots_df = pd.read_sql(lots_query, connection)
            
            if lots_df.empty:
                self.log_message("利用可能なロットが見つかりませんでした")
                return pd.DataFrame()
            
            self.log_message(f"利用可能なロットを取得しました: {len(lots_df)}件")
            
            return lots_df
            
        except Exception as e:
            self.log_message(f"利用可能ロットの取得中にエラーが発生しました: {str(e)}")
            return pd.DataFrame()
    
    def assign_lots_to_shortage(self, shortage_df, lots_df):
        """不足数に対してロットを割り当て"""
        try:
            if shortage_df.empty or lots_df.empty:
                return pd.DataFrame()
            
            assignment_results = []
            
            # 品番ごとに処理
            for product_number in shortage_df['品番'].unique():
                product_shortage = shortage_df[shortage_df['品番'] == product_number]
                product_lots = lots_df[lots_df['品番'] == product_number].copy()
                
                if product_lots.empty:
                    continue
                
                # 指示日順でソート
                product_lots = product_lots.sort_values('指示日')
                
                # 品番ごとの不足数を取得（マイナス値のまま）
                initial_shortage = product_shortage['不足数'].iloc[0]
                current_shortage = initial_shortage
                
                # ロットを順番に割り当て（itertuples()で高速化）
                # 列名のインデックスマップを作成
                lot_cols = {col: idx for idx, col in enumerate(product_lots.columns)}
                
                for lot in product_lots.itertuples(index=False):
                    if current_shortage >= 0:  # 不足数が0以上になったら終了
                        break
                    
                    lot_quantity = int(lot[lot_cols['数量']]) if pd.notna(lot[lot_cols['数量']]) else 0
                    
                    # 割り当て結果を記録
                    assignment_result = {
                        '出荷予定日': product_shortage['出荷予定日'].iloc[0],
                        '品番': product_number,
                        '品名': product_shortage['品名'].iloc[0],
                        '客先': product_shortage['客先'].iloc[0],
                        '出荷数': int(product_shortage['出荷数'].iloc[0]),
                        '在庫数': int(product_shortage['在庫数'].iloc[0]),
                        '在梱包数': int(product_shortage['梱包・完了'].iloc[0]),
                        '不足数': current_shortage,  # 現在の不足数（マイナス値）
                        'ロット数量': lot_quantity,  # ロット全体の数量を表示
                        '指示日': lot[lot_cols.get('指示日', -1)] if '指示日' in lot_cols and pd.notna(lot[lot_cols['指示日']]) else '',
                        '号機': lot[lot_cols.get('号機', -1)] if '号機' in lot_cols and pd.notna(lot[lot_cols['号機']]) else '',
                        '現在工程番号': lot[lot_cols.get('現在工程番号', -1)] if '現在工程番号' in lot_cols and pd.notna(lot[lot_cols['現在工程番号']]) else '',
                        '現在工程名': lot[lot_cols.get('現在工程名', -1)] if '現在工程名' in lot_cols and pd.notna(lot[lot_cols['現在工程名']]) else '',
                        '現在工程二次処理': lot[lot_cols.get('現在工程二次処理', -1)] if '現在工程二次処理' in lot_cols and pd.notna(lot[lot_cols['現在工程二次処理']]) else '',
                        '生産ロットID': lot[lot_cols.get('生産ロットID', -1)] if '生産ロットID' in lot_cols and pd.notna(lot[lot_cols['生産ロットID']]) else ''
                    }
                    assignment_results.append(assignment_result)
                    
                    # 次のロットの不足数を計算（ロット数量を加算）
                    current_shortage += lot_quantity
            
            if assignment_results:
                result_df = pd.DataFrame(assignment_results)
                self.log_message(f"ロット割り当て完了: {len(result_df)}件")
                return result_df
            else:
                self.log_message("ロット割り当て結果がありません")
                return pd.DataFrame()
                
        except Exception as e:
            self.log_message(f"ロット割り当て中にエラーが発生しました: {str(e)}")
            return pd.DataFrame()
    
    def process_lot_assignment(self, connection, main_df, start_progress=0.65):
        """ロット割り当て処理のメイン処理"""
        try:
            # 不足数がマイナスのデータを抽出
            self.update_progress(start_progress + 0.05, "不足データを抽出中...")
            shortage_df = main_df[main_df['不足数'] < 0].copy()
            
            if shortage_df.empty:
                self.log_message("不足数がマイナスのデータがありません")
                return
            
            self.log_message(f"不足数がマイナスのデータ: {len(shortage_df)}件")
            
            # 利用可能なロットを取得
            self.update_progress(start_progress + 0.10, "利用可能なロットを取得中...")
            lots_df = self.get_available_lots_for_shortage(connection, shortage_df)
            
            if lots_df.empty:
                self.log_message("利用可能なロットが見つかりませんでした")
                return
            
            # ロット割り当てを実行
            self.update_progress(start_progress + 0.15, "ロットを割り当て中...")
            assignment_df = self.assign_lots_to_shortage(shortage_df, lots_df)
            
            if not assignment_df.empty:
                # ロット割り当て結果は選択式表示のため、ここでは表示しない
                # self.display_lot_assignment_table(assignment_df)
                
                # ロット割り当てデータを保存（エクスポート用）
                self.current_assignment_data = assignment_df
                
                # 検査員割振り処理を実行（進捗は連続させる）
                # ロット割り当て: 0.65-0.85 (0.2の範囲)
                # 検査員割振り: 0.85-1.0 (0.15の範囲)
                self.process_inspector_assignment(assignment_df, start_progress=0.85)
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
            
            # マスタファイルを並列で読み込み（高速化）
            # 進捗範囲を調整：start_progressから終了まで（マスタ読み込み用）
            progress_base = start_progress
            # start_progressに応じて進捗範囲を動的に調整
            # 目標: マスタ読み込み完了後、0.95-0.97の範囲に到達
            if start_progress >= 0.85:
                # 0.85以降から始まる場合: 0.85→0.92（0.07の範囲）
                progress_range_master = 0.07
            elif start_progress >= 0.1:
                # 0.1以降から始まる場合: start_progress→0.9（残りの範囲）
                progress_range_master = 0.9 - start_progress
            else:
                # 通常: 0.1→0.9（0.8の範囲）
                progress_range_master = 0.8
            
            self.update_progress(progress_base, "マスタファイルを読み込み中...")
            masters = self.load_masters_parallel(progress_base=progress_base, progress_range=progress_range_master)
            
            product_master_df = masters.get('product')
            inspector_master_df = masters.get('inspector')
            skill_master_df = masters.get('skill')
            
            if product_master_df is None:
                self.log_message("製品マスタの読み込みに失敗しました")
                return
            
            if inspector_master_df is None:
                self.log_message("検査員マスタの読み込みに失敗しました")
                return
            
            if skill_master_df is None:
                self.log_message("スキルマスタの読み込みに失敗しました")
                return
            
            # マスタデータを保存
            self.inspector_master_data = inspector_master_df
            self.skill_master_data = skill_master_df
            
            # 検査員割振りテーブルを作成（製品マスタパスを渡す）
            # マスタ読み込み完了後の進捗を計算
            master_end_progress = progress_base + progress_range_master
            # テーブル作成と割り当ての進捗範囲を調整（残りを1.0まで）
            remaining_progress = 1.0 - master_end_progress
            table_progress = master_end_progress + (remaining_progress * 0.3)  # 残りの30%
            assign_progress = master_end_progress + (remaining_progress * 0.7)  # 残りの70%
            
            self.update_progress(table_progress, "検査員割振りテーブルを作成中...")
            product_master_path = self.config.product_master_path if self.config else None
            inspector_df = self.inspector_manager.create_inspector_assignment_table(assignment_df, product_master_df, product_master_path)
            if inspector_df is None:
                self.log_message("検査員割振りテーブルの作成に失敗しました")
                return
            
            # 製品マスタが更新された場合は再読み込み
            if product_master_path and Path(product_master_path).exists():
                # 再読み込みは次の処理で行うため、ここではログのみ
                pass
            
            # 検査員を割り当て（スキル値付きで保存）
            self.update_progress(assign_progress, "検査員を割り当て中...")
            inspector_df_with_skills = self.inspector_manager.assign_inspectors(inspector_df, inspector_master_df, skill_master_df, True)
            
            # 現在の表示状態に応じてデータを設定
            if self.show_skill_values:
                inspector_df = inspector_df_with_skills
            else:
                # スキル値を非表示にする場合、氏名のみのデータを作成
                inspector_df = inspector_df_with_skills.copy()
                for col in inspector_df.columns:
                    if col.startswith('検査員'):
                        inspector_df[col] = inspector_df[col].astype(str).apply(
                            lambda x: x.split('(')[0].strip() if '(' in x and ')' in x else x
                        )
            
            # 検査員割振りデータを保存（エクスポート用）
            self.current_inspector_data = inspector_df
            self.original_inspector_data = inspector_df_with_skills.copy()  # スキル値付きの元データを保持
            
            self.update_progress(1.0, "検査員割振り処理が完了しました")
            self.log_message(f"検査員割振り処理が完了しました: {len(inspector_df)}件")
            
        except Exception as e:
            self.log_message(f"検査員割振り処理中にエラーが発生しました: {str(e)}")
    
    def calculate_cumulative_shortage(self, assignment_df):
        """同一品番の連続行で不足数を累積計算"""
        try:
            if assignment_df.empty:
                return assignment_df
            
            # 出荷予定日昇順、同一品番は指示日古い順でソート
            assignment_df = assignment_df.sort_values(['出荷予定日', '品番', '指示日']).reset_index(drop=True)
            
            # 不足数を再計算
            current_product = None
            current_shortage = 0
            
            for index, row in assignment_df.iterrows():
                if current_product != row['品番']:
                    # 新しい品番の場合は初期不足数を設定
                    current_shortage = row['不足数']
                    current_product = row['品番']
                else:
                    # 同一品番の場合は前のロット数量を加算して不足数を更新
                    previous_lot_quantity = assignment_df.iloc[index-1]['ロット数量']
                    current_shortage = current_shortage + previous_lot_quantity
                    
                    # 不足数列を更新
                    assignment_df.at[index, '不足数'] = current_shortage
            
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
            lot_frame = ctk.CTkFrame(self.main_scroll_frame, fg_color="#F8FAFC", corner_radius=12)
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
            for index, row in assignment_df.iterrows():
                values = []
                for col in lot_columns:
                    if pd.notna(row[col]):
                        if col == '出荷予定日' or col == '指示日':
                            try:
                                date_value = pd.to_datetime(row[col])
                                values.append(date_value.strftime('%Y/%m/%d'))
                            except:
                                values.append(str(row[col]))
                        elif col in lot_numeric_columns:
                            try:
                                values.append(str(int(row[col])))
                            except:
                                values.append(str(row[col]))
                        else:
                            values.append(str(row[col]))
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
        self.progress_bar.set(0)
        self.progress_label.configure(text="待機中...")
    
    def run(self):
        """アプリケーションの実行"""
        self.log_message("出荷検査データ抽出システムを起動しました")
        self.log_message("設定を確認してください")
        
        # 設定情報の表示
        if self.config and self.config.validate_config():
            pass  # 設定は正常に読み込まれている
        
        self.root.mainloop()
    
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
                    
                    for key, future in as_completed(futures):
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
    
    def load_product_master_cached(self):
        """キャッシュ付き製品マスタ読み込み"""
        cache_key = 'product_master'
        
        # キャッシュチェック（スレッドセーフな方法で）
        try:
            if cache_key in self.master_cache:
                if datetime.now() - self.cache_timestamps[cache_key] < self.cache_ttl:
                    logger.info("製品マスタをキャッシュから読み込みました")
                    return self.master_cache[cache_key]
        except Exception:
            pass  # キャッシュチェックでエラーが発生した場合は通常読み込みに進む
        
        # キャッシュミスの場合は通常読み込み
        df = self.load_product_master()
        if df is not None:
            try:
                self.master_cache[cache_key] = df
                self.cache_timestamps[cache_key] = datetime.now()
            except Exception:
                pass  # キャッシュ保存でエラーが発生しても続行
        
        return df
    
    def load_inspector_master_cached(self):
        """キャッシュ付き検査員マスタ読み込み"""
        cache_key = 'inspector_master'
        
        # キャッシュチェック（高速化: 簡潔なチェック）
        try:
            if cache_key in self.master_cache and cache_key in self.cache_timestamps:
                if datetime.now() - self.cache_timestamps[cache_key] < self.cache_ttl:
                    return self.master_cache[cache_key]
        except (KeyError, AttributeError):
            pass  # キャッシュチェックでエラーが発生した場合は通常読み込みに進む
        
        # キャッシュミスの場合は通常読み込み
        df = self.load_inspector_master()
        if df is not None:
            try:
                self.master_cache[cache_key] = df
                self.cache_timestamps[cache_key] = datetime.now()
            except Exception:
                pass  # キャッシュ保存でエラーが発生しても続行
        
        return df
    
    def load_skill_master_cached(self):
        """キャッシュ付きスキルマスタ読み込み"""
        cache_key = 'skill_master'
        
        # キャッシュチェック（高速化: 簡潔なチェック）
        try:
            if cache_key in self.master_cache and cache_key in self.cache_timestamps:
                if datetime.now() - self.cache_timestamps[cache_key] < self.cache_ttl:
                    return self.master_cache[cache_key]
        except (KeyError, AttributeError):
            pass  # キャッシュチェックでエラーが発生した場合は通常読み込みに進む
        
        # キャッシュミスの場合は通常読み込み
        df = self.load_skill_master()
        if df is not None:
            try:
                self.master_cache[cache_key] = df
                self.cache_timestamps[cache_key] = datetime.now()
            except Exception:
                pass  # キャッシュ保存でエラーが発生しても続行
        
        return df
    
    def load_inspection_target_csv_cached(self):
        """キャッシュ付き検査対象CSV読み込み"""
        cache_key = 'inspection_target_csv'
        
        # キャッシュチェック（高速化: 簡潔なチェック）
        try:
            if cache_key in self.master_cache and cache_key in self.cache_timestamps:
                if datetime.now() - self.cache_timestamps[cache_key] < self.cache_ttl:
                    return self.master_cache[cache_key]
        except (KeyError, AttributeError):
            pass  # キャッシュチェックでエラーが発生した場合は通常読み込みに進む
        
        # キャッシュミスの場合は通常読み込み
        keywords = self.load_inspection_target_csv()
        if keywords:
            try:
                self.master_cache[cache_key] = keywords
                self.cache_timestamps[cache_key] = datetime.now()
            except Exception:
                pass  # キャッシュ保存でエラーが発生しても続行
        
        return keywords
    
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
            
            # 列名を確認
            self.log_message(f"製品マスタの列: {df.columns.tolist()}")
            
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
                    self.log_message(f"列名をマッピングしました: {column_mapping}")
                else:
                    self.log_message(f"必要な列が見つかりません: {missing_columns}")
                    return None
            
            self.log_message(f"製品マスタを読み込みました: {len(df)}件")
            return df
            
        except Exception as e:
            error_msg = f"製品マスタの読み込みに失敗しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            return None
    
    
    def display_inspector_assignment_table(self, inspector_df):
        """検査員割振りテーブルを表示"""
        try:
            # 既存のテーブルセクションを削除
            self.hide_current_table()
            
            # 検査員割振りセクションを作成
            inspector_frame = ctk.CTkFrame(self.main_scroll_frame, fg_color="#F8FAFC", corner_radius=12)
            inspector_frame.table_section = True
            inspector_frame.pack(fill="x", padx=20, pady=(10, 20))
            
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
            
            # スキル表示切り替えボタン
            button_text = "スキル非表示" if self.show_skill_values else "スキル表示"
            self.skill_toggle_button = ctk.CTkButton(
                button_frame,
                text=button_text,
                command=self.toggle_skill_display,
                width=100,
                height=30,
                font=ctk.CTkFont(family="Yu Gothic", size=12),
                fg_color="#6B7280",
                hover_color="#4B5563"
            )
            self.skill_toggle_button.pack(side="right")
            
            # 詳細表示切り替えボタン
            detail_button_text = "詳細非表示" if self.show_graph else "詳細表示"
            self.graph_toggle_button = ctk.CTkButton(
                button_frame,
                text=detail_button_text,
                command=self.toggle_detail_display,
                width=100,
                height=30,
                font=ctk.CTkFont(family="Yu Gothic", size=12),
                fg_color="#10B981",
                hover_color="#059669"
            )
            self.graph_toggle_button.pack(side="right", padx=(0, 25))
            
            # テーブルフレーム
            table_frame = tk.Frame(inspector_frame)
            table_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
            
            # 列の定義
            inspector_columns = [
                "出荷予定日", "品番", "品名", "客先", "生産ロットID", "ロット数量", 
                "指示日", "号機", "現在工程名", "秒/個", "検査時間",
                "検査員人数", "分割検査時間", "チーム情報", "検査員1", "検査員2", "検査員3", "検査員4", "検査員5"
            ]
            
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
            for _, row in inspector_df.iterrows():
                values = []
                for col in inspector_columns:
                    # 列が存在しない場合は空文字を表示
                    if col not in inspector_df.columns:
                        values.append('')
                        continue
                    
                    if col == '出荷予定日' or col == '指示日':
                        try:
                            date_value = pd.to_datetime(row[col])
                            values.append(date_value.strftime('%Y/%m/%d'))
                        except:
                            values.append(str(row[col]) if pd.notna(row[col]) else '')
                    elif col.startswith('検査員'):
                        # 検査員名の表示制御
                        inspector_name = str(row[col]) if pd.notna(row[col]) else ''
                        if not self.show_skill_values:
                            # スキル値を非表示にする場合、括弧内を削除
                            if '(' in inspector_name and ')' in inspector_name:
                                inspector_name = inspector_name.split('(')[0].strip()
                        else:
                            # スキル値を表示する場合、元のデータから再構築
                            if '(' not in inspector_name and ')' not in inspector_name:
                                # 元のデータからスキル値を取得
                                if self.original_inspector_data is not None:
                                    try:
                                        original_row = self.original_inspector_data.iloc[row.name]
                                        original_name = str(original_row[col])
                                        if '(' in original_name and ')' in original_name:
                                            inspector_name = original_name
                                    except:
                                        pass
                        values.append(inspector_name)
                    else:
                        values.append(str(row[col]) if pd.notna(row[col]) else '')
                
                # 交互行色を適用
                tag = "even" if row_index % 2 == 0 else "odd"
                inspector_tree.insert("", "end", values=values, tags=(tag,))
                row_index += 1
            
            # タグの設定（交互行色）
            inspector_tree.tag_configure("even", background="#F9FAFB")
            inspector_tree.tag_configure("odd", background="#FFFFFF")
            
            # マウスホイールイベントのバインド
            def on_inspector_mousewheel(event):
                inspector_tree.yview_scroll(int(-1 * (event.delta / 120)), "units")
                return "break"
            
            inspector_tree.bind("<MouseWheel>", on_inspector_mousewheel)
            
            # テーブルに入ったときと出たときのイベント（精度向上のため、コンテナフレームにも追加）
            # 注意: unbind_allは使わず、テーブル専用のスクロールを優先的に処理
            def on_inspector_enter(event):
                # テーブル内ではテーブルのスクロールを優先（メインスクロールは無効化しない）
                pass
            
            def on_inspector_leave(event):
                # テーブルから出たときはメインスクロールを再バインド（念のため）
                self.bind_main_scroll()
            
            inspector_tree.bind("<Enter>", on_inspector_enter)
            inspector_tree.bind("<Leave>", on_inspector_leave)
            table_frame.bind("<Enter>", on_inspector_enter)
            table_frame.bind("<Leave>", on_inspector_leave)
            
            self.log_message(f"検査員割振りテーブルを表示しました: {len(inspector_df)}件")
            
        except Exception as e:
            error_msg = f"検査員割振りテーブルの表示に失敗しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
    
    def toggle_skill_display(self):
        """スキル表示の切り替え"""
        try:
            self.show_skill_values = not self.show_skill_values
            
            # ボタンテキストを更新
            if self.show_skill_values:
                self.skill_toggle_button.configure(text="スキル非表示")
            else:
                self.skill_toggle_button.configure(text="スキル表示")
            
            # データを現在の表示状態に応じて切り替え
            if self.original_inspector_data is not None:
                if self.show_skill_values:
                    # スキル値付きのデータを表示
                    self.current_inspector_data = self.original_inspector_data.copy()
                else:
                    # 氏名のみのデータを作成
                    self.current_inspector_data = self.original_inspector_data.copy()
                    for col in self.current_inspector_data.columns:
                        if col.startswith('検査員'):
                            self.current_inspector_data[col] = self.current_inspector_data[col].astype(str).apply(
                                lambda x: x.split('(')[0].strip() if '(' in x and ')' in x else x
                            )
                
                # テーブルを再表示
                self.display_inspector_assignment_table(self.current_inspector_data)
                
        except Exception as e:
            error_msg = f"スキル表示切り替え中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            messagebox.showerror("エラー", error_msg)
    
    def toggle_detail_display(self):
        """詳細表示の切り替え（ポップアップ表示）"""
        try:
            if not hasattr(self, 'current_inspector_data') or self.current_inspector_data is None:
                messagebox.showwarning("警告", "検査員割当てデータがありません。先に検査員割当てを実行してください。")
                return
            
            # 詳細表示ポップアップを表示
            self.show_detail_popup()
                
        except Exception as e:
            error_msg = f"詳細表示切り替え中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            messagebox.showerror("エラー", error_msg)
    
    def create_inspector_graph(self):
        """検査員の検査時間集計グラフを作成"""
        try:
            if self.current_inspector_data is None:
                self.log_message("グラフ表示するデータがありません")
                return
            
            # 既存のグラフを削除
            self.hide_inspector_graph()
            
            # 検査員の検査時間を集計
            inspector_hours = {}
            
            for _, row in self.current_inspector_data.iterrows():
                for i in range(1, 6):  # 検査員1〜5
                    inspector_col = f'検査員{i}'
                    if pd.notna(row[inspector_col]) and str(row[inspector_col]).strip() != '':
                        inspector_name = str(row[inspector_col])
                        # スキル値や(新)を除去して氏名のみを取得
                        if '(' in inspector_name:
                            inspector_name = inspector_name.split('(')[0].strip()
                        
                        # 分割検査時間を取得
                        divided_time = row.get('分割検査時間', 0)
                        if pd.notna(divided_time):
                            if inspector_name in inspector_hours:
                                inspector_hours[inspector_name] += float(divided_time)
                            else:
                                inspector_hours[inspector_name] = float(divided_time)
            
            if not inspector_hours:
                self.log_message("グラフ表示するデータがありません")
                return
            
            # グラフフレームを作成
            self.graph_frame = ctk.CTkFrame(self.main_scroll_frame, fg_color="#F8FAFC", corner_radius=12)
            self.graph_frame.pack(fill="x", padx=20, pady=(0, 20))
            
            # グラフタイトル
            graph_title = ctk.CTkLabel(
                self.graph_frame,
                text="検査員別検査時間集計",
                font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
                text_color="#1E3A8A"
            )
            graph_title.pack(pady=(15, 10))
            
            # matplotlibの設定
            plt.rcParams['font.family'] = 'Yu Gothic'
            plt.rcParams['axes.unicode_minus'] = False
            
            # グラフを作成
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # データをソート（検査時間の降順）
            sorted_inspectors = sorted(inspector_hours.items(), key=lambda x: x[1], reverse=True)
            names = [item[0] for item in sorted_inspectors]
            hours = [item[1] for item in sorted_inspectors]
            
            # バーチャートを作成
            bars = ax.bar(range(len(names)), hours, color='#3B82F6', alpha=0.8)
            
            # グラフの設定
            ax.set_xlabel('検査員', fontsize=12)
            ax.set_ylabel('検査時間 (時間)', fontsize=12)
            ax.set_title('検査員別検査時間集計', fontsize=14, fontweight='bold')
            ax.set_xticks(range(len(names)))
            ax.set_xticklabels(names, rotation=45, ha='right')
            
            # グリッドを追加
            ax.grid(True, alpha=0.3)
            
            # 各バーの上に値を表示
            for i, (bar, hour) in enumerate(zip(bars, hours)):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                       f'{hour:.1f}h', ha='center', va='bottom', fontsize=10)
            
            # レイアウトを調整
            plt.tight_layout()
            
            # Tkinterに埋め込み
            canvas = FigureCanvasTkAgg(fig, self.graph_frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True, padx=15, pady=(0, 15))
            
            self.log_message(f"検査員別検査時間集計グラフを表示しました: {len(inspector_hours)}人")
            
        except Exception as e:
            error_msg = f"グラフ作成中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            messagebox.showerror("エラー", error_msg)
    
    def hide_inspector_graph(self):
        """検査員グラフを非表示にする"""
        try:
            if self.graph_frame is not None:
                self.graph_frame.destroy()
                self.graph_frame = None
        except Exception as e:
            logger.error(f"グラフ非表示中にエラーが発生しました: {str(e)}")
    
    def show_detail_popup(self):
        """詳細表示ポップアップを表示"""
        try:
            # 既存のポップアップがあれば閉じる
            if hasattr(self, 'detail_popup') and self.detail_popup is not None:
                self.detail_popup.destroy()
            
            # ポップアップウィンドウを作成
            self.detail_popup = ctk.CTkToplevel(self.root)
            self.detail_popup.title("検査員詳細表示")
            self.detail_popup.geometry("1200x800")
            self.detail_popup.resizable(True, True)
            
            # メインフレーム
            main_frame = ctk.CTkFrame(self.detail_popup)
            main_frame.pack(fill="both", expand=True, padx=10, pady=10)
            
            # タイトル
            title_label = ctk.CTkLabel(
                main_frame,
                text="検査員詳細表示",
                font=ctk.CTkFont(family="Yu Gothic", size=20, weight="bold")
            )
            title_label.pack(pady=(10, 20))
            
            # タブビューを作成
            tabview = ctk.CTkTabview(main_frame)
            tabview.pack(fill="both", expand=True, padx=10, pady=(0, 10))
            
            # グラフタブ
            tabview.add("グラフ表示")
            graph_frame = tabview.tab("グラフ表示")
            self.create_detail_graph(graph_frame)
            
            # ロット一覧タブ
            tabview.add("ロット一覧")
            lot_frame = tabview.tab("ロット一覧")
            self.create_inspector_lot_list(lot_frame)
            
            # 閉じるボタン
            close_button = ctk.CTkButton(
                main_frame,
                text="閉じる",
                command=self.close_detail_popup,
                width=100,
                height=35,
                font=ctk.CTkFont(family="Yu Gothic", size=12),
                fg_color="#6B7280",
                hover_color="#4B5563"
            )
            close_button.pack(pady=(10, 10))
            
        except Exception as e:
            logger.error(f"詳細表示ポップアップ作成中にエラーが発生しました: {str(e)}")
            messagebox.showerror("エラー", f"詳細表示ポップアップ作成中にエラーが発生しました: {str(e)}")
    
    def close_detail_popup(self):
        """詳細表示ポップアップを閉じる"""
        try:
            if hasattr(self, 'detail_popup') and self.detail_popup is not None:
                self.detail_popup.destroy()
                self.detail_popup = None
        except Exception as e:
            logger.error(f"詳細表示ポップアップ閉じる中にエラーが発生しました: {str(e)}")
    
    def create_detail_graph(self, parent):
        """詳細表示用のグラフを作成"""
        try:
            if self.current_inspector_data is None:
                return
            
            # 検査員の検査時間集計を取得（実際に割り当てられた検査員のみ）
            inspector_hours = {}
            for _, row in self.current_inspector_data.iterrows():
                # 検査員1～5を確認
                for i in range(1, 6):
                    inspector_col = f'検査員{i}'
                    if inspector_col in row and pd.notna(row[inspector_col]):
                        inspector_name = str(row[inspector_col]).strip()
                        
                        # 空文字列をスキップ
                        if not inspector_name:
                            continue
                        
                        # スキル値や(新)を除去して名前のみ取得
                        if '(' in inspector_name:
                            inspector_name = inspector_name.split('(')[0].strip()
                        
                        # 名前が空の場合はスキップ
                        if not inspector_name:
                            continue
                        
                        if inspector_name not in inspector_hours:
                            inspector_hours[inspector_name] = 0.0
                        
                        # 分割検査時間を加算
                        if '分割検査時間' in row and pd.notna(row['分割検査時間']):
                            inspector_hours[inspector_name] += float(row['分割検査時間'])
            
            if not inspector_hours:
                no_data_label = ctk.CTkLabel(
                    parent,
                    text="検査員データがありません",
                    font=ctk.CTkFont(family="Yu Gothic", size=14)
                )
                no_data_label.pack(expand=True)
                return
            
            # グラフ用のフレーム
            graph_frame = ctk.CTkFrame(parent)
            graph_frame.pack(fill="both", expand=True, padx=10, pady=10)
            
            # matplotlibの設定
            plt.rcParams['font.family'] = 'Yu Gothic'
            plt.rcParams['axes.unicode_minus'] = False
            
            # グラフを作成
            fig, ax = plt.subplots(figsize=(12, 8))
            
            # データをソート（時間の降順）
            sorted_inspectors = sorted(inspector_hours.items(), key=lambda x: x[1], reverse=True)
            names = [item[0] for item in sorted_inspectors]
            hours = [item[1] for item in sorted_inspectors]
            
            # バーチャートを作成
            bars = ax.bar(range(len(names)), hours, color='#10B981', alpha=0.8)
            
            # グラフの設定
            ax.set_xlabel('検査員', fontsize=12)
            ax.set_ylabel('検査時間 (時間)', fontsize=12)
            ax.set_title('検査員別検査時間集計', fontsize=14, fontweight='bold')
            ax.set_xticks(range(len(names)))
            ax.set_xticklabels(names, rotation=45, ha='right')
            
            # グリッドを追加
            ax.grid(True, alpha=0.3)
            
            # 数値をバーの上に表示
            for i, (bar, hour) in enumerate(zip(bars, hours)):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                       f'{hour:.1f}h', ha='center', va='bottom', fontsize=10)
            
            plt.tight_layout()
            
            # Tkinterに埋め込み
            canvas = FigureCanvasTkAgg(fig, graph_frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True)
            
        except Exception as e:
            logger.error(f"詳細グラフ作成中にエラーが発生しました: {str(e)}")
            error_label = ctk.CTkLabel(
                parent,
                text=f"グラフ作成中にエラーが発生しました: {str(e)}",
                font=ctk.CTkFont(family="Yu Gothic", size=12),
                text_color="red"
            )
            error_label.pack(expand=True)
    
    def create_inspector_lot_list(self, parent):
        """検査員ごとのロット一覧を作成"""
        try:
            if self.current_inspector_data is None:
                no_data_label = ctk.CTkLabel(
                    parent,
                    text="検査員データがありません",
                    font=ctk.CTkFont(family="Yu Gothic", size=14)
                )
                no_data_label.pack(expand=True)
                return
            
            # 検査員ごとのロットデータを整理（実際に割り当てられた検査員のみ）
            inspector_lots = {}
            for _, row in self.current_inspector_data.iterrows():
                # 検査員1～5を確認
                for i in range(1, 6):
                    inspector_col = f'検査員{i}'
                    if inspector_col in row and pd.notna(row[inspector_col]):
                        inspector_name = str(row[inspector_col]).strip()
                        
                        # 空文字列をスキップ
                        if not inspector_name:
                            continue
                        
                        # スキル値や(新)を除去して名前のみ取得
                        if '(' in inspector_name:
                            inspector_name = inspector_name.split('(')[0].strip()
                        
                        # 名前が空の場合はスキップ
                        if not inspector_name:
                            continue
                        
                        if inspector_name not in inspector_lots:
                            inspector_lots[inspector_name] = []
                        
                        # ロット情報を追加
                        lot_info = {
                            '生産ロットID': row.get('生産ロットID', ''),
                            '指示日': row.get('指示日', ''),
                            '出荷予定日': row.get('出荷予定日', ''),
                            'ロット数量': row.get('ロット数量', ''),
                            '分割検査時間': row.get('分割検査時間', ''),
                            '品番': row.get('品番', ''),
                            '品名': row.get('品名', ''),
                            'チーム情報': row.get('チーム情報', '')
                        }
                        inspector_lots[inspector_name].append(lot_info)
            
            if not inspector_lots:
                no_data_label = ctk.CTkLabel(
                    parent,
                    text="ロットデータがありません",
                    font=ctk.CTkFont(family="Yu Gothic", size=14)
                )
                no_data_label.pack(expand=True)
                return
            
            # 日付フォーマット関数（全データ処理の前に定義）
            def format_date(date_value):
                """日付をyyyy/mm/dd形式に変換"""
                if pd.isna(date_value) or date_value == '' or date_value is None:
                    return ''
                try:
                    # pandasのTimestamp型または文字列を処理
                    if isinstance(date_value, pd.Timestamp):
                        return date_value.strftime('%Y/%m/%d')
                    elif isinstance(date_value, str):
                        # 文字列の場合、まずパースを試みる
                        parsed_date = pd.to_datetime(date_value, errors='coerce')
                        if pd.notna(parsed_date):
                            return parsed_date.strftime('%Y/%m/%d')
                        return str(date_value)
                    else:
                        # その他の型（datetime等）を処理
                        from datetime import datetime
                        if isinstance(date_value, datetime):
                            return date_value.strftime('%Y/%m/%d')
                        return str(date_value)
                except Exception:
                    return str(date_value)
            
            # すべてのロットデータを収集して列幅を計算
            headers = ['生産ロットID', '指示日', '出荷予定日', '品番', '品名', 'ロット数量', '検査時間', 'チーム情報']
            
            # フォントメトリクスで実際の幅を測定するためのフォントオブジェクト
            font_data = tk.font.Font(family="Yu Gothic", size=9)
            font_header = tk.font.Font(family="Yu Gothic", size=10, weight="bold")
            
            # 各列の最大幅を計算
            column_widths = {}
            for header in headers:
                # ヘッダーの幅を測定（フォントメトリクスを使用）
                header_width = font_header.measure(header)
                max_width = header_width
                
                # すべてのロットデータから最大幅を計算
                for inspector_name, lots in inspector_lots.items():
                    for lot in lots:
                        if header == '生産ロットID':
                            value = str(lot.get('生産ロットID', ''))
                        elif header == '指示日':
                            value = format_date(lot.get('指示日', ''))
                        elif header == '出荷予定日':
                            value = format_date(lot.get('出荷予定日', ''))
                        elif header == '品番':
                            value = str(lot.get('品番', ''))
                        elif header == '品名':
                            value = str(lot.get('品名', ''))
                        elif header == 'ロット数量':
                            value = str(lot.get('ロット数量', ''))
                        elif header == '検査時間':
                            value = f"{lot.get('分割検査時間', 0):.1f}h" if pd.notna(lot.get('分割検査時間', 0)) else "0h"
                        elif header == 'チーム情報':
                            value = str(lot.get('チーム情報', ''))
                        else:
                            value = ''
                        
                        # 実際の文字列幅を測定（フォントメトリクスを使用）
                        if value:
                            value_width = font_data.measure(value)
                            # パディングを追加（左右10ピクセルずつ）
                            value_width += 20
                            
                            if value_width > max_width:
                                max_width = value_width
                
                # 最小幅と最大幅の制限
                min_width = header_width + 20  # ヘッダー文字列より小さくしない
                max_width = max(min_width, max_width)
                max_width = min(max_width, 500)  # 最大500ピクセルまで（ただし品名とチーム情報はもう少し長く）
                
                if header in ['品名', 'チーム情報']:
                    max_width = min(max_width, 600)  # 長いテキスト列は600ピクセルまで
                
                column_widths[header] = max(int(max_width), 60)  # 最小60ピクセル
            
            # スクロール可能なフレーム
            scroll_frame = ctk.CTkScrollableFrame(parent)
            scroll_frame.pack(fill="both", expand=True, padx=10, pady=10)
            
            # マウスホイールイベントのバインド（詳細結果テーブル用）
            def on_detail_mousewheel(event):
                # CTkScrollableFrameのスクロール速度を他のテーブルと同等にするため、スクロール量を13倍に設定
                scroll_amount = int(-1 * (event.delta / 120)) * 13
                # CTkScrollableFrameの正しいスクロールメソッドを使用
                if hasattr(scroll_frame, 'yview_scroll'):
                    scroll_frame.yview_scroll(scroll_amount, "units")
                else:
                    # CTkScrollableFrameの場合は内部のCanvasを直接操作
                    canvas = scroll_frame._parent_canvas
                    if canvas:
                        canvas.yview_scroll(scroll_amount, "units")
                return "break"
            
            scroll_frame.bind("<MouseWheel>", on_detail_mousewheel)
            
            # ポップアップウィンドウ全体にもバインド（全画面でスクロール可能にする）
            if hasattr(self, 'detail_popup') and self.detail_popup is not None:
                self.detail_popup.bind("<MouseWheel>", on_detail_mousewheel)
                # ポップアップ内のすべてのウィジェットにもバインド
                def bind_detail_scroll_to_children(widget):
                    """再帰的に子ウィジェットにマウスホイールイベントをバインド"""
                    try:
                        widget.bind("<MouseWheel>", on_detail_mousewheel)
                        for child in widget.winfo_children():
                            bind_detail_scroll_to_children(child)
                    except:
                        pass
                
                # ポップアップウィンドウの子ウィジェットにバインド
                bind_detail_scroll_to_children(self.detail_popup)
            
            # 検査員ごとにセクションを作成
            for inspector_name, lots in inspector_lots.items():
                # 検査員セクション
                inspector_section = ctk.CTkFrame(scroll_frame)
                inspector_section.pack(fill="x", padx=5, pady=5)
                
                # 検査員名とロット数
                total_hours = sum(lot.get('分割検査時間', 0) for lot in lots if pd.notna(lot.get('分割検査時間', 0)))
                header_text = f"{inspector_name} ({len(lots)}ロット, 合計: {total_hours:.1f}時間)"
                header_label = ctk.CTkLabel(
                    inspector_section,
                    text=header_text,
                    font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                    fg_color="#E5E7EB"
                )
                header_label.pack(fill="x", padx=5, pady=5)
                
                # ロット一覧テーブル（スクロール不要、全体のスクロールを使用）
                lot_frame = tk.Frame(inspector_section, bg="white")
                lot_frame.pack(fill="x", padx=5, pady=(0, 5))
                
                # テーブルヘッダー（計算済みの列幅を使用）
                for j, header in enumerate(headers):
                    header_label = tk.Label(
                        lot_frame,
                        text=header,
                        font=ctk.CTkFont(family="Yu Gothic", size=10, weight="bold"),
                        bg="#F3F4F6",
                        relief="solid",
                        borderwidth=1,
                        anchor="center"
                    )
                    header_label.grid(row=0, column=j, sticky="ew", padx=1, pady=1)
                
                # ロットデータ行
                for i, lot in enumerate(lots):
                    row_data = [
                        str(lot.get('生産ロットID', '')),
                        format_date(lot.get('指示日', '')),
                        format_date(lot.get('出荷予定日', '')),
                        str(lot.get('品番', '')),
                        str(lot.get('品名', '')),
                        str(lot.get('ロット数量', '')),
                        f"{lot.get('分割検査時間', 0):.1f}h" if pd.notna(lot.get('分割検査時間', 0)) else "0h",
                        str(lot.get('チーム情報', ''))
                    ]
                    
                    for j, data in enumerate(row_data):
                        header = headers[j]
                        anchor_pos = "e" if header in ['ロット数量', '検査時間'] else "w"
                        data_label = tk.Label(
                            lot_frame,
                            text=data,
                            font=ctk.CTkFont(family="Yu Gothic", size=9),
                            bg="white" if i % 2 == 0 else "#F9FAFB",
                            relief="solid",
                            borderwidth=1,
                            anchor=anchor_pos,
                            wraplength=column_widths.get(header, 100) * 8  # 折り返し対応
                        )
                        data_label.grid(row=i+1, column=j, sticky="ew", padx=1, pady=1)
                
                # 列の重みと最小幅を設定（計算された最大幅を使用）
                for j, header in enumerate(headers):
                    lot_frame.grid_columnconfigure(j, weight=0, minsize=column_widths.get(header, 100))
                
                # テーブルにマウスホイールイベントをバインド（メインテーブル用）
                def on_lot_table_mousewheel(event, scroll_frame_ref=scroll_frame):
                    # CTkScrollableFrameのスクロール速度を他のテーブルと同等にするため、スクロール量を13倍に設定
                    scroll_amount = int(-1 * (event.delta / 120)) * 13
                    # CTkScrollableFrameの正しいスクロールメソッドを使用
                    if hasattr(scroll_frame_ref, 'yview_scroll'):
                        scroll_frame_ref.yview_scroll(scroll_amount, "units")
                    else:
                        # CTkScrollableFrameの場合は内部のCanvasを直接操作
                        canvas = scroll_frame_ref._parent_canvas
                        if canvas:
                            canvas.yview_scroll(scroll_amount, "units")
                    return "break"
                
                # lot_frameとその中のすべてのウィジェットにマウスホイールイベントをバインド
                lot_frame.bind("<MouseWheel>", on_lot_table_mousewheel)
                inspector_section.bind("<MouseWheel>", on_lot_table_mousewheel)
                
                # テーブル内のすべてのラベルにもバインド
                for widget in lot_frame.winfo_children():
                    widget.bind("<MouseWheel>", on_lot_table_mousewheel)
                
        except Exception as e:
            logger.error(f"ロット一覧作成中にエラーが発生しました: {str(e)}")
            error_label = ctk.CTkLabel(
                parent,
                text=f"ロット一覧作成中にエラーが発生しました: {str(e)}",
                font=ctk.CTkFont(family="Yu Gothic", size=12),
                text_color="red"
            )
            error_label.pack(expand=True)
    
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
    
    def quit_application(self):
        """アプリケーションを完全に終了する"""
        try:
            # ログ出力
            logger.info("アプリケーションを終了しています...")
            
            # カレンダーウィンドウを閉じる
            if hasattr(self, 'calendar_window') and self.calendar_window is not None:
                self.calendar_window.destroy()
                self.calendar_window = None
            
            # グラフフレームを破棄
            if hasattr(self, 'graph_frame') and self.graph_frame is not None:
                self.graph_frame.destroy()
                self.graph_frame = None
            
            # メインウィンドウを破棄
            if hasattr(self, 'root'):
                self.root.quit()  # mainloopを終了
                self.root.destroy()  # ウィンドウを破棄
            
            # プロセスを強制終了（最後の手段）
            import os
            import sys
            os._exit(0)
            
        except Exception as e:
            logger.error(f"アプリケーション終了中にエラーが発生しました: {e}")
            # エラーが発生しても強制終了
            import os
            os._exit(0)
    
    
    def start_inspector_assignment(self):
        """検査員割振りを開始"""
        try:
            if self.current_assignment_data is None or self.current_assignment_data.empty:
                messagebox.showwarning("警告", "ロット割り当て結果がありません。\n先にデータを抽出してください。")
                return
            
            # 製品マスタファイルを読み込み
            product_master_df = self.load_product_master()
            if product_master_df is None:
                return
            
            # 検査員割振りテーブルを作成（製品マスタパスを渡す）
            product_master_path = self.config.product_master_path if self.config else None
            inspector_df = self.inspector_manager.create_inspector_assignment_table(self.current_assignment_data, product_master_df, product_master_path)
            if inspector_df is None:
                return
            
            # 検査員割振りテーブルを表示
            self.display_inspector_assignment_table(inspector_df)
            
            # データを保存（エクスポート用）
            self.current_inspector_data = inspector_df
            
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
                self.update_button_states("main")
            elif table_type == "assignment" and self.current_assignment_data is not None:
                self.display_lot_assignment_table(self.current_assignment_data)
                self.current_display_table = "assignment"
                self.update_button_states("assignment")
            elif table_type == "inspector" and self.current_inspector_data is not None:
                self.display_inspector_assignment_table(self.current_inspector_data)
                self.current_display_table = "inspector"
                self.update_button_states("inspector")
            else:
                self.log_message(f"{table_type}テーブルのデータがありません")
                
        except Exception as e:
            error_msg = f"テーブル表示中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
    
    def hide_current_table(self):
        """現在表示中のテーブルを非表示にする"""
        try:
            # 既存のテーブルセクションを削除
            for widget in self.main_scroll_frame.winfo_children():
                if hasattr(widget, 'table_section'):
                    widget.destroy()
        except Exception as e:
            logger.error(f"テーブル非表示中にエラーが発生しました: {str(e)}")
    
    def update_button_states(self, active_table):
        """テーブル選択ボタンの状態を更新"""
        try:
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
                # 時刻フォーマットを試行
                df['開始時刻'] = pd.to_datetime(df['開始時刻'], format='%H:%M').dt.time
                df['終了時刻'] = pd.to_datetime(df['終了時刻'], format='%H:%M').dt.time
                
                # 就業時間を計算
                df['就業時間'] = pd.to_datetime(df['終了時刻'].astype(str)) - pd.to_datetime(df['開始時刻'].astype(str))
                df['就業時間'] = df['就業時間'].dt.total_seconds() / 3600 - 1  # 休憩1時間を引く
                
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
            self.log_message(f"検査対象キーワード: {keywords}")
            
            return keywords
            
        except Exception as e:
            error_msg = f"検査対象CSVの読み込みに失敗しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            # エラー時も空リストを返して処理を継続
            return []
    
    
    
    
    
    def export_selected_table(self):
        """選択されたテーブルをExcel出力"""
        try:
            if self.current_display_table is None:
                messagebox.showwarning("警告", "表示中のテーブルがありません。\n先にテーブルを選択してください。")
                return
            
            if self.current_display_table == "main" and self.current_main_data is not None:
                self.excel_exporter.export_main_data_to_excel(self.current_main_data)
            elif self.current_display_table == "assignment" and self.current_assignment_data is not None:
                self.excel_exporter.export_lot_assignment_to_excel(self.current_assignment_data)
            elif self.current_display_table == "inspector" and self.current_inspector_data is not None:
                self.excel_exporter.export_inspector_assignment_to_excel(self.current_inspector_data)
            else:
                messagebox.showwarning("警告", "エクスポート可能なデータがありません。")
                
        except Exception as e:
            error_msg = f"Excel出力中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            messagebox.showerror("エラー", error_msg)


def main():
    """メイン関数"""
    try:
        app = ModernDataExtractorUI()
        app.run()
    except Exception as e:
        logger.error(f"アプリケーションの起動に失敗しました: {e}")
        messagebox.showerror("エラー", f"アプリケーションの起動に失敗しました:\n{str(e)}")


if __name__ == "__main__":
    main()
# 使用例:
# ui = ModernDataExtractorUI()
# ui.run()
