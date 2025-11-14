"""
外観検査振分支援システム - メインUI
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
import sys
from pathlib import Path
import json
from loguru import logger
from app.config import DatabaseConfig
import calendar
import locale
from app.export.excel_exporter_service import ExcelExporter
from app.export.google_sheets_exporter_service import GoogleSheetsExporter
from app.assignment.inspector_assignment_service import InspectorAssignmentManager
from app.services.cleaning_request_service import get_cleaning_lots
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.font_manager as fm
from PIL import Image


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
        self.root.title("外観検査振分支援システム")
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
        
        # 当日検査品入力用の変数
        self.product_code_entry = None  # 品番入力フィールド
        self.inspectable_lots_entry = None  # 検査可能ロット数／日入力フィールド
        self.register_button = None  # 登録確定ボタン
        self.registered_products = []  # 登録された品番のリスト [{品番, ロット数}, ...]
        self.registered_products_frame = None  # 登録リスト表示フレーム
        self.registered_list_container = None  # 登録リストコンテナ
        
        # 登録済み品番リストの保存ファイルパス
        self.registered_products_file = Path(__file__).parent.parent.parent / "registered_products.json"
        
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
        
        # 休暇情報テーブル用の変数
        self.vacation_info_frame = None
        
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

        # メインスクロールのバインド状態
        self._main_scroll_bound = False

        # UIの構築
        self.setup_ui()
        
        # ログ設定
        self.setup_logging()
        
        # ウィンドウのアイコンを設定（exe化対応、ログ設定後に実行）
        try:
            icon_path = self._get_icon_path("appearance_sorting_system.ico")
            logger.info(f"アイコンパス解決結果: {icon_path}")
            logger.info(f"アイコンファイル存在確認: {Path(icon_path).exists() if icon_path else 'None'}")
            
            if icon_path and Path(icon_path).exists():
                # CustomTkinterのCTkウィンドウでは、Tkinterの標準メソッドを使用
                # root.tkでTkinterのルートウィンドウにアクセス
                try:
                    self.root.tk.call('wm', 'iconbitmap', self.root._w, icon_path)
                    logger.info(f"ウィンドウアイコンを設定しました: {icon_path}")
                except Exception as icon_error:
                    # iconbitmapが失敗した場合、iconphotoを試す
                    try:
                        from PIL import Image
                        import tkinter as tk
                        icon_photo = tk.PhotoImage(file=icon_path)
                        self.root.iconphoto(False, icon_photo)
                        logger.info(f"ウィンドウアイコンを設定しました（iconphoto使用）: {icon_path}")
                    except Exception as iconphoto_error:
                        logger.warning(f"iconbitmapとiconphotoの両方が失敗しました: {icon_error}, {iconphoto_error}")
            else:
                logger.warning(f"アイコンファイルが見つかりません: {icon_path}")
        except Exception as e:
            logger.warning(f"ウィンドウアイコンの設定に失敗しました: {e}", exc_info=True)
        
        # 設定の読み込み
        self.load_config()
        
        # 登録済み品番リストの読み込み
        self.load_registered_products()
        
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
                scroll_steps = base_steps * 10  # スクロールを速くする
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
    
    def setup_logging(self, execution_id: str = None):
        """ログ設定
        
        Args:
            execution_id: 実行ID（指定された場合、そのIDを含むファイル名でログを作成）
        """
        from pathlib import Path
        from datetime import datetime
        import sys
        
        logger.remove()  # デフォルトのハンドラーを削除
        
        # exe化されている場合とそうでない場合でログディレクトリを決定
        if getattr(sys, 'frozen', False):
            # exe化されている場合：exeファイルの場所を基準にする
            application_path = Path(sys.executable).parent
        else:
            # 通常のPython実行の場合：スクリプトの場所を基準にする
            application_path = Path(__file__).parent.parent.parent
        
        # ログディレクトリを作成
        log_dir = application_path / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # ログファイルのパス
        if execution_id:
            # 実行ごとにファイルを作成（日時を含む）
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = log_dir / f"app_{timestamp}_{execution_id}.log"
        else:
            # 初期起動時は日付ごとにファイルを分ける（後方互換性のため）
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
        if getattr(sys, 'frozen', False):
            temp_dir = Path(sys._MEIPASS)
            temp_file = temp_dir / icon_filename
            if temp_file.exists():
                logger.info(f"アイコンファイルを一時ディレクトリから使用しました: {temp_file}")
                return str(temp_file)

            exe_dir = Path(sys.executable).parent
            exe_file = exe_dir / icon_filename
            if exe_file.exists():
                logger.info(f"アイコンファイルを実行ファイルのディレクトリから使用しました: {exe_file}")
                return str(exe_file)

            logger.warning(f"アイコンファイルが見つかりませんでした: {icon_filename}")
            return icon_filename
        else:
            script_dir = Path(__file__).parent.parent.parent
            icon_path = script_dir / icon_filename
            if icon_path.exists():
                logger.info(f"アイコンファイルを読み込みました: {icon_path}")
                return str(icon_path)
            return icon_filename
    
    def _get_image_path(self, image_filename: str) -> str:
        """
        画像ファイルのパスを解決（exe化対応）
        
        Args:
            image_filename: 画像ファイル名
            
        Returns:
            解決された画像ファイルのパス
        """
        if getattr(sys, 'frozen', False):
            # exe化されている場合
            # まず一時ディレクトリ（sys._MEIPASS）を確認（埋め込まれたファイル）
            temp_dir = Path(sys._MEIPASS)
            temp_file = temp_dir / image_filename
            if temp_file.exists():
                return str(temp_file)
            
            # 次にexeと同じ階層を確認
            exe_dir = Path(sys.executable).parent
            exe_file = exe_dir / image_filename
            if exe_file.exists():
                return str(exe_file)
            
            # 見つからない場合は元のパスを返す
            return image_filename
        else:
            # 通常のPython実行の場合：スクリプトの場所を基準にする
            script_dir = Path(__file__).parent.parent.parent
            image_path = script_dir / image_filename
            if image_path.exists():
                return str(image_path)
            return image_filename
    
    def create_title_section(self, parent):
        """タイトルセクションの作成"""
        title_frame = ctk.CTkFrame(parent, height=70, fg_color="white", corner_radius=0)
        title_frame.pack(fill="x", pady=(5, 15))  # 上部の余白を5pxに削減
        title_frame.pack_propagate(False)
        
        # タイトルと画像を中央配置するコンテナ
        title_container = ctk.CTkFrame(title_frame, fg_color="white", corner_radius=0)
        title_container.place(relx=0.5, rely=0.5, anchor="center")  # 中央配置
        
        # 画像を読み込む
        image_filename = "ChatGPT Image 2025年11月13日 16_05_27.png"
        image_path = self._get_image_path(image_filename)
        
        try:
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
        except Exception as e:
            logger.warning(f"画像の読み込みに失敗しました: {e}")
            # 画像が読み込めない場合は画像なしで続行
        
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
        
        product_code_label = ctk.CTkLabel(
            product_code_frame,
            text="品番（製品マスタと完全一致）",
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            text_color="#374151"
        )
        product_code_label.pack(anchor="w", pady=(0, 4))
        
        self.product_code_entry = ctk.CTkEntry(
            product_code_frame,
            placeholder_text="品番を入力",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=40,
            border_width=1,
            fg_color="white",
            text_color="#374151"
        )
        self.product_code_entry.pack(fill="x")
        
        # 検査可能ロット数／日入力セクション
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
        self.product_code_entry.bind("<KeyRelease>", self.check_input_fields)
        self.inspectable_lots_entry.bind("<KeyRelease>", self.check_input_fields)
        self.product_code_entry.bind("<FocusOut>", self.check_input_fields)
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
        lots = self.inspectable_lots_entry.get().strip()
        
        # 入力チェック
        if not product_code or not lots:
            return
        
        # 既に登録されているかチェック
        for item in self.registered_products:
            if item['品番'] == product_code:
                # 既に登録されている場合は更新
                item['ロット数'] = lots
                # 検査員情報がない場合は初期化
                if '固定検査員' not in item:
                    item['固定検査員'] = []
                self.update_registered_list()
                # ファイルに保存
                self.save_registered_products()
                # 入力フィールドをクリア
                self.product_code_entry.delete(0, "end")
                self.inspectable_lots_entry.delete(0, "end")
                self.check_input_fields()
                return
        
        # 新規登録
        self.registered_products.append({
            '品番': product_code,
            'ロット数': lots,
            '固定検査員': []  # 検査員固定情報を追加
        })
        
        # リストを更新
        self.update_registered_list()
        
        # ファイルに保存
        self.save_registered_products()
        
        # 入力フィールドをクリア
        self.product_code_entry.delete(0, "end")
        self.inspectable_lots_entry.delete(0, "end")
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
            
            item_frame = ctk.CTkFrame(self.registered_list_container, fg_color="#F3F4F6", corner_radius=6)
            item_frame.pack(fill="x", pady=(0, 4), padx=5)
            
            # 情報表示フレーム（一行で表示）
            info_frame = ctk.CTkFrame(item_frame, fg_color="transparent")
            info_frame.pack(side="left", fill="x", expand=True, padx=10, pady=6)
            
            # 一行で表示するフレーム
            single_row = ctk.CTkFrame(info_frame, fg_color="transparent")
            single_row.pack(fill="x")
            
            # 品番ラベル
            product_label = ctk.CTkLabel(
                single_row,
                text="品番：",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151",
                anchor="w"
            )
            product_label.pack(side="left")
            
            # 品番の値（固定幅で位置を揃える）
            product_value = ctk.CTkLabel(
                single_row,
                text=item['品番'],
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151",
                width=150,
                anchor="w"
            )
            product_value.pack(side="left")
            
            # 検査可能ロット数／日のラベル
            lots_label = ctk.CTkLabel(
                single_row,
                text="検査可能ロット数／日：",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151",
                anchor="w"
            )
            lots_label.pack(side="left")
            
            # ロット数の値
            lots_value = ctk.CTkLabel(
                single_row,
                text=f"{item['ロット数']}ロット",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151",
                anchor="w"
            )
            lots_value.pack(side="left")
            
            # 固定検査員の表示
            fixed_inspectors_label = ctk.CTkLabel(
                single_row,
                text="固定検査員：",
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#374151",
                anchor="w"
            )
            fixed_inspectors_label.pack(side="left", padx=(20, 5))
            
            # 固定検査員の値（テーブルに入りきるまで表示）
            # 表示可能な幅を計算（より広い幅を確保、日本語1文字あたり約12pxを想定）
            max_display_width = 900  # ピクセル単位の最大表示幅（拡大）
            char_width = 12  # 日本語1文字あたりの幅（概算、より小さめに設定）
            max_chars = max_display_width // char_width  # 約75文字分
            
            if item['固定検査員']:
                # 表示可能な検査員名を動的に計算
                displayed_names = []
                current_length = 0
                ellipsis_length = len(" ... (+99)")  # 省略記号の長さ（最大件数を考慮）
                
                for inspector_name in item['固定検査員']:
                    # 検査員名の長さ（カンマとスペースを含む）
                    name_length = len(inspector_name) + 2 if displayed_names else len(inspector_name)
                    
                    # 省略記号を含めた場合の長さ
                    if displayed_names:
                        total_length_with_ellipsis = current_length + name_length + ellipsis_length
                    else:
                        total_length_with_ellipsis = name_length + ellipsis_length
                    
                    # 表示可能な範囲内かチェック（より寛容に）
                    if total_length_with_ellipsis <= max_chars:
                        displayed_names.append(inspector_name)
                        current_length += name_length
                    else:
                        # これ以上表示できない場合は省略
                        break
                
                if displayed_names:
                    fixed_inspectors_text = ", ".join(displayed_names)
                    remaining_count = len(item['固定検査員']) - len(displayed_names)
                    if remaining_count > 0:
                        fixed_inspectors_text += f" ... (+{remaining_count})"
                else:
                    # 最初の1名も表示できない場合は件数のみ表示
                    fixed_inspectors_text = f"... (+{len(item['固定検査員'])})"
            else:
                fixed_inspectors_text = "未設定"
            
            fixed_inspectors_value = ctk.CTkLabel(
                single_row,
                text=fixed_inspectors_text,
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#059669" if item['固定検査員'] else "#6B7280",
                anchor="w"
            )
            fixed_inspectors_value.pack(side="left", fill="x", expand=True)
            
            # ボタンフレーム
            button_frame = ctk.CTkFrame(item_frame, fg_color="transparent")
            button_frame.pack(side="right", padx=10, pady=6)
            
            # 検査員固定ボタン
            inspector_button = ctk.CTkButton(
                button_frame,
                text="検査員固定",
                command=lambda idx=idx: self.fix_inspectors_for_product(idx),
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                width=100,
                height=32,
                fg_color="#10B981" if item['固定検査員'] else "#6B7280",
                hover_color="#059669" if item['固定検査員'] else "#4B5563",
                text_color="white"
            )
            inspector_button.pack(side="left", padx=(0, 5))
            
            # 削除ボタン
            delete_button = ctk.CTkButton(
                button_frame,
                text="登録削除",
                command=lambda idx=idx: self.delete_registered_product(idx),
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                width=100,
                height=32,
                fg_color="#EF4444",
                hover_color="#DC2626",
                text_color="white"
            )
            delete_button.pack(side="left")
    
    def delete_registered_product(self, index):
        """登録された品番を削除"""
        if 0 <= index < len(self.registered_products):
            self.registered_products.pop(index)
            self.update_registered_list()
            # ファイルに保存
            self.save_registered_products()
    
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
    
    def _set_fixed_inspectors_to_manager(self):
        """登録済み品番の固定検査員情報をInspectorAssignmentManagerに設定"""
        try:
            if not hasattr(self, 'inspector_manager') or self.inspector_manager is None:
                return
            
            # 固定検査員情報を辞書形式で構築
            fixed_inspectors_dict = {}
            for item in self.registered_products:
                product_number = item.get('品番', '')
                fixed_inspectors = item.get('固定検査員', [])
                if product_number and fixed_inspectors:
                    fixed_inspectors_dict[product_number] = fixed_inspectors
            
            # InspectorAssignmentManagerに設定
            self.inspector_manager.fixed_inspectors_by_product = fixed_inspectors_dict
            
            if fixed_inspectors_dict:
                self.log_message(f"固定検査員情報を設定しました: {len(fixed_inspectors_dict)}品番")
                for product, inspectors in fixed_inspectors_dict.items():
                    self.log_message(f"  品番 '{product}': {', '.join(inspectors)}")
            else:
                self.log_message("固定検査員情報は設定されていません")
                
        except Exception as e:
            self.log_message(f"固定検査員情報の設定に失敗しました: {str(e)}")
            logger.error(f"固定検査員情報の設定に失敗しました: {str(e)}", exc_info=True)
    
    def load_registered_products(self):
        """登録済み品番リストをファイルから読み込む"""
        try:
            if self.registered_products_file.exists():
                with open(self.registered_products_file, 'r', encoding='utf-8') as f:
                    self.registered_products = json.load(f)
                # 後方互換性: 検査員情報がない場合は初期化
                for item in self.registered_products:
                    if '固定検査員' not in item:
                        item['固定検査員'] = []
                # UIが構築されている場合はリストを更新
                if self.registered_list_container is not None:
                    self.update_registered_list()
                logger.info(f"登録済み品番リストを読み込みました: {len(self.registered_products)}件")
        except Exception as e:
            logger.error(f"登録済み品番リストの読み込みに失敗しました: {str(e)}")
            self.registered_products = []
    
    def save_registered_products(self):
        """登録済み品番リストをファイルに保存"""
        try:
            with open(self.registered_products_file, 'w', encoding='utf-8') as f:
                json.dump(self.registered_products, f, ensure_ascii=False, indent=2)
            logger.debug(f"登録済み品番リストを保存しました: {len(self.registered_products)}件")
        except Exception as e:
            logger.error(f"登録済み品番リストの保存に失敗しました: {str(e)}")
    
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
            placeholder_text="YYYY/MM/DD",
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
        
        # ボタンフレーム（左右配置用）
        buttons_frame = ctk.CTkFrame(button_frame, fg_color="transparent")
        buttons_frame.pack(expand=True, fill="x", pady=5)
        
        # 左側のボタングループ（主要操作）
        left_buttons_frame = ctk.CTkFrame(buttons_frame, fg_color="transparent")
        left_buttons_frame.pack(side="left", expand=True, fill="x")
        
        # 右側のボタングループ（出力・終了）
        right_buttons_frame = ctk.CTkFrame(buttons_frame, fg_color="transparent")
        right_buttons_frame.pack(side="right", expand=True, fill="x")
        
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
        
        # アプリ終了ボタン（右側）
        self.exit_button = ctk.CTkButton(
            right_buttons_frame,
            text="アプリ終了",
            command=self.quit_application,
            font=ctk.CTkFont(family="Yu Gothic", size=15, weight="bold"),
            height=45,
            width=120,
            fg_color="#EF4444",
            hover_color="#DC2626",
            corner_radius=10,
            border_width=0,
            text_color="white"
        )
        self.exit_button.pack(side="right", padx=(15, 0))
        
        # Googleスプレッドシート出力ボタン（右側）
        self.google_sheets_button = ctk.CTkButton(
            right_buttons_frame,
            text="Googleスプレッドシートへ出力",
            command=self.export_to_google_sheets,
            font=ctk.CTkFont(family="Yu Gothic", size=15, weight="bold"),
            height=45,
            width=240,
            fg_color="#10B981",
            hover_color="#059669",
            corner_radius=10,
            border_width=0,
            text_color="white"
        )
        self.google_sheets_button.pack(side="right", padx=(0, 0))
    
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
            # データ抽出実行ごとに新しいログファイルを作成
            execution_id = f"{start_date}_{end_date}".replace("-", "").replace(" ", "_").replace(":", "")
            self.setup_logging(execution_id=execution_id)
            
            self.log_message(f"データ抽出を開始します")
            self.log_message(f"抽出期間: {start_date} ～ {end_date}")
            
            # 【追加】休暇予定を取得（データ抽出開始日付を使用）
            self.update_progress(0.01, "休暇予定を取得中...")
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
                    vacation_data = load_vacation_schedule(
                        sheets_url=vacation_sheets_url,
                        credentials_path=credentials_path,
                        year=extraction_date.year,
                        month=extraction_date.month
                    )
                    
                    # 対象日の休暇情報を取得
                    vacation_data_for_date = get_vacation_for_date(vacation_data, extraction_date)
                    
                    self.log_message(f"休暇予定を取得しました: {len(vacation_data_for_date)}名")
                    
                    # 検査員マスタを読み込む（休暇情報のフィルタリング用）
                    if inspector_master_df is None:
                        try:
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
            connection = self.config.get_connection()
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
                f"検査員割振り結果を自動表示しました"
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
            
            # テーブル構造を確認
            columns_query = f"SELECT TOP 1 * FROM [t_現品票履歴]"
            sample_df = pd.read_sql(columns_query, connection)
            
            if sample_df.empty:
                self.log_message("t_現品票履歴テーブルにデータが見つかりません")
                return pd.DataFrame()
            
            # 実際の列名を取得
            actual_columns = sample_df.columns.tolist()
            
            # 利用可能な列のみでクエリを作成
            available_columns = [col for col in actual_columns if col in [
                "品番", "数量", "指示日", "号機", "現在工程番号", "現在工程名", 
                "現在工程二次処理", "生産ロットID"
            ]]
            
            if not available_columns:
                available_columns = actual_columns
            
            columns_str = ", ".join([f"[{col}]" for col in available_columns])
            
            # 品番のリストをSQL用の文字列に変換
            product_numbers_str = "', '".join([str(pn) for pn in registered_product_numbers])
            
            # WHERE条件を構築
            where_conditions = [f"品番 IN ('{product_numbers_str}')"]
            
            # 現在工程名が存在する場合のみ条件を追加
            if "現在工程名" in available_columns:
                where_conditions.append("現在工程名 NOT LIKE '%完了%'")
                where_conditions.append("現在工程名 NOT LIKE '%梱包%'")
                
                # 検査対象.csvのキーワードでフィルタリング
                if self.inspection_target_keywords:
                    keyword_conditions = []
                    for keyword in self.inspection_target_keywords:
                        escaped_keyword = keyword.replace("'", "''").replace("%", "[%]").replace("_", "[_]")
                        keyword_conditions.append(f"現在工程名 LIKE '%{escaped_keyword}%'")
                    
                    if keyword_conditions:
                        keyword_filter = "(" + " OR ".join(keyword_conditions) + ")"
                        where_conditions.append(keyword_filter)
            
            where_clause = " AND ".join(where_conditions)
            
            # ORDER BY条件（指示日順、生産日の古い順）
            order_conditions = ["品番"]
            if "指示日" in available_columns:
                order_conditions.append("指示日 ASC")
            elif "号機" in available_columns:
                order_conditions.append("号機 ASC")
            
            order_clause = ", ".join(order_conditions)
            
            # クエリを実行
            lots_query = f"""
            SELECT {columns_str}
            FROM [t_現品票履歴]
            WHERE {where_clause}
            ORDER BY {order_clause}
            """
            
            lots_df = pd.read_sql(lots_query, connection)
            
            if lots_df.empty:
                self.log_message("登録済み品番のロットが見つかりませんでした")
                return pd.DataFrame()
            
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
            registered_lots_df = self.get_registered_products_lots(connection)
            
            if registered_lots_df.empty:
                return assignment_df
            
            # 登録済み品番ごとに処理
            additional_assignments = []
            
            for registered_item in self.registered_products:
                product_number = registered_item['品番']
                max_lots_per_day = int(registered_item['ロット数'])
                
                # 該当品番のロットを取得
                product_lots = registered_lots_df[registered_lots_df['品番'] == product_number].copy()
                
                if product_lots.empty:
                    continue
                
                # 指示日順でソート（生産日の古い順）
                if '指示日' in product_lots.columns:
                    product_lots = product_lots.copy()
                    product_lots['_指示日_ソート用'] = product_lots['指示日'].apply(
                        lambda x: str(x) if pd.notna(x) else ''
                    )
                    product_lots = product_lots.sort_values('_指示日_ソート用', na_position='last')
                    product_lots = product_lots.drop(columns=['_指示日_ソート用'])
                
                # 検査可能ロット数／日を考慮してロットを割り当て
                assigned_count = 0
                lot_cols = {col: idx for idx, col in enumerate(product_lots.columns)}
                
                for lot in product_lots.itertuples(index=False):
                    if assigned_count >= max_lots_per_day:
                        break
                    
                    # main_dfから該当品番の情報を取得
                    product_in_main = main_df[main_df['品番'] == product_number]
                    
                    if not product_in_main.empty:
                        main_row = product_in_main.iloc[0]
                    else:
                        # main_dfに存在しない場合は、ロットの情報のみを使用
                        main_row = None
                    
                    lot_quantity = int(lot[lot_cols['数量']]) if pd.notna(lot[lot_cols['数量']]) else 0
                    
                    # 出荷予定日は「先行検査」とする
                    shipping_date = "先行検査"
                    
                    assignment_result = {
                        '出荷予定日': shipping_date,
                        '品番': product_number,
                        '品名': main_row.get('品名', '') if main_row is not None else '',
                        '客先': main_row.get('客先', '') if main_row is not None else '',
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
            
            assignment_results = []
            
            # 品番ごとに処理
            for product_number in shortage_df['品番'].unique():
                try:
                    product_shortage = shortage_df[shortage_df['品番'] == product_number]
                    product_lots = lots_df[lots_df['品番'] == product_number].copy()
                    
                    if product_lots.empty:
                        continue
                    
                    # 指示日順でソート（型を統一してからソート）
                    if '指示日' in product_lots.columns:
                        # 指示日を文字列に統一してからソート（None/NaNは最後に）
                        product_lots = product_lots.copy()
                        product_lots['_指示日_ソート用'] = product_lots['指示日'].apply(
                            lambda x: str(x) if pd.notna(x) else ''
                        )
                        product_lots = product_lots.sort_values('_指示日_ソート用', na_position='last')
                        product_lots = product_lots.drop(columns=['_指示日_ソート用'])
                    else:
                        # 指示日列がない場合はそのまま
                        pass
                    
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
                        
                        # 出荷予定日の決定：ロットに設定されている場合はそれを使用（洗浄二次処理依頼のロット用）
                        # ロットに「出荷予定日」列があり、値が設定されている場合はそれを使用
                        if '出荷予定日' in lot_cols and pd.notna(lot[lot_cols['出荷予定日']]):
                            shipping_date = lot[lot_cols['出荷予定日']]
                        else:
                            # ロットに設定がない場合は、不足データの出荷予定日を使用
                            shipping_date = product_shortage['出荷予定日'].iloc[0]
                        
                        # 割り当て結果を記録
                        assignment_result = {
                            '出荷予定日': shipping_date,
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
                        
                except Exception as e:
                    # 個別の品番でエラーが発生しても、他の品番の処理を継続
                    self.log_message(f"品番 {product_number} のロット割り当て中にエラーが発生しました: {str(e)}")
                    continue
            
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
            
            # 通常の在庫ロットを取得
            self.update_progress(start_progress + 0.10, "利用可能なロットを取得中...")
            lots_df = self.get_available_lots_for_shortage(connection, shortage_df)
            
            # 洗浄二次処理依頼からロットを取得（追加で取得）
            cleaning_lots_df = pd.DataFrame()
            if (self.config.google_sheets_url_cleaning and 
                self.config.google_sheets_url_cleaning_instructions and 
                self.config.google_sheets_credentials_path):
                try:
                    self.update_progress(start_progress + 0.12, "洗浄二次処理依頼からロットを取得中...")
                    cleaning_lots_df = get_cleaning_lots(
                        connection,
                        self.config.google_sheets_url_cleaning,
                        self.config.google_sheets_url_cleaning_instructions,
                        self.config.google_sheets_credentials_path,
                        log_callback=self.log_message
                    )
                    if not cleaning_lots_df.empty:
                        self.log_message(f"洗浄二次処理依頼から {len(cleaning_lots_df)}件のロットを取得しました")
                except Exception as e:
                    self.log_message(f"洗浄二次処理依頼からのロット取得中にエラーが発生しました: {str(e)}")
            
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
            
            if lots_df.empty:
                self.log_message("利用可能なロットが見つかりませんでした")
                return
            
            # ロット割り当てを実行
            self.update_progress(start_progress + 0.15, "ロットを割り当て中...")
            assignment_df = self.assign_lots_to_shortage(shortage_df, lots_df)
            
            # 登録済み品番のロットを割り当て（追加）
            if self.registered_products:
                self.update_progress(start_progress + 0.17, "登録済み品番のロットを割り当て中...")
                assignment_df = self.assign_registered_products_lots(connection, main_df, assignment_df)
            
            # 洗浄二次処理依頼のロットを追加（不足数がマイナスの品番と一致するものも含む）
            if not cleaning_lots_df.empty:
                # 不足数がマイナスの品番リストを取得
                shortage_product_numbers = set(shortage_df['品番'].unique())
                
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
                    assigned_lot_ids = set(assignment_df['生産ロットID'].dropna().unique())
                
                # 不足数がマイナスの品番と一致するが、まだ割り当てられていないロットを抽出
                cleaning_lots_in_shortage_not_assigned = cleaning_lots_in_shortage[
                    ~cleaning_lots_in_shortage['生産ロットID'].isin(assigned_lot_ids)
                ].copy()
                
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
                    
                    for row_tuple in all_additional_cleaning_lots.itertuples(index=True):
                        lot_row_idx = row_tuple[0]  # インデックス
                        lot_row = all_additional_cleaning_lots.loc[lot_row_idx]  # Seriesとして扱うために元の行を取得
                        
                        # 品番がmain_dfに存在するか確認
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
                                '現在工程番号': lot_row.get('現在工程番号', ''),
                                '現在工程名': lot_row.get('現在工程名', ''),
                                '現在工程二次処理': lot_row.get('現在工程二次処理', ''),
                                '生産ロットID': lot_row.get('生産ロットID', '')
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
                                '現在工程番号': lot_row.get('現在工程番号', ''),
                                '現在工程名': lot_row.get('現在工程名', ''),
                                '現在工程二次処理': lot_row.get('現在工程二次処理', ''),
                                '生産ロットID': lot_row.get('生産ロットID', '')
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
            
            # 固定検査員情報を設定
            self._set_fixed_inspectors_to_manager()
            
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
            
            # データ抽出完了後に自動で検査員割振りテーブルを表示
            self.root.after(0, lambda: self.show_table("inspector"))
            
        except Exception as e:
            self.log_message(f"検査員割振り処理中にエラーが発生しました: {str(e)}")
    
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
            
            # 不足数を再計算
            current_product = None
            current_shortage = 0
            
            # 列名から列インデックスへのマッピングを作成（高速化：itertuples()を使用）
            product_col_idx = assignment_df.columns.get_loc('品番')
            shortage_col_idx = assignment_df.columns.get_loc('不足数')
            lot_qty_col_idx = assignment_df.columns.get_loc('ロット数量')
            
            for row_tuple in assignment_df.itertuples(index=True):
                index = row_tuple[0]  # インデックス
                product_number = row_tuple[product_col_idx + 1]  # itertuplesはインデックスを含むため+1
                shortage_value = row_tuple[shortage_col_idx + 1] if shortage_col_idx < len(row_tuple) - 1 else 0
                
                if current_product != product_number:
                    # 新しい品番の場合は初期不足数を設定
                    current_shortage = shortage_value
                    current_product = product_number
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
            logger.debug(f"製品マスタの列: {df.columns.tolist()}")
            
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
                    logger.debug(f"列名をマッピングしました: {column_mapping}")
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
    
    def display_vacation_info_table(self, vacation_data, extraction_date, inspector_master_df=None):
        """休暇情報テーブルを表示"""
        try:
            # 既存の休暇情報テーブルがあれば削除
            if hasattr(self, 'vacation_info_frame') and self.vacation_info_frame:
                try:
                    self.vacation_info_frame.destroy()
                except:
                    pass
            
            # 休暇情報セクションを作成
            vacation_frame = ctk.CTkFrame(self.main_scroll_frame, fg_color="#EFF6FF", corner_radius=12)
            vacation_frame.table_section = True
            vacation_frame.vacation_section = True  # 休暇情報テーブルのマーカー
            vacation_frame.pack(fill="x", padx=20, pady=(10, 10))
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
            
            # テーブルフレーム
            table_frame = tk.Frame(vacation_frame)
            table_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
            
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
            
            # 列の設定
            vacation_tree.heading("検査員名", text="検査員名", anchor="center")
            vacation_tree.heading("休暇内容", text="休暇内容", anchor="center")
            vacation_tree.column("検査員名", width=200, anchor="w")
            vacation_tree.column("休暇内容", width=300, anchor="w")
            
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
            
            table_frame.grid_rowconfigure(0, weight=1)
            table_frame.grid_columnconfigure(0, weight=1)
            
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
            
            # スキル表示切り替えボタン
            button_text = "スキル非表示" if self.show_skill_values else "スキル表示"
            self.skill_toggle_button = ctk.CTkButton(
                button_frame,
                text=button_text,
                command=self.toggle_skill_display,
                width=100,
                height=30,
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
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
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
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
                "検査員人数", "分割検査時間", "検査員1", "検査員2", "検査員3", "検査員4", "検査員5"
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
                        # 検査員名の表示制御
                        inspector_name = str(col_value) if pd.notna(col_value) else ''
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
                    
                    # 検査員列（検査員1～5）の場合のみメニューを表示
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
            
            # 選択された検査員を保持
            selected_inspector = {'name': None, 'code': None}
            
            # 検査員リストを作成
            inspector_names = inspector_master_df['#氏名'].dropna().astype(str).str.strip()
            inspector_names = inspector_names[inspector_names != ''].unique().tolist()
            
            # 各検査員にラジオボタンを作成
            for inspector_name in sorted(inspector_names):
                # 検査員コードを取得
                inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                if inspector_info.empty:
                    continue
                
                inspector_code = inspector_info.iloc[0]['#ID']
                
                # ラジオボタンを作成
                radio = ctk.CTkRadioButton(
                    scroll_frame,
                    text=inspector_name,
                    value=inspector_name,
                    command=lambda name=inspector_name, code=inspector_code: set_selected(name, code),
                    font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold")
                )
                radio.pack(anchor="w", pady=2)
                
                # 現在の検査員を選択状態にする
                if current_inspector:
                    current_name_clean = current_inspector.split('(')[0].strip()
                    if inspector_name == current_name_clean:
                        radio.select()
                        selected_inspector['name'] = inspector_name
                        selected_inspector['code'] = inspector_code
            
            def set_selected(name, code):
                selected_inspector['name'] = name
                selected_inspector['code'] = code
            
            # ボタンフレーム
            button_frame = ctk.CTkFrame(dialog, fg_color="transparent")
            button_frame.pack(pady=10)
            
            def on_ok():
                if selected_inspector['name']:
                    # 検査員を変更
                    self.update_inspector_assignment(
                        original_index, col_name, col_index,
                        selected_inspector['name'], selected_inspector['code'],
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
                
                # 詳細表示ポップアップが開いている場合は更新
                self.update_detail_popup_if_open()
                
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
            
            # データフレームを更新
            # スキル表示の設定に応じて検査員名を設定
            if self.show_skill_values:
                # スキル値を取得（簡易版：スキルマスタから取得する必要がある）
                new_inspector_display = new_inspector_name
            else:
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
            
            # 詳細表示ポップアップが開いている場合は更新
            self.update_detail_popup_if_open()
            
            self.log_message(
                f"検査員を変更しました: {old_inspector_name if old_inspector_name else '未割当'} → {new_inspector_name} "
                f"(品番: {product_number}, {col_name})"
            )
            
        except Exception as e:
            self.log_message(f"検査員割当ての更新に失敗しました: {str(e)}")
            logger.error(f"検査員割当ての更新に失敗しました: {str(e)}", exc_info=True)
    
    def _recalculate_inspector_count_and_divided_time(self, df, row_index):
        """検査員人数と分割検査時間を再計算"""
        try:
            row = df.loc[row_index]
            
            # 検査員1～5の列を確認して、実際に割り当てられている検査員数をカウント
            inspector_count = 0
            for i in range(1, 6):
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
            
            # 列名から列インデックスへのマッピングを作成（高速化：itertuples()を使用）
            inspector_col_indices = {f'検査員{i}': self.current_inspector_data.columns.get_loc(f'検査員{i}') for i in range(1, 6) if f'検査員{i}' in self.current_inspector_data.columns}
            divided_time_col_idx = self.current_inspector_data.columns.get_loc('分割検査時間') if '分割検査時間' in self.current_inspector_data.columns else None
            
            for row_tuple in self.current_inspector_data.itertuples(index=True):
                row_idx = row_tuple[0]  # インデックス
                row = self.current_inspector_data.loc[row_idx]  # Seriesとして扱うために元の行を取得
                
                for i in range(1, 6):  # 検査員1〜5
                    inspector_col = f'検査員{i}'
                    if inspector_col in inspector_col_indices:
                        inspector_col_idx = inspector_col_indices[inspector_col]
                        inspector_value = row_tuple[inspector_col_idx + 1] if inspector_col_idx < len(row_tuple) - 1 else None  # itertuplesはインデックスを含むため+1
                        if pd.notna(inspector_value) and str(inspector_value).strip() != '':
                            inspector_name = str(inspector_value)
                            # スキル値や(新)を除去して氏名のみを取得
                            if '(' in inspector_name:
                                inspector_name = inspector_name.split('(')[0].strip()
                            
                            # 分割検査時間を取得
                            if divided_time_col_idx is not None:
                                divided_time = row_tuple[divided_time_col_idx + 1] if divided_time_col_idx < len(row_tuple) - 1 else 0
                            else:
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
            self.graph_frame = ctk.CTkFrame(self.main_scroll_frame, fg_color="#EFF6FF", corner_radius=12)
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
            self.detail_popup.geometry("1400x900")
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
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
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
    
    def update_detail_popup_if_open(self):
        """詳細表示ポップアップが開いている場合は更新"""
        try:
            if hasattr(self, 'detail_popup') and self.detail_popup is not None:
                try:
                    # ポップアップが存在し、破棄されていないか確認
                    if self.detail_popup.winfo_exists():
                        # 現在のタブを取得
                        tabview = None
                        for widget in self.detail_popup.winfo_children():
                            if isinstance(widget, ctk.CTkFrame):
                                for child in widget.winfo_children():
                                    if isinstance(child, ctk.CTkTabview):
                                        tabview = child
                                        break
                                if tabview:
                                    break
                        
                        if tabview:
                            current_tab = tabview.get()
                            
                            # タブの内容を再描画
                            # グラフタブの場合
                            if current_tab == "グラフ表示":
                                graph_frame = tabview.tab("グラフ表示")
                                # 既存のグラフを削除
                                for widget in graph_frame.winfo_children():
                                    widget.destroy()
                                # グラフを再作成
                                self.create_detail_graph(graph_frame)
                            
                            # ロット一覧タブの場合
                            elif current_tab == "ロット一覧":
                                lot_frame = tabview.tab("ロット一覧")
                                # 既存のリストを削除
                                for widget in lot_frame.winfo_children():
                                    widget.destroy()
                                # リストを再作成
                                self.create_inspector_lot_list(lot_frame)
                except Exception as e:
                    logger.debug(f"詳細表示ポップアップの更新に失敗: {str(e)}")
        except Exception as e:
            logger.debug(f"詳細表示ポップアップ更新チェックに失敗: {str(e)}")
    
    def create_detail_graph(self, parent):
        """詳細表示用のグラフを作成"""
        try:
            if self.current_inspector_data is None:
                return
            
            # 検査員の検査時間集計を取得（実際に割り当てられた検査員のみ）
            inspector_hours = {}
            # 列名から列インデックスへのマッピングを作成（高速化：itertuples()を使用）
            inspector_col_indices = {f'検査員{i}': self.current_inspector_data.columns.get_loc(f'検査員{i}') for i in range(1, 6) if f'検査員{i}' in self.current_inspector_data.columns}
            divided_time_col_idx = self.current_inspector_data.columns.get_loc('分割検査時間') if '分割検査時間' in self.current_inspector_data.columns else None
            
            for row_tuple in self.current_inspector_data.itertuples(index=True):
                row_idx = row_tuple[0]  # インデックス
                row = self.current_inspector_data.loc[row_idx]  # Seriesとして扱うために元の行を取得
                
                # 検査員1～5を確認
                for i in range(1, 6):
                    inspector_col = f'検査員{i}'
                    if inspector_col in inspector_col_indices:
                        inspector_col_idx = inspector_col_indices[inspector_col]
                        inspector_value = row_tuple[inspector_col_idx + 1] if inspector_col_idx < len(row_tuple) - 1 else None  # itertuplesはインデックスを含むため+1
                        if pd.notna(inspector_value):
                            inspector_name = str(inspector_value).strip()
                            
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
                            if divided_time_col_idx is not None:
                                divided_time_value = row_tuple[divided_time_col_idx + 1] if divided_time_col_idx < len(row_tuple) - 1 else None
                            else:
                                divided_time_value = row.get('分割検査時間', None)
                            if pd.notna(divided_time_value):
                                inspector_hours[inspector_name] += float(divided_time_value)
            
            if not inspector_hours:
                no_data_label = ctk.CTkLabel(
                    parent,
                    text="検査員データがありません",
                    font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold")
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
                font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
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
            # 列名から列インデックスへのマッピングを作成（高速化：itertuples()を使用）
            inspector_col_indices = {f'検査員{i}': self.current_inspector_data.columns.get_loc(f'検査員{i}') for i in range(1, 6) if f'検査員{i}' in self.current_inspector_data.columns}
            lot_info_cols = ['生産ロットID', '指示日', '出荷予定日', 'ロット数量', '分割検査時間', '品番', '品名', 'チーム情報']
            lot_info_col_indices = {col: self.current_inspector_data.columns.get_loc(col) for col in lot_info_cols if col in self.current_inspector_data.columns}
            
            for row_tuple in self.current_inspector_data.itertuples(index=True):
                row_idx = row_tuple[0]  # インデックス
                row = self.current_inspector_data.loc[row_idx]  # Seriesとして扱うために元の行を取得
                
                # 検査員1～5を確認
                for i in range(1, 6):
                    inspector_col = f'検査員{i}'
                    if inspector_col in inspector_col_indices:
                        inspector_col_idx = inspector_col_indices[inspector_col]
                        inspector_value = row_tuple[inspector_col_idx + 1] if inspector_col_idx < len(row_tuple) - 1 else None  # itertuplesはインデックスを含むため+1
                        if pd.notna(inspector_value):
                            inspector_name = str(inspector_value).strip()
                            
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
                            lot_info = {}
                            for col in lot_info_cols:
                                if col in lot_info_col_indices:
                                    col_idx = lot_info_col_indices[col]
                                    col_value = row_tuple[col_idx + 1] if col_idx < len(row_tuple) - 1 else None  # itertuplesはインデックスを含むため+1
                                    lot_info[col] = col_value if pd.notna(col_value) else ''
                                else:
                                    lot_info[col] = row.get(col, '')
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
            font_data = tk.font.Font(family="Yu Gothic", size=11)
            font_header = tk.font.Font(family="Yu Gothic", size=12, weight="bold")
            
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
            
            # スクロール可能なフレーム（スクロールバーのスタイルを改善）
            # CustomTkinterのバージョンによっては、scrollbar_button_colorなどのパラメータが
            # 存在しない場合があるため、try-exceptで対応
            try:
                scroll_frame = ctk.CTkScrollableFrame(
                    parent,
                    scrollbar_button_color="#9CA3AF",
                    scrollbar_button_hover_color="#6B7280",
                    corner_radius=8
                )
            except TypeError:
                # パラメータが存在しない場合はデフォルトで作成
                scroll_frame = ctk.CTkScrollableFrame(
                    parent,
                    corner_radius=8
                )
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
                inspector_section.pack(fill="x", padx=10, pady=8)
                
                # 検査員名とロット数
                total_hours = sum(lot.get('分割検査時間', 0) for lot in lots if pd.notna(lot.get('分割検査時間', 0)))
                header_text = f"{inspector_name} ({len(lots)}ロット, 合計: {total_hours:.1f}時間)"
                header_label = ctk.CTkLabel(
                    inspector_section,
                    text=header_text,
                    font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
                    fg_color="#E5E7EB"
                )
                header_label.pack(fill="x", padx=8, pady=8)
                
                # ロット一覧テーブル（スクロール不要、全体のスクロールを使用）
                lot_frame = tk.Frame(inspector_section, bg="white")
                lot_frame.pack(fill="x", padx=8, pady=(0, 8))
                
                # テーブルヘッダー（計算済みの列幅を使用）
                for j, header in enumerate(headers):
                    header_label = tk.Label(
                        lot_frame,
                        text=header,
                        font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
                        bg="#E5E7EB",
                        fg="#1F2937",
                        relief="solid",
                        borderwidth=2,
                        anchor="center"
                    )
                    header_label.grid(row=0, column=j, sticky="ew", padx=2, pady=3, ipadx=5, ipady=5)
                
                # ロットデータ行（ホバー効果と選択行のハイライトを追加）
                selected_row_ref = {'value': None}  # この検査員セクションの選択された行を追跡
                row_labels = {}  # 行ごとのラベルを保持 {row_idx: [label1, label2, ...]}
                
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
                    
                    # 行全体の背景色を決定
                    base_bg = "white" if i % 2 == 0 else "#F3F4F6"
                    row_labels[i] = []  # この行のラベルを保持
                    
                    for j, data in enumerate(row_data):
                        header = headers[j]
                        anchor_pos = "e" if header in ['ロット数量', '検査時間'] else "w"
                        data_label = tk.Label(
                            lot_frame,
                            text=data,
                            font=ctk.CTkFont(family="Yu Gothic", size=11),
                            bg=base_bg,
                            fg="#111827",
                            relief="solid",
                            borderwidth=1,
                            anchor=anchor_pos,
                            wraplength=column_widths.get(header, 100) * 8  # 折り返し対応
                        )
                        data_label.grid(row=i+1, column=j, sticky="ew", padx=2, pady=2, ipadx=5, ipady=4)
                        row_labels[i].append(data_label)  # ラベルを保存
                        
                        # ホバー効果とクリックイベントを追加
                        def make_handlers(row_idx, bg_color, labels_list):
                            """行ごとのイベントハンドラを作成"""
                            def on_enter(event):
                                """マウスが行に入った時の処理"""
                                if selected_row_ref['value'] != row_idx:
                                    # 選択されていない行の場合のみホバー色を適用
                                    for label in labels_list:
                                        label.config(bg="#E0E7FF")  # 薄い青
                            
                            def on_leave(event):
                                """マウスが行から出た時の処理"""
                                if selected_row_ref['value'] != row_idx:
                                    # 選択されていない行の場合のみ元の色に戻す
                                    for label in labels_list:
                                        label.config(bg=bg_color)
                            
                            def on_click(event):
                                """行がクリックされた時の処理"""
                                # 前の選択行を元の色に戻す
                                if selected_row_ref['value'] is not None:
                                    prev_bg = "white" if selected_row_ref['value'] % 2 == 0 else "#F3F4F6"
                                    if selected_row_ref['value'] in row_labels:
                                        for label in row_labels[selected_row_ref['value']]:
                                            label.config(bg=prev_bg)
                                
                                # 新しい選択行をハイライト
                                selected_row_ref['value'] = row_idx
                                for label in labels_list:
                                    label.config(bg="#DBEAFE")  # 選択時の青
                            
                            return on_enter, on_leave, on_click
                        
                        on_enter, on_leave, on_click = make_handlers(i, base_bg, row_labels[i])
                        data_label.bind("<Enter>", on_enter)
                        data_label.bind("<Leave>", on_leave)
                        data_label.bind("<Button-1>", on_click)
                        # カーソルをポインターに変更
                        data_label.config(cursor="hand2")
                
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
    
    def cleanup_resources(self):
        """リソースのクリーンアップ"""
        try:
            logger.info("リソースをクリーンアップしています...")
            
            # カレンダーウィンドウを閉じる
            if hasattr(self, 'calendar_window') and self.calendar_window is not None:
                try:
                    self.calendar_window.destroy()
                except:
                    pass
                self.calendar_window = None
            
            # グラフフレームを破棄
            if hasattr(self, 'graph_frame') and self.graph_frame is not None:
                try:
                    self.graph_frame.destroy()
                except:
                    pass
                self.graph_frame = None
            
            # matplotlibのリソースをクリーンアップ
            try:
                import matplotlib.pyplot as plt
                plt.close('all')
            except:
                pass
            
            logger.info("リソースのクリーンアップが完了しました")
            
        except Exception as e:
            logger.error(f"リソースクリーンアップ中にエラーが発生しました: {e}")
    
    def quit_application(self):
        """アプリケーションを完全に終了する"""
        try:
            # ログ出力
            logger.info("アプリケーションを終了しています...")
            
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
