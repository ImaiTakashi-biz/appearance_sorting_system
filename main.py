"""
出荷検査データ抽出システム - メインUI
近未来的なデザインで出荷予定日を指定してデータを抽出する
"""

import customtkinter as ctk
import tkinter as tk
from tkinter import messagebox, filedialog, ttk
import pandas as pd
import pyodbc
from datetime import datetime, date
import threading
import os
from pathlib import Path
from loguru import logger
from config import DatabaseConfig
import calendar
import locale
from excel_exporter import ExcelExporter
from inspector_assignment import InspectorAssignmentManager


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
        self.root.geometry("1200x800")
        self.root.minsize(1000, 700)
        
        # ウィンドウの背景色を白に設定
        self.root.configure(fg_color=("white", "white"))
        
        # ウィンドウを中央に配置
        self.center_window()
        
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
        
        # 検査員割当てマネージャーの初期化
        self.inspector_manager = InspectorAssignmentManager(log_callback=self.log_message)
        
        # データ保存用変数
        self.current_main_data = None
        self.current_assignment_data = None
        self.current_inspector_data = None
        
        # スキル表示状態管理
        self.show_skill_values = False
        self.original_inspector_data = None  # 元のデータを保持
        
        # マスタデータ保存用変数
        self.inspector_master_data = None
        self.skill_master_data = None
        
        # 現在表示中のテーブル
        self.current_display_table = None
        
        # UIの構築
        self.setup_ui()
        
        # ログ設定
        self.setup_logging()
        
        # 設定の読み込み
        self.load_config()
        
        # デフォルトのスクロール動作を使用（カスタムスクロールを削除）
    
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
        """メイン画面のスクロールをバインド（デフォルト動作を使用）"""
        # デフォルトのスクロール動作を使用（カスタムスクロールを削除）
        pass
    
    def setup_logging(self):
        """ログ設定"""
        # ログはコンソール出力のみ（exe化対応）
        logger.remove()  # デフォルトのハンドラーを削除
        logger.add(
            lambda msg: print(msg, end=""),
            level="INFO",
            format="{time:HH:mm:ss} | {level} | {message}"
        )
    
    def load_config(self):
        """設定の読み込み"""
        try:
            self.config = DatabaseConfig()
            if self.config.validate_config():
                self.status_label.configure(text="データベース接続状態: 接続可能", text_color="#059669")
                logger.info("設定の読み込みが完了しました")
            else:
                self.status_label.configure(text="データベース接続状態: 接続不可", text_color="#DC2626")
                logger.error("設定の検証に失敗しました")
        except Exception as e:
            self.status_label.configure(text=f"データベース接続状態: エラー", text_color="#DC2626")
            logger.error(f"設定の読み込みに失敗しました: {e}")
    
    def setup_ui(self):
        """UIの構築"""
        # メインスクロールフレーム
        self.main_scroll_frame = ctk.CTkScrollableFrame(self.root, fg_color="white", corner_radius=0)
        self.main_scroll_frame.pack(fill="both", expand=True, padx=0, pady=0)
        
        # タイトルセクション
        self.create_title_section(self.main_scroll_frame)
        
        # 設定セクション
        self.create_config_section(self.main_scroll_frame)
        
        # 日付選択セクション
        self.create_date_section(self.main_scroll_frame)
        
        # ボタンセクション
        self.create_button_section(self.main_scroll_frame)
        
        # 進捗セクション
        self.create_progress_section(self.main_scroll_frame)
        
        # データ表示セクションは選択式表示のため削除
        # self.create_data_display_section(self.main_scroll_frame)
        
        # ログセクションは削除
    
    def create_title_section(self, parent):
        """タイトルセクションの作成"""
        title_frame = ctk.CTkFrame(parent, height=100, fg_color="white", corner_radius=0)
        title_frame.pack(fill="x", pady=(20, 30))
        title_frame.pack_propagate(False)
        
        # メインタイトル
        title_label = ctk.CTkLabel(
            title_frame,
            text="出荷検査データ抽出システム",
            font=ctk.CTkFont(family="Yu Gothic", size=32, weight="bold"),
            text_color="#1E3A8A"  # 濃い青
        )
        title_label.pack(pady=(20, 5))
        
        # サブタイトル
        subtitle_label = ctk.CTkLabel(
            title_frame,
            text="出荷予定日を指定してAccessデータベースからデータを抽出",
            font=ctk.CTkFont(family="Yu Gothic", size=16),
            text_color="#64748B"  # グレー
        )
        subtitle_label.pack()
    
    def create_config_section(self, parent):
        """設定セクションの作成"""
        config_frame = ctk.CTkFrame(parent, fg_color="#F8FAFC", corner_radius=12)
        config_frame.pack(fill="x", pady=(0, 20), padx=20)
        
        # 設定情報の表示
        config_info_frame = ctk.CTkFrame(config_frame, fg_color="transparent")
        config_info_frame.pack(fill="x", padx=20, pady=15)
        
        # ステータス表示のみ
        self.status_label = ctk.CTkLabel(
            config_info_frame,
            text="データベース接続状態: 確認中...",
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            anchor="w",
            text_color="#6B7280"
        )
        self.status_label.pack(fill="x")
    
    def create_date_section(self, parent):
        """日付選択セクションの作成"""
        date_frame = ctk.CTkFrame(parent, fg_color="#F8FAFC", corner_radius=12)
        date_frame.pack(fill="x", pady=(0, 20), padx=20)
        
        # セクションタイトル
        date_title = ctk.CTkLabel(
            date_frame,
            text="出荷予定日選択",
            font=ctk.CTkFont(family="Yu Gothic", size=20, weight="bold"),
            text_color="#1E3A8A"
        )
        date_title.pack(pady=(15, 10))
        
        # 期間選択フレーム
        period_frame = ctk.CTkFrame(date_frame, fg_color="white", corner_radius=8)
        period_frame.pack(fill="x", padx=15, pady=(0, 15))
        
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
        button_frame.pack(fill="x", pady=(20, 20), padx=20)
        
        # ボタンフレーム
        buttons_frame = ctk.CTkFrame(button_frame, fg_color="transparent")
        buttons_frame.pack(fill="x", pady=20)
        
        # データ抽出ボタン
        self.extract_button = ctk.CTkButton(
            buttons_frame,
            text="データ抽出開始",
            command=self.start_extraction,
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            height=48,
            fg_color="#3B82F6",
            hover_color="#2563EB",
            corner_radius=8
        )
        self.extract_button.pack(side="left", padx=(0, 10))
        
        # 設定リロードボタン
        self.reload_button = ctk.CTkButton(
            buttons_frame,
            text="設定リロード",
            command=self.reload_config,
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=48,
            width=140,
            fg_color="#6B7280",
            hover_color="#4B5563",
            corner_radius=8
        )
        self.reload_button.pack(side="left", padx=(0, 10))
        
        # Excel出力ボタン
        self.export_button = ctk.CTkButton(
            buttons_frame,
            text="Excel出力",
            command=self.export_selected_table,
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=48,
            width=120,
            fg_color="#10B981",
            hover_color="#059669",
            corner_radius=8
        )
        self.export_button.pack(side="left", padx=(0, 10))
        
        # テーブル選択フレーム
        table_selection_frame = ctk.CTkFrame(buttons_frame, fg_color="transparent")
        table_selection_frame.pack(side="left", padx=(20, 10))
        
        # テーブル選択ラベル
        table_label = ctk.CTkLabel(
            table_selection_frame,
            text="表示テーブル:",
            font=ctk.CTkFont(family="Yu Gothic", size=14)
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
            command=self.root.quit,
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=48,
            width=100,
            fg_color="#EF4444",
            hover_color="#DC2626",
            corner_radius=8
        )
        self.exit_button.pack(side="right")
    
    def create_progress_section(self, parent):
        """進捗セクションの作成"""
        progress_frame = ctk.CTkFrame(parent, fg_color="#F8FAFC", corner_radius=12)
        progress_frame.pack(fill="x", pady=(0, 20), padx=20)
        
        # 進捗ラベル
        self.progress_label = ctk.CTkLabel(
            progress_frame,
            text="待機中...",
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            text_color="#1E3A8A"
        )
        self.progress_label.pack(pady=(20, 10))
        
        # プログレスバー
        self.progress_bar = ctk.CTkProgressBar(
            progress_frame,
            height=24,
            progress_color="#3B82F6",
            fg_color="#E5E7EB"
        )
        self.progress_bar.pack(fill="x", padx=20, pady=(0, 20))
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
            self.update_progress(0.1, "データベースに接続中...")
            connection_string = self.config.get_connection_string()
            connection = pyodbc.connect(connection_string)
            
            self.log_message("データベース接続が完了しました")
            
            # まずテーブル構造を確認
            self.log_message("テーブル構造を確認中...")
            columns_query = f"SELECT TOP 1 * FROM [{self.config.access_table_name}]"
            sample_df = pd.read_sql(columns_query, connection)
            
            if sample_df.empty:
                self.log_message("テーブルにデータが見つかりません")
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
            
            # 利用可能な列のみでクエリを作成
            columns_str = ", ".join([f"[{col}]" for col in available_columns])
            query = f"SELECT {columns_str} FROM [{self.config.access_table_name}]"
            
            # データの抽出
            self.update_progress(0.4, "データを抽出中...")
            df = pd.read_sql(query, connection)
            
            # t_現品票履歴から梱包工程の数量を取得
            self.update_progress(0.5, "梱包工程データを取得中...")
            packaging_data = self.get_packaging_quantities(connection, df)
            
            # 梱包数量をメインデータに結合
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
            
            # 出荷予定日でフィルタリング
            if not df.empty and '出荷予定日' in df.columns:
                # 日付列をdatetime型に変換
                df['出荷予定日'] = pd.to_datetime(df['出荷予定日'], errors='coerce')
                
                # 期間でフィルタリング
                mask = (df['出荷予定日'] >= pd.to_datetime(start_date)) & (df['出荷予定日'] <= pd.to_datetime(end_date))
                df = df[mask]
                
                # 出荷予定日順でソート
                df = df.sort_values('出荷予定日')
            
            if df is None or df.empty:
                self.log_message("指定された期間にデータが見つかりませんでした")
                self.update_progress(1.0, "完了（データなし）")
                return
            
            self.log_message(f"抽出完了: {len(df)}件のレコード")
            
            # データをアプリ上に表示
            self.update_progress(0.7, "データを表示中...")
            
            # データをテキスト形式で表示
            # データは選択式表示のため、ここでは表示しない
            # self.display_data(df)
            
            # データを保存（エクスポート用）
            self.current_main_data = df
            
            # 不足数がマイナスの品番に対してロット割り当てを実行
            self.update_progress(0.9, "ロット割り当て処理中...")
            self.process_lot_assignment(connection, df)
            
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
        print(log_entry)  # コンソール出力のみ
        logger.info(message)
    
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
            
            # 各列の設定（列幅を調整）
            column_widths = {
                "品番": 100,
                "品名": 200,
                "客先": 150,
                "出荷予定日": 120,
                "出荷数": 80,
                "在庫数": 80,
                "梱包・完了": 100,
                "不足数": 80,
                "処理": 100
            }
            
            # 右詰めにする数値列
            numeric_columns = ["出荷数", "在庫数", "梱包・完了", "不足数"]
            
            for col in columns:
                width = column_widths.get(col, 120)
                # 数値列は右詰め、その他は左詰め
                anchor = "e" if col in numeric_columns else "w"
                data_tree.column(col, width=width, anchor=anchor)
                data_tree.heading(col, text=col)
            
            # データの挿入（最初の100件まで）
            display_limit = min(100, len(df))
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
                
                # データを挿入
                item_id = data_tree.insert("", "end", values=values)
                
                # 不足数がマイナスの場合は背景を赤くする
                if '不足数' in columns and pd.notna(row['不足数']):
                    try:
                        shortage = float(row['不足数'])
                        # 不足数を整数で表示
                        data_tree.set(item_id, '不足数', str(int(shortage)))
                        
                        # マイナス値の場合は背景を赤くする
                        if shortage < 0:
                            # タグを設定してスタイルを適用
                            data_tree.item(item_id, tags=('negative',))
                            # 不足数列の背景色を直接設定
                            data_tree.set(item_id, '不足数', str(int(shortage)))
                    except:
                        pass
            
            # 件数制限の表示
            if len(df) > 100:
                data_tree.insert("", "end", values=["... 他 " + str(len(df) - 100) + "件のデータがあります"] + [""] * (len(columns) - 1))
            
            # タグの設定
            data_tree.tag_configure("negative", background="#FEE2E2", foreground="#DC2626")
            
            # マウスホイールイベントのバインド
            def on_data_mousewheel(event):
                data_tree.yview_scroll(int(-1 * (event.delta / 120)), "units")
                return "break"
            
            data_tree.bind("<MouseWheel>", on_data_mousewheel)
            
            # テーブルに入ったときと出たときのイベント
            def on_data_enter(event):
                self.main_scroll_frame.unbind_all("<MouseWheel>")
            
            def on_data_leave(event):
                self.bind_main_scroll()
            
            data_tree.bind("<Enter>", on_data_enter)
            data_tree.bind("<Leave>", on_data_leave)
            
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
                
                # ロットを順番に割り当て
                for _, lot in product_lots.iterrows():
                    if current_shortage >= 0:  # 不足数が0以上になったら終了
                        break
                    
                    lot_quantity = int(lot['数量']) if pd.notna(lot['数量']) else 0
                    
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
                        '指示日': lot.get('指示日', '') if pd.notna(lot.get('指示日', '')) else '',
                        '号機': lot.get('号機', '') if pd.notna(lot.get('号機', '')) else '',
                        '現在工程番号': lot.get('現在工程番号', '') if pd.notna(lot.get('現在工程番号', '')) else '',
                        '現在工程名': lot.get('現在工程名', '') if pd.notna(lot.get('現在工程名', '')) else '',
                        '現在工程二次処理': lot.get('現在工程二次処理', '') if pd.notna(lot.get('現在工程二次処理', '')) else '',
                        '生産ロットID': lot.get('生産ロットID', '') if pd.notna(lot.get('生産ロットID', '')) else ''
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
    
    def process_lot_assignment(self, connection, main_df):
        """ロット割り当て処理のメイン処理"""
        try:
            # 不足数がマイナスのデータを抽出
            shortage_df = main_df[main_df['不足数'] < 0].copy()
            
            if shortage_df.empty:
                self.log_message("不足数がマイナスのデータがありません")
                return
            
            self.log_message(f"不足数がマイナスのデータ: {len(shortage_df)}件")
            
            # 利用可能なロットを取得
            lots_df = self.get_available_lots_for_shortage(connection, shortage_df)
            
            if lots_df.empty:
                self.log_message("利用可能なロットが見つかりませんでした")
                return
            
            # ロット割り当てを実行
            assignment_df = self.assign_lots_to_shortage(shortage_df, lots_df)
            
            if not assignment_df.empty:
                # ロット割り当て結果は選択式表示のため、ここでは表示しない
                # self.display_lot_assignment_table(assignment_df)
                
                # ロット割り当てデータを保存（エクスポート用）
                self.current_assignment_data = assignment_df
                
                # 検査員割振り処理を実行
                self.process_inspector_assignment(assignment_df)
            else:
                self.log_message("ロット割り当て結果がありません")
                
        except Exception as e:
            self.log_message(f"ロット割り当て処理中にエラーが発生しました: {str(e)}")
    
    def process_inspector_assignment(self, assignment_df):
        """検査員割振り処理を実行"""
        try:
            if assignment_df.empty:
                self.log_message("ロット割り当て結果がありません")
                return
            
            # 製品マスタファイルを読み込み
            product_master_df = self.load_product_master()
            if product_master_df is None:
                self.log_message("製品マスタの読み込みに失敗しました")
                return
            
            # 検査員マスタファイルを読み込み
            inspector_master_df = self.load_inspector_master()
            if inspector_master_df is None:
                self.log_message("検査員マスタの読み込みに失敗しました")
                return
            
            # スキルマスタファイルを読み込み
            skill_master_df = self.load_skill_master()
            if skill_master_df is None:
                self.log_message("スキルマスタの読み込みに失敗しました")
                return
            
            # マスタデータを保存
            self.inspector_master_data = inspector_master_df
            self.skill_master_data = skill_master_df
            
            # 検査員割振りテーブルを作成
            inspector_df = self.inspector_manager.create_inspector_assignment_table(assignment_df, product_master_df)
            if inspector_df is None:
                self.log_message("検査員割振りテーブルの作成に失敗しました")
                return
            
            # 検査員を割り当て（スキル値付きで保存）
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
                height=15  # 高さを増加
            )
            
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
            
            # デフォルトのスクロール動作を使用（カスタムスクロールを削除）
            
            # スタイル設定
            lot_style = ttk.Style()
            lot_style.configure("LotTreeview", 
                               background="white",
                               foreground="#374151",
                               fieldbackground="white",
                               font=("MS Gothic", 9))
            lot_style.map("LotTreeview",
                         background=[('selected', '#3B82F6')],
                         foreground=[('selected', 'white')])
            
            # 列の定義（画像で要求されているプロパティを含む）
            lot_columns = [
                "出荷予定日", "品番", "品名", "客先", "出荷数", "在庫数", "在梱包数", "不足数",
                "生産ロットID", "ロット数量", "指示日", "号機", "現在工程番号", "現在工程名", "現在工程二次処理"
            ]
            lot_tree["columns"] = lot_columns
            
            # 列の設定
            lot_column_widths = {
                "出荷予定日": 100, "品番": 100, "品名": 200, "客先": 150,
                "出荷数": 80, "在庫数": 80, "在梱包数": 100, "不足数": 80,
                "生産ロットID": 120, "ロット数量": 100, "指示日": 100, "号機": 80,
                "現在工程番号": 120, "現在工程名": 150, "現在工程二次処理": 150
            }
            
            # 右詰めにする数値列
            lot_numeric_columns = ["出荷数", "在庫数", "在梱包数", "不足数", "ロット数量"]
            
            for col in lot_columns:
                width = lot_column_widths.get(col, 120)
                anchor = "e" if col in lot_numeric_columns else "w"
                lot_tree.column(col, width=width, anchor=anchor)
                lot_tree.heading(col, text=col)
            
            # データの挿入
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
                
                lot_tree.insert("", "end", values=values)
            
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
            
            # Excelファイルを読み込み
            df = pd.read_excel(file_path)
            
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
            
            # スキル表示切り替えボタン
            button_text = "スキル非表示" if self.show_skill_values else "スキル表示"
            self.skill_toggle_button = ctk.CTkButton(
                title_frame,
                text=button_text,
                command=self.toggle_skill_display,
                width=100,
                height=30,
                font=ctk.CTkFont(family="Yu Gothic", size=12)
            )
            self.skill_toggle_button.pack(side="right")
            
            # テーブルフレーム
            table_frame = tk.Frame(inspector_frame)
            table_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
            
            # 列の定義
            inspector_columns = [
                "出荷予定日", "品番", "品名", "客先", "ロットID", "数量", 
                "ロット日", "号機", "現在工程名", "秒/個", "検査時間",
                "検査員人数", "分割検査時間", "検査員1", "検査員2", "検査員3", "検査員4", "検査員5"
            ]
            
            # Treeviewの作成
            inspector_tree = ttk.Treeview(table_frame, columns=inspector_columns, show="headings", height=10)
            
            # 列の設定
            inspector_column_widths = {
                "出荷予定日": 100, "品番": 100, "品名": 200, "客先": 150,
                "ロットID": 120, "数量": 80, "ロット日": 100, "号機": 80,
                "現在工程名": 150, "秒/個": 80, "検査時間": 80,
                "検査員人数": 80, "分割検査時間": 100, "検査員1": 120, "検査員2": 120,
                "検査員3": 120, "検査員4": 120, "検査員5": 120
            }
            
            # 右詰めにする数値列
            inspector_numeric_columns = ["数量", "秒/個", "検査時間", "検査員人数", "分割検査時間"]
            
            for col in inspector_columns:
                inspector_tree.heading(col, text=col)
                width = inspector_column_widths.get(col, 100)
                inspector_tree.column(col, width=width, anchor="center")
                
                # 数値列は右詰め
                if col in inspector_numeric_columns:
                    inspector_tree.column(col, anchor="e")
            
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
            for _, row in inspector_df.iterrows():
                values = []
                for col in inspector_columns:
                    if col == '出荷予定日' or col == 'ロット日':
                        try:
                            date_value = pd.to_datetime(row[col])
                            values.append(date_value.strftime('%Y/%m/%d'))
                        except:
                            values.append(str(row[col]))
                    elif col.startswith('検査員'):
                        # 検査員名の表示制御
                        inspector_name = str(row[col])
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
                        values.append(str(row[col]))
                inspector_tree.insert("", "end", values=values)
            
            # マウスホイールイベントのバインド
            def on_inspector_mousewheel(event):
                inspector_tree.yview_scroll(int(-1 * (event.delta / 120)), "units")
                return "break"
            
            inspector_tree.bind("<MouseWheel>", on_inspector_mousewheel)
            
            # テーブルに入ったときと出たときのイベント
            def on_inspector_enter(event):
                self.main_scroll_frame.unbind_all("<MouseWheel>")
            
            def on_inspector_leave(event):
                self.bind_main_scroll()
            
            inspector_tree.bind("<Enter>", on_inspector_enter)
            inspector_tree.bind("<Leave>", on_inspector_leave)
            
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
            
            # 検査員割振りテーブルを作成
            inspector_df = self.inspector_manager.create_inspector_assignment_table(self.current_assignment_data, product_master_df)
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
            
            # CSVファイルを読み込み（ヘッダーなし）
            df = pd.read_csv(file_path, encoding='utf-8-sig', header=None)
            
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
            
            # CSVファイルを読み込み（ヘッダーなし）
            df = pd.read_csv(file_path, encoding='utf-8-sig', header=None)
            
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
