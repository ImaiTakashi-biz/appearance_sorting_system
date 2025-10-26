"""
カレンダーウィジェット - 出荷検査データ抽出システム
カレンダー機能を管理
"""

import customtkinter as ctk
import calendar
from datetime import datetime, date


class CalendarWidget:
    """カレンダーウィジェットの管理クラス"""
    
    def __init__(self, parent, on_date_selected=None):
        """初期化"""
        self.parent = parent
        self.on_date_selected = on_date_selected
        self.calendar_window = None
        self.current_year = datetime.now().year
        self.current_month = datetime.now().month
        self.selected_date = None
        self.current_date_type = None  # "start" or "end"
        
        # main.pyと同様に日付管理変数を追加
        self.selected_start_date = None
        self.selected_end_date = None
        
        # カレンダーを日曜スタートに設定
        calendar.setfirstweekday(6)
    
    def show_calendar_popup(self, date_type, current_date=None):
        """カレンダーポップアップを表示"""
        if self.calendar_window is not None:
            self.calendar_window.destroy()
        
        # 日付タイプを設定
        self.current_date_type = date_type
        
        # 現在の日付を設定
        if current_date:
            if date_type == "start":
                self.selected_start_date = current_date
            else:
                self.selected_end_date = current_date
        
        # ポップアップウィンドウを作成
        self.calendar_window = ctk.CTkToplevel(self.parent)
        self.calendar_window.title(f"{'開始日' if date_type == 'start' else '終了日'}を選択")
        self.calendar_window.geometry("420x550")  # ボタンが見切れないようサイズ調整
        self.calendar_window.resizable(False, False)
        
        # ウィンドウを中央に配置
        self.calendar_window.transient(self.parent)
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
            font=ctk.CTkFont(family="Yu Gothic", size=20, weight="bold"),
            text_color="#1E3A8A"
        )
        title_label.pack(pady=(20, 15))
        
        # カレンダーヘッダー
        header_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        header_frame.pack(fill="x", padx=15, pady=(0, 10))
        
        # 前月ボタン
        prev_button = ctk.CTkButton(
            header_frame,
            text="◀",
            width=32,
            height=32,
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            fg_color="#3B82F6",
            hover_color="#2563EB",
            command=self.prev_month_popup
        )
        prev_button.pack(side="left")
        
        # 年月表示
        self.month_year_label_popup = ctk.CTkLabel(
            header_frame,
            text=f"{self.current_year}年 {self.current_month}月",
            font=ctk.CTkFont(family="Yu Gothic", size=18, weight="bold"),
            text_color="#1E3A8A"
        )
        self.month_year_label_popup.pack(side="left", expand=True)
        
        # 次月ボタン
        next_button = ctk.CTkButton(
            header_frame,
            text="▶",
            width=32,
            height=32,
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            fg_color="#3B82F6",
            hover_color="#2563EB",
            command=self.next_month_popup
        )
        next_button.pack(side="right")
        
        # 今日ボタン
        today_button = ctk.CTkButton(
            header_frame,
            text="今日",
            width=50,
            height=32,
            font=ctk.CTkFont(family="Yu Gothic", size=12, weight="bold"),
            fg_color="#10B981",
            hover_color="#059669",
            command=self.go_to_today_popup
        )
        today_button.pack(side="right", padx=(0, 8))
        
        # 曜日ヘッダー（日曜スタート）
        weekdays = ["日", "月", "火", "水", "木", "金", "土"]
        weekday_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        weekday_frame.pack(fill="x", padx=15, pady=(0, 5))
        
        for i, day in enumerate(weekdays):
            day_label = ctk.CTkLabel(
                weekday_frame,
                text=day,
                font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
                text_color="#6B7280",
                width=50
            )
            day_label.grid(row=0, column=i, padx=2, pady=5)
        
        # カレンダーグリッド
        self.calendar_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        self.calendar_frame.pack(fill="both", expand=True, padx=15, pady=(0, 10))
        
        # 選択された日付の表示
        initial_text = f"{'開始日' if self.current_date_type == 'start' else '終了日'}を選択してください"
        if self.current_date_type == "start" and self.selected_start_date:
            initial_text = f"開始日: {self.selected_start_date.strftime('%Y年%m月%d日')}"
        elif self.current_date_type == "end" and self.selected_end_date:
            initial_text = f"終了日: {self.selected_end_date.strftime('%Y年%m月%d日')}"
        
        self.selected_dates_label_popup = ctk.CTkLabel(
            main_frame,
            text=initial_text,
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            text_color="#1E3A8A"
        )
        self.selected_dates_label_popup.pack(pady=10)
        
        # ボタンフレーム
        button_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        button_frame.pack(fill="x", padx=15, pady=(15, 20))
        
        # 確定ボタン
        confirm_button = ctk.CTkButton(
            button_frame,
            text="確定",
            command=self.confirm_date_selection,
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            width=80,
            height=40,
            fg_color="#3B82F6",
            hover_color="#2563EB",
            corner_radius=8
        )
        confirm_button.pack(side="left", padx=(0, 8))
        
        # キャンセルボタン
        cancel_button = ctk.CTkButton(
            button_frame,
            text="キャンセル",
            command=self.cancel_date_selection,
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            width=80,
            height=40,
            fg_color="#6B7280",
            hover_color="#4B5563",
            corner_radius=8
        )
        cancel_button.pack(side="right", padx=(8, 0))
        
        # カレンダーを更新
        self.update_calendar_popup()
    
    def update_calendar_popup(self):
        """カレンダーポップアップを更新"""
        # 既存のカレンダーボタンを削除
        for widget in self.calendar_frame.winfo_children():
            widget.destroy()
        
        # カレンダーを生成
        cal = calendar.monthcalendar(self.current_year, self.current_month)
        
        # 今日の日付
        today = date.today()
        
        # カレンダーボタンを作成
        for week_num, week in enumerate(cal):
            for day_num, day in enumerate(week):
                if day == 0:
                    # 空のセル
                    empty_label = ctk.CTkLabel(
                        self.calendar_frame,
                        text="",
                        width=50,
                        height=30
                    )
                    empty_label.grid(row=week_num, column=day_num, padx=2, pady=2)
                else:
                    # 日付ボタン
                    button_text = str(day)
                    button_date = date(self.current_year, self.current_month, day)
                    
                    # ボタンの色を決定（main.pyの実装に合わせて修正）
                    if button_date == today:
                        fg_color = "#DBEAFE"  # 今日は薄い青
                        text_color = "#1E3A8A"
                    elif (self.current_date_type == "start" and self.selected_start_date and button_date == self.selected_start_date):
                        fg_color = "#3B82F6"  # 選択日は青
                        text_color = "white"
                    elif (self.current_date_type == "end" and self.selected_end_date and button_date == self.selected_end_date):
                        fg_color = "#3B82F6"  # 選択日は青
                        text_color = "white"
                    else:
                        fg_color = "white"  # 通常は白
                        text_color = "#374151"
                    
                    day_button = ctk.CTkButton(
                        self.calendar_frame,
                        text=button_text,
                        width=50,
                        height=30,
                        font=ctk.CTkFont(family="Yu Gothic", size=12),
                        fg_color=fg_color,
                        hover_color="#F3F4F6",
                        text_color=text_color,
                        command=lambda d=day: self.select_date_popup(d)
                    )
                    day_button.grid(row=week_num, column=day_num, padx=2, pady=2)
    
    def update_selected_date_display(self, selected_date):
        """選択された日付の表示を更新（非推奨：update_selected_dates_display_popupを使用）"""
        # このメソッドは非推奨です。update_selected_dates_display_popupを使用してください。
        pass
    
    def select_date_popup(self, day):
        """日付を選択（main.pyの実装に合わせて修正）"""
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
        """選択された日付の表示を更新（main.pyの実装に合わせて修正）"""
        if self.current_date_type == "start" and self.selected_start_date:
            self.selected_dates_label_popup.configure(
                text=f"開始日: {self.selected_start_date.strftime('%Y年%m月%d日')}"
            )
        elif self.current_date_type == "end" and self.selected_end_date:
            self.selected_dates_label_popup.configure(
                text=f"終了日: {self.selected_end_date.strftime('%Y年%m月%d日')}"
            )
        else:
            self.selected_dates_label_popup.configure(
                text=f"{'開始日' if self.current_date_type == 'start' else '終了日'}を選択してください"
            )
    
    def prev_month_popup(self):
        """前月に移動"""
        if self.current_month == 1:
            self.current_month = 12
            self.current_year -= 1
        else:
            self.current_month -= 1
        
        self.month_year_label_popup.configure(
            text=f"{self.current_year}年 {self.current_month}月"
        )
        self.update_calendar_popup()
    
    def next_month_popup(self):
        """次月に移動"""
        if self.current_month == 12:
            self.current_month = 1
            self.current_year += 1
        else:
            self.current_month += 1
        
        self.month_year_label_popup.configure(
            text=f"{self.current_year}年 {self.current_month}月"
        )
        self.update_calendar_popup()
    
    def go_to_today_popup(self):
        """今日の日付に移動（main.pyの実装に合わせて修正）"""
        today = date.today()
        self.current_year = today.year
        self.current_month = today.month
        
        # 今日の日付を自動選択
        if self.current_date_type == "start":
            self.selected_start_date = today
        else:
            self.selected_end_date = today
        
        # 選択された日付の表示を更新
        self.update_selected_dates_display_popup()
        
        # カレンダーを更新
        self.update_calendar_popup()
    
    def confirm_date_selection(self):
        """日付選択を確定（main.pyの実装に合わせて修正）"""
        if self.current_date_type == "start" and self.selected_start_date:
            if self.on_date_selected:
                self.on_date_selected(self.current_date_type, self.selected_start_date)
            if self.calendar_window:
                self.calendar_window.destroy()
                self.calendar_window = None
        elif self.current_date_type == "end" and self.selected_end_date:
            if self.on_date_selected:
                self.on_date_selected(self.current_date_type, self.selected_end_date)
            if self.calendar_window:
                self.calendar_window.destroy()
                self.calendar_window = None
        else:
            # 日付が選択されていない場合は警告を表示
            from tkinter import messagebox
            messagebox.showwarning("警告", f"{'開始日' if self.current_date_type == 'start' else '終了日'}を選択してください")
    
    def cancel_date_selection(self):
        """日付選択をキャンセル"""
        if self.calendar_window:
            self.calendar_window.destroy()
            self.calendar_window = None
