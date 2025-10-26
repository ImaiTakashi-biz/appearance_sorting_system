"""
UIコンポーネント - 出荷検査データ抽出システム
UI関連のコンポーネントを管理
"""

import customtkinter as ctk
import tkinter as tk
from tkinter import ttk


class UIComponents:
    """UIコンポーネントの管理クラス"""
    
    @staticmethod
    def create_title_section(parent):
        """タイトルセクションの作成"""
        title_frame = ctk.CTkFrame(parent, fg_color="white", corner_radius=12)
        title_frame.pack(fill="x", pady=(0, 20), padx=20)
        title_frame.pack_propagate(False)
        
        # メインタイトル
        title_label = ctk.CTkLabel(
            title_frame,
            text="出荷検査データ抽出システム",
            font=ctk.CTkFont(family="Yu Gothic", size=32, weight="bold"),
            text_color="#1E3A8A"
        )
        title_label.pack(pady=(20, 5))
        
        # サブタイトル
        subtitle_label = ctk.CTkLabel(
            title_frame,
            text="出荷予定日を指定してデータを抽出します",
            font=ctk.CTkFont(family="Yu Gothic", size=16),
            text_color="#6B7280"
        )
        subtitle_label.pack(pady=(0, 20))
        
        return title_frame
    
    @staticmethod
    def create_database_status_section(parent):
        """データベース接続状態セクションの作成"""
        status_frame = ctk.CTkFrame(parent, fg_color="#F8FAFC", corner_radius=12)
        status_frame.pack(fill="x", pady=(0, 20), padx=20)
        
        # データベース接続状態ラベル
        status_label = ctk.CTkLabel(
            status_frame,
            text="データベース接続状態: 確認中...",
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            text_color="#374151"
        )
        status_label.pack(pady=15)
        
        return status_frame, status_label
    
    @staticmethod
    def create_date_selection_section(parent):
        """日付選択セクションの作成"""
        date_frame = ctk.CTkFrame(parent, fg_color="white", corner_radius=12)
        date_frame.pack(fill="x", pady=(0, 20), padx=20)
        
        # タイトル
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
        
        return date_frame, period_frame
    
    @staticmethod
    def create_period_selector(parent):
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
        
        start_date_entry = ctk.CTkEntry(
            start_date_frame,
            placeholder_text="YYYY/MM/DD",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=40,
            border_width=1,
            fg_color="white",
            text_color="#374151"
        )
        start_date_entry.pack(side="left", fill="x", expand=True, padx=10, pady=10)
        
        # 開始日カレンダーボタン
        start_calendar_button = ctk.CTkButton(
            start_date_frame,
            text="📅",
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
        
        end_date_entry = ctk.CTkEntry(
            end_date_frame,
            placeholder_text="YYYY/MM/DD",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=40,
            border_width=1,
            fg_color="white",
            text_color="#374151"
        )
        end_date_entry.pack(side="left", fill="x", expand=True, padx=10, pady=10)
        
        # 終了日カレンダーボタン
        end_calendar_button = ctk.CTkButton(
            end_date_frame,
            text="📅",
            width=32,
            height=32,
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            fg_color="transparent",
            hover_color="#F3F4F6",
            text_color="#6B7280"
        )
        end_calendar_button.pack(side="right", padx=(0, 8), pady=8)
        
        return {
            'start_date_entry': start_date_entry,
            'end_date_entry': end_date_entry,
            'start_calendar_button': start_calendar_button,
            'end_calendar_button': end_calendar_button
        }
    
    @staticmethod
    def create_button_section(parent):
        """ボタンセクションの作成"""
        button_frame = ctk.CTkFrame(parent, fg_color="white", corner_radius=12)
        button_frame.pack(fill="x", pady=(0, 20), padx=20)
        
        # ボタンフレーム
        buttons_frame = ctk.CTkFrame(button_frame, fg_color="transparent")
        buttons_frame.pack(fill="x", pady=20)
        
        # データ抽出ボタン
        extract_button = ctk.CTkButton(
            buttons_frame,
            text="データ抽出開始",
            font=ctk.CTkFont(family="Yu Gothic", size=16, weight="bold"),
            height=48,
            fg_color="#3B82F6",
            hover_color="#2563EB",
            corner_radius=8
        )
        extract_button.pack(side="left", padx=(0, 10))
        
        # 設定リロードボタン
        reload_button = ctk.CTkButton(
            buttons_frame,
            text="設定リロード",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=48,
            width=140,
            fg_color="#6B7280",
            hover_color="#4B5563",
            corner_radius=8
        )
        reload_button.pack(side="left", padx=(0, 10))
        
        # エクスポートボタン
        export_button = ctk.CTkButton(
            buttons_frame,
            text="Excel出力",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=48,
            width=120,
            fg_color="#10B981",
            hover_color="#059669",
            corner_radius=8
        )
        export_button.pack(side="right", padx=(0, 10))
        
        # 終了ボタン
        exit_button = ctk.CTkButton(
            buttons_frame,
            text="終了",
            font=ctk.CTkFont(family="Yu Gothic", size=14),
            height=48,
            width=100,
            fg_color="#EF4444",
            hover_color="#DC2626",
            corner_radius=8
        )
        exit_button.pack(side="right")
        
        return {
            'extract_button': extract_button,
            'reload_button': reload_button,
            'export_button': export_button,
            'exit_button': exit_button
        }
    
    @staticmethod
    def create_progress_section(parent):
        """進捗セクションの作成"""
        progress_frame = ctk.CTkFrame(parent, fg_color="#F8FAFC", corner_radius=12)
        progress_frame.pack(fill="x", pady=(0, 20), padx=20)
        
        # 進捗ラベル
        progress_label = ctk.CTkLabel(
            progress_frame,
            text="待機中...",
            font=ctk.CTkFont(family="Yu Gothic", size=14, weight="bold"),
            text_color="#374151"
        )
        progress_label.pack(pady=(15, 5))
        
        # プログレスバー
        progress_bar = ctk.CTkProgressBar(
            progress_frame,
            width=400,
            height=20,
            progress_color="#3B82F6"
        )
        progress_bar.pack(pady=(0, 15))
        progress_bar.set(0)
        
        return progress_frame, progress_label, progress_bar
    
    @staticmethod
    def create_data_display_section(parent):
        """データ表示セクションの作成"""
        data_frame = ctk.CTkFrame(parent, fg_color="white", corner_radius=12)
        data_frame.pack(fill="both", expand=True, pady=(0, 20), padx=20)
        
        # データ表示タイトル
        data_title = ctk.CTkLabel(
            data_frame,
            text="抽出データ",
            font=ctk.CTkFont(family="Yu Gothic", size=20, weight="bold"),
            text_color="#1E3A8A"
        )
        data_title.pack(pady=(20, 15))
        
        # データ表示フレーム
        data_display_frame = ctk.CTkFrame(data_frame, fg_color="white", corner_radius=8)
        data_display_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
        
        return data_frame, data_display_frame
    
    @staticmethod
    def create_data_table(parent):
        """データ表示用のテーブルを作成"""
        # テーブルとスクロールバー用のフレーム
        table_container = tk.Frame(parent, bg="white")
        table_container.pack(fill="both", expand=True, padx=15, pady=15)
        
        # Treeviewの作成（高さを増加）
        data_tree = ttk.Treeview(
            table_container,
            show="headings",
            height=20
        )
        
        # スクロールバーの追加
        v_scrollbar = ttk.Scrollbar(table_container, orient="vertical", command=data_tree.yview)
        h_scrollbar = ttk.Scrollbar(table_container, orient="horizontal", command=data_tree.xview)
        
        data_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        # グリッド配置でスクロールバーを適切に配置
        data_tree.grid(row=0, column=0, sticky="nsew")
        v_scrollbar.grid(row=0, column=1, sticky="ns")
        h_scrollbar.grid(row=1, column=0, sticky="ew")
        
        # グリッドの重み設定
        table_container.grid_rowconfigure(0, weight=1)
        table_container.grid_columnconfigure(0, weight=1)
        
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
        style.configure("Treeview.Negative", 
                        background="#FEE2E2",
                        foreground="#DC2626",
                        fieldbackground="#FEE2E2",
                        font=("MS Gothic", 10, "bold"))
        
        # タグの設定
        data_tree.tag_configure("negative", background="#FEE2E2", foreground="#DC2626")
        
        return data_tree
