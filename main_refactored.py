"""
出荷検査データ抽出システム - メインアプリケーション（リファクタリング版）
機能別に分離されたモジュールを使用
"""

import customtkinter as ctk
import tkinter as tk
from tkinter import messagebox, ttk
import threading
from datetime import datetime, date
from loguru import logger
import pandas as pd

# 分離されたモジュールをインポート
from config import DatabaseConfig
from ui_components import UIComponents
from database_handler import DatabaseHandler
from calendar_widget import CalendarWidget
from lot_assignment import LotAssignment
from excel_exporter import ExcelExporter


class ModernDataExtractorUI:
    """出荷検査データ抽出システムのメインUIクラス"""
    
    def __init__(self):
        """UIの初期化"""
        # 日本語ロケール設定
        try:
            import locale
            locale.setlocale(locale.LC_TIME, 'ja_JP.UTF-8')
        except:
            pass
        
        # CustomTkinterの設定
        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")
        
        # メインウィンドウの作成
        self.root = ctk.CTk()
        self.root.title("出荷検査データ抽出システム")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 700)
        self.root.configure(fg_color=("white", "white"))
        
        # 変数の初期化
        self.config = None
        self.is_extracting = False
        self.data_tree = None
        
        # モジュールの初期化
        self.db_handler = None
        self.lot_assignment = LotAssignment()
        self.calendar_widget = CalendarWidget(self.root, self.on_date_selected)
        self.excel_exporter = ExcelExporter()
        
        
        # データ保存用変数
        self.current_main_data = None
        self.current_assignment_data = None
        
        # UIの構築
        self.setup_ui()
        
        # ログ設定
        self.setup_logging()
        
        # 設定の読み込み
        self.load_config()
        
        # ウィンドウを中央に配置
        self.center_window()
    
    def setup_ui(self):
        """UIのセットアップ"""
        # メインスクロールフレーム
        self.main_scroll_frame = ctk.CTkScrollableFrame(
            self.root, 
            fg_color="white",
            scrollbar_button_color="#E5E7EB",
            scrollbar_button_hover_color="#D1D5DB"
        )
        self.main_scroll_frame.pack(fill="both", expand=True)
        
        # UIコンポーネントの作成
        self.create_ui_components()
    
    def create_ui_components(self):
        """UIコンポーネントを作成"""
        # タイトルセクション
        UIComponents.create_title_section(self.main_scroll_frame)
        
        # データベース状態セクション
        self.status_frame, self.status_label = UIComponents.create_database_status_section(self.main_scroll_frame)
        
        # 日付選択セクション
        self.date_frame, self.period_frame = UIComponents.create_date_selection_section(self.main_scroll_frame)
        
        # 期間選択UI
        self.date_components = UIComponents.create_period_selector(self.period_frame)
        
        # カレンダーボタンのイベントを設定
        self.date_components['start_calendar_button'].configure(
            command=lambda: self.show_calendar_with_current_date("start")
        )
        self.date_components['end_calendar_button'].configure(
            command=lambda: self.show_calendar_with_current_date("end")
        )
        
        # ボタンセクション
        self.button_components = UIComponents.create_button_section(self.main_scroll_frame)
        
        # ボタンのイベントを設定
        self.button_components['extract_button'].configure(command=self.start_extraction)
        self.button_components['reload_button'].configure(command=self.reload_config)
        self.button_components['export_button'].configure(command=self.export_to_excel)
        self.button_components['exit_button'].configure(command=self.root.quit)
        
        # 進捗セクション
        self.progress_frame, self.progress_label, self.progress_bar = UIComponents.create_progress_section(self.main_scroll_frame)
        
        # データ表示セクション
        self.data_frame, self.data_display_frame = UIComponents.create_data_display_section(self.main_scroll_frame)
        
        # データテーブル
        self.data_tree = UIComponents.create_data_table(self.data_display_frame)
    
    def setup_logging(self):
        """ログ設定"""
        logger.remove()
        logger.add(
            lambda msg: print(msg, end=""),
            format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
            level="INFO"
        )
    
    def center_window(self):
        """ウィンドウを画面中央に配置"""
        window_width = 1200
        window_height = 800
        
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2
        
        self.root.geometry(f"{window_width}x{window_height}+{x}+{y}")
    
    def load_config(self):
        """設定の読み込み"""
        try:
            self.config = DatabaseConfig()
            if self.config.validate_config():
                self.status_label.configure(
                    text="データベース接続状態: 接続可能",
                    text_color="#10B981"
                )
                self.db_handler = DatabaseHandler(self.config)
            else:
                self.status_label.configure(
                    text="データベース接続状態: 接続不可",
                    text_color="#EF4444"
                )
        except Exception as e:
            self.status_label.configure(
                text="データベース接続状態: エラー",
                text_color="#EF4444"
            )
            logger.error(f"設定読み込みエラー: {str(e)}")
    
    def reload_config(self):
        """設定のリロード"""
        self.load_config()
        messagebox.showinfo("完了", "設定をリロードしました")
    
    def show_calendar_with_current_date(self, date_type):
        """現在の日付を取得してカレンダーを表示"""
        try:
            current_date_str = self.date_components[f'{date_type}_date_entry'].get().strip()
            current_date = None
            
            if current_date_str:
                try:
                    # 現在の日付文字列を解析
                    current_date = datetime.strptime(current_date_str, "%Y/%m/%d").date()
                except ValueError:
                    # 日付が無効な場合は今日の日付を使用
                    current_date = date.today()
            else:
                # 日付が空の場合は今日の日付を使用
                current_date = date.today()
            
            # カレンダーを表示
            self.calendar_widget.show_calendar_popup(date_type, current_date)
            
        except Exception as e:
            logger.error(f"カレンダー表示エラー: {str(e)}")
            # エラーの場合は今日の日付でカレンダーを表示
            self.calendar_widget.show_calendar_popup(date_type, date.today())
    
    def on_date_selected(self, date_type, selected_date):
        """日付選択時のコールバック"""
        try:
            if date_type == "start":
                self.date_components['start_date_entry'].delete(0, "end")
                self.date_components['start_date_entry'].insert(0, selected_date.strftime("%Y/%m/%d"))
                logger.info(f"開始日を設定: {selected_date.strftime('%Y/%m/%d')}")
            else:
                self.date_components['end_date_entry'].delete(0, "end")
                self.date_components['end_date_entry'].insert(0, selected_date.strftime("%Y/%m/%d"))
                logger.info(f"終了日を設定: {selected_date.strftime('%Y/%m/%d')}")
        except Exception as e:
            logger.error(f"日付設定エラー: {str(e)}")
            messagebox.showerror("エラー", f"日付の設定中にエラーが発生しました: {str(e)}")
    
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
    
    def parse_date_input(self):
        """日付入力の解析"""
        try:
            start_date_str = self.date_components['start_date_entry'].get().strip()
            end_date_str = self.date_components['end_date_entry'].get().strip()
            
            if not start_date_str or not end_date_str:
                messagebox.showerror("エラー", "開始日と終了日を入力してください")
                return None, None
            
            start_date = datetime.strptime(start_date_str, "%Y/%m/%d").date()
            end_date = datetime.strptime(end_date_str, "%Y/%m/%d").date()
            
            if start_date > end_date:
                messagebox.showerror("エラー", "開始日は終了日より前の日付を入力してください")
                return None, None
            
            return start_date, end_date
            
        except ValueError as e:
            messagebox.showerror("日付エラー", str(e))
            return None, None
    
    def start_extraction(self):
        """データ抽出の開始"""
        if self.is_extracting:
            messagebox.showwarning("警告", "既にデータ抽出が実行中です")
            return
        
        if not self.config or not self.config.validate_config():
            messagebox.showerror("エラー", "データベース設定が無効です。設定を確認してください。")
            return
        
        start_date, end_date = self.parse_date_input()
        if start_date is None or end_date is None:
            return
        
        # バックグラウンドでデータ抽出を実行
        self.is_extracting = True
        self.button_components['extract_button'].configure(state="disabled", text="抽出中...")
        self.progress_bar.set(0)
        self.progress_label.configure(text="データベースに接続中...")
        
        thread = threading.Thread(
            target=self.extract_data_thread,
            args=(start_date, end_date)
        )
        thread.daemon = True
        thread.start()
    
    def extract_data_thread(self, start_date, end_date):
        """データ抽出のスレッド処理"""
        try:
            self.log_message(f"データ抽出を開始します")
            self.log_message(f"抽出期間: {start_date} ～ {end_date}")
            
            # データベース接続
            self.update_progress(0.1, "データベースに接続中...")
            if not self.db_handler.connect():
                raise Exception("データベース接続に失敗しました")
            
            # メインデータの抽出
            self.update_progress(0.3, "メインデータを抽出中...")
            df = self.db_handler.extract_main_data(start_date, end_date)
            
            if df.empty:
                raise Exception("抽出対象のデータが見つかりませんでした")
            
            # 梱包工程データの取得と結合
            self.update_progress(0.5, "梱包工程データを取得中...")
            product_numbers = df['品番'].unique().tolist()
            packaging_df = self.db_handler.get_packaging_quantities(product_numbers)
            df = self.db_handler.merge_packaging_data(df, packaging_df)
            
            # 不足数の計算
            self.update_progress(0.7, "不足数を計算中...")
            df = self.db_handler.calculate_shortage(df)
            
            # データの表示
            self.update_progress(0.8, "データを表示中...")
            self.display_data(df)
            
            # データを保存（エクスポート用）
            self.current_main_data = df
            
            # ロット割り当て処理
            self.update_progress(0.9, "ロット割り当て処理中...")
            self.process_lot_assignment(df)
            
            # 完了
            self.update_progress(1.0, "データ抽出が完了しました")
            self.log_message(f"処理完了! {len(df)}件のデータを表示しました")
            
            self.root.after(0, lambda: messagebox.showinfo(
                "完了", 
                f"データ抽出が完了しました!\n\n"
                f"抽出件数: {len(df)}件\n"
                f"データがアプリ上に表示されました"
            ))
            
        except Exception as e:
            error_msg = f"データ抽出中にエラーが発生しました: {str(e)}"
            self.log_message(f"エラー: {error_msg}")
            self.update_progress(0, "エラーが発生しました")
            
            self.root.after(0, lambda: messagebox.showerror("エラー", error_msg))
            
        finally:
            # データベース接続を切断
            if self.db_handler:
                self.db_handler.disconnect()
            
            # UIの状態をリセット
            self.root.after(0, self.reset_ui_state)
    
    def display_data(self, df):
        """データの表示"""
        try:
            # 既存のデータをクリア
            for item in self.data_tree.get_children():
                self.data_tree.delete(item)
            
            # 列の定義
            columns = ["品番", "品名", "客先", "出荷予定日", "出荷数", "在庫数", "梱包・完了", "不足数"]
            self.data_tree["columns"] = columns
            
            # 列の設定
            column_widths = {
                "品番": 100, "品名": 200, "客先": 150, "出荷予定日": 100,
                "出荷数": 80, "在庫数": 80, "梱包・完了": 100, "不足数": 80
            }
            
            # 右詰めにする数値列
            numeric_columns = ["出荷数", "在庫数", "梱包・完了", "不足数"]
            
            for col in columns:
                width = column_widths.get(col, 120)
                anchor = "e" if col in numeric_columns else "w"
                self.data_tree.column(col, width=width, anchor=anchor)
                self.data_tree.heading(col, text=col)
            
            # データの挿入
            for index, row in df.iterrows():
                values = []
                for col in columns:
                    if pd.notna(row[col]):
                        if col == '出荷予定日':
                            try:
                                date_value = pd.to_datetime(row[col])
                                values.append(date_value.strftime('%Y/%m/%d'))
                            except:
                                values.append(str(row[col]))
                        elif col in numeric_columns:
                            try:
                                values.append(str(int(row[col])))
                            except:
                                values.append(str(row[col]))
                        else:
                            values.append(str(row[col]))
                    else:
                        values.append("")
                
                # 不足数がマイナスの場合はタグを設定
                tags = []
                if col == '不足数' and pd.notna(row[col]) and row[col] < 0:
                    tags.append("negative")
                
                self.data_tree.insert("", "end", values=values, tags=tags)
            
        except Exception as e:
            self.log_message(f"データ表示中にエラーが発生しました: {str(e)}")
    
    def process_lot_assignment(self, main_df):
        """ロット割り当て処理"""
        try:
            # 不足数がマイナスのデータを抽出
            shortage_df = self.lot_assignment.get_shortage_products(main_df)
            
            if shortage_df.empty:
                self.log_message("不足数がマイナスのデータがありません")
                return
            
            # 利用可能なロットを取得
            product_numbers = shortage_df['品番'].unique().tolist()
            lots_df = self.db_handler.get_available_lots(product_numbers)
            
            if lots_df.empty:
                self.log_message("利用可能なロットがありません")
                return
            
            # ロット割り当てを実行
            assignment_df = self.lot_assignment.assign_lots_to_shortage(shortage_df, lots_df)
            
            if not assignment_df.empty:
                # ソート
                assignment_df = self.lot_assignment.sort_assignment_results(assignment_df)
                
                # ロット割り当て結果を表示
                self.display_lot_assignment_table(assignment_df)
                
                # ロット割り当てデータを保存（エクスポート用）
                self.current_assignment_data = assignment_df
            else:
                self.log_message("ロット割り当て結果がありません")
                
        except Exception as e:
            self.log_message(f"ロット割り当て処理中にエラーが発生しました: {str(e)}")
    
    def display_lot_assignment_table(self, assignment_df):
        """ロット割り当て結果テーブルを表示"""
        try:
            # ロット割り当てセクションを作成
            lot_frame = ctk.CTkFrame(self.main_scroll_frame, fg_color="white", corner_radius=12)
            lot_frame.pack(fill="x", pady=(0, 20), padx=20)
            
            # タイトル
            lot_title = ctk.CTkLabel(
                lot_frame,
                text="ロット割り当て結果",
                font=ctk.CTkFont(family="Yu Gothic", size=20, weight="bold"),
                text_color="#1E3A8A"
            )
            lot_title.pack(pady=(20, 15))
            
            # テーブルフレーム
            lot_table_frame = ctk.CTkFrame(lot_frame, fg_color="white", corner_radius=8)
            lot_table_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
            
            # ロット割り当てテーブルを作成
            self.create_lot_assignment_table(lot_table_frame, assignment_df)
            
        except Exception as e:
            self.log_message(f"ロット割り当てセクション作成中にエラーが発生しました: {str(e)}")
    
    def create_lot_assignment_table(self, parent, assignment_df):
        """ロット割り当て結果テーブルを作成"""
        try:
            # テーブルとスクロールバー用のフレーム
            lot_table_container = tk.Frame(parent, bg="white")
            lot_table_container.pack(fill="both", expand=True, padx=15, pady=15)
            
            # テーブル作成
            lot_tree = ttk.Treeview(
                lot_table_container,
                show="headings",
                height=15
            )
            
            # スクロールバー
            lot_v_scrollbar = ttk.Scrollbar(lot_table_container, orient="vertical", command=lot_tree.yview)
            lot_h_scrollbar = ttk.Scrollbar(lot_table_container, orient="horizontal", command=lot_tree.xview)
            lot_tree.configure(yscrollcommand=lot_v_scrollbar.set, xscrollcommand=lot_h_scrollbar.set)
            
            # グリッド配置
            lot_tree.grid(row=0, column=0, sticky="nsew")
            lot_v_scrollbar.grid(row=0, column=1, sticky="ns")
            lot_h_scrollbar.grid(row=1, column=0, sticky="ew")
            
            # グリッドの重み設定
            lot_table_container.grid_rowconfigure(0, weight=1)
            lot_table_container.grid_columnconfigure(0, weight=1)
            
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
                "生産ロットID", "ロット数量", "指示日", "号機", "現在工程名", "現在工程二次処理"
            ]
            lot_tree["columns"] = lot_columns
            
            # 列の設定
            lot_column_widths = {
                "出荷予定日": 100, "品番": 100, "品名": 200, "客先": 150,
                "出荷数": 80, "在庫数": 80, "在梱包数": 100, "不足数": 80,
                "生産ロットID": 120, "ロット数量": 100, "指示日": 100, "号機": 80,
                "現在工程名": 150, "現在工程二次処理": 150
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
    
    def update_progress(self, value, text):
        """進捗の更新"""
        self.root.after(0, lambda: self.progress_bar.set(value))
        self.root.after(0, lambda: self.progress_label.configure(text=text))
    
    def log_message(self, message):
        """ログメッセージの出力"""
        print(message)
        logger.info(message)
    
    def reset_ui_state(self):
        """UIの状態をリセット"""
        self.is_extracting = False
        self.button_components['extract_button'].configure(state="normal", text="データ抽出開始")
        self.progress_bar.set(0)
        self.progress_label.configure(text="待機中...")
    
    def run(self):
        """アプリケーションの実行"""
        self.log_message("出荷検査データ抽出システムを起動しました")
        self.log_message("設定を確認してください")
        
        if self.config and self.config.validate_config():
            pass
        
        self.root.mainloop()


def main():
    """メイン関数"""
    app = ModernDataExtractorUI()
    app.run()


if __name__ == "__main__":
    main()
