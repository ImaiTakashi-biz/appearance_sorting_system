"""
データベースハンドラー - 出荷検査データ抽出システム
データベース操作を管理
"""

import pandas as pd
import pyodbc
from loguru import logger
from config import DatabaseConfig


class DatabaseHandler:
    """データベース操作を管理するクラス"""
    
    def __init__(self, config: DatabaseConfig):
        """初期化"""
        self.config = config
        self.connection = None
    
    def connect(self):
        """データベースに接続"""
        try:
            connection_string = self.config.get_connection_string()
            self.connection = pyodbc.connect(connection_string)
            logger.info("データベース接続が完了しました")
            return True
        except Exception as e:
            logger.error(f"データベース接続エラー: {str(e)}")
            return False
    
    def disconnect(self):
        """データベース接続を切断"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("データベース接続を切断しました")
    
    def get_table_columns(self, table_name):
        """テーブルの列情報を取得"""
        try:
            columns_query = f"SELECT TOP 1 * FROM [{table_name}]"
            sample_df = pd.read_sql(columns_query, self.connection)
            columns = list(sample_df.columns)
            logger.info(f"テーブルの列: {columns}")
            return columns
        except Exception as e:
            logger.error(f"テーブル列取得エラー: {str(e)}")
            return []
    
    def extract_main_data(self, start_date, end_date):
        """メインデータの抽出"""
        try:
            # まず、テーブルの列情報を取得
            columns_info = self.get_table_columns(self.config.access_table_name)
            logger.info(f"テーブル列情報: {columns_info}")
            
            # 利用可能な列から必要な列を選択（「処理」列は存在しないため削除）
            available_columns = [col for col in columns_info if col in [
                "品番", "品名", "客先", "出荷予定日", "出荷数", "出荷 数", "在庫数"
            ]]
            
            if not available_columns:
                logger.error("必要な列が見つかりません")
                return pd.DataFrame()
            
            # 列名をエスケープ
            escaped_columns = [f"[{col}]" for col in available_columns]
            columns_str = ", ".join(escaped_columns)
            
            # クエリを構築
            query = f"SELECT {columns_str} FROM [{self.config.access_table_name}]"
            logger.info(f"実行クエリ: {query}")
            
            # データを取得
            df = pd.read_sql(query, self.connection)
            
            if df.empty:
                logger.warning("データが取得できませんでした")
                return pd.DataFrame()
            
            # 日付でフィルタリング
            if '出荷予定日' in df.columns:
                df['出荷予定日'] = pd.to_datetime(df['出荷予定日'], errors='coerce')
                # 日付型を統一（datetime64[ns]に変換）
                start_datetime = pd.to_datetime(start_date)
                end_datetime = pd.to_datetime(end_date)
                df = df[(df['出荷予定日'] >= start_datetime) & (df['出荷予定日'] <= end_datetime)]
            
            # 列名を統一（スペースを削除）
            if '出荷 数' in df.columns:
                df = df.rename(columns={'出荷 数': '出荷数'})
            
            logger.info(f"メインデータ抽出完了: {len(df)}件")
            return df
            
        except Exception as e:
            logger.error(f"メインデータ抽出エラー: {str(e)}")
            return pd.DataFrame()
    
    
    def get_packaging_quantities(self, product_numbers):
        """梱包工程データを取得"""
        try:
            if not product_numbers:
                return pd.DataFrame()
            
            # 品番リストを文字列に変換
            product_list = "', '".join(product_numbers)
            
            # 梱包工程データを取得
            packaging_query = f"""
                SELECT 品番, 数量
                FROM [t_現品票履歴]
                WHERE 品番 IN ('{product_list}')
                AND 現在工程名 LIKE '%梱包%'
            """
            
            packaging_df = pd.read_sql(packaging_query, self.connection)
            
            # 品番ごとに数量を合計
            if not packaging_df.empty:
                packaging_df['数量'] = pd.to_numeric(packaging_df['数量'], errors='coerce').fillna(0)
                packaging_summary = packaging_df.groupby('品番')['数量'].sum().reset_index()
                packaging_summary.columns = ['品番', '梱包・完了']
                logger.info(f"梱包工程データを取得しました: {len(packaging_summary)}件")
                return packaging_summary
            else:
                logger.info("梱包工程データが見つかりませんでした")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"梱包工程データ取得エラー: {str(e)}")
            return pd.DataFrame()
    
    def get_available_lots(self, product_numbers):
        """利用可能なロットを取得"""
        try:
            if not product_numbers:
                return pd.DataFrame()
            
            # 品番リストを文字列に変換
            product_list = "', '".join(product_numbers)
            
            # t_現品票履歴テーブルの列情報を取得
            lots_columns_info = self.get_table_columns("t_現品票履歴")
            logger.info(f"t_現品票履歴テーブル列情報: {lots_columns_info}")
            
            # 利用可能な列から必要な列を選択
            available_lots_columns = [col for col in lots_columns_info if col in [
                "品番", "数量", "指示日", "号機", "現在工程名", "現在工程二次処理", "生産ロットID"
            ]]
            
            if not available_lots_columns:
                logger.error("t_現品票履歴テーブルに必要な列が見つかりません")
                return pd.DataFrame()
            
            # 列名をエスケープ
            escaped_lots_columns = [f"[{col}]" for col in available_lots_columns]
            lots_columns_str = ", ".join(escaped_lots_columns)
            
            # 利用可能なロットを取得（完了・梱包以外）
            lots_query = f"""
                SELECT {lots_columns_str}
                FROM [t_現品票履歴]
                WHERE 品番 IN ('{product_list}')
                AND 現在工程名 NOT LIKE '%完了%'
                AND 現在工程名 NOT LIKE '%梱包%'
            """
            logger.info(f"ロット取得クエリ: {lots_query}")
            
            lots_df = pd.read_sql(lots_query, self.connection)
            
            if not lots_df.empty:
                lots_df['指示日'] = pd.to_datetime(lots_df['指示日'], errors='coerce')
                lots_df = lots_df.dropna(subset=['指示日'])
                logger.info(f"利用可能なロットを取得しました: {len(lots_df)}件")
                return lots_df
            else:
                logger.info("利用可能なロットが見つかりませんでした")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"ロット取得エラー: {str(e)}")
            return pd.DataFrame()
    
    def merge_packaging_data(self, main_df, packaging_df):
        """梱包データをメインデータに結合"""
        try:
            if packaging_df.empty:
                main_df['梱包・完了'] = 0
                return main_df
            
            # メインデータに梱包データを結合
            merged_df = main_df.merge(
                packaging_df, 
                on='品番', 
                how='left'
            )
            
            # 梱包・完了がNaNの場合は0に設定
            merged_df['梱包・完了'] = merged_df['梱包・完了'].fillna(0).astype(int)
            
            logger.info(f"梱包工程データを結合しました: {len(merged_df)}件")
            return merged_df
            
        except Exception as e:
            logger.error(f"梱包データ結合エラー: {str(e)}")
            main_df['梱包・完了'] = 0
            return main_df
    
    def calculate_shortage(self, df):
        """不足数を計算"""
        try:
            # 数値列を数値型に変換
            numeric_columns = ['出荷数', '在庫数', '梱包・完了']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # 不足数を計算: (在庫数 + 梱包・完了) - 出荷数
            df['不足数'] = (df['在庫数'] + df['梱包・完了']) - df['出荷数']
            
            logger.info("不足数を計算しました")
            return df
            
        except Exception as e:
            logger.error(f"不足数計算エラー: {str(e)}")
            df['不足数'] = 0
            return df
