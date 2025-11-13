"""
設定管理モジュール
環境変数からAccessデータベースの設定を読み込む
"""

import os
import sys
from pathlib import Path
from app.env_loader import load_env_file
from loguru import logger


class DatabaseConfig:
    """データベース設定管理クラス"""

    def __init__(self, env_file_path: str = "config.env"):
        """
        初期化

        Args:
            env_file_path (str): 環境変数ファイルのパス
        """
        # exe化されている場合とそうでない場合でconfig.envのパスを決定
        if getattr(sys, 'frozen', False):
            # exe化されている場合：exeファイルの場所を基準にする
            application_path = Path(sys.executable).parent
            self.env_file_path = str(application_path / env_file_path)
        else:
            # 通常のPython実行の場合：相対パスまたは絶対パスを使用
            self.env_file_path = env_file_path
        
        self._load_config()
    
    def _get_resource_path(self, file_path: str) -> str:
        """
        exe化されている場合とそうでない場合でリソースファイルのパスを解決
        
        Args:
            file_path: ファイルパス（相対パスまたはファイル名）
            
        Returns:
            解決されたファイルパス
        """
        if getattr(sys, 'frozen', False):
            # exe化されている場合
            # まず一時ディレクトリ（sys._MEIPASS）を確認（埋め込まれたファイル）
            temp_dir = Path(sys._MEIPASS)
            temp_file = temp_dir / Path(file_path).name
            if temp_file.exists():
                return str(temp_file)
            
            # 次にexeと同じ階層を確認
            exe_dir = Path(sys.executable).parent
            exe_file = exe_dir / Path(file_path).name
            if exe_file.exists():
                return str(exe_file)
            
            # 見つからない場合は元のパスを返す
            return file_path
        else:
            # 通常のPython実行の場合
            return file_path

    def _load_config(self):
        """設定ファイルを読み込み"""
        try:
            # 環境変数ファイルの存在確認
            if not Path(self.env_file_path).exists():
                raise FileNotFoundError(f"設定ファイルが見つかりません: {self.env_file_path}")

            # 環境変数を読み込み
            load_env_file(self.env_file_path)

            # 設定値を取得
            self.access_file_path = os.getenv("ACCESS_FILE_PATH")
            self.access_table_name = os.getenv("ACCESS_TABLE_NAME")
            self.db_driver = os.getenv("DB_DRIVER", "Microsoft Access Driver (*.mdb, *.accdb)")
            self.product_master_path = os.getenv("PRODUCT_MASTER_PATH")
            self.inspector_master_path = os.getenv("INSPECTOR_MASTER_PATH")
            self.skill_master_path = os.getenv("SKILL_MASTER_PATH")
            self.inspection_target_csv_path = os.getenv("INSPECTION_TARGET_CSV_PATH")
            self.google_sheets_url = os.getenv("GOOGLE_SHEETS_URL")
            
            # Google認証情報ファイルのパスを解決（exe化対応）
            credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
            if credentials_path:
                self.google_sheets_credentials_path = self._get_resource_path(credentials_path)
            else:
                self.google_sheets_credentials_path = None
            
            self.google_sheets_url_cleaning = os.getenv("GOOGLE_SHEETS_URL_CLEANING")
            self.google_sheets_url_cleaning_instructions = os.getenv("GOOGLE_SHEETS_URL_CLEANING_INSTRUCTIONS")

            # 必須設定の確認
            if not self.access_file_path:
                raise ValueError("ACCESS_FILE_PATHが設定されていません")
            if not self.access_table_name:
                raise ValueError("ACCESS_TABLE_NAMEが設定されていません")

            # Accessファイルの存在確認
            if not Path(self.access_file_path).exists():
                raise FileNotFoundError(f"Accessファイルが見つかりません: {self.access_file_path}")

            logger.info("設定ファイルの読み込みが完了しました")
            logger.info(f"Accessファイル: {self.access_file_path}")
            logger.info(f"テーブル名: {self.access_table_name}")
            logger.info(f"製品マスタ: {self.product_master_path}")
            logger.info(f"検査員マスタ: {self.inspector_master_path}")
            logger.info(f"スキルマスタ: {self.skill_master_path}")
            logger.info(f"検査対象CSV: {self.inspection_target_csv_path}")
            logger.info(f"GoogleスプレッドシートURL: {self.google_sheets_url}")
            logger.info(f"Google認証情報: {self.google_sheets_credentials_path}")
            logger.info(f"洗浄二次処理依頼URL: {self.google_sheets_url_cleaning}")
            logger.info(f"洗浄指示URL: {self.google_sheets_url_cleaning_instructions}")

        except Exception as e:
            logger.error(f"設定の読み込みに失敗しました: {e}")
            raise

    def get_connection_string(self) -> str:
        """データベース接続文字列を生成"""
        # Accessファイルのパスを正規化
        normalized_path = str(Path(self.access_file_path).resolve())

        connection_string = (
            f"DRIVER={{{self.db_driver}}};"
            f"DBQ={normalized_path};"
            "ExtendedAnsiSQL=1;"
        )

        return connection_string

    def validate_config(self) -> bool:
        """設定の妥当性を検証"""
        try:
            # 必須設定の確認
            if not all([self.access_file_path, self.access_table_name]):
                return False

            # ファイル存在確認
            if not Path(self.access_file_path).exists():
                return False

            return True

        except Exception as e:
            logger.error(f"設定の検証に失敗しました: {e}")
            return False







