"""
設定管理モジュール
環境変数からAccessデータベースの設定を読み込む
"""

import os
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
        self.env_file_path = env_file_path
        self._load_config()

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






