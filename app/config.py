"""
設定管理モジュール
環境変数からAccessデータベースの設定を読み込む
"""

import os
import sys
from pathlib import Path
from app.env_loader import load_env_file
from loguru import logger
import pyodbc


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
            # exe化されている場合
            # まず一時ディレクトリ（sys._MEIPASS）を確認（埋め込まれたファイル）
            temp_dir = Path(sys._MEIPASS)
            temp_file = temp_dir / Path(env_file_path).name
            if temp_file.exists():
                # 埋め込まれたファイルが見つかった場合
                self.env_file_path = str(temp_file)
            else:
                # 埋め込まれたファイルが見つからない場合、exeファイルの場所を基準にする
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
            self.process_master_path = os.getenv("PROCESS_MASTER_PATH")
            self.google_sheets_url = os.getenv("GOOGLE_SHEETS_URL")
            
            # Google認証情報ファイルのパスを解決（exe化対応）
            credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
            if credentials_path:
                self.google_sheets_credentials_path = self._get_resource_path(credentials_path)
            else:
                self.google_sheets_credentials_path = None
            
            self.google_sheets_url_cleaning = os.getenv("GOOGLE_SHEETS_URL_CLEANING")
            self.google_sheets_url_cleaning_instructions = os.getenv("GOOGLE_SHEETS_URL_CLEANING_INSTRUCTIONS")
            
            # 登録済み品番リストファイルのパスを取得
            # NAS上のUNCパスもそのまま使用可能（絶対パスの場合はそのまま返す）
            registered_products_path = os.getenv("REGISTERED_PRODUCTS_PATH")
            if registered_products_path:
                # UNCパス（\\で始まる）の場合はそのまま使用
                if registered_products_path.startswith('\\\\'):
                    self.registered_products_path = registered_products_path
                else:
                    # 相対パスの場合は_get_resource_pathで解決
                    self.registered_products_path = self._get_resource_path(registered_products_path)
            else:
                self.registered_products_path = None
            
            # ログディレクトリのパスを取得
            # NAS上のUNCパスもそのまま使用可能（絶対パスの場合はそのまま返す）
            log_dir_path = os.getenv("LOG_DIR_PATH")
            if log_dir_path:
                # UNCパス（\\で始まる）の場合はそのまま使用
                if log_dir_path.startswith('\\\\'):
                    self.log_dir_path = log_dir_path
                else:
                    # 相対パスの場合は_get_resource_pathで解決
                    self.log_dir_path = self._get_resource_path(log_dir_path)
            else:
                self.log_dir_path = None

            # 必須設定の確認
            if not self.access_file_path:
                raise ValueError("ACCESS_FILE_PATHが設定されていません")
            if not self.access_table_name:
                raise ValueError("ACCESS_TABLE_NAMEが設定されていません")

            # Accessファイルの存在確認
            if not Path(self.access_file_path).exists():
                raise FileNotFoundError(f"Accessファイルが見つかりません: {self.access_file_path}")

            logger.info("✅ 設定ファイルの読み込みが完了しました")
            logger.debug(f"Accessファイル: {self.access_file_path}")
            logger.debug(f"テーブル名: {self.access_table_name}")
            logger.debug(f"製品マスタ: {self.product_master_path}")
            logger.debug(f"検査員マスタ: {self.inspector_master_path}")
            logger.debug(f"スキルマスタ: {self.skill_master_path}")
            logger.debug(f"検査対象CSV: {self.inspection_target_csv_path}")
            logger.debug(f"工程マスタ: {self.process_master_path}")
            logger.debug(f"GoogleスプレッドシートURL: {self.google_sheets_url}")
            logger.debug(f"Google認証情報: {self.google_sheets_credentials_path}")
            logger.debug(f"洗浄二次処理依頼URL: {self.google_sheets_url_cleaning}")
            logger.debug(f"洗浄指示URL: {self.google_sheets_url_cleaning_instructions}")
            logger.debug(f"登録済み品番リスト: {self.registered_products_path}")
            logger.debug(f"ログディレクトリ: {self.log_dir_path}")

        except Exception as e:
            logger.error(f"設定の読み込みに失敗しました: {e}")
            raise

    def _get_available_access_drivers(self) -> list[str]:
        """
        システムにインストールされているAccess用ODBCドライバーを取得
        
        Returns:
            利用可能なAccessドライバー名のリスト
        """
        available_drivers = []
        try:
            drivers = pyodbc.drivers()
            logger.info(f"システムにインストールされている全ODBCドライバー数: {len(drivers)}")
            
            # Access関連のドライバーを検索（より広範囲に）
            access_keywords = ['Access', 'ACE', 'Jet']
            for driver in drivers:
                driver_lower = driver.lower()
                if any(keyword.lower() in driver_lower for keyword in access_keywords):
                    available_drivers.append(driver)
            
            logger.info(f"利用可能なAccessドライバー: {available_drivers}")
            
            # デバッグ用：ドライバーが見つからない場合の詳細情報
            if not available_drivers:
                logger.warning(
                    f"Access用ODBCドライバーが見つかりませんでした。\n"
                    f"Pythonのビット数: {sys.maxsize > 2**32 and '64bit' or '32bit'}\n"
                    f"システムにインストールされている全ドライバー（最初の20個）: {drivers[:20]}"
                )
        except Exception as e:
            logger.warning(f"ドライバー検出中にエラーが発生しました: {e}")
        
        return available_drivers

    def _get_driver_candidates(self) -> list[str]:
        """
        試行するドライバー候補のリストを取得（32bit/64bit両対応）
        
        Returns:
            ドライバー名のリスト（優先順位順）
        """
        candidates = []
        
        # 1. 設定ファイルで指定されたドライバーを最初に試行
        if self.db_driver:
            candidates.append(self.db_driver)
        
        # 2. 利用可能なドライバーを追加（最優先：実際にインストールされているもの）
        available_drivers = self._get_available_access_drivers()
        for driver in available_drivers:
            if driver not in candidates:
                candidates.append(driver)
        
        # 3. 一般的なODBCドライバー名をフォールバックとして追加
        # 重要: OLEDBプロバイダー名（Microsoft.ACE.OLEDB.*）はODBC接続文字列では使用できないため削除
        # 32bit/64bitで異なる可能性があるため、バリエーションを追加
        common_odbc_drivers = [
            "Microsoft Access Driver (*.mdb, *.accdb)",  # 64bit版（一般的）
            "Microsoft Access Driver (*.mdb)",            # 旧版
            # 以下は環境によって異なる可能性があるバリエーション
            "Microsoft Access Driver (*.mdb, *.accdb) 2016",
            "Microsoft Access Driver (*.mdb, *.accdb) 2010",
        ]
        for driver in common_odbc_drivers:
            if driver not in candidates:
                candidates.append(driver)
        
        logger.info(f"試行するドライバー候補（優先順位順）: {candidates}")
        return candidates

    def get_connection_string(self, driver_name: str = None) -> str:
        """
        データベース接続文字列を生成
        
        Args:
            driver_name: 使用するドライバー名（Noneの場合は設定値を使用）
        
        Returns:
            接続文字列
        """
        # Accessファイルのパスを正規化
        normalized_path = str(Path(self.access_file_path).resolve())
        
        # ドライバー名を決定
        driver = driver_name or self.db_driver

        connection_string = (
            f"DRIVER={{{driver}}};"
            f"DBQ={normalized_path};"
            "ExtendedAnsiSQL=1;"
        )

        return connection_string

    def get_connection(self) -> pyodbc.Connection:
        """
        データベース接続を取得（自動的に利用可能なドライバーを検出）
        
        Returns:
            pyodbc.Connection: データベース接続オブジェクト
        
        Raises:
            Exception: すべてのドライバーで接続に失敗した場合
        """
        candidates = self._get_driver_candidates()
        last_error = None
        
        for driver in candidates:
            try:
                connection_string = self.get_connection_string(driver_name=driver)
                logger.info(f"ドライバー '{driver}' で接続を試行中...")
                connection = pyodbc.connect(connection_string)
                logger.info(f"ドライバー '{driver}' で接続に成功しました")
                return connection
            except pyodbc.Error as e:
                error_code = e.args[0] if e.args else ""
                logger.warning(f"ドライバー '{driver}' での接続に失敗: {error_code}")
                last_error = e
                continue
            except Exception as e:
                logger.warning(f"ドライバー '{driver}' での接続中に予期しないエラー: {e}")
                last_error = e
                continue
        
        # すべてのドライバーで失敗した場合
        error_msg = (
            f"すべてのAccessドライバーでの接続に失敗しました。\n"
            f"試行したドライバー: {candidates}\n"
            f"最後のエラー: {last_error}"
        )
        logger.error(error_msg)
        raise ConnectionError(error_msg) from last_error

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







