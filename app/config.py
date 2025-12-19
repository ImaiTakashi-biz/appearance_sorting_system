"""
設定管理モジュール
環境変数からAccessデータベースの設定を読み込む
"""

from typing import Optional, List
import os
import sys
from pathlib import Path
from app.env_loader import load_env_file
from app.utils.path_resolver import resolve_resource_path
from loguru import logger

# ログ分類（app_.logの視認性向上）
logger = logger.bind(channel="CFG")
import pyodbc


class DatabaseConfig:
    """データベース設定管理クラス"""
    
    # キャッシュ設定定数
    CONNECTION_CACHE_TTL = 300  # 5分間（秒）
    
    # クラス変数として接続をキャッシュ（高速化）
    _connection_cache = None
    _connection_cache_timestamp = None
    _connection_cache_ttl = CONNECTION_CACHE_TTL

    def __init__(self, env_file_path: str = "config.env") -> None:
        """
        初期化

        Args:
            env_file_path: 環境変数ファイルのパス
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
        return resolve_resource_path(file_path)

    def _load_config(self) -> None:
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
            self.shipping_stock_table_name = os.getenv("SHIPPING_STOCK_TABLE_NAME")
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

            # 起動時のログ出力を削減（高速化のため）
            # エラー時のみ詳細ログを出力

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
            logger.debug(f"システムにインストールされている全ODBCドライバー数: {len(drivers)}")
            
            # Access関連のドライバーを検索（より広範囲に）
            access_keywords = ['Access', 'ACE', 'Jet']
            for driver in drivers:
                driver_lower = driver.lower()
                if any(keyword.lower() in driver_lower for keyword in access_keywords):
                    available_drivers.append(driver)
            
            logger.debug(f"利用可能なAccessドライバー: {available_drivers}")
            
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
        
        # ログ出力を削減（高速化のため）
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
            "ReadOnly=1;"
        )

        return connection_string

    def get_connection(self, timeout: int = 30) -> pyodbc.Connection:
        """
        データベース接続を取得（キャッシュ機能付き・高速化）
        
        Args:
            timeout: 接続タイムアウト（秒、現在は未使用だが将来の拡張用）
        
        Returns:
            pyodbc.Connection: データベース接続オブジェクト
        
        Raises:
            ConnectionError: すべてのドライバーで接続に失敗した場合
        """
        import time
        
        # キャッシュが有効な場合は再利用（高速化）
        if (DatabaseConfig._connection_cache is not None and 
            DatabaseConfig._connection_cache_timestamp is not None):
            elapsed = time.time() - DatabaseConfig._connection_cache_timestamp
            if elapsed < DatabaseConfig.CONNECTION_CACHE_TTL:
                try:
                    # 接続が有効か確認（高速チェック）
                    DatabaseConfig._connection_cache.execute("SELECT 1")
                    return DatabaseConfig._connection_cache
                except:
                    # 接続が無効な場合はキャッシュをクリア
                    DatabaseConfig._connection_cache = None
                    DatabaseConfig._connection_cache_timestamp = None
        
        # 新しい接続を取得
        candidates = self._get_driver_candidates()
        last_error = None
        
        for driver in candidates:
            try:
                connection_string = self.get_connection_string(driver_name=driver)
                connection = pyodbc.connect(connection_string)
                
                # キャッシュに保存（高速化のため）
                DatabaseConfig._connection_cache = connection
                DatabaseConfig._connection_cache_timestamp = time.time()
                
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

    @staticmethod
    def close_all_connections() -> None:
        """
        すべてのキャッシュされた接続を閉じる（リソース解放）
        アプリケーション終了時に呼び出すことを推奨
        """
        if DatabaseConfig._connection_cache is not None:
            try:
                DatabaseConfig._connection_cache.close()
            except Exception:
                # 接続が既に閉じられている場合は無視
                pass
            finally:
                DatabaseConfig._connection_cache = None
                DatabaseConfig._connection_cache_timestamp = None

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







