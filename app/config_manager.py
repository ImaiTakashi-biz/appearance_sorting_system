"""
アプリケーション設定管理モジュール
割り当てルールの設定値を管理
"""

import json
import os
import sys
from pathlib import Path
from typing import Optional
from loguru import logger


class AppConfigManager:
    """アプリケーション設定管理クラス"""
    
    # デフォルト値
    DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD = 4.0  # 同一品番の4時間上限（厳格上限）
    DEFAULT_REQUIRED_INSPECTORS_THRESHOLD = 3.0  # 必要人数計算の3時間基準
    
    def __init__(self, config_file_path: Optional[str] = None):
        """
        初期化
        
        Args:
            config_file_path: 設定ファイルのパス（Noneの場合はconfig.envから読み込む）
        """
        # パスが指定されていない場合は、config.envから読み込む
        if config_file_path is None:
            # 環境変数から読み込む（config.envで必須）
            self.config_file_path = os.getenv("APP_SETTINGS_PATH")
            if not self.config_file_path:
                raise ValueError(
                    "APP_SETTINGS_PATHが設定されていません。\n"
                    "config.envにAPP_SETTINGS_PATHを設定してください。"
                )
        else:
            self.config_file_path = config_file_path
        
        self._load_config()
    
    def _load_config(self) -> None:
        """
        設定ファイルを読み込み
        
        ネットワーク共有パスから読み込む。読み込みに失敗した場合はデフォルト値を使用。
        """
        try:
            config_path = Path(self.config_file_path)
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    
                    # バリデーション: product_limit_hard_threshold
                    product_limit = config.get(
                        'product_limit_hard_threshold', 
                        self.DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD
                    )
                    if not isinstance(product_limit, (int, float)) or product_limit <= 0:
                        logger.warning(
                            f"無効なproduct_limit_hard_threshold値: {product_limit}。"
                            f"デフォルト値({self.DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD})を使用します。"
                        )
                        product_limit = self.DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD
                    self.product_limit_hard_threshold = float(product_limit)
                    
                    # バリデーション: required_inspectors_threshold
                    threshold = config.get(
                        'required_inspectors_threshold',
                        self.DEFAULT_REQUIRED_INSPECTORS_THRESHOLD
                    )
                    if not isinstance(threshold, (int, float)) or threshold <= 0:
                        logger.warning(
                            f"無効なrequired_inspectors_threshold値: {threshold}。"
                            f"デフォルト値({self.DEFAULT_REQUIRED_INSPECTORS_THRESHOLD})を使用します。"
                        )
                        threshold = self.DEFAULT_REQUIRED_INSPECTORS_THRESHOLD
                    self.required_inspectors_threshold = float(threshold)
                    
                # ログ出力を削除（不要な出力を抑制）
            else:
                # 設定ファイルが存在しない場合はデフォルト値を使用
                logger.warning(f"設定ファイルが見つかりません: {self.config_file_path}。デフォルト値を使用します。")
                self.product_limit_hard_threshold = self.DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD
                self.required_inspectors_threshold = self.DEFAULT_REQUIRED_INSPECTORS_THRESHOLD
        except json.JSONDecodeError as e:
            logger.error(f"設定ファイルのJSON形式が不正です: {e}")
            logger.warning(f"デフォルト値を使用します。")
            self.product_limit_hard_threshold = self.DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD
            self.required_inspectors_threshold = self.DEFAULT_REQUIRED_INSPECTORS_THRESHOLD
        except Exception as e:
            logger.error(f"設定ファイルの読み込みに失敗しました: {e}")
            logger.warning(f"デフォルト値を使用します。")
            # エラー時はデフォルト値を使用
            self.product_limit_hard_threshold = self.DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD
            self.required_inspectors_threshold = self.DEFAULT_REQUIRED_INSPECTORS_THRESHOLD
    
    def _save_config(self) -> None:
        """
        設定ファイルに保存
        
        ネットワーク共有パスに保存する。保存に失敗した場合はエラーをログに記録。
        """
        try:
            config = {
                'product_limit_hard_threshold': self.product_limit_hard_threshold,
                'required_inspectors_threshold': self.required_inspectors_threshold
            }
            config_path = Path(self.config_file_path)
            # ディレクトリが存在しない場合は作成
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            logger.info(f"設定を保存しました: {self.config_file_path}")
        except Exception as e:
            logger.error(f"設定ファイルの保存に失敗しました: {e}")
            # ネットワーク共有パスへの保存が失敗する可能性があるため、例外を再発生させない
            # 代わりにログに記録する
    
    def update_product_limit_hard_threshold(self, value: float):
        """同一品番の4時間上限を更新"""
        self.product_limit_hard_threshold = value
        self._save_config()
    
    def update_required_inspectors_threshold(self, value: float):
        """必要人数計算の3時間基準を更新"""
        self.required_inspectors_threshold = value
        self._save_config()
    
    def reset_to_default(self):
        """デフォルト値にリセット"""
        self.product_limit_hard_threshold = self.DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD
        self.required_inspectors_threshold = self.DEFAULT_REQUIRED_INSPECTORS_THRESHOLD
        self._save_config()
    
    def get_product_limit_hard_threshold(self) -> float:
        """同一品番の4時間上限を取得"""
        return self.product_limit_hard_threshold
    
    def get_required_inspectors_threshold(self) -> float:
        """必要人数計算の3時間基準を取得"""
        return self.required_inspectors_threshold

