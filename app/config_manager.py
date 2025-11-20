"""
アプリケーション設定管理モジュール
割り当てルールの設定値を管理
"""

import json
import os
import sys
from pathlib import Path
from loguru import logger


class AppConfigManager:
    """アプリケーション設定管理クラス"""
    
    # デフォルト値
    DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD = 4.0  # 同一品番の4時間上限（厳格上限）
    DEFAULT_REQUIRED_INSPECTORS_THRESHOLD = 3.0  # 必要人数計算の3時間基準
    
    def __init__(self, config_file_path: str = "app_settings.json"):
        """
        初期化
        
        Args:
            config_file_path: 設定ファイルのパス
        """
        # exe化されている場合とそうでない場合でパスを決定
        if getattr(sys, 'frozen', False):
            # exe化されている場合、exeファイルの場所を基準にする
            application_path = Path(sys.executable).parent
            self.config_file_path = str(application_path / config_file_path)
        else:
            # 通常のPython実行の場合
            self.config_file_path = config_file_path
        
        self._load_config()
    
    def _load_config(self):
        """設定ファイルを読み込み"""
        try:
            if Path(self.config_file_path).exists():
                with open(self.config_file_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self.product_limit_hard_threshold = config.get(
                        'product_limit_hard_threshold', 
                        self.DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD
                    )
                    self.required_inspectors_threshold = config.get(
                        'required_inspectors_threshold',
                        self.DEFAULT_REQUIRED_INSPECTORS_THRESHOLD
                    )
            else:
                # 設定ファイルが存在しない場合はデフォルト値を使用
                self.product_limit_hard_threshold = self.DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD
                self.required_inspectors_threshold = self.DEFAULT_REQUIRED_INSPECTORS_THRESHOLD
                self._save_config()  # デフォルト値で設定ファイルを作成
        except Exception as e:
            logger.error(f"設定ファイルの読み込みに失敗しました: {e}")
            # エラー時はデフォルト値を使用
            self.product_limit_hard_threshold = self.DEFAULT_PRODUCT_LIMIT_HARD_THRESHOLD
            self.required_inspectors_threshold = self.DEFAULT_REQUIRED_INSPECTORS_THRESHOLD
    
    def _save_config(self):
        """設定ファイルに保存"""
        try:
            config = {
                'product_limit_hard_threshold': self.product_limit_hard_threshold,
                'required_inspectors_threshold': self.required_inspectors_threshold
            }
            with open(self.config_file_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            logger.info(f"設定を保存しました: {self.config_file_path}")
        except Exception as e:
            logger.error(f"設定ファイルの保存に失敗しました: {e}")
            raise
    
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

