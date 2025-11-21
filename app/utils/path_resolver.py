"""
パス解決ユーティリティ
exe化対応のリソースファイルパス解決機能を提供
"""

import sys
from pathlib import Path
from typing import Optional


def resolve_resource_path(file_path: str, base_dir: Optional[Path] = None) -> str:
    """
    リソースファイルのパスを解決（exe化対応）
    
    exe化されている場合とそうでない場合でリソースファイルのパスを解決します。
    まず一時ディレクトリ（sys._MEIPASS）を確認し、次にexeファイルの場所を確認します。
    
    Args:
        file_path: ファイルパス（相対パスまたはファイル名）
        base_dir: ベースディレクトリ（通常実行時の場合に使用）
        
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
        if base_dir:
            resolved_path = base_dir / Path(file_path).name
            if resolved_path.exists():
                return str(resolved_path)
        
        # ベースディレクトリが指定されていない、またはファイルが見つからない場合
        # 元のパスを返す（絶対パスの場合はそのまま、相対パスの場合はそのまま）
        return file_path

