"""
外観検査振分支援システム - メインエントリーポイント
"""
import sys
import tkinter as tk
from tkinter import messagebox
from pathlib import Path
from loguru import logger

# ログ設定（起動時のエラーも記録）
try:
    from app.ui.ui_handlers import ModernDataExtractorUI
except ImportError as e:
    # モジュールインポートエラーの場合は早期にエラーを表示
    root = tk.Tk()
    root.withdraw()
    messagebox.showerror(
        "インポートエラー",
        f"必要なモジュールのインポートに失敗しました。\n\n"
        f"エラー詳細: {str(e)}\n\n"
        f"必要なライブラリがインストールされているか確認してください。"
    )
    sys.exit(1)


def main() -> None:
    """アプリケーションのエントリーポイント"""
    try:
        ui = ModernDataExtractorUI()
        
        # 起動時の設定検証
        if not ui.config.validate_config():
            error_msg = (
                "設定の検証に失敗しました。\n\n"
                "以下の設定を確認してください：\n"
                "1. config.envファイルが正しい場所に存在するか\n"
                "2. ACCESS_FILE_PATHが正しく設定されているか\n"
                "3. ACCESS_TABLE_NAMEが正しく設定されているか\n"
                "4. Accessファイルが存在し、アクセス可能か\n\n"
                f"設定ファイルのパス: {ui.config.env_file_path}"
            )
            messagebox.showerror("設定エラー", error_msg)
            logger.error("設定の検証に失敗しました")
            sys.exit(1)
        
        ui.run()
        
    except FileNotFoundError as e:
        # 設定ファイルが見つからない場合
        root = tk.Tk()
        root.withdraw()
        error_msg = str(e)
        if "config.env" in error_msg or "設定ファイル" in error_msg:
            message = (
                "設定ファイル（config.env）が見つかりません。\n\n"
                f"エラー詳細: {error_msg}\n\n"
                "exeファイルと同じフォルダにconfig.envファイルを配置してください。"
            )
        else:
            message = (
                f"必要なファイルが見つかりません。\n\n"
                f"エラー詳細: {error_msg}\n\n"
                "設定ファイル（config.env）を確認してください。"
            )
        messagebox.showerror("設定ファイルエラー", message)
        logger.error(f"設定ファイルエラー: {e}")
        sys.exit(1)
        
    except ValueError as e:
        # 設定値のエラー
        root = tk.Tk()
        root.withdraw()
        error_msg = str(e)
        if "ACCESS_FILE_PATH" in error_msg or "ACCESS_TABLE_NAME" in error_msg:
            message = (
                "必須設定が不足しています。\n\n"
                f"エラー詳細: {error_msg}\n\n"
                "config.envファイルに以下の設定を追加してください：\n"
                "- ACCESS_FILE_PATH\n"
                "- ACCESS_TABLE_NAME"
            )
        else:
            message = (
                f"設定値に問題があります。\n\n"
                f"エラー詳細: {error_msg}\n\n"
                "config.envファイルの設定を確認してください。"
            )
        messagebox.showerror("設定値エラー", message)
        logger.error(f"設定値エラー: {e}")
        sys.exit(1)
        
    except ConnectionError as e:
        # データベース接続エラー
        root = tk.Tk()
        root.withdraw()
        message = (
            "データベースへの接続に失敗しました。\n\n"
            f"エラー詳細: {str(e)}\n\n"
            "以下の点を確認してください：\n"
            "1. Accessファイルが存在し、アクセス可能か\n"
            "2. Microsoft Access Database Engineがインストールされているか\n"
            "3. ファイルが他のアプリケーションで開かれていないか\n"
            "4. ネットワークパスの場合、接続が確立されているか"
        )
        messagebox.showerror("データベース接続エラー", message)
        logger.error(f"データベース接続エラー: {e}")
        sys.exit(1)
        
    except Exception as exc:
        # その他の予期しないエラー
        root = tk.Tk()
        root.withdraw()
        message = (
            "アプリケーションの起動に失敗しました。\n\n"
            f"エラー詳細: {str(exc)}\n\n"
            "ログファイルを確認してください。\n"
            "問題が解決しない場合は、管理者に連絡してください。"
        )
        messagebox.showerror("起動エラー", message)
        logger.error(f"アプリケーションの起動に失敗しました: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
