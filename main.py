from app.ui.ui_handlers import ModernDataExtractorUI
from loguru import logger


def main() -> None:
    """アプリケーションのエントリーポイント"""
    try:
        ModernDataExtractorUI().run()
    except Exception as exc:
        logger.error(f"アプリケーションの起動に失敗しました: {exc}")
        raise


if __name__ == "__main__":
    main()
