from pathlib import Path
from dotenv import load_dotenv


def load_env_file(env_file_path: str) -> None:
    if not Path(env_file_path).exists():
        raise FileNotFoundError(f"設定ファイルが見つかりません: {env_file_path}")
    load_dotenv(env_file_path)
