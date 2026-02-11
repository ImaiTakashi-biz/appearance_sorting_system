from pathlib import Path
import os
from dotenv import load_dotenv


def load_env_file(env_file_path: str) -> None:
    if not Path(env_file_path).exists():
        raise FileNotFoundError(f"設定ファイルが見つかりません: {env_file_path}")
    load_dotenv(env_file_path)
    _apply_debug_overrides()


def _parse_bool(value: str | None) -> bool:
    return (str(value or "").strip().lower() in {"1", "true", "yes", "on"})


def _apply_debug_overrides() -> None:
    if not _parse_bool(os.environ.get("DEBUG_MODE")):
        return

    debug_flags = {
        "PERF_LOG_ENABLED": "1",
        "DEBUG_SIGNATURE_LOG_ENABLED": "1",
        "DEBUG_SNAPSHOT_DIFF_ENABLED": "1",
        "DEBUG_ASSIGNMENT_DIFF_ENABLED": "1",
        "UI_DEBUG_LOG_ENABLED": "1",
    }
    for key, value in debug_flags.items():
        os.environ[key] = value

    numeric_overrides = {
        "DEBUG_SNAPSHOT_KEYS_MAX": "500",
        "DEBUG_SNAPSHOT_DIFF_MAX_LINES": "200",
    }
    for key, value in numeric_overrides.items():
        os.environ[key] = value
