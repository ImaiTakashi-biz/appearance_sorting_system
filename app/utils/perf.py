from __future__ import annotations

from contextlib import contextmanager
from time import perf_counter
from typing import Any, Dict, Iterator, Optional


@contextmanager
def perf_timer(
    logger: Any,
    label: str,
    *,
    threshold_ms: float = 0.0,
    extra: Optional[Dict[str, Any]] = None,
) -> Iterator[None]:
    """
    軽量な処理時間計測（DEBUGログのみ）。
    - 機能/結果を変えずにボトルネックを特定する用途
    """
    start = perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (perf_counter() - start) * 1000.0
        if elapsed_ms < threshold_ms:
            return
        bound = logger
        if extra:
            try:
                bound = logger.bind(**extra)
            except Exception:
                bound = logger
        try:
            bound.bind(channel="PERF").debug("PERF {}: {:.1f} ms", label, elapsed_ms)
        except Exception:
            # 計測ログが失敗しても本処理は継続
            pass

