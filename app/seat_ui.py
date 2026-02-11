from __future__ import annotations

import csv
import json
import os
import re
from typing import Dict, List, Optional

# 本番統合時の指針:
# - load_seating_chart / save_seating_chart / build_initial_seating_chart は共通ライブラリに移行する想定。
# - attach_dummy_lots は実システムではDB/APIアクセスで置き換えられる設計にしています。

CONFIG_ENV_PATH = "config.env"
DEFAULT_SEATING_JSON_PATH = r"\\192.168.1.200\共有\dev_tools\外観検査振分支援システム\seating_chart\seating_chart.json"
DEFAULT_SEATING_HTML_PATH = r"\\192.168.1.200\共有\dev_tools\外観検査振分支援システム\seating_chart\seat_ui.html"


GRID_POSITIONS = (
    [(1, col) for col in range(1, 9)]
    + [(2, col) for col in range(1, 9)]
    + [(3, col) for col in range(1, 9)]
    + [(4, col) for col in range(1, 9)]
    + [(5, position) for position in (1.5, 3.5, 5.5, 7.5)]
)


def _parse_config_env() -> Dict[str, str]:
    if not os.path.exists(CONFIG_ENV_PATH):
        return {}
    parsed: Dict[str, str] = {}
    with open(CONFIG_ENV_PATH, encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            parsed[key.strip()] = value.strip()
    return parsed


_ENV_CONFIG = _parse_config_env()
SEATING_JSON_PATH = _ENV_CONFIG.get("SEATING_JSON_PATH", DEFAULT_SEATING_JSON_PATH)
SEATING_HTML_PATH = _ENV_CONFIG.get("SEATING_HTML_PATH", DEFAULT_SEATING_HTML_PATH)
SEATING_JSON_FILE_NAME = os.path.basename(SEATING_JSON_PATH) or "seating_chart.json"


def _load_inspectors_from_csv(path: str) -> List[str]:
    for encoding in ("utf-8", "cp932", "shift_jis"):
        try:
            with open(path, newline="", encoding=encoding) as handle:
                reader = csv.reader(handle)
                names: List[str] = []
                for idx, row in enumerate(reader, start=1):
                    if idx < 3 or len(row) < 2:
                        continue
                    name = row[1].strip()
                    if name:
                        names.append(name)
                if names:
                    return names
        except (UnicodeDecodeError, OSError):
            continue
    return []


def _detect_inspector_csv() -> Optional[str]:
    env = _parse_config_env()
    for key, value in env.items():
        if "INSPECTOR_MASTER_PATH" in key:
            return value
    return None


def _ensure_seat_names() -> List[str]:
    path = _detect_inspector_csv()
    if not path:
        raise FileNotFoundError("Inspector CSV path not found in config.env.")
    inspectors = _load_inspectors_from_csv(path)
    if not inspectors:
        raise ValueError(f"Could not load inspectors from CSV: {path}")
    return inspectors


def _load_default_inspector_names() -> List[str]:
    """config.env がない開発環境でも安全にインスペクタ名リストを取得するための補助関数。"""
    try:
        return _ensure_seat_names()
    except (FileNotFoundError, ValueError):
        return []


def attach_dummy_lots(chart: Dict[str, List[Dict[str, object]]]) -> Dict[str, List[Dict[str, object]]]:
    """ダミーロットを付与（本番では実データに差し替える）。"""
    products = ["A-001", "B-145", "C-210", "D-330", "E-512", "F-021"]
    processes = ["外観検査", "組立検査", "仕上げ検査", "再検査"]
    qty_options = [32, 48, 60, 90, 120, 180]
    sec_options = [2.5, 3.4, 4.8, 5.2, 6.1]
    counter = 1
    for seat_idx, seat in enumerate(chart.get("seats", []), start=1):
        lots: List[Dict[str, object]] = []
        lot_count = 2 + (seat_idx % 2)
        for slot in range(lot_count):
            lot_id = f"L{seat_idx:02d}-{counter:03d}"
            product = products[(seat_idx + slot) % len(products)]
            process = processes[(seat_idx + slot) % len(processes)]
            lots.append(
                {
                    "lot_id": lot_id,
                    "product_code": product,
                    "product_name": f"品番{product}",
                    "process_name": process,
                    "quantity": qty_options[(seat_idx + slot) % len(qty_options)],
                    "sec_per_piece": sec_options[(seat_idx + slot) % len(sec_options)],
                }
            )
            counter += 1
        seat["lots"] = lots
    return chart


def build_initial_seating_chart(inspector_names: List[str]) -> Dict[str, List[Dict[str, object]]]:
    """
    座席の初期配置を作成し、検査員名と位置のみを含む辞書を返す。

    1 つの seat エントリの例:
    {
        "id": "s1",
        "name": "検査員名",
        "row": 1,
        "col": 1,
        "lots": [
            {
                "lot_id": "L001",
                "product_name": "品番XYZ",
                "quantity": 100,
                "sec_per_piece": 2.5
            },
            ...
        ]
    }
    """
    entries: List[Dict[str, object]] = []
    idx = 0
    while len(entries) < len(GRID_POSITIONS):
        row, col = GRID_POSITIONS[len(entries)]
        name = inspector_names[idx % len(inspector_names)] if inspector_names else ""
        entries.append({"id": f"s{len(entries) + 1}", "name": name, "row": row, "col": col, "lots": []})
        idx += 1
    chart = {"seats": entries}
    return chart


def attach_lots_to_chart(
    chart: Dict[str, List[Dict[str, object]]],
    lots_by_inspector: Dict[str, List[Dict[str, object]]],
    preserve_split_lots: bool = False,
) -> Dict[str, List[Dict[str, object]]]:
    """
    既存の座席配置に lot 情報を紐づける。
    lots_by_inspector のキーには検査員名または座席 ID を受け入れ、見つかった最初のリストを seat["lots"] にセットする。
    """
    split_suffix_pattern = re.compile(r"-S\\d+$")
    seats = chart.setdefault("seats", [])
    for seat in seats:
        seat_name = (seat.get("name") or "").strip()
        seat_id = seat.get("id")
        resolved_lots: List[Dict[str, object]] = []
        for key in (seat_name, seat.get("name"), seat_id):
            if not key:
                continue
            candidate = lots_by_inspector.get(key)
            if candidate:
                resolved_lots = candidate
                break
        preserved: List[Dict[str, object]] = []
        if preserve_split_lots:
            for lot in seat.get("lots", []) or []:
                if not isinstance(lot, dict):
                    continue
                lot_id = lot.get("lot_id") or ""
                if lot.get("split_group") or (
                    isinstance(lot_id, str) and split_suffix_pattern.search(lot_id)
                ):
                    preserved.append(lot)
        if preserved:
            resolved_ids = {
                lot.get("lot_id")
                for lot in resolved_lots
                if isinstance(lot, dict) and lot.get("lot_id")
            }
            merged = list(resolved_lots)
            for lot in preserved:
                lot_id = lot.get("lot_id")
                if lot_id and lot_id in resolved_ids:
                    continue
                merged.append(lot)
            seat["lots"] = merged
        else:
            seat["lots"] = list(resolved_lots) if resolved_lots else []
    return chart


def load_seating_chart(path: str) -> Dict[str, List[Dict[str, object]]]:
    """指定パスから seating_chart.json を読み込み、seat["lots"] を list で初期化します。"""
    with open(path, "r", encoding="utf-8") as handle:
        chart = json.load(handle)
        for seat in chart.get("seats", []):
            if not isinstance(seat.get("lots"), list):
                seat["lots"] = []
    return chart


def save_seating_chart(path: str, chart: Dict[str, List[Dict[str, object]]]) -> None:
    """座席チャートを指定パスに保存し、必要ならディレクトリを作成します。"""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(chart, handle, ensure_ascii=False, indent=2)


def normalize_split_metadata(chart: Dict[str, List[Dict[str, object]]]) -> Dict[str, List[Dict[str, object]]]:
    """split_group/split_total/split_index が欠けている場合に補完する。"""
    split_suffix_pattern = re.compile(r"-S\\d+$")

    def _is_explicit_split(lot: Dict[str, object]) -> bool:
        lot_id = lot.get("lot_id") or ""
        return bool(lot.get("split_group") or (isinstance(lot_id, str) and split_suffix_pattern.search(lot_id)))

    def _split_group_key(lot: Dict[str, object]) -> str:
        if lot.get("split_group"):
            return str(lot.get("split_group"))
        lot_id = lot.get("lot_id") or ""
        if isinstance(lot_id, str) and split_suffix_pattern.search(lot_id):
            return split_suffix_pattern.sub("", lot_id)
        return ""

    def _iter_lots():
        for seat in chart.get("seats", []):
            for lot in seat.get("lots", []) or []:
                yield lot
        for lot in chart.get("unassigned_lots", []) or []:
            yield lot

    groups: Dict[str, List[Dict[str, object]]] = {}
    for lot in _iter_lots():
        if not isinstance(lot, dict):
            continue
        if not _is_explicit_split(lot):
            continue
        key = _split_group_key(lot)
        if not key:
            continue
        group_key = str(key)
        groups.setdefault(group_key, []).append(lot)

    for group_key, lots in groups.items():
        if len(lots) < 2:
            continue
        group_id = ""
        for lot in lots:
            if lot.get("split_group"):
                group_id = str(lot.get("split_group"))
                break
        if not group_id:
            group_id = group_key
        total = len(lots)
        for index, lot in enumerate(lots, start=1):
            lot["split_group"] = group_id
            lot["split_total"] = total
            lot["split_index"] = index

    return chart


def ensure_seating_json_exists() -> Dict[str, List[Dict[str, object]]]:
    if os.path.exists(SEATING_JSON_PATH):
        try:
            chart = load_seating_chart(SEATING_JSON_PATH)
            if len(chart.get("seats", [])) < len(GRID_POSITIONS):
                raise ValueError("incomplete chart")
            return chart
        except (json.JSONDecodeError, OSError, ValueError):
            pass
    inspectors = _ensure_seat_names()
    chart = build_initial_seating_chart(inspectors)
    chart = attach_dummy_lots(chart)
    save_seating_chart(SEATING_JSON_PATH, chart)
    return chart


DEFAULT_INSPECTOR_NAMES = _load_default_inspector_names()

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ja">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>検査ロット振分けレイアウト</title>
    <style>
      * { box-sizing: border-box; }
      body {
        margin: 0;
        min-height: 100vh;
        background: #f3f3f3;
        color: #111;
        font-family: "Noto Sans JP", "Segoe UI", system-ui, sans-serif;
      }
      body.editing main {
        grid-template-columns: minmax(0, 1fr) minmax(220px, 240px);
      }
      body:not(.editing) .editor-panel {
        display: none;
      }
      main {
        display: grid;
        grid-template-columns: 1fr;
        gap: 0.6rem;
        padding: 1rem 1.5rem 1.5rem;
        min-height: 100vh;
        width: min(1480px, 100%);
        margin: 0 auto;
      }
      .grid-area {
        background: #fff;
        border-radius: 1rem;
        padding: 0.9rem 1rem 1.1rem;
        box-shadow: 0 18px 40px rgba(0, 0, 0, 0.08);
        position: relative;
        overflow: hidden;
        max-width: 100%;
        min-height: 420px;
        display: flex;
        flex-direction: column;
        gap: 0.9rem;
      }
      .grid-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 1rem;
        padding: 0.35rem 0.25rem 0.75rem;
        position: sticky;
        top: 0;
        z-index: 20;
        background: rgba(255, 255, 255, 0.92);
        backdrop-filter: blur(10px);
        border-bottom: 1px solid rgba(0, 0, 0, 0.06);
      }
      .grid-body {
        display: grid;
        grid-template-columns: minmax(0, 1fr) 260px;
        gap: 1rem;
        align-items: start;
        width: 100%;
        min-height: 420px;
      }
      body.editing .grid-body {
        grid-template-columns: 1fr;
      }
      .legend-panel {
        border-radius: 0.9rem;
        padding: 0.35rem 0.85rem;
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        background: #fff;
        font-size: clamp(0.72rem, 0.9vw, 0.85rem);
        color: #333;
        font-weight: 600;
        border: 1px solid transparent;
        background-image:
          linear-gradient(#fff, #fff),
          linear-gradient(135deg, #ff4c4c, #f2a200);
        background-origin: border-box;
        background-clip: padding-box, border-box;
        box-shadow: 0 0 0 2px rgba(255, 76, 76, 0.35);
        white-space: nowrap;
      }
      .legend-text {
        display: flex;
        gap: 0.5rem;
        align-items: center;
        flex-wrap: nowrap;
        white-space: nowrap;
      }
      .legend-label {
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
      }
      .legend-swatch {
        width: 12px;
        height: 12px;
        border-radius: 3px;
        border: 1px solid rgba(0, 0, 0, 0.1);
        display: inline-flex;
      }
      .legend-swatch--red {
        background: #ff4c4c;
      }
      .legend-swatch--yellow {
        background: #f4a200;
      }
      .legend-panel::after {
        content: " "; 
        display: block;
      }
      .side-panel {
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
        position: sticky;
        top: 0.9rem;
        max-height: calc(100vh - 2.2rem);
        min-height: 0;
        overflow: hidden;
      }
      body.editing .side-panel {
        display: none;
      }
      .unassigned-area {
        border: 1px solid #d9d9d9;
        border-radius: 0.8rem;
        padding: 0.4rem 0.9rem;
        background: #fff;
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
        min-height: 0;
        flex: 1 1 auto;
      }
      .unassigned-drop-target {
        border-color: #1f7aef;
        box-shadow: 0 0 0 2px rgba(31, 122, 239, 0.25);
      }
      .unassigned-title {
        font-size: 0.85rem;
        font-weight: 700;
        margin: 0;
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        gap: 0.5rem;
      }
      .unassigned-count {
        font-size: 0.78rem;
        color: #666;
        font-weight: 700;
      }
      .unassigned-lots {
        display: grid;
        grid-template-columns: 1fr;
        gap: 0.4rem;
        min-height: 0;
        overflow: auto;
        padding: 0.2rem 0.1rem 0.1rem;
        flex: 1 1 auto;
      }
      .unassigned-empty-state {
        font-size: 0.78rem;
        color: #555;
      }
      .grid-header h1 {
        margin: 0;
        font-size: 1.4rem;
      }
      .title-block {
        display: flex;
        flex-direction: column;
        gap: 0.15rem;
      }
      .edit-instruction {
        margin: 0;
        font-size: 0.85rem;
        color: #555;
        white-space: pre-line;
      }
      .grid-actions {
        display: flex;
        gap: 0.5rem;
        align-items: center;
      }
      body.editing .grid-actions .legend-panel {
        display: none;
      }
      #seat-grid-area {
        border: 1px solid rgba(0, 0, 0, 0.08);
        border-radius: 1rem;
        background: linear-gradient(180deg, rgba(246, 248, 251, 0.8), rgba(255, 255, 255, 1));
        overflow-x: auto;
        overflow-y: visible;
        height: auto;
        min-height: 420px;
        max-height: none;
      }
      #seat-grid {
        min-height: 520px;
        width: 100%;
        position: relative;
        margin-bottom: 0;
        max-width: 100%;
        padding: 0.75rem;
      }
      .seat-card {
        position: absolute;
        border-radius: 1rem;
        border: 1px solid #d6d6d6;
        background: #fff;
        box-shadow: 0 8px 20px rgba(0, 0, 0, 0.1);
        display: flex;
        flex-direction: column;
        align-items: stretch;
        gap: 0.4rem;
        padding: 0.75rem;
        font-weight: 600;
        font-size: 0.95rem;
        transition: border-color 0.2s ease, transform 0.2s ease;
        cursor: pointer;
        overflow: hidden;
      }
      .seat-card.selected {
        border-color: #1f7aef;
        box-shadow: 0 0 0 3px rgba(31, 122, 239, 0.25);
      }
      .seat-card.drop-target {
        box-shadow: 0 0 0 3px rgba(31, 122, 239, 0.35);
      }
      .seat-card.seat-card--today-lot {
        border-color: #ff4c4c;
        box-shadow: 0 16px 30px rgba(255, 76, 76, 0.35);
      }
      .seat-card.seat-card--same-day-cleaning {
        border-color: #f4a200;
        box-shadow: 0 16px 30px rgba(244, 162, 0, 0.35);
      }
      body.editing .seat-card.seat-card--today-lot,
      body.editing .seat-card.seat-card--same-day-cleaning {
        border-color: #d6d6d6;
        box-shadow: 0 8px 20px rgba(0, 0, 0, 0.1);
      }
      .seat-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 0.4rem;
      }
      .seat-name {
        font-size: 0.92rem;
        font-weight: 700;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: clip;
        flex: 1 1 auto;
        max-width: none;
        min-width: 0;
        word-break: keep-all;
        line-height: 1.1;
      }
      .total-time {
        font-size: 0.78rem;
        color: #555;
        flex: 0 0 auto;
      }
      .lot-list {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
        width: 100%;
        flex: 1 1 auto;
        min-height: 0;
        max-height: none;
        overflow: auto;
        scrollbar-width: thin;
        -ms-overflow-style: auto;
      }
      .lot-card {
        background: #f5f5f5;
        border-radius: 0.55rem;
        padding: 0.15rem 0.35rem;
        border: 1px solid #e2e2e2;
        position: relative;
        cursor: grab;
        text-align: left;
        font-size: 0.7rem;
        line-height: 1.1;
        min-height: 18px;
        display: inline-flex;
        align-items: center;
        gap: 0.25rem;
        width: 100%;
        min-width: 0;
        white-space: nowrap;
        box-sizing: border-box;
      }
      .lot-card--today {
        background: #ffe5e5;
        border-color: #ff6a6a;
        color: #5a1111;
      }
      .lot-card--same-day-cleaning {
        background: #fff6d8;
        border-color: #f3c64a;
        color: #4a3300;
      }
      .lot-card:active {
        cursor: grabbing;
      }
      .lot-card.dragging-lot {
        opacity: 0.9;
        border-color: #1f7aef;
        background: #e4f0ff;
        box-shadow: 0 0 0 2px rgba(31, 122, 239, 0.35);
      }
      .lot-card--group-highlight {
        border-color: #1f7aef;
        background: #eaf2ff;
        box-shadow: 0 0 0 2px rgba(31, 122, 239, 0.25);
      }
      .lot-card--hover-highlight {
        border-color: #16a34a;
        background: #e8f7ee;
        box-shadow: 0 0 0 2px rgba(22, 163, 74, 0.25);
      }
      .seat-card--group-highlight {
        border-color: #1f7aef !important;
        background: #f4f8ff !important;
        box-shadow: 0 0 0 2px rgba(31, 122, 239, 0.2) !important;
        outline: 2px solid rgba(31, 122, 239, 0.45);
        outline-offset: 2px;
      }
      .unassigned-area--group-highlight {
        border-color: #1f7aef;
        background: #f4f8ff;
        box-shadow: 0 0 0 2px rgba(31, 122, 239, 0.18);
      }
      .lot-context-menu {
        position: absolute;
        top: 0;
        left: 0;
        transform: translate(-9999px, -9999px);
        background: #fff;
        border-radius: 0.8rem;
        padding: 0.8rem;
        box-shadow: 0 12px 30px rgba(0, 0, 0, 0.25);
        display: flex;
        flex-direction: column;
        gap: 0.4rem;
        font-size: 0.85rem;
        min-width: 220px;
        z-index: 60;
      }
      .lot-context-menu.hidden {
        opacity: 0;
        pointer-events: none;
      }
      .lot-context-menu.visible {
        opacity: 1;
        pointer-events: auto;
      }
      .lot-context-menu label {
        font-size: 0.75rem;
        color: #555;
      }
      .lot-context-menu input {
        width: 100%;
        border-radius: 0.5rem;
        border: 1px solid #d3d3d3;
        padding: 0.45rem;
        font-size: 0.8rem;
      }
      .lot-context-menu button {
        width: 100%;
        padding: 0.4rem 0.55rem;
        font-size: 0.85rem;
        border-radius: 0.6rem;
        border: none;
        cursor: pointer;
      }
      .lot-context-menu button.primary {
        background: #1f7aef;
        color: #fff;
      }
      .lot-context-menu button.secondary {
        background: #f0f0f0;
        color: #222;
      }
      .floating-tooltip {
        position: fixed;
        left: 0;
        top: 0;
        transform: translate(-50%, -110%);
        background: rgba(0, 0, 0, 0.85);
        color: #fff;
        padding: 0.35rem 0.65rem;
        border-radius: 0.65rem;
        font-size: 0.75rem;
        white-space: nowrap;
        pointer-events: none;
        opacity: 0;
        transition: opacity 0.2s ease, transform 0.2s ease;
        z-index: 120;
      }
      .floating-tooltip.visible {
        opacity: 1;
      }
      .lot-line {
        font-size: 0.72rem;
        color: #444;
        margin: 0;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        min-width: 0;
        flex: 1 1 auto;
      }
      .editor-panel {
        background: #fff;
        border-radius: 1rem;
        padding: 1.5rem;
        box-shadow: 0 18px 40px rgba(0, 0, 0, 0.08);
        display: flex;
        flex-direction: column;
        gap: 0.65rem;
      }
      .editor-panel h2 {
        margin: 0;
        font-size: 1.2rem;
      }
      label {
        font-size: 0.9rem;
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
      }
      input {
        padding: 0.55rem 0.9rem;
        border-radius: 0.5rem;
        border: 1px solid #d0d0d0;
        font-size: 1rem;
      }
      button {
        border: none;
        border-radius: 0.7rem;
        padding: 0.75rem 1rem;
        font-weight: 600;
        cursor: pointer;
      }
      button.primary {
        background: #1f7aef;
        color: #fff;
      }
      button.secondary {
        background: #e0e0e0;
        color: #222;
      }
      .lot-time {
        font-size: 0.65rem;
        color: #555;
        margin: 0;
        text-align: right;
      }
      .inspector-dropdown {
        position: absolute;
        background: #fff;
        border-radius: 1rem;
        box-shadow: 0 16px 30px rgba(0, 0, 0, 0.18);
        padding: 0.85rem;
        min-width: 200px;
        display: none;
        flex-direction: column;
        gap: 0.3rem;
        z-index: 25;
        opacity: 0;
        transform: translateY(-12px) scale(0.95);
      }
      .inspector-dropdown.visible {
        display: flex;
        animation: fadeScale 0.25s ease-out forwards;
      }
      .inspector-dropdown.closing {
        animation: fadeScaleReverse 0.2s ease-in forwards;
      }
      .inspector-dropdown button {
        text-align: left;
        background: #f0f0f0;
        border: none;
        border-radius: 0.9rem;
        padding: 0.45rem 0.8rem;
        font-size: 0.95rem;
      }
      @keyframes fadeScale {
        from {
          opacity: 0;
          transform: translateY(-12px) scale(0.95);
        }
        to {
          opacity: 1;
          transform: translateY(0) scale(1);
        }
      }
      @keyframes fadeScaleReverse {
        from {
          opacity: 1;
          transform: translateY(0) scale(1);
        }
        to {
          opacity: 0;
          transform: translateY(-10px) scale(0.95);
        }
      }
      .mode-toggle {
        min-width: 140px;
      }
      @media screen and (max-width: 1100px) {
        main {
          grid-template-columns: 1fr;
        }
        body.editing main {
          grid-template-columns: 1fr;
        }
        .grid-area {
          border-radius: 0.8rem;
          padding: 0.75rem 1rem 1.2rem;
        }
        .grid-header {
          flex-direction: column;
          align-items: flex-start;
        }
        .grid-body {
          grid-template-columns: 1fr;
        }
        .grid-actions {
          flex-wrap: wrap;
          justify-content: flex-end;
          width: 100%;
        }
        .legend-panel {
          font-size: 0.74rem;
          padding: 0.3rem 0.75rem;
        }
        .legend-text {
          gap: 0.45rem;
        }
        .mode-toggle {
          min-width: 120px;
        }
        .editor-panel {
          display: flex;
        }
      }
      @media screen and (max-width: 700px) {
        .grid-header h1 {
          font-size: 1.1rem;
        }
        .edit-instruction {
          font-size: 0.8rem;
        }
        .grid-area {
          padding: 0.6rem 0.75rem 1rem;
        }
        .legend-panel {
          font-size: 0.72rem;
        }
        #seat-grid-area {
          height: auto;
        }
        .editor-panel {
          width: 100%;
        }
      }
    </style>
  </head>
  <body>
    <main>
      <section class="grid-area">
        <div class="grid-header" role="banner">
          <div class="title-block">
            <h1 id="board-title">検査ロット振分けレイアウト</h1>
            <p class="edit-instruction">座席編集モード: 座席位置の入れ替えのみ。ロット編集モード: ロットカードをドラッグで別席に移動できます。</p>
          </div>
          <div class="grid-actions">
            <button id="save-json" class="primary mode-toggle" type="button">変更を保存</button>
            <button id="toggle-edit" class="secondary mode-toggle" type="button">座席編集モード</button>
          </div>
        </div>
        <div class="grid-body">
          <div id="seat-grid-area" aria-label="座席表">
            <div id="seat-grid" aria-live="polite"></div>
          </div>
          <aside class="side-panel" aria-label="未割当ロット">
            <div class="legend-panel" id="display-rules-legend">
              <span class="legend-text">
                表示ルール：
                <span class="legend-label">
                  <span class="legend-swatch legend-swatch--red"></span>当日出荷
                </span>
                <span class="legend-label">
                  <span class="legend-swatch legend-swatch--yellow"></span>当日洗浄
                </span>
              </span>
            </div>
            <div class="unassigned-area" id="unassigned-area">
              <div class="unassigned-title">
                未割当ロット
                <span id="unassigned-count" class="unassigned-count"></span>
              </div>
              <div id="unassigned-lots" class="unassigned-lots"></div>
            </div>
          </aside>
        </div>
        <div id="lot-split-menu" class="lot-context-menu hidden" aria-hidden="true">
          <label for="lot-split-count">何分割しますか？</label>
          <input id="lot-split-count" type="number" min="2" value="2" />
          <button id="lot-split-apply" class="primary" type="button">分割を適用</button>
          <button id="lot-split-cancel" class="secondary" type="button">キャンセル</button>
        </div>
        <div id="inspector-dropdown" class="inspector-dropdown">
          <div class="dropdown-title">検査員を選択</div>
          <div id="inspector-list"></div>
        </div>
        <div id="floating-tooltip" class="floating-tooltip" aria-hidden="true"></div>
      </section>
      <aside class="editor-panel">
        <h2>座席編集パネル</h2>
        <label>座席ID <input type="text" id="seat-id" readonly /></label>
        <label>座席担当 <input type="text" id="seat-name" list="inspector-names" /></label>
        <label>行(row) <input type="number" id="seat-row" min="1" step="0.5" /></label>
        <label>列(col) <input type="number" id="seat-col" min="1" step="0.5" /></label>
        <button id="apply-seat" class="primary" type="button">適用</button>
        <button id="clear-seat" class="secondary" type="button">空席にする</button>
      </aside>
    </main>
    <datalist id="inspector-names"></datalist>
      <script>
      const seatingData = SEATING_DATA_PLACEHOLDER;
      const INSPECTOR_CANDIDATES = INSPECTOR_CANDIDATES_PLACEHOLDER;
      const INSPECTOR_COLUMN_MAP = INSPECTOR_COLUMN_MAP_PLACEHOLDER;
      const SEATING_JSON_PATH = SEATING_JSON_PATH_PLACEHOLDER;
      const SEATING_JSON_FILE_NAME = SEATING_JSON_FILE_NAME_PLACEHOLDER;
      const SAVE_ENDPOINT = SAVE_ENDPOINT_PLACEHOLDER;
      const seats = Array.isArray(seatingData.seats) ? seatingData.seats : [];
      let unassignedLots = Array.isArray(seatingData.unassigned_lots) ? [...seatingData.unassigned_lots] : [];
      const inspectorColumnMap = INSPECTOR_COLUMN_MAP && typeof INSPECTOR_COLUMN_MAP === "string"
        ? JSON.parse(INSPECTOR_COLUMN_MAP)
        : (INSPECTOR_COLUMN_MAP || {});
      let selectedSeatId = null;
      let draggingSeatId = null;
      let draggingLot = null;
      let editingMode = false;

      const grid = document.getElementById("seat-grid");
      const seatIdInput = document.getElementById("seat-id");
      const seatNameInput = document.getElementById("seat-name");
      const seatRowInput = document.getElementById("seat-row");
      const seatColInput = document.getElementById("seat-col");
      const applyButton = document.getElementById("apply-seat");
      const clearButton = document.getElementById("clear-seat");
      const saveButton = document.getElementById("save-json");
      const toggleEditButton = document.getElementById("toggle-edit");
      const gridActions = document.querySelector(".grid-actions");
      const displayRulesLegend = document.getElementById("display-rules-legend");
      const placeDisplayRulesLegend = () => {
        if (!gridActions || !displayRulesLegend || !saveButton) {
          return;
        }
        if (gridActions.contains(displayRulesLegend)) {
          return;
        }
        gridActions.insertBefore(displayRulesLegend, saveButton);
      };
      placeDisplayRulesLegend();
      const inspectorDropdown = document.getElementById("inspector-dropdown");
      const inspectorList = document.getElementById("inspector-list");
      const inspectorDatalist = document.getElementById("inspector-names");
      const unassignedContainer = document.getElementById("unassigned-lots");
      const unassignedArea = document.getElementById("unassigned-area");
      const unassignedCount = document.getElementById("unassigned-count");
      const lotSplitMenu = document.getElementById("lot-split-menu");
      const lotSplitCountInput = document.getElementById("lot-split-count");
      const lotSplitApplyButton = document.getElementById("lot-split-apply");
      const lotSplitCancelButton = document.getElementById("lot-split-cancel");
      const modeSizes = {
        view: { width: 180, height: 150, gap: 8 },
        editing: { width: 160, height: 120, gap: 8 },
      };
      let currentSlotWidth = modeSizes.view.width;
      let currentSlotHeight = modeSizes.view.height;
      let currentSlotGap = modeSizes.view.gap;
      const getGridExtent = () => {
        if (!seats.length) {
          return { maxRow: 1, maxCol: 1 };
        }
        const rowValues = seats.map((seat) => Number(seat.row) || 0);
        const colValues = seats.map((seat) => Number(seat.col) || 0);
        return {
          maxRow: Math.max(1, ...rowValues),
          maxCol: Math.max(1, ...colValues),
        };
      };
      const boardTitle = document.getElementById("board-title");
      const gridArea = document.querySelector(".grid-area");
      const seatGridArea = document.getElementById("seat-grid-area");
      let activeSplitTarget = null;
      if (unassignedContainer) {
        unassignedContainer.addEventListener("dragover", (event) => {
          if (!editingMode && draggingLot) {
            event.preventDefault();
            (unassignedArea || unassignedContainer).classList.add("unassigned-drop-target");
          }
        });
        unassignedContainer.addEventListener("dragleave", () => {
          (unassignedArea || unassignedContainer).classList.remove("unassigned-drop-target");
        });
        unassignedContainer.addEventListener("drop", (event) => {
          event.preventDefault();
          if (!draggingLot) {
            return;
          }
          moveLot(draggingLot.seatId, "unassigned", draggingLot.lotId);
          draggingLot = null;
          (unassignedArea || unassignedContainer).classList.remove("unassigned-drop-target");
          clearDropStyles();
        });
      }
      const buildFileUrl = (path) => {
        if (!path) {
          return null;
        }
        const normalized = path.trim().replace(/\\\\+/g, "/");
        if (normalized.startsWith("//")) {
          return encodeURI(`file:${normalized}`);
        }
        if (/^[A-Za-z]:/.test(normalized)) {
          return encodeURI(`file:///${normalized}`);
        }
        return encodeURI(normalized);
      };
      const normalizeShippingDateValue = (value) => {
        if (value === undefined || value === null) {
          return "";
        }
        if (typeof value === "string") {
          return value.trim();
        }
        if (typeof value === "number") {
          return String(value);
        }
        if (value instanceof Date) {
          return value.toISOString().split("T")[0];
        }
        return String(value).trim();
      };
      const parseShippingDateToDate = (value) => {
        if (!value) {
          return null;
        }
        const normalizedValue = value.replace(/年|月/g, "-").replace(/日/g, "").replace(/\\s+/g, " ").trim();
        const match = normalizedValue.match(/\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}/);
        const target = match ? match[0] : normalizedValue;
        const parsed = Date.parse(target.replace(/\\//g, "-"));
        if (Number.isNaN(parsed)) {
          return null;
        }
        const parsedDate = new Date(parsed);
        return new Date(parsedDate.getFullYear(), parsedDate.getMonth(), parsedDate.getDate());
      };
      const isShippingDateToday = (value) => {
        const parsed = parseShippingDateToDate(value);
        if (!parsed) {
          return false;
        }
        const today = new Date();
        return (
          parsed.getFullYear() === today.getFullYear() &&
          parsed.getMonth() === today.getMonth() &&
          parsed.getDate() === today.getDate()
        );
      };
      const isSameDayCleaningShippingDate = (value) => /当日洗浄/.test(value);
      const getUnassignedLotPriority = (lot) => {
        const shippingValue = normalizeShippingDateValue(lot?.shipping_date);
        const trimmed = shippingValue.trim();
        const lowerCase = trimmed.toLowerCase();
        const isSameDayCleaning = /当日洗浄/.test(trimmed);
        const isPreinspection =
          lowerCase === "先行検査" ||
          lowerCase === "当日先行検査" ||
          lowerCase.includes("先行検査");
        if (isSameDayCleaning || isPreinspection) {
          return { rank: 1, timestamp: 0 };
        }
        const parsedDate = parseShippingDateToDate(trimmed);
        if (parsedDate) {
          return { rank: 2, timestamp: parsedDate.getTime() };
        }
        return { rank: 3, timestamp: Number.MAX_SAFE_INTEGER };
      };
      const compareUnassignedLotPriority = (a, b) => {
        const keyA = getUnassignedLotPriority(a);
        const keyB = getUnassignedLotPriority(b);
        if (keyA.rank !== keyB.rank) {
          return keyA.rank - keyB.rank;
        }
        if (keyA.timestamp !== keyB.timestamp) {
          return keyA.timestamp - keyB.timestamp;
        }
        const idA = String(a?.lot_id || a?.lot_key || "");
        const idB = String(b?.lot_id || b?.lot_key || "");
        return idA.localeCompare(idB);
      };
      const loadLatestSeatingData = async () => {
        const url = buildFileUrl(SEATING_JSON_PATH);
        if (!url) {
          return;
        }
        try {
          const response = await fetch(url, { cache: "no-store" });
          if (!response.ok) {
            return;
          }
          const latest = await response.json();
          if (!latest || !Array.isArray(latest.seats)) {
            return;
          }
          seats.length = 0;
          latest.seats.forEach((seat) => seats.push(seat));
          unassignedLots.length = 0;
          if (Array.isArray(latest.unassigned_lots)) {
            latest.unassigned_lots.forEach((lot) => unassignedLots.push(lot));
          }
          renderSeats();
        } catch (error) {
          console.warn("Failed to load latest seating JSON:", error);
        }
      };

      const applyModeSizes = () => {
        const { width: baseWidth, height: baseHeight, gap: baseGap } = editingMode ? modeSizes.editing : modeSizes.view;
        currentSlotGap = baseGap;
        currentSlotWidth = baseWidth;
        currentSlotHeight = baseHeight;
        const widthSource = seatGridArea || gridArea;
        if (!widthSource) {
          return;
        }
        const { maxCol } = getGridExtent();
        const safeCols = Math.max(1, Math.ceil(maxCol || 1));
        const horizontalPadding = 32;
        const availableWidth = Math.max(widthSource.clientWidth - horizontalPadding, 0);
        const computedWidth = Math.floor((availableWidth - (safeCols - 1) * currentSlotGap) / safeCols);
        const normalizedWidth = Number.isFinite(computedWidth) ? computedWidth : baseWidth;
        const targetWidth = Math.max(110, Math.min(baseWidth * 1.4, normalizedWidth));
        currentSlotWidth = targetWidth;
        const aspectRatio = baseHeight / baseWidth;
        currentSlotHeight = Math.max(90, Math.round(targetWidth * aspectRatio));
      };

      const uniqueInspectorNames = () =>
        Array.from(new Set(INSPECTOR_CANDIDATES.filter((value) => value && value.trim())));

      const fillDatalist = () => {
        const unique = uniqueInspectorNames();
        inspectorDatalist.innerHTML = "";
        unique.forEach((name) => {
          const option = document.createElement("option");
          option.value = name;
          inspectorDatalist.appendChild(option);
        });
      };

      const updateEditorPanel = () => {
        if (!selectedSeatId) {
          seatIdInput.value = "";
          seatNameInput.value = "";
          seatRowInput.value = "";
          seatColInput.value = "";
          return;
        }
        const target = seats.find((seat) => seat.id === selectedSeatId);
        if (!target) {
          return;
        }
        seatIdInput.value = target.id;
        seatNameInput.value = target.name;
        seatRowInput.value = target.row;
        seatColInput.value = target.col;
      };

      const showInspectorDropdown = () => {
        inspectorDropdown.style.display = "flex";
        inspectorDropdown.classList.remove("closing");
        inspectorDropdown.classList.add("visible");
      };

      const closeInspectorDropdown = () => {
        if (!inspectorDropdown.classList.contains("visible")) {
          return;
        }
        inspectorDropdown.classList.remove("visible");
        inspectorDropdown.classList.add("closing");
        const onAnimationEnd = () => {
          inspectorDropdown.style.display = "none";
          inspectorDropdown.classList.remove("closing");
          inspectorDropdown.removeEventListener("animationend", onAnimationEnd);
        };
        inspectorDropdown.addEventListener("animationend", onAnimationEnd);
      };

      const populateDropdown = () => {
        inspectorList.innerHTML = "";
        const addOption = (name) => {
          const button = document.createElement("button");
          button.type = "button";
          button.textContent = name;
          button.addEventListener("click", () => {
            if (!selectedSeatId) {
              return;
            }
            const target = seats.find((seat) => seat.id === selectedSeatId);
            if (!target) {
              return;
            }
            target.name = name === "空席" ? "" : name;
            renderSeats();
            updateEditorPanel();
            closeInspectorDropdown();
          });
          inspectorList.appendChild(button);
        };
        addOption("空席");
        uniqueInspectorNames().forEach((name) => addOption(name));
      };

      const setInspectorDropdownPosition = (card) => {
        const { offsetTop, offsetLeft, offsetHeight } = card;
        const dropdownHeight = inspectorDropdown.scrollHeight || 160;
        let top = offsetTop + offsetHeight + currentSlotGap;
        if (top + dropdownHeight > grid.clientHeight) {
          top = Math.max(8, offsetTop - dropdownHeight - currentSlotGap);
        }
        inspectorDropdown.style.top = `${top}px`;
        inspectorDropdown.style.left = `${Math.max(8, offsetLeft)}px`;
      };

      const openInspectorDropdown = (seatId, card) => {
        if (!editingMode) {
          return;
        }
        selectedSeatId = seatId;
        updateEditorPanel();
        populateDropdown();
        setInspectorDropdownPosition(card);
        showInspectorDropdown();
      };

      const swapSeats = (fromId, toId) => {
        const first = seats.find((seat) => seat.id === fromId);
        const second = seats.find((seat) => seat.id === toId);
        if (!first || !second) {
          return;
        }
        [first.row, second.row] = [second.row, first.row];
        [first.col, second.col] = [second.col, first.col];
      };

      const setSelectedSeat = (seatId) => {
        if (!editingMode) {
          return;
        }
        selectedSeatId = seatId;
        updateEditorPanel();
        renderSeats();
      };

      const clearDropStyles = () => {
        grid.querySelectorAll(".seat-card").forEach((card) => card.classList.remove("drop-target"));
        if (unassignedContainer) {
          (unassignedArea || unassignedContainer).classList.remove("unassigned-drop-target");
        }
      };

      const updateGridDimensions = () => {
        if (!grid) {
          return;
        }
        const { maxRow, maxCol } = getGridExtent();
        const height = Math.max(maxRow * (currentSlotHeight + currentSlotGap), currentSlotHeight + currentSlotGap);
        const width = Math.max(maxCol * (currentSlotWidth + currentSlotGap), currentSlotWidth + currentSlotGap);
        grid.style.height = `${height}px`;
        grid.style.width = `${width}px`;
      };

      const calculateTotalSecondsForSeat = (seat) => {
        const lots = Array.isArray(seat.lots) ? seat.lots : [];
        return lots.reduce((acc, lot) => {
          const inspectionTimeHours = Number(lot.inspection_time);
          if (!Number.isNaN(inspectionTimeHours) && inspectionTimeHours > 0) {
            return acc + inspectionTimeHours * 3600;
          }
          const quantity = Number(lot.quantity) || 0;
          const secPerPiece = Number(lot.sec_per_piece) || 0;
          return acc + quantity * secPerPiece;
        }, 0);
      };

      const formatSecondsToHoursString = (seconds) => {
        const hours = seconds / 3600;
        return `${hours.toFixed(1)}H`;
      };

      const renderUnassignedLots = () => {
        rebuildLotIdCounts();
        if (unassignedCount) {
          unassignedCount.textContent = unassignedLots.length ? `${unassignedLots.length}件` : "";
        }
        if (!unassignedContainer) {
          return;
        }
        unassignedContainer.innerHTML = "";
        if (!unassignedLots.length) {
          const empty = document.createElement("div");
          empty.className = "unassigned-empty-state";
          empty.textContent = "未割当ロットはありません";
          unassignedContainer.appendChild(empty);
          return;
        }
        const orderedUnassignedLots = [...unassignedLots].sort(compareUnassignedLotPriority);
        orderedUnassignedLots.forEach((lot) => {
          unassignedContainer.appendChild(createLotCard("unassigned", lot));
        });
      };

      const getDragAfterElement = (container, y) => {
        const draggableElements = [...container.querySelectorAll(".lot-card:not(.dragging-lot)")];
        return draggableElements.reduce(
          (closest, child) => {
            const box = child.getBoundingClientRect();
            const offset = y - box.top - box.height / 2;
            if (offset < 0 && offset > closest.offset) {
              return { offset: offset, element: child };
            } else {
              return closest;
            }
          },
          { offset: Number.NEGATIVE_INFINITY, element: null }
        ).element;
      };

      const removeLotFromSource = (sourceId, lotId) => {
        if (sourceId === "unassigned") {
          const idx = unassignedLots.findIndex((lot) => lot.lot_id === lotId);
          if (idx === -1) {
            return null;
          }
          return unassignedLots.splice(idx, 1)[0];
        }
        const sourceSeat = seats.find((seat) => seat.id === sourceId);
        if (!sourceSeat || !Array.isArray(sourceSeat.lots)) {
          return null;
        }
        const index = sourceSeat.lots.findIndex((lot) => lot.lot_id === lotId);
        if (index === -1) {
          return null;
        }
        return sourceSeat.lots.splice(index, 1)[0];
      };

      const moveLot = (fromSeatId, toSeatId, lotId, insertIndex = null) => {
        if (!fromSeatId || !toSeatId || !lotId) {
          return;
        }
        const lot = removeLotFromSource(fromSeatId, lotId);
        if (!lot) {
          return;
        }
        if (toSeatId === "unassigned") {
          if (insertIndex !== null && insertIndex >= 0 && insertIndex <= unassignedLots.length) {
            unassignedLots.splice(insertIndex, 0, lot);
          } else {
            unassignedLots.push(lot);
          }
          lot.source_inspector_col = "";
          renderUnassignedLots();
        } else {
          const targetSeat = seats.find((seat) => seat.id === toSeatId);
          if (!targetSeat || !Array.isArray(targetSeat.lots)) {
            return;
          }
          const inspectorColumn = inspectorColumnMap[targetSeat.name];
          if (typeof inspectorColumn === "string" && inspectorColumn.trim()) {
            lot.source_inspector_col = inspectorColumn;
          }
          if (fromSeatId === toSeatId) {
            // 同じ座席内での移動：順番を入れ替え
            if (insertIndex !== null && insertIndex >= 0 && insertIndex <= targetSeat.lots.length) {
              targetSeat.lots.splice(insertIndex, 0, lot);
            } else {
              targetSeat.lots.push(lot);
            }
          } else {
            // 別の座席への移動
            if (insertIndex !== null && insertIndex >= 0 && insertIndex <= targetSeat.lots.length) {
              targetSeat.lots.splice(insertIndex, 0, lot);
            } else {
              targetSeat.lots.push(lot);
            }
          }
        }
        renderSeats();
      };
      const distributeIntegerValue = (value, segments) => {
        if (!Number.isFinite(value) || segments <= 0) {
          return Array(segments).fill(0);
        }
        const base = Math.floor(value / segments);
        let remainder = value - base * segments;
        const values = [];
        for (let index = 0; index < segments; index += 1) {
          const extra = remainder > 0 ? 1 : 0;
          values.push(base + extra);
          if (extra) {
            remainder -= 1;
          }
        }
        return values;
      };

      const distributeFloatValue = (value, segments, decimals = 4) => {
        if (!Number.isFinite(value) || segments <= 0) {
          return Array(segments).fill(0);
        }
        const base = value / segments;
        let remainder = value - base * segments;
        remainder = Math.abs(remainder) < 1e-8 ? 0 : remainder;
        const values = [];
        for (let index = 0; index < segments; index += 1) {
          let entry = base;
          if (index === segments - 1 && remainder) {
            entry += remainder;
          }
          if (typeof decimals === "number") {
            entry = Number(entry.toFixed(decimals));
          }
          values.push(entry);
        }
        return values;
      };

      const createSplitLotsForLot = (lot, countValue) => {
        const normalizedCount = Number.isFinite(countValue)
          ? Math.max(2, Math.floor(countValue))
          : 2;
        const splitGroupLabel =
          lot.split_group || lot.lot_id || `split-${Date.now().toString(36)}`;
        const uniqueSuffix = Date.now().toString(36);
        const sanitizedIdBase = `${splitGroupLabel}-${uniqueSuffix}`.replace(/[^A-Za-z0-9_-]/g, "-");
        const inspectionHours = Number(lot.inspection_time) || 0;
        const inspectionValues =
          inspectionHours > 0
            ? distributeFloatValue(inspectionHours, normalizedCount, 4)
            : Array(normalizedCount).fill(0);
        const clones = [];
        for (let index = 0; index < normalizedCount; index += 1) {
          const clone = { ...lot };
          const sequence = index + 1;
          clone.split_group = splitGroupLabel;
          clone.split_index = sequence;
          clone.split_total = normalizedCount;
          clone.lot_id = `${sanitizedIdBase}-S${sequence}`;
          // 分割後も元ロットと同じlot_keyを維持する
          clone.lot_key = lot.lot_key || lot.lot_id || splitGroupLabel;
          if (inspectionHours > 0) {
            clone.inspection_time = inspectionValues[index];
          }
          clones.push(clone);
        }
        return clones;
      };

      const hideLotSplitMenu = () => {
        if (!lotSplitMenu) {
          activeSplitTarget = null;
          return;
        }
        lotSplitMenu.classList.remove("visible");
        lotSplitMenu.classList.add("hidden");
        lotSplitMenu.setAttribute("aria-hidden", "true");
        lotSplitMenu.style.transform = "translate(-9999px, -9999px)";
        activeSplitTarget = null;
      };

      const openLotSplitMenu = (seatId, lotId, event) => {
        if (!lotSplitMenu || !lotId) {
          return;
        }
        activeSplitTarget = { seatId, lotId };
        lotSplitMenu.classList.remove("hidden");
        lotSplitMenu.classList.add("visible");
        lotSplitMenu.setAttribute("aria-hidden", "false");
        lotSplitMenu.style.transform = "translate(0, 0)";
        if (lotSplitCountInput) {
          lotSplitCountInput.value = "2";
        }
        const areaRect = gridArea?.getBoundingClientRect();
        const menuWidth = lotSplitMenu.offsetWidth || 220;
        const menuHeight = lotSplitMenu.offsetHeight || 140;
        let left = event.clientX;
        let top = event.clientY;
        if (areaRect) {
          left -= areaRect.left;
          top -= areaRect.top;
          const maxLeft = Math.max(8, areaRect.width - menuWidth - 8);
          const maxTop = Math.max(8, areaRect.height - menuHeight - 8);
          left = Math.min(Math.max(8, left), maxLeft);
          top = Math.min(Math.max(8, top), maxTop);
        }
        lotSplitMenu.style.left = `${left}px`;
        lotSplitMenu.style.top = `${top}px`;
        lotSplitCountInput?.focus();
      };

      const applySplitForActiveLot = () => {
        if (!activeSplitTarget) {
          return;
        }
        const { seatId, lotId } = activeSplitTarget;
        const rawCount = Number(lotSplitCountInput?.value);
        const splitCount = Number.isFinite(rawCount) ? Math.max(2, Math.floor(rawCount)) : 2;
        let targetLots = null;
        if (seatId === "unassigned") {
          targetLots = unassignedLots;
        } else {
          const seat = seats.find((candidate) => candidate.id === seatId);
          if (!seat || !Array.isArray(seat.lots)) {
            hideLotSplitMenu();
            return;
          }
          targetLots = seat.lots;
        }
        const index = targetLots.findIndex((lot) => lot.lot_id === lotId);
        if (index === -1) {
          hideLotSplitMenu();
          return;
        }
        const lot = targetLots[index];
        const splitted = createSplitLotsForLot(lot, splitCount);
        targetLots.splice(index, 1, ...splitted);
        hideLotSplitMenu();
        renderSeats();
      };

      const normalizeInspectorList = (value) => {
        if (!value) {
          return [];
        }
        if (Array.isArray(value)) {
          return value.filter((item) => !!item).map((item) => item.trim()).filter(Boolean);
        }
        return String(value)
          .split(/[、,;/]+/)
          .map((item) => item.trim())
          .filter(Boolean);
      };

      const floatingTooltip = document.getElementById("floating-tooltip");
      const highlightSplitGroup = (groupId) => {
        document
          .querySelectorAll(".lot-card--group-highlight")
          .forEach((el) => el.classList.remove("lot-card--group-highlight"));
        document
          .querySelectorAll(".seat-card--group-highlight")
          .forEach((el) => el.classList.remove("seat-card--group-highlight"));
        document
          .querySelectorAll(".unassigned-area--group-highlight")
          .forEach((el) => el.classList.remove("unassigned-area--group-highlight"));
        if (!groupId) {
          return;
        }
        document
          .querySelectorAll(`.lot-card[data-highlight-group="${groupId}"]`)
          .forEach((el) => el.classList.add("lot-card--group-highlight"));
        document.querySelectorAll(".seat-card").forEach((card) => {
          const hasGroup = card.querySelector(`.lot-card[data-highlight-group="${groupId}"]`);
          if (hasGroup) {
            card.classList.add("seat-card--group-highlight");
          }
        });
        if (unassignedArea || unassignedContainer) {
          const hasGroup =
            unassignedContainer?.querySelector(`.lot-card[data-highlight-group="${groupId}"]`) ||
            unassignedArea?.querySelector(`.lot-card[data-highlight-group="${groupId}"]`);
          if (hasGroup) {
            (unassignedArea || unassignedContainer).classList.add("unassigned-area--group-highlight");
          }
        }
      };

      let lotIdCounts = new Map();
      const rebuildLotIdCounts = () => {
        const counts = new Map();
        const addLotId = (lot) => {
          if (!lot || typeof lot !== "object") {
            return;
          }
          const lotId = lot.lot_id ? String(lot.lot_id) : "";
          if (!lotId) {
            return;
          }
          counts.set(lotId, (counts.get(lotId) || 0) + 1);
        };
        seats.forEach((seat) => {
          if (!seat || !Array.isArray(seat.lots)) {
            return;
          }
          seat.lots.forEach(addLotId);
        });
        if (Array.isArray(unassignedLots)) {
          unassignedLots.forEach(addLotId);
        }
        lotIdCounts = counts;
      };

      const getHighlightGroupId = (lot) => {
        if (!lot || typeof lot !== "object") {
          return "";
        }
        if (lot.split_group) {
          return String(lot.split_group);
        }
        const lotId = lot.lot_id ? String(lot.lot_id) : "";
        if (lotId && (lotIdCounts.get(lotId) || 0) >= 2) {
          return lotId;
        }
        return "";
      };

      const updateFloatingTooltipPosition = (rect) => {
        if (!floatingTooltip || !rect) {
          return;
        }
        const viewportWidth = window.innerWidth || document.documentElement.clientWidth;
        const viewportHeight = window.innerHeight || document.documentElement.clientHeight;
        let left = rect.left + rect.width / 2;
        let top = rect.top;
        const safeMargin = 24;
        left = Math.min(Math.max(left, safeMargin), viewportWidth - safeMargin);
        top = Math.min(Math.max(top, safeMargin), viewportHeight - safeMargin);
        floatingTooltip.style.left = `${left}px`;
        floatingTooltip.style.top = `${top}px`;
      };

      const showFloatingTooltip = (event, lot) => {
        if (!floatingTooltip) {
          return;
        }
        floatingTooltip.innerHTML = generateLotTooltipContent(lot);
        floatingTooltip.classList.add("visible");
        updateFloatingTooltipPosition(event.currentTarget.getBoundingClientRect());
      };

      const hideFloatingTooltip = () => {
        if (!floatingTooltip) {
          return;
        }
        floatingTooltip.classList.remove("visible");
      };

      const generateLotTooltipContent = (lot) => {
        const shippingDate = normalizeShippingDateValue(lot.shipping_date) || "未設定";
        const productNameRaw = (lot.product_name || "").trim();
        const productCode = ((lot.product_code || lot.lot_id || "").replace(/^品番/, "").trim()) || "";
        const productLabel = productNameRaw || productCode || "未設定";
        const processName = (lot.process_name || "").replace(/^工程名?/, "").trim();
        const inspectionHours = Number(lot.inspection_time) || 0;
        const inspectionText = inspectionHours > 0 ? `${inspectionHours.toFixed(2)}h` : "未設定";
        const lines = [
          `出荷予定日：${shippingDate}`,
          processName ? `工程：${processName}` : "工程：未設定",
          `検査時間：${inspectionText}`,
          `品名：${productLabel}`,
        ];
        return lines.join("<br />");
      };

      const createLotCard = (seatId, lot) => {
        const lotCard = document.createElement("div");
        lotCard.className = "lot-card";
        lotCard.draggable = true;
        lotCard.dataset.seatId = seatId;
        lotCard.dataset.lotId = lot.lot_id;
        const highlightGroupId = getHighlightGroupId(lot);
        if (highlightGroupId) {
          lotCard.dataset.highlightGroup = highlightGroupId;
        }

        const shippingDateRaw = normalizeShippingDateValue(lot.shipping_date);
        const hasSameDayCleaning = isSameDayCleaningShippingDate(shippingDateRaw);
        const hasTodayShipping = !hasSameDayCleaning && isShippingDateToday(shippingDateRaw);
        if (hasSameDayCleaning) {
          lotCard.classList.add("lot-card--same-day-cleaning");
        } else if (hasTodayShipping) {
          lotCard.classList.add("lot-card--today");
        }
        const product = (lot.product_code || lot.product_name || lot.lot_id || "未設定").replace(/^品番/, "").trim();
        const process = (lot.process_name || "工程未設定").replace(/^工程名?/, "").trim();
        const line = document.createElement("div");
        line.className = "lot-line";
        line.textContent = `${product} ｜ ${process}`;
        lotCard.appendChild(line);
        lotCard.addEventListener("contextmenu", (event) => {
          event.preventDefault();
          event.stopPropagation();
          if (editingMode) {
            return;
          }
          openLotSplitMenu(seatId, lot.lot_id, event);
        });

        lotCard.addEventListener("mouseenter", function (event) {
          if (editingMode || draggingLot) {
            return;
          }
          lotCard.classList.add("lot-card--hover-highlight");
          if (highlightGroupId) {
            highlightSplitGroup(highlightGroupId);
          }
          showFloatingTooltip(event, lot);
        });
        lotCard.addEventListener("mousemove", function (event) {
          if (!floatingTooltip || !floatingTooltip.classList.contains("visible") || draggingLot) {
            return;
          }
          updateFloatingTooltipPosition(event.currentTarget.getBoundingClientRect());
        });
        lotCard.addEventListener("mouseleave", () => {
          hideFloatingTooltip();
          lotCard.classList.remove("lot-card--hover-highlight");
          if (highlightGroupId) {
            highlightSplitGroup("");
          }
        });
        lotCard.addEventListener("click", () => {
          if (highlightGroupId) {
            highlightSplitGroup(highlightGroupId);
          }
        });

        lotCard.addEventListener("dragstart", (event) => {
          if (editingMode) {
            event.preventDefault();
            return;
          }
          event.stopPropagation();
          // ドラッグ開始時にツールチップを非表示にする
          hideFloatingTooltip();
          lotCard.classList.remove("lot-card--hover-highlight");
          if (highlightGroupId) {
            highlightSplitGroup("");
          }
          draggingLot = { seatId, lotId: lot.lot_id };
          lotCard.classList.add("dragging-lot");
          event.dataTransfer?.setData("text/plain", lot.lot_id);
        });

        lotCard.addEventListener("dragend", () => {
          draggingLot = null;
          lotCard.classList.remove("dragging-lot");
          clearDropStyles();
          // ドラッグ終了時にツールチップを非表示にする（マウスが離れた状態なので）
          hideFloatingTooltip();
        });

        return lotCard;
      };

      const createSeatCard = (seat) => {
        const card = document.createElement("button");
        card.type = "button";
        card.className = "seat-card";
        card.dataset.seatId = seat.id;
        const left = (seat.col - 1) * (currentSlotWidth + currentSlotGap);
        const top = (seat.row - 1) * (currentSlotHeight + currentSlotGap);
        card.style.left = `${left}px`;
        card.style.top = `${top}px`;
        card.draggable = editingMode;
        card.style.width = `${currentSlotWidth}px`;
        card.style.height = `${currentSlotHeight}px`;

        const seatLots = Array.isArray(seat.lots) ? seat.lots : [];
        const seatHasSameDayCleaningLot = seatLots.some((lot) =>
          isSameDayCleaningShippingDate(normalizeShippingDateValue(lot.shipping_date))
        );
        const seatHasTodayLot = seatLots.some((lot) => {
          const label = normalizeShippingDateValue(lot.shipping_date);
          return !isSameDayCleaningShippingDate(label) && isShippingDateToday(label);
        });
        if (seatHasTodayLot) {
          card.classList.add("seat-card--today-lot");
        } else if (seatHasSameDayCleaningLot) {
          card.classList.add("seat-card--same-day-cleaning");
        }

        const hasName = !!seat.name;
        if (hasName) {
          const header = document.createElement("div");
          header.className = "seat-header";

          const nameLabel = document.createElement("span");
          nameLabel.className = "seat-name";
          nameLabel.textContent = seat.name;
          header.appendChild(nameLabel);

          if (!editingMode) {
            const totalLabel = document.createElement("span");
            totalLabel.className = "total-time";
            totalLabel.textContent = formatSecondsToHoursString(calculateTotalSecondsForSeat(seat));
            header.appendChild(totalLabel);
          }

          card.appendChild(header);
        } else if (!editingMode && seatLots.length > 0) {
          // 空席でもロットがある場合はヘッダーを表示（空席表示）
          const header = document.createElement("div");
          header.className = "seat-header";
          const nameLabel = document.createElement("span");
          nameLabel.className = "seat-name";
          nameLabel.textContent = "空席";
          nameLabel.style.color = "#999";
          header.appendChild(nameLabel);
          card.appendChild(header);
        }

        if (!editingMode) {
          const lotList = document.createElement("div");
          lotList.className = "lot-list";
          lotList.dataset.seatId = seat.id;
          seatLots.forEach((lot) => lotList.appendChild(createLotCard(seat.id, lot)));
          
          // 同じ座席内でのロットカードの順番入れ替えを可能にする
          lotList.addEventListener("dragover", (event) => {
            if (!draggingLot || editingMode) {
              return;
            }
            event.preventDefault();
            const afterElement = getDragAfterElement(lotList, event.clientY);
            const draggingElement = lotList.querySelector(`[data-lot-id="${draggingLot.lotId}"]`);
            if (afterElement == null) {
              lotList.appendChild(draggingElement);
            } else {
              lotList.insertBefore(draggingElement, afterElement);
            }
          });
          
          lotList.addEventListener("drop", (event) => {
            event.preventDefault();
            if (!draggingLot || editingMode) {
              return;
            }
            const targetSeatId = lotList.dataset.seatId;
            if (!targetSeatId) {
              return;
            }
            const afterElement = getDragAfterElement(lotList, event.clientY);
            const targetSeat = seats.find((s) => s.id === targetSeatId);
            if (!targetSeat || !Array.isArray(targetSeat.lots)) {
              return;
            }
            let insertIndex = null;
            if (afterElement) {
              const afterLotId = afterElement.dataset.lotId;
              insertIndex = targetSeat.lots.findIndex((lot) => lot.lot_id === afterLotId);
            } else {
              insertIndex = targetSeat.lots.length;
            }
            moveLot(draggingLot.seatId, targetSeatId, draggingLot.lotId, insertIndex);
            draggingLot = null;
            clearDropStyles();
          });
          
          card.appendChild(lotList);
        }

        if (seat.id === selectedSeatId) {
          card.classList.add("selected");
        }

        card.addEventListener("click", () => setSelectedSeat(seat.id));
        card.addEventListener("dblclick", () => openInspectorDropdown(seat.id, card));
        card.addEventListener("dragstart", (event) => {
          if (!editingMode || draggingLot) {
            event.preventDefault();
            return;
          }
          draggingSeatId = seat.id;
          card.classList.add("dragging");
          event.dataTransfer?.setData("text/plain", seat.id);
        });
        card.addEventListener("dragend", () => {
          draggingSeatId = null;
          card.classList.remove("dragging");
          clearDropStyles();
        });
        card.addEventListener("dragover", (event) => {
          if ((editingMode && draggingSeatId) || (!editingMode && draggingLot)) {
            event.preventDefault();
            card.classList.add("drop-target");
          }
        });
        card.addEventListener("dragleave", () => card.classList.remove("drop-target"));
        card.addEventListener("drop", (event) => {
          event.preventDefault();
          const targetId = event.currentTarget.dataset.seatId;
          if (!targetId) return;
          // ロットリストへのドロップはlotListのdropイベントで処理されるため、ここでは処理しない
          if (event.target.closest(".lot-list")) {
            return;
          }
          if (!editingMode && draggingLot) {
            const targetSeat = seats.find((seat) => seat.id === targetId);
            if (targetSeat && Array.isArray(targetSeat.lots)) {
              moveLot(draggingLot.seatId, targetId, draggingLot.lotId, targetSeat.lots.length);
            } else {
              moveLot(draggingLot.seatId, targetId, draggingLot.lotId);
            }
            draggingLot = null;
            renderSeats();
            clearDropStyles();
            return;
          }
          if (editingMode && draggingSeatId && draggingSeatId !== targetId) {
            swapSeats(draggingSeatId, targetId);
            renderSeats();
            updateEditorPanel();
          }
          clearDropStyles();
        });

        return card;
      };

      const renderSeats = () => {
        hideLotSplitMenu();
        hideFloatingTooltip();
        applyModeSizes();
        grid.innerHTML = "";
        rebuildLotIdCounts();
        seats
          .slice()
          .sort((a, b) => (a.row === b.row ? a.col - b.col : a.row - b.row))
          .forEach((seat) => grid.appendChild(createSeatCard(seat)));
        updateGridDimensions();
        renderUnassignedLots();
        fitSeatNames();
      };

      const fitSeatNames = () => {
        // 検査員名を省略ではなく縮小して1行に収める
        const headers = grid.querySelectorAll(".seat-header");
        headers.forEach((header) => {
          const nameEl = header.querySelector(".seat-name");
          if (!nameEl) {
            return;
          }
          nameEl.style.removeProperty("font-size");
          nameEl.style.removeProperty("white-space");
          nameEl.style.removeProperty("display");
          nameEl.style.removeProperty("-webkit-line-clamp");
          nameEl.style.removeProperty("-webkit-box-orient");
          nameEl.style.removeProperty("overflow");
          nameEl.title = nameEl.textContent || "";

          const totalEl = header.querySelector(".total-time");
          const headerWidth = header.clientWidth || 0;
          const totalWidth = totalEl ? (totalEl.getBoundingClientRect().width || 0) : 0;
          const gap = 10;
          const available = Math.max(0, headerWidth - totalWidth - gap);
          if (!available) {
            return;
          }

          // 現在のフォントサイズから最小まで段階的に縮小
          const computed = window.getComputedStyle(nameEl);
          const basePx = Math.max(10, parseFloat(computed.fontSize || "14"));
          const minPx = Math.max(12, Math.min(basePx, basePx * 0.86));

          let current = basePx;
          nameEl.style.fontSize = `${current}px`;
          let safe = 0;
          while (safe < 20 && nameEl.scrollWidth > available && current > minPx) {
            current = Math.max(minPx, current - 0.5);
            nameEl.style.fontSize = `${current}px`;
            safe += 1;
          }
          // 省略ではなく縮小を優先（どうしても収まらない場合でも、切れは発生し得るが省略記号は出さない）
          nameEl.style.textOverflow = "clip";
        });
      };

      const setEditingMode = (enabled) => {
        editingMode = enabled;
        applyModeSizes();
        document.body.classList.toggle("editing", enabled);
        toggleEditButton.textContent = enabled ? "ロット編集モード" : "座席編集モード";
        if (boardTitle) {
        boardTitle.textContent = editingMode ? "座席プレビュー" : "検査ロット振分けレイアウト";
        }
        if (!enabled) {
          selectedSeatId = null;
          updateEditorPanel();
          closeInspectorDropdown();
        }
        setInstructionContent();
        renderSeats();
      };

      applyButton.addEventListener("click", () => {
        if (!editingMode || !selectedSeatId) {
          return;
        }
        const target = seats.find((seat) => seat.id === selectedSeatId);
        if (!target) {
          return;
        }
        const parsedRow = parseFloat(seatRowInput.value);
        const parsedCol = parseFloat(seatColInput.value);
        target.name = seatNameInput.value.trim();
        if (!Number.isNaN(parsedRow)) {
          target.row = parsedRow;
        }
        if (!Number.isNaN(parsedCol)) {
          target.col = parsedCol;
        }
        renderSeats();
      });

      clearButton.addEventListener("click", () => {
        if (!editingMode || !selectedSeatId) {
          return;
        }
        const target = seats.find((seat) => seat.id === selectedSeatId);
        if (!target) {
          return;
        }
        target.name = "";
        target.lots = [];
        seatNameInput.value = "";
        renderSeats();
        updateEditorPanel();
      });

      lotSplitApplyButton?.addEventListener("click", applySplitForActiveLot);
      lotSplitCancelButton?.addEventListener("click", hideLotSplitMenu);
      lotSplitCountInput?.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          applySplitForActiveLot();
        }
      });

      toggleEditButton.addEventListener("click", () => setEditingMode(!editingMode));

      const buildSplitGroupId = (groupKey, fallback) => {
        const raw = String(groupKey || fallback || `split-${Date.now().toString(36)}`).trim();
        return raw || `split-${Date.now().toString(36)}`;
      };

      const normalizeSplitMetadata = () => {
        const groups = new Map();
        const splitSuffixPattern = /-S\\d+$/;
        const isExplicitSplit = (lot) =>
          !!(lot && (lot.split_group || (lot.lot_id && splitSuffixPattern.test(lot.lot_id))));
        const getSplitGroupKey = (lot) => {
          if (lot.split_group) {
            return String(lot.split_group);
          }
          if (lot.lot_id && splitSuffixPattern.test(lot.lot_id)) {
            return String(lot.lot_id).replace(splitSuffixPattern, "");
          }
          return "";
        };
        const addLot = (lot) => {
          if (!lot || typeof lot !== "object") {
            return;
          }
          if (!isExplicitSplit(lot)) {
            return;
          }
          const groupKey = getSplitGroupKey(lot);
          if (!groupKey) {
            return;
          }
          const key = String(groupKey);
          if (!groups.has(key)) {
            groups.set(key, []);
          }
          groups.get(key).push(lot);
        };
        seats.forEach((seat) => {
          if (!seat || !Array.isArray(seat.lots)) {
            return;
          }
          seat.lots.forEach(addLot);
        });
        if (Array.isArray(unassignedLots)) {
          unassignedLots.forEach(addLot);
        }

        groups.forEach((lots) => {
          if (lots.length < 2) {
            return;
          }
          let groupId = "";
          for (const lot of lots) {
            if (lot.split_group) {
              groupId = lot.split_group;
              break;
            }
          }
          if (!groupId) {
            const seed = lots.find((lot) => lot.lot_id || lot.split_group);
            groupId = buildSplitGroupId(
              seed?.lot_id || seed?.split_group,
              "split"
            );
          }
          const total = lots.length;
          lots.forEach((lot, index) => {
            lot.split_group = groupId;
            lot.split_total = total;
            lot.split_index = index + 1;
          });
        });
      };

      const craftJsonPayload = () => {
        // 分割メタ情報が欠けている場合は保存前に補完する
        normalizeSplitMetadata();
        return {
          seats,
          unassigned_lots: unassignedLots
        };
      };

      const saveJsonFileSystem = async () => {
        try {
          const payload = craftJsonPayload();
          const endpoint = SAVE_ENDPOINT || "/save-seating-chart";
          const response = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });
          if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`保存に失敗しました: ${response.status} ${response.statusText}${errorText ? ` - ${errorText}` : ""}`);
          }
          alert(`${SEATING_JSON_FILE_NAME} をネットワーク共有に保存しました。`);
        } catch (error) {
          console.error("保存エラー:", error);
          alert(`保存に失敗しました: ${error.message}`);
        }
      };



    if (saveButton) {
      saveButton.addEventListener("click", saveJsonFileSystem);
    }

    document.addEventListener("click", (event) => {
        hideFloatingTooltip();
        const clickInsideLot = event.target.closest(".lot-card");
        if (lotSplitMenu && !lotSplitMenu.contains(event.target) && !clickInsideLot) {
          hideLotSplitMenu();
        }
        if (!editingMode) {
          return;
        }
        if (!inspectorDropdown.contains(event.target) && !event.target.closest(".seat-card")) {
          closeInspectorDropdown();
        }
      });

      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
          hideLotSplitMenu();
        }
      });

      const setInstructionContent = () => {
        const instruction = document.querySelector(".edit-instruction");
        if (!instruction) {
          return;
        }
        instruction.textContent = editingMode
          ? "座席位置を変更をしたら【変更を保存】ボタンで自動的にネットワーク共有に保存されます。"
          : `アプリから起動された座席表は【変更を保存】ボタンで自動的にネットワーク共有に保存されます。
割当を整えたらアプリに戻って「ロット振分変更反映」を押してください。`;
      };

      document.addEventListener("DOMContentLoaded", () => {
        fillDatalist();
        setEditingMode(false);
        renderSeats();
        setInstructionContent();
        loadLatestSeatingData();
      });

      let resizeAnimationFrame = 0;
      const scheduleResizeRender = () => {
        if (resizeAnimationFrame) {
          cancelAnimationFrame(resizeAnimationFrame);
        }
        if (typeof window.requestAnimationFrame !== "function") {
          renderSeats();
          return;
        }
        resizeAnimationFrame = requestAnimationFrame(() => {
          resizeAnimationFrame = 0;
          renderSeats();
        });
      };
      window.addEventListener("resize", scheduleResizeRender);
      if (typeof ResizeObserver === "function" && gridArea) {
        const layoutObserver = new ResizeObserver(scheduleResizeRender);
        layoutObserver.observe(gridArea);
      }
    </script>
  </body>
</html>
"""


def generate_html(
    chart: Dict[str, List[Dict[str, object]]],
    output_path: str,
    inspector_candidates: Optional[List[str]] = None,
    save_endpoint: Optional[str] = None,
) -> None:
    """HTML_TEMPLATEにシートを埋め込み、座席UIを生成する共通関数。inspector_candidatesを指定すればautocomplete候補を制御できます。"""
    candidates = inspector_candidates or DEFAULT_INSPECTOR_NAMES
    sorted_names = sorted({name for name in candidates if name and name.strip()})
    column_map = chart.get("inspector_column_map", {}) or {}
    html = (
        HTML_TEMPLATE.replace("SEATING_DATA_PLACEHOLDER", json.dumps(chart, ensure_ascii=False))
        .replace("INSPECTOR_CANDIDATES_PLACEHOLDER", json.dumps(sorted_names, ensure_ascii=False))
        .replace("INSPECTOR_COLUMN_MAP_PLACEHOLDER", json.dumps(column_map, ensure_ascii=False))
        .replace("SEATING_JSON_PATH_PLACEHOLDER", json.dumps(SEATING_JSON_PATH, ensure_ascii=False))
        .replace("SEATING_JSON_FILE_NAME_PLACEHOLDER", json.dumps(SEATING_JSON_FILE_NAME, ensure_ascii=False))
        .replace("SAVE_ENDPOINT_PLACEHOLDER", json.dumps(save_endpoint or "", ensure_ascii=False))
    )
    directory = os.path.dirname(output_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write(html)


def main() -> None:
    chart = ensure_seating_json_exists()
    generate_html(chart, SEATING_HTML_PATH)
    print(f"HTMLを生成しました: {SEATING_HTML_PATH}")
    print(f"JSONを生成または更新しました: {SEATING_JSON_PATH}")


if __name__ == "__main__":
    main()
