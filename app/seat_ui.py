from __future__ import annotations

import csv
import json
import os
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
) -> Dict[str, List[Dict[str, object]]]:
    """
    既存の座席配置に lot 情報を紐づける。
    lots_by_inspector のキーには検査員名または座席 ID を受け入れ、見つかった最初のリストを seat["lots"] にセットする。
    """
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
    <title>検査ロット振分けレイアウト（ロット移動）</title>
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
        grid-template-columns: minmax(0, 1fr) 340px;
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
      }
      .grid-area {
        background: #fff;
        border-radius: 1rem;
        padding: 1rem 1.25rem 1.5rem;
        box-shadow: 0 18px 40px rgba(0, 0, 0, 0.08);
        position: relative;
        overflow: hidden;
      }
      .grid-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 1rem;
        margin-bottom: 0.8rem;
      }
      .grid-header h1 {
        margin: 0;
        font-size: 2rem;
      }
      .title-block {
        display: flex;
        flex-direction: column;
        gap: 0.15rem;
      }
      .edit-instruction {
        margin: 0;
        font-size: 0.95rem;
        color: #555;
      }
      .download-hint {
        margin: 0;
        font-size: 0.85rem;
        color: #1f7aef;
      }
      .grid-actions {
        display: flex;
        gap: 0.5rem;
        align-items: center;
      }
      #seat-grid {
        min-height: 500px;
        position: relative;
        margin-bottom: 2rem;
      }
      .seat-card {
        position: absolute;
        width: 180px;
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
      .seat-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 0.4rem;
      }
      .seat-name {
        font-size: 0.92rem;
        font-weight: 700;
      }
      .total-time {
        font-size: 0.78rem;
        color: #555;
      }
      .lot-list {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
        width: 100%;
        flex: 1;
        height: calc(5 * 20px + 0.25rem * 4);
        max-height: calc(5 * 20px + 0.25rem * 4);
        overflow-y: auto;
      }
      .lot-card {
        background: #f5f5f5;
        border-radius: 0.55rem;
        padding: 0.15rem 0.35rem;
        border: 1px solid #e2e2e2;
        cursor: grab;
        text-align: left;
        font-size: 0.7rem;
        line-height: 1.1;
        min-height: 18px;
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
      .lot-line {
        font-size: 0.72rem;
        color: #444;
        margin: 0;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
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
        font-size: 1.4rem;
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
        }
        .editor-panel {
          display: flex;
        }
      }
    </style>
  </head>
  <body>
    <main>
      <section class="grid-area">
        <div class="grid-header">
          <div class="title-block">
            <h1 id="board-title">検査ロット振分けレイアウト（ロット移動）</h1>
            <p class="edit-instruction">座席編集モード: 座席位置の入れ替えのみ。ロット編集モード: ロットカードをドラッグで別席に移動できます。</p>
          </div>
        <div class="grid-actions">
            <button id="save-json" class="primary mode-toggle" type="button">変更を保存</button>
            <button id="toggle-edit" class="secondary mode-toggle" type="button">座席編集モード</button>
          </div>
        </div>
        <p class="download-hint" id="json-hint"></p>
        <div id="seat-grid" aria-live="polite"></div>
        <div id="inspector-dropdown" class="inspector-dropdown">
          <div class="dropdown-title">検査員を選択</div>
          <div id="inspector-list"></div>
        </div>
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
        const SEATING_JSON_PATH = SEATING_JSON_PATH_PLACEHOLDER;
        const SEATING_JSON_FILE_NAME = SEATING_JSON_FILE_NAME_PLACEHOLDER;
      const seats = Array.isArray(seatingData.seats) ? seatingData.seats : [];
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
      const inspectorDropdown = document.getElementById("inspector-dropdown");
      const inspectorList = document.getElementById("inspector-list");
      const inspectorDatalist = document.getElementById("inspector-names");
      const modeSizes = {
        view: { width: 180, height: 150, gap: 8 },
        editing: { width: 135, height: 100, gap: 8 },
      };
      let currentSlotWidth = modeSizes.view.width;
      let currentSlotHeight = modeSizes.view.height;
      let currentSlotGap = modeSizes.view.gap;
      const boardTitle = document.getElementById("board-title");
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
          renderSeats();
        } catch (error) {
          console.warn("Failed to load latest seating JSON:", error);
        }
      };

      const applyModeSizes = () => {
        const { width, height, gap } = editingMode ? modeSizes.editing : modeSizes.view;
        currentSlotWidth = width;
        currentSlotHeight = height;
        currentSlotGap = gap;
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
      };

      const updateGridDimensions = () => {
        if (!seats.length) {
          return;
        }
        const maxRow = Math.max(...seats.map((seat) => seat.row || 0));
        const maxCol = Math.max(...seats.map((seat) => seat.col || 0));
        grid.style.height = `${maxRow * (currentSlotHeight + currentSlotGap)}px`;
        grid.style.width = `${maxCol * (currentSlotWidth + currentSlotGap)}px`;
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

      const moveLot = (fromSeatId, toSeatId, lotId) => {
        if (!fromSeatId || !toSeatId || !lotId || fromSeatId === toSeatId) {
          return;
        }
        const fromSeat = seats.find((seat) => seat.id === fromSeatId);
        const toSeat = seats.find((seat) => seat.id === toSeatId);
        if (!fromSeat || !toSeat || !Array.isArray(fromSeat.lots) || !Array.isArray(toSeat.lots)) {
          return;
        }
        const index = fromSeat.lots.findIndex((lot) => lot.lot_id === lotId);
        if (index === -1) {
          return;
        }
        const [lot] = fromSeat.lots.splice(index, 1);
        toSeat.lots.push(lot);
      };

      const createLotCard = (seatId, lot) => {
        const lotCard = document.createElement("div");
        lotCard.className = "lot-card";
        lotCard.draggable = true;
        lotCard.dataset.seatId = seatId;
        lotCard.dataset.lotId = lot.lot_id;

        const product = (lot.product_name || lot.lot_id || "未設定").replace(/^品番/, "").trim();
        const process = (lot.process_name || "工程未設定").replace(/^工程名?/, "").trim();
        const line = document.createElement("div");
        line.className = "lot-line";
        line.textContent = `${product} ｜ ${process}`;

        lotCard.appendChild(line);
        const inspectionTime = Number(lot.inspection_time) || 0;

        lotCard.addEventListener("dragstart", (event) => {
          if (editingMode) {
            event.preventDefault();
            return;
          }
          event.stopPropagation();
          draggingLot = { seatId, lotId: lot.lot_id };
          lotCard.classList.add("dragging-lot");
          event.dataTransfer?.setData("text/plain", lot.lot_id);
        });

        lotCard.addEventListener("dragend", () => {
          draggingLot = null;
          lotCard.classList.remove("dragging-lot");
          clearDropStyles();
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
        }

        if (!editingMode && hasName) {
          const lotList = document.createElement("div");
          lotList.className = "lot-list";
          const lots = Array.isArray(seat.lots) ? seat.lots : [];
          lots.forEach((lot) => lotList.appendChild(createLotCard(seat.id, lot)));
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
          if (!editingMode && draggingLot) {
            moveLot(draggingLot.seatId, targetId, draggingLot.lotId);
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
        applyModeSizes();
        grid.innerHTML = "";
        seats
          .slice()
          .sort((a, b) => (a.row === b.row ? a.col - b.col : a.row - b.row))
          .forEach((seat) => grid.appendChild(createSeatCard(seat)));
        updateGridDimensions();
      };

      const setEditingMode = (enabled) => {
        editingMode = enabled;
        applyModeSizes();
        document.body.classList.toggle("editing", enabled);
        toggleEditButton.textContent = enabled ? "ロット編集モード" : "座席編集モード";
        if (boardTitle) {
          boardTitle.textContent = editingMode ? "座席プレビュー（位置調整）" : "検査ロット振分けレイアウト（ロット移動）";
        }
        if (!enabled) {
          selectedSeatId = null;
          updateEditorPanel();
          closeInspectorDropdown();
        }
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

      toggleEditButton.addEventListener("click", () => setEditingMode(!editingMode));

      const fileSystemAvailable = () => typeof window.showSaveFilePicker === "function";
      const craftJsonPayload = () => ({ seats });
      const saveJsonFileSystem = async () => {
        if (!fileSystemAvailable()) {
          alert("FileSystem Access API をサポートしていない環境では保存できません。");
          return;
        }
        try {
          const payload = craftJsonPayload();
        const handle = await window.showSaveFilePicker({
          suggestedName: SEATING_JSON_FILE_NAME,
          types: [
            {
              description: "JSON Files",
              accept: { "application/json": [".json"] },
            },
          ],
          excludeAcceptAllOption: true,
        });
        const writable = await handle.createWritable();
        await writable.write(JSON.stringify(payload, null, 2));
        await writable.close();
        alert(`${SEATING_JSON_FILE_NAME} を保存しました。${SEATING_JSON_PATH} に上書きしてください。`);
      } catch (error) {
        if (error?.name !== "AbortError") {
          console.error("FileSystem Access API error", error);
        }
      }
    };


    if (saveButton) {
      saveButton.addEventListener("click", saveJsonFileSystem);
    }

    document.addEventListener("click", (event) => {
        if (!editingMode) {
          return;
        }
        if (!inspectorDropdown.contains(event.target) && !event.target.closest(".seat-card")) {
          closeInspectorDropdown();
        }
      });

      const setInstructionContent = () => {
        const instruction = document.querySelector(".edit-instruction");
        const downloadHint = document.getElementById("json-hint");
        if (instruction) {
          instruction.textContent = "";
        }
        if (downloadHint) {
          downloadHint.textContent = `Save path: ${SEATING_JSON_PATH}`;
        }
      };

      document.addEventListener("DOMContentLoaded", () => {
        fillDatalist();
        setEditingMode(false);
        renderSeats();
        setInstructionContent();
        loadLatestSeatingData();
      });
    </script>
  </body>
</html>
"""


def generate_html(
    chart: Dict[str, List[Dict[str, object]]],
    output_path: str,
    inspector_candidates: Optional[List[str]] = None,
) -> None:
    """
    HTML_TEMPLATE にシートを埋め込み、座席UIを生成する共通関数。
    inspector_candidates を指定すれば autocomplete 候補を制御できます。
    """
    candidates = inspector_candidates or DEFAULT_INSPECTOR_NAMES
    sorted_names = sorted({name for name in candidates if name and name.strip()})
    inspector_json = json.dumps(sorted_names, ensure_ascii=False)
    html = (
        HTML_TEMPLATE.replace("SEATING_DATA_PLACEHOLDER", json.dumps(chart, ensure_ascii=False))
        .replace("INSPECTOR_CANDIDATES_PLACEHOLDER", inspector_json)
        .replace("SEATING_JSON_PATH_PLACEHOLDER", json.dumps(SEATING_JSON_PATH, ensure_ascii=False))
        .replace(
            "SEATING_JSON_FILE_NAME_PLACEHOLDER",
            json.dumps(SEATING_JSON_FILE_NAME, ensure_ascii=False),
        )
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
