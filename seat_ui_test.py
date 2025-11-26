from __future__ import annotations

import csv
import json
import os
from typing import Dict, List, Optional

# ローカルまたはNAS上のパス（環境に合わせて書き換えてください）
SEATING_JSON_PATH = r"C:\Users\SEIZOU-20\Desktop\seating_chart.json"
SEATING_HTML_PATH = r"C:\Users\SEIZOU-20\Desktop\seat_ui_test.html"
CONFIG_ENV_PATH = "config.env"

FALLBACK_NAMES = [
    "鈴木悦代",
    "新井登志子",
    "前森美加",
    "谷藤奈津代",
    "宮谷博美",
    "豊田彩矢香",
    "大上明日香",
    "笠原文美代",
    "金井恵美",
    "鈴木里奈子",
    "氏家美奈",
    "加藤メリー",
    "勅使河原陽子",
    "金室裕子",
    "中野優美",
    "山中美佳",
    "南千鶴子",
    "横田美幸",
    "髙野香菜子",
    "島嵜聖人",
    "落合美里",
    "岩田美早季",
    "柴﨑楓華",
    "松本尚美",
    "伊藤喜代子",
    "田端政子",
    "大嶋渚",
    "坂本美由紀",
    "豊田奈津美",
    "門田義裕",
    "鋤崎楓華",
    "南千鶴子",
    "鈴木里奈子",
    "豊田彩矢香",
    "金井恵美",
    "松本尚美",
]

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
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            parsed[key.strip()] = value.strip()
    return parsed


def _load_inspectors_from_csv(path: str) -> List[str]:
    for encoding in ("utf-8", "cp932", "shift_jis"):
        try:
            with open(path, newline="", encoding=encoding) as handle:
                reader = csv.reader(handle)
                names: List[str] = []
                for idx, row in enumerate(reader, start=1):
                    if idx < 3:
                        continue
                    if len(row) < 2:
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
    if path:
        inspectors = _load_inspectors_from_csv(path)
        if inspectors:
            return inspectors
    return FALLBACK_NAMES


def _build_seating_chart() -> Dict[str, List[Dict[str, object]]]:
    names = _ensure_seat_names()
    entries: List[Dict[str, object]] = []
    idx = 0
    while len(entries) < len(GRID_POSITIONS):
        row, col = GRID_POSITIONS[len(entries)]
        name = names[idx % len(names)]
        entries.append({"id": f"s{len(entries) + 1}", "name": name, "row": row, "col": col})
        idx += 1
    return {"seats": entries}


def ensure_seating_json_exists() -> Dict[str, List[Dict[str, object]]]:
    if not os.path.exists(SEATING_JSON_PATH):
        chart = _build_seating_chart()
        _write_chart(chart)
        return chart
    try:
        with open(SEATING_JSON_PATH, "r", encoding="utf-8") as handle:
            chart = json.load(handle)
            if len(chart.get("seats", [])) < len(GRID_POSITIONS):
                raise ValueError("incomplete chart")
            return chart
    except (json.JSONDecodeError, OSError, ValueError):
        chart = _build_seating_chart()
        _write_chart(chart)
        return chart


def _write_chart(chart: Dict[str, List[Dict[str, object]]]) -> None:
    _ensure_dir()
    with open(SEATING_JSON_PATH, "w", encoding="utf-8") as handle:
        json.dump(chart, handle, ensure_ascii=False, indent=2)


def _ensure_dir() -> None:
    directory = os.path.dirname(SEATING_JSON_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)


def _ensure_dir_for(path: str) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)


DEFAULT_NAMES = _ensure_seat_names()

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang=\"ja\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>座席表プレビュー</title>
    <style>
      * {
        box-sizing: border-box;
      }
      body {
        margin: 0;
        min-height: 100vh;
        background: #f3f3f3;
        color: #111;
        font-family: \"Noto Sans JP\", \"Segoe UI\", system-ui, sans-serif;
      }
      body.editing main {
        grid-template-columns: minmax(0, 1fr) 320px;
      }
      body:not(.editing) .edit-instruction {
        display: none;
      }
      body:not(.editing) .editor-panel {
        display: none;
      }
      body:not(.editing) #download-json {
        display: none;
      }
      body:not(.editing) .download-hint {
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
      .grid-actions {
        display: flex;
        gap: 0.5rem;
        align-items: center;
      }
      .download-hint {
        font-size: 0.85rem;
        color: #666;
        display: flex;
        align-items: center;
        gap: 0.35rem;
      }
      .download-hint code {
        background: #f0f0f0;
        padding: 0.15rem 0.4rem;
        border-radius: 0.45rem;
        font-size: 0.85rem;
      }
      #seat-grid {
        min-height: 500px;
        position: relative;
      }
      .seat-card {
        position: absolute;
        width: 150px;
        height: 120px;
        border-radius: 1rem;
        border: 1px solid #d6d6d6;
        background: #fff;
        box-shadow: 0 8px 20px rgba(0, 0, 0, 0.1);
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 600;
        font-size: 1rem;
        transition: border-color 0.2s ease, transform 0.2s ease;
        cursor: pointer;
      }
      .seat-card.selected {
        border-color: #1f7aef;
        box-shadow: 0 0 0 3px rgba(31, 122, 239, 0.25);
      }
      .seat-card.dragging {
        opacity: 0.65;
        transform: scale(1.05);
      }
      .seat-card.drop-target {
        box-shadow: 0 0 0 3px rgba(31, 122, 239, 0.35);
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
            <h1 id="board-title">検査ロット振分け表</h1>
            <p class="edit-instruction">ドラッグ&ドロップ・ダブルクリックで編集し、「JSONダウンロード」で保存。</p>
          </div>
          <div class="grid-actions">
            <button id="download-json" class="primary" type="button">JSONダウンロード</button>
            <button id="toggle-edit" class="secondary mode-toggle" type="button">座席編集モード</button>
          </div>
        </div>
        <div class="download-hint">
          <span>保存先:</span>
          <code id="hint-path">SEATING_PATH_PLACEHOLDER</code>
        </div>
        <div id="seat-grid" aria-live="polite"></div>
        <div id="inspector-dropdown" class="inspector-dropdown">
          <div class="dropdown-title">検査員を選択</div>
          <div id="inspector-list"></div>
        </div>
      </section>
      <aside class="editor-panel">
        <h2>座席編集パネル</h2>
        <label>
          座席ID
          <input type="text" id="seat-id" readonly />
        </label>
        <label>
          検査員名
          <input type="text" id="seat-name" list="inspector-names" />
        </label>
        <label>
          行（row）
          <input type="number" id="seat-row" min="1" step="0.5" />
        </label>
        <label>
          列（col）
          <input type="number" id="seat-col" min="1" step="0.5" />
        </label>
        <button id="apply-seat" class="primary" type="button">適用</button>
        <button id="clear-seat" class="secondary" type="button">空席にする</button>
      </aside>
    </main>
    <datalist id="inspector-names"></datalist>
    <script>
      const seatingData = SEATING_DATA_PLACEHOLDER;
      const INSPECTOR_CANDIDATES = INSPECTOR_CANDIDATES_PLACEHOLDER;
      const targetJsonPath = "SEATING_PATH_PLACEHOLDER";
      const seats = Array.isArray(seatingData.seats) ? seatingData.seats : [];
      let selectedSeatId = null;
      let draggingSeatId = null;
      let editingMode = false;

      const grid = document.getElementById("seat-grid");
      const seatIdInput = document.getElementById("seat-id");
      const seatNameInput = document.getElementById("seat-name");
      const seatRowInput = document.getElementById("seat-row");
      const seatColInput = document.getElementById("seat-col");
      const applyButton = document.getElementById("apply-seat");
      const clearButton = document.getElementById("clear-seat");
      const downloadButton = document.getElementById("download-json");
      const toggleEditButton = document.getElementById("toggle-edit");
      const inspectorDropdown = document.getElementById("inspector-dropdown");
      const inspectorList = document.getElementById("inspector-list");
      const inspectorDatalist = document.getElementById("inspector-names");
      const hintPath = document.getElementById("hint-path");
      const modeSizes = {
        view: { width: 180, height: 120, gap: 5 },
        editing: { width: 140, height: 100, gap: 5 },
      };
      let currentSlotWidth = modeSizes.view.width;
      let currentSlotHeight = modeSizes.view.height;
      let currentSlotGap = modeSizes.view.gap;
      const boardTitle = document.getElementById("board-title");

      if (hintPath) {
        hintPath.textContent = targetJsonPath;
      }

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

      const createSeatCard = (seat) => {
        const card = document.createElement("button");
        card.type = "button";
        card.className = "seat-card";
        card.dataset.seatId = seat.id;
        const left = (seat.col - 1) * (currentSlotWidth + currentSlotGap);
        const top = (seat.row - 1) * (currentSlotHeight + currentSlotGap);
        card.style.left = `${left}px`;
        card.style.top = `${top}px`;
        card.draggable = true;
        card.style.width = `${currentSlotWidth}px`;
        card.style.height = `${currentSlotHeight}px`;

        const label = document.createElement("span");
        label.textContent = seat.name;
        card.appendChild(label);

        if (seat.id === selectedSeatId) {
          card.classList.add("selected");
        }

        card.addEventListener("click", () => setSelectedSeat(seat.id));
        card.addEventListener("dblclick", () => openInspectorDropdown(seat.id, card));
        card.addEventListener("dragstart", (event) => {
          if (!editingMode) {
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
          if (!editingMode) {
            return;
          }
          event.preventDefault();
          card.classList.add("drop-target");
        });
        card.addEventListener("dragleave", () => card.classList.remove("drop-target"));
        card.addEventListener("drop", (event) => {
          if (!editingMode) {
            return;
          }
          event.preventDefault();
          const targetId = event.currentTarget.dataset.seatId;
          if (draggingSeatId && targetId && draggingSeatId !== targetId) {
            swapSeats(draggingSeatId, targetId);
            renderSeats();
            updateEditorPanel();
          }
          clearDropStyles();
        });

        return card;
      };

      const renderSeats = () => {
        grid.innerHTML = "";
        seats
          .slice()
          .sort((a, b) => (a.row === b.row ? a.col - b.col : a.row - b.row))
          .forEach((seat) => {
            grid.appendChild(createSeatCard(seat));
          });
        updateGridDimensions();
      };

      const setEditingMode = (enabled) => {
        editingMode = enabled;
        document.body.classList.toggle("editing", enabled);
        toggleEditButton.textContent = enabled ? "通常表示" : "座席編集モード";
        const { width, height, gap } = editingMode ? modeSizes.editing : modeSizes.view;
        currentSlotWidth = width;
        currentSlotHeight = height;
        currentSlotGap = gap;
        if (boardTitle) {
          boardTitle.textContent = editingMode ? "座席表プレビュー" : "検査ロット振分け表";
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
        seatNameInput.value = "";
        renderSeats();
        updateEditorPanel();
      });

      toggleEditButton.addEventListener("click", () => setEditingMode(!editingMode));

      downloadButton.addEventListener("click", () => {
        if (!editingMode) {
          return;
        }
        const payload = { seats };
        const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement("a");
        anchor.href = url;
        anchor.download = "seating_chart.json";
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        URL.revokeObjectURL(url);
        if (hintPath) {
          hintPath.textContent = targetJsonPath;
        }
      });

      document.addEventListener("click", (event) => {
        if (!editingMode) {
          return;
        }
        if (!inspectorDropdown.contains(event.target) && !event.target.closest(".seat-card")) {
          closeInspectorDropdown();
        }
      });

      document.addEventListener("DOMContentLoaded", () => {
        fillDatalist();
        setEditingMode(false);
        renderSeats();
      });
    </script>
  </body>
</html>
"""


def generate_html_from_template(chart: Dict[str, List[Dict[str, object]]]) -> None:
    sorted_names = sorted({name for name in DEFAULT_NAMES if name and name.strip()})
    inspector_json = json.dumps(sorted_names, ensure_ascii=False)
    html = (
        HTML_TEMPLATE.replace("SEATING_DATA_PLACEHOLDER", json.dumps(chart, ensure_ascii=False))
        .replace("INSPECTOR_CANDIDATES_PLACEHOLDER", inspector_json)
        .replace("SEATING_PATH_PLACEHOLDER", SEATING_JSON_PATH)
    )
    _ensure_dir_for(SEATING_HTML_PATH)
    with open(SEATING_HTML_PATH, "w", encoding="utf-8") as handle:
        handle.write(html)


def main() -> None:
    chart = ensure_seating_json_exists()
    generate_html_from_template(chart)
    print(f"HTML を生成しました: {SEATING_HTML_PATH}")
    print(f"JSON を生成または更新しました: {SEATING_JSON_PATH}")


if __name__ == "__main__":
    main()
