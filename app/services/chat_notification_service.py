"""
ARAICHAT通知サービス
工程ごとのARAICHATルームにメッセージを送信する機能を提供
"""

import json
import hashlib
import time
import threading
from typing import Optional, Dict, List, Any
from pathlib import Path
import os
import requests
from loguru import logger
import pandas as pd


class ChatNotificationService:
    """ARAICHAT通知サービス"""

    _dedupe_lock = threading.Lock()
    _in_flight_keys: set[str] = set()
    _in_flight_cond = threading.Condition(_dedupe_lock)
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        room_config_path: Optional[str] = None
    ):
        """
        初期化
        
        Args:
            base_url: ARAICHATのベースURL
            api_key: ARAICHATのAPIキー
            room_config_path: 工程ごとのROOM_ID設定ファイルのパス
        """
        self.base_url = base_url
        self.api_key = api_key
        self.room_config_path = room_config_path
        self.process_room_map: Dict[str, str] = {}
        self.default_room_id: Optional[str] = None
        self._dedupe_cache_ttl_seconds = 10 * 60  # 10分（タイムアウト再送/二重クリックの抑止）

        # 送信の体感速度を優先（ReadTimeout時は重複送信防止のため再送しない設計）
        def _read_int_env(name: str, default: int) -> int:
            raw = os.getenv(name)
            if raw is None:
                return default
            try:
                val = int(str(raw).strip())
                return val if val > 0 else default
            except Exception:
                return default

        self._timeout_connect_seconds = _read_int_env("ARAICHAT_TIMEOUT_CONNECT_SECONDS", 3)
        # 「完了」はサーバ応答(2xx)を受け取った場合のみ表示したいため、既定は長めに待つ
        self._timeout_read_seconds = _read_int_env("ARAICHAT_TIMEOUT_READ_SECONDS", 60)
        self._max_retries = _read_int_env("ARAICHAT_MAX_RETRIES", 2)
        self._backoff_seconds = _read_int_env("ARAICHAT_BACKOFF_SECONDS", 1)
        
        if room_config_path:
            self._load_room_config()

    def _get_dedupe_cache_path(self) -> Path:
        local_app_data = os.getenv("LOCALAPPDATA")
        if local_app_data:
            base_dir = Path(local_app_data)
        else:
            base_dir = Path.home() / "AppData" / "Local"
            if not base_dir.exists():
                base_dir = Path.home()

        cache_dir = base_dir / "appearance_sorting_system" / "araichat"
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            return Path("araichat_send_dedupe.json")
        return cache_dir / "send_dedupe.json"

    def _load_dedupe_cache(self) -> Dict[str, float]:
        path = self._get_dedupe_cache_path()
        try:
            if not path.exists():
                return {}
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return {}
            out: Dict[str, float] = {}
            for key, ts in data.items():
                try:
                    out[str(key)] = float(ts)
                except Exception:
                    continue
            return out
        except Exception:
            return {}

    def _save_dedupe_cache(self, cache: Dict[str, float]) -> None:
        path = self._get_dedupe_cache_path()
        try:
            tmp_path = path.with_suffix(path.suffix + ".tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False)
            tmp_path.replace(path)
        except Exception:
            pass

    @staticmethod
    def _build_dedupe_key(room_id: str, message: str) -> str:
        digest = hashlib.sha256(f"{room_id}\n{message}".encode("utf-8", errors="ignore")).hexdigest()
        return digest

    def _should_suppress_duplicate(self, dedupe_key: str, now: float) -> bool:
        with self._dedupe_lock:
            cache = self._load_dedupe_cache()
            ttl = float(self._dedupe_cache_ttl_seconds)
            last_ts = cache.get(dedupe_key)
            return last_ts is not None and (now - float(last_ts)) <= ttl

    def _record_sent(self, dedupe_key: str, sent_at: float) -> None:
        with self._dedupe_lock:
            cache = self._load_dedupe_cache()
            ttl = float(self._dedupe_cache_ttl_seconds)
            cache = {k: v for k, v in cache.items() if isinstance(v, (int, float)) and (sent_at - float(v)) <= ttl}
            cache[dedupe_key] = float(sent_at)
            self._save_dedupe_cache(cache)

    def _load_room_config(self) -> None:
        """工程ごとのROOM_ID設定を読み込み"""
        try:
            if not self.room_config_path or not Path(self.room_config_path).exists():
                logger.warning(f"ARAICHAT ROOM設定ファイルが見つかりません: {self.room_config_path}")
                return
            
            with open(self.room_config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # デフォルトのROOM_IDを取得
            self.default_room_id = config.get('default_room_id')
            
            # 工程ごとのROOM_IDマッピング
            process_mappings = config.get('process_rooms', {})
            self.process_room_map = {
                str(process_name).strip(): str(room_id).strip()
                for process_name, room_id in process_mappings.items()
            }
            
            logger.info(f"ARAICHAT ROOM設定を読み込みました: {len(self.process_room_map)}件の工程マッピング")
            
        except Exception as e:
            logger.error(f"ARAICHAT ROOM設定の読み込みに失敗しました: {e}", exc_info=True)
    
    def get_room_id_for_process(self, process_name: Optional[str]) -> Optional[str]:
        """
        工程名に対応するROOM_IDを取得
        
        Args:
            process_name: 工程名
        
        Returns:
            ROOM_ID（見つからない場合はデフォルトROOM_ID）
        """
        if not process_name:
            return self.default_room_id
        
        process_name_clean = str(process_name).strip()
        
        # 完全一致で検索
        if process_name_clean in self.process_room_map:
            return self.process_room_map[process_name_clean]
        
        # 部分一致で検索（工程名に含まれるキーワードで検索）
        for key, room_id in self.process_room_map.items():
            if key in process_name_clean or process_name_clean in key:
                return room_id
        
        # 見つからない場合はデフォルトROOM_IDを返す
        return self.default_room_id
    
    def send_message(
        self,
        message: str,
        process_name: Optional[str] = None,
        title: Optional[str] = None
    ) -> bool:
        """
        メッセージをARAICHATに送信
        
        Args:
            message: 送信するメッセージ
            process_name: 工程名（工程ごとの送信先を決定するため）
            title: メッセージのタイトル（オプション）
        
        Returns:
            送信成功時はTrue、失敗時はFalse
        """
        if not self.base_url:
            logger.warning("ARAICHAT_BASE_URLが設定されていません")
            return False
        
        if not self.api_key:
            logger.warning("ARAICHAT_API_KEYが設定されていません")
            return False
        
        room_id = self.get_room_id_for_process(process_name)
        
        if not room_id:
            logger.warning(f"工程 '{process_name}' に対応するROOM_IDが見つかりません")
            return False

        # クライアント側の恒久対策：同一ROOM/同一本文を短時間で二重送信しない
        full_message = f"{title}\n{message}" if title else message
        dedupe_key = self._build_dedupe_key(room_id=str(room_id), message=str(full_message))
        now = time.time()
        if self._should_suppress_duplicate(dedupe_key, now):
            logger.warning(f"ARAICHAT二重送信を抑止しました: 工程={process_name}, ROOM_ID={room_id}")
            return True

        with self._dedupe_lock:
            if dedupe_key in self._in_flight_keys:
                # 既に同一送信が走っている場合、完了(=サーバ応答取得)まで待つ
                logger.warning(f"ARAICHAT送信が進行中のため待機します: 工程={process_name}, ROOM_ID={room_id}")
                self._in_flight_cond.wait(timeout=float(self._timeout_read_seconds) + 10.0)
                if self._should_suppress_duplicate(dedupe_key, time.time()):
                    return True
                return False
            self._in_flight_keys.add(dedupe_key)
        
        # URLの末尾スラッシュを調整
        base_url = self.base_url.rstrip("/")
        url = f"{base_url}/api/integrations/send/{room_id}"
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            # サーバが対応している場合に二重送信を防ぐ（未対応でも無害）
            "Idempotency-Key": dedupe_key,
            "X-Idempotency-Key": dedupe_key,
        }
        
        data = {"text": full_message}
        
        # リトライ設定
        max_retries = int(self._max_retries)
        backoff_seconds = int(self._backoff_seconds)  # 初期待機時間（指数バックオフ）
        timeout_connect = int(self._timeout_connect_seconds)   # 接続タイムアウト（秒）
        timeout_read = int(self._timeout_read_seconds)     # 読み取りタイムアウト（秒）
        
        try:
            for attempt in range(1, max_retries + 1):
                start_time = None
                try:
                    if attempt > 1:
                        wait_time = backoff_seconds * (2 ** (attempt - 2))
                        logger.info(f"ARAICHAT送信リトライ {attempt}/{max_retries}（{wait_time}秒待機後）...")
                        time.sleep(wait_time)

                    timeout = (timeout_connect, timeout_read)
                    start_time = time.time()

                    resp = requests.post(url, headers=headers, data=data, timeout=timeout)
                    elapsed_time = time.time() - start_time

                    logger.debug(f"ARAICHAT送信レスポンス: {resp.status_code}（処理時間: {elapsed_time:.2f}秒）")

                    resp.raise_for_status()
                    _ = resp.json()
                    logger.info(f"ARAICHAT送信成功: 工程={process_name}, ROOM_ID={room_id}")
                    self._record_sent(dedupe_key, time.time())
                    return True

                except requests.exceptions.ReadTimeout as e:
                    # 送信済みだが応答が返らない可能性があるため、
                    # Idempotency-Key を付与した上で「同一キーで再試行」して完了確認を試みる。
                    elapsed_time = (time.time() - start_time) if start_time else 0
                    if attempt < max_retries:
                        logger.warning(
                            f"ARAICHAT送信ReadTimeout（{elapsed_time:.2f}秒）: {e} - 同一Idempotency-Keyで再試行します"
                        )
                        continue
                    logger.error(
                        f"ARAICHAT送信ReadTimeout（{elapsed_time:.2f}秒）: {e} - 完了確認できないため失敗扱いにします"
                    )
                    return False

                except (requests.exceptions.ConnectTimeout, requests.exceptions.Timeout) as e:
                    elapsed_time = (time.time() - start_time) if start_time else 0
                    if attempt < max_retries:
                        logger.warning(f"ARAICHAT送信タイムアウト（{elapsed_time:.2f}秒）: {e} - リトライします")
                        continue
                    logger.error(f"ARAICHAT送信タイムアウトエラー（{max_retries}回試行後）: 工程={process_name}")
                    return False

                except requests.exceptions.HTTPError as e:
                    status_code = e.response.status_code if e.response else None
                    response_text = e.response.text if e.response else ""

                    # 一時的なサーバーエラー（5xx）の場合はリトライ
                    if status_code and 500 <= status_code < 600 and attempt < max_retries:
                        logger.warning(f"ARAICHAT送信HTTP {status_code} エラー: {e} - リトライします")
                        continue

                    logger.error(
                        f"ARAICHAT送信HTTPエラー: {e}, "
                        f"ステータスコード: {status_code}, "
                        f"レスポンス: {response_text}, "
                        f"工程: {process_name}"
                    )
                    return False

                except requests.exceptions.RequestException as e:
                    if attempt < max_retries:
                        logger.warning(f"ARAICHAT送信ネットワークエラー: {e} - リトライします")
                        continue
                    logger.error(f"ARAICHAT送信リクエストエラー（{max_retries}回試行後）: {e}, 工程: {process_name}")
                    return False

                except Exception as e:
                    logger.error(f"ARAICHAT送信予期しないエラー: {e}, 工程: {process_name}", exc_info=True)
                    return False

            return False
        finally:
            with self._dedupe_lock:
                self._in_flight_keys.discard(dedupe_key)
                self._in_flight_cond.notify_all()

    @staticmethod
    def _format_date_value(value: Any, default: str) -> str:
        if pd.isna(value):
            return default
        try:
            if isinstance(value, str):
                dt = pd.to_datetime(value, errors='coerce')
                if pd.notna(dt):
                    return dt.strftime('%Y/%m/%d')
                return str(value)
            return pd.to_datetime(value).strftime('%Y/%m/%d')
        except Exception:
            return str(value)
    
    def send_non_inspection_lots_notification(
        self,
        non_inspection_lots_df: pd.DataFrame,
        process_name: Optional[str] = None
    ) -> bool:
        """
        検査対象外ロット情報をARAICHATに送信
        
        Args:
            non_inspection_lots_df: 検査対象外ロットのDataFrame
            process_name: 工程名
        
        Returns:
            送信成功時はTrue、失敗時はFalse
        """
        if non_inspection_lots_df.empty:
            return False
        
        # メッセージを構築（希望フォーマットに合わせる）
        message_parts = []
        
        # タイトルを追加（タイトル後に1行改行）
        message_parts.append("ー出荷間近の外観未検査ロット情報ー\n\n")
        
        # タイトルの下に案内文を追加
        message_parts.append("各ロットの現在工程の部門においては、内容を必ず確認し、進捗の停滞や滞留が出ないよう、出荷予定日が近いロットは優先対応をお願いします\n")
        
        # 一行改行
        message_parts.append("\n")
        
        # 番号付きリストの記号（①、②、③...）
        number_symbols = ['①', '②', '③', '④', '⑤', '⑥', '⑦', '⑧', '⑨', '⑩',
                         '⑪', '⑫', '⑬', '⑭', '⑮', '⑯', '⑰', '⑱', '⑲', '⑳']
        
        # ロットごとに情報を表示
        for lot_index, row in enumerate(non_inspection_lots_df.itertuples(index=False), start=1):
            # 番号記号を取得（20個を超える場合は数字で表示）
            if lot_index <= len(number_symbols):
                lot_number = number_symbols[lot_index - 1]
            else:
                lot_number = f"【{lot_index}】"
            
            # ロット区切り（最初のロット以外は空行を追加）
            if lot_index > 1:
                message_parts.append("\n")
            
            # ロット番号を追加
            message_parts.append(f"{lot_number}\n")
            
            # 出荷予定日を取得（最優先表示、yyyy/mm/dd形式）
            shipping_date = getattr(row, '出荷予定日', None)
            shipping_date_str = self._format_date_value(shipping_date, default="未設定")
            
            # 品番
            product_number = getattr(row, '品番', '')
            product_number_str = str(product_number) if pd.notna(product_number) else "不明"
            
            # 生産ロットID
            lot_id = getattr(row, '生産ロットID', '')
            lot_id_str = str(lot_id) if pd.notna(lot_id) and str(lot_id).strip() else "未設定"
            
            # 指示日（yyyy/mm/dd形式）
            instruction_date = getattr(row, '指示日', '')
            instruction_date_str = self._format_date_value(instruction_date, default="未設定")
            
            # 現在工程
            current_process = getattr(row, '現在工程名', '')
            current_process_str = str(current_process) if pd.notna(current_process) else "不明"
            
            # 希望フォーマットでメッセージを構築
            message_parts.append(f"出荷予定日：{shipping_date_str}\n")
            message_parts.append(f"品番：{product_number_str}\n")
            message_parts.append(f"生産ロットID：{lot_id_str}\n")
            message_parts.append(f"指示日：{instruction_date_str}\n")
            message_parts.append(f"現在工程は【{current_process_str}】です\n")
        
        message = "".join(message_parts)
        
        return self.send_message(
            message=message,
            process_name=process_name,
            title=None  # タイトルはメッセージ内に含めるためNoneに設定
        )




