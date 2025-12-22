"""
ARAICHAT通知サービス
工程ごとのARAICHATルームにメッセージを送信する機能を提供
"""

import json
import time
from typing import Optional, Dict, List, Any
from pathlib import Path
import requests
from loguru import logger
import pandas as pd


class ChatNotificationService:
    """ARAICHAT通知サービス"""
    
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
        
        if room_config_path:
            self._load_room_config()
    
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
        
        # URLの末尾スラッシュを調整
        base_url = self.base_url.rstrip("/")
        url = f"{base_url}/api/integrations/send/{room_id}"
        
        headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        
        # メッセージを構築
        full_message = f"{title}\n{message}" if title else message
        data = {"text": full_message}
        
        # リトライ設定
        max_retries = 3
        backoff_seconds = 2  # 初期待機時間（指数バックオフ: 2秒、4秒、8秒）
        timeout_connect = 5   # 接続タイムアウト（秒）
        timeout_read = 30     # 読み取りタイムアウト（秒、テキスト送信なので180秒は不要）
        
        for attempt in range(1, max_retries + 1):
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
                return True
                
            except requests.exceptions.Timeout as e:
                elapsed_time = time.time() - start_time if 'start_time' in locals() else 0
                if attempt < max_retries:
                    logger.warning(f"ARAICHAT送信タイムアウト（{elapsed_time:.2f}秒）: {e} - リトライします")
                    continue
                else:
                    logger.error(f"ARAICHAT送信タイムアウトエラー（{max_retries}回試行後）: 工程={process_name}")
                    return False
                    
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response else None
                response_text = e.response.text if e.response else ""
                
                # 一時的なサーバーエラー（5xx）の場合はリトライ
                if status_code and 500 <= status_code < 600 and attempt < max_retries:
                    logger.warning(f"ARAICHAT送信HTTP {status_code} エラー: {e} - リトライします")
                    continue
                else:
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
                else:
                    logger.error(f"ARAICHAT送信リクエストエラー（{max_retries}回試行後）: {e}, 工程: {process_name}")
                    return False
                    
            except Exception as e:
                logger.error(f"ARAICHAT送信予期しないエラー: {e}, 工程: {process_name}", exc_info=True)
                return False
        
        return False

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
        
        # タイトルを追加（ー出荷間近の外観未検査ロット情報ー）
        message_parts.append("ー出荷間近の外観未検査ロット情報ー\n")
        
        # タイトルの下に案内文を追加
        message_parts.append("現在工程の部門において、進捗状況を確認し、必要に応じて後工程へ至急回す等の対応指示をご検討ください\n")
        
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




