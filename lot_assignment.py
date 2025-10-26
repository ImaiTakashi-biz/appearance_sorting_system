"""
ロット割り当て - 出荷検査データ抽出システム
ロット割り当て機能を管理
"""

import pandas as pd
from loguru import logger


class LotAssignment:
    """ロット割り当て機能を管理するクラス"""
    
    def __init__(self):
        """初期化"""
        pass
    
    def assign_lots_to_shortage(self, shortage_df, lots_df):
        """不足数に対してロットを割り当て"""
        try:
            assignment_results = []
            
            # 不足数がマイナスの品番を取得
            shortage_products = shortage_df['品番'].unique()
            
            for product_number in shortage_products:
                # 品番ごとの不足データを取得
                product_shortage = shortage_df[shortage_df['品番'] == product_number]
                product_lots = lots_df[lots_df['品番'] == product_number].copy()
                
                if product_lots.empty:
                    continue
                
                # 指示日順でソート
                product_lots = product_lots.sort_values('指示日')
                
                # 品番ごとの不足数を取得（マイナス値のまま）
                initial_shortage = product_shortage['不足数'].iloc[0]
                current_shortage = initial_shortage
                
                # ロットを順番に割り当て
                for _, lot in product_lots.iterrows():
                    if current_shortage >= 0:  # 不足数が0以上になったら終了
                        break
                    
                    lot_quantity = int(lot['数量']) if pd.notna(lot['数量']) else 0
                    
                    # 割り当て結果を記録（画像で要求されているプロパティを含む）
                    assignment_result = {
                        '出荷予定日': product_shortage['出荷予定日'].iloc[0],
                        '品番': product_number,
                        '品名': product_shortage['品名'].iloc[0],
                        '客先': product_shortage['客先'].iloc[0],
                        '出荷数': int(product_shortage['出荷数'].iloc[0]),
                        '在庫数': int(product_shortage['在庫数'].iloc[0]),
                        '在梱包数': int(product_shortage['梱包・完了'].iloc[0]),
                        '不足数': current_shortage,  # 現在の不足数（マイナス値）
                        '生産ロットID': lot.get('生産ロットID', '') if pd.notna(lot.get('生産ロットID', '')) else '',
                        'ロット数量': lot_quantity,  # ロット全体の数量を表示
                        '指示日': lot['指示日'],
                        '号機': lot['号機'] if pd.notna(lot['号機']) else '',
                        '現在工程名': lot.get('現在工程名', '') if pd.notna(lot.get('現在工程名', '')) else '',
                        '現在工程二次処理': lot.get('現在工程二次処理', '') if pd.notna(lot.get('現在工程二次処理', '')) else ''
                    }
                    assignment_results.append(assignment_result)
                    
                    # 次のロットの不足数を計算（ロット数量を加算）
                    current_shortage += lot_quantity
            
            if assignment_results:
                result_df = pd.DataFrame(assignment_results)
                logger.info(f"ロット割り当て完了: {len(result_df)}件")
                return result_df
            else:
                logger.info("ロット割り当て結果がありません")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"ロット割り当て中にエラーが発生しました: {str(e)}")
            return pd.DataFrame()
    
    def get_shortage_products(self, main_df):
        """不足数がマイナスの品番を取得"""
        try:
            shortage_df = main_df[main_df['不足数'] < 0].copy()
            
            if shortage_df.empty:
                logger.info("不足数がマイナスのデータがありません")
                return pd.DataFrame()
            
            logger.info(f"不足数がマイナスのデータ: {len(shortage_df)}件")
            return shortage_df
            
        except Exception as e:
            logger.error(f"不足品番取得エラー: {str(e)}")
            return pd.DataFrame()
    
    def sort_assignment_results(self, assignment_df):
        """割り当て結果をソート"""
        try:
            if assignment_df.empty:
                return assignment_df
            
            # 出荷予定日昇順、同一品番は指示日古い順でソート
            sorted_df = assignment_df.sort_values(['出荷予定日', '品番', '指示日']).reset_index(drop=True)
            
            logger.info("割り当て結果をソートしました")
            return sorted_df
            
        except Exception as e:
            logger.error(f"ソート処理エラー: {str(e)}")
            return assignment_df
