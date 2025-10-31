"""
検査員割当てロジック
検査員の割当て、スキルマッチング、新製品チーム対応などの機能を提供
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


class InspectorAssignmentManager:
    """検査員割当て管理クラス"""
    
    def __init__(self, log_callback=None):
        """
        初期化
        
        Args:
            log_callback: ログ出力用のコールバック関数
        """
        self.log_callback = log_callback
        # 検査員の割り当て履歴を追跡（公平な割り当てのため）
        self.inspector_assignment_count = {}
        self.inspector_last_assignment = {}
        # 検査員の勤務時間を追跡（勤務時間超過を防ぐため）
        self.inspector_work_hours = {}
        self.inspector_daily_assignments = {}
        # 品番ごとの累計作業時間を検査員別に追跡（同一品番の4時間上限判定に使用）
        # 形式: { inspector_code: { product_number: hours } }
        self.inspector_product_hours = {}
    
    def log_message(self, message):
        """ログメッセージを出力"""
        if self.log_callback:
            self.log_callback(message)
        logger.info(message)
    
    def create_inspector_assignment_table(self, assignment_df, product_master_df):
        """検査員割振りテーブルを作成"""
        try:
            if assignment_df.empty:
                self.log_message("ロット割り当て結果がありません")
                return None
            
            if product_master_df is None or product_master_df.empty:
                self.log_message("製品マスタが読み込まれていません")
                return None
            
            inspector_results = []
            
            for _, row in assignment_df.iterrows():
                product_number = row['品番']
                current_process_number = row.get('現在工程番号', '')
                lot_quantity = row.get('ロット数量', 0)
                
                # ロット数量が0の場合はスキップ（検査員を割り当てない）
                if lot_quantity == 0 or pd.isna(lot_quantity):
                    self.log_message(f"ロット数量が0のため、品番 {product_number} の検査員割り当てをスキップします")
                    continue
                
                # 製品マスタから該当する品番のデータを取得
                product_master_rows = product_master_df[product_master_df['品番'] == product_number]
                
                if product_master_rows.empty:
                    # 品番が一致しない場合はスキップ
                    continue
                
                inspection_time_per_unit = None
                
                # 工程番号が一致する行を探す
                if current_process_number and current_process_number != '':
                    matching_rows = product_master_rows[product_master_rows['工程番号'] == current_process_number]
                    if not matching_rows.empty:
                        inspection_time_per_unit = matching_rows.iloc[0]['検査時間']
                
                # 工程番号が一致しない場合は、品番一致した行の検査時間を取得
                if inspection_time_per_unit is None or pd.isna(inspection_time_per_unit):
                    inspection_time_per_unit = product_master_rows.iloc[0]['検査時間']
                
                if pd.isna(inspection_time_per_unit):
                    continue
                
                # 検査時間を計算（秒 × ロット数量）
                total_inspection_time_seconds = inspection_time_per_unit * lot_quantity
                
                # 時間表示に変換（○.○H）
                total_inspection_time_hours = total_inspection_time_seconds / 3600
                
                # 秒/個はそのまま使用（既に秒単位）
                seconds_per_unit = inspection_time_per_unit
                
                inspector_result = {
                    '出荷予定日': row['出荷予定日'],
                    '品番': product_number,
                    '品名': row['品名'],
                    '客先': row['客先'],
                    '生産ロットID': row.get('生産ロットID', ''),
                    'ロット数量': lot_quantity,
                    '指示日': row.get('指示日', ''),
                    '号機': row.get('号機', ''),
                    '現在工程名': row.get('現在工程名', ''),
                    '現在工程番号': current_process_number,
                    '秒/個': round(seconds_per_unit, 1),
                    '検査時間': round(total_inspection_time_hours, 1)
                }
                
                inspector_results.append(inspector_result)
            
            if not inspector_results:
                self.log_message("検査員割振りデータが生成されませんでした")
                return None
            
            inspector_df = pd.DataFrame(inspector_results)
            self.log_message(f"検査員割振りテーブルを作成しました: {len(inspector_df)}件")
            
            return inspector_df
            
        except Exception as e:
            error_msg = f"検査員割振りテーブルの作成に失敗しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            return None
    
    def assign_inspectors(self, inspector_df, inspector_master_df, skill_master_df, show_skill_values=False):
        """検査員を割り当てる"""
        try:
            if inspector_df is None or inspector_df.empty:
                return inspector_df
            
            if inspector_master_df is None or inspector_master_df.empty:
                self.log_message("検査員マスタが読み込まれていません")
                return inspector_df
            
            if skill_master_df is None or skill_master_df.empty:
                self.log_message("スキルマスタが読み込まれていません")
                return inspector_df
            
            # デバッグ: 検査員マスタの全検査員を表示
            self.log_message("=== 検査員マスタの全検査員 ===")
            for _, inspector_row in inspector_master_df.iterrows():
                self.log_message(f"検査員: {inspector_row['#氏名']} (コード: {inspector_row['#ID']}, 新製品チーム: {inspector_row.iloc[7] if len(inspector_row) > 7 else 'N/A'})")
            self.log_message("================================")
            
            # デバッグ: スキルマスタの検査員コードを表示
            self.log_message("=== スキルマスタの検査員コード ===")
            skill_codes = skill_master_df.columns[2:].tolist()  # 品番、工程を除く検査員コード列
            self.log_message(f"スキルマスタの検査員コード: {skill_codes}")
            self.log_message("=================================")
            
            # 結果用のDataFrameを作成
            result_df = inspector_df.copy()
            
            # 新しい列を追加
            result_df['検査員人数'] = 0
            result_df['分割検査時間'] = 0.0
            result_df['検査員1'] = ''
            result_df['検査員2'] = ''
            result_df['検査員3'] = ''
            result_df['検査員4'] = ''
            result_df['検査員5'] = ''
            result_df['チーム情報'] = ''
            
            # 出荷予定日でソート（古い順）- 最優先ルール
            # 日付形式を統一してからソート
            result_df['出荷予定日'] = pd.to_datetime(result_df['出荷予定日'], errors='coerce')
            
            # 新規品かどうかを判定する列を追加（ソート前に）
            def is_new_product_row(row):
                product_number = row['品番']
                skill_rows = skill_master_df[skill_master_df.iloc[:, 0] == product_number]
                return skill_rows.empty
            
            result_df['_is_new_product'] = result_df.apply(is_new_product_row, axis=1)
            
            # 出荷予定日順にソートし、同じ出荷予定日の場合は新規品を優先
            result_df = result_df.sort_values(
                ['出荷予定日', '_is_new_product'], 
                ascending=[True, False],  # 出荷予定日は昇順、新規品フラグは降順（Trueを先に）
                na_position='last'
            ).reset_index(drop=True)
            
            # ソート用の列を削除
            result_df = result_df.drop(columns=['_is_new_product'])
            
            self.log_message("出荷予定日の古い順でソートしました（最優先ルール）。同じ出荷予定日の場合は新規品を優先します")
            
            # ソート結果をログで確認
            self.log_message("=== 出荷予定日順での処理順序 ===")
            for idx, row in result_df.iterrows():
                product_number = row['品番']
                skill_rows = skill_master_df[skill_master_df.iloc[:, 0] == product_number]
                is_new = "新規品" if skill_rows.empty else "既存品"
                self.log_message(f"順序{idx+1}: 品番={product_number}, 出荷予定日={row['出荷予定日']}, {is_new}")
            self.log_message("================================")
            
            # 各ロットに対して検査員を割り当て
            for index, row in result_df.iterrows():
                inspection_time = row['検査時間']
                product_number = row['品番']
                process_number = row.get('現在工程番号', '')
                lot_quantity = row.get('ロット数量', 0)
                
                # ロット数量が0の場合は検査員を割り当てない
                if lot_quantity == 0 or pd.isna(lot_quantity) or inspection_time == 0 or pd.isna(inspection_time):
                    self.log_message(f"ロット数量が0または検査時間が0のため、品番 {product_number} の検査員割り当てをスキップします")
                    result_df.at[index, '検査員人数'] = 0
                    result_df.at[index, '分割検査時間'] = 0.0
                    for i in range(1, 6):
                        result_df.at[index, f'検査員{i}'] = ''
                    result_df.at[index, 'チーム情報'] = '数量0のため未割当'
                    continue
                
                # 必要な検査員人数を計算（3時間を超える場合は複数人）
                if inspection_time <= 3.0:
                    required_inspectors = 1
                else:
                    required_inspectors = max(2, int(inspection_time / 3.0) + 1)
                
                # 分割検査時間を計算
                divided_time = inspection_time / required_inspectors
                
                # デバッグログ出力
                self.log_message(f"品番 {product_number}: 検査時間 {inspection_time:.1f}h → 必要人数 {required_inspectors}人, 分割時間 {divided_time:.1f}h")
                
                # スキルマスタから該当する品番と工程番号のスキル情報を取得
                available_inspectors = self.get_available_inspectors(
                    product_number, process_number, skill_master_df, inspector_master_df
                )
                
                # 新規品かどうかを判定（スキルマスタに登録がない場合）
                is_new_product = False
                
                # get_available_inspectorsは既に新製品チームを返す場合があるが、明示的に確認
                skill_rows = skill_master_df[skill_master_df.iloc[:, 0] == product_number]
                if skill_rows.empty:
                    is_new_product = True
                    self.log_message(f"品番 {product_number} は新規品です（スキルマスタ未登録）")
                    
                    # available_inspectorsが空の場合は、新製品チームを取得
                    if not available_inspectors:
                        self.log_message(f"新製品チームのメンバーを取得します")
                        available_inspectors = self.get_new_product_team_inspectors(inspector_master_df)
                        if not available_inspectors:
                            self.log_message(f"新製品チームのメンバーも見つからないため、スキップします")
                            continue
                        self.log_message(f"新製品チームメンバー: {len(available_inspectors)}人取得しました")
                    else:
                        self.log_message(f"新規品 {product_number}: get_available_inspectorsから {len(available_inspectors)}人の検査員が返されました（新製品チームの可能性あり）")
                elif not available_inspectors:
                    self.log_message(f"品番 {product_number} の検査員が見つかりません（スキルマスタには登録がありますが、条件に合う検査員がいません）")
                    continue
                
                # 検査員を割り当て（新規品の場合はフラグを渡す）
                assigned_inspectors = self.select_inspectors(
                    available_inspectors, required_inspectors, divided_time, inspector_master_df, product_number, is_new_product=is_new_product
                )
                
                # デバッグログ出力
                self.log_message(f"品番 {product_number}: 要求人数 {required_inspectors}人 → 実際に選択された人数 {len(assigned_inspectors)}人")
                
                # 検査員が選択されなかった場合（ルール違反を避けるため未割当）
                if len(assigned_inspectors) == 0:
                    self.log_message(f"警告: 品番 {product_number} はルール違反を避けるため未割当とします")
                    result_df.at[index, '検査員人数'] = 0
                    result_df.at[index, '分割検査時間'] = 0.0
                    for i in range(1, 6):
                        result_df.at[index, f'検査員{i}'] = ''
                    result_df.at[index, 'チーム情報'] = '未割当'
                    continue
                elif len(assigned_inspectors) != required_inspectors:
                    self.log_message(f"警告: 品番 {product_number} で人数が不足しています (要求: {required_inspectors}人, 実際: {len(assigned_inspectors)}人)")
                
                # 結果を設定
                result_df.at[index, '検査員人数'] = len(assigned_inspectors)
                result_df.at[index, '分割検査時間'] = round(divided_time, 1)
                
                # 現在の日付を取得（勤務時間の履歴追跡用）
                current_time = pd.Timestamp.now()
                current_date = current_time.date()
                
                # 検査員名を設定
                team_members = []
                for i, inspector in enumerate(assigned_inspectors):
                    if i < 5:  # 最大5人まで
                        if show_skill_values:
                            # 新規品チームの場合はスキル値を表示せず(新)のみ
                            if inspector.get('is_new_team', False):
                                inspector_name = f"{inspector['氏名']}(新)"
                            else:
                                inspector_name = f"{inspector['氏名']}({inspector['スキル']})"
                        else:
                            # スキル非表示でも新規品チームの場合は(新)を表示
                            if inspector.get('is_new_team', False):
                                inspector_name = f"{inspector['氏名']}(新)"
                            else:
                                inspector_name = inspector['氏名']
                        
                        result_df.at[index, f'検査員{i+1}'] = inspector_name
                        team_members.append(inspector['氏名'])
                        
                        # 履歴を更新（勤務時間と品番別時間のトラッキング）
                        code = inspector['コード']
                        
                        # 日次勤務時間の履歴を更新
                        if code not in self.inspector_daily_assignments:
                            self.inspector_daily_assignments[code] = {}
                        if current_date not in self.inspector_daily_assignments[code]:
                            self.inspector_daily_assignments[code][current_date] = 0.0
                        self.inspector_daily_assignments[code][current_date] += divided_time
                        
                        # 総勤務時間の履歴を更新
                        if code not in self.inspector_work_hours:
                            self.inspector_work_hours[code] = 0.0
                        self.inspector_work_hours[code] += divided_time
                        
                        # 同一品番の累計時間を更新（4時間上限のためのトラッキング）
                        if code not in self.inspector_product_hours:
                            self.inspector_product_hours[code] = {}
                        self.inspector_product_hours[code][product_number] = (
                            self.inspector_product_hours[code].get(product_number, 0.0) + divided_time
                        )
                
                # チーム情報を設定
                if len(assigned_inspectors) > 1:
                    team_info = f"チーム: {', '.join(team_members)}"
                else:
                    team_info = f"個人: {team_members[0] if team_members else ''}"
                
                result_df.at[index, 'チーム情報'] = team_info
            
            self.log_message(f"第1次割り当てが完了しました: {len(result_df)}件")
            
            # 割り当て統計を表示（第1次）
            self.log_message("=== 第1次割り当て統計 ===")
            self.print_assignment_statistics(inspector_master_df)
            
            # 全体最適化を実行（勤務時間超過の調整と偏りの是正）
            self.log_message("=== 全体最適化を開始 ===")
            result_df = self.optimize_assignments(result_df, inspector_master_df, skill_master_df, show_skill_values)
            self.log_message("=== 全体最適化が完了 ===")
            
            # 最終割り当て統計を表示
            self.log_message("=== 最終割り当て統計 ===")
            self.print_assignment_statistics(inspector_master_df)
            
            return result_df
            
        except Exception as e:
            error_msg = f"検査員割り当て中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg)
            return inspector_df
    
    def get_available_inspectors(self, product_number, process_number, skill_master_df, inspector_master_df):
        """利用可能な検査員を取得"""
        try:
            available_inspectors = []
            
            # デバッグ情報を出力
            self.log_message(f"品番 '{product_number}' の検査員を検索中...")
            self.log_message(f"工程番号: '{process_number}'")
            
            # スキルマスタから該当する品番の行を取得（完全一致のみ）
            skill_rows = skill_master_df[skill_master_df.iloc[:, 0] == product_number]
            
            if skill_rows.empty:
                self.log_message(f"品番 '{product_number}' がスキルマスタに見つかりません")
                # フォールバック: 新製品チームのメンバーを取得
                return self.get_new_product_team_inspectors(inspector_master_df)
            else:
                self.log_message(f"品番 '{product_number}' でマッチしました: {len(skill_rows)}件")
                
                # デバッグ: スキルマスタの詳細情報を表示
                self.log_message(f"スキルマスタの列: {skill_master_df.columns.tolist()[:10]}...")
                for idx, row in skill_rows.iterrows():
                    self.log_message(f"スキル行 {idx}: 品番={row.iloc[0]}, 工程={row.iloc[1]}")
                    # 検査員コードとスキル値を表示（全列を対象）
                    for i in range(2, len(row)):
                        col_name = skill_master_df.columns[i]
                        skill_value = row.iloc[i]
                        if pd.notna(skill_value) and str(skill_value).strip() != '':
                            self.log_message(f"  {col_name}: {skill_value}")
            
            # 工程番号による絞り込み処理
            filtered_skill_rows = []
            # 追加仕様: 現在工程番号が空欄の場合は工程による絞り込みを行わず、品番一致行をすべて対象
            if process_number is None or str(process_number).strip() == '':
                self.log_message("現在工程番号が空欄のため、工程フィルタをスキップして品番一致行を全件採用")
                for _, skill_row in skill_rows.iterrows():
                    filtered_skill_rows.append(skill_row)
            else:
                for _, skill_row in skill_rows.iterrows():
                    skill_process_number = skill_row.iloc[1]  # 工程番号列
                    
                    # スキルマスタの工程番号が空欄の場合は、工程番号が一致しなくてもOK
                    if pd.isna(skill_process_number) or skill_process_number == '':
                        filtered_skill_rows.append(skill_row)
                        self.log_message(f"工程番号が空欄のため、工程番号条件を無視して追加")
                    elif str(skill_process_number) == str(process_number):
                        filtered_skill_rows.append(skill_row)
                        self.log_message(f"工程番号 '{process_number}' でマッチしました")
                    else:
                        self.log_message(f"工程番号 '{skill_process_number}' は条件に一致しません")
            
            if not filtered_skill_rows:
                self.log_message(f"工程番号 '{process_number}' に一致するスキル情報が見つかりません")
                # フォールバック: 新製品チームのメンバーを取得
                return self.get_new_product_team_inspectors(inspector_master_df)
            
            # スキル情報から検査員を取得
            self.log_message(f"スキルマスタの列数: {len(skill_master_df.columns)}")
            # 検査員列は V002 以降をすべて対象（従来の Z040 までの固定上限を撤廃）
            self.log_message(f"スキルマスタの列名: {list(skill_master_df.columns[2:])}")
            
            # 処理対象の検査員コードを事前に確認
            valid_inspector_codes = []
            for i in range(2, len(skill_master_df.columns)):
                col_name = skill_master_df.columns[i]
                if pd.notna(col_name) and str(col_name).strip() != '':
                    valid_inspector_codes.append(col_name)
            self.log_message(f"処理対象検査員コード数: {len(valid_inspector_codes)}")
            
            for skill_row in filtered_skill_rows:
                # スキルマスタの列構造: 品番, 工程, V002, V004, V005, ...（右端は可変）
                # 列2以降の全列を検査員コード列として扱う
                for i in range(2, len(skill_master_df.columns)):
                    col_name = skill_master_df.columns[i]
                    inspector_code = col_name
                    skill_value = skill_row.iloc[i]  # ilocを使用してインデックスでアクセス
                    
                    # 列名が空またはNaNの場合はスキップ
                    if pd.isna(col_name) or str(col_name).strip() == '':
                        continue
                    
                    # スキル値が1, 2, 3のいずれかで、かつ空でない場合
                    if pd.notna(skill_value) and str(skill_value).strip() != '' and str(skill_value).strip() in ['1', '2', '3']:
                        self.log_message(f"スキル値 {skill_value} の検査員コード {inspector_code} を処理中")
                        # 検査員マスタから該当する検査員の情報を取得
                        # 検査員コード（V002, V004等）で検索
                        inspector_info = inspector_master_df[inspector_master_df['#ID'] == inspector_code]
                        if not inspector_info.empty:
                            inspector_data = inspector_info.iloc[0]
                            
                            # 勤務時間を事前チェック（0時間の検査員を除外）
                            start_time = inspector_data['開始時刻']
                            end_time = inspector_data['終了時刻']
                            
                            if pd.notna(start_time) and pd.notna(end_time):
                                try:
                                    # 時刻文字列を時間に変換
                                    if isinstance(start_time, str):
                                        start_hour = float(start_time.split(':')[0]) + float(start_time.split(':')[1]) / 60.0
                                    else:
                                        start_hour = start_time.hour + start_time.minute / 60.0
                                        
                                    if isinstance(end_time, str):
                                        end_hour = float(end_time.split(':')[0]) + float(end_time.split(':')[1]) / 60.0
                                    else:
                                        end_hour = end_time.hour + end_time.minute / 60.0
                                    
                                    # 基本勤務時間を計算
                                    max_daily_hours = end_hour - start_hour
                                    
                                    # 休憩時間（12:15～13:00）を含む場合は1時間を差し引く
                                    if start_hour <= 12.25 and end_hour >= 13.0:
                                        max_daily_hours -= 1.0
                                    
                                    # 勤務時間が0以下の場合は候補から除外
                                    if max_daily_hours <= 0:
                                        self.log_message(f"警告: 検査員 '{inspector_data['#氏名']}' の勤務時間が0時間以下です (開始: {start_time}, 終了: {end_time}) - 候補から除外")
                                        continue
                                        
                                except Exception as e:
                                    self.log_message(f"警告: 検査員 '{inspector_data['#氏名']}' の勤務時間計算に失敗: {e} - 候補から除外")
                                    continue
                            else:
                                self.log_message(f"警告: 検査員 '{inspector_data['#氏名']}' の時刻情報が不正です - 候補から除外")
                                continue
                            
                            available_inspectors.append({
                                '氏名': inspector_data['#氏名'],
                                'スキル': int(str(skill_value).strip()),
                                '就業時間': inspector_data['開始時刻'],
                                'コード': inspector_code,
                                'is_new_team': False  # 通常の検査員
                            })
                            self.log_message(f"検査員 '{inspector_data['#氏名']}' (コード: {inspector_code}, スキル: {skill_value}) を追加")
                        else:
                            self.log_message(f"警告: 検査員コード '{inspector_code}' が検査員マスタに見つかりません")
                            # 検査員マスタの全コードを表示
                            self.log_message(f"検査員マスタの利用可能なコード: {list(inspector_master_df['#ID'].values)}")
            
            self.log_message(f"利用可能な検査員: {len(available_inspectors)}人")
            
            # 利用可能な検査員の詳細をログ出力
            if available_inspectors:
                self.log_message("=== 利用可能な検査員一覧 ===")
                for insp in available_inspectors:
                    self.log_message(f"  {insp['氏名']} (コード: {insp['コード']}, スキル: {insp['スキル']})")
                self.log_message("=============================")
            else:
                self.log_message("警告: 利用可能な検査員が0人です")
            
            return available_inspectors
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            self.log_message(f"利用可能な検査員取得中にエラーが発生しました: {str(e)}")
            self.log_message(f"エラー詳細: {error_detail}")
            # エラーが発生した場合は新製品チームにフォールバック
            self.log_message("エラーのため新製品チームにフォールバックします")
            return self.get_new_product_team_inspectors(inspector_master_df)
    
    def get_new_product_team_inspectors(self, inspector_master_df):
        """新製品チームの検査員を取得"""
        try:
            new_product_team_inspectors = []
            
            # デバッグ: 新製品チーム列の内容を確認
            self.log_message("=== 新製品チーム列の確認 ===")
            if len(inspector_master_df.columns) > 7:
                team_column = inspector_master_df.iloc[:, 7]
                self.log_message(f"新製品チーム列の値: {team_column.unique()}")
                for idx, value in team_column.items():
                    if pd.notna(value) and str(value).strip() != '':
                        self.log_message(f"行 {idx}: {value}")
            else:
                self.log_message("新製品チーム列が存在しません")
            self.log_message("=============================")
            
            # 検査員マスタのH列（新製品チーム）が"★"のメンバーを取得
            new_product_team_rows = inspector_master_df[inspector_master_df.iloc[:, 7] == '★']
            
            if new_product_team_rows.empty:
                self.log_message("新製品チームのメンバーが見つかりません")
                return []
            
            for _, inspector_row in new_product_team_rows.iterrows():
                # 勤務時間を事前チェック（0時間の検査員を除外）
                start_time = inspector_row['開始時刻']
                end_time = inspector_row['終了時刻']
                
                if pd.notna(start_time) and pd.notna(end_time):
                    try:
                        # 時刻文字列を時間に変換
                        if isinstance(start_time, str):
                            start_hour = float(start_time.split(':')[0]) + float(start_time.split(':')[1]) / 60.0
                        else:
                            start_hour = start_time.hour + start_time.minute / 60.0
                            
                        if isinstance(end_time, str):
                            end_hour = float(end_time.split(':')[0]) + float(end_time.split(':')[1]) / 60.0
                        else:
                            end_hour = end_time.hour + end_time.minute / 60.0
                        
                        # 基本勤務時間を計算
                        max_daily_hours = end_hour - start_hour
                        
                        # 休憩時間（12:15～13:00）を含む場合は1時間を差し引く
                        if start_hour <= 12.25 and end_hour >= 13.0:
                            max_daily_hours -= 1.0
                        
                        # 勤務時間が0以下の場合は候補から除外
                        if max_daily_hours <= 0:
                            self.log_message(f"警告: 新製品チームメンバー '{inspector_row['#氏名']}' の勤務時間が0時間以下です (開始: {start_time}, 終了: {end_time}) - 候補から除外")
                            continue
                            
                    except Exception as e:
                        self.log_message(f"警告: 新製品チームメンバー '{inspector_row['#氏名']}' の勤務時間計算に失敗: {e} - 候補から除外")
                        continue
                else:
                    self.log_message(f"警告: 新製品チームメンバー '{inspector_row['#氏名']}' の時刻情報が不正です - 候補から除外")
                    continue
                
                new_product_team_inspectors.append({
                    '氏名': inspector_row['#氏名'],
                    'スキル': 2,  # 新製品チームは中スキルとして扱う
                    '就業時間': inspector_row['開始時刻'],
                    'コード': inspector_row['#ID'],
                    'is_new_team': True  # 新規品チームフラグ
                })
                self.log_message(f"新製品チームメンバー '{inspector_row['#氏名']}' (コード: {inspector_row['#ID']}) を追加")
            
            self.log_message(f"新製品チームメンバー: {len(new_product_team_inspectors)}人")
            return new_product_team_inspectors
            
        except Exception as e:
            self.log_message(f"新製品チームメンバー取得中にエラーが発生しました: {str(e)}")
            return []
    
    def select_inspectors(self, available_inspectors, required_count, divided_time, inspector_master_df, product_number, is_new_product=False):
        """検査員を選択する（スキル組み合わせ考慮・勤務時間考慮・公平な割り当て方式）"""
        try:
            if not available_inspectors:
                if is_new_product:
                    self.log_message(f"新規品 {product_number}: 新製品チームメンバーが利用可能な検査員がいません")
                return []
            
            # 各検査員の割り当て回数と最終割り当て時刻を更新
            current_time = pd.Timestamp.now()
            current_date = current_time.date()
            
            # 利用可能な検査員に割り当て履歴を追加
            for inspector in available_inspectors:
                inspector_code = inspector['コード']
                if inspector_code not in self.inspector_assignment_count:
                    self.inspector_assignment_count[inspector_code] = 0
                if inspector_code not in self.inspector_last_assignment:
                    self.inspector_last_assignment[inspector_code] = pd.Timestamp.min
                if inspector_code not in self.inspector_work_hours:
                    self.inspector_work_hours[inspector_code] = 0.0
                if inspector_code not in self.inspector_daily_assignments:
                    self.inspector_daily_assignments[inspector_code] = {}
                if current_date not in self.inspector_daily_assignments[inspector_code]:
                    self.inspector_daily_assignments[inspector_code][current_date] = 0.0
            
            # 勤務時間を考慮して利用可能な検査員をフィルタリング
            if is_new_product:
                self.log_message(f"新規品 {product_number}: 新製品チームメンバー {len(available_inspectors)}人をフィルタリング中")
            available_inspectors = self.filter_available_inspectors(available_inspectors, divided_time, inspector_master_df)
            
            if is_new_product:
                self.log_message(f"新規品 {product_number}: 勤務時間チェック後 {len(available_inspectors)}人が利用可能")

            # 追加ルール1: 同一品番での累計4時間を超える検査員を除外
            filtered_by_product = []
            for insp in available_inspectors:
                code = insp['コード']
                current = self.inspector_product_hours.get(code, {}).get(product_number, 0.0)
                if current + divided_time > 4.0:
                    self.log_message(f"検査員 '{insp['氏名']}' は品番 {product_number} の累計が {current:.1f}h のため除外 (+{divided_time:.1f}hで4h超過)")
                    continue
                filtered_by_product.append(insp)
            
            # 総勤務時間制約は削除（検査員マスタの勤務時間制約のみを使用）
            # 検査員マスタの勤務時間が個別に設定されているため、統一的な総勤務時間制約は適用しない

            if not filtered_by_product:
                if is_new_product:
                    self.log_message(f"警告: 新規品 {product_number} の4時間上限または勤務時間上限により全員が除外。ルール違反を避けるため、このロットは未割当とします")
                else:
                    self.log_message(f"警告: 品番 {product_number} の4時間上限により全員が除外。ルール違反を避けるため、このロットは未割当とします")
                return []
            
            if is_new_product:
                self.log_message(f"新規品 {product_number}: 4時間上限チェック後 {len(filtered_by_product)}人が利用可能")
            
            # スキル組み合わせロジックを適用
            selected_inspectors = self.select_inspectors_with_skill_combination(
                filtered_by_product, required_count, divided_time, current_time, current_date, inspector_master_df
            )
            
            return selected_inspectors
            
        except Exception as e:
            self.log_message(f"検査員選択中にエラーが発生しました: {str(e)}")
            return []
    
    def select_inspectors_with_skill_combination(self, available_inspectors, required_count, divided_time, current_time, current_date, inspector_master_df):
        """スキル組み合わせを考慮した検査員選択"""
        try:
            # スキルレベル別に検査員を分類
            skill_groups = {
                1: [],
                2: [],
                3: [],
                'new': []  # 新製品チーム
            }
            
            for inspector in available_inspectors:
                if inspector.get('is_new_team', False):
                    skill_groups['new'].append(inspector)
                else:
                    skill = inspector.get('スキル', 1)
                    if skill in skill_groups:
                        skill_groups[skill].append(inspector)
                    else:
                        skill_groups[1].append(inspector)  # デフォルトはスキル1
            
            # 各グループ内で優先度を計算（総勤務時間を最優先）
            for skill_level, inspectors in skill_groups.items():
                if not inspectors:
                    continue
                    
                inspectors_with_priority = []
                for insp in inspectors:
                    code = insp['コード']
                    assignment_count = self.inspector_assignment_count.get(code, 0)
                    total_hours = self.inspector_work_hours.get(code, 0.0)
                    last_assignment = self.inspector_last_assignment.get(code, pd.Timestamp.min)
                    # 総勤務時間を最優先（時間の偏りを防ぐ）
                    priority = (-total_hours, -assignment_count, last_assignment)
                    inspectors_with_priority.append((priority, insp))
                
                # 優先度順にソート
                inspectors_with_priority.sort(key=lambda x: x[0])
                skill_groups[skill_level] = [insp for _, insp in inspectors_with_priority]
            
            # 利用可能な検査員の総数を確認
            total_available = sum(len(inspectors) for inspectors in skill_groups.values())
            if total_available < required_count:
                self.log_message(f"警告: 利用可能な検査員数 {total_available}人 が要求人数 {required_count}人 より少ないため、新製品チームを追加で検索します")
                
                # 新製品チームを追加で検索
                new_team_inspectors = self.get_new_product_team_inspectors(inspector_master_df)
                if new_team_inspectors:
                    # 新製品チームをスキルグループに追加
                    skill_groups['new'] = new_team_inspectors
                    total_available = sum(len(inspectors) for inspectors in skill_groups.values())
                    self.log_message(f"新製品チーム追加後: 利用可能な検査員数 {total_available}人")
                
                # それでも足りない場合は可能な限り選択
                if total_available < required_count:
                    self.log_message(f"最終警告: 利用可能な検査員数 {total_available}人 が要求人数 {required_count}人 より少ないため、可能な限り選択します")
                    required_count = total_available
            
            # スキル組み合わせロジックを適用
            selected_inspectors = []
            
            if required_count == 1:
                # 1人の場合は最も総勤務時間が少ない人を選択（公平性を強化）
                all_inspectors_with_priority = []
                for skill_level, inspectors in skill_groups.items():
                    for insp in inspectors:
                        code = insp['コード']
                        assignment_count = self.inspector_assignment_count.get(code, 0)
                        total_hours = self.inspector_work_hours.get(code, 0.0)
                        last_assignment = self.inspector_last_assignment.get(code, pd.Timestamp.min)
                        
                        # 公平性を強化: 総勤務時間を最優先（時間の偏りを防ぐ）
                        # 総勤務時間が同じ場合は割り当て回数、さらに同じ場合は最終割り当て時刻で判定
                        priority = (-total_hours, -assignment_count, last_assignment)
                        all_inspectors_with_priority.append((priority, insp))
                
                all_inspectors_with_priority.sort(key=lambda x: x[0])
                if all_inspectors_with_priority:
                    selected_inspectors.append(all_inspectors_with_priority[0][1])
            
            elif required_count == 2:
                # 2人の場合の組み合わせロジック
                selected_inspectors = self.select_two_inspectors_with_skill_combination(skill_groups)
            
            elif required_count == 3:
                # 3人の場合の組み合わせロジック
                selected_inspectors = self.select_three_inspectors_with_skill_combination(skill_groups)
            
            else:
                # 4人以上の場合は公平な割り当て（公平性を強化）
                all_inspectors_with_priority = []
                for skill_level, inspectors in skill_groups.items():
                    for insp in inspectors:
                        code = insp['コード']
                        assignment_count = self.inspector_assignment_count.get(code, 0)
                        total_hours = self.inspector_work_hours.get(code, 0.0)
                        last_assignment = self.inspector_last_assignment.get(code, pd.Timestamp.min)
                        
                        # 公平性を強化: 総勤務時間を最優先（時間の偏りを防ぐ）
                        # 総勤務時間が同じ場合は割り当て回数、さらに同じ場合は最終割り当て時刻で判定
                        priority = (-total_hours, -assignment_count, last_assignment)
                        all_inspectors_with_priority.append((priority, insp))
                
                all_inspectors_with_priority.sort(key=lambda x: x[0])
                selected_inspectors = [insp for _, insp in all_inspectors_with_priority[:required_count]]
            
            # 選択された検査員の履歴を更新
            for insp in selected_inspectors:
                code = insp['コード']
                self.inspector_assignment_count[code] += 1
                self.inspector_last_assignment[code] = current_time
                self.inspector_work_hours[code] += divided_time
                self.inspector_daily_assignments[code][current_date] += divided_time
                
                # ログ出力
                count = self.inspector_assignment_count.get(code, 0)
                skill_info = f"スキル: {insp['スキル']}" if not insp.get('is_new_team', False) else "新製品チーム"
                self.log_message(f"検査員 '{insp['氏名']}' ({skill_info}, 割り当て回数: {count}) を選択")
            
            return selected_inspectors
            
        except Exception as e:
            self.log_message(f"スキル組み合わせ選択中にエラーが発生しました: {str(e)}")
            return []
    
    def select_two_inspectors_with_skill_combination(self, skill_groups):
        """2人の検査員をスキル組み合わせ考慮で選択"""
        try:
            selected = []
            
            # 利用可能な検査員の総数を確認
            total_available = sum(len(inspectors) for inspectors in skill_groups.values())
            self.log_message(f"2人選択: 利用可能な検査員総数 {total_available}人")
            for skill_level, inspectors in skill_groups.items():
                self.log_message(f"  スキル{skill_level}: {len(inspectors)}人")
            
            if total_available < 2:
                self.log_message(f"警告: 2人選択要求だが、利用可能な検査員は {total_available}人 のみ")
                # 利用可能な分だけ選択
                for skill_level, inspectors in skill_groups.items():
                    for inspector in inspectors:
                        if len(selected) >= total_available:
                            break
                        selected.append(inspector)
                        self.log_message(f"  選択: {inspector['氏名']} (スキル: {inspector.get('スキル', '新製品')})")
                return selected
            
            # スキル3がいる場合の組み合わせ（公平性を考慮）
            if skill_groups[3]:
                skill3_inspector = skill_groups[3][0]
                selected.append(skill3_inspector)
                self.log_message(f"  スキル3選択: {skill3_inspector['氏名']}")
                
                # 2人目を選択（公平性を考慮）
                if len(selected) < 2:
                    # 全候補から最も総勤務時間が少ない人を選択
                    all_remaining = []
                    for skill_level, inspectors in skill_groups.items():
                        for insp in inspectors:
                            if insp not in selected:
                                code = insp['コード']
                                assignment_count = self.inspector_assignment_count.get(code, 0)
                                total_hours = self.inspector_work_hours.get(code, 0.0)
                                last_assignment = self.inspector_last_assignment.get(code, pd.Timestamp.min)
                                # 総勤務時間を最優先（時間の偏りを防ぐ）
                                priority = (-total_hours, -assignment_count, last_assignment)
                                all_remaining.append((priority, insp))
                    
                    if all_remaining:
                        all_remaining.sort(key=lambda x: x[0])
                        selected.append(all_remaining[0][1])
                        code = all_remaining[0][1]['コード']
                        self.log_message(f"  2人目選択: {all_remaining[0][1]['氏名']} (総勤務時間: {self.inspector_work_hours.get(code, 0.0):.1f}h)")
                    else:
                        self.log_message("  2人目が見つからないため、1人のみ")
            
            # スキル3がいない場合（公平性を考慮）
            else:
                # 全候補から最も総勤務時間が少ない人を2人選択
                all_candidates = []
                for skill_level, inspectors in skill_groups.items():
                    for insp in inspectors:
                        code = insp['コード']
                        assignment_count = self.inspector_assignment_count.get(code, 0)
                        total_hours = self.inspector_work_hours.get(code, 0.0)
                        last_assignment = self.inspector_last_assignment.get(code, pd.Timestamp.min)
                        # 総勤務時間を最優先（時間の偏りを防ぐ）
                        priority = (-total_hours, -assignment_count, last_assignment)
                        all_candidates.append((priority, insp))
                
                if all_candidates:
                    all_candidates.sort(key=lambda x: x[0])
                    # 上位2人を選択
                    for i in range(min(2, len(all_candidates))):
                        selected.append(all_candidates[i][1])
                        code = all_candidates[i][1]['コード']
                        self.log_message(f"  選択{i+1}: {all_candidates[i][1]['氏名']} (総勤務時間: {self.inspector_work_hours.get(code, 0.0):.1f}h)")
            
            return selected
            
        except Exception as e:
            self.log_message(f"2人選択中にエラーが発生しました: {str(e)}")
            return []
    
    def select_three_inspectors_with_skill_combination(self, skill_groups):
        """3人の検査員をスキル組み合わせ考慮で選択"""
        try:
            selected = []
            
            # 利用可能な検査員の総数を確認
            total_available = sum(len(inspectors) for inspectors in skill_groups.values())
            if total_available < 3:
                self.log_message(f"警告: 3人選択要求だが、利用可能な検査員は {total_available}人 のみ")
                # 利用可能な分だけ選択
                for skill_level, inspectors in skill_groups.items():
                    for inspector in inspectors:
                        if len(selected) >= total_available:
                            break
                        selected.append(inspector)
                return selected
            
            # スキル3がいる場合の組み合わせ（公平性を考慮）
            if skill_groups[3]:
                skill3_inspector = skill_groups[3][0]
                selected.append(skill3_inspector)
                self.log_message(f"  スキル3選択: {skill3_inspector['氏名']}")
                
                # 残り2人を公平性を考慮して選択
                remaining_candidates = []
                for skill_level, inspectors in skill_groups.items():
                    for insp in inspectors:
                        if insp not in selected:
                            code = insp['コード']
                            assignment_count = self.inspector_assignment_count.get(code, 0)
                            total_hours = self.inspector_work_hours.get(code, 0.0)
                            last_assignment = self.inspector_last_assignment.get(code, pd.Timestamp.min)
                            # 総勤務時間を最優先（時間の偏りを防ぐ）
                            priority = (-total_hours, -assignment_count, last_assignment)
                            remaining_candidates.append((priority, insp))
                
                if remaining_candidates:
                    remaining_candidates.sort(key=lambda x: x[0])
                    # 上位2人を選択
                    for i in range(min(2, len(remaining_candidates))):
                        selected.append(remaining_candidates[i][1])
                        code = remaining_candidates[i][1]['コード']
                        self.log_message(f"  選択{i+2}: {remaining_candidates[i][1]['氏名']} (総勤務時間: {self.inspector_work_hours.get(code, 0.0):.1f}h)")
            
            # スキル3がいない場合（公平性を考慮）
            else:
                # 全候補から最も総勤務時間が少ない人を3人選択
                all_candidates = []
                for skill_level, inspectors in skill_groups.items():
                    for insp in inspectors:
                        code = insp['コード']
                        assignment_count = self.inspector_assignment_count.get(code, 0)
                        total_hours = self.inspector_work_hours.get(code, 0.0)
                        last_assignment = self.inspector_last_assignment.get(code, pd.Timestamp.min)
                        # 総勤務時間を最優先（時間の偏りを防ぐ）
                        priority = (-total_hours, -assignment_count, last_assignment)
                        all_candidates.append((priority, insp))
                
                if all_candidates:
                    all_candidates.sort(key=lambda x: x[0])
                    # 上位3人を選択
                    for i in range(min(3, len(all_candidates))):
                        selected.append(all_candidates[i][1])
                        code = all_candidates[i][1]['コード']
                        self.log_message(f"  選択{i+1}: {all_candidates[i][1]['氏名']} (総勤務時間: {self.inspector_work_hours.get(code, 0.0):.1f}h)")
            
            return selected
            
        except Exception as e:
            self.log_message(f"3人選択中にエラーが発生しました: {str(e)}")
            return []
    
    def filter_available_inspectors(self, available_inspectors, divided_time, inspector_master_df):
        """勤務時間を考慮して利用可能な検査員をフィルタリング（厳密なチェック）"""
        try:
            filtered_inspectors = []
            current_date = pd.Timestamp.now().date()
            
            for inspector in available_inspectors:
                inspector_code = inspector['コード']
                
                # 現在の日付での累積勤務時間を取得
                daily_hours = self.inspector_daily_assignments.get(inspector_code, {}).get(current_date, 0.0)
                
                # 追加する作業時間
                additional_hours = divided_time
                
                # 検査員マスタから該当検査員の勤務時間を取得
                inspector_info = inspector_master_df[inspector_master_df['#ID'] == inspector_code]
                
                if not inspector_info.empty:
                    inspector_data = inspector_info.iloc[0]
                    # 開始時刻と終了時刻から勤務時間を計算
                    start_time = inspector_data['開始時刻']
                    end_time = inspector_data['終了時刻']
                    
                    # 勤務時間を計算（時間単位）
                    if pd.notna(start_time) and pd.notna(end_time):
                        try:
                            # 時刻文字列を時間に変換
                            if isinstance(start_time, str):
                                start_hour = float(start_time.split(':')[0]) + float(start_time.split(':')[1]) / 60.0
                            else:
                                start_hour = start_time.hour + start_time.minute / 60.0
                                
                            if isinstance(end_time, str):
                                end_hour = float(end_time.split(':')[0]) + float(end_time.split(':')[1]) / 60.0
                            else:
                                end_hour = end_time.hour + end_time.minute / 60.0
                            
                            # 基本勤務時間を計算
                            max_daily_hours = end_hour - start_hour
                            
                            # 勤務時間が0以下の場合は警告を出して除外
                            if max_daily_hours <= 0:
                                self.log_message(f"警告: 検査員 '{inspector['氏名']}' の勤務時間が0時間以下です (開始: {start_time}, 終了: {end_time}) - 除外します")
                                continue
                            
                            # 休憩時間（12:15～13:00）を含む場合は1時間を差し引く
                            # 12:15 = 12.25時間、13:00 = 13.0時間
                            if start_hour <= 12.25 and end_hour >= 13.0:
                                max_daily_hours -= 1.0
                                self.log_message(f"検査員 '{inspector['氏名']}' は休憩時間を含むため、勤務時間から1時間を差し引きます (元: {end_hour - start_hour:.1f}h → 調整後: {max_daily_hours:.1f}h)")
                            
                            # 最終的な勤務時間が0以下になった場合は除外
                            if max_daily_hours <= 0:
                                self.log_message(f"警告: 検査員 '{inspector['氏名']}' の調整後勤務時間が0時間以下です - 除外します")
                                continue
                            
                        except Exception as e:
                            # 計算に失敗した場合は8時間をデフォルトとする
                            self.log_message(f"警告: 検査員 '{inspector['氏名']}' の勤務時間計算に失敗: {e} - デフォルト8時間を使用")
                            max_daily_hours = 8.0
                    else:
                        # 時刻情報がない場合は8時間をデフォルトとする
                        self.log_message(f"警告: 検査員 '{inspector['氏名']}' の時刻情報が不正です (開始: {start_time}, 終了: {end_time}) - デフォルト8時間を使用")
                        max_daily_hours = 8.0
                else:
                    # 検査員マスタにない場合は8時間をデフォルトとする
                    self.log_message(f"警告: 検査員 '{inspector['氏名']}' が検査員マスタに見つかりません - デフォルト8時間を使用")
                    max_daily_hours = 8.0
                
                # 勤務時間超過チェック（厳密に - 余裕を持たせてチェック）
                # 小数点以下を考慮して、0.05時間（3分）の余裕を持たせる
                if daily_hours + additional_hours <= max_daily_hours - 0.05:
                    filtered_inspectors.append(inspector)
                    self.log_message(f"検査員 '{inspector['氏名']}' は利用可能 (今日の勤務時間: {daily_hours:.1f}h + {additional_hours:.1f}h = {daily_hours + additional_hours:.1f}h, 最大勤務時間: {max_daily_hours:.1f}h)")
                else:
                    self.log_message(f"検査員 '{inspector['氏名']}' は勤務時間超過のため除外 (今日の勤務時間: {daily_hours:.1f}h + {additional_hours:.1f}h = {daily_hours + additional_hours:.1f}h > {max_daily_hours - 0.05:.1f}h)")
            
            return filtered_inspectors
            
        except Exception as e:
            self.log_message(f"検査員フィルタリング中にエラーが発生しました: {str(e)}")
            return available_inspectors
    
    def get_inspector_max_hours(self, inspector_code, inspector_master_df):
        """検査員の最大勤務時間を取得（検査員マスタから）"""
        try:
            inspector_info = inspector_master_df[inspector_master_df['#ID'] == inspector_code]
            if not inspector_info.empty:
                inspector_data = inspector_info.iloc[0]
                start_time = inspector_data['開始時刻']
                end_time = inspector_data['終了時刻']
                
                if pd.notna(start_time) and pd.notna(end_time):
                    try:
                        # 時刻文字列を時間に変換
                        if isinstance(start_time, str):
                            start_hour = float(start_time.split(':')[0]) + float(start_time.split(':')[1]) / 60.0
                        else:
                            start_hour = start_time.hour + start_time.minute / 60.0
                            
                        if isinstance(end_time, str):
                            end_hour = float(end_time.split(':')[0]) + float(end_time.split(':')[1]) / 60.0
                        else:
                            end_hour = end_time.hour + end_time.minute / 60.0
                        
                        # 基本勤務時間を計算
                        max_daily_hours = end_hour - start_hour
                        
                        # 休憩時間（12:15～13:00）を含む場合は1時間を差し引く
                        if start_hour <= 12.25 and end_hour >= 13.0:
                            max_daily_hours -= 1.0
                        
                        return max_daily_hours
                    except:
                        return 8.0  # デフォルト
                else:
                    return 8.0  # デフォルト
            else:
                return 8.0  # デフォルト
        except:
            return 8.0  # デフォルト

    def print_assignment_statistics(self, inspector_master_df=None):
        """割り当て統計を表示"""
        try:
            if not self.inspector_assignment_count:
                self.log_message("割り当て統計: まだ割り当てがありません")
                return
            
            self.log_message("=== 検査員割り当て統計 ===")
            
            # 割り当て回数でソート
            sorted_assignments = sorted(self.inspector_assignment_count.items(), 
                                      key=lambda x: x[1], reverse=True)
            
            total_assignments = sum(self.inspector_assignment_count.values())
            inspector_count = len(self.inspector_assignment_count)
            average_assignments = total_assignments / inspector_count if inspector_count > 0 else 0
            
            self.log_message(f"総割り当て回数: {total_assignments}回")
            self.log_message(f"検査員数: {inspector_count}人")
            self.log_message(f"平均割り当て回数: {average_assignments:.1f}回")
            self.log_message("")
            
            # 各検査員の割り当て回数と勤務時間を表示
            for inspector_code, count in sorted_assignments:
                work_hours = self.inspector_work_hours.get(inspector_code, 0.0)
                daily_hours = self.inspector_daily_assignments.get(inspector_code, {}).get(pd.Timestamp.now().date(), 0.0)
                
                # 検査員マスタから最大勤務時間を取得
                if inspector_master_df is not None:
                    max_hours = self.get_inspector_max_hours(inspector_code, inspector_master_df)
                    # 個別の最大勤務時間に基づく上限チェック
                    status = ""
                    if work_hours > max_hours:
                        status = f" ⚠️ {max_hours:.1f}h超過"
                    elif work_hours > max_hours * 0.8:
                        status = f" ⚠️ {max_hours:.1f}hの80%超過"
                    
                    self.log_message(f"検査員 {inspector_code}: {count}回 (総勤務時間: {work_hours:.1f}h/{max_hours:.1f}h, 今日: {daily_hours:.1f}h){status}")
                else:
                    # 検査員マスタがない場合は従来の表示
                    status = ""
                    if work_hours > 8.0:
                        status = " ⚠️ 8時間超過"
                    elif work_hours > 6.0:
                        status = " ⚠️ 6時間超過"
                    
                    self.log_message(f"検査員 {inspector_code}: {count}回 (総勤務時間: {work_hours:.1f}h, 今日: {daily_hours:.1f}h){status}")
            
            # 偏り度を計算
            max_count = max(self.inspector_assignment_count.values())
            min_count = min(self.inspector_assignment_count.values())
            imbalance = max_count - min_count
            
            self.log_message("")
            self.log_message(f"最大割り当て回数: {max_count}回")
            self.log_message(f"最小割り当て回数: {min_count}回")
            self.log_message(f"偏り度: {imbalance}回")
            
            if imbalance <= 1:
                self.log_message("✅ 割り当ては非常に公平です")
            elif imbalance <= 2:
                self.log_message("⚠️ 割り当てに軽微な偏りがあります")
            else:
                self.log_message("❌ 割り当てに偏りがあります")
            
            self.log_message("========================")
            
        except Exception as e:
            self.log_message(f"統計表示中にエラーが発生しました: {str(e)}")
    
    def optimize_assignments(self, result_df, inspector_master_df, skill_master_df, show_skill_values=False):
        """全体最適化：勤務時間超過の是正と偏りの調整"""
        try:
            self.log_message("全体最適化フェーズ0: result_dfから実際の割り当てを再計算")
            
            # 最優先ルール: 出荷予定日の古い順にソート（処理の最初に必ず実行）
            result_df['出荷予定日'] = pd.to_datetime(result_df['出荷予定日'], errors='coerce')
            result_df = result_df.sort_values('出荷予定日', na_position='last').reset_index(drop=True)
            self.log_message("最適化処理開始前に出荷予定日の古い順でソートしました（最優先ルール）")
            
            current_date = pd.Timestamp.now().date()
            
            # result_dfから実際の割り当てを読み取って、履歴を再計算（正確な状態を把握）
            self.inspector_daily_assignments = {}
            self.inspector_work_hours = {}
            self.inspector_product_hours = {}
            
            for index, row in result_df.iterrows():
                product_number = row['品番']
                divided_time = row.get('分割検査時間', 0.0)
                inspection_time = row.get('検査時間', divided_time)
                
                # 各検査員の割り当てを確認
                for i in range(1, 6):
                    inspector_col = f'検査員{i}'
                    if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                        inspector_name = str(row[inspector_col]).strip()
                        # スキル値や(新)を除去
                        if '(' in inspector_name:
                            inspector_name = inspector_name.split('(')[0].strip()
                        
                        if not inspector_name:
                            continue
                        
                        # 検査員コードを取得
                        inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                        if not inspector_info.empty:
                            inspector_code = inspector_info.iloc[0]['#ID']
                            
                            # 履歴を初期化
                            if inspector_code not in self.inspector_daily_assignments:
                                self.inspector_daily_assignments[inspector_code] = {}
                            if current_date not in self.inspector_daily_assignments[inspector_code]:
                                self.inspector_daily_assignments[inspector_code][current_date] = 0.0
                            if inspector_code not in self.inspector_work_hours:
                                self.inspector_work_hours[inspector_code] = 0.0
                            if inspector_code not in self.inspector_product_hours:
                                self.inspector_product_hours[inspector_code] = {}
                            if product_number not in self.inspector_product_hours[inspector_code]:
                                self.inspector_product_hours[inspector_code][product_number] = 0.0
                            
                            # 履歴を累積
                            self.inspector_daily_assignments[inspector_code][current_date] += divided_time
                            self.inspector_work_hours[inspector_code] += divided_time
                            self.inspector_product_hours[inspector_code][product_number] += divided_time
            
            self.log_message("履歴の再計算が完了しました")
            
            # 各検査員の最大勤務時間を取得
            inspector_max_hours = {}
            for _, inspector_row in inspector_master_df.iterrows():
                code = inspector_row['#ID']
                max_hours = self.get_inspector_max_hours(code, inspector_master_df)
                inspector_max_hours[code] = max_hours
            
            # フェーズ1: 勤務時間超過と同一品番4時間超過を検出・是正（繰り返し処理）
            self.log_message("全体最適化フェーズ1: 勤務時間超過と同一品番4時間超過の検出と是正を開始")
            
            max_iterations = 10  # 最大10回繰り返し
            iteration = 0
            
            while iteration < max_iterations:
                iteration += 1
                self.log_message(f"是正処理 イテレーション {iteration}")
                
                violations_found = False
                overworked_assignments = []
                product_limit_violations = []
                
                # 最優先ルール: 出荷予定日の古い順にソート（毎回のイテレーションで確実に）
                result_df['出荷予定日'] = pd.to_datetime(result_df['出荷予定日'], errors='coerce')
                result_df_sorted = result_df.sort_values('出荷予定日', na_position='last').reset_index(drop=True)
                self.log_message(f"イテレーション {iteration}: 出荷予定日の古い順でソートしました（最優先ルール）")
                
                for idx, (index, row) in enumerate(result_df_sorted.iterrows()):
                    product_number = row['品番']
                    divided_time = row.get('分割検査時間', 0.0)
                    inspection_time = row.get('検査時間', divided_time)
                    
                    # 各検査員の割り当てを確認
                    for i in range(1, 6):
                        inspector_col = f'検査員{i}'
                        if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                            inspector_name = str(row[inspector_col]).strip()
                            # スキル値や(新)を除去
                            if '(' in inspector_name:
                                inspector_name = inspector_name.split('(')[0].strip()
                            
                            if not inspector_name:
                                continue
                            
                            # 検査員コードを取得
                            inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                            if not inspector_info.empty:
                                inspector_code = inspector_info.iloc[0]['#ID']
                                
                                # 現在の履歴を取得
                                daily_hours = self.inspector_daily_assignments.get(inspector_code, {}).get(current_date, 0.0)
                                max_hours = inspector_max_hours.get(inspector_code, 8.0)
                                product_hours = self.inspector_product_hours.get(inspector_code, {}).get(product_number, 0.0)
                                
                                # 勤務時間超過をチェック（厳密に - 0.05時間の余裕を考慮）
                                # 実際の勤務時間が最大勤務時間を超えている場合のみ違反とする
                                if daily_hours > max_hours - 0.05:
                                    excess = daily_hours - max_hours
                                    overworked_assignments.append((index, inspector_code, inspector_name, excess, divided_time, product_number, inspection_time, i))
                                    violations_found = True
                                    self.log_message(f"⚠️ 勤務時間超過: 検査員 '{inspector_name}' (コード: {inspector_code}) {daily_hours:.1f}h > {max_hours:.1f}h (超過: {excess:.1f}h, 品番: {product_number}, ロットインデックス: {index})")
                                
                                # 同一品番の4時間超過をチェック（厳密に）
                                if product_hours > 4.0:
                                    excess = product_hours - 4.0
                                    product_limit_violations.append((index, inspector_code, inspector_name, excess, divided_time, product_number, inspection_time, i))
                                    violations_found = True
                                    self.log_message(f"⚠️ 同一品番4時間超過: 検査員 '{inspector_name}' (コード: {inspector_code}) 品番 {product_number} {product_hours:.1f}h > 4.0h (超過: {excess:.1f}h, ロットインデックス: {index})")
                
                # 違反が見つからない場合は終了
                if not violations_found:
                    self.log_message(f"全てのルール違反が解消されました（{iteration}回目のイテレーションで完了）")
                    result_df = result_df_sorted
                    break
                
                # 違反を是正（出荷予定日が古い順）
                all_violations = overworked_assignments + product_limit_violations
                # 重複を除去（同じロットの複数の違反を1つにまとめる）
                unique_violations = {}
                for violation in all_violations:
                    index = violation[0]
                    if index not in unique_violations:
                        unique_violations[index] = violation
                    else:
                        # 既存の違反より新しい違反の方が重要度が高い場合は置き換え
                        existing = unique_violations[index]
                        if violation[3] > existing[3]:  # excess値が大きい方
                            unique_violations[index] = violation
                
                sorted_violations = sorted(unique_violations.values(), 
                    key=lambda x: (result_df_sorted.at[x[0], '出荷予定日'] if x[0] < len(result_df_sorted) else pd.Timestamp.min, x[0]))
                
                self.log_message(f"違反ロット数: {len(sorted_violations)}件を是正します")
                
                # 各違反を是正
                fixed_any = False
                fixed_indices = set()
                for violation in sorted_violations:
                    index, inspector_code, inspector_name, excess, divided_time, product_number, inspection_time, inspector_col_num = violation
                    # 既に是正済みのロットはスキップ
                    if index in fixed_indices:
                        continue
                    
                    self.log_message(f"違反是正を試みます: ロットインデックス {index}, 検査員 {inspector_name}, 品番 {product_number}")
                    fixed = self.fix_single_violation(
                        index, inspector_code, inspector_name, divided_time, product_number, inspection_time, inspector_col_num,
                        result_df_sorted, inspector_master_df, skill_master_df, inspector_max_hours, current_date, show_skill_values
                    )
                    if fixed:
                        fixed_any = True
                        fixed_indices.add(index)
                        self.log_message(f"✅ 違反是正成功: ロットインデックス {index}")
                
                if not fixed_any and len(sorted_violations) > 0:
                    # 是正できなかった違反がある場合は、該当ロットを未割当にする
                    self.log_message(f"⚠️ 是正できなかった違反が {len(sorted_violations)}件あります。該当ロットを未割当にします")
                    for violation in sorted_violations:
                        index, inspector_code, inspector_name, excess, divided_time, product_number, inspection_time, inspector_col_num = violation
                        self.clear_assignment(result_df_sorted, index)
                        self.log_message(f"⚠️ ロットインデックス {index} (品番: {product_number}) を未割当にしました")
                    result_df = result_df_sorted
                    # 履歴を再計算してから次へ
                    self.inspector_daily_assignments = {}
                    self.inspector_work_hours = {}
                    self.inspector_product_hours = {}
                    for idx, (index, row) in enumerate(result_df_sorted.iterrows()):
                        product_number = row['品番']
                        divided_time = row.get('分割検査時間', 0.0)
                        for i in range(1, 6):
                            inspector_col = f'検査員{i}'
                            if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                                inspector_name = str(row[inspector_col]).strip()
                                if '(' in inspector_name:
                                    inspector_name = inspector_name.split('(')[0].strip()
                                if not inspector_name:
                                    continue
                                inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                                if not inspector_info.empty:
                                    inspector_code = inspector_info.iloc[0]['#ID']
                                    if inspector_code not in self.inspector_daily_assignments:
                                        self.inspector_daily_assignments[inspector_code] = {}
                                    if current_date not in self.inspector_daily_assignments[inspector_code]:
                                        self.inspector_daily_assignments[inspector_code][current_date] = 0.0
                                    if inspector_code not in self.inspector_work_hours:
                                        self.inspector_work_hours[inspector_code] = 0.0
                                    if inspector_code not in self.inspector_product_hours:
                                        self.inspector_product_hours[inspector_code] = {}
                                    if product_number not in self.inspector_product_hours[inspector_code]:
                                        self.inspector_product_hours[inspector_code][product_number] = 0.0
                                    self.inspector_daily_assignments[inspector_code][current_date] += divided_time
                                    self.inspector_work_hours[inspector_code] += divided_time
                                    self.inspector_product_hours[inspector_code][product_number] += divided_time
                    continue  # 次のイテレーションへ
                
                # 履歴を再度再計算（是正後の状態を反映）
                self.inspector_daily_assignments = {}
                self.inspector_work_hours = {}
                self.inspector_product_hours = {}
                
                for idx, (index, row) in enumerate(result_df_sorted.iterrows()):
                    product_number = row['品番']
                    divided_time = row.get('分割検査時間', 0.0)
                    inspection_time = row.get('検査時間', divided_time)
                    
                    for i in range(1, 6):
                        inspector_col = f'検査員{i}'
                        if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                            inspector_name = str(row[inspector_col]).strip()
                            if '(' in inspector_name:
                                inspector_name = inspector_name.split('(')[0].strip()
                            if not inspector_name:
                                continue
                            
                            inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                            if not inspector_info.empty:
                                inspector_code = inspector_info.iloc[0]['#ID']
                                
                                if inspector_code not in self.inspector_daily_assignments:
                                    self.inspector_daily_assignments[inspector_code] = {}
                                if current_date not in self.inspector_daily_assignments[inspector_code]:
                                    self.inspector_daily_assignments[inspector_code][current_date] = 0.0
                                if inspector_code not in self.inspector_work_hours:
                                    self.inspector_work_hours[inspector_code] = 0.0
                                if inspector_code not in self.inspector_product_hours:
                                    self.inspector_product_hours[inspector_code] = {}
                                if product_number not in self.inspector_product_hours[inspector_code]:
                                    self.inspector_product_hours[inspector_code][product_number] = 0.0
                                
                                self.inspector_daily_assignments[inspector_code][current_date] += divided_time
                                self.inspector_work_hours[inspector_code] += divided_time
                                self.inspector_product_hours[inspector_code][product_number] += divided_time
                
                result_df = result_df_sorted
            
            # フェーズ1.5: 最終違反チェック（是正が完全に機能したか確認）
            self.log_message("全体最適化フェーズ1.5: 最終違反チェックを開始")
            
            # 最終的な履歴を再計算
            self.inspector_daily_assignments = {}
            self.inspector_work_hours = {}
            self.inspector_product_hours = {}
            
            for index, row in result_df.iterrows():
                product_number = row['品番']
                divided_time = row.get('分割検査時間', 0.0)
                
                for i in range(1, 6):
                    inspector_col = f'検査員{i}'
                    if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                        inspector_name = str(row[inspector_col]).strip()
                        if '(' in inspector_name:
                            inspector_name = inspector_name.split('(')[0].strip()
                        if not inspector_name:
                            continue
                        
                        inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                        if not inspector_info.empty:
                            inspector_code = inspector_info.iloc[0]['#ID']
                            
                            if inspector_code not in self.inspector_daily_assignments:
                                self.inspector_daily_assignments[inspector_code] = {}
                            if current_date not in self.inspector_daily_assignments[inspector_code]:
                                self.inspector_daily_assignments[inspector_code][current_date] = 0.0
                            if inspector_code not in self.inspector_work_hours:
                                self.inspector_work_hours[inspector_code] = 0.0
                            if inspector_code not in self.inspector_product_hours:
                                self.inspector_product_hours[inspector_code] = {}
                            if product_number not in self.inspector_product_hours[inspector_code]:
                                self.inspector_product_hours[inspector_code][product_number] = 0.0
                            
                            self.inspector_daily_assignments[inspector_code][current_date] += divided_time
                            self.inspector_work_hours[inspector_code] += divided_time
                            self.inspector_product_hours[inspector_code][product_number] += divided_time
            
            # 最終違反チェック
            final_violations = []
            for index, row in result_df.iterrows():
                product_number = row['品番']
                divided_time = row.get('分割検査時間', 0.0)
                
                for i in range(1, 6):
                    inspector_col = f'検査員{i}'
                    if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                        inspector_name = str(row[inspector_col]).strip()
                        if '(' in inspector_name:
                            inspector_name = inspector_name.split('(')[0].strip()
                        if not inspector_name:
                            continue
                        
                        inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                        if not inspector_info.empty:
                            inspector_code = inspector_info.iloc[0]['#ID']
                            daily_hours = self.inspector_daily_assignments.get(inspector_code, {}).get(current_date, 0.0)
                            max_hours = inspector_max_hours.get(inspector_code, 8.0)
                            product_hours = self.inspector_product_hours.get(inspector_code, {}).get(product_number, 0.0)
                            
                            # 最終チェックも厳密に（0.05時間の余裕を考慮）
                            if daily_hours > max_hours - 0.05:
                                final_violations.append((index, inspector_code, inspector_name, "勤務時間超過", daily_hours, max_hours))
                                self.log_message(f"❌ 最終チェック: 勤務時間超過が残っています - 検査員 '{inspector_name}' {daily_hours:.1f}h > {max_hours:.1f}h (ロット {index})")
                            
                            if product_hours > 4.0:
                                final_violations.append((index, inspector_code, inspector_name, "同一品番4時間超過", product_hours, 4.0))
                                self.log_message(f"❌ 最終チェック: 同一品番4時間超過が残っています - 検査員 '{inspector_name}' 品番 {product_number} {product_hours:.1f}h > 4.0h (ロット {index})")
            
            if final_violations:
                self.log_message(f"⚠️ 警告: {len(final_violations)}件の違反が最終チェックで検出されました。該当ロットを未割当にします")
                for violation in final_violations:
                    index = violation[0]
                    self.clear_assignment(result_df, index)
                    self.log_message(f"⚠️ ロットインデックス {index} を未割当にしました（{violation[3]}）")
                
                # 未割当後の履歴を再計算
                self.inspector_daily_assignments = {}
                self.inspector_work_hours = {}
                self.inspector_product_hours = {}
                
                for index, row in result_df.iterrows():
                    product_number = row['品番']
                    divided_time = row.get('分割検査時間', 0.0)
                    
                    for i in range(1, 6):
                        inspector_col = f'検査員{i}'
                        if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                            inspector_name = str(row[inspector_col]).strip()
                            if '(' in inspector_name:
                                inspector_name = inspector_name.split('(')[0].strip()
                            if not inspector_name:
                                continue
                            
                            inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                            if not inspector_info.empty:
                                inspector_code = inspector_info.iloc[0]['#ID']
                                
                                if inspector_code not in self.inspector_daily_assignments:
                                    self.inspector_daily_assignments[inspector_code] = {}
                                if current_date not in self.inspector_daily_assignments[inspector_code]:
                                    self.inspector_daily_assignments[inspector_code][current_date] = 0.0
                                if inspector_code not in self.inspector_work_hours:
                                    self.inspector_work_hours[inspector_code] = 0.0
                                if inspector_code not in self.inspector_product_hours:
                                    self.inspector_product_hours[inspector_code] = {}
                                if product_number not in self.inspector_product_hours[inspector_code]:
                                    self.inspector_product_hours[inspector_code][product_number] = 0.0
                                
                                self.inspector_daily_assignments[inspector_code][current_date] += divided_time
                                self.inspector_work_hours[inspector_code] += divided_time
                                self.inspector_product_hours[inspector_code][product_number] += divided_time
            else:
                self.log_message("✅ 最終チェック: 全てのルール違反が解消されました")
            
            # フェーズ2: 偏りの是正（総勤務時間の偏りを調整）
            self.log_message("全体最適化フェーズ2: 偏りの是正を開始")
            
            # 平均勤務時間を計算
            total_hours = sum(self.inspector_work_hours.values())
            active_inspectors = [code for code in self.inspector_work_hours.keys() if self.inspector_work_hours.get(code, 0.0) > 0]
            
            if active_inspectors:
                avg_hours = total_hours / len(active_inspectors)
                max_hours_val = max(self.inspector_work_hours.values())
                min_hours_val = min([self.inspector_work_hours.get(code, 0.0) for code in active_inspectors])
                imbalance = max_hours_val - min_hours_val
                
                self.log_message(f"偏り分析: 平均 {avg_hours:.1f}h, 最大 {max_hours_val:.1f}h, 最小 {min_hours_val:.1f}h, 偏り {imbalance:.1f}h")
                
                # 偏りが大きい場合（平均の15%以上）、調整を試みる
                imbalance_threshold = avg_hours * 0.15
                if imbalance > imbalance_threshold and len(active_inspectors) > 1:
                    self.log_message(f"偏りが大きいため調整を試みます (閾値: {imbalance_threshold:.1f}h, 実際: {imbalance:.1f}h)")
                    
                    # 多忙な検査員から余裕のある検査員へ一部を移動
                    # （ただし出荷予定日の順序は維持）
                    
                    over_loaded = [(code, hours) for code, hours in self.inspector_work_hours.items() 
                                   if hours > avg_hours * 1.1]
                    under_loaded = [(code, hours) for code, hours in self.inspector_work_hours.items() 
                                    if hours < avg_hours * 0.9 and hours > 0]
                    
                    if over_loaded and under_loaded:
                        self.log_message(f"調整対象: 多忙 {len(over_loaded)}人, 余裕あり {len(under_loaded)}人")
                        
                        # 多忙な検査員を勤務時間の多い順にソート
                        over_loaded.sort(key=lambda x: x[1], reverse=True)
                        # 余裕のある検査員を勤務時間の少ない順にソート
                        under_loaded.sort(key=lambda x: x[1])
                        
                        # 出荷予定日の古い順にソート（順序を維持）
                        result_df['出荷予定日'] = pd.to_datetime(result_df['出荷予定日'], errors='coerce')
                        result_df_sorted = result_df.sort_values('出荷予定日', na_position='last').reset_index(drop=True)
                        
                        # 再割当て回数を制限（無限ループを防ぐ）
                        max_reassignments = 50
                        reassignment_count = 0
                        
                        # 各多忙な検査員について、割り当てられたロットを確認
                        for overloaded_code, overloaded_hours in over_loaded:
                            if reassignment_count >= max_reassignments:
                                break
                            
                            # この検査員が割り当てられているロットを取得（出荷予定日順）
                            assigned_lots = []
                            for index, row in result_df_sorted.iterrows():
                                product_number = row['品番']
                                divided_time = row.get('分割検査時間', 0.0)
                                
                                # このロットにこの検査員が含まれているか確認
                                for i in range(1, 6):
                                    inspector_col = f'検査員{i}'
                                    if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                                        inspector_name = str(row[inspector_col]).strip()
                                        if '(' in inspector_name:
                                            inspector_name = inspector_name.split('(')[0].strip()
                                        
                                        if not inspector_name:
                                            continue
                                        
                                        inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                                        if not inspector_info.empty:
                                            lot_inspector_code = inspector_info.iloc[0]['#ID']
                                            if lot_inspector_code == overloaded_code:
                                                assigned_lots.append((index, product_number, divided_time, i, row))
                                                break
                            
                            # 各ロットについて、余裕のある検査員への再割当てを試みる
                            for lot_index, product_number, divided_time, inspector_col_num, row in assigned_lots:
                                if reassignment_count >= max_reassignments:
                                    break
                                
                                # 再割当て可能かチェック（出荷予定日が古い順に処理）
                                process_number = row.get('現在工程番号', '')
                                
                                # スキルマスタに登録があるか確認
                                skill_rows = skill_master_df[skill_master_df.iloc[:, 0] == product_number]
                                is_new_product = skill_rows.empty
                                
                                # 利用可能な検査員を取得
                                if is_new_product:
                                    available_inspectors = self.get_new_product_team_inspectors(inspector_master_df)
                                else:
                                    available_inspectors = self.get_available_inspectors(
                                        product_number, process_number, skill_master_df, inspector_master_df
                                    )
                                
                                if not available_inspectors:
                                    continue
                                
                                # 現在のロットの他の検査員を取得（再割当て時に除外するため）
                                current_codes = []
                                for i in range(1, 6):
                                    inspector_col = f'検査員{i}'
                                    if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                                        inspector_name_check = str(row[inspector_col]).strip()
                                        if '(' in inspector_name_check:
                                            inspector_name_check = inspector_name_check.split('(')[0].strip()
                                        if not inspector_name_check:
                                            continue
                                        inspector_info_check = inspector_master_df[inspector_master_df['#氏名'] == inspector_name_check]
                                        if not inspector_info_check.empty:
                                            current_codes.append(inspector_info_check.iloc[0]['#ID'])
                                
                                # 余裕のある検査員の中から、条件を満たす候補を探す
                                replacement_candidates = []
                                for insp in available_inspectors:
                                    candidate_code = insp['コード']
                                    
                                    # 既に割り当てられている人は除外
                                    if candidate_code in current_codes:
                                        continue
                                    
                                    # 多忙な人（平均の110%以上）への再割当ては避ける
                                    candidate_total_hours = self.inspector_work_hours.get(candidate_code, 0.0)
                                    if candidate_total_hours > avg_hours * 1.05:
                                        continue
                                    
                                    # 勤務時間制約をチェック
                                    candidate_max_hours = inspector_max_hours.get(candidate_code, 8.0)
                                    if not self.check_work_hours_capacity(candidate_code, divided_time, candidate_max_hours, current_date):
                                        continue
                                    
                                    # 同一品番の4時間上限をチェック
                                    candidate_product_hours = self.inspector_product_hours.get(candidate_code, {}).get(product_number, 0.0)
                                    if candidate_product_hours + divided_time > 4.0:
                                        continue
                                    
                                    # 候補として追加（総勤務時間が少ない順に優先）
                                    replacement_candidates.append((candidate_total_hours, candidate_code, insp))
                                
                                # 最も総勤務時間が少ない候補を選択
                                if replacement_candidates:
                                    replacement_candidates.sort(key=lambda x: x[0])
                                    _, new_code, replacement_inspector = replacement_candidates[0]
                                    
                                    # 再割当てを実行
                                    # 元の検査員名を取得
                                    old_inspector_name = None
                                    for i in range(1, 6):
                                        inspector_col = f'検査員{i}'
                                        if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                                            inspector_name_check = str(row[inspector_col]).strip()
                                            if '(' in inspector_name_check:
                                                inspector_name_check = inspector_name_check.split('(')[0].strip()
                                            if not inspector_name_check:
                                                continue
                                            inspector_info_check = inspector_master_df[inspector_master_df['#氏名'] == inspector_name_check]
                                            if not inspector_info_check.empty:
                                                if inspector_info_check.iloc[0]['#ID'] == overloaded_code:
                                                    old_inspector_name = inspector_name_check
                                                    break
                                    
                                    if old_inspector_name:
                                        # 新しい検査員名を設定
                                        if show_skill_values:
                                            if replacement_inspector.get('is_new_team', False):
                                                replacement_name = f"{replacement_inspector['氏名']}(新)"
                                            else:
                                                replacement_name = f"{replacement_inspector['氏名']}({replacement_inspector['スキル']})"
                                        else:
                                            if replacement_inspector.get('is_new_team', False):
                                                replacement_name = f"{replacement_inspector['氏名']}(新)"
                                            else:
                                                replacement_name = replacement_inspector['氏名']
                                        
                                        # 結果データフレームで該当する検査員を置き換え
                                        result_df_sorted.at[lot_index, f'検査員{inspector_col_num}'] = replacement_name
                                        
                                        # 履歴を更新（元の検査員から時間を引く）
                                        old_daily = self.inspector_daily_assignments.get(overloaded_code, {}).get(current_date, 0.0)
                                        old_total = self.inspector_work_hours.get(overloaded_code, 0.0)
                                        self.inspector_daily_assignments[overloaded_code][current_date] = max(0.0, old_daily - divided_time)
                                        self.inspector_work_hours[overloaded_code] = max(0.0, old_total - divided_time)
                                        
                                        # 品番別累計時間も更新
                                        if overloaded_code in self.inspector_product_hours:
                                            if product_number in self.inspector_product_hours[overloaded_code]:
                                                self.inspector_product_hours[overloaded_code][product_number] = max(
                                                    0.0, 
                                                    self.inspector_product_hours[overloaded_code][product_number] - divided_time
                                                )
                                        
                                        # 新しい検査員に時間を追加
                                        if new_code not in self.inspector_daily_assignments:
                                            self.inspector_daily_assignments[new_code] = {}
                                        if current_date not in self.inspector_daily_assignments[new_code]:
                                            self.inspector_daily_assignments[new_code][current_date] = 0.0
                                        
                                        self.inspector_daily_assignments[new_code][current_date] += divided_time
                                        if new_code not in self.inspector_work_hours:
                                            self.inspector_work_hours[new_code] = 0.0
                                        self.inspector_work_hours[new_code] += divided_time
                                        
                                        # 品番別累計時間も更新
                                        if new_code not in self.inspector_product_hours:
                                            self.inspector_product_hours[new_code] = {}
                                        self.inspector_product_hours[new_code][product_number] = (
                                            self.inspector_product_hours[new_code].get(product_number, 0.0) + divided_time
                                        )
                                        
                                        # チーム情報を更新
                                        self.update_team_info(result_df_sorted, lot_index, inspector_master_df, show_skill_values)
                                        
                                        reassignment_count += 1
                                        self.log_message(
                                            f"偏り是正: '{old_inspector_name}' ({overloaded_hours:.1f}h) → "
                                            f"'{replacement_inspector['氏名']}' ({self.inspector_work_hours[new_code]:.1f}h) "
                                            f"(品番: {product_number}, 出荷予定日: {row['出荷予定日']})"
                                        )
                                        
                                        # 再割当て後、多忙な検査員のリストを更新
                                        overloaded_hours = self.inspector_work_hours.get(overloaded_code, 0.0)
                                        if overloaded_hours <= avg_hours * 1.1:
                                            # この検査員はもう多忙ではないので終了
                                            break
                        
                        # 結果を更新
                        result_df = result_df_sorted
                        
                        # 再割当て後の偏りを再計算
                        total_hours_after = sum(self.inspector_work_hours.values())
                        active_inspectors_after = [code for code in self.inspector_work_hours.keys() 
                                                   if self.inspector_work_hours.get(code, 0.0) > 0]
                        if active_inspectors_after:
                            avg_hours_after = total_hours_after / len(active_inspectors_after)
                            max_hours_after = max(self.inspector_work_hours.values())
                            min_hours_after = min([self.inspector_work_hours.get(code, 0.0) for code in active_inspectors_after])
                            imbalance_after = max_hours_after - min_hours_after
                            self.log_message(
                                f"偏り是正後: 平均 {avg_hours_after:.1f}h, 最大 {max_hours_after:.1f}h, "
                                f"最小 {min_hours_after:.1f}h, 偏り {imbalance_after:.1f}h "
                                f"(改善: {imbalance - imbalance_after:.1f}h)"
                            )
                        
                        self.log_message(f"偏り是正: {reassignment_count}件の再割当てを実行しました")
            
            # フェーズ2.5: 偏り是正後の最終検証（勤務時間超過の再チェック）
            self.log_message("全体最適化フェーズ2.5: 偏り是正後の最終検証を開始")
            
            # 履歴を再計算
            self.inspector_daily_assignments = {}
            self.inspector_work_hours = {}
            self.inspector_product_hours = {}
            
            for index, row in result_df.iterrows():
                product_number = row['品番']
                divided_time = row.get('分割検査時間', 0.0)
                
                for i in range(1, 6):
                    inspector_col = f'検査員{i}'
                    if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                        inspector_name = str(row[inspector_col]).strip()
                        if '(' in inspector_name:
                            inspector_name = inspector_name.split('(')[0].strip()
                        if not inspector_name:
                            continue
                        
                        inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                        if not inspector_info.empty:
                            inspector_code = inspector_info.iloc[0]['#ID']
                            
                            if inspector_code not in self.inspector_daily_assignments:
                                self.inspector_daily_assignments[inspector_code] = {}
                            if current_date not in self.inspector_daily_assignments[inspector_code]:
                                self.inspector_daily_assignments[inspector_code][current_date] = 0.0
                            if inspector_code not in self.inspector_work_hours:
                                self.inspector_work_hours[inspector_code] = 0.0
                            if inspector_code not in self.inspector_product_hours:
                                self.inspector_product_hours[inspector_code] = {}
                            if product_number not in self.inspector_product_hours[inspector_code]:
                                self.inspector_product_hours[inspector_code][product_number] = 0.0
                            
                            self.inspector_daily_assignments[inspector_code][current_date] += divided_time
                            self.inspector_work_hours[inspector_code] += divided_time
                            self.inspector_product_hours[inspector_code][product_number] += divided_time
            
            # 勤務時間超過を再チェック
            phase2_5_violations = []
            for index, row in result_df.iterrows():
                product_number = row['品番']
                divided_time = row.get('分割検査時間', 0.0)
                
                for i in range(1, 6):
                    inspector_col = f'検査員{i}'
                    if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                        inspector_name = str(row[inspector_col]).strip()
                        if '(' in inspector_name:
                            inspector_name = inspector_name.split('(')[0].strip()
                        if not inspector_name:
                            continue
                        
                        inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                        if not inspector_info.empty:
                            inspector_code = inspector_info.iloc[0]['#ID']
                            daily_hours = self.inspector_daily_assignments.get(inspector_code, {}).get(current_date, 0.0)
                            max_hours = inspector_max_hours.get(inspector_code, 8.0)
                            product_hours = self.inspector_product_hours.get(inspector_code, {}).get(product_number, 0.0)
                            
                            # 勤務時間超過をチェック（厳密に）
                            if daily_hours > max_hours - 0.05:
                                phase2_5_violations.append((index, inspector_code, inspector_name, "勤務時間超過", daily_hours, max_hours))
                                self.log_message(f"❌ フェーズ2.5検証: 勤務時間超過が検出されました - 検査員 '{inspector_name}' {daily_hours:.1f}h > {max_hours:.1f}h (ロット {index})")
                            
                            # 同一品番4時間超過をチェック
                            if product_hours > 4.0:
                                phase2_5_violations.append((index, inspector_code, inspector_name, "同一品番4時間超過", product_hours, 4.0))
                                self.log_message(f"❌ フェーズ2.5検証: 同一品番4時間超過が検出されました - 検査員 '{inspector_name}' 品番 {product_number} {product_hours:.1f}h > 4.0h (ロット {index})")
            
            if phase2_5_violations:
                self.log_message(f"⚠️ 警告: フェーズ2.5検証で {len(phase2_5_violations)}件の違反が検出されました。該当ロットを未割当にします")
                for violation in phase2_5_violations:
                    index = violation[0]
                    self.clear_assignment(result_df, index)
                    self.log_message(f"⚠️ ロットインデックス {index} を未割当にしました（{violation[3]}）")
                
                # 未割当後の履歴を再計算
                self.inspector_daily_assignments = {}
                self.inspector_work_hours = {}
                self.inspector_product_hours = {}
                
                for index, row in result_df.iterrows():
                    product_number = row['品番']
                    divided_time = row.get('分割検査時間', 0.0)
                    
                    for i in range(1, 6):
                        inspector_col = f'検査員{i}'
                        if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                            inspector_name = str(row[inspector_col]).strip()
                            if '(' in inspector_name:
                                inspector_name = inspector_name.split('(')[0].strip()
                            if not inspector_name:
                                continue
                            
                            inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                            if not inspector_info.empty:
                                inspector_code = inspector_info.iloc[0]['#ID']
                                
                                if inspector_code not in self.inspector_daily_assignments:
                                    self.inspector_daily_assignments[inspector_code] = {}
                                if current_date not in self.inspector_daily_assignments[inspector_code]:
                                    self.inspector_daily_assignments[inspector_code][current_date] = 0.0
                                if inspector_code not in self.inspector_work_hours:
                                    self.inspector_work_hours[inspector_code] = 0.0
                                if inspector_code not in self.inspector_product_hours:
                                    self.inspector_product_hours[inspector_code] = {}
                                if product_number not in self.inspector_product_hours[inspector_code]:
                                    self.inspector_product_hours[inspector_code][product_number] = 0.0
                                
                                self.inspector_daily_assignments[inspector_code][current_date] += divided_time
                                self.inspector_work_hours[inspector_code] += divided_time
                                self.inspector_product_hours[inspector_code][product_number] += divided_time
            else:
                self.log_message("✅ フェーズ2.5検証: 全てのルール違反が解消されました")
            
            # フェーズ3: 未割当ロットの再処理（出荷予定日順、新規品優先）
            self.log_message("全体最適化フェーズ3: 未割当ロットの再処理を開始")
            
            # 未割当のロットを取得（出荷予定日順）
            unassigned_indices = []
            for index, row in result_df.iterrows():
                inspector_count = row.get('検査員人数', 0)
                if inspector_count == 0 or pd.isna(inspector_count) or inspector_count == 0:
                    unassigned_indices.append(index)
            
            if unassigned_indices:
                self.log_message(f"未割当ロットが {len(unassigned_indices)}件見つかりました。再処理を開始します")
                
                # 出荷予定日順にソート（元のインデックスを保持）
                # 同じ出荷予定日の場合は新規品を優先
                unassigned_df = result_df.loc[unassigned_indices].copy()
                unassigned_df['_original_index'] = unassigned_indices  # 元のインデックスを保持
                unassigned_df['出荷予定日'] = pd.to_datetime(unassigned_df['出荷予定日'], errors='coerce')
                
                # 新規品かどうかを判定
                def is_new_product_for_unassigned(row):
                    product_number = row['品番']
                    skill_rows = skill_master_df[skill_master_df.iloc[:, 0] == product_number]
                    return skill_rows.empty
                
                unassigned_df['_is_new_product'] = unassigned_df.apply(is_new_product_for_unassigned, axis=1)
                
                # 出荷予定日順にソートし、同じ出荷予定日の場合は新規品を優先
                unassigned_df = unassigned_df.sort_values(
                    ['出荷予定日', '_is_new_product'], 
                    ascending=[True, False],  # 出荷予定日は昇順、新規品フラグは降順（Trueを先に）
                    na_position='last'
                ).reset_index(drop=True)
                
                # 各未割当ロットを再処理
                original_indices = unassigned_df['_original_index'].tolist()  # 元のインデックスを保存
                
                # ソート用の列を削除
                unassigned_df = unassigned_df.drop(columns=['_is_new_product', '_original_index'])
                
                # 各未割当ロットを再処理
                for idx, row in unassigned_df.iterrows():
                    original_index = original_indices[idx]  # 元のインデックスを取得
                    product_number = row['品番']
                    inspection_time = row.get('検査時間', 0.0)
                    process_number = row.get('現在工程番号', '')
                    lot_quantity = row.get('ロット数量', 0)
                    
                    # ロット数量が0の場合は検査員を割り当てない
                    if lot_quantity == 0 or pd.isna(lot_quantity) or inspection_time == 0 or pd.isna(inspection_time):
                        self.log_message(f"未割当ロット再処理: ロット数量が0または検査時間が0のため、品番 {product_number} の検査員割り当てをスキップします")
                        continue
                    
                    # 必要人数を計算
                    if inspection_time <= 3.0:
                        required_inspectors = 1
                    else:
                        required_inspectors = max(2, int(inspection_time / 3.0) + 1)
                    
                    divided_time = inspection_time / required_inspectors
                    
                    # 新規品かどうかを判定
                    skill_rows = skill_master_df[skill_master_df.iloc[:, 0] == product_number]
                    is_new_product = skill_rows.empty
                    
                    # 利用可能な検査員を取得
                    if is_new_product:
                        self.log_message(f"未割当ロット再処理: 品番 {product_number} は新規品です。新製品チームを優先的に取得します")
                        available_inspectors = self.get_new_product_team_inspectors(inspector_master_df)
                    else:
                        available_inspectors = self.get_available_inspectors(
                            product_number, process_number, skill_master_df, inspector_master_df
                        )
                    
                    if not available_inspectors:
                        if is_new_product:
                            self.log_message(f"警告: 新規品 {product_number} の新製品チームが見つかりません")
                        else:
                            self.log_message(f"警告: 品番 {product_number} の検査員が見つかりません")
                        continue
                    
                    # 検査員を選択（新規品の場合は優先的に割り当て）
                    assigned_inspectors = self.select_inspectors(
                        available_inspectors, required_inspectors, divided_time, 
                        inspector_master_df, product_number, is_new_product=is_new_product
                    )
                    
                    if assigned_inspectors:
                        # 割り当て成功
                        result_df.at[original_index, '検査員人数'] = len(assigned_inspectors)
                        result_df.at[original_index, '分割検査時間'] = round(divided_time, 1)
                        
                        # 検査員名を設定
                        team_members = []
                        for i, inspector in enumerate(assigned_inspectors):
                            if i < 5:
                                if show_skill_values:
                                    if inspector.get('is_new_team', False):
                                        inspector_name = f"{inspector['氏名']}(新)"
                                    else:
                                        inspector_name = f"{inspector['氏名']}({inspector['スキル']})"
                                else:
                                    if inspector.get('is_new_team', False):
                                        inspector_name = f"{inspector['氏名']}(新)"
                                    else:
                                        inspector_name = inspector['氏名']
                                
                                result_df.at[original_index, f'検査員{i+1}'] = inspector_name
                                team_members.append(inspector['氏名'])
                                
                                # 履歴を更新
                                code = inspector['コード']
                                if code not in self.inspector_product_hours:
                                    self.inspector_product_hours[code] = {}
                                self.inspector_product_hours[code][product_number] = (
                                    self.inspector_product_hours[code].get(product_number, 0.0) + divided_time
                                )
                        
                        # チーム情報を設定
                        if len(assigned_inspectors) > 1:
                            team_info = f"チーム: {', '.join(team_members)}"
                        else:
                            team_info = f"個人: {team_members[0] if team_members else ''}"
                        
                        result_df.at[original_index, 'チーム情報'] = team_info
                        self.log_message(f"未割当ロット再処理成功: 品番 {product_number}, 出荷予定日 {row['出荷予定日']}, {len(assigned_inspectors)}人割り当て")
                    else:
                        self.log_message(f"警告: 未割当ロット {product_number} の再処理に失敗しました（ルール違反を避けるため未割当のまま）")
                
                # 履歴を再計算（再処理後の状態を反映）
                self.inspector_daily_assignments = {}
                self.inspector_work_hours = {}
                self.inspector_product_hours = {}
                
                current_date_temp = pd.Timestamp.now().date()
                for index, row in result_df.iterrows():
                    product_number = row['品番']
                    divided_time = row.get('分割検査時間', 0.0)
                    
                    for i in range(1, 6):
                        inspector_col = f'検査員{i}'
                        if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                            inspector_name = str(row[inspector_col]).strip()
                            if '(' in inspector_name:
                                inspector_name = inspector_name.split('(')[0].strip()
                            if not inspector_name:
                                continue
                            
                            inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                            if not inspector_info.empty:
                                inspector_code = inspector_info.iloc[0]['#ID']
                                
                                if inspector_code not in self.inspector_daily_assignments:
                                    self.inspector_daily_assignments[inspector_code] = {}
                                if current_date_temp not in self.inspector_daily_assignments[inspector_code]:
                                    self.inspector_daily_assignments[inspector_code][current_date_temp] = 0.0
                                if inspector_code not in self.inspector_work_hours:
                                    self.inspector_work_hours[inspector_code] = 0.0
                                if inspector_code not in self.inspector_product_hours:
                                    self.inspector_product_hours[inspector_code] = {}
                                if product_number not in self.inspector_product_hours[inspector_code]:
                                    self.inspector_product_hours[inspector_code][product_number] = 0.0
                                
                                self.inspector_daily_assignments[inspector_code][current_date_temp] += divided_time
                                self.inspector_work_hours[inspector_code] += divided_time
                                self.inspector_product_hours[inspector_code][product_number] += divided_time
                
                self.log_message("未割当ロットの再処理が完了しました")
            else:
                self.log_message("未割当ロットはありませんでした")
            
            # フェーズ3.5: 未割当ロット再処理後の最終検証（勤務時間超過の再チェック）
            self.log_message("全体最適化フェーズ3.5: 未割当ロット再処理後の最終検証を開始")
            
            # 履歴を再計算
            self.inspector_daily_assignments = {}
            self.inspector_work_hours = {}
            self.inspector_product_hours = {}
            
            for index, row in result_df.iterrows():
                product_number = row['品番']
                divided_time = row.get('分割検査時間', 0.0)
                
                for i in range(1, 6):
                    inspector_col = f'検査員{i}'
                    if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                        inspector_name = str(row[inspector_col]).strip()
                        if '(' in inspector_name:
                            inspector_name = inspector_name.split('(')[0].strip()
                        if not inspector_name:
                            continue
                        
                        inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                        if not inspector_info.empty:
                            inspector_code = inspector_info.iloc[0]['#ID']
                            
                            if inspector_code not in self.inspector_daily_assignments:
                                self.inspector_daily_assignments[inspector_code] = {}
                            if current_date not in self.inspector_daily_assignments[inspector_code]:
                                self.inspector_daily_assignments[inspector_code][current_date] = 0.0
                            if inspector_code not in self.inspector_work_hours:
                                self.inspector_work_hours[inspector_code] = 0.0
                            if inspector_code not in self.inspector_product_hours:
                                self.inspector_product_hours[inspector_code] = {}
                            if product_number not in self.inspector_product_hours[inspector_code]:
                                self.inspector_product_hours[inspector_code][product_number] = 0.0
                            
                            self.inspector_daily_assignments[inspector_code][current_date] += divided_time
                            self.inspector_work_hours[inspector_code] += divided_time
                            self.inspector_product_hours[inspector_code][product_number] += divided_time
            
            # 勤務時間超過を再チェック
            phase3_5_violations = []
            for index, row in result_df.iterrows():
                product_number = row['品番']
                divided_time = row.get('分割検査時間', 0.0)
                
                for i in range(1, 6):
                    inspector_col = f'検査員{i}'
                    if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                        inspector_name = str(row[inspector_col]).strip()
                        if '(' in inspector_name:
                            inspector_name = inspector_name.split('(')[0].strip()
                        if not inspector_name:
                            continue
                        
                        inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                        if not inspector_info.empty:
                            inspector_code = inspector_info.iloc[0]['#ID']
                            daily_hours = self.inspector_daily_assignments.get(inspector_code, {}).get(current_date, 0.0)
                            max_hours = inspector_max_hours.get(inspector_code, 8.0)
                            product_hours = self.inspector_product_hours.get(inspector_code, {}).get(product_number, 0.0)
                            
                            # 勤務時間超過をチェック（厳密に）
                            if daily_hours > max_hours - 0.05:
                                phase3_5_violations.append((index, inspector_code, inspector_name, "勤務時間超過", daily_hours, max_hours))
                                self.log_message(f"❌ フェーズ3.5検証: 勤務時間超過が検出されました - 検査員 '{inspector_name}' {daily_hours:.1f}h > {max_hours:.1f}h (ロット {index})")
                            
                            # 同一品番4時間超過をチェック
                            if product_hours > 4.0:
                                phase3_5_violations.append((index, inspector_code, inspector_name, "同一品番4時間超過", product_hours, 4.0))
                                self.log_message(f"❌ フェーズ3.5検証: 同一品番4時間超過が検出されました - 検査員 '{inspector_name}' 品番 {product_number} {product_hours:.1f}h > 4.0h (ロット {index})")
            
            if phase3_5_violations:
                self.log_message(f"⚠️ 警告: フェーズ3.5検証で {len(phase3_5_violations)}件の違反が検出されました。該当ロットを未割当にします")
                for violation in phase3_5_violations:
                    index = violation[0]
                    self.clear_assignment(result_df, index)
                    self.log_message(f"⚠️ ロットインデックス {index} を未割当にしました（{violation[3]}）")
                
                # 未割当後の履歴を再計算
                self.inspector_daily_assignments = {}
                self.inspector_work_hours = {}
                self.inspector_product_hours = {}
                
                for index, row in result_df.iterrows():
                    product_number = row['品番']
                    divided_time = row.get('分割検査時間', 0.0)
                    
                    for i in range(1, 6):
                        inspector_col = f'検査員{i}'
                        if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                            inspector_name = str(row[inspector_col]).strip()
                            if '(' in inspector_name:
                                inspector_name = inspector_name.split('(')[0].strip()
                            if not inspector_name:
                                continue
                            
                            inspector_info = inspector_master_df[inspector_master_df['#氏名'] == inspector_name]
                            if not inspector_info.empty:
                                inspector_code = inspector_info.iloc[0]['#ID']
                                
                                if inspector_code not in self.inspector_daily_assignments:
                                    self.inspector_daily_assignments[inspector_code] = {}
                                if current_date not in self.inspector_daily_assignments[inspector_code]:
                                    self.inspector_daily_assignments[inspector_code][current_date] = 0.0
                                if inspector_code not in self.inspector_work_hours:
                                    self.inspector_work_hours[inspector_code] = 0.0
                                if inspector_code not in self.inspector_product_hours:
                                    self.inspector_product_hours[inspector_code] = {}
                                if product_number not in self.inspector_product_hours[inspector_code]:
                                    self.inspector_product_hours[inspector_code][product_number] = 0.0
                                
                                self.inspector_daily_assignments[inspector_code][current_date] += divided_time
                                self.inspector_work_hours[inspector_code] += divided_time
                                self.inspector_product_hours[inspector_code][product_number] += divided_time
            else:
                self.log_message("✅ フェーズ3.5検証: 全てのルール違反が解消されました")
            
            # 最適化後に全体のチーム情報を再計算（確実に一致させるため）
            self.log_message("全体最適化フェーズ4: チーム情報の再計算を開始")
            
            # 最終的に出荷予定日順にソート（最優先ルールの維持）
            result_df['出荷予定日'] = pd.to_datetime(result_df['出荷予定日'], errors='coerce')
            result_df = result_df.sort_values('出荷予定日', na_position='last').reset_index(drop=True)
            self.log_message("最終結果を出荷予定日の古い順でソートしました（最優先ルール）")
            
            for index, row in result_df.iterrows():
                self.update_team_info(result_df, index, inspector_master_df, show_skill_values)
            self.log_message("チーム情報の再計算が完了しました")
            
            self.log_message("全体最適化が完了しました")
            return result_df
            
        except Exception as e:
            error_msg = f"全体最適化中にエラーが発生しました: {str(e)}"
            self.log_message(error_msg)
            logger.error(error_msg, exc_info=True)
            return result_df
    
    def fix_single_violation(self, index, inspector_code, inspector_name, divided_time, product_number, inspection_time, inspector_col_num, result_df, inspector_master_df, skill_master_df, inspector_max_hours, current_date, show_skill_values):
        """単一の違反（勤務時間超過または同一品番4時間超過）を是正"""
        try:
            row = result_df.iloc[index]
            
            # このロットの他の検査員を確認
            current_inspectors = []
            for i in range(1, 6):
                inspector_col = f'検査員{i}'
                if pd.notna(row.get(inspector_col)) and str(row[inspector_col]).strip() != '':
                    inspector_name_check = str(row[inspector_col]).strip()
                    if '(' in inspector_name_check:
                        inspector_name_check = inspector_name_check.split('(')[0].strip()
                    current_inspectors.append((i, inspector_name_check))
            
            # 超過している検査員を一時的に外す
            if len(current_inspectors) > 1:
                # 複数人で分担している場合は、超過している検査員を外して別の人に置き換え
                removed_inspector = None
                for i, name in current_inspectors:
                    if name == inspector_name:
                        removed_inspector = (i, name)
                        break
                
                if removed_inspector:
                    # 代替検査員を探す
                    process_number = row.get('現在工程番号', '')
                    # スキルマスタに登録があるか確認
                    skill_rows = skill_master_df[skill_master_df.iloc[:, 0] == product_number]
                    available_inspectors = self.get_available_inspectors(
                        product_number, process_number, skill_master_df, inspector_master_df
                    )
                    # 新規品の場合は新製品チームも取得
                    if not available_inspectors and skill_rows.empty:
                        self.log_message(f"新規品 {product_number}: 新製品チームを取得します")
                        available_inspectors = self.get_new_product_team_inspectors(inspector_master_df)
                    
                    # 既に割り当てられている検査員を除外
                    current_codes = []
                    for _, name in current_inspectors:
                        if name != inspector_name:
                            info = inspector_master_df[inspector_master_df['#氏名'] == name]
                            if not info.empty:
                                current_codes.append(info.iloc[0]['#ID'])
                    
                    # 既に割り当てられている人以外で、勤務時間に余裕がある人を探す
                    replacement_candidates = []
                    for insp in available_inspectors:
                        if insp['コード'] not in current_codes:
                            code = insp['コード']
                            max_hours = inspector_max_hours.get(code, 8.0)
                            # 勤務時間チェック
                            if not self.check_work_hours_capacity(code, divided_time, max_hours, current_date):
                                continue
                            # 同一品番の4時間上限チェック
                            current_product_hours = self.inspector_product_hours.get(code, {}).get(product_number, 0.0)
                            if current_product_hours + divided_time > 4.0:
                                continue
                            total_hours = self.inspector_work_hours.get(code, 0.0)
                            replacement_candidates.append((total_hours, insp))
                    
                    if replacement_candidates:
                        # 最も総勤務時間が少ない人を選択
                        replacement_candidates.sort(key=lambda x: x[0])
                        replacement_inspector = replacement_candidates[0][1]
                        
                        # 置き換えを実行
                        i_col, _ = removed_inspector
                        if show_skill_values:
                            if replacement_inspector.get('is_new_team', False):
                                replacement_name = f"{replacement_inspector['氏名']}(新)"
                            else:
                                replacement_name = f"{replacement_inspector['氏名']}({replacement_inspector['スキル']})"
                        else:
                            if replacement_inspector.get('is_new_team', False):
                                replacement_name = f"{replacement_inspector['氏名']}(新)"
                            else:
                                replacement_name = replacement_inspector['氏名']
                        
                        result_df.at[index, f'検査員{i_col}'] = replacement_name
                        
                        # 履歴を更新（元の検査員から時間を引く）
                        old_code = inspector_code
                        old_daily = self.inspector_daily_assignments.get(old_code, {}).get(current_date, 0.0)
                        old_total = self.inspector_work_hours.get(old_code, 0.0)
                        self.inspector_daily_assignments[old_code][current_date] = max(0.0, old_daily - divided_time)
                        self.inspector_work_hours[old_code] = max(0.0, old_total - divided_time)
                        
                        # 品番別累計時間も更新
                        if old_code in self.inspector_product_hours:
                            if product_number in self.inspector_product_hours[old_code]:
                                self.inspector_product_hours[old_code][product_number] = max(0.0, self.inspector_product_hours[old_code][product_number] - divided_time)
                        
                        # 新しい検査員に時間を追加
                        new_code = replacement_inspector['コード']
                        if new_code not in self.inspector_daily_assignments:
                            self.inspector_daily_assignments[new_code] = {}
                        if current_date not in self.inspector_daily_assignments[new_code]:
                            self.inspector_daily_assignments[new_code][current_date] = 0.0
                        
                        self.inspector_daily_assignments[new_code][current_date] += divided_time
                        if new_code not in self.inspector_work_hours:
                            self.inspector_work_hours[new_code] = 0.0
                        self.inspector_work_hours[new_code] += divided_time
                        
                        # 品番別累計時間も更新
                        if new_code not in self.inspector_product_hours:
                            self.inspector_product_hours[new_code] = {}
                        self.inspector_product_hours[new_code][product_number] = (
                            self.inspector_product_hours[new_code].get(product_number, 0.0) + divided_time
                        )
                        
                        # チーム情報を更新
                        self.update_team_info(result_df, index, inspector_master_df, show_skill_values)
                        
                        self.log_message(f"置き換え: '{inspector_name}' → '{replacement_inspector['氏名']}' (品番: {product_number}, 出荷予定日: {row['出荷予定日']})")
                        return True
            
            elif len(current_inspectors) == 1:
                # 1人だけの場合、増員するか他の人に置き換え
                # ただし、検査時間が3時間未満の場合は増員しない（1人で対応すべき）
                if inspection_time < 3.0:
                    # 置き換え処理（増員ではなく）
                    process_number = row.get('現在工程番号', '')
                    # スキルマスタに登録があるか確認
                    skill_rows = skill_master_df[skill_master_df.iloc[:, 0] == product_number]
                    available_inspectors = self.get_available_inspectors(
                        product_number, process_number, skill_master_df, inspector_master_df
                    )
                    # 新規品の場合は新製品チームも取得
                    if not available_inspectors and skill_rows.empty:
                        self.log_message(f"新規品 {product_number}: 置き換え用に新製品チームを取得します")
                        available_inspectors = self.get_new_product_team_inspectors(inspector_master_df)
                    
                    # 既に割り当てられている検査員を除外
                    current_codes = [inspector_code]
                    
                    # 置き換え候補を探す（同一品番の4時間上限チェックも含む）
                    replacement_candidates = []
                    for insp in available_inspectors:
                        if insp['コード'] not in current_codes:
                            code = insp['コード']
                            max_hours = inspector_max_hours.get(code, 8.0)
                            # 勤務時間チェック
                            if not self.check_work_hours_capacity(code, inspection_time, max_hours, current_date):
                                continue
                            # 同一品番の4時間上限チェック
                            current_product_hours = self.inspector_product_hours.get(code, {}).get(product_number, 0.0)
                            if current_product_hours + inspection_time > 4.0:
                                continue
                            total_hours = self.inspector_work_hours.get(code, 0.0)
                            replacement_candidates.append((total_hours, insp))
                    
                    if replacement_candidates:
                        # 最も総勤務時間が少ない人に置き換え
                        replacement_candidates.sort(key=lambda x: x[0])
                        replacement_inspector = replacement_candidates[0][1]
                        
                        if show_skill_values:
                            if replacement_inspector.get('is_new_team', False):
                                replacement_name = f"{replacement_inspector['氏名']}(新)"
                            else:
                                replacement_name = f"{replacement_inspector['氏名']}({replacement_inspector['スキル']})"
                        else:
                            if replacement_inspector.get('is_new_team', False):
                                replacement_name = f"{replacement_inspector['氏名']}(新)"
                            else:
                                replacement_name = replacement_inspector['氏名']
                        
                        result_df.at[index, '検査員1'] = replacement_name
                        
                        # 履歴を更新（元の検査員から時間を引く）
                        old_code = inspector_code
                        old_daily = self.inspector_daily_assignments.get(old_code, {}).get(current_date, 0.0)
                        old_total = self.inspector_work_hours.get(old_code, 0.0)
                        self.inspector_daily_assignments[old_code][current_date] = max(0.0, old_daily - inspection_time)
                        self.inspector_work_hours[old_code] = max(0.0, old_total - inspection_time)
                        
                        # 品番別累計時間も更新
                        if old_code in self.inspector_product_hours:
                            if product_number in self.inspector_product_hours[old_code]:
                                self.inspector_product_hours[old_code][product_number] = max(0.0, self.inspector_product_hours[old_code][product_number] - inspection_time)
                        
                        # 新しい検査員に時間を追加
                        new_code = replacement_inspector['コード']
                        if new_code not in self.inspector_daily_assignments:
                            self.inspector_daily_assignments[new_code] = {}
                        if current_date not in self.inspector_daily_assignments[new_code]:
                            self.inspector_daily_assignments[new_code][current_date] = 0.0
                        
                        self.inspector_daily_assignments[new_code][current_date] += inspection_time
                        if new_code not in self.inspector_work_hours:
                            self.inspector_work_hours[new_code] = 0.0
                        self.inspector_work_hours[new_code] += inspection_time
                        
                        # 品番別累計時間も更新
                        if new_code not in self.inspector_product_hours:
                            self.inspector_product_hours[new_code] = {}
                        self.inspector_product_hours[new_code][product_number] = (
                            self.inspector_product_hours[new_code].get(product_number, 0.0) + inspection_time
                        )
                        
                        result_df.at[index, '分割検査時間'] = round(inspection_time, 1)
                        
                        # チーム情報を更新
                        self.update_team_info(result_df, index, inspector_master_df, show_skill_values)
                        
                        self.log_message(f"置き換え: '{inspector_name}' → '{replacement_inspector['氏名']}' (品番: {product_number}, 検査時間: {inspection_time:.1f}h, 出荷予定日: {row['出荷予定日']})")
                        return True
                else:
                    # 検査時間が3時間以上の場合は増員を試みる
                    process_number = row.get('現在工程番号', '')
                    # スキルマスタに登録があるか確認
                    skill_rows = skill_master_df[skill_master_df.iloc[:, 0] == product_number]
                    available_inspectors = self.get_available_inspectors(
                        product_number, process_number, skill_master_df, inspector_master_df
                    )
                    # 新規品の場合は新製品チームも取得
                    if not available_inspectors and skill_rows.empty:
                        self.log_message(f"新規品 {product_number}: 増員用に新製品チームを取得します")
                        available_inspectors = self.get_new_product_team_inspectors(inspector_master_df)
                    
                    # 既に割り当てられている検査員を除外
                    current_codes = [inspector_code]
                    
                    # 追加できる検査員を探す
                    addition_candidates = []
                    for insp in available_inspectors:
                        if insp['コード'] not in current_codes:
                            code = insp['コード']
                            max_hours = inspector_max_hours.get(code, 8.0)
                            if not self.check_work_hours_capacity(code, divided_time, max_hours, current_date):
                                continue
                            # 同一品番の4時間上限チェック
                            current_product_hours = self.inspector_product_hours.get(code, {}).get(product_number, 0.0)
                            if current_product_hours + divided_time > 4.0:
                                continue
                            total_hours = self.inspector_work_hours.get(code, 0.0)
                            addition_candidates.append((total_hours, insp))
                    
                    if addition_candidates and len(current_inspectors) < 5:
                        # 最も総勤務時間が少ない人を追加
                        addition_candidates.sort(key=lambda x: x[0])
                        addition_inspector = addition_candidates[0][1]
                        new_count = len(current_inspectors) + 1
                        
                        if show_skill_values:
                            if addition_inspector.get('is_new_team', False):
                                addition_name = f"{addition_inspector['氏名']}(新)"
                            else:
                                addition_name = f"{addition_inspector['氏名']}({addition_inspector['スキル']})"
                        else:
                            if addition_inspector.get('is_new_team', False):
                                addition_name = f"{addition_inspector['氏名']}(新)"
                            else:
                                addition_name = addition_inspector['氏名']
                        
                        result_df.at[index, f'検査員{new_count}'] = addition_name
                        result_df.at[index, '検査員人数'] = new_count
                        
                        # 新しい検査員に時間を追加
                        new_code = addition_inspector['コード']
                        if new_code not in self.inspector_daily_assignments:
                            self.inspector_daily_assignments[new_code] = {}
                        if current_date not in self.inspector_daily_assignments[new_code]:
                            self.inspector_daily_assignments[new_code][current_date] = 0.0
                        
                        # 元の検査員の時間を半分に（2人で分担）
                        old_daily = self.inspector_daily_assignments.get(inspector_code, {}).get(current_date, 0.0)
                        self.inspector_daily_assignments[inspector_code][current_date] = old_daily - divided_time / 2
                        self.inspector_work_hours[inspector_code] = max(0.0, self.inspector_work_hours.get(inspector_code, 0.0) - divided_time / 2)
                        
                        # 品番別累計時間も更新
                        if inspector_code in self.inspector_product_hours:
                            if product_number in self.inspector_product_hours[inspector_code]:
                                self.inspector_product_hours[inspector_code][product_number] = max(0.0, self.inspector_product_hours[inspector_code][product_number] - divided_time / 2)
                        
                        # 新しい検査員に時間を追加
                        new_divided_time = divided_time / 2
                        self.inspector_daily_assignments[new_code][current_date] += new_divided_time
                        if new_code not in self.inspector_work_hours:
                            self.inspector_work_hours[new_code] = 0.0
                        self.inspector_work_hours[new_code] += new_divided_time
                        
                        # 品番別累計時間も更新
                        if new_code not in self.inspector_product_hours:
                            self.inspector_product_hours[new_code] = {}
                        self.inspector_product_hours[new_code][product_number] = (
                            self.inspector_product_hours[new_code].get(product_number, 0.0) + new_divided_time
                        )
                        
                        result_df.at[index, '分割検査時間'] = round(new_divided_time, 1)
                        
                        # チーム情報を更新
                        self.update_team_info(result_df, index, inspector_master_df, show_skill_values)
                        
                        self.log_message(f"増員: '{inspector_name}' に '{addition_inspector['氏名']}' を追加 (品番: {product_number}, 出荷予定日: {row['出荷予定日']})")
                        return True
            
            # 是正できなかった場合は、このロットを未割当にする
            self.log_message(f"⚠️ ルール違反を是正できませんでした。品番 {product_number} のロットを未割当にします")
            self.clear_assignment(result_df, index)
            return False
            
        except Exception as e:
            self.log_message(f"違反是正中にエラーが発生しました: {str(e)}")
            # エラー時も未割当にする
            try:
                self.clear_assignment(result_df, index)
            except:
                pass
            return False
    
    def clear_assignment(self, result_df, index):
        """ロットの割り当てをクリア（未割当にする）"""
        try:
            current_date = pd.Timestamp.now().date()
            row = result_df.iloc[index]
            product_number = row.get('品番', '')
            divided_time = row.get('分割検査時間', 0.0)
            
            # 履歴からこのロットの時間を引く（割り当てされている検査員の時間を戻す）
            for i in range(1, 6):
                inspector_col = f'検査員{i}'
                inspector_name = row.get(inspector_col, '')
                if pd.notna(inspector_name) and str(inspector_name).strip() != '':
                    inspector_name_str = str(inspector_name).strip()
                    if '(' in inspector_name_str:
                        inspector_name_str = inspector_name_str.split('(')[0].strip()
                    
                    # 検査員コードを取得して履歴から時間を引く
                    # 注意: この時点で履歴が既に再計算されている場合、時間を引く処理は不要
                    # 履歴の再計算は呼び出し元で行われる前提
            
            # 検査員1～5をクリア
            for i in range(1, 6):
                result_df.at[index, f'検査員{i}'] = ''
            result_df.at[index, '検査員人数'] = 0
            result_df.at[index, '分割検査時間'] = 0.0
            result_df.at[index, 'チーム情報'] = '未割当'
            
            self.log_message(f"ロットを未割当にしました: 品番 {product_number}, インデックス {index}")
            
        except Exception as e:
            self.log_message(f"未割当処理中にエラーが発生しました: {str(e)}")
    
    def check_work_hours_capacity(self, inspector_code, additional_hours, max_hours, current_date):
        """検査員の勤務時間に余裕があるかチェック"""
        try:
            daily_hours = self.inspector_daily_assignments.get(inspector_code, {}).get(current_date, 0.0)
            # 0.05時間（3分）の余裕を持たせる
            return daily_hours + additional_hours <= max_hours - 0.05
        except:
            return False
    
    def update_team_info(self, result_df, index, inspector_master_df, show_skill_values=False):
        """チーム情報を更新（最適化後に呼び出す）"""
        try:
            team_members = []
            
            # 現在の検査員を取得（検査員1～5）
            for i in range(1, 6):
                inspector_col = f'検査員{i}'
                inspector_name = result_df.at[index, inspector_col]
                
                if pd.notna(inspector_name) and str(inspector_name).strip() != '':
                    # スキル値や(新)を除去して実名を取得
                    actual_name = str(inspector_name)
                    if '(' in actual_name:
                        actual_name = actual_name.split('(')[0].strip()
                    
                    team_members.append(actual_name)
            
            # チーム情報を設定
            if len(team_members) > 1:
                team_info = f"チーム: {', '.join(team_members)}"
            elif len(team_members) == 1:
                team_info = f"個人: {team_members[0]}"
            else:
                team_info = ""
            
            result_df.at[index, 'チーム情報'] = team_info
            return team_info
            
        except Exception as e:
            self.log_message(f"チーム情報更新中にエラーが発生しました: {str(e)}")
            return ""
    
    def reset_assignment_history(self):
        """割り当て履歴をリセット"""
        self.inspector_assignment_count = {}
        self.inspector_last_assignment = {}
        self.inspector_work_hours = {}
        self.inspector_daily_assignments = {}
        self.inspector_product_hours = {}
        self.log_message("検査員割り当て履歴と勤務時間をリセットしました")
