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
                    'ロットID': row.get('生産ロットID', ''),
                    '数量': lot_quantity,
                    'ロット日': row.get('指示日', ''),
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
    
    def assign_inspectors(self, inspector_df, inspector_master_df, skill_master_df):
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
            
            # 各ロットに対して検査員を割り当て
            for index, row in result_df.iterrows():
                inspection_time = row['検査時間']
                product_number = row['品番']
                process_number = row.get('現在工程番号', '')
                
                # 必要な検査員人数を計算（3時間を超える場合は複数人）
                if inspection_time <= 3.0:
                    required_inspectors = 1
                else:
                    required_inspectors = max(2, int(inspection_time / 3.0) + 1)
                
                # 分割検査時間を計算
                divided_time = inspection_time / required_inspectors
                
                # スキルマスタから該当する品番と工程番号のスキル情報を取得
                available_inspectors = self.get_available_inspectors(
                    product_number, process_number, skill_master_df, inspector_master_df
                )
                
                if not available_inspectors:
                    self.log_message(f"品番 {product_number} の検査員が見つかりません")
                    # 新製品チームのメンバーを取得
                    available_inspectors = self.get_new_product_team_inspectors(inspector_master_df)
                    if not available_inspectors:
                        self.log_message(f"新製品チームのメンバーも見つからないため、スキップします")
                        continue
                
                # 検査員を割り当て
                assigned_inspectors = self.select_inspectors(
                    available_inspectors, required_inspectors, divided_time, inspector_master_df
                )
                
                # 結果を設定
                result_df.at[index, '検査員人数'] = len(assigned_inspectors)
                result_df.at[index, '分割検査時間'] = round(divided_time, 1)
                
                # 検査員名を設定
                for i, inspector in enumerate(assigned_inspectors):
                    if i < 5:  # 最大5人まで
                        result_df.at[index, f'検査員{i+1}'] = f"{inspector['氏名']}({inspector['スキル']})"
            
            self.log_message(f"検査員割り当てが完了しました: {len(result_df)}件")
            
            # 割り当て統計を表示
            self.print_assignment_statistics()
            
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
                    # 検査員コードとスキル値を表示
                    for i in range(2, min(10, len(row))):  # 最初の8列の検査員コードを表示
                        col_name = skill_master_df.columns[i]
                        skill_value = row.iloc[i]
                        if pd.notna(skill_value) and str(skill_value).strip() != '':
                            self.log_message(f"  {col_name}: {skill_value}")
            
            # 工程番号による絞り込み処理
            filtered_skill_rows = []
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
            for skill_row in filtered_skill_rows:
                # スキルマスタの列構造: 品番, 工程, V002, V004, V005, ..., Z040
                # 列2から列33までが検査員コード（V002からZ040まで）
                for i in range(2, min(34, len(skill_master_df.columns))):
                    col_name = skill_master_df.columns[i]
                    inspector_code = col_name
                    skill_value = skill_row.iloc[i]  # ilocを使用してインデックスでアクセス
                    
                    # スキル値が1, 2, 3のいずれかで、かつ空でない場合
                    if pd.notna(skill_value) and str(skill_value).strip() != '' and str(skill_value).strip() in ['1', '2', '3']:
                        # 検査員マスタから該当する検査員の情報を取得
                        # 検査員コード（V002, V004等）で検索
                        inspector_info = inspector_master_df[inspector_master_df['#ID'] == inspector_code]
                        if not inspector_info.empty:
                            inspector_data = inspector_info.iloc[0]
                            available_inspectors.append({
                                '氏名': inspector_data['#氏名'],
                                'スキル': int(str(skill_value).strip()),
                                '就業時間': inspector_data['開始時刻'],
                                'コード': inspector_code
                            })
                            self.log_message(f"検査員 '{inspector_data['#氏名']}' (コード: {inspector_code}, スキル: {skill_value}) を追加")
                        else:
                            self.log_message(f"検査員コード '{inspector_code}' が検査員マスタに見つかりません")
            
            self.log_message(f"利用可能な検査員: {len(available_inspectors)}人")
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
                new_product_team_inspectors.append({
                    '氏名': inspector_row['#氏名'],
                    'スキル': 2,  # 新製品チームは中スキルとして扱う
                    '就業時間': inspector_row['開始時刻'],
                    'コード': inspector_row['#ID']
                })
                self.log_message(f"新製品チームメンバー '{inspector_row['#氏名']}' (コード: {inspector_row['#ID']}) を追加")
            
            self.log_message(f"新製品チームメンバー: {len(new_product_team_inspectors)}人")
            return new_product_team_inspectors
            
        except Exception as e:
            self.log_message(f"新製品チームメンバー取得中にエラーが発生しました: {str(e)}")
            return []
    
    def select_inspectors(self, available_inspectors, required_count, divided_time, inspector_master_df):
        """検査員を選択する（勤務時間考慮・公平な割り当て方式）"""
        try:
            if not available_inspectors:
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
            available_inspectors = self.filter_available_inspectors(available_inspectors, divided_time, inspector_master_df)
            
            if not available_inspectors:
                self.log_message("勤務時間内で利用可能な検査員がいません")
                return []
            
            # スキルレベル別に分類
            high_skill_inspectors = [insp for insp in available_inspectors if insp['スキル'] == 1]
            medium_skill_inspectors = [insp for insp in available_inspectors if insp['スキル'] == 2]
            low_skill_inspectors = [insp for insp in available_inspectors if insp['スキル'] == 3]
            
            # 各スキルレベル内で公平に選択する関数
            def select_fairly_from_group(group, count):
                if not group or count <= 0:
                    return []
                
                # 割り当て回数が少ない順、最後の割り当てが古い順でソート
                group_with_priority = []
                for insp in group:
                    code = insp['コード']
                    assignment_count = self.inspector_assignment_count.get(code, 0)
                    last_assignment = self.inspector_last_assignment.get(code, pd.Timestamp.min)
                    # 優先度: 割り当て回数が少ないほど高く、最後の割り当てが古いほど高い
                    priority = (-assignment_count, last_assignment)
                    group_with_priority.append((priority, insp))
                
                # 優先度順にソート
                group_with_priority.sort(key=lambda x: x[0])
                
                # 上位から選択
                selected = []
                for _, insp in group_with_priority[:count]:
                    selected.append(insp)
                    # 割り当て履歴を更新
                    code = insp['コード']
                    self.inspector_assignment_count[code] += 1
                    self.inspector_last_assignment[code] = current_time
                    # 勤務時間を更新
                    self.inspector_work_hours[code] += divided_time
                    self.inspector_daily_assignments[code][current_date] += divided_time
                
                return selected
            
            selected_inspectors = []
            
            if required_count == 1:
                # 1人の場合は、最も割り当て回数が少ない検査員を選択
                all_inspectors = high_skill_inspectors + medium_skill_inspectors + low_skill_inspectors
                if all_inspectors:
                    # 全員の中で最も割り当て回数が少ない人を選択
                    min_count = min(self.inspector_assignment_count.get(insp['コード'], 0) for insp in all_inspectors)
                    candidates = [insp for insp in all_inspectors 
                                if self.inspector_assignment_count.get(insp['コード'], 0) == min_count]
                    
                    # 同点の場合は最後の割り当てが最も古い人を選択
                    oldest_candidate = min(candidates, 
                                        key=lambda x: self.inspector_last_assignment.get(x['コード'], pd.Timestamp.min))
                    selected_inspectors.append(oldest_candidate)
                    
                    # 割り当て履歴を更新
                    code = oldest_candidate['コード']
                    self.inspector_assignment_count[code] += 1
                    self.inspector_last_assignment[code] = current_time
                    # 勤務時間を更新
                    self.inspector_work_hours[code] += divided_time
                    self.inspector_daily_assignments[code][current_date] += divided_time
            else:
                # 複数人の場合は、スキルバランスを考慮しつつ公平に割り当て
                # 高スキル者を優先しつつ、各スキルレベル内では公平に選択
                
                # 高スキル者がいる場合は、高スキル者を中心に割り当て
                if high_skill_inspectors:
                    high_count = min(len(high_skill_inspectors), required_count)
                    selected_inspectors.extend(select_fairly_from_group(high_skill_inspectors, high_count))
                    required_count -= high_count
                
                # 残りが必要な場合は中スキル者から選択
                if required_count > 0 and medium_skill_inspectors:
                    medium_count = min(len(medium_skill_inspectors), required_count)
                    selected_inspectors.extend(select_fairly_from_group(medium_skill_inspectors, medium_count))
                    required_count -= medium_count
                
                # まだ残りが必要な場合は低スキル者から選択
                if required_count > 0 and low_skill_inspectors:
                    low_count = min(len(low_skill_inspectors), required_count)
                    selected_inspectors.extend(select_fairly_from_group(low_skill_inspectors, low_count))
            
            # 選択された検査員の情報をログ出力
            for insp in selected_inspectors:
                code = insp['コード']
                count = self.inspector_assignment_count.get(code, 0)
                self.log_message(f"検査員 '{insp['氏名']}' (スキル: {insp['スキル']}, 割り当て回数: {count}) を選択")
            
            return selected_inspectors
            
        except Exception as e:
            self.log_message(f"検査員選択中にエラーが発生しました: {str(e)}")
            return []
    
    def filter_available_inspectors(self, available_inspectors, divided_time, inspector_master_df):
        """勤務時間を考慮して利用可能な検査員をフィルタリング"""
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
                            
                            # 休憩時間（12:15～13:00）を含む場合は1時間を差し引く
                            # 12:15 = 12.25時間、13:00 = 13.0時間
                            if start_hour <= 12.25 and end_hour >= 13.0:
                                max_daily_hours -= 1.0
                                self.log_message(f"検査員 '{inspector['氏名']}' は休憩時間を含むため、勤務時間から1時間を差し引きます (元: {end_hour - start_hour:.1f}h → 調整後: {max_daily_hours:.1f}h)")
                            
                        except:
                            # 計算に失敗した場合は8時間をデフォルトとする
                            max_daily_hours = 8.0
                    else:
                        # 時刻情報がない場合は8時間をデフォルトとする
                        max_daily_hours = 8.0
                else:
                    # 検査員マスタにない場合は8時間をデフォルトとする
                    max_daily_hours = 8.0
                
                # 勤務時間超過チェック
                if daily_hours + additional_hours <= max_daily_hours:
                    filtered_inspectors.append(inspector)
                    self.log_message(f"検査員 '{inspector['氏名']}' は利用可能 (今日の勤務時間: {daily_hours:.1f}h + {additional_hours:.1f}h = {daily_hours + additional_hours:.1f}h, 最大勤務時間: {max_daily_hours:.1f}h)")
                else:
                    self.log_message(f"検査員 '{inspector['氏名']}' は勤務時間超過のため除外 (今日の勤務時間: {daily_hours:.1f}h + {additional_hours:.1f}h = {daily_hours + additional_hours:.1f}h > {max_daily_hours:.1f}h)")
            
            return filtered_inspectors
            
        except Exception as e:
            self.log_message(f"検査員フィルタリング中にエラーが発生しました: {str(e)}")
            return available_inspectors
    
    def print_assignment_statistics(self):
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
                self.log_message(f"検査員 {inspector_code}: {count}回 (総勤務時間: {work_hours:.1f}h, 今日: {daily_hours:.1f}h)")
            
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
    
    def reset_assignment_history(self):
        """割り当て履歴をリセット"""
        self.inspector_assignment_count = {}
        self.inspector_last_assignment = {}
        self.inspector_work_hours = {}
        self.inspector_daily_assignments = {}
        self.log_message("検査員割り当て履歴と勤務時間をリセットしました")
