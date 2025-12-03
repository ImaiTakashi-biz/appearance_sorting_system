# 外観検査振分支援システム

このプロジェクトは Access の出荷検査残データとマスタ情報を組み合わせ、検査員のスキル・休暇・固定アサイン情報を考慮して公平な割当を自動化するWindows向けPythonアプリです。

## 特徴
- Access から抽出した出荷予定と不足数を pandas で前処理し、必要なロットを自動で洗い出します。
- 先行検査品・洗浄依頼・登録済み品番ごとの取り置きルールを適用したうえで、`InspectorAssignmentManager` が休暇・勤務時間・同一品番上限などの制約を満たす検査員を選びます。
- 結果は Excel/CSV/Google スプレッドシート形式で出力し、CustomTkinter を使ったUIで進捗・割当状況・休暇表・登録済み品番リストを確認できます。
- `inspector_assignment_rules_help.html` を起動時に表示することで、誰でも同じルールを参照できるドキュメントを提供しています。

## 構成
```
main.py                        # 起動スクリプト（検証とUIの立ち上げ）
app/
  ├── ui/                      # CustomTkinter UI ロジックと座席ビュー生成
  ├── assignment/              # InspectorAssignmentManager による割当ロジック
  ├── services/                # 洗浄依頼や休暇情報の取得サービス
  ├── export/                  # Excel/Google Sheets 出力処理
  ├── config.py                # Access 接続と設定管理
  ├── config_manager.py        # JSON 設定値の読み書き（しきい値など）
  ├── env_loader.py            # config.env から環境変数ロード
  └── seat_ui.py               # 座席HTML生成とローカル保存ユーティリティ
```

## 環境構築
1. Python 3.10 以上の仮想環境を作成してアクティブ化
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. `config.env` に Access ファイルやマスタファイル、Google Sheets URL などを記載
3. 必要に応じて `app_settings.json` に `product_limit_hard_threshold` や `required_inspectors_threshold` を設定（ファイルが見つからない場合はデフォルト値を使用）
4. Microsoft Access Database Engine （Python と同じビット版）をインストール

## 実行方法
- 開発中: `.venv\Scripts\activate` してから `python main.py`
- 出力: メイン画面で開始日／終了日を指定して抽出・割当・エクスポートを実行
- exe 配布: `build_exe.bat` または `pyinstaller build.spec`

## 設定項目の主な内容（`config.env`）
- `ACCESS_FILE_PATH` / `ACCESS_TABLE_NAME`: Access ファイルとテーブル名
- `PRODUCT_MASTER_PATH`, `INSPECTOR_MASTER_PATH`, `SKILL_MASTER_PATH`, `PROCESS_MASTER_PATH`: マスタCSV/Excel のパス
- `GOOGLE_SHEETS_URL`, `GOOGLE_SHEETS_CREDENTIALS_PATH`: Google 出力用
- `GOOGLE_SHEETS_URL_CLEANING` / `GOOGLE_SHEETS_URL_CLEANING_INSTRUCTIONS`: 洗浄依頼（追加ロット）用
- `REGISTERED_PRODUCTS_PATH`, `APP_SETTINGS_PATH`, `LOG_DIR_PATH`: 登録品番リスト、アプリ設定、ログ保存先

## 主要なファイル
- `app/ui/ui_handlers.py`: 抽出〜割当〜表示を統括。`ModernDataExtractorUI` が進捗表示と検査員割当結果の管理を担当します。
- `app/assignment/inspector_assignment_service.py`: 同一品番4時間上限・勤務時間・タブー制御を含むスコアベースの割当マネージャ。
- `app/services/cleaning_request_service.py`: Google Sheets と Access を組み合わせて洗浄および依頼ロットをバッチ取得。
- `app/seat_ui.py`: 座席表示HTMLを生成し、File System Access API でJSON/HTMLを保存・更新します。
- `app/seat_ui.py` で生成される座席表では、初回だけ保存先をダイアログで指定し、2回目以降は選択済みの同じファイルに自動上書きされます。座席表で割当を整えた後はアプリに戻って「ロット振分変更反映」を押し、反映が完了したら「Googleスプレッドシートへ出力」で最新の割当結果を送ってください。

## パフォーマンス
- Access 接続は `DatabaseConfig` でキャッシュされ、クエリは必要な列だけを抽出してフェッチを最適化します。
- マスタやテーブル構造、ロットのキャッシュを適度に導入して同じボタン押下でのレスポンスを安定させています。

## トラブルシューティング
- 設定ファイルが読み込めない場合: `config.env` のパスを確認し、`APP_SETTINGS_PATH` も正しく指定されているか確認
- Access ドライバが見つからない場合: `Microsoft Access Driver (*.mdb, *.accdb)` をインストールし、`pyodbc.drivers()` で利用可能か確認
- Google Sheets 出力エラー: `GOOGLE_SHEETS_CREDENTIALS_PATH` のJSONとAPIの有効化、URLの共有設定を確認
- ログ: `LOG_DIR_PATH` に出力される `app_YYYYMMDD.log` を参照

## 開発・検証
- 型チェック: `check_types.bat` または `mypy app --config-file mypy.ini`
- UIヘルプ: `inspector_assignment_rules_help.html` をブラウザで開くと割当ルール・制約の要約が確認できます。

## 付録
- `inspector_assignment_rules_help.html`: 検査員割当ルールの説明（当日洗浄・先行検査の制約、固定検査員の扱い、緩和ポリシーなど）。
- `seat_ui.py` で生成される HTML を一度保存すれば File System Access API で以降自動上書きされます。
