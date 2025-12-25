# 外観検査振分支援システム

現在のバージョン: `v1.1.3`（`version.py` を参照）

このプロジェクトは Access の出荷検査残データとマスタ情報を組み合わせ、検査員のスキル・休暇・固定アサイン情報を考慮して公平な割当を自動化するWindows向けPythonアプリです。

## 特徴
- Access から抽出した出荷予定と不足数を pandas で前処理し、必要なロットを自動で洗い出します。
- 先行検査品・洗浄依頼・登録済み品番ごとの取り置きルールを適用したうえで、`InspectorAssignmentManager` が休暇・勤務時間・同一品番上限などの制約を満たす検査員を選びます。
- 結果は Excel/CSV/Google スプレッドシート形式で出力し、CustomTkinter を使ったUIで進捗・割当状況・休暇表・登録済み品番リストを確認できます。
- 割当ルールの概要は `inspector_assignment_rules_help.html`（メニューの「ガイド」から表示）で参照できます。

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
  ├── seat_ui.py               # 座席HTML生成（座席表UI）
  └── seat_ui_server.py        # 座席表UIのローカルHTTP配信・保存API
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
- `ACCESS_VBA_ENABLED`, `ACCESS_VBA_FILE_PATH`, `ACCESS_VBA_MACRO_NAME`: 抽出開始時にAccess側マクロを実行して不足集計テーブルを更新（任意）
- `REGISTERED_PRODUCTS_PATH`, `APP_SETTINGS_PATH`, `LOG_DIR_PATH`: 登録品番リスト、アプリ設定、ログ保存先

## 主要なファイル
- `app/ui/ui_handlers.py`: 抽出〜割当〜表示を統括。`ModernDataExtractorUI` が進捗表示と検査員割当結果の管理を担当します。
- `app/assignment/inspector_assignment_service.py`: 同一品番4時間上限・勤務時間・タブー制御を含むスコアベースの割当マネージャ。
- `app/services/cleaning_request_service.py`: Google Sheets と Access を組み合わせて洗浄および依頼ロットをバッチ取得。
- `app/seat_ui.py` / `app/seat_ui_server.py`: 座席表示HTMLを生成し、ローカルHTTPサーバ経由でブラウザから編集・保存できます。保存ボタンで `SEATING_JSON_PATH`（共有上の `seating_chart.json`）へ反映されます。
- 座席表で割当を整えた後はアプリに戻って「ロット振分変更反映」を押し、反映が完了したら「Googleスプレッドシートへ出力」で最新の割当結果を送ってください。

## パフォーマンス
- Access 接続は `DatabaseConfig` でキャッシュされ、クエリは必要な列だけを抽出してフェッチを最適化します。
- マスタやテーブル構造、ロットのキャッシュを適度に導入して同じボタン押下でのレスポンスを安定させています。

## バージョン管理方針
本アプリは Semantic Versioning に準拠し、以下の形式でバージョンを管理します。

vメジャー.マイナー.パッチ（例：v1.2.3）

- メジャー：大きな仕様変更（DB構造変更など）
- マイナー：機能追加・画面追加
- パッチ：バグ修正・軽微な調整

バージョン情報は `version.py` に一元管理し、exeファイル名にはバージョンを含めません（固定名のまま）。

## リリース手順
1. `version.py` の `APP_VERSION` を更新
2. `CHANGELOG.md` に変更内容を追記
3. exeをビルド（ファイル名は固定）
4. 配布・更新

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
- 座席表UIは `app/seat_ui_server.py` のローカルHTTPサーバ経由で表示・保存され、保存内容は `SEATING_JSON_PATH` に反映されます。
