# 外観検査振分支援システム

外観検査の検査員割振りを自動化する支援システムです。Accessデータベースから出荷検査残データを抽出し、スキルマッチングと公平性を考慮した検査員割振りを自動実行します。

## 機能

### 🎯 主要機能
- **データ抽出**: Accessデータベースから出荷検査残データを抽出
- **検査員割振り**: スキルマッチングと公平性を考慮した自動割振り
- **ロット割り当て**: 不足数に基づくロット数の自動計算
- **Excel/Googleスプレッドシート出力**: 結果を複数形式で出力
- **休暇管理**: 休暇予定を考慮した割振り
- **当日検査品登録**: 当日検査対象品の登録機能
- **リアルタイム進捗表示**: プログレスバーとステータス表示
- **設定リロード**: 設定ファイルの再読み込み機能

### 🎨 UI機能
- **近未来的なデザイン**: ライトテーマとブルーテーマ
- **出荷予定日指定**: 開始日と終了日を指定してデータを抽出
- **出力形式選択**: CSV、Excel、Googleスプレッドシートでの出力
- **出力先フォルダ選択**: 任意のフォルダにデータを保存
- **リアルタイムログ**: 実行状況をリアルタイムで確認

## ファイル構成

```
appearance_sorting_system/
├── main.py                          # メインエントリーポイント
├── app/
│   ├── __init__.py
│   ├── config.py                    # 設定管理モジュール
│   ├── env_loader.py                # 環境変数読み込み
│   ├── ui/
│   │   ├── __init__.py
│   │   └── ui_handlers.py          # UI処理
│   ├── assignment/
│   │   ├── __init__.py
│   │   └── inspector_assignment_service.py  # 検査員割振りロジック
│   ├── export/
│   │   ├── __init__.py
│   │   ├── excel_exporter_service.py        # Excel出力
│   │   └── google_sheets_exporter_service.py  # Googleスプレッドシート出力
│   └── services/
│       ├── __init__.py
│       ├── cleaning_request_service.py      # 洗浄依頼処理
│       └── vacation_schedule_service.py     # 休暇予定処理
├── config.env                       # 環境変数設定ファイル
├── requirements.txt                 # 必要なライブラリ
├── build_exe.bat                    # exe化用バッチファイル
├── build.spec                       # PyInstaller設定ファイル
└── README.md                        # このファイル
```

## セットアップ

### 1. 仮想環境の構築

```bash
# 仮想環境を作成
python -m venv .venv

# 仮想環境をアクティベート（Windows）
.venv\Scripts\activate

# 必要なライブラリをインストール
pip install -r requirements.txt
```

### 2. 環境変数の設定

`config.env`ファイルで以下の設定を行ってください：

```env
# Accessファイル設定
ACCESS_FILE_PATH=\\192.168.1.200\共有\生産管理課\出荷検査\出荷不足Excel\出荷検査一覧(出荷不足のみ).accdb
ACCESS_TABLE_NAME=T_出荷検査残

# データベース接続設定
DB_DRIVER=Microsoft Access Driver (*.mdb, *.accdb)

# マスタファイル設定（NAS共有）
PRODUCT_MASTER_PATH=\\192.168.1.200\共有\dev_tools\外観検査振分支援システム\master\製品マスタ_auto.xlsx
INSPECTOR_MASTER_PATH=\\192.168.1.200\共有\dev_tools\外観検査振分支援システム\master\検査員マスタ.csv
SKILL_MASTER_PATH=\\192.168.1.200\共有\dev_tools\外観検査振分支援システム\master\スキルマスタ_auto.csv
INSPECTION_TARGET_CSV_PATH=\\192.168.1.200\共有\dev_tools\外観検査振分支援システム\master\検査対象.csv
PROCESS_MASTER_PATH=\\192.168.1.200\共有\dev_tools\外観検査振分支援システム\master\工程マスタ_auto.xlsx

# Googleスプレッドシート設定
GOOGLE_SHEETS_CREDENTIALS_PATH=aptest-384703-24764f69b34f.json
GOOGLE_SHEETS_URL=https://docs.google.com/spreadsheets/d/...
GOOGLE_SHEETS_URL_CLEANING=https://docs.google.com/spreadsheets/d/...
GOOGLE_SHEETS_URL_VACATION=https://docs.google.com/spreadsheets/d/...

# その他
REGISTERED_PRODUCTS_PATH=\\192.168.1.200\共有\dev_tools\外観検査振分支援システム\master\registered_products.json
LOG_DIR_PATH=\\192.168.1.200\共有\dev_tools\外観検査振分支援システム\logs
```

### 3. Access Driverの確認

Windows環境でAccessデータベースに接続するには、Microsoft Access Database Engineが必要です。

- **32bit版**: Microsoft Access Database Engine 2016 Redistributable (32bit)
- **64bit版**: Microsoft Access Database Engine 2016 Redistributable (64bit)

**注意**: Pythonのビット数と一致するドライバーをインストールしてください。

## 使用方法

### 開発環境での実行

```bash
# 仮想環境をアクティベート
.venv\Scripts\activate

# アプリケーションを実行
python main.py
```

### デスクトップアプリケーション（exe）として実行

#### 1. exeファイルの作成

```bash
# バッチファイルを使用（推奨）
build_exe.bat

# または手動で実行
.venv\Scripts\activate
pyinstaller build.spec
```

#### 2. exeファイルの実行

1. `dist`フォルダ内の「外観検査振分支援システム.exe」を実行
2. 同じフォルダに「config.env」ファイルを配置
3. config.envファイルでAccessファイルのパスとテーブル名を設定

## 実行結果

指定した出力フォルダに以下のファイルが生成されます：
- `出荷検査残_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSS.csv` - CSV形式のデータ
- `出荷検査残_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSS.xlsx` - Excel形式のデータ
- `ロット割当_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSS.xlsx` - ロット割当結果
- `検査員割振り_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSS.xlsx` - 検査員割振り結果

## パフォーマンス最適化

本システムは以下の最適化が実装されています：

- **データベース接続キャッシュ**: 接続を5分間キャッシュして再利用
- **テーブル構造キャッシュ**: テーブル構造を1時間キャッシュ
- **マスタデータキャッシュ**: マスタファイルを5分間キャッシュ
- **並列処理**: マスタファイルの並列読み込み
- **ログ出力の最適化**: 不要なログ出力を削減

## トラブルシューティング

### よくあるエラー

1. **"Microsoft Access Driver not found"**
   - Microsoft Access Database Engineをインストールしてください
   - Pythonのビット数と一致するドライバーをインストールしてください

2. **"Accessファイルが見つかりません"**
   - `config.env`の`ACCESS_FILE_PATH`が正しいか確認してください
   - NAS共有パスが正しくアクセスできるか確認してください

3. **"テーブルが見つかりません"**
   - `config.env`の`ACCESS_TABLE_NAME`が正しいか確認してください

4. **exeファイルが起動しない**
   - ウイルス対策ソフトがブロックしていないか確認
   - Windows Defenderの除外設定を追加
   - `config.env`ファイルがexeと同じフォルダにあるか確認

5. **Googleスプレッドシートへの接続エラー**
   - `GOOGLE_SHEETS_CREDENTIALS_PATH`が正しいか確認
   - 認証情報JSONファイルが存在するか確認
   - インターネット接続を確認

### ログの確認

アプリケーション内のログセクションで実行状況を確認できます。また、NAS共有のログディレクトリにもログファイルが保存されます。

## 技術仕様

- **UI フレームワーク**: CustomTkinter 5.2.2
- **データ処理**: pandas 2.1.4
- **データベース**: pyodbc 5.0.1
- **Excel出力**: openpyxl 3.1.2
- **Googleスプレッドシート**: gspread 5.12.0
- **exe化**: PyInstaller 6.3.0
- **Python**: 3.8以上

## 型チェック

本プロジェクトでは`mypy`を使用した型チェックをサポートしています。

### 型チェックの実行

```bash
# バッチファイルを使用（推奨）
check_types.bat

# または手動で実行
mypy app --config-file mypy.ini
```

### 型ヒントの追加状況

- ✅ 主要メソッドに型ヒントを追加済み
- ✅ 重複コードの共通化を実施
- ✅ 長い関数の分割を実施

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。
