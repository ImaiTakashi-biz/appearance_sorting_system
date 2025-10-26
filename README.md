# 出荷検査データ抽出システム

Accessデータベースから出荷検査残データを抽出する美しいUIアプリケーションです。

## 機能

### 🎨 美しいUIアプリケーション
- **近未来的なデザイン**: ダークテーマとサイバーパンク風のカラーパレット
- **出荷予定日指定**: 開始日と終了日を指定してデータを抽出
- **リアルタイム進捗表示**: プログレスバーとステータス表示
- **出力形式選択**: CSV、Excel、または両方での出力
- **出力先フォルダ選択**: 任意のフォルダにデータを保存
- **リアルタイムログ**: 実行状況をリアルタイムで確認
- **設定リロード**: 設定ファイルの再読み込み機能

## ファイル構成

```
appearance_sorting_system/
├── main.py                       # メインアプリケーション
├── config.py                     # 設定管理モジュール
├── config.env                    # 環境変数設定ファイル
├── requirements.txt              # 必要なライブラリ
├── build_exe.py                  # exe化用スクリプト
├── build.bat                     # exe化用バッチファイル
└── README.md                     # このファイル
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
ACCESS_FILE_PATH=C:\Users\SEIZOU-20\Desktop\振分システム情報\出荷検査一覧(出荷不足のみ).accdb
ACCESS_TABLE_NAME=T_出荷検査残

# データベース接続設定
DB_DRIVER=Microsoft Access Driver (*.mdb, *.accdb)
```

### 3. Access Driverの確認

Windows環境でAccessデータベースに接続するには、Microsoft Access Database Engineが必要です。

- **32bit版**: Microsoft Access Database Engine 2016 Redistributable (32bit)
- **64bit版**: Microsoft Access Database Engine 2016 Redistributable (64bit)

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
build.bat

# または手動で実行
.venv\Scripts\activate
python build_exe.py
```

#### 2. exeファイルの実行

1. `dist`フォルダ内の「出荷検査データ抽出システム.exe」を実行
2. 同じフォルダに「config.env」ファイルを配置
3. config.envファイルでAccessファイルのパスとテーブル名を設定

## 実行結果

指定した出力フォルダに以下のファイルが生成されます：
- `出荷検査残_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSS.csv` - CSV形式のデータ
- `出荷検査残_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSS.xlsx` - Excel形式のデータ

## カスタマイズ

### カスタムクエリの実行

`main.py`の`extract_data_thread`メソッド内で、以下のようにカスタムクエリを指定できます：

```python
# 例：特定の条件でデータを抽出
query = f"""
SELECT * FROM [{self.config.access_table_name}] 
WHERE 出荷予定日 >= #{start_date}# 
AND 出荷予定日 <= #{end_date}#
AND ステータス = '未処理'
ORDER BY 出荷予定日
"""
```

### 出力形式の変更

CSV・Excel以外の形式で出力したい場合は、`extract_data_thread`メソッドに新しい保存処理を追加してください。

## exe化について

### ビルドオプション

- **--onefile**: 単一のexeファイルとして出力
- **--windowed**: コンソールウィンドウを非表示
- **--add-data**: 設定ファイルを含める
- **--hidden-import**: 必要なライブラリを明示的に指定

### 配布時の注意点

1. **config.envファイル**: exeファイルと同じフォルダに配置
2. **Access Driver**: 実行環境にMicrosoft Access Database Engineをインストール
3. **ファイルサイズ**: 約50-100MB程度のexeファイルが生成されます

## トラブルシューティング

### よくあるエラー

1. **"Microsoft Access Driver not found"**
   - Microsoft Access Database Engineをインストールしてください

2. **"Accessファイルが見つかりません"**
   - `config.env`の`ACCESS_FILE_PATH`が正しいか確認してください

3. **"テーブルが見つかりません"**
   - `config.env`の`ACCESS_TABLE_NAME`が正しいか確認してください

4. **exeファイルが起動しない**
   - ウイルス対策ソフトがブロックしていないか確認
   - Windows Defenderの除外設定を追加

### ログの確認

アプリケーション内のログセクションで実行状況を確認できます。

## 技術仕様

- **UI フレームワーク**: CustomTkinter 5.2.2
- **データ処理**: pandas 2.1.4
- **データベース**: pyodbc 5.0.1
- **exe化**: PyInstaller 6.3.0
- **Python**: 3.8以上

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。