@echo off
chcp 65001 > nul
echo ========================================
echo 外観検査振分システム - EXEビルド
echo ========================================
echo.

REM PyInstallerがインストールされているか確認
python -c "import PyInstaller" 2>nul
if errorlevel 1 (
    echo PyInstallerがインストールされていません。
    echo インストール中...
    pip install pyinstaller
    if errorlevel 1 (
        echo PyInstallerのインストールに失敗しました。
        pause
        exit /b 1
    )
)

REM アイコンファイルの変換
echo.
echo アイコンファイルを変換中...
python convert_icon.py
if errorlevel 1 (
    echo 警告: アイコンファイルの変換に失敗しました。
    echo 既存のICOファイルを使用するか、PNGファイルを確認してください。
)

REM JSONファイルの存在確認
if not exist "aptest-384703-24764f69b34f.json" (
    echo 警告: aptest-384703-24764f69b34f.json が見つかりません。
    echo ビルドを続行しますが、Google Sheets機能が動作しない可能性があります。
)

REM specファイルを使用してビルド
echo.
echo ビルドを開始します...
pyinstaller build.spec --clean

if errorlevel 1 (
    echo.
    echo ビルドに失敗しました。
    pause
    exit /b 1
)

echo.
echo ========================================
echo ビルドが完了しました！
echo ========================================
echo.
echo 出力先: dist\外観検査振分支援システム.exe
echo.
echo 配布時の注意事項:
echo 1. dist\外観検査振分支援システム.exe を配布してください
echo 2. config.env はexeに埋め込まれています（別途配置不要）
echo 3. aptest-384703-24764f69b34f.json はexeに埋め込まれています
echo 4. logsフォルダは自動的に作成されます
echo.
pause

