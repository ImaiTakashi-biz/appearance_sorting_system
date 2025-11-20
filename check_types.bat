@echo off
chcp 65001 > nul
echo ========================================
echo 型チェック（mypy）
echo ========================================
echo.

REM mypyがインストールされているか確認
python -c "import mypy" 2>nul
if errorlevel 1 (
    echo mypyがインストールされていません。
    echo インストール中...
    pip install mypy
    if errorlevel 1 (
        echo mypyのインストールに失敗しました。
        pause
        exit /b 1
    )
)

echo.
echo 型チェックを実行中...
echo.

REM mypyで型チェックを実行
mypy app --config-file mypy.ini

if errorlevel 1 (
    echo.
    echo ========================================
    echo 型エラーが見つかりました
    echo ========================================
    pause
    exit /b 1
) else (
    echo.
    echo ========================================
    echo 型チェック完了（エラーなし）
    echo ========================================
    pause
    exit /b 0
)
