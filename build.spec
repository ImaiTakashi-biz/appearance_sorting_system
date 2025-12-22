# -*- mode: python ; coding: utf-8 -*-
import os

block_cipher = None

# アイコンファイルの絶対パスを取得
icon_path = os.path.abspath('appearance_sorting_system.ico')

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('config.env', '.'),  # config.envをexeに埋め込む
        ('aptest-384703-24764f69b34f.json', '.'),  # Google認証情報JSONファイルを埋め込む
        ('araichat_room_config.json', '.'),  # ARAICHATルーム設定ファイルを埋め込む
        ('appearance_sorting_system.ico', '.'),  # アイコンファイルを埋め込む
        ('inspector_assignment_rules_help.html', '.'),  # ヘルプHTMLファイルを埋め込む
    ],
    hiddenimports=[
        'pandas',
        'pyodbc',
        'openpyxl',
        'loguru',
        'customtkinter',
        'PIL',
        'gspread',
        'google.auth',
        'google.oauth2',
        'google.oauth2.service_account',
        'googleapiclient',
        'googleapiclient.discovery',
        'googleapiclient.errors',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'matplotlib',
        'scipy',
        'IPython',
        'jupyter',
        'notebook',
        'pytest',
        'setuptools',
        'distutils',
        'tkinter.test',
        'unittest',
        # pyarrow関連（pandasの依存だが実際には使用していない - 大幅なサイズ削減）
        'pyarrow',
        'pyarrow.dataset',
        'pyarrow.parquet',
        'pyarrow.flight',
        'pyarrow.compute',
        # pandas/numpyのテストモジュール（pandas.plottingとpandas.io.formats.styleはpandasの内部で使用される可能性があるため除外しない）
        'pandas.tests',
        'numpy.tests',
        'numpy.f2py',
        # PILの不要なモジュール（基本的な画像読み込みのみ使用）
        'PIL.tests',
        'PIL.ImageTk',
        'PIL.ImageQt',
        'PIL.ImageShow',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='外観検査振分支援システム',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,  # Windowsではstripコマンドが利用できないためFalse
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # コンソールウィンドウを非表示
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=icon_path,  # ICOファイルを使用（絶対パス）
    onefile=True,  # onefileモード
)
