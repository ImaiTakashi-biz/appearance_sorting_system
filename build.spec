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
        ('appearance_sorting_system.ico', '.'),  # アイコンファイルを埋め込む
    ],
    hiddenimports=[
        'pandas',
        'pyodbc',
        'openpyxl',
        'loguru',
        'customtkinter',
        'Pillow',
        'matplotlib',
        'gspread',
        'google.auth',
        'google.auth.oauthlib',
        'google.auth.httplib2',
        'google.oauth2',
        'google.oauth2.service_account',
        'googleapiclient',
        'googleapiclient.discovery',
        'googleapiclient.errors',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
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
    strip=False,
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

