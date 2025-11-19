"""
PNGファイルをICO形式に変換するスクリプト
"""
from PIL import Image
from pathlib import Path

def convert_png_to_ico(png_path: str, ico_path: str = None):
    """
    PNGファイルをICO形式に変換
    
    Args:
        png_path: 変換元のPNGファイルパス
        ico_path: 出力先のICOファイルパス（Noneの場合は自動生成）
    """
    try:
        # PNGファイルを開く
        img = Image.open(png_path)
        
        # RGBAモードに変換（透明度を保持）
        if img.mode != 'RGBA':
            img = img.convert('RGBA')
        
        # 出力パスが指定されていない場合は自動生成
        if ico_path is None:
            png_file = Path(png_path)
            ico_path = str(png_file.with_suffix('.ico'))
        
        # 既存のICOファイルを削除（上書きを確実にするため）
        if Path(ico_path).exists():
            Path(ico_path).unlink()
        
        # ICO形式で保存（複数のサイズを含める）
        # Windowsでよく使われるサイズを指定（PyInstallerで確実に認識されるサイズ）
        sizes = [(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
        
        # ICO形式で保存（複数のサイズを含める）
        # PILのsaveメソッドが自動的に複数サイズを処理する
        img.save(ico_path, format='ICO', sizes=sizes)
        
        print(f'[OK] アイコンファイルを変換しました: {ico_path}')
        print(f'[INFO] 含まれるサイズ: {sizes}')
        return ico_path
    except Exception as e:
        print(f'[ERROR] エラー: {e}')
        raise

if __name__ == "__main__":
    png_file = "ChatGPT Image 2025年11月19日 13_13_22.png"
    ico_file = "appearance_sorting_system.ico"
    
    if Path(png_file).exists():
        convert_png_to_ico(png_file, ico_file)
    else:
        print(f'[ERROR] エラー: {png_file} が見つかりません')

