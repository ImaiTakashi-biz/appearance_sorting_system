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
        
        # 出力パスが指定されていない場合は自動生成
        if ico_path is None:
            png_file = Path(png_path)
            ico_path = str(png_file.with_suffix('.ico'))
        
        # ICO形式で保存（複数のサイズを含める）
        # Windowsでよく使われるサイズを指定
        sizes = [(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
        img.save(ico_path, format='ICO', sizes=sizes)
        
        print(f'[OK] アイコンファイルを変換しました: {ico_path}')
        return ico_path
    except Exception as e:
        print(f'[ERROR] エラー: {e}')
        raise

if __name__ == "__main__":
    png_file = "ChatGPT Image 2025年11月13日 16_05_27.png"
    ico_file = "appearance_sorting_system.ico"
    
    if Path(png_file).exists():
        convert_png_to_ico(png_file, ico_file)
    else:
        print(f'[ERROR] エラー: {png_file} が見つかりません')

