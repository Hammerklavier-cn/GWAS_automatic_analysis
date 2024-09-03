from typing import Iterable

def progress_bar(message: str, total: int, current: int, bar_length: int = 50, fill: str = '█', printEnd: str = "\r"):
    current_percentage = current / total
    current_length = int(current_percentage * bar_length)
    # 使用正确的字符串拼接方式创建进度条
    bar = fill * current_length + " " * (bar_length - current_length)
    print(f'\r|{bar}| {message} | ({current}/{total})', end=printEnd)