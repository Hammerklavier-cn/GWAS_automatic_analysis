from typing import Iterable
import sys

import sys

import sys

def progress_bar(message: str, total: int, current: int, bar_length: int = 50, fill: str = '█', printEnd: str = "\r"):
    current_percentage = current / total
    current_length = int(current_percentage * bar_length)
    # 使用正确的字符串拼接方式创建进度条
    bar = fill * current_length + " " * (bar_length - current_length)
    
    # 计算整个进度条的总长度
    total_length = len(f'|{bar}| {message} | ({current}/{total})')
    
    # 输出并确保字符串长度固定
    sys.stdout.write(f'\r|{bar}| {message.ljust(120)} | ({current}/{total})'.ljust(total_length))
    sys.stdout.flush()