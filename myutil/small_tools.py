import logging

def progress_bar(message: str, total: int, current: int, bar_length: int = 50, fill: str = '█', printEnd: str = "\r"):
    last_length = 0
    #while True:
    if True:
        # print(" " * last_length, end=printEnd)
        current_percentage = current / total
        current_length = int(current_percentage * bar_length)
        # 使用正确的字符串拼接方式创建进度条
        bar = fill * current_length + " " * (bar_length - current_length)
        
        # 计算整个进度条的总长度
        total_length = len(f'|{bar}| {message} | ({current}/{total})')
        
        # 输出并确保字符串长度固定
        print(f'\r|{bar}| {message.ljust(70)} | ({current}/{total})'.ljust(total_length), end=printEnd)
        last_length = total_length
        #yield

class ProgressBar:
    def __init__(self, printEnd="\r") -> None:
        self.printEnd = printEnd
        self.last_length = 0
        pass

    def print_progress(self, message: str, total: int, current: int, bar_length: int = 50, fill: str = '█'):
        self.clear_progress()
        current_percentage = current / total
        current_length = int(current_percentage * bar_length)
        # 使用正确的字符串拼接方式创建进度条
        bar = fill * current_length + " " * (bar_length - current_length)
        
        # 计算整个进度条的总长度
        total_length = len(f'|{bar}| {message} | ({current}/{total})')
        
        # 输出并确保字符串长度固定
        print(f'\r|{bar}| {message.ljust(70)} | ({current}/{total})'.ljust(total_length), end=self.printEnd)
        self.last_length = total_length
    
    def clear_progress(self):
        print(" " * self.last_length, end=self.printEnd)
    

def create_logger(
    name, level=logging.DEBUG, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
) -> logging.Logger:
    """
    Create a logger object with the given name and level.
    Args:
        name (str): The name of the logger.
        level (int): The logging level.
        format (str): The logging format.
    Returns:
        logging.Logger: The logger object.
    """
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(level)
        ch = logging.StreamHandler()
        formatter = logging.Formatter(format)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

def count_line(file_path: str) -> int:
    # get the number of independent SNPs
    with open(file_path) as reader:
        return sum([1 for _ in reader])

