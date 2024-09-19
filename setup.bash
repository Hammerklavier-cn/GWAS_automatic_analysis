#!/bin/bash

# 获取 Python 3 的版本号
python_version=$(python3 -V 2>&1)

# 检查命令是否成功执行
if [[ $? -ne 0 ]]; then
    echo "Python 3 is not installed."
    exit 1
fi

# 提取版本号（例如：3.9.7）
version_number=$(echo "$python_version" | grep -o -E '[0-9]+\.[0-9]+\.[0-9]+')

# 检查版本号是否至少是 3.8
if [[ $version_number =~ ^3\.([8-9]|[1-9][0-9]) ]]; then
    echo "Python version is ${version_number}, which is 3.8 or higher."
else
    echo "Error: Your Python version (${version_number}) is lower than 3.8."
    exit 1
fi

# 检查是否安装了 pip
if ! command -v pip3 &> /dev/null; then
    echo "Error: pip3 is not installed."
    exit 1
fi

# 创建虚拟环境
python3 -m venv .venv
source .venv/bin/activate
echo "Virtual environment created and activated."

# 安装依赖项
pip install -i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple --upgrade pip
pip install -i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple--upgrade setuptools
echo "Installing dependencies..."
pip3 install -i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple -r requirements.txt
echo "Dependencies installed."
echo "Finished."