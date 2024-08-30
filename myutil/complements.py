import os, sys, subprocess, logging
from Classes import FileManagement

import json
import os
import re
import sys

import numpy as np
import pandas as pd


def phenotype_complement(
        input_file,
        output_prefix='table/RD4', # 将会在当前目录下，创建table文件夹，里面创建一系列RD4开头的csv文件
        num_cols_per_file=10
):

    if not input_file.endswith('.csv'):
        return '输入格式错误'

    json_str = {}
    path = os.path.abspath(os.path.dirname(output_prefix))
    if not os.path.exists(path):
        os.makedirs(path)

    header = pd.read_csv(input_file, nrows=0)

    header_list = header.columns.to_list()[1::]
    cols_first_appear = []
    last = ''
    # 用正则不太行，有一些第一批是这样的：f.12000.2.0
    # 记录第一批的col的索引
    for i in range(len(header_list)):
        now = header_list[i].split('.')[1]
        if not last == now:
            last = now
            cols_first_appear.append(i + 1)

    # 分批写入csv
    now_index = 1
    total_count = len(cols_first_appear)
    count = 0
    while now_index < total_count:
        # 写入第一列和依此往后10列，并读取
        cols_to_read = [0]
        end_index = min(now_index + num_cols_per_file - 1, total_count)
        for i in range(now_index, end_index + 1):
            cols_to_read.append(cols_first_appear[i - 1])
        df = pd.read_csv(input_file, usecols=cols_to_read, low_memory=False)

        # 替换NA——>-9
        for i in range(len(df.columns) - 1):
            df.iloc[:, i + 1] = df.iloc[:, i + 1].replace({np.nan: -9, '': -9})

        # 重命名f.eid为IID，新增FID，最前面，等于IID, 输出文件
        output_file = f"{output_prefix}_{count}.txt"
        df.rename(columns={df.columns[0]: 'IID'}, inplace=True)
        df.insert(0, 'FID', df['IID'])
        df.to_csv(output_file, sep=' ', index=False)

        title_list = []
        for title in cols_to_read[1::]:
            title = header.columns.values[title]
            title_list.append(title)
        json_str[output_file] = title_list

        # 更新起始列，以及文件计数
        now_index = end_index + 1
        count += 1

    # 结果写入文件，方便查看
    with open('phenomena.json', 'w') as f:
        f.write(json.dumps(json_str))

    print('再次处理文件，替换-9.0为-9')
    count = 0
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        with open(file_path, 'r') as f:
            content = f.read()
        with open(file_path + 'temp', 'w') as f:
            f.write(content.replace('-9.0', '-9'))
        os.remove(file_path)
        os.rename(file_path + 'temp', file_path)
        progress_bar(count, total_count // 10 + 1)
        count += 1


def gender_complement(
    fm: FileManagement,
    input_name: str,
    gender_info_path: str,
    gender_reference_path: str = "./myutil/gender_serial_reference.csv"
) -> str:
    """
    complement plink-format file with gender information.

    # Args:
    
    **fm**: FileManagement object
    
    **input_name (str)**: _input file **name**_
    
    **phenotype_info_path (str)**: _path to phenotype information file_
    
    **gender_reference_path (str)**: _path to gender reference file, which tells which sex a gender code refers to_
    
    # Returns:
    
    str: _complemented plink file name_
    """
    
    return ["complemented_file_name"]  # placeholder