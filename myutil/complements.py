import os, sys, subprocess, logging

from pandas import StringDtype
from Classes import FileManagement
from myutil.small_tools import ProgressBar
from deprecated.sphinx import deprecated

import json
import os
import re
import sys

import numpy as np
import pandas as pd


# 分割表格，并且只提取了所有表型第一次采样的数据
@deprecated(reason="Deprecated for it doesn't meet project's requirements", version="1.0")
def phenotype_complement(
        input_file,
        output_prefix='table/RD4', # 将会在当前目录下，创建table文件夹，里面创建一系列RD4开头的csv文件
        num_cols_per_file=10
):
    progress_bar = ProgressBar()

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

def extract_phenotype_info(
    input_name: str,
    pheno_info_path: str
) -> dict[str, str]:
    """
    Extract phenotype information from a file, 
    generate csvs, containing [FID, IID, phenotype_value].
    Return a dict of [phenotype_name, generated_csv_path]

    Assert phenotype name with format `f.[number].[number].[number]`,
    Only the smallest version of first number is used.

    Args:
        input_name (str):
            name of plink file (without extension).
        pheno_info_path (str):
            path to phenotype information file.
    Returns:
        dict (dict[str, str]) : 
            dict of [phenotype_name, generated_csv_path]
    Generate Files:
        %(input_name)s_%(f.*.*).txt"""
        
    generated_files: dict[str, str] = {}
    
    progress_bar = ProgressBar()

    # get header
    '''if not pheno_info_path.endswith('.csv'):
        logging.error("Input file should be a csv file.")
        sys.exit(1)'''
    headers = pd.read_csv(pheno_info_path, nrows=1, sep=r"\s+", header=None).iloc[0,:].to_list()
    logging.debug("headers: %s", headers)
    accepted_headers: list[str] = []
    count = 0
    iid_header = None
    for header in headers:
        # show progress bar
        count += 1
        progress_bar.print_progress(f"processing {header}", len(headers), count)
        
        if re.match(r'^f\.\d+\.\d+\.\d+$', header):
            logging.debug("%s is a valid phenotype name", header)
            for i in range(10):
                for j in range(10):
                    if re.match(rf'^f\.\d+\.{i}.{j}', header):
                        i_min = i; j_min = j
                        logging.debug("f.%d.%d.%d is accepted", i, j)
                        accepted_headers.append(header)
                        break
                    else:
                        logging.debug("f.%d.%d.%d is not accepted", i, j)
                if re.match(rf'^f\.\d+\.{i}.{j}', header):
                    logging.debug("f.%d.%d.%d is accepted", i, j)
                    break
        elif re.match(r'^f\.eid.*', header):
            logging.debug("%s is the header of IID column", header)
            iid_header = header
        else:
            logging.debug("%s is not a valid phenotype name", header)
    if not iid_header:
        logging.error("No IID column found")
        sys.exit(4)
    logging.info("Accepted phenotype names: %r", accepted_headers)

    fam_df = pd.read_csv(
        f"{input_name}.fam",
        usecols=[0,1], sep=r"\s+",
        dtype=pd.StringDtype()
    )
    fam_df.columns = pd.Index(["FID", "IID"])

    # load phenotype data and save them
    count = 0; print()
    for header in accepted_headers:
        count += 1
        progress_bar.print_progress(f"processing {header}", len(accepted_headers), count)
        pheno_df = pd.read_csv(
            pheno_info_path, 
            usecols=[iid_header, header], sep=r"\s+",
            dtype=pd.StringDtype()
        )
        # replace NA and other strings with -9
        pattern = r"^-*\d+\.?\d*$"
        pheno_df.loc[:, header] = pheno_df.loc[:, header].fillna("-9")
        for i in range(len(pheno_df)):
            if pheno_df.iloc[i, 1] == np.nan:
                pheno_df.iloc[i, 1] = "-9"
                logging.debug("%s is replaced with -9, for it is not available.", pheno_df.iloc[i, 1])
            elif re.match(pattern, pheno_df.iloc[i, 1]):
                logging.debug("%s is a valid phenotype number")
            elif re.match(r"nan?", pheno_df.iloc[i, 1], re.IGNORECASE):
                pheno_df.iloc[i, 1] = "-9"
                logging.debug("%s is replaced with -9, for it is not available.", pheno_df.iloc[i, 1])
            '''else:
                pheno_df.iloc[i, 1] = "-9"
                logging.debug("%s is replaced with -9, for it is not a valid number, may be a string.", pheno_df.iloc[i, 1])'''

        # merge phenotype data with .fam in order to complement FID.
        pheno_tosave = pd.merge(
            fam_df, pheno_df, 
            how="inner", left_on="IID", right_on=iid_header
        ).loc[:,["FID", "IID", header]]
        pheno_tosave.to_csv(
            f"{input_name}_{header}.txt",
            sep='\t', header=True, index=False
        )
        generated_files[header] = f"{input_name}_{header}.txt"

    return generated_files

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
    # Note: This function is merged in `devide pop by gender``
    return ["complemented_file_name"]  # placeholder