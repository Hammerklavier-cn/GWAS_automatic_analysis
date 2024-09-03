# GWAS_automatic_analysis

> Author: Hammerklavier

## 介绍  --  Introduction

自动化进行GWAS分析的脚本。

Script which is expected to perform GWAS wholly automatically.

## 运行环境  --  Environment

- Python 3.8+
- numpy, pandas, scipy, matplotlib, seaborn, openpyxl

## 环境配置  --  Environment Configuration

需要自行下载 `plink`，建议使用 v1.9 版本。

可以使用 `setup.bash` 脚本实现一键部署。该脚本会检查你的 python 版本（>= 3.8），生成一个名为 `.venv` 的虚拟环境，并安装必要的库。

You need to download `plink` executable yourself. v1.9 is recommended.

You can deploy the environment simply by running `./setup.bash`. It will check your python version (>= 3.8), generate a virtual environment named `.venv` and install necessary libraries.

## 运行 --  Run

`python3 main.py --your-parameter`

## 参数说明 --  Parameter

通过 `python3 main.py -h` 查看。

Usage to be seen by running `python3 main.py -h`.

## 功能测试 --  Test

所有的测试代码以及文件都在 `./testsuite` 中, 包括测试脚本（单元测试和全流程测试），测试用文件和测试结果的预期 SHA256 值。

测试将在 `./test/` 中运行，运行结束后会自动删除其中的文件。

## 异常退出序号  --  Exit Code

-3: 意料之外的错误。

-3: Unexpected error.

-2: 未能成功创建必要的链接。

-2: failed to create necessary links.

-1：程序设计缺陷。

-1: defect in design.

1: 输入文件格式不受支持。

- 注：原始输入文件应以 `.vcf`, `.vcf.gz`, `.bed` 或 `.ped` 结尾。
  ·
  1: Unsupported input file format.
- P.S. Input file is expected to end with `.vcf`, `.vcf.gz`, `.bed` or `.ped`.

2: plink 执行失败。

2: plink execution failed.

3: 文件读取失败

3: Failed to read file.

4: 文件内容查找失败

4: Failed to find certain contents of a file.

11: 功能尚未实现

## 注  --  Post Script

项目尚未完成，请不要使用。

The project is still unfinished and not for use now.

## 使用方法  --  Usage
