# GWAS_automatic_analysis

> Author: Hammerklavier

## 介绍 -- Introduction

自动化进行GWAS分析的脚本。

Script which is expected to perform GWAS wholly automatically.

## 运行环境 -- Environment

- Python 3.10+ required
- Python packages, seen in `requirements.txt`: numpy, pandas, polars, matplotlib, seaborn, openpyxl, ...

## 环境配置 -- Environment Configuration

需要自行下载 `plink`，建议使用 v1.9 版本。

可以使用 `setup.bash` 脚本实现一键部署。该脚本会检查你的 python 版本（>= 3.8），生成一个名为 `.venv` 的虚拟环境，并安装必要的库。

You need to download `plink` executable yourself. v1.9 is recommended.

You can deploy the environment simply by running `./setup.bash`. It will check your python version (>= 3.8), generate a virtual environment named `.venv` and install necessary libraries.

## 运行 -- Run

`python3 main.py --your-parameter`

## 参数说明 -- Parameter

通过 `python3 main.py -h` 查看。

Usage to be seen by running `python3 main.py -h`.

## 输出文件 -- Output Files

- 普通关联分析:
  - `summary-q.tsv`: 通过筛选后的所有 SNP 和**数量**性状的关联分析结果。
    - 默认列名：
      - _CHR_: SNP 的染色体号
      - _SNP_: SNP 名称
      - _BP_: SNP 在该染色体上的位点
      - _NMISS_: SNP 的非缺失样本数
      - _BETA_: SNP 和表型的回归系数
      - _SE_: SNP 和表型的回归系数的标准误
      - _R2_: 决定系数的平方
      - _T_: Wald test T 检验量
      - _P_: Wald test P 值
      - _P'_: Bonferroni 校正后 P 值
      - _gender_: 人群性别
      - _ethnic_: 人群种族
      - _phenotype_: 表型名称
      - _G11_: A1（通常为次等位基因）纯合子的表型均值
      - _G12_: A1、A2 杂合子的表型均值
      - _G22_: A2 （通常为主等位基因）纯合子的表型均值
    - 若启用 `--perm` 选项，在 _P'_ 和 _gender_ 之间会增加以下列：
      - _PERM_P_1_: Permutation Test 经验 p 值（点态），或更低 p 值的置换计数
      - _PERM_P_2_: Permutation Test 经验 p 值（max(T) 族系校正），或更低 p 值的置换计数
  - `summary-q-significant.tsv`: `summary-q.tsv` 中达到显著性水平的 SNP 和表型的关联分析结果，列名相同
    - 筛选条件：
      - 若指定 `--perm` 选项，保留 _perm_p_2_ $< \alpha$ 的行
      - 否则，保留 _P'_ < $\alpha$ 的行
        - 若指定 `--ld-correct`，$P' = \frac{P}{indep\_snp}$
        - 否则，$P' = \frac{P}{total\_snp}$
  - `summary-qt_means.tsv`: 各 SNP 各基因型的表型均值。每五行为一个 SNP，每一列是一个基因型。列名为：
    - _CHR_: SNP 所在的染色体
    - _SNP_: SNP 名称
    - _VALUE_: 后三列的值类型。取 'GENO', 'COUNTS', 'FREQ', 'MEAN' 或 'SD'
    - _G11_: A1（通常为次等位基因）纯合子的值
    - _G12_: A1、A2 杂合子的值
    - _G22_: A2 （通常为主等位基因）纯合子的值
    - _gender_: 人群性别
    - _ethnic_: 人群种族
    - _phenotype_: 表型名称
  - `summary-means-significant.tsv`: 筛选 `summary-means.tsv` 中达到显著性水平的 SNP

## 功能测试 -- Test

所有的测试代码以及文件都在 `./testsuite` 中, 包括测试脚本（单元测试和全流程测试），测试用文件和测试结果的预期 SHA256 值。

测试将在 `./test/` 中运行，运行结束后会自动删除其中的文件。

## 异常退出序号 -- Exit Code

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

## 注 -- Post Script

toolkit 内容正在开发中，部分组件尚未实现完全。

## 使用方法 -- Usage

### toolkit 小工具
