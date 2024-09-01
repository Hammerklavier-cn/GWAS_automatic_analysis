import re
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
from threading import Thread
import logging, os, sys
from multiprocessing import Process
from Classes import FileManagement

def divide_pop_by_ethnic(
        fm: FileManagement,
        input_name: str, 
        ethnic_info_path: str, 
        reference_path: str = "./myutil/ethnic_serial_reference.tsv"
    ) -> list[list[str]]:
    """
    Divide population by ethnicity.
    
    Parameters
    input_name: str
        The name of input plink files (without extension).
    ethnic_info_path: str
        The path of ethnic information file.
    reference_path: str
        The path of ethnic reference file.
    
    Returns
    -------
    list[list[str, str]]
        [`ethnic_name`, `file_path`].
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s -- %(levelname)s -- %(message)s")
    # read files
    try:
        eth_ref = pd.read_csv(reference_path, sep="\t", dtype=pd.StringDtype())
        if ethnic_info_path.endswith(".tsv") or ethnic_info_path.endswith("csv"):
            eth_info = pd.read_csv(ethnic_info_path, sep=r"\s+")
        elif ethnic_info_path.endswith(".xlsx") or ethnic_info_path.endswith(".xls"):
            eth_info = pd.read_excel(ethnic_info_path, dtype=pd.StringDtype())
        else:
            logging.error("Unsupported file format: %s", os.path.splitext(ethnic_info_path)[-1])
            sys.exit(1)
    except Exception as e:
        logging.error("An error occurred while reading %s: %s", ethnic_info_path, e)
        sys.exit(3)

    # Merge individuals' ethnic information with ethnics reference.
    ## Firstly, recognise 'ethnic_coding' column in ethnic information file
    ##  and rename column name.
    pattern = r".*ethnic.*|.*coding.*|.*code.*"
    for col_name in eth_info.columns:
        if re.match(pattern, col_name, re.IGNORECASE):
            eth_col_name = col_name
            break
    else:
        logging.error(f"No {pattern} column found in %s", ethnic_info_path)
        sys.exit(4)
    eth_info.rename(columns={eth_col_name: "ethnic_coding"}, inplace=True)
    ## rename ID to IID
    pattern = r".*id.*"
    for col_name in eth_info.columns:
        if re.match(pattern, col_name, re.IGNORECASE):
            id_col_name = col_name
            break
    else:
        logging.error(f"No {pattern} column found in %s", ethnic_info_path)
        sys.exit(4)
    eth_info.rename(columns={id_col_name: "IID"}, inplace=True)
    ## Also rename the coding column from the reference file.
    pattern = r".*coding.*|.*code.*"
    for col_name in eth_ref.columns:
        if re.match(pattern, col_name, re.IGNORECASE):
            eth_col_name = col_name
            break
    else:
        logging.error(f"No {pattern} column found in %s", reference_path)
        sys.exit(4)
    eth_ref.rename(columns={eth_col_name: "ethnic_coding_ref"}, inplace=True)
    ## Also, rename "ethnic code meaning" to "meaning"
    pattern = r".*meaning.*|.*mean.*"
    for col_name in eth_ref.columns:
        if re.match(pattern, col_name, re.IGNORECASE):
            eth_col_name = col_name
            break
    else:
        logging.error(f"No {pattern} column found in %s", reference_path)
        sys.exit(4)
    eth_ref.rename(columns={eth_col_name: "meaning"}, inplace=True)

    ## Join two dataframes by 'ethnic' colomn.
    logging.info("Joining %s.fam with ethnic information...", input_name)
    merged_eth = pd.merge(eth_info, eth_ref, how="inner", left_on="ethnic_coding", right_on="ethnic_coding_ref")
    ## Divide population by ethnicity.
    group_list: list[list[str]] = []
    ### Divide population into small ethnic groups and save list of individuals in each group.
    ethnic_names = set(eth_ref["meaning"])
    fam = pd.read_csv(f"{input_name}.fam", sep=r"\s+", header=None, usecols=[0, 1], engine="c", dtype=pd.StringDtype()) 
    fam.columns = pd.Index(["FID", "IID"])
    merged_fam = pd.merge(fam, merged_eth, how="inner", left_on="IID", right_on="IID", suffixes=(None,"_right"))
    for ethnic_name in ethnic_names:    # specify certain ethnic group:
        logging.info(f"Dividing population by ethnicity: {ethnic_name}...")
        ## Then write result to a csv file, which should only contain certain columns (FID, IID)
        merged_fam[merged_fam["meaning"] == ethnic_name].loc[:,["FID", "IID"]].to_csv(
            f"{input_name}_{ethnic_name}.csv", sep="\t", index=False, header=True
        )
        
        ## use plink `--keep` parameter to filter individuals.
        plink_cmd = [
            fm.plink,
            "--bfile", input_name,
            "--keep", f"{input_name}_{ethnic_name}.csv",
            "--make-bed",
            "--out", f"{input_name}_{ethnic_name}"
        ]
        
        subprocess.run(
            plink_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            check=True,
        )
        
        logging.info("Successfully divided population by ethnicity: %s", ethnic_name)
        
        group_list.append([ethnic_name, f"{input_name}_{ethnic_name}"])

    ### Divide population into large ethnic groups.
    
    #### to be implemented yet.

    return group_list

def divide_pop_by_gender(
    plink_path: str,
    input_name: str,
    gender_reference_path: str,
    gender_info_path: str
) -> list[list[str]]:
    """
    Divide population by gender.
    
    # Parameters:
    
    **plink_path** (str): _Path of plink executable._
    
    **input_name** (str):_The name of input plink files (without extension)._

    **gender_reference_path** (str): _Path of gender-seerial reference file, containing meaning of coding in the gender info file._

    **gender_info_path** (str): _Path of gender info file, containing gender information of each individual.
    
    # Returns:
    
    list[list[str]]: _A list of lists, containing [`gender`, `relating file path`]._
    """

     # generate a .csv file, containing [FID, IID, Sex] columns.
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s -- %(levelname)s -- %(message)s")
    # read files
    try:
        gender_ref_df = pd.read_csv(gender_reference_path, sep="\t", dtype=pd.StringDtype())

        if gender_info_path.endswith(".tsv") or gender_info_path.endswith("csv"):
            gender_info_df = pd.read_csv(gender_info_path, sep=r"\s+")
        elif gender_info_path.endswith(".xlsx") or gender_info_path.endswith(".xls"):
            gender_info_df = pd.read_excel(gender_info_path, dtype=pd.StringDtype())
        else:
            logging.error("Unsupported file format: %s", os.path.splitext(gender_info_path)[-1])
            sys.exit(1)
    except Exception as e:
        logging.error("An error occurred while reading %s: %s", gender_info_path, e)
        sys.exit(3)

    pattern = r".*sex.*|.*gender.*"
    ## rename "coding" column in gender_info_df to "original_sex_coding"
    for col_name in gender_info_df.columns:
        if re.match(pattern, col_name, re.IGNORECASE):
            gender_info_df.rename(
                columns={col_name: "original_sex_coding"},
                inplace=True
            )
    ## rename "coding" column in gender_ref_df to "sex_coding"
    for col_name in gender_ref_df.columns:
        if re.match(pattern, col_name, re.IGNORECASE):
            gender_ref_df.rename(
                columns={col_name: "sex_coding"},
                inplace=True
            )
    ## rename "id" column in gender_info_df to "id_info"
    pattern = r".*id.*"
    for col_name in gender_info_df.columns:
        if re.match(pattern, col_name, re.IGNORECASE):
            gender_info_df.rename(
                columns={col_name: "id_info"},
                inplace=True
            )
    
    ## merge gender_serial_reference and gender_info
    merged_sex_info = pd.merge(
        gender_info_df, gender_ref_df,
        how="inner",
        left_on="original_sex_coding",
        right_on="sex_coding"
    )

    ## merge merged_gender_info and .fam and replace the original one
    fam_df = pd.read_csv(f"{input_name}.fam", sep=r"\s+", dtype=pd.StringDtype(), header=None)
    fam_df.columns = pd.Index(["FID",'IID','PID','MID','Sex',"Phenotype"])
    merged_fam = pd.merge(
        fam_df, merged_sex_info, how='inner', 
        left_on='IID', right_on="id_info")
    merged_fam["FID",'IID','PID','MID','sex_coding','Phenoype']

    # divide plink file by gender
    gender_list: list[list[str]] = []
    for sex_coding in merged_sex_info:
        logging.info(f'Dividing population by gender:{sex_coding}...')
        ## Then write result to a csv file, which should only contain certain columns (FID, IID)
        merged_fam[merged_fam["meaning"] == sex_coding].loc[:,["FID", "IID"]].to_csv(
            f"{input_name}_{sex_coding}.csv", sep=r'\s+', index=False, header=True
        )    
        ## use plink '--keep'parameter to filter individuals.
        plink_cmd_gender = [
            plink_path,
            '--bfile', merged_fam,
            '--keep', f'{merged_fam}_{merged_sex_info}.csv',
            '--make-bed',
            '--out', f'{merged_fam}_{merged_sex_info}'
        ]
        subprocess.run(
            plink_cmd_gender,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            check=True,
        )
    
    logging.info('successfully divided population by gender: %s', sex_coding)
    gender_list.append([sex_coding,f'{input_name}_{sex_coding}'])


    return [["gender","file_path"]] # place holder for mypy check
