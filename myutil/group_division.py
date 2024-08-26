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
    print("merged_eth", merged_eth)
    merged_fam = pd.merge(fam, merged_eth, how="inner", left_on="IID", right_on="IID", suffixes=(None,"_right"))
    print("merged fam: \n", merged_fam)
    for ethnic_name in ethnic_names:    # specify certain ethnic group:
        logging.info(f"Dividing population by ethnicity: {ethnic_name}...")
        ## Then write result to a csv file, which should only contain certain columns (FID, IID)
        merged_fam[merged_fam["meaning"] == ethnic_name].loc[:,["FID", "IID"]].to_csv(
            f"{input_name}_{ethnic_name}.csv", sep="\t", index=False, header=True
        )
        
        ## use plink `--keep` parameter to filter individuals.
        plink_cmd = f"plink --bfile {input_name} --keep {input_name}_{ethnic_name}.csv --make-bed --out {input_name}_{ethnic_name}"
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