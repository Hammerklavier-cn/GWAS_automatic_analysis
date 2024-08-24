import re
import pandas as pd
import matplotlib.pyplot as plt
from threading import Thread
import logging, os, sys
from multiprocessing import Process

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s -- %(levelname)s: %(message)s")
def divide_pop_by_ethnic(
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
    
    # read files
    try:
        eth_ref = pd.read_csv(reference_path, sep=r"\s+")
        if ethnic_info_path.endswith(".tsv") or ethnic_info_path.endswith("csv"):
            eth_info = pd.read_csv(ethnic_info_path, sep=r"\s+")
        elif ethnic_info_path.endswith(".xlsx") or ethnic_info_path.endswith(".xls"):
            eth_info = pd.read_excel(ethnic_info_path)
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
    merged_eth = pd.merge(eth_info, eth_ref, how="inner", left_on="ethnic_code", right_on="ethnic_coding")
    ## Divide population by ethnicity.
    group_list: list[list[str]] = []
    ### Divide population into small ethnic groups and save list of individuals in each group.
    ethnic_names = set(eth_info["meaning"])
    for ethnic_name in ethnic_names:    # specify certain ethnic group:
        '''
        # depricated low efficiency code.
        ethnic_group = pd.DataFrame(columns=merged_eth.columns)
        for index, row in merged_eth.iterrows():
            if row["ethnic_code"] == ethnic_name:
                ethnic_group = pd.concat([ethnic_group, row.to_frame().T], ignore_index=True)'''
        ethnic_group = merged_eth[merged_eth["meaning"] == ethnic_name]
        ## Then write result to a csv file, which should only contain certain columns (FID, IID)
        fam = pd.read_csv(f"{input_name}.fam", sep="\s+", header=None, usecols=[0, 1], engine="c") 
        fam.columns = pd.Index(["FID", "IID"])
        merged_fam = pd.merge(fam, ethnic_group, how="inner", left_on="IID", right_on="IID", suffixes=(None,"_right"))
        merged_fam["FID", "IID"].to_csv(f"{input_name}_{ethnic_name}.csv", sep="\t", index=False, header=True)
        
        group_list.append([ethnic_name, f"{input_name}_{ethnic_name}.csv"])

    ### Divide population into large ethnic groups.
    
    #### to be implemented yet.

    return group_list