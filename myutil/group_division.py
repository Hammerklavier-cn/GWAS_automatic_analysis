import re
import pandas as pd
import matplotlib.pyplot as plt
from threading import Thread
import logging, os
from multiprocessing import Process

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s -- %(levelname)s: %(message)s")
def divide_pop_by_ethnic(
        input_name: str, 
        ethnic_info_path: str, 
        reference_path: str = "./myutil/ethnic_serial_reference.tsv"
    ) -> list[list[str, str]]:
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
        A list of list of ethnic information.
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
            os.exit(1)
    except Exception as e:
        logging.error("An error occurred while reading %s: %s", ethnic_info_path, e)
        os.exit(3)

    # Merge individuals' ethnic information with ethnics reference.
    ## Firstly, recognise 'ethnic' column in ethnic information and reference file.
    pattern = r".*ethnic.*"
    for col_name in eth_info.columns:
        if re.match(pattern, col_name, re.IGNORECASE):
            eth_col_name = col_name
            break
    eth_info.rename(columns={eth_col_name: "ethnic_code"}, inplace=True)
    ## Join two dataframes by 'ethnic' colomn.
    pd.merge(eth_info, eth_ref, how="left", left_on="ethnic_code", right_on="meaning")
    ## Divide population by ethnicity.
    # eth_info = eth_info.groupby("ethnic_info").apply(lambda x: x.sample(frac=1).reset_index(drop=True))
    
    
    for i in range(len(eth_ref)):
        eth_ref.iloc[i, 0] = eth_ref.iloc[i, 0].replace("_", " ")
    pass