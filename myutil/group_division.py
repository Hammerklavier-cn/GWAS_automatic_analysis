import pandas as pd
import matplotlib.pyplot as plt
from threading import Thread
from multiprocessing import Process

def divide_pop_by_ethnic(
        input_name: str, 
        ethnic_info_path: str, 
        reference_path: str = "./myutil/ethnic_serial_reference.tsv"
    ) -> list[list[str, str]]:
    """
    Divide population by ethnicity.
    
    Parameters
    input_name: str
        The name of output files.
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
    
    eth_ref = pd.read_csv(reference_path, sep=r"\s+")
    eth_info = pd.read_csv(ethnic_info_path, sep=r"\s+")
    
    # Merge individuals' ethnic information with ethnics reference.
    # 
    pd.merge(eth_info, eth_ref, how="left", on="ethnic")
    
    
    for i in range(len(eth_ref)):
        eth_ref.iloc[i, 0] = eth_ref.iloc[i, 0].replace("_", " ")
    pass