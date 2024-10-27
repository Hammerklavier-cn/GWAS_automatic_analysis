import os
import subprocess
import sys
from myutil import small_tools
from typing import Literal
import pandas as pd

logger = small_tools.create_logger("AssociationLogger")

def quantitive_association(
    plink_path: str,
    input_name: str,
    phenotype_name: str|None,
    phenotype_info_path: str,
    output_name: str,
    gender: str|None,
    ethnic: str|None
) -> tuple[str|None, str|None, str|None, str] | None:
    """
    Performs a quantitative association analysis on the given input file.
    
    The function uses the plink command-line tool to perform the analysis.
    
    It will generate a association result file named `output_name.qaaoc`
    
    Args:
        plink_path (str): 
            Path to the plink executable file.
        input_name (str): 
            Name of the input file.
        phenotype_name (str | None):
            Name of the phenotype.
        phenotype_info_path (str):
            Path to the phenotype information file.
        output_name (str):
            Name of the output file.

    Returns:
        tuple (tuple[str, str, str, str] | None):
            (gender, ethnic, phenotype name, path name of the output file)
    """
    logger.info("Performing quantitative association analysis...")
    try:
        command = [
            plink_path,
            "--bfile", input_name,
            "--pheno", phenotype_info_path,
            "--assoc",
            "--out", output_name
        ]
        subprocess.run(
            command,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=None        
        )
    except subprocess.CalledProcessError as e:
        logger.warning(f"Error occurred while running plink: {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return
    logger.info("Quantitative association analysis completed.")
    return gender, ethnic, phenotype_name, output_name

def result_filter(
    input_path: str,
    output_path: str,
    gender: Literal["Men", "Women"] | None = None,
    ethnic: str | None = None,
    phenotype: str | None = None,
    *,
    err_2_p: float = 0.05
) -> tuple[str|None, str|None, str|None, pd.DataFrame] | None:
    if not os.path.exists(f"{input_path}.qassoc"):
        logger.error("Input file does not exist: %s", f"{input_path}.qassoc")
        """Note: There are two kinds of result files, one with extension .assoc, and one with .qassoc."""
        return

    assoc = pd.read_csv(f"{input_path}.qassoc", sep=r"\s+", index_col=False, skipinitialspace=True)
    threshold = err_2_p / assoc.shape[0]

    assoc_passed = assoc[assoc["P"] < threshold]
    assoc_passed = assoc_passed[assoc_passed["NMISS"] > 80]
    assoc_passed = assoc_passed[assoc_passed["R2"] > 0.8]

    return gender, ethnic, phenotype, assoc_passed
