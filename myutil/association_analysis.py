from io import StringIO
import logging
import os
import re
import subprocess
import sys
from typing_extensions import deprecated
from myutil import small_tools
from typing import Literal
import pandas as pd
import polars as pl

logger = small_tools.create_logger("AssociationLogger", level=logging.WARN)

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
    gender: Literal["Men", "Women", None] = None,
    ethnic: str | None = None,
    phenotype: str | None = None,
    *,
    # strict_filter: bool = False,
    alpha = 0.05,
    adjust_alpha_by_quantity: bool = True
) -> tuple[
    Literal["Men", "Women", None],
    str | None,
    str | None,
    Literal["assoc", "qassoc"],
    pl.DataFrame
] | None:
    # Find .assoc or .qassoc file.
    if os.path.exists(f"{input_path}.qassoc"):

        logger.info("Get {input_path}.qassoc, which is a .qassoc file")
        # purge duplicated whitespace
        with open(f"{input_path}.qassoc") as reader:
            content = reader.read().strip()
            content = re.sub(r" {2,}", " ", content)
            content = re.sub(r" +\n +", "\n", content)

        with StringIO() as memory_file:
            memory_file.write(content)
            memory_file.seek(0)

    # Warning! Preprocess input file rrequired!
            qassoc_df = pl.read_csv(
                memory_file,
                separator=" ",
                has_header=True,
                null_values="NA"
            )

        # qassoc_df.columns = [x.strip() for x in qassoc_df.columns]

        match adjust_alpha_by_quantity:
            case True:
                threshold = alpha / len(qassoc_df)
            case False:
                threshold = alpha

        filtered_df = qassoc_df.filter(
            pl.col.P <= threshold
        ).with_columns(
            gender = pl.lit(gender),
            ethnic = pl.lit(ethnic),
            phenotype = pl.lit(phenotype)
        )

        return gender, ethnic, phenotype, "qassoc", filtered_df

    elif os.path.exists(f"{input_path}.assoc"):

        logger.info("Get {input_path}.assoc, which is a .assoc file")

        with open(f"{input_path}.assoc") as reader:
            content = reader.read().strip()
            content = re.sub(r"[ \t]{2,}", " ", content)
            content = re.sub(r"[ \t]+\n[ \t]+", "\n", content)

        with StringIO() as memory_file:
            memory_file.write(content)
            memory_file.seek(0)

            assoc_df = pl.read_csv(
                memory_file,
                separator=" ",
                has_header=True,
                null_values="NA"
            )

        # qassoc_df.columns = [x.strip() for x in qassoc_df.columns]

        match adjust_alpha_by_quantity:
            case True:
                threshold = alpha / len(assoc_df)
            case False:
                threshold = alpha

        filtered_df = assoc_df.filter(
            pl.col.P <= threshold
        ).with_columns(
            gender = pl.lit(gender),
            ethnic = pl.lit(ethnic),
            phenotype = pl.lit(phenotype)
        )

        return gender, ethnic, phenotype, "assoc", filtered_df
    else:
        logger.warning("Neither .assoc nor .qassoc file found for input path: %s", input_path)
        return None

    pass

#@deprecated("Ineffective solution powered by pandas. Will be replaced by polars.")
def result_filter_old(
    input_path: str,
    output_path: str,
    gender: Literal["Men", "Women"] | None = None,
    ethnic: str | None = None,
    phenotype: str | None = None,
    *,
    advanced_filter: bool = False,
    err_2_p: float = 0.05
) -> tuple[str|None, str|None, str|None, pd.DataFrame] | None:
    if not os.path.exists(f"{input_path}.qassoc"):
        logger.error("Input file does not exist: %s", f"{input_path}.qassoc")
        """Note: There are two kinds of result files, one with extension .assoc, and one with .qassoc."""
        return

    assoc = pd.read_csv(f"{input_path}.qassoc", sep=r"\s+", index_col=False, skipinitialspace=True)
    threshold = err_2_p / assoc.shape[0]

    assoc_passed = assoc[assoc["P"] < threshold]
    if advanced_filter:
        assoc_passed = assoc_passed[assoc_passed["NMISS"] > 80]
        assoc_passed = assoc_passed[assoc_passed["R2"] > 0.8]

    return gender, ethnic, phenotype, assoc_passed
