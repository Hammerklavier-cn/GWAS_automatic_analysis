from io import StringIO
import logging
import os
import re
import subprocess
from typing_extensions import deprecated
from Classes import Gender
from myutil import small_tools
from typing import Literal
import pandas as pd
import polars as pl

logger = small_tools.create_logger("AssociationLogger", level=logging.WARN)

def quantitive_association(
    plink_path: str,
    input_name: str,
    phenotype_name: str,
    phenotype_info_path: str,
    output_name: str,
    gender: Gender,
    ethnic: str
) -> tuple[Gender, str, str, str] | None:
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
    gender: Gender,
    ethnic: str,
    phenotype: str,
    *,
    alpha = 0.05,
    adjust_alpha_by_quantity: bool = True
) -> tuple[
    Gender,
    str,
    str,
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
                null_values=["NA", "Na", "na", "", "NaN", "nan", "NAN", "Nan"]
            ).with_columns(
                pl.col.P.cast(pl.Float64, strict=False)
            ).drop_nulls()

            if len(qassoc_df) == 0:
                logger.warning("No valid data found in .qassoc file")
                return None

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
            pl.col.P < threshold
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

@deprecated("Ineffective solution powered by pandas. Will be replaced by polars.")
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

    return gender, ethnic, phenotype, assoc_passed # type: ignore

def linear_regression(
    plink_path: str,
    input_name: str,
    phenotype_name: str,
    phenotype_info_path: str,
    output_name: str,
    gender: Literal["male", "female"] | None = None,
    ethnic: str | None = None,
) -> tuple[Literal["male", "female"] | None, str | None, str | None, str] | None:
    """
    Perform a linear regression analysis using PLINK for a quantitive phenotype.

    It will generate a `.assoc.linear` file, which contains the results of the linear regression analysis.

    Args:
        plink_path (str):
            The path to the PLINK executable.
        input_name (str):
            The name of the input file.
        phenotype_name (str):
            The name of the phenotype.
        phenotype_info_path (str):
            The path to the phenotype information file.
        output_name (str):
            Name of the output file.
        gender (Literal["male", "female"] | None, optional):
            The gender of the individuals. Defaults to None.
        ethnic (str | None, optional):
            The ethnicity of the individuals. Defaults to None.
    Returns:
        tuple (tuple[Literal["male", "female"] | None, str | None, str | None, str]):
            A tuple containing the gender, ethnicity, phenotype, and output file name.
    Generate Files:
        %(output_name)s.assoc.linear
    """
    logger.info("Perform a linear regression analysis using PLINK for a quantitive phenotype")
    pass

def multidimensional_scaling(
    plink_path: str,
    input_name: str,
    phenotype_name: str | None,
    phenotype_info_path: str,
    output_path: str,
    dimension_count: int = 10
):
    """
    Perform a multidimensional scaling analysis using PLINK for a quantitive or binary phenotype.

    It will generate a `.assoc.mds` file, which contains the results of the multidimensional scaling analysis.

    Args:
        plink_path (str):
            Path to the plink executable.
        input_name (str):
            Name of the input file. (path excluding the extension)
        phenotype_info_path (str):
            Path to the phenotype information file.
        output_path (str):
            Path to the output file. (path including the extension)
        dimension_count (int):
            Number of dimensions to use for the multidimensional scaling analysis. Defaults to 10.
    Generate File:
        %(output_path)
    """
    logger.info("Perform a multidimensional scaling analysis using PLINK for a quantitive or binary phenotype")
    command = [
        plink_path,
        "--bfile", input_name,
        "--pheno", phenotype_info_path,
        "--cluster",
        "--mds-plot", str(dimension_count),
        "--out", f"{input_name}_{phenotype_name}",
    ]
    subprocess.run(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=True
    )
    logger.info(
        "Finished calculating multidimensional scaling analysis between %s and %s.",
        phenotype_name,
        input_name
    )
    logger.debug("Transferring file to `--covar`` format")
    mds_result_lf = pl.scan_csv(
        f"{input_name}_{phenotype_name}.mds",
        separator="\t",
        infer_schema=False,
        null_values="NA"
    )

    pass
