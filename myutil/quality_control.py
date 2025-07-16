import os
import sys
import logging
import subprocess
from typing import Optional, Literal
from Classes import FileManagement, Gender


def filter_high_missingness(
    fm: FileManagement,
    input_path_name: str,
    save_path_name: str,
    gender: Gender,
    ethnic: Optional[str],
    missingness_threshold: float = 0.02
) -> tuple[Gender, Optional[str], str] | None:
    """
    Remove SNPs and individuals with high missingness rate.

    Args:
        **fm** (FileManagement): FileManagement object, containing parameters from argparse.
        **input_path_name** (str): Name of the input file.
        **save_path_name** (str): Path name of the output file.
        **missingness_threshold** (float): Threshold of missingness rate.

    Returns:
        tuple[Gender, Optional[str], str]: gender, ethnic, path name of the output file.
    """
    logging.basicConfig(level=logging.WARNING,
                        format="%(asctime)s -- %(levelname)s -- %(message)s")
    try:
        logging.info(
            "Removing SNPs and individuals with missingness rate of %s.",
            str(missingness_threshold)
        )
        command: list = [
            fm.plink,
            "--bfile", input_path_name,
            "--geno", str(missingness_threshold),
            "--mind", str(missingness_threshold),
            "--make-bed",
            "--out", save_path_name
        ]
        subprocess.run(command, stdout=subprocess.DEVNULL,
                       stderr=None, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(
            "An error occurred when running plink: %s", e
        )
        return
    except Exception as e:
        logging.error(
            "Unexpected error occurred: %s", e
        )
        return

    logging.info("Missingness filtering completed.")
    return gender, ethnic, save_path_name


def filter_maf(
    fm: FileManagement,
    input_path_name: str,
    save_path_name: str,
    /,
    gender: Gender,
    ethnic: str | None,
    *,
    maf_threshold: float = 0.01
) -> tuple[Gender, str | None, str] | None:
    """
    Remove SNPs with low minor allele frequency (MAF).

    Args:
        **fm** (FileManagement): FileManagement object, containing parameters from argparse.
        **input_path_name** (str): Name of the input file.
        **save_path_name** (str): Path name of the output file.
        **maf_threshold** (float): Threshold of minor allele frequency.
    Returns:
        **tuple** (tuple[str, str, str] | None): gender, ethnic, path name of the output file. If error ocurred, return None.
    """

    logging.info(
        "Removing MAF with threshold of %s.", str(maf_threshold)
    )
    try:
        command = [
            fm.plink,
            "--bfile", input_path_name,
            "--maf", str(maf_threshold),
            "--make-bed",
            "--out", save_path_name
        ]
        subprocess.run(command, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, check=True)
    except subprocess.CalledProcessError as e:
        if not "12" in str(e):
            logging.error(
                "\nWarning: An error occurred when running plink: %s", e
            )
            # raise subprocess.CalledProcessError(e.returncode, e.cmd, e.output, e.stderr)
            return
        return
    except Exception as e:
        logging.error(
            "Unexpected error occurred: %s", e
        )
        return
    logging.info("MAF filtering completed.")
    return gender, ethnic, save_path_name


def filter_hwe(
    fm: FileManagement,
    input_path_name: str,
    save_path_name: str,
    gender: Gender,
    ethnic: Optional[str],
    hwe_threshold: float = 1e-6
) -> tuple[Gender, Optional[str], str] | None:
    """
    Filter SNPs based on Hardy-Weinberg equilibrium (HWE) p-values.

    Args:
        fm: FileManagement object containing parameters from argparse.
        input_path_name: Path to the input PLINK binary fileset (without extension).
        save_path_name: Path prefix for the output files.
        gender: Gender information to be returned with results.
        ethnic: Ethnicity information to be returned with results.
        hwe_threshold: HWE p-value threshold (default: 1e-6, recommended for quantitative traits).

    Returns:
        Tuple containing (Gender, ethnic, output_path) if successful, None otherwise.
        The output_path will be the same as save_path_name (without extension).
    """
    logging.info(
        "Removing SNPs with HWE p-value threshold of %s.", str(hwe_threshold)
    )
    try:
        command = [
            fm.plink,
            "--bfile", input_path_name,
            "--hwe", str(hwe_threshold), "midp",
            "--make-bed",
            "--out", save_path_name
        ]
        subprocess.run(
            command, stdout=subprocess.DEVNULL, stderr=None, check=True
        )
    except subprocess.CalledProcessError as e:
        logging.error(
            "An error occurred when running plink: %s", e
        )
        return
    except Exception as e:
        logging.error(
            "Unexpected error occurred: %s", e
        )
        return
    logging.info("HWE filtering completed.")
    return gender, ethnic, save_path_name


def ld_pruning(
    plink_path: str,
    input_path_name: str,
    save_path_name: str,
    gender: Gender,
    ethnic: str,
    window_size: int = 50,
    step_size: int = 5,
    r2_threshold: float = 0.2
) -> str | None:
    """
    Calculate linkage disequilibrium (LD).

    Args:
        plink_path (str):
            Path to plink executable.
        input_path_name (str):
            Name of the input file.
        save_path_name (str):
            Path name of the output file.
        gender (Gender):
            Gender (enum) of the samples.
        ethnic (str):
            Ethnicity of the samples.
        window_size (int):
            Window size for LD calculation. Default is 50.
        step_size (int):
            Step size for LD calculation. Default is 5.
        r2_threshold (float):
            Threshold of r^2 values. Default is 0.2.

    Returns:
        output_path_name str | None:
            Path name of the output file. If process failed, returns None.
            Note that the output file has a suffix of ".prune.in" or ".prune.out". You need to
            manually add this in your code.
    """
    try:
        command = [
            plink_path,
            "--bfile", input_path_name,
            "--indep-pairwise", window_size, step_size, r2_threshold,
            "--out", save_path_name,
        ]
        subprocess.run(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"LD pruning failed with error code {e.returncode}")
        return
    except Exception as e:
        logging.error("Unexpected error occurred: %s", str(e))
        sys.exit(1)

    logging.info("LD pruning completed successfully")
    return save_path_name
