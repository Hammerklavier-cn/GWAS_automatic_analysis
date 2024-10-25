import os, sys, logging, subprocess
from typing import Optional
from Classes import FileManagement

def filter_high_missingness(
    fm: FileManagement,
    input_path_name: str,
    save_path_name: str,
    gender: Optional[str],
    ethnic: Optional[str],
    missingness_threshold: float = 0.02
) -> None:
    """
    Remove SNPs and individuals with high missingness rate.
    
    Args:
        **fm** (FileManagement): FileManagement object, containing parameters from argparse.
        **input_path_name** (str): Name of the input file.
        **save_path_name** (str): Path name of the output file.
        **missingness_threshold** (float): Threshold of missingness rate.
    """
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s -- %(levelname)s -- %(message)s")
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
        subprocess.run(command, stdout=subprocess.DEVNULL, stderr=None, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(
            "An error occurred when running plink: %s", e
        )
        sys.exit(2)
    except Exception as e:
        logging.error(
            "Unexpected error occurred: %s", e
        )
        sys.exit(-3)
    logging.info("Missingness filtering completed.")
    
def filter_maf(
    fm: FileManagement,
    input_path_name: str,
    save_path_name: str,
    maf_threshold: float = 0.01
) -> None:
    """
    Remove SNPs with low minor allele frequency (MAF).
    
    Args:
        **fm** (FileManagement): FileManagement object, containing parameters from argparse.
        **input_path_name** (str): Name of the input file.
        **save_path_name** (str): Path name of the output file.
        **maf_threshold** (float): Threshold of minor allele frequency.
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
        subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    except subprocess.CalledProcessError as e:
        if not "12" in str(e):
            logging.error(
                "\nWarning: An error occurred when running plink: %s", e
            )
            raise subprocess.CalledProcessError(e.returncode, e.cmd, e.output, e.stderr)
    except Exception as e:
        logging.error(
            "Unexpected error occurred: %s", e
        )
        sys.exit(-3)
    logging.info("MAF filtering completed.")
    
def filter_hwe(
    fm: FileManagement,
    input_path_name: str,
    save_path_name: str,
    hwe_threshold: float = 1e-25
) -> None:
    """
    Filter SNPs with low hwe p values.
    
    Args:
        **fm** (FileManagement): FileManagement object, containing parameters from argparse.
        **input_path_name** (str): Name of the input file.
        **save_path_name** (str): Path name of the output file.
        **hwe_threshold** (float): Threshold of hwe p values.
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
        sys.exit(2)
    except Exception as e:
        logging.error(
            "Unexpected error occurred: %s", e
        )
        sys.exit(-3)
    logging.info("HWE filtering completed.")
