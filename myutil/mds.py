import logging
import subprocess
import sys

from Classes import Gender
from myutil.small_tools import create_logger

logger = create_logger("GroupDivisionLogger", level=logging.WARN)

def principle_component_analysis(
    plink_path: str,
    input_name: str,
    indep_snp_path: str,
    output_name: str,
    gender: Gender,
    ethnicity: str,
    n_components: int = 10,
) -> str | None:
    """
    Perform principle component analysis on the input data.
    The result will be saved in ${output_name}.eigenvec and ${output_name}.eigenval.

    Args:
        plink_path (str):
            Path to the PLINK binary file.
        input_name (str):
            Name of the input data.
        indep_snp_path (str):
            Path to the `.prune.in` file generated from `--indep-pairwise` process.
        output_name (str):
            Name of the output data.
        gender (Gender):
            Gender of the individuals.
        ethnicity (str):
            Ethnicity of the individuals.
        n_components (int):
            Number of components to retain.

    Returns:
        str | None:
            If analysis succeeds, path to the eigenvec file (with
            .eigenvec extension) will be returned.
    """

    logger.info(
        "Performing principle component analysis for %s. SNP set: %s...",
        input_name, indep_snp_path)

    command = [
        plink_path,
        "--bfile", input_name,
        "--extract", indep_snp_path,
        "--pca", n_components,
        "--out", output_name,
    ]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        logger.error("Error running plink command: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        sys.exit(1)

    return f"{output_name}.eigenvec"
