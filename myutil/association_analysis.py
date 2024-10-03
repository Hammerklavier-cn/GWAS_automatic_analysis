import subprocess
import sys
from myutil import small_tools

logger = small_tools.create_logger("AssociationLogger")

def quantitive_association(
    plink_path: str,
    input_name: str,
    phenotype_name: str|None,
    phenotype_info_path: str,
    output_name: str
) -> None:
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
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(-3)
    logger.info("Quantitative association analysis completed.")
