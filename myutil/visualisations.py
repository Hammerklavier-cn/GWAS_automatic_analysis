import os, sys, logging, subprocess
from typing import Literal
import matplotlib.pyplot as plt
import pandas as pd

def missing(
    input_name: str,
    save_path_name: str,
    ethnic: str | None = None,
    gender: Literal["Men", "Women"] | None = None
    ) -> None:
    """
    Visualise missing data proportions.
    
    # Args:
    
    **input_name** (str): _Name of the input file._
    
    **save_path_name** (str): _Path to save the visualisations (without suffix and extension)._
    
    # Returns:
    
    **None**
    
    # Generate Files:
    
    **${save_path_name}.imiss**: _Missingness of individuals._
    
    **${save_path_name}.lmiss**: _Missingness of SNPs._
    
    **${save_path_name}_imiss_visualisation.png**: _Visualisation of missingness of individuals._
    
    **${save_path_name}_lmiss_visualisation.png**: _Visualisation of missingness of SNPs._
    """
    
    input_file_path = os.path.dirname(input_name)
    
    save_path_name = os.path.join(input_file_path, "../","")
    
    logging.info("Calculating proportions of missing data of %s %s data set", ethnic, gender)
    try:
        command = [
            "plink", 
            "--bfile", input_name, 
            "--missing", 
            "--out", save_path_name
        ]
        subprocess.run(
            command,
            stdout=subprocess.DEVNULL,
            stderr=None,
            check=True
        )
    except subprocess.CalledProcessError as e:
        logging.error(f"Error calculating proportions of missing data plink: {e}")
        sys.exit(2)
    logging.info("Finished Calculation")
    
    logging.info("Visualising missing data")
    
    # Visualise missingness of individuals.
    logging.info("Visualising missingness of individuals")
    imiss_df = pd.read_csv(f"{input_name}.imiss", sep=r"\s+", usecols=["F_MISS"])

    plt.hist(imiss_df, density=True, bins=20)  
    ## density=True makes bin's raw count divided by the total number of counts and the bin width, 
    ## so that the area under the histogram integrates to 1.
    plt.title(f"Histogram of SNP missingness per individual from {ethnic} {gender} data set")
    plt.xlabel("Individuals' SNP missing rate")
    plt.ylabel("Frequency / Intercept")
    plt.tight_layout()      # Remove whitespace and avoid overlap around the plot.
    plt.savefig(f"{save_path_name}_imiss.png", dpi=300)
    ## clear figure to prevent conflicts.
    plt.clf()
    
    # Visualise missingness of SNPs.
    lmiss_df = pd.read_csv(f"{input_name}.lmiss", engine="c", sep=r"\s+", usecols=["F_MISS"])

    plt.hist(lmiss_df, density=True, bins=20)
    plt.title(f"Histogram of individual missingness per SNP from {ethnic} {gender} data set")
    plt.xlabel("SNPs' individual missing rate")
    plt.ylabel("Frequency / Intercept")
    plt.tight_layout()
    plt.savefig(f"{save_path_name}_lmiss.png", dpi=300)
