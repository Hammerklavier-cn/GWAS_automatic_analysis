import os, sys, logging, subprocess

def missing_visualisation(input_name: str) -> None:
    """
    Visualise missing data proportions.
    
    # Args:
    
    **input_name** (str): _Name of the input file._
    
    # Returns:
    
    **None**
    
    # Generate Files:
    
    **${input_name}.imiss**: _Missingness of individuals._
    
    **${input_name}.lmiss**: _Missingness of SNPs._
    
    **${input_name}_imiss_visualisation.png**: _Visualisation of missingness of individuals._
    
    **${input_name}_lmiss_visualisation.png**: _Visualisation of missingness of SNPs._
    """
    
    logging.info("Calculating proportions of missing data")
    subprocess.run(["plink", "--bfile", input_name, "--missing", "--out", input_name])