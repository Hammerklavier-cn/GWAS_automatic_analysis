import os, sys, subprocess, logging
from Classes import FileManagement

def phenotype_complement(
    fm: FileManagement,
    input_name: str,
    phenotype_info_path: str,
) -> str:
    """
    complement plink-format file with phenotype information.
    
    # Args:
    
    **fm**: FileManagement object
    
    **input_name (str)**: _input file **name**_
    
    **phenotype_info_path (str)**: _path to phenotype information file_
    
    # Returns:
    
    str: _complemented plink file name_
    """
    
    return ["complemented_file_name"]   # placeholder

def gender_complement(
    fm: FileManagement,
    input_name: str,
    gender_info_path: str,
    gender_reference_path: str = "./myutil/gender_serial_reference.csv"
) -> str:
    """
    complement plink-format file with gender information.

    # Args:
    
    **fm**: FileManagement object
    
    **input_name (str)**: _input file **name**_
    
    **phenotype_info_path (str)**: _path to phenotype information file_
    
    **gender_reference_path (str)**: _path to gender reference file, which tells which sex a gender code refers to_
    
    # Returns:
    
    str: _complemented plink file name_
    """
    
    return ["complemented_file_name"]  # placeholder