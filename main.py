# -*- coding: utf-8 -*-

"""
    This is an automatic single and multiple SNP -- phenotype association analysis script.
"""

# import neccesary libraries
## standard libraries
import os, logging, sys, argparse
from typing import Literal

## self-defined libraries
from args_setup import myargs
from gwas_check import file_format_check
from Classes import FileManagement
from myutil import group_division

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s -- %(levelname)s -- %(message)s"
)

logging.debug(f"{os.getcwd()=}")

# argsparse
## set up argsparse
parser = myargs.setup()
args = parser.parse_args()

## check args
myargs.check(parser)

# file management
fm = FileManagement(args)

# standardise source file
fm.source_standardisation()

# group population by ethnicity

ethnic_groups = group_division.divide_pop_by_ethnic(
    fm,
    fm.working_file,
    fm.ethnic_info_file_path,
    fm.ethnic_reference_path
)
logging.info(ethnic_groups)

# quality control
for ethnic_name, file_path in ethnic_groups:
    pass
