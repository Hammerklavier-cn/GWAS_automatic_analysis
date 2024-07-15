# -*- coding: utf-8 -*-

"""
    This is an automatic single and multiple SNP -- phenotype association analysis script.
"""

# import libraries
## standard libraries
import os, logging, sys, argparse
from typing import Literal

## self-defined libraries
from args_setup import myargs
from gwas_check import file_format_check
from Classes import FileManagement

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s -- %(levelname)s -- %(message)s"
)

logging.debug(f"{os.getcwd()=}")

# argsparse
## set up argsparse
parser = myargs.setup()
args = parser.parse_args()

## check args
analysis_mode: Literal["single", "multi"]
analysis_mode, source_file_name, plink_path = myargs.check(parser)

# file management
FMT = FileManagement(source_file_name, plink_path)