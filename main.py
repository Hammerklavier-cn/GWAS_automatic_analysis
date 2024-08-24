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
FMT = FileManagement(args)

# standardise source file
FMT.source_standardisation()

# group population by ethnicity

