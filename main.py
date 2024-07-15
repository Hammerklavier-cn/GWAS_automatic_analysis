# -*- coding: utf-8 -*-

"""
    This is an automatic single and multiple SNP -- phenotype association analysis script.
"""

# import libraries
## standard libraries
import pprint
import os, logging, sys, argparse
from typing import Literal
from args_setup import myargs

## self-defined libraries

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s -- %(levelname)s -- %(message)s"
)

# argsparse
## set up argsparse
parser = myargs.setup()

## check args
analysis_mode: Literal["single", "multi"]
analysis_mode, source_file_name = myargs.check(parser)
##
