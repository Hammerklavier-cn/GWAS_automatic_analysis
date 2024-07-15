# -*- coding: utf-8 -*-

from argparse import ArgumentParser, Namespace
import argparse
from typing import Literal
import logging, os

from matplotlib.pylab import f

def setup():
    parser = argparse.ArgumentParser()
    ### determine single SNP or multiple SNP analysis mode
    sing_multi_modegroup = parser.add_argument_group()
    sing_multi_modegroup.add_argument(
        "--single", action="store_true",
        help="to analyse association between single SNP and phenotype"
    )
    sing_multi_modegroup.add_argument(
        "--multiple", type=int,
        help="to analyse association between multiple SNP and phenotype"
    )
    ### designate source file. Currently .vcf and .bed, .ped file is supported.
    parser.add_argument(
        "--file-name", type=str, required=True,
        help="target file name. It can either be .vcf(.gz) file or plink format files (i.e., .bed, or .ped file). It can be either relative path or absolute path."
    )
    ### designate plink executable file path. If not designate, one in PATH will be used.
    parser.add_argument(
        "--plink-path", type=str,
        help="`plink` executable file path. If not assigned, one in PATH will be used. Note: This programme is only designed for plink v1.90."
    )
    return parser

def check(parser: ArgumentParser):
    ## get args
    args = parser.parse_args()
    ## args logical check
    ### check analysis mode
    analysis_mode: Literal["single", "multi"]
    if (not args.single) and (args.multiple is None):
        parser.error("You should at least choone one of `--single` and `--multiple`!")
    elif args.multiple:
        logging.info(f"To analyse association between phenotype and {args.multiple} SNPs...")
        analysis_mode = "multi"
        logging.fatal("`--multiple` mode is still a feature function!")
        os._exit(1)
    elif args.single:
        logging.info(f"To analyse association between phenotype and single SNP...")
        analysis_mode = "single"
    else:
        logging.fatal("Defects in args check algorithm! Please report this to q5vsx3@163.com.")
        os._exit(2)
    ### check if sorce file is valid
    source_file_name: str
    if not os.path.isfile(args.file_name):
        logging.debug(f"{os.path.isfile(f"{args.file_name}")=}")
        parser.error(f"{args.file_name} is not a valid file!")
        os._exit(1)
    else:
        source_file_name = args.file_name
        logging.debug("Valid source file path")
    return analysis_mode, source_file_name