# -*- coding: utf-8 -*-

from argparse import ArgumentParser, Namespace
import argparse
import shutil
from typing import Literal
import logging, os

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
        help="target file name. It can either be .vcf(.gz) file or plink format files (must end with .bed, or .ped file). It can be either relative path or absolute path."
    )
    ### designate plink executable file path. If not designate, one in PATH will be used.
    parser.add_argument(
        "--plink-path", type=str,
        help="`plink` executable file path. If not assigned, one in PATH will be used. Note: This programme is only designed for plink v1.90."
    )
    parser.add_argument(
        "--phenotype", type=str, default=None,
        help="csv file which contains phenotype data. Default is None."
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
        os.exit(11)
    elif args.single:
        logging.info(f"To analyse association between phenotype and single SNP...")
        analysis_mode = "single"
    else:
        logging.fatal("Defects in args check algorithm! Please report this to q5vsx3@163.com.")
        os.exit(-1)
    ### check if sorce file is valid
    source_file_name: str
    if not os.path.isfile(args.file_name):
        logging.debug(f"{os.path.isfile(f'{args.file_name}')=}")
        parser.error(f"{args.file_name} is not a valid file!")
    else:
        source_file_name = args.file_name
        logging.debug("Valid source file path")
    ### check plink
    plink_path: str | None
    if args.plink_path:
        plink_path = shutil.which(args.plink_path)
        if plink_path is None:
            logging.error("%s is not a valid plink executable path!", args.plink_path)
            parser.error("`--plink-path` requires a valid plink executable path!")
    else:
        plink_path = shutil.which("plink")
        if plink_path is None:
            logging.error("plink executable not found in PATH!")
            parser.error("plink executable not found in PATH!")
    logging.info("plink executable path: %s", plink_path)
    ### check phenotype
    if args.phenotype is None and \
        not (source_file_name.endswitch(".vcf") 
             or source_file_name.endswitch(".vcf.gz")):
        parser.error(
            "missign --phenotype option. \
            You must specific a file containing phenotype data as genotype data is in `.vcf` format!")
    
    return analysis_mode, source_file_name, plink_path