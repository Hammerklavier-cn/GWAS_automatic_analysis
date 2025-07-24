# -*- coding: utf-8 -*-

from argparse import ArgumentParser
import argparse
import shutil
from typing import Literal
import logging, os, sys

def setup():
    parser = argparse.ArgumentParser()
    ### determine single SNP or multiple SNP analysis mode
    single_multi_modegroup = parser.add_argument_group(
        description="Determine single SNP or multiple SNP analysis mode.")
    single_multi_exclusive_group = single_multi_modegroup.add_mutually_exclusive_group()
    single_multi_exclusive_group.add_argument(
        "--single", action="store_true",
        help="To analyse association between single SNP and phenotype. This is the default option."
    )
    single_multi_exclusive_group.add_argument(
        "--multiple", type=int,
        help="To analyse association between multiple SNP and phenotype. Warning: This feature is not implemented yet!"
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
    ### designate phenotype file path.
    phenotype_group = parser.add_argument_group(
        title="Phenotype relating options",
        description="Specify files containing phenotype information, or folder containing seperate files, each of which contains information about single phenotype."
    )
    phenotype_exclusive_group = phenotype_group.add_mutually_exclusive_group()
    phenotype_exclusive_group.add_argument(
        "--phenotype", type=str, default=None,
        help="csv/tsv/xls(x) file which contains phenotype data. Default is None."
    )
    phenotype_exclusive_group.add_argument(
        "--phenotypes-folder", type=str, default=None,
        help="Folder containing files, each of which contains single phenotype data. Default is None."
    )

    ### designate ethnic info file path.
    ethnic_group = parser.add_argument_group(
        title="Ethnicity relating options",
        description="Specify files containing ethnicity information. Note that `--ethnic` and `--ethnic-reference` must be provided together."
    )
    ethnic_group.add_argument(
        "--ethnic", type=str, default=None,
        help="csv/tsv/xls(x) file which contains ethnic info. Default is None."
    )
    ### designate reference file path of serial number of ethnic.
    ethnic_group.add_argument(
        "--ethnic-reference", type=str, default=None,
        help="csv/tsv/xls(x) file which contains ethnic-serial reference. Default is None, meaning that the group will not be divided by ethnic."
    )
    ethnic_group.add_argument(
        "--loose-ethnic-filter", action="store_true",
        help="Filter pops according to general ethnic group"
    )

    gender_group = parser.add_argument_group(
        title="Gender relating options",
        description="Specify files containing gender information. Note that `--gender` and `--gender-reference` must be provided together."
    )
    gender_group.add_argument(
        "--gender", type=str, default=None,
        help="csv/tsv/xls(x) file which contains gender info. Default is empty string."
    )
    gender_group.add_argument(
        "--gender-reference", type=str, default=None,
        help="csv/tsv/xls(x) file which contains gender-serial reference. Default is None, meaning that the group will not be divided by gender."
    )
    gender_group.add_argument(
        "--divide-pop-by-gender", action="store_true",
        help="Whether or not to divide population by gender."
    )

    assoc_group = parser.add_argument_group(
        title="Association calculation options"
    )
    assoc_group.add_argument(
        "--ld-correct", action="store_true",
        help="Whether or not to use the number of independent SNPs from LD pruning result as the N value of Bonferroni correction."
    )
    assoc_group.add_argument(
        "--perm", type=int, default=None, const=1_000_000,
        help="\
Whether or not to perform permutation test, and how many times permutation is performed. If no value is assigned with it, the default is 1_000_000. \
Note that this procedure is computationally intensive (yet the implementation is efficient)."
    )
    assoc_group.add_argument(
        "--alpha", type=float, default=0.05,
        help="Bonferroni / permutation corrected alpha value, used for filtering positive SNPs."
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
        sys.exit(11)
    elif args.single:
        logging.info("To analyse association between phenotype and single SNP...")
        analysis_mode = "single"
    else:
        logging.fatal("Defects in args check algorithm! Please report this to q5vsx3@163.com.")
        sys.exit(-1)

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

    ### check gender options
    match (args.gender, args.gender_reference):
        case (None, str() as path):
            logging.error("`--gender-reference` offered as %s; `--gender` required.", path)
            parser.error("Both `--gender` and `--gender-reference` are required!")
        case (str() as path, None):
            logging.error("`--gender` offered as %s; `--gender-reference` required.", path)
            parser.error("Both `--gender` and `--gender-reference` are required!")
        case (str() as path, str() as ref_path):
            logging.info("Gender reference path: %s", ref_path)
            logging.info("Gender path: %s", path)

    ### check phenotype
    if args.phenotype is None and \
        (source_file_name.endswith(".vcf")
            or source_file_name.endswith(".vcf.gz")):
        parser.error("""
            missing --phenotype option.
            You must specific a file containing phenotype data as genotype data is in `.vcf` format!
        """)
    elif args.phenotype is not None:
        if not os.path.exists(args.phenotype):
            parser.error(f"""
                Phenotype file does not exist. Given: {args.phenotype}
            """)

    ### check ethnic options
    match (args.ethnic, args.ethnic_reference):
        case (None, str() as path):
            logging.error("`--ethnic-reference` offered as %s; `--ethnic` required.", path)
            parser.error("Both `--ethnic` and `--ethnic-reference` are required!")
        case (str() as path, None):
            logging.error("`--ethnic` offered as %s; `--ethnic-reference` required.", path)
            parser.error("Both `--ethnic` and `--ethnic-reference` are required!")
        case (str() as path, str() as ref_path):
            logging.info("Ethnic reference path: %s", ref_path)
            logging.info("Ethnic path: %s", path)
