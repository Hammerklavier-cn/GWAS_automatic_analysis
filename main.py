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
from myutil import group_division, quality_control, small_tools
import myutil.visualisations as vislz

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s -- %(levelname)s -- %(message)s"
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
output = fm.source_standardisation()
outputs = [output]
output_cache: list = []

# complete gender information in .fam and divide population into male and female groups.
if args.gender:
    print("Completing gender information...")
    outputs = group_division.divide_pop_by_gender(
        fm.plink,
        output,
        fm.gender_reference_path,
        fm.gender_info_file_path
    )
    print("Gender information complement finished.")
else:
    logging.warning("No gender information provided, skipping gender complement.")

# divide population into ethnic groups
logging.info("Dividing population into ethnic groups...")
for output in outputs:
    small_tools.progress_bar(
        f"Divide {output[1]} into ethnic groups...",
        len(outputs),
        outputs.index(output) + 1
    )
    output_temp = group_division.divide_pop_by_ethnic(
        fm,
        output[1],
        fm.ethnic_info_file_path,
        fm.ethnic_reference_path
    )
    output_cache.extend(output_temp)
print("")
logging.debug(f"{output_cache=}")
outputs = output_cache
output_cache = []   # clear the cache

# QC
## 1. filter high missingness
### visualisation
logging.info("Visualising missingness...")
for output in outputs:
    small_tools.progress_bar(
        f"Visualising missingness for {output[1]}...",
        len(outputs),
        outputs.index(output) + 1
    )
    vislz.missing(
        fm,
        output[1],
        os.path.join(os.path.dirname(output[1]), "../",os.path.basename(output[1])+".png")
    )
print("")
logging.info("Visualising missingness finished.")
### Filtering
logging.info("Filtering high missingness...")
for output in outputs:
    small_tools.progress_bar(
        f"Filtering high missingness for {output[1]}...",
        len(outputs),
        outputs.index(output) + 1
    )
    quality_control.filter_high_missingness(
        fm,
        output[1],
        f"{output[1]}_no_miss",
        0.02
    )
    output_cache.append(f"{output[1]}_no_miss")
print("")
logging.info("Filtering high missingness finished.")

outputs = output_cache
output_cache = []

## 2. filter HWE
logging.info("Filtering HWE...")
print("outputs:", outputs)
for output in outputs:
    small_tools.progress_bar(
        f"Filtering HWE for {output}...",
        len(outputs),
        outputs.index(output) + 1
    )
    print()
    quality_control.filter_hwe(
        fm,
        output,
        f"{output}_hwe"
    )
    output_cache.append(f"{output}_no_hwe")


# group population by ethnicity



# quality control
for ethnic_name, file_path in ethnic_groups:
    pass
