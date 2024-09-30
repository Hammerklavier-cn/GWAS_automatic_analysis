# -*- coding: utf-8 -*-

"""
    This is an automatic single and multiple SNP -- phenotype association analysis script.
"""

print("""
Automatic single and multiple SNP -- phenotype association analysis python script.
    Author: Hammerklavier-cn, akka2318, Ciztro.
    Version: 0.1rc2
""")

# import neccesary libraries
## standard libraries
import subprocess
import os, logging, sys, argparse
from typing import Literal

## self-defined libraries
from args_setup import myargs
from gwas_check import file_format_check
from Classes import FileManagement
from myutil import association_analysis, group_division, quality_control, small_tools
from myutil.complements import extract_phenotype_info
import myutil.visualisations as vislz

## multiprocessing libraries
from queue import Queue
from threading import Thread
from multiprocessing import Process, Event, cpu_count
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from concurrent.futures._base import Future as FutureClass

### Note: the logging library should be gradually replaced with self-defined logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s -- %(levelname)s -- %(message)s"
)
logger = small_tools.create_logger("MainLogger", level=logging.INFO)

logger.info(f"{os.getcwd()=}")

# argsparse
## set up argsparse
parser = myargs.setup()
args = parser.parse_args()

## check args
myargs.check(parser)

# file management
fm = FileManagement(args)

# progress bar
progress_bar = small_tools.ProgressBar()

# standardise source file
print("Standardising source file...")
logger.info("Standardising source file...")
output = fm.source_standardisation()
outputs = [output]
output_cache: list = []
output_queue = Queue()

# complete gender information in .fam and divide population into male and female groups.
if args.gender:
    print("Completing gender information...")
    logger.info("Completing gender information...")
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
print("Dividing population into ethnic groups...")

with ProcessPoolExecutor() as pool:
    futures: list[FutureClass] = []
    for output in outputs:
        progress_bar.print_progress(
            f"Divide {os.path.relpath(output[1])} into ethnic groups...",
            len(outputs),
            outputs.index(output) + 1
        )
        futures.append(
            pool.submit(
                group_division.divide_pop_by_ethnic,
                    fm,
                    output[1],
                    fm.ethnic_info_file_path,
                    fm.ethnic_reference_path
            )
        )
    # This output contains [ethnic_name, output_file_path]
    for future in futures:
        outputs.extend(future.result())
    #print("outputs:", outputs)
logger.info("Division finished.")
print()

# QC
## 1. filter high missingness
### visualisation
print("Visualising missingness...")
logger.info("Visualising missingness...")

with ProcessPoolExecutor() as pool:
    futures: list[FutureClass] = []
    for output in outputs:
        progress_bar.print_progress(
            f"Visualising missingness for {os.path.relpath(output[1])}...",
            len(outputs),
            outputs.index(output) + 1
        )
        futures.append(
            pool.submit(
                vislz.minor_allele_frequency,
                fm,
                output[1],
                os.path.join(os.path.dirname(output[1]), "../",os.path.basename(output[1])+".png")
            )
        )
print()

logging.info("Visualising missingness finished.")
### Filtering
print("Filtering high missingness...")

with ProcessPoolExecutor() as pool:
    futures: list[FutureClass] = []
    output_cache = []
    for output in outputs:
        progress_bar.print_progress(
            f"Filtering high missingness for {os.path.relpath(output[1])}...",
            len(outputs),
            outputs.index(output) + 1
        )
        futures.append(
            pool.submit(
                quality_control.filter_high_missingness,
                    fm,
                    output[1],
                    f"{output[1]}_no_miss",
                    0.02               
            )
        )
        output_cache.append(f"{output[1]}_no_miss")
print()
logger.info("Filtering high missingness finished.")
outputs = output_cache
output_cache = []

## 2. filter HWE
print("Visualising HWE...")
with ProcessPoolExecutor() as pool:
    for output in outputs:
        progress_bar.print_progress(
            f"Visualising HWE for {os.path.relpath(output)}...",
            len(outputs),
            outputs.index(output) + 1
        )
        pool.submit(
            vislz.hardy_weinberg,
            fm,
            output,
            os.path.join(os.path.dirname(output), "../",os.path.basename(output))
        )
print("\nFiltering HWE...")
with ProcessPoolExecutor() as pool:
    futures: list[FutureClass] = []
    for output in outputs:
        progress_bar.print_progress(
            f"Filtering HWE for {os.path.relpath(output)}...",
            len(outputs),
            outputs.index(output) + 1
        )
        futures.append(
            pool.submit(
                quality_control.filter_hwe,
                fm,
                output,
                f"{output}_hwe",
            )
        )
        output_cache.append(f"{output}_hwe")
outputs = output_cache
output_cache = []
logger.info("Filtering HWE finished.")
print()

## 3. filter MAF
### visualisation
print("Visualising MAF...")
with ProcessPoolExecutor() as pool:
    for output in outputs:
        progress_bar.print_progress(
            f"Visualising MAF for {os.path.relpath(output)}...",
            len(outputs),
            outputs.index(output) + 1
        )
        pool.submit(
            vislz.minor_allele_frequency,
            fm,
            output,
            os.path.join(os.path.dirname(output), "../",os.path.basename(output))
        )
logger.info("MAF visualisation finished.")
print()
### filter MAF
print("Filtering MAF...")
with ProcessPoolExecutor() as pool:
    futures: list[FutureClass] = []
    for output in outputs:
        progress_bar.print_progress(
            f"Filtering MAF for {os.path.relpath(output)}...",
            len(outputs),
            outputs.index(output) + 1
        )
        futures.append(
            pool.submit(
                quality_control.filter_maf,
                fm,
                output,
                f"{output}_maf",
                0.005
            )
        )
        output_cache.append(f"{output}_maf")
outputs = output_cache
output_cache = []
print()
logger.info("Filtering MAF finished.")


### Now we have finished all QC processes.
### Next, we shall first split the phenotype source files and then perform the GWAS analysis.

print("Splitting phenotype source files...")
pheno_files = extract_phenotype_info(
    fm.output_name_temp_root + "_standardised",
    fm.phenotype_file_path
)
print("")
sys.exit()
print("Performing GWAS analysis & visualisation...")
os.makedirs("assoc_pictures")
print("Phenotype files:", pheno_files)
for pheno_file in pheno_files:
    for output in outputs:
        progress_bar.print_progress(
            f"Calculating association for {pheno_file} and {output}...",
            len(outputs) * len(pheno_files),
            pheno_files.index(pheno_file)*len(outputs) + outputs.index(output) + 1
        )
        association_analysis.quantitive_association(
            fm.plink,
            output,
            None,
            pheno_file,
            f"{os.path.basename(output)}_{os.path.splitext(os.path.basename(pheno_file))[0]}"
        )
        progress_bar.print_progress(
            f"Visualising association for {pheno_file} and {output}...",
            len(outputs) * len(pheno_files),
            pheno_files.index(pheno_file)*len(outputs) + outputs.index(output) + 1
        )
        vislz.assoc_visualisation(
            f"{os.path.basename(output)}_{os.path.splitext(os.path.basename(pheno_file))[0]}.qassoc", 
            os.path.join("assoc_pictures", f"({os.path.basename(output)}_{os.path.splitext(os.path.basename(pheno_file))[0]}_assoc)")
        )