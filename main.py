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
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from concurrent.futures._base import Future as FutureClass

### Note: the logging library should be gradually replaced with self-defined logger

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
            outputs.index(output) + 1 # type: ignore
        )
        futures.append(
            pool.submit(
                group_division.divide_pop_by_ethnic,
                    fm,
                    output[1],
                    fm.ethnic_info_file_path,
                    fm.ethnic_reference_path,
                    output[0]
            )
        )
    # This output contains [ethnic_name, output_file_path]
    for future in futures:
        print(future.result())
        output_cache.extend(future.result())
    #print("outputs:", outputs)
outputs = output_cache
output_cache = []
logger.info("Division finished.")
print()

# QC
## 1. filter high missingness
### visualisation
print("Visualising missingness...")
logger.info("Visualising missingness...")
os.makedirs("missingness_visualisations", exist_ok=True)
print(outputs)
with ProcessPoolExecutor() as pool:
    futures: list[FutureClass] = []
    for output in outputs:
        progress_bar.print_progress(
            f"Visualising missingness for {os.path.relpath(output[1])}...",
            len(outputs),
            outputs.index(output) + 1 # type: ignore
        )
        futures.append(
            pool.submit(
                vislz.minor_allele_frequency,
                fm,
                output[2],
                os.path.join(os.path.dirname(output[2]), "../", "missingness_visualisations",os.path.basename(output[2])),
                gender=output[0], ethnic=output[1]
            )
        )
print()
logging.info("Visualising missingness finished.")


### Filtering
"""
Current format of `outputs` is [[gender, ethnic, file_name],]
"""
print("Filtering high missingness...")

with ProcessPoolExecutor() as pool:
    futures: list[FutureClass] = []
    output_cache = []
    for output in outputs:
        progress_bar.print_progress(
            f"Filtering high missingness for {os.path.relpath(output[2])}...",
            len(outputs),
            outputs.index(output) + 1 # type: ignore
        )
        futures.append(
            pool.submit(
                quality_control.filter_high_missingness,
                    fm,
                    output[2],
                    f"{output[2]}_no_miss",
                    output[0],
                    output[1],
                    missingness_threshold = 0.02               
            )
        )
    output_cache = [future.result() for future in as_completed(futures) if future.result() is not None]
print()
logger.info("Filtering high missingness finished.")
outputs = output_cache
output_cache = []

## 2. filter HWE
print("Visualising HWE...")
os.makedirs("./hwe_visualisation")
with ProcessPoolExecutor() as pool:
    for output in outputs:
        progress_bar.print_progress(
            f"Visualising HWE for {os.path.relpath(output[2])}...",
            len(outputs),
            outputs.index(output) + 1
        )
        pool.submit(
            vislz.hardy_weinberg,
            fm,
            output[2],
            os.path.join(os.path.dirname(output[2]), "../", "hwe_visualisation", os.path.basename(output[2])+"hwe"),
            output[1],
            output[0]
        )
print("\nFiltering HWE...")
with ProcessPoolExecutor() as pool:
    futures: list[FutureClass] = []
    for output in outputs:
        progress_bar.print_progress(
            f"Filtering HWE for {os.path.relpath(output[2])}...",
            len(outputs),
            outputs.index(output) + 1
        )
        futures.append(
            pool.submit(
                quality_control.filter_hwe,
                fm,
                output[2],
                f"{output[2]}_hwe",
                output[0],
                output[1],
            )
        )
    output_cache = [future.result() for future in as_completed(futures) if future.result() is not None]
outputs = output_cache
output_cache = []
logger.info("Filtering HWE finished.")
print()

## 3. filter MAF
### visualisation
print("Visualising MAF...")
os.makedirs("./maf_visualisation")
with ProcessPoolExecutor() as pool:
    for output in outputs:
        progress_bar.print_progress(
            f"Visualising MAF for {os.path.relpath(output[2])}...",
            len(outputs),
            outputs.index(output) + 1
        )
        pool.submit(
            vislz.minor_allele_frequency,
            fm,
            output[2],
            os.path.join(os.path.dirname(output[2]), "../", "maf_visualisation", os.path.basename(output[2])+"_maf"),
            gender=output[0], ethnic=output[1]
        )
logger.info("MAF visualisation finished.")
print()
### filter MAF
print("Filtering MAF...")
with ProcessPoolExecutor() as pool:
    futures: list[FutureClass] = []
    for output in outputs:
        progress_bar.print_progress(
            f"Filtering MAF for {os.path.relpath(output[2])}...",
            len(outputs),
            outputs.index(output) + 1
        )
        futures.append(
            pool.submit(
                quality_control.filter_maf,
                fm,
                output[2],
                f"{output[2]}_maf",
                output[0], output[1],
                maf_threshold=0.005
            )
        )
    output_cache = [future.result() for future in as_completed(futures) if future.result() is not None]
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
print("Performing GWAS analysis & visualisation...")
os.makedirs("assoc_pictures")
print("Phenotype files:", pheno_files)
os.mkdir("assoc_results")
with ProcessPoolExecutor(max_workers=cpu_count()) as pool:
    futures: list[FutureClass] = []
    for pheno_file in pheno_files:
        for output in outputs:
            ## Calculate association
            pool.submit(
                progress_bar.print_progress,
                    f"Calculating association between {os.path.basename(pheno_file[0])} and {os.path.basename(output[2])}...",
                    len(outputs) * len(pheno_files),
                    pheno_files.index(pheno_file)*len(outputs) + outputs.index(output) + 1
            )
            futures.append(pool.submit(
                association_analysis.quantitive_association,
                    fm.plink,
                    output[2],
                    pheno_file[0],
                    pheno_file[1],
                    os.path.join("assoc_results", f"{os.path.basename(output[2])}_{os.path.splitext(os.path.basename(pheno_file[1]))[0]}"),
                    output[0],
                    output[1]
            ))
    output_cache = [future.result() for future in as_completed(futures)]
outputs = output_cache
output_cache = []
with ProcessPoolExecutor(max_workers=cpu_count()) as pool:
    for output in outputs:
        ## Visualise association
        pool.submit(
            progress_bar.print_progress,
                f"Visualising association of {output[2]}...",
                len(outputs) + 1,
                outputs.index(output)
        )
        pool.submit(
            vislz.assoc_visualisation,
                f"{output[3]}.qassoc", 
                os.path.join("assoc_pictures", f"{os.path.basename(output[3])}_assoc"),
                output[0], output[1], output[2]
        )

## 4. Generate summary
print("Generating summary...")
os.mkdir("summary")
with open("summary/summary.tsv", "w") as f:
    colomns = ["CHR","SNP","BP","NMISS","BETA","SE","R2","T","P","gender","ethnic"]
    f.write("{}\n".format('\t'.join(colomns)))
    for output in outputs:
        progress_bar.print_progress(
            f"processing {output[3]}", len(outputs), outputs.index(output)
        )
        res = association_analysis.result_filter(
            output[3],
            os.path.join("./summary", f"{os.path.basename(output[3])}_summary.csv"),
            output[0],output[1],output[2]
        )
        if res is None:
            continue
        res_df = res[3]
        res_df["gender"] = output[0]
        res_df["ethnic"] = output[1]
        res_df["phenotype"] = output[2]
        res_df.to_csv(
            f, sep="\t", mode="a", header=False, index=False
        )




'''for pheno_file in pheno_files:
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
        )'''