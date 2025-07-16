# -*- coding: utf-8 -*-

"""
    This is an automatic single and multiple SNP -- phenotype association analysis script.
"""

__authors__ = ["hammerklavier", "akka2318", "Ciztro"]
__version__ = "0.1.1"
__description__ = "Automatic single and multiple SNP -- phenotype association analysis python script."

# import neccesary libraries
## standard libraries
import os, logging
import sys
from typing import Optional

import polars as pl

## self-defined libraries
### Self defined logger
from myutil import mds, small_tools
from myutil import complements
logger = small_tools.create_logger("MainLogger", level=logging.WARN)

from args_setup import myargs
from Classes import FileManagement, Gender
from myutil import association_analysis, group_division, quality_control, small_tools
from myutil.complements import extract_phenotype_info
import myutil.visualisations as vislz

## multiprocessing libraries
from queue import Queue
import multiprocessing as mp
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures._base import Future as FutureClass

### Note: the logging library should be gradually replaced with self-defined logger

if __name__ == "__main__":

    print(f"""
    {__description__}
        Authors: {' '.join(__authors__)}
        Version: {__version__}
    """)

    logger.info(f"{os.getcwd()=}")

    mp.set_start_method("spawn")

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
    outputs1: list[tuple[Gender, str]] = []
    # output_cache: list = []
    output_queue = Queue()

    # complete gender information in .fam and divide population into male and female groups.
    print(fm.gender_info_file_path,
          fm.gender_reference_path, fm.divide_pop_by_gender)
    match (fm.gender_info_file_path, fm.gender_reference_path, fm.divide_pop_by_gender):
        case (str(), str(), True):
            print("Dividing population by gender...")
            logger.info(
                "Completing gender information and divide population by gender...")
            outputs1 = group_division.divide_pop_by_gender(
                fm.plink,
                output,
                fm.gender_reference_path,
                fm.gender_info_file_path
            )
        case (str(), str(), False):
            # Complete gender information but do not divide pop by gender
            logger.info("Completing gender information...")
            print("Completing gender information...")
            outputs1 = complements.gender_complement(
                fm.plink,
                output,
                fm.gender_info_file_path,
                fm.gender_reference_path
            )
            pass
        case _:
            print("Gender information is not provided. Skip gender complement.")
            outputs1 = [(Gender.UNKNOWN, output)]

    # if args.gender:
    #     print("Completing gender information...")
    #     logger.info("Completing gender information...")
    #     outputs = group_division.divide_pop_by_gender(
    #         fm.plink,
    #         output,
    #         fm.gender_reference_path,
    #         fm.gender_info_file_path
    #     )
    #     print("Gender information complement finished.")
    # else:
    #     logging.warning("No gender information provided, skipping gender complement.")

    # divide population into ethnic groups
    match (fm.ethnic_info_file_path, fm.ethnic_reference_path):
        case (str(), str()):
            print("Dividing population into ethnic groups...")
            output_cache: list[tuple[Gender, str, str]] = []
            for output in outputs1:
                progress_bar.print_progress(
                    f"Divide {os.path.relpath(
                        output[1])} into ethnic groups...",
                    len(outputs1),
                    outputs1.index(output) + 1
                )
                result = group_division.divide_pop_by_ethnic(
                    fm.plink,
                    output[1],
                    fm.ethnic_info_file_path,
                    fm.ethnic_reference_path,
                    output[0],
                    fm.loose_ethnic_filter
                )
                output_cache.extend(result)
            print("")
            logger.info("Division finished.")
        case _:
            print("No ethnic information provided, skipping ethnic complement.")
            output_cache = [(output[0], "all ethnic groups", output[1])
                            for output in outputs1]
    outputs: list[tuple[Gender, str, str]] = output_cache

    # QC
    ## 1. filter high missingness
    ### visualisation
    print("Visualising missingness...")
    logger.info("Visualising missingness...")
    os.makedirs("missingness_visualisations", exist_ok=True)
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
                    output[2],
                    os.path.join(os.path.dirname(
                        output[2]), "../", "missingness_visualisations", os.path.basename(output[2])),
                    gender=output[0], ethnic=output[1]
                )
            )
    print("")
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
                f"Filtering high missingness for {
                    os.path.relpath(output[2])}...",
                len(outputs),
                outputs.index(output) + 1  # type: ignore
            )
            futures.append(
                pool.submit(
                    quality_control.filter_high_missingness,
                    fm,
                    output[2],
                    f"{output[2]}_no_miss",
                    output[0],
                    output[1],
                    missingness_threshold=0.02
                )
            )
        output_cache = [
            future.result()
            for future in as_completed(futures)
            if future.result() is not None
        ]
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
                os.path.join(os.path.dirname(
                    output[2]), "../", "hwe_visualisation", os.path.basename(output[2])+"hwe"),
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
        output_cache = [future.result() for future in as_completed(
            futures) if future.result() is not None]
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
                os.path.join(os.path.dirname(
                    output[2]), "../", "maf_visualisation", os.path.basename(output[2])+"_maf"),
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
                    maf_threshold=0.01
                )
            )
        output_cache = [future.result() for future in as_completed(
            futures) if future.result() is not None]
    outputs = output_cache
    output_cache = []
    print()
    logger.info("Filtering MAF finished.")

    # ## 4. Multi-dimensional scaling
    # ### LD pruning
    # logger.info("Executing LD pruning...")
    # os.mkdir("LD_pruning")
    # # (gender, ethnic, plink file, LD pruning result)
    # ld_pruning_outputs: list[tuple[Gender, str, str, str]] = []
    # for index, output in enumerate(outputs):
    #     progress_bar.print_progress(
    #         f"LD pruning for {os.path.relpath(output[2])}...",
    #         len(outputs),
    #         index
    #     )
    #     ld_pruning_output = quality_control.ld_pruning(
    #         fm.plink,
    #         output[2],
    #         os.path.join("LD_pruning", f"{os.path.basename(output[2])}_indep-SNP"),
    #         output[0],
    #         output[1],
    #     )
    #     if ld_pruning_output is not None:
    #         ld_pruning_outputs.append((*output, ld_pruning_output))

    """OOM will occur in the PCA process!"""
    # ### PCA
    # logger.info("Executing PCA...")
    # os.mkdir("PCA")
    # # (gender, ethnic, plink file, PCA result)
    # pca_outputs: list[tuple[Gender, str, str, str]] = []
    # for index, output in enumerate(ld_pruning_outputs):
    #     progress_bar.print_progress(
    #         f"PCA for {os.path.relpath(output[2])}...",
    #         len(outputs),
    #         index
    #     )
    #     pca_output = mds.principle_component_analysis(
    #         fm.plink,
    #         output[2],
    #         output[3],
    #         os.path.join("PCA", f"{os.path.basename(output[2])}_PCA"),
    #         output[0],
    #         output[1],
    #     )
    #     if pca_output is not None:
    #         pca_outputs.append((output[0], output[1], pca_output, pca_output))

    # Future: Additional covariants can be added here (say, age, BMI, ethnic, etc.).
    #         Note that in this programme, sex is forcely included as an covariate.
    #

    # Now we have finished all QC processes.
    # Next, we shall first split the phenotype source files and then perform the GWAS analysis.

    print("Splitting phenotype source files...")
    match fm.phenotype_file_path, fm.phenotype_folder_path:
        case str() as path, None:
            pheno_files = extract_phenotype_info(
                fm.output_name_temp_root + "_standardised",
                path
            )
        case None, str() as path:
            pheno_files = [(file, os.path.splitext(os.path.basename(file))[0])
                           for file in os.listdir(path) if file.endswith(".txt")]
        case _:
            logger.fatal("Theoratically impossible!")
            sys.exit(1)

    # Association analysis

    print("")
    print("Performing GWAS analysis & visualisation...")
    os.makedirs("assoc_pictures")
    print("Phenotype files:", pheno_files)
    os.mkdir("assoc_results")

    with ProcessPoolExecutor(max_workers=int(cpu_count()/1.5)) as pool:
        futures: list[FutureClass] = []
        for pheno_file in pheno_files:
            for output in outputs:
                ## Calculate association
                pool.submit(
                    progress_bar.print_progress,
                    f"Calculating association between {os.path.basename(pheno_file[0])} and {
                        os.path.basename(output[2])}...",
                    len(outputs) * len(pheno_files),
                    pheno_files.index(pheno_file)*len(outputs) +
                    outputs.index(output) + 1
                )
                ## The following implementation does not support covariates and will be deprecated.
                futures.append(pool.submit(
                    association_analysis.quantitive_association,
                    fm.plink,
                    output[2],
                    pheno_file[0],
                    pheno_file[1],
                    os.path.join("assoc_results", f"{os.path.basename(output[2])}_{
                                 os.path.splitext(os.path.basename(pheno_file[1]))[0]}"),
                    output[0],
                    output[1]
                ))


        output_cache2: list[tuple[Gender, str, str, str]] = [future.result(
        ) for future in as_completed(futures) if future.result() is not None]
    outputs2 = output_cache2
    output_cache2 = []

    # for output in outputs:
    #     progress_bar.print_progress(
    #         f"Visualising association results of {os.path.basename(output[2])}...",
    #         len(outputs),
    #         outputs.index(output) + 1
    #     )
    #     vislz.assoc_visualisation(
    #         f"{output[3]}.qassoc",
    #         os.path.join("assoc_pictures", f"{os.path.basename(output[3])}_assoc"),
    #         output[0], output[1], output[2]
    #     )

    with ProcessPoolExecutor(max_workers=int(cpu_count() * 2 / 3)) as pool:
        for output in outputs2:
            ## Visualise association
            pool.submit(
                progress_bar.print_progress,
                f"Visualising association of {output[2]}...",
                len(outputs2) + 1,
                outputs2.index(output)
            )
            pool.submit(
                vislz.assoc_visualisation,
                f"{output[3]}.qassoc",
                os.path.join("assoc_pictures", f"{
                             os.path.basename(output[3])}_assoc"),
                output[0], output[1], output[2]
            )

    ## 4. Generate summary
    print("Generating summary...")
    os.mkdir("summary")

    qassoc_df: Optional[pl.DataFrame] = None
    assoc_df: Optional[pl.DataFrame] = None

    for output in outputs2:
        progress_bar.print_progress(
            f"Filtering {output[3]}", len(outputs2), outputs2.index(output)
        )
        res = association_analysis.result_filter(
            output[3],
            os.path.join(
                "./summary", f"{os.path.basename(output[3])}_summary.csv"),
            output[0], output[1], output[2],
            alpha=0.05,
            adjust_alpha_by_quantity=True
        )
        if res is None:
            continue
        if res[3] == "assoc":
            if assoc_df is None:
                assoc_df = res[4]
            else:
                assoc_df.vstack(res[4], in_place=True)
        elif res[3] == "qassoc":
            if qassoc_df is None:
                qassoc_df = res[4]
            else:
                qassoc_df.vstack(res[4], in_place=True)

    if qassoc_df is not None:
        qassoc_df.unique().write_csv(
            "summary-q_adjusted.tsv",
            separator="\t",
            include_header=True
        )
    if assoc_df is not None:
        assoc_df.unique().write_csv(
            "summary-b_adjusted.tsv",
            separator="\t",
            include_header=True
        )

    for output in outputs2:
        progress_bar.print_progress(
            f"Filtering {output[3]}", len(outputs2), outputs2.index(output)
        )
        res = association_analysis.result_filter(
            output[3],
            os.path.join(
                "./summary", f"{os.path.basename(output[3])}_summary.csv"),
            output[0], output[1], output[2],
            alpha=0.05,
            adjust_alpha_by_quantity=False
        )
        if res is None:
            continue
        if res[3] == "assoc":
            if assoc_df is None:
                assoc_df = res[4]
            else:
                assoc_df.vstack(res[4], in_place=True)
        elif res[3] == "qassoc":
            if qassoc_df is None:
                qassoc_df = res[4]
            else:
                qassoc_df.vstack(res[4], in_place=True)

    if qassoc_df is not None:
        qassoc_df.write_csv(
            "summary-q.tsv",
            separator="\t",
            include_header=True
        )

    if assoc_df is not None:
        assoc_df.write_csv(
            "summary-b.tsv",
            separator="\t",
            include_header=True
        )

    # with open("summary.tsv", "w") as f:
    #     flag = False
    #     colomns = ["CHR","SNP","BP","NMISS","BETA","SE","R2","T","P","gender","ethnic","phenotype"]
    #     f.write("{}\n".format('\t'.join(colomns)))
    #     for output in outputs:
    #         progress_bar.print_progress(
    #             f"Filtering {output[3]}", len(outputs), outputs.index(output)
    #         )
    #         res = association_analysis.result_filter_old(
    #             output[3],
    #             os.path.join("./summary", f"{os.path.basename(output[3])}_summary.csv"),
    #             output[0],output[1],output[2]
    #         )
    #         if res is None:
    #             continue
    #         res_df = res[3]
    #         res_df["gender"] = output[0]
    #         res_df["ethnic"] = output[1]
    #         res_df["phenotype"] = output[2]
    #         # if flag is False:
    #         #     flag = True
    #         #     print(res_df)
    #         res_df.to_csv(
    #             f, sep="\t", mode="a", header=False, index=False
    #         )
    # ### sort summary.tsv by P-value
    # summary_df = pd.read_csv("summary.tsv", sep="\t", index_col=False)
    # summary_df.sort_values(by="P", inplace=True)
    # print(summary_df)

    # summary_df.to_csv("summary.tsv", sep="\t", mode="w", header=True, index=False)
    # summary_df.to_sql("summary", sqlite3.Connection("./summary.db"), if_exists="replace", index=False, chunksize=100, method='multi')

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
