import cli
import libutils
import os
import sys

import matplotlib.pyplot as plt
from xlsxwriter import Workbook

if __name__ == "__main__":
    args = cli.get_parser().parse_args()
    print(args)

    # Initialize output directory
    if args.output:
        if os.path.exists(args.output):
            print(f"Output directory {args.output} already exists.")
        else:
            os.makedirs(args.output)
            print(f"Created output directory {args.output}")

    results_lf = libutils.read_gwas_results(args.input)
    reference_lf = libutils.read_reference_data(args.phenotype_reference)

    # sns.set_style("whitegrid")
    plt.style.use("seaborn-v0_8-whitegrid")

    with Workbook(f"{args.output}/concatenated_results.xlsx") as wb:
        lfs = libutils.concatenate_results(results_lf, reference_lf)
        lfs[0].collect().write_excel(wb, worksheet="concatenated")
        lfs[1].collect().write_excel(wb, worksheet="mean")

    if cli.AnalysisOption.ALL.name in args.analysis:
        print("Perform all analysis projects")
        libutils.snp_frequency_rank(
            results_lf, reference_lf, save_path=f"{args.output}/snp_frequency_rank")
        libutils.snp_ethnicity_pair_rank(
            results_lf, reference_lf, save_path=f"{args.output}/snp_ethnics_pair_rank")
        libutils.snp_phenotype_pair_rank(
            results_lf, reference_lf, save_path=f"{args.output}/snp_phenotype_pair_rank")
        libutils.snp_phenotype_duplication_rank(
            results_lf, save_path=f"{args.output}/snp_phenotype_duplication_rank")
        libutils.phenotype_frequency_rank(
            results_lf, reference_lf, save_path=f"{args.output}/phenotype_frequency_rank")
        libutils.phenotype_snp_pair_rank(
            results_lf, reference_lf, save_path=f"{args.output}/phenotype_snp_pair_rank")
        libutils.phenotype_ethnicity_pair_rank(
            results_lf, reference_lf, save_path=f"{args.output}/phenotype_ethnicity_pair_rank")

        sys.exit(0)

    for project in args.analysis:
        match cli.AnalysisOption[project]:
            case cli.AnalysisOption.SNP_FREQUENCY_RANK:
                libutils.snp_frequency_rank(
                    results_lf, reference_lf, save_path=f"{args.output}/snp_frequency_rank")
            case cli.AnalysisOption.SNP_ETHNICITY_PAIR_RANK:
                libutils.snp_ethnicity_pair_rank(
                    results_lf, reference_lf, save_path=f"{args.output}/snp_ethnics_pair_rank")
            case cli.AnalysisOption.SNP_PHENOTYPE_PAIR_RANK:
                libutils.snp_phenotype_pair_rank(
                    results_lf, reference_lf, save_path=f"{args.output}/snp_phenotype_pair_rank")
            case cli.AnalysisOption.SNP_PHENOTYPE_DUPLICATION_RANK:
                libutils.snp_phenotype_duplication_rank(
                    results_lf, save_path=f"{args.output}/snp_phenotype_duplication_rank")
            case cli.AnalysisOption.PHENOTYPE_FREQUENCY_RANK:
                libutils.phenotype_frequency_rank(
                    results_lf, reference_lf, save_path=f"{args.output}/phenoype_frequency_rank")
            case cli.AnalysisOption.PHENOTYPE_SNP_PAIR_RANK:
                libutils.phenotype_snp_pair_rank(
                    results_lf, reference_lf, save_path=f"{args.output}/phenotype_snp_pair_rank")
            case cli.AnalysisOption.PHENOTYPE_ETHNICITY_PAIR_RANK:
                libutils.phenotype_ethnicity_pair_rank(
                    results_lf, reference_lf, save_path=f"{args.output}/phenotype_ethnicity_pair_rank")
