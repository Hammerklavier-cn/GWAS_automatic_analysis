import cli
import libutils
import os
import sys

import matplotlib.pyplot as plt

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
    # print(results_lf.collect())

    # sns.set_style("whitegrid")
    plt.style.use("seaborn-v0_8-whitegrid")

    if cli.AnalysisOption.ALL.name in args.analysis:
        print("Perform all analysis projects")
        libutils.snp_frequency_rank(
            results_lf, f"{args.output}/snp_frequency_rank")
        libutils.snp_ethnicity_pair_rank(
            results_lf, f"{args.output}/snp_ethnics_pair_rank")
        libutils.snp_phenotype_pair_rank(
            results_lf, f"{args.output}/snp_phenotype_pair_rank")
        libutils.snp_phenotype_duplication_rank(
            results_lf, f"{args.output}/snp_phenotype_duplication_rank")

        sys.exit(0)

    for project in args.analysis:
        match cli.AnalysisOption[project]:
            case cli.AnalysisOption.SNP_FREQUENCY_RANK:
                libutils.snp_frequency_rank(
                    results_lf, f"{args.output}/snp_frequency_rank")
            case cli.AnalysisOption.SNP_ETHNICITY_PAIR_RANK:
                libutils.snp_ethnicity_pair_rank(
                    results_lf, f"{args.output}/snp_ethnics_pair_rank")
            case cli.AnalysisOption.SNP_PHENOTYPE_PAIR_RANK:
                libutils.snp_phenotype_pair_rank(
                    results_lf, f"{args.output}/snp_phenotype_pair_rank")
            case cli.AnalysisOption.SNP_PHENOTYPE_DUPLICATION_RANK:
                libutils.snp_phenotype_duplication_rank(
                    results_lf, f"{args.output}/snp_phenotype_duplication_rank")
            case _:
                pass
