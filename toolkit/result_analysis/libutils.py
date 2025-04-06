import polars as pl
import matplotlib.pyplot as plt
# import seaborn as sns

from typing import Sequence

QASSOC_COLUMNS = {
    "CHR": str,
    "SNP": str,
    "BP": int,
    "NMISS": int,
    "BETA": float,
    "SE": float,
    "R2": float,
    "T": float,
    "P": float,
    "gender": str,
    "ethnic": str,
    "phenotype": str
}
NULL_VALUES = ["NA", "Na", "na", "NAN", "NaN", "Nan", "nan", "NULL", "null"]


def read_gwas_results(file_paths: Sequence[str]) -> pl.LazyFrame:
    results_df = pl.DataFrame(
        schema=QASSOC_COLUMNS,
    ).with_columns(
        pl.col.P.cast(pl.Float64, strict=False).drop_nulls()
    )

    for file_path in file_paths:
        result_df = pl.read_csv(
            file_path,
            separator="\t" if file_path.endswith(".tsv") else ",",
            has_header=True,
            null_values=NULL_VALUES,
            schema_overrides=QASSOC_COLUMNS
        )
        if result_df.is_empty():
            continue
        elif result_df.columns != list(QASSOC_COLUMNS.keys()):
            print(f"Skipping {file_path} due to mismatched columns: Expected {
                  QASSOC_COLUMNS.keys()}, got {result_df.columns}")
            continue

        results_df = results_df.vstack(result_df)

    results_lf = results_df.rechunk().lazy()

    return results_lf


def snp_frequency_rank(lf: pl.LazyFrame, save_path: str = "results/snp_frequency_rank"):
    RELATED_PHENOTYPE_PAIRS = "related phenotype pairs"
    rank_lf = (
        lf
        .unique()
        .group_by("SNP")
        .agg("phenotype")
        .with_columns(
            pl.col.phenotype.list.len().alias(RELATED_PHENOTYPE_PAIRS),
        )
        .rename(
            {"phenotype": "phenotypes"}
        )
        .sort(
            by=RELATED_PHENOTYPE_PAIRS,
            descending=True,
            maintain_order=True
        )
    )

    rank_lf.collect().write_excel(f"{save_path}.xlsx")

    rank_head_df = rank_lf.head(50).select(
        "SNP", RELATED_PHENOTYPE_PAIRS).collect()

    fig, ax = plt.subplots()
    fig.set_dpi(150)
    fig.set_size_inches(15, 5)
    # fig.set_layout_engine("compressed")
    ax.xaxis.set_tick_params(rotation=90)

    ax.bar(
        rank_head_df["SNP"],
        rank_head_df[RELATED_PHENOTYPE_PAIRS],
        # color="blue",
        # alpha=0.7,
        # edgecolor="black",
        linewidth=1,
        width=0.6,
        align="center",
        label="Frequency Rank"
    )

    ax.set_title(
        "Frequency Rank of SNPs' Significant Association with Phenotypes")
    ax.set_xlabel("SNP")
    ax.set_ylabel("Frequency")

    fig.tight_layout()
    plt.savefig(f"{save_path}.png", dpi=300)


def snp_phenotype_pair_rank(lf: pl.LazyFrame, save_path: str = "results/snp_phenotype_pair_rank"):
    RELATED_PHENOTYPE_PAIRS_FRQ = "related phenotype pairs frequency"
    rank_lf = (
        lf
        .unique()
        .group_by("SNP")
        .agg("phenotype")
        .with_columns(
            pl.col.phenotype.list.unique(),
        )
        .with_columns(
            pl.col.phenotype.list.len().alias(RELATED_PHENOTYPE_PAIRS_FRQ),
        )
        .rename(
            {"phenotype": "phenotypes"}
        )
        .sort(
            by=RELATED_PHENOTYPE_PAIRS_FRQ,
            descending=True,
            maintain_order=True
        )
    )

    rank_lf.collect().write_excel(f"{save_path}.xlsx")

    rank_head_df = rank_lf.head(50).select(
        "SNP", RELATED_PHENOTYPE_PAIRS_FRQ).collect()

    fig, ax = plt.subplots()
    fig.set_dpi(150)
    fig.set_size_inches(15, 5)
    # fig.set_layout_engine("compressed")
    ax.xaxis.set_tick_params(rotation=90)

    ax.bar(
        rank_head_df["SNP"],
        rank_head_df[RELATED_PHENOTYPE_PAIRS_FRQ],
        linewidth=1,
        width=0.6,
        align="center",
        label="Frequency Rank"
    )

    ax.set_title(
        "Frequency Rank of SNPs' Significant Association with Phenotypes (Pairwise)")
    ax.set_xlabel("SNP")
    ax.set_ylabel("Frequency")

    fig.tight_layout()
    plt.savefig(f"{save_path}.png", dpi=300)


def snp_ethnicity_pair_rank(lf: pl.LazyFrame, save_path: str = "results/snp_ethnic_pair_rank"):
    RELATED_ETHNICITY_PAIRS_FRQ = "related ethnicity pairs frequency"
    rank_lf = (
        lf
        .unique()
        .group_by("SNP")
        .agg("ethnic")
        .with_columns(
            pl.col.ethnic.list.unique(),
        )
        .with_columns(
            pl.col.ethnic.list.len().alias(RELATED_ETHNICITY_PAIRS_FRQ),
        )
        .rename(
            {"ethnic": "ethnics"}
        )
        .sort(
            by=RELATED_ETHNICITY_PAIRS_FRQ,
            descending=True,
            maintain_order=True
        )
    )

    rank_lf.collect().write_excel(f"{save_path}.xlsx")

    rank_head_df = rank_lf.head(50).select(
        "SNP", RELATED_ETHNICITY_PAIRS_FRQ).collect()

    fig, ax = plt.subplots()
    fig.set_dpi(150)
    fig.set_size_inches(15, 5)
    # fig.set_layout_engine("compressed")
    ax.xaxis.set_tick_params(rotation=90)

    ax.bar(
        rank_head_df["SNP"],
        rank_head_df[RELATED_ETHNICITY_PAIRS_FRQ],
        linewidth=1,
        width=0.6,
        align="center",
        label="Frequency Rank"
    )

    ax.set_title(
        "Frequency Rank of populations with significant associations of each SNP (Pairwise)")
    """每个 SNP 在不同种族中与表型显著关联的种族计数"""
    ax.set_xlabel("SNP")
    ax.set_ylabel("Frequency")

    fig.tight_layout()
    plt.savefig(f"{save_path}.png", dpi=300)

def snp_phenotype_duplication_rank(lf: pl.LazyFrame, save_path: str = "results/snp_phenotype_duplication_rank"):
    DUPLICATION_FRQ = "SNP-phenotype duplication count"
    rank_lf = (
        lf
        .unique()
        .group_by("SNP", "phenotype")
        .agg("ethnic")
        .with_columns(
            pl.col.ethnic.list.len().alias(DUPLICATION_FRQ),
        )
        .sort(
            by=DUPLICATION_FRQ,
            descending=True,
            maintain_order=True
        ).with_columns(
            pl.concat_str([pl.col.SNP, pl.col.phenotype], separator="-").alias("SNP-phenotype"),
        )
    )

    rank_lf.collect().write_excel(f"{save_path}.xlsx")

    rank_head_df = rank_lf.head(50).select(
        "SNP-phenotype", DUPLICATION_FRQ).collect()

    fig, ax = plt.subplots()
    fig.set_dpi(150)
    fig.set_size_inches(15, 5)
    # fig.set_layout_engine("compressed")
    ax.xaxis.set_tick_params(rotation=90)

    ax.bar(
        rank_head_df["SNP-phenotype"],
        rank_head_df[DUPLICATION_FRQ],
        linewidth=1,
        width=0.6,
        align="center",
        label="Frequency Rank"
    )

    ax.set_title(
        "Frequency Rank of Significant Associations of SNP-phenotype Pair")
    """"""
    ax.set_xlabel("SNP")
    ax.set_ylabel("Frequency")

    fig.tight_layout()
    plt.savefig(f"{save_path}.png", dpi=300)
