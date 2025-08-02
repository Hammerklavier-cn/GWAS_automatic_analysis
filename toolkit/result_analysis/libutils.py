from enum import Enum
import polars as pl
import matplotlib.pyplot as plt
# import seaborn as sns

from typing import Sequence


class QassocColumns(Enum):
    CHR = "CHR"
    SNP = "SNP"
    BP = "BP"
    NMISS = "NMISS"
    BETA = "BETA"
    SE = "SE"
    R2 = "R2"
    T = "T"
    P = "P"
    Pn = "P'"
    PERM_P_1 = "PERM_P_1"
    PERM_P_2 = "PERM_P_2"
    GENDER = "gender"
    ETHNIC = "ethnic"
    PHENOTYPE = "phenotype"
    G11 = "G11"
    G12 = "G12"
    G22 = "G22"


QASSOC_COLUMNS_SCHEMA = {
    QassocColumns.CHR.value: pl.String,
    QassocColumns.SNP.value: pl.String,
    QassocColumns.BP.value: pl.Int64,
    QassocColumns.NMISS.value: pl.Int64,
    QassocColumns.BETA.value: pl.Float64,
    QassocColumns.SE.value: pl.Float64,
    QassocColumns.R2.value: pl.Float64,
    QassocColumns.T.value: pl.Float64,
    QassocColumns.P.value: pl.Float64,
    QassocColumns.Pn.value: pl.Float64,
    QassocColumns.PERM_P_1: pl.Float64,
    QassocColumns.PERM_P_2: pl.Float64,
    QassocColumns.GENDER.value: pl.String,
    QassocColumns.ETHNIC.value: pl.String,
    QassocColumns.PHENOTYPE.value: pl.String,
    QassocColumns.G11.value: pl.Float64,
    QassocColumns.G12.value: pl.Float64,
    QassocColumns.G22.value: pl.Float64,
}


class ReferenceColumns(Enum):
    FIELD_ID = "field_id"
    PHENOTYPE = "phenotype_name"


REFERENCE_COLUMNS_SCHEMA = {
    ReferenceColumns.FIELD_ID.value: pl.String,
    ReferenceColumns.PHENOTYPE.value: pl.String
}

NULL_VALUES = ["NA", "Na", "na", "NAN", "NaN", "Nan", "nan", "NULL", "null"]


def read_gwas_results(file_paths: list[str]) -> pl.LazyFrame:
    # results_df = pl.DataFrame(
    #     schema=QASSOC_COLUMNS_SCHEMA,
    # ).with_columns(
    #     pl.col(QassocColumns.P.value).cast(
    #         pl.Float64, strict=False).drop_nulls()
    # )

    result_df_list: list[pl.DataFrame] = []

    for file_path in file_paths:
        result_df = pl.read_csv(
            file_path,
            separator="\t" if file_path.endswith(".tsv") else ",",
            has_header=True,
            null_values=NULL_VALUES,
        )
        if result_df.is_empty():
            continue

        result_df_list.append(result_df)

    results_df: pl.DataFrame | None = None
    mutual_header: list[str] | None = None
    for result_df in result_df_list:
        header = result_df.columns
        match mutual_header, header:
            case None, list():
                mutual_header = [
                    s for s in header
                    if s in [
                        column.value for column in QassocColumns
                    ]
                ]
                results_df = result_df.select([
                    pl.col(s).cast(QASSOC_COLUMNS_SCHEMA[s], strict=False)
                    for s in mutual_header
                ]).drop_nulls()

            case list(), list():
                assert results_df is not None

                verified_header = [
                    s for s in header
                    if s in [
                        column.value for column in QassocColumns
                    ]
                ]
                old_mutual_header = mutual_header
                mutual_header = [
                    s for s in verified_header
                    if s in old_mutual_header
                ]

                if mutual_header != old_mutual_header:
                    results_df = results_df.select(mutual_header)

                results_df.vstack(
                    result_df.select(
                        mutual_header
                    ).cast(
                        {
                            col: QASSOC_COLUMNS_SCHEMA[col]
                            for col in mutual_header
                        }
                    )
                )

    assert results_df is not None

    results_lf = results_df.rechunk().lazy()

    return results_lf


def read_reference_data(file_path: str) -> pl.LazyFrame:
    """read phenotype reference data"""
    lf = pl.scan_csv(
        file_path,
        separator="\t" if file_path.endswith(".tsv") else ",",
        has_header=False,
        null_values=NULL_VALUES,
        schema=REFERENCE_COLUMNS_SCHEMA
    )

    return lf

def concatenate_results(results_lf: pl.LazyFrame, reference_lf: pl.LazyFrame) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Concatenate results and reference data"""
    merged_results =  (
        results_lf
            .with_columns(
                field_id=pl.col(QassocColumns.PHENOTYPE.value).str.split(".").list.get(1)
            )
            .rename({QassocColumns.PHENOTYPE.value: "f.id"})
            .join(
                reference_lf,
                left_on="field_id",
                right_on=ReferenceColumns.FIELD_ID.value,
                how="inner"
            )
            .drop("field_id")
    )
    avg_lf = (
        merged_results
            .group_by(ReferenceColumns.PHENOTYPE.value)
            .agg(
                pl.col(QassocColumns.BETA.value).mean().alias("beta_mean"),
                pl.col(QassocColumns.SE.value).mean().alias("se_mean"),
                pl.col(QassocColumns.R2.value).mean().alias("r2_mean"),
                pl.col(QassocColumns.P.value).mean().alias("p_mean"),
            )
    )
    return merged_results, avg_lf

def snp_frequency_rank(
    result_lf: pl.LazyFrame,
    reference_lf: pl.LazyFrame,
    *,
    save_path: str = "results/snp_frequency_rank"
) -> pl.LazyFrame:
    """
    Calculate the frequency rank of SNPs based on the occurrence frequency of related phenotypes.

    Args:
        result_lf (pl.LazyFrame): LazyFrame containing SNP data.
        reference_lf (pl.LazyFrame): LazyFrame containing phenotype reference data.
        save_path (str): Path to save the results.

    Returns:
        pl.LazyFrame: LazyFrame containing SNP frequency rank data.
    """
    RELATED_PHENO_OCCUR_FREQ = "related phenotype occurrence frequency"

    result_lf = result_lf.with_columns(
        field_id=pl.col(QassocColumns.PHENOTYPE.value).str.split(".").list.get(1)
    ).drop(QassocColumns.PHENOTYPE.value)

    rank_lf = (
        result_lf.join(
            reference_lf,
            left_on="field_id",
            right_on=ReferenceColumns.FIELD_ID.value
        )
        .unique()
        .with_columns(
            pl.concat_str(
                [
                    pl.col(QassocColumns.ETHNIC.value),
                    pl.col(QassocColumns.GENDER.value),
                    pl.col(ReferenceColumns.PHENOTYPE.value)
                ],
                separator="-"
            )
                .alias("$(ethnic)-$(gender)-$(phenotype)")
        )
        .group_by(QassocColumns.SNP.value)
        .agg(
            pl.col("$(ethnic)-$(gender)-$(phenotype)"),
            pl.col("$(ethnic)-$(gender)-$(phenotype)").count().alias(RELATED_PHENO_OCCUR_FREQ)
        )
        .sort(
            by=RELATED_PHENO_OCCUR_FREQ,
            descending=True,
            maintain_order=True
        )
    )

    rank_lf.collect().write_excel(f"{save_path}.xlsx")

    rank_head_df = rank_lf.head(50).select(
        QassocColumns.SNP.value, RELATED_PHENO_OCCUR_FREQ).collect()

    fig, ax = plt.subplots()
    fig.set_dpi(150)
    fig.set_size_inches(15, 5)
    # fig.set_layout_engine("compressed")
    ax.xaxis.set_tick_params(rotation=90)

    ax.bar(
        rank_head_df[QassocColumns.SNP.value],
        rank_head_df[RELATED_PHENO_OCCUR_FREQ],
        linewidth=1,
        width=0.6,
        align="center",
        label="Frequency Rank"
    )

    for i in rank_head_df.iter_rows(named=False):
        ax.text(i[0], i[1], str(i[1]), ha="center", va="bottom")

    ax.set_title(
        "Frequency Rank of SNPs' Significant Association with Phenotypes")
    ax.set_xlabel("SNP")
    ax.set_ylabel("Frequency")

    fig.tight_layout()
    plt.savefig(f"{save_path}.png", dpi=300)

    return rank_lf


def snp_phenotype_pair_rank(
    result_lf: pl.LazyFrame,
    reference_lf: pl.LazyFrame,
    *,
    save_path: str = "results/snp_phenotype_pair_rank"
) -> pl.LazyFrame:
    """
    Calculate the frequency ranking of SNPs' SNP-phenotype pairs (no duplication).

    Args:
        result_lf (pl.LazyFrame): Input LazyFrame containing GWAS results with SNP and phenotype data.
        reference_lf (pl.LazyFrame): LazyFrame containing phenotype reference data.
        save_path (str): Output path prefix for saving results (will generate .xlsx and .png files).

    Returns:
        pl.LazyFrame: Processed LazyFrame with SNPs' SNP-phenotype association frequencies ranked by count.
    """
    RELATED_PHENOTYPE_PAIRS_FRQ = "SNPs' phenotype pairs frequency"

    result_lf = result_lf.with_columns(
        field_id=pl.col(QassocColumns.PHENOTYPE.value).str.split(".").list.get(1)
    ).drop(QassocColumns.PHENOTYPE.value)

    rank_lf = (
        result_lf.join(
            reference_lf,
            left_on="field_id",
            right_on=ReferenceColumns.FIELD_ID.value
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col(QassocColumns.ETHNIC.value),
                    pl.col(QassocColumns.GENDER.value),
                ],
                separator="-"
            )
                .alias("$(ethnic)-$(gender)")
        )
        .group_by(QassocColumns.SNP.value, ReferenceColumns.PHENOTYPE.value)
        .agg(pl.col("$(ethnic)-$(gender)").alias("[$($(ethnic)-$(gender))]"))
        .with_columns(
            pl.concat_list(ReferenceColumns.PHENOTYPE.value)
        )
        .with_columns(
            pl.concat_list(
                pl.col("[$($(ethnic)-$(gender))]"),
                pl.col(ReferenceColumns.PHENOTYPE.value)
            )
                .alias("[$($(ethnic)-$(gender),), $(phenotype)]")
        )
        .group_by(QassocColumns.SNP.value)
        .agg(
            pl.col("[$($(ethnic)-$(gender),), $(phenotype)]"),
            pl.col("[$($(ethnic)-$(gender),), $(phenotype)]").count().alias(RELATED_PHENOTYPE_PAIRS_FRQ)
        )
        .sort(
            by=RELATED_PHENOTYPE_PAIRS_FRQ,
            descending=True,
            maintain_order=True
        )
    )

    rank_lf.collect().write_excel(f"{save_path}.xlsx")

    rank_head_df = rank_lf.head(50).select(
        QassocColumns.SNP.value, RELATED_PHENOTYPE_PAIRS_FRQ).collect()

    fig, ax = plt.subplots()
    fig.set_dpi(150)
    fig.set_size_inches(15, 5)
    # fig.set_layout_engine("compressed")
    ax.xaxis.set_tick_params(rotation=90)

    ax.bar(
        rank_head_df[QassocColumns.SNP.value],
        rank_head_df[RELATED_PHENOTYPE_PAIRS_FRQ],
        linewidth=1,
        width=0.6,
        align="center",
        label="Frequency Rank"
    )

    for i in rank_head_df.iter_rows(named=False):
        ax.text(i[0], i[1], str(i[1]), ha="center", va="bottom")

    ax.set_title(
        "Frequency Rank of SNPs' Significant Association with Phenotypes (Pairwise)")
    ax.set_xlabel("SNP")
    ax.set_ylabel("Frequency")

    fig.tight_layout()
    plt.savefig(f"{save_path}.png", dpi=300)

    return rank_lf


def snp_ethnicity_pair_rank(
    result_lf: pl.LazyFrame,
    reference_lf: pl.LazyFrame,
    *,
    save_path: str = "results/snp_ethnic_pair_rank"
) -> pl.LazyFrame:
    """
    Calculate the frequency ranking of SNP-ethnicity pairs (no duplication).

    Args:
        result_lf (pl.LazyFrame): Input LazyFrame containing GWAS results with SNP and ethnicity data.
        reference_lf (pl.LazyFrame): Input LazyFrame containing SNP frequency rank data.
        save_path (str): Output path prefix for saving results (will generate .xlsx and .png files).

    Returns:
        pl.LazyFrame: Processed LazyFrame with SNP-ethnicity association frequencies ranked by count.
    """
    RELATED_ETHNICITY_PAIRS_FRQ = "SNPs' ethnicity pairs frequency"
    result_lf = result_lf.with_columns(
        field_id=pl.col(QassocColumns.PHENOTYPE.value).str.split(".").list.get(1)
    ).drop(QassocColumns.PHENOTYPE.value)

    rank_lf = (
        result_lf.join(
            reference_lf,
            left_on="field_id",
            right_on=ReferenceColumns.FIELD_ID.value,
            how="inner"
        )
        .unique()
        .with_columns(
            pl.concat_str(
                [
                    pl.col(QassocColumns.ETHNIC.value),
                    pl.col(QassocColumns.GENDER.value),
                ],
                separator="-"
            )
                .alias("$(ethnic)-$(gender)")
        )
        .group_by(QassocColumns.SNP.value, "$(ethnic)-$(gender)")
        .agg(pl.col(ReferenceColumns.PHENOTYPE.value).alias("[$(phenotype)]"))
        .with_columns(
            pl.concat_list(pl.col("$(ethnic)-$(gender)").alias("[$(ethnic)-$(gender)]"))
        )
        .with_columns(
            pl.concat_list(
                pl.col("[$(phenotype)]"),
                pl.col("[$(ethnic)-$(gender)]"),
            )
                .alias("[$($(phenotype),), $(ethnic)-$(gender)]")
        )
        .group_by(QassocColumns.SNP.value)
        .agg(
            pl.col("[$($(phenotype),), $(ethnic)-$(gender)]"),
            pl.col("[$($(phenotype),), $(ethnic)-$(gender)]").count().alias(RELATED_ETHNICITY_PAIRS_FRQ)
        )
        .sort(
            by=RELATED_ETHNICITY_PAIRS_FRQ,
            descending=True,
            maintain_order=True
        )
    )

    rank_lf.collect().write_excel(f"{save_path}.xlsx")

    rank_head_df = rank_lf.head(50).select(
        QassocColumns.SNP.value, RELATED_ETHNICITY_PAIRS_FRQ).collect()

    fig, ax = plt.subplots()
    fig.set_dpi(150)
    fig.set_size_inches(15, 5)
    # fig.set_layout_engine("compressed")
    ax.xaxis.set_tick_params(rotation=90)

    ax.bar(
        rank_head_df[QassocColumns.SNP.value],
        rank_head_df[RELATED_ETHNICITY_PAIRS_FRQ],
        linewidth=1,
        width=0.6,
        align="center",
        label="Frequency Rank"
    )

    for i in rank_head_df.iter_rows(named=False):
        ax.text(i[0], i[1], str(i[1]), ha="center", va="bottom")

    ax.set_title(
        "Frequency Rank of populations with significant associations of each SNP (Pairwise)")
    """每个 SNP 在不同种族中与表型显著关联的种族计数"""
    ax.set_xlabel("SNP")
    ax.set_ylabel("Frequency")

    fig.tight_layout()
    plt.savefig(f"{save_path}.png", dpi=300)

    return rank_lf


def snp_phenotype_duplication_rank(
    lf: pl.LazyFrame,
    *,
    save_path: str = "results/snp_phenotype_duplication_rank"):
    """
    Calculate the duplication frequency ranking of SNP-phenotype pairs across different ethnic groups.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing GWAS results with SNP, phenotype, and ethnicity data.
        save_path (str): Output path prefix for saving results (will generate .xlsx and .png files).

    Returns:
        pl.LazyFrame: Processed LazyFrame with SNP-phenotype pairs ranked by their duplication count across ethnicities.

    Note:
        The duplication count represents the number of ethnic groups in which the SNP-phenotype association is observed.
    """
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
            pl.concat_str([pl.col.SNP, pl.col.phenotype], separator="-").alias("SNP-Phenotype"),
        )
    )

    rank_lf.collect().write_excel(f"{save_path}.xlsx")

    rank_head_df = rank_lf.head(50).select(
        "SNP-Phenotype", DUPLICATION_FRQ).collect()

    fig, ax = plt.subplots()
    fig.set_dpi(150)
    fig.set_size_inches(15, 5)
    # fig.set_layout_engine("compressed")
    ax.xaxis.set_tick_params(rotation=90)

    ax.bar(
        rank_head_df["SNP-Phenotype"],
        rank_head_df[DUPLICATION_FRQ],
        linewidth=1,
        width=0.6,
        align="center",
        label="Frequency Rank"
    )

    for i in rank_head_df.iter_rows(named=False):
        ax.text(i[0], i[1], str(i[1]), ha="center", va="bottom")


    ax.set_title(
        "Frequency Rank of Significant Associations of SNP-phenotype Pair")
    """"""
    ax.set_xlabel("SNP-Phenotype")
    ax.set_ylabel("Frequency")

    fig.tight_layout()
    plt.savefig(f"{save_path}.png", dpi=300)

def phenotype_frequency_rank(
    result_lf: pl.LazyFrame,
    reference_lf: pl.LazyFrame,
    *,
    save_path: str = "results/phenotype_frequency_rank",
) -> pl.LazyFrame:
    """
    Calculate the frequency rank of phenotypes based on their occurrence frequency in the GWAS analysis result.

    Args:
        result_lf (pl.LazyFrame): The GWAS analysis result.
        reference_lf (pl.LazyFrame): The phenotype reference data, recording phenotype f.id and corresponding phenotype name.
        save_path (str, optional): The path to save the result. Defaults to "results/phenotype_frequency_rank".

    Returns:
        pl.LazyFrame: LazyFrame containing the frequency rank of phenotypes.
    """
    RELATED_SNP_COUNT = "related SNPs count"

    rank_lf = (
        result_lf
            .with_columns(
                field_id=pl.col(QassocColumns.PHENOTYPE.value).str.split(".").list.get(1)
            )
            .drop(QassocColumns.PHENOTYPE.value)
            .join(
                reference_lf,
                left_on="field_id",
                right_on=ReferenceColumns.FIELD_ID.value,
                how="inner"
            )
            .unique()
            .with_columns(
                pl.concat_str(
                    [
                        pl.col(QassocColumns.ETHNIC.value),
                        pl.col(QassocColumns.GENDER.value),
                        pl.col(QassocColumns.SNP.value),
                    ],
                    separator="-"
                ).alias("$(ethnic)-$(gender)-$(snp)")
            )
            .group_by(ReferenceColumns.PHENOTYPE.value)
            .agg(
                pl.col("$(ethnic)-$(gender)-$(snp)"),
                pl.col("$(ethnic)-$(gender)-$(snp)").count().alias(RELATED_SNP_COUNT)
            )
            .sort(
                by=RELATED_SNP_COUNT,
                descending=True,
                maintain_order=True,
            )
    )

    rank_lf.collect().write_excel(f"{save_path}.xlsx")

    # plotting
    rank_head_df = rank_lf.head(50).select(
        pl.col(ReferenceColumns.PHENOTYPE.value).alias("phenotype"),
        RELATED_SNP_COUNT
    ).collect()

    fig, ax = plt.subplots()
    fig.set_dpi(150)
    fig.set_size_inches(15, 5)
    ax.xaxis.set_tick_params(rotation=90)

    ax.bar(
        rank_head_df["phenotype"],
        rank_head_df[RELATED_SNP_COUNT],
        linewidth=1,
        width=0.6,
        align="center",
        label="Frequency Rank"
    )

    for i in rank_head_df.iter_rows(named=False):
        ax.text(i[0], i[1], str(i[1]), ha="center", va="bottom")

    ax.set_title(
        "Frequency Rank of Phenotypes' Significant Associations with SNPs"
    )
    ax.set_xlabel("Phenotype")
    ax.set_ylabel("Frequency")

    fig.tight_layout()
    plt.savefig(f"{save_path}.png", dpi=300)

    return rank_lf

def phenotype_snp_pair_rank(
    result_lf: pl.LazyFrame,
    reference_lf: pl.LazyFrame,
    *,
    save_path: str = "results/snp_phenotype_pair_rank"
) -> pl.LazyFrame:
    """
    Calculate the frequency ranking of phenotypes' SNP-phenotype pairs (no duplication)

    Args:
        result_lf (pl.LazyFrame): Input LazyFrame containing GWAS calculation results
            generated by this project.
        reference_lf (pl.LazyFrame): LazyFrame containing phenotype reference data.
        save_path (str): Output path prefix for saving results (will generate .xlsx and
            .png files). Extension will be generated automatically.

    Returns:
        pl.LazyFrame: Processed Lazyrame with phenotypes' SNP-phenotype pair ranked count.
    """
    RELATED_PAIRS_FRQ = "Phenotypes' snp pairs number"

    rank_lf = (
        result_lf
            .with_columns(
                pl.col(QassocColumns.PHENOTYPE.value)
                    .str
                    .split(".")
                    .list
                    .get(1)
                    .alias("field_id")
            )
            .drop(QassocColumns.PHENOTYPE.value)
            .join(
                reference_lf,
                left_on="field_id",
                right_on=ReferenceColumns.FIELD_ID.value,
                how="inner"
            )
            .with_columns(
                pl.concat_str(
                    [
                        pl.col(QassocColumns.ETHNIC.value),
                        pl.col(QassocColumns.GENDER.value),
                    ],
                    separator="-"
                ).alias("$(ethnic)-$(gender)")
            )
            .group_by(QassocColumns.SNP.value, ReferenceColumns.PHENOTYPE.value)
            .agg(pl.col("$(ethnic)-$(gender)").alias("[$($(ethnic)-$(gender))]"))
            .with_columns(
                pl.concat_list(QassocColumns.SNP.value)
            )
            .with_columns(
                pl.concat_list(
                    pl.col("[$($(ethnic)-$(gender))]"),
                    pl.col(QassocColumns.SNP.value),
                )
                    .alias("[$($(ethnic)-$(gender),), $(SNP)]")
            )
            .group_by(ReferenceColumns.PHENOTYPE.value)
            .agg(
                pl.col("[$($(ethnic)-$(gender),), $(SNP)]"),
                pl.col("[$($(ethnic)-$(gender),), $(SNP)]").count().alias(RELATED_PAIRS_FRQ)
            )
            .sort(
                by=RELATED_PAIRS_FRQ,
                descending=True,
                maintain_order=True
            )
    )

    rank_lf.collect().write_excel(f"{save_path}.xlsx")

    # visualisation
    rank_head_df = rank_lf.head(50).select(
        ReferenceColumns.PHENOTYPE.value, RELATED_PAIRS_FRQ
    ).collect()

    fig, ax = plt.subplots()
    fig.set_dpi(150)
    fig.set_size_inches(15, 5)
    ax.get_xaxis().set_tick_params(rotation=90)

    ax.bar(
        rank_head_df[ReferenceColumns.PHENOTYPE.value],
        rank_head_df[RELATED_PAIRS_FRQ],
        linewidth=1,
        width=0.6,
        align="center",
        label="Frequency Rank"
    )

    for i in rank_head_df.iter_rows(named=False):
        ax.text(i[0], i[1], str(i[1]), ha="center", va="bottom")

    ax.set_title(
        "Frequency Rank of Phenotypes' Significant Association with SNPs (Pairwise)"
    )
    ax.set_xlabel("Phenotype")
    ax.set_ylabel("Frequency")

    fig.tight_layout()
    plt.savefig(f"{save_path}.png", dpi=300)

    return rank_lf

def phenotype_ethnicity_pair_rank(
    result_lf: pl.LazyFrame,
    reference_lf: pl.LazyFrame,
    *,
    save_path: str,
) -> pl.LazyFrame:
    """
    Calculate the frequency ranking of phenotype-ethnicity pairs (no duplication).

    Args:
        result_lf (pl.LazyFrame): Input LazyFrame containing GWAS results with SNP and ethnicity data.
        reference_lf (pl.LazyFrame): Input LazyFrame containing SNP frequency rank data.
        save_path (str): Output path prefix for saving results (will generate .xlsx and .png files).

    Returns:
        pl.LazyFrame: LazyFrame containing the frequency ranking of phenotype-ethnicity pairs.
    """
    RELATED_PAIRS_FRQ = "phenotypes' ethnicity pairs frequency"

    rank_lf = (
        result_lf
            .with_columns(
                pl.col(QassocColumns.PHENOTYPE.value)
                    .str
                    .split(".")
                    .list
                    .get(1)
                    .alias("field_id")
            )
            .join(
                reference_lf,
                left_on="field_id",
                right_on=ReferenceColumns.FIELD_ID.value,
                how="inner",
            )
            .unique()
            .with_columns(
                pl.concat_str(
                    [
                        pl.col(QassocColumns.ETHNIC.value),
                        pl.col(QassocColumns.GENDER.value),
                    ],
                    separator="-"
                )
                    .alias("$(ethnic)-$(gender)")
            )
            .group_by(
                ReferenceColumns.PHENOTYPE.value,
                "$(ethnic)-$(gender)"
            )
            .agg(
                pl.col(QassocColumns.SNP.value).alias("[$($(SNP),)]")
            )
            .with_columns(
                pl.concat_list(
                    pl.col("$(ethnic)-$(gender)").alias("[$(ethnic)-$(gender)]")
                )
            )
            .with_columns(
                pl.concat_list(
                    pl.col("[$($(SNP),)]"),
                    pl.col("[$(ethnic)-$(gender)]")
                )
                    .alias("[$($(SNP),), $(ethnic)-$(gender)]")
            )
            .group_by(ReferenceColumns.PHENOTYPE.value)
            .agg(
                pl.col("[$($(SNP),), $(ethnic)-$(gender)]"),
                pl.col("[$($(SNP),), $(ethnic)-$(gender)]").count().alias(RELATED_PAIRS_FRQ)
            )
            .sort(
                by=RELATED_PAIRS_FRQ,
                descending=True,
                maintain_order=True,
            )
    )

    rank_lf.collect().write_excel(f"{save_path}.xlsx")

    rank_head_df = rank_lf.head(50).select(
        pl.col(ReferenceColumns.PHENOTYPE.value).alias("phenotype"),
        pl.col(RELATED_PAIRS_FRQ)
    ).collect()

    fig, ax = plt.subplots()
    fig.set_dpi(150)
    fig.set_size_inches(15, 5)

    ax.xaxis.set_tick_params(rotation=90)

    ax.bar(
        rank_head_df["phenotype"],
        rank_head_df[RELATED_PAIRS_FRQ],
        linewidth=1,
        width=0.6,
        align="center",
        label="Frequency Rank"
    )

    for i in rank_head_df.iter_rows(named=False):
        ax.text(i[0], i[1], str(i[1]), ha="center", va="bottom")

    ax.set_title(
        "Rank of Population Group Numbers with Significant SNP-phenotype Associations"
    )
    ax.set_xlabel("Phenotype")
    ax.set_ylabel("Frequency")

    fig.tight_layout()
    plt.savefig(f"{save_path}.png", dpi=300)

    return rank_lf

    pass
