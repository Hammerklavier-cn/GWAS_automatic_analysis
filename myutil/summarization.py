from dataclasses import dataclass
import logging, os
import sys
import polars as pl

from Classes import Gender


@dataclass
class QassocResult:
    """Results from `plink --assoc`

    Attributes:
        qassoc_path (str): Path to the `qassoc` file.
        qt_means_path (str | None): Path to the `qt_means` file.
        mperm_path (str | None): Path to the `mperm` file.
        gender (Gender): Gender.
        ethnic_name (str): Ethnicity.
        phenotype_name (str): Name of the phenotype.

    Note that file extensions are expected for all three files.
    """

    qassoc_path: str
    qt_means_path: str | None
    mperm_path: str | None
    gender: Gender
    ethnic_name: str
    phenotype_name: str


def generate_quantitative_summary(
    qassoc_results: list[QassocResult],
    bonferroni_n: int,
    alpha: float,
    output_prefix: str,
) -> None:
    """Generate a summary of the quantitative association analysis, including
    `summary-q.tsv` and `summary-qt_means.tsv` (if `qt_means_path` is not None).

    Args:
        qassoc_results (list[QassocResult]): List of QassocResult objects.
        bonferroni_n (int): Bonferroni's correction factor.
        output_prefix (str): Prefix for the output files.
    """
    concat_qassoc_results_list: list[pl.DataFrame] = []
    sig_concat_qassoc_results_list: list[pl.DataFrame] = []
    concat_qassoc_header: list[str] = []

    qt_mean_results_list: list[pl.DataFrame] = []
    sig_qt_mean_results_list: list[pl.DataFrame] = []

    for qassoc_res in qassoc_results:
        concat_qassoc_res, qt_mean_res = _concat_qassoc_mperm_mean(
            qassoc_res, bonferroni_n
        )

        # Append `concat_qassoc_results`
        match concat_qassoc_header:
            case []:
                concat_qassoc_header = concat_qassoc_res.columns
            case list() as header:
                assert header == concat_qassoc_res.columns

        concat_qassoc_results_list.append(concat_qassoc_res)

        # Append `qt_mean_results`
        if qt_mean_res is not None:
            qt_mean_results_list.append(qt_mean_res)

        # Find out significant SNP
        sig_concat_qassoc_res = concat_qassoc_res.filter(
            pl.col("PERM_P_2" if "PERM_P_2" in concat_qassoc_header else "P'") < alpha
        )

        if sig_concat_qassoc_res.height > 0:
            # Append `sig_concat_qassoc_results`
            sig_concat_qassoc_results_list.append(sig_concat_qassoc_res)

            if qt_mean_res is not None:
                # Append `sig_qt_mean_results`
                sig_qt_mean_res = qt_mean_res.join(
                    sig_concat_qassoc_res.select(["CHR", "SNP"]),
                    on=["CHR", "SNP"],
                    how="inner",
                )
                if sig_qt_mean_res.height > 0:
                    sig_qt_mean_results_list.append(sig_qt_mean_res)

    # concat results and write to files
    logging.info("saving qassoc summary to %s-q.tsv", output_prefix)
    concat_qassoc_results_df = pl.concat(
        concat_qassoc_results_list, how="vertical", rechunk=True
    )
    concat_qassoc_results_df.write_csv(
        f"{output_prefix}-q.tsv", separator="\t", include_header=True
    )

    logging.info(
        "saving significant qassoc summary to %s-q-significant.tsv", output_prefix
    )
    if len(sig_concat_qassoc_results_list) > 0:
        sig_concat_qassoc_results_df = pl.concat(
            sig_concat_qassoc_results_list, how="vertical", rechunk=True
        )
        sig_concat_qassoc_results_df.write_csv(
            f"{output_prefix}-q-significant.tsv", separator="\t", include_header=True
        )
    else:
        logging.info(
            "No significant results found, skipping %s-q-significant.tsv", output_prefix
        )

    logging.info("saving qt-means summary to %s-qt_means.tsv", output_prefix)
    if len(qt_mean_results_list) > 0:
        concat_qt_mean_results_df = pl.concat(
            qt_mean_results_list, how="vertical", rechunk=True
        )
        concat_qt_mean_results_df.write_csv(
            f"{output_prefix}-qt_means.tsv", separator="\t", include_header=True
        )
    else:
        logging.info(
            "No qt_means results found, skipping %s-qt_means.tsv", output_prefix
        )

    logging.info(
        "saving significant qt-means summary to %s-qt_means-significant.tsv",
        output_prefix,
    )
    if len(sig_qt_mean_results_list) > 0:
        sig_qt_mean_results_df = pl.concat(
            sig_qt_mean_results_list, how="vertical", rechunk=True
        )
        sig_qt_mean_results_df.write_csv(
            f"{output_prefix}-qt_means-significant.tsv",
            separator="\t",
            include_header=True,
        )
    else:
        logging.info(
            "No significant qt_means results found, skipping %s-qt_means-significant.tsv",
            output_prefix,
        )


def _concat_qassoc_mperm_mean(
    qassoc_result: QassocResult,
    bonferroni_n: int,
) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    """Join *.qassoc*, *.qassoc.mperm* (if exists), *.qassoc.mean* (if exists) files

    Args:
        qassoc_result (QassocResult): QassocResult object.
        bonferroni_n (int): Bonferroni's correction factor.
    """
    logging.debug(
        "Generating qassoc summary for %s %s, %s",
        qassoc_result.gender.value,
        qassoc_result.ethnic_name,
        qassoc_result.phenotype_name,
    )

    if not os.path.exists(qassoc_result.qassoc_path):
        logging.error("File '%s' not found.", qassoc_result.qassoc_path)
        raise FileNotFoundError(f"File '{qassoc_result.qassoc_path}' not found.")
    if qassoc_result.mperm_path is not None and not os.path.exists(
        qassoc_result.mperm_path
    ):
        logging.error("File '%s' not found.", qassoc_result.mperm_path)
        raise FileNotFoundError(f"File '{qassoc_result.mperm_path}' not found.")
    if qassoc_result.qt_means_path is not None and not os.path.exists(
        qassoc_result.qt_means_path
    ):
        logging.error("File '%s' not found.", qassoc_result.qt_means_path)
        raise FileNotFoundError(f"File '{qassoc_result.qt_means_path}' not found.")

    qassoc_df = _parse_qassoc_file(qassoc_result.qassoc_path)
    qassoc_df = qassoc_df.with_columns(pl.col("P").mul(bonferroni_n).alias("P'"))

    if qassoc_result.mperm_path is not None:
        mperm_df = _parse_mperm_file(qassoc_result.mperm_path).rename(
            {"EMP1": "PERM_P_1", "EMP2": "PERM_P_2"}
        )

        qassoc_df = qassoc_df.join(
            mperm_df,
            on=["CHR", "SNP"],
            how="inner",
        )

    qassoc_df = qassoc_df.with_columns(
        pl.lit(qassoc_result.gender.value).alias("gender"),
        pl.lit(qassoc_result.ethnic_name).alias("ethnic"),
        pl.lit(qassoc_result.phenotype_name).alias("phenotype"),
    )

    if qassoc_result.qt_means_path is not None:
        qt_means_df = _parse_qt_means_file(qassoc_result.qt_means_path)

        qt_means_mean_df = qt_means_df.filter(pl.col("VALUE") == "MEAN")

        assert qt_means_mean_df.height > 0

        qassoc_df = qassoc_df.join(
            qt_means_mean_df.select(
                pl.col("CHR"),
                pl.col("SNP"),
                pl.col("G11"),
                pl.col("G12"),
                pl.col("G22"),
            ),
            on=["CHR", "SNP"],
            how="inner",
        )

    else:
        qt_means_df = None

    match qassoc_result.mperm_path, qassoc_result.qt_means_path:
        case str(), str():
            expected_columns = [
                "CHR",
                "SNP",
                "BP",
                "NMISS",
                "BETA",
                "SE",
                "R2",
                "T",
                "P",
                "P'",
                "PERM_P_1",
                "PERM_P_2",
                "gender",
                "ethnic",
                "phenotype",
                "G11",
                "G12",
                "G22",
            ]
            try:
                assert qassoc_df.columns == expected_columns
            except AssertionError:
                logging.error(
                    "Skipping %s due to mismatched columns: Expected %s, got %s",
                    qassoc_result.mperm_path,
                    expected_columns,
                    qassoc_df.columns,
                )
                sys.exit(1)
        case None, str():
            assert qassoc_df.columns == [
                "CHR",
                "SNP",
                "BP",
                "NMISS",
                "BETA",
                "SE",
                "R2",
                "T",
                "P",
                "P'",
                "gender",
                "ethnic",
                "phenotype",
                "G11",
                "G12",
                "G22",
            ]
        case str(), None:
            assert qassoc_df.columns == [
                "CHR",
                "SNP",
                "BP",
                "NMISS",
                "BETA",
                "SE",
                "R2",
                "T",
                "P",
                "P'",
                "PERM_P_1",
                "PERM_P_2",
                "gender",
                "ethnic",
                "phenotype",
            ]
        case None, None:
            assert qassoc_df.columns == [
                "CHR",
                "SNP",
                "BP",
                "NMISS",
                "BETA",
                "SE",
                "R2",
                "T",
                "P",
                "P'",
                "gender",
                "ethnic",
                "phenotype",
            ]
        case _:
            raise Exception("Theoretically unreachable!")

    return qassoc_df, qt_means_df


def _parse_qassoc_file(qassoc_path: str) -> pl.DataFrame:
    """
    Parse a `.qassoc` file into a pl.DataFrame

    Args:
        qassoc_path (str): Path to the .qassoc file.

    Returns:
        pl.DataFrame: Parsed DataFrame with columns:
            - CHR: Chromosome
            - SNP: SNP identifier
            - BP: Base pair position
            - NMISS: Number of non-missing samples
            - BETA: Regression coefficient
            - SE: Standard error
            - R2: R-squared value
            - T: T-statistic
            - P: P-value
    """

    logging.debug("Parsing .qassoc file: %s", qassoc_path)

    if not os.path.exists(qassoc_path):
        logging.error("File '%s' not found.", qassoc_path)
        raise FileNotFoundError(f"File '{qassoc_path}' not found.")

    qassoc_df = pl.read_csv(
        qassoc_path,
        has_header=False,
        skip_rows=1,
        new_columns=["whole_line"],
    )
    qassoc_df = qassoc_df.select(
        pl.col("whole_line")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .str.split(" ")
    )

    assert (
        len(qassoc_df.select(pl.col("whole_line").first()).to_series().to_list()[0])
        == 9
    )

    headers = ["CHR", "SNP", "BP", "NMISS", "BETA", "SE", "R2", "T", "P"]
    qassoc_df = qassoc_df.select(
        [
            pl.col("whole_line").list.get(i).alias(header)
            for i, header in enumerate(headers)
        ]
    ).with_columns(
        pl.col("BP").cast(pl.Int64),
        pl.col("NMISS").cast(pl.UInt32),
        pl.col("BETA").cast(pl.Float64),
        pl.col("SE").cast(pl.Float64),
        pl.col("R2").cast(pl.Float64),
        pl.col("T").cast(pl.Float64),
        pl.col("P").cast(pl.Float64),
    )

    return qassoc_df


def _parse_mperm_file(mperm_path) -> pl.DataFrame:
    """
    Parse a `.qassoc.mperm` file into a pl.DataFrame

    Args:
        mperm_path (str): Path to the .qassoc.mperm file.

    Returns:
        pl.DataFrame: Parsed DataFrame with columns:
            - CHR: Chromosome
            - SNP: SNP identifier
            - EMP1: Empirical p-value (pointwise), or lower-p-value permutation count
            - EMP2: Corrected empirical p-value (max(T) familywise) or permutation count
    """
    logging.debug("Parsing .mperm file: %s", mperm_path)

    if not os.path.exists(mperm_path):
        logging.error("File '%s' not found.", mperm_path)
        raise FileNotFoundError(f"File '{mperm_path}' not found.")

    mperm_df = pl.read_csv(
        mperm_path,
        has_header=False,
        skip_rows=1,
        new_columns=["whole_line"],
    )
    mperm_df = mperm_df.select(
        pl.col("whole_line")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .str.split(" ")
    )

    assert (
        len(mperm_df.select(pl.col("whole_line").first()).to_series().to_list()[0]) == 4
    )

    headers = ["CHR", "SNP", "EMP1", "EMP2"]
    mperm_df = mperm_df.select(
        [
            pl.col("whole_line").list.get(i).alias(header)
            for i, header in enumerate(headers)
        ]
    ).with_columns(pl.col("EMP1").cast(pl.Float64), pl.col("EMP2").cast(pl.Float64))

    return mperm_df


def _parse_qt_means_file(qt_means_path: str) -> pl.DataFrame:
    """Parses a .qassoc.means file into a DataFrame.

    Args:
        qt_means_path (str): Path to the .qassoc.means file.

    Returns:
        pl.DataFrame: Parsed DataFrame with columns:
            - CHR
            - SNP
            - VALUE
            - G11
            - G12
            - G22
    """
    if not os.path.exists(qt_means_path):
        raise FileNotFoundError(f"File '{qt_means_path}' not found.")

    qt_means_df = pl.read_csv(
        qt_means_path,
        has_header=False,
        skip_rows=1,
        new_columns=["whole_line"],
    )
    qt_means_df = qt_means_df.select(
        pl.col("whole_line")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .str.split(" ")
    )

    assert (
        len(qt_means_df.select(pl.col("whole_line").first()).to_series().to_list()[0])
        == 6
    )

    headers = ["CHR", "SNP", "VALUE", "G11", "G12", "G22"]
    return qt_means_df.select(
        [
            pl.col("whole_line").list.get(i).alias(header)
            for i, header in enumerate(headers)
        ]
    )
