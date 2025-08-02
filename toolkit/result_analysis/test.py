import unittest
import os
import libutils
import polars as pl

INPUT_FILES = (
    "../../results/MRC1-clean_ethnic-results/MRC1-part3-split_gender/summary-q_adjusted.tsv",
    "../../results/MRC1-clean_ethnic-results/MRC1-part2-split_gender/summary-q_adjusted.tsv",
    "../../results/MRC1-clean_ethnic-results/MRC1-part1-split_gender/summary-q_adjusted.tsv",
)
REFERENCE_FILE = "run/reference.tsv"


def file_is_exists(file_path: str) -> bool:
    if os.path.isfile(file_path):
        return True
    else:
        return False


class TestReadInput(unittest.TestCase):
    def setUp(self) -> None:
        self.input_files = INPUT_FILES
        self.reference_file = REFERENCE_FILE

    def test_file_exists(self):

        for file in self.input_files:
            self.assertTrue(file_is_exists(file))
        self.assertTrue(file_is_exists(self.reference_file))

    def test_read_reference(self):

        ref_lf = libutils.read_reference_data(self.reference_file)

        self.assertEqual(
            ref_lf.collect_schema(),
            {
                "field_id": pl.Utf8,
                "phenotype_name": pl.Utf8
            }
        )

    def test_read_input(self):

        input_lf = libutils.read_gwas_results(list(self.input_files))

class TestAnalyse(unittest.TestCase):
    def setUp(self) -> None:
        self.input_lf = libutils.read_gwas_results(list(INPUT_FILES))
        self.reference_lf = libutils.read_reference_data(REFERENCE_FILE)

        os.makedirs("test_results", exist_ok=True)

    def test_snp_frequency_rank(self):
        libutils.snp_frequency_rank(self.input_lf, self.reference_lf, save_path="test_results/snp-freq-rank")

    def test_snp_phenotype_pair_rank(self):
        libutils.snp_phenotype_pair_rank(self.input_lf, self.reference_lf, save_path="test_results/snp-phenotype-pair-rank")

    def test_snp_ethnicity_pair_rank(self):
        libutils.snp_ethnicity_pair_rank(self.input_lf, self.reference_lf, save_path="test_results/snp-ethnicity-pair-rank")

    def test_snp_phenotype_duplication_rank(self):
        libutils.snp_phenotype_duplication_rank(self.input_lf, save_path="test_results/snp-phenotype-duplication-rank")

    def test_phenotype_frequency_rank(self):
        libutils.phenotype_frequency_rank(self.input_lf, self.reference_lf, save_path="test_results/phenotype-frequency-rank")

    def test_phenotype_snp_pair_rank(self):
        libutils.phenotype_snp_pair_rank(self.input_lf, self.reference_lf, save_path="test_results/phenotype-snp-pair-rank")

    def test_phenotype_ethnicity_pair_rank(self):
        libutils.phenotype_ethnicity_pair_rank(self.input_lf, self.reference_lf, save_path="test_results/phenotype-ethnicity-pair-rank")

    def test_save_merged(self):
        self.input_lf.collect().write_excel("test_results/merged_results.xlsx")



if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestReadInput))
    suite.addTests(loader.loadTestsFromTestCase(TestAnalyse))
    unittest.TextTestRunner(verbosity=2).run(suite)
