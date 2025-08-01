import os, unittest
import shutil
import time
import functools
from typing import Callable, Any

from Classes import Gender
from myutil.small_tools import count_line
from myutil.visualisations import assoc_mperm_visualisation, assoc_visualisation
from myutil.quality_control import ld_pruning


def timing_decorator(func: Callable) -> Callable:
    """
    装饰器：用于测量并打印函数的执行时间

    Args:
        func (Callable): 需要被装饰的函数

    Returns:
        Callable: 装饰后的函数
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        # print(f"{func.__name__} 执行耗时: {execution_time:.4f} 秒")
        print(f"{execution_time:.2f} s", end="\t", flush=True)
        return result

    return wrapper


#
# Flags
PLINK_PATH: str = ""
# SNP_NUMs: int = 0
INDEPENDENT_SNP_NUMs: int = 25
PERM_COUNTS = 1_000_000
CLEAN_UP: bool = False
FAST = 0


class Test00EnvironmentSetup(unittest.TestCase):

    @staticmethod
    def find_plink_path() -> str | None:
        res = shutil.which("plink")
        if res is None:
            raise FileNotFoundError("plink not found")
        return res

    def setUp(self):
        super().setUp()

    @timing_decorator
    def test_00_find_plink(self):
        global PLINK_PATH

        res = self.find_plink_path()
        if res is None:
            raise FileNotFoundError("plink not found")
        PLINK_PATH = res


class Test02Analysis(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:

        # check file existence
        for extension in [".bed", ".fam", ".bim"]:
            file_name = f"STAB2_white_male_filtered{extension}"
            if not os.path.exists(os.path.join("test_data", file_name)):
                raise FileNotFoundError(f'File "{file_name}" not found.' "")

        if not os.path.exists(os.path.join("test_data", "ld_prune")):
            os.mkdir(os.path.join("test_data", "ld_prune"))

    @timing_decorator
    def test_01_ld_pruning(self):
        global PLINK_PATH
        global INDEPENDENT_SNP_NUMs

        if PLINK_PATH == "":
            raise self.skipTest("plink not found")

        res = ld_pruning(
            PLINK_PATH,
            os.path.join("test_data", "STAB2_white_male_filtered"),
            os.path.join(
                os.path.join(
                    "test_data", "ld_prune", "STAB2_white_male_filtered_pruned"
                )
            ),
        )

        if res is None:
            self.fail("plink failed to execute LD pruning")

        for extension in ["in", "out"]:
            file_path = os.path.join(
                "test_data",
                "ld_prune",
                f"STAB2_white_male_filtered_pruned.prune.{extension}",
            )
            if not os.path.exists(file_path):
                self.fail("Expected pruned file not found at {file_path}")

    @timing_decorator
    def test_02_kept_line_count(self):
        global INDEPENDENT_SNP_NUMs

        count = count_line(
            os.path.join(
                "test_data", "ld_prune", "STAB2_white_male_filtered_pruned.prune.in"
            ),
        )

        if count != INDEPENDENT_SNP_NUMs:
            self.fail("Line count does not match the expected INDEPENDENT_SNP_NUMs")

    @classmethod
    def tearDownClass(cls) -> None:
        if CLEAN_UP:
            shutil.rmtree(os.path.join("test_data", "ld_prune"))


class Test03Visualisation(unittest.TestCase):
    def setUp(self):

        # check file existence
        if not os.path.exists("test_data"):
            raise FileNotFoundError("test_data folder does not exist")

        if not os.path.exists(os.path.join("test_data", "visualisation")):
            os.mkdir(os.path.join("test_data", "visualisation"))

    @timing_decorator
    @unittest.skipUnless(
        os.path.exists("test_data/assoc_mperm.qassoc"),
        "assoc_mperm.qassoc file not found",
    )
    def test_09_assoc_visualisation(self):

        assoc_visualisation(
            "test_data/assoc_mperm.qassoc",
            "test_data/visualisation/assoc",
            gender=Gender.MALE,
            ethnic="British",
            phenotype="f.32820.0.0",
            n=INDEPENDENT_SNP_NUMs,
            alpha=0.05,
        )

    @timing_decorator
    def test_10_mperm_visualisation(self):

        assoc_mperm_visualisation(
            os.path.join("test_data", "assoc_mperm"),
            os.path.join("test_data", "visualisation", "assoc_mperm"),
            gender=Gender.MALE,
            ethnic_name="British",
            phenotype_name="f.32820.0.0",
            n=INDEPENDENT_SNP_NUMs,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        if CLEAN_UP:
            shutil.rmtree(os.path.join("test_data", "visualisation"))


class Test04AssociationAnalysis(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        for ext in ["bed", "bim", "fam"]:
            file = f"test_data/STAB2_white_male_filtered.{ext}"

        for file in [
            f"test_data/STAB2_white_male_filtered.{ext}"
            for ext in ["bed", "bim", "fam"]
        ] + ["test_data/STAB2_standardised_f.30820.0.0.tsv"]:
            if not os.path.exists(file):
                raise FileNotFoundError(f"{file} not found.")

        os.makedirs("test_data/assoc", exist_ok=True)

    @timing_decorator
    def test_01_qassoc_mperm(self):

        if FAST >= 1:
            self.skipTest("FAST >= 1")

        from myutil.association_analysis import quantitative_association

        os.makedirs("test_data/assoc/qassoc_mperm", exist_ok=True)

        _ = quantitative_association(
            PLINK_PATH,
            "test_data/STAB2_white_male_filtered",
            "f.30820",
            "test_data/STAB2_standardised_f.30820.0.0.tsv",
            "test_data/assoc/qassoc_mperm/STAB2_white_male_filtered_f.30820",
            gender=Gender.MALE,
            ethnic="British",
            mperm=PERM_COUNTS,
        )

        for file in [
            f"test_data/assoc/qassoc_mperm/STAB2_white_male_filtered_f.30820.{ext}"
            for ext in ["qassoc", "qassoc.mperm", "qassoc.means"]
        ]:
            if not os.path.exists(file):
                raise FileNotFoundError(f"{file} not found.")

    def test_02_qassoc(self):

        if FAST >= 2:
            self.skipTest("Fast >= 2")

        from myutil.association_analysis import quantitative_association

        os.makedirs("test_data/assoc/qassoc", exist_ok=True)

        _ = quantitative_association(
            PLINK_PATH,
            "test_data/STAB2_white_male_filtered",
            "f.30820",
            "test_data/STAB2_standardised_f.30820.0.0.tsv",
            "test_data/assoc/qassoc/STAB2_white_male_filtered_f.30820",
            gender=Gender.MALE,
            ethnic="British",
            mperm=None,
        )

        for file in [
            f"test_data/assoc/qassoc_mperm/STAB2_white_male_filtered_f.30820.{ext}"
            for ext in ["qassoc", "qassoc.means"]
        ]:
            if not os.path.exists(file):
                raise FileNotFoundError(f"{file} not found.")


class Test05Summarization(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # check file existence
        if not os.path.exists("test_data"):
            raise FileNotFoundError("test_data folder does not exist")

        for file in [
            "test_data/assoc_mperm.qassoc",
            "test_data/assoc_mperm.qassoc.mperm",
            "test_data/assoc_mperm.qassoc.means",
        ]:
            if not os.path.exists(file):
                raise FileNotFoundError(f"File {file} does not exist")

        # make directorys
        os.makedirs("test_data/summary", exist_ok=True)

    @timing_decorator
    def test_01_qassoc_mperm_qt_means_summary(self):
        from myutil.summarization import generate_quantitative_summary, QassocResult

        os.makedirs("test_data/summary/qassoc_mperm_qt-means", exist_ok=True)

        generate_quantitative_summary(
            [
                QassocResult(
                    qassoc_path="test_data/assoc_mperm.qassoc",
                    mperm_path="test_data/assoc_mperm.qassoc.mperm",
                    qt_means_path="test_data/assoc_mperm.qassoc.means",
                    gender=Gender.MALE,
                    ethnic_name="British",
                    phenotype_name="f.32820.0.0",
                )
            ],
            bonferroni_n=INDEPENDENT_SNP_NUMs,
            alpha=0.05,
            output_prefix="test_data/summary/qassoc_mperm_qt-means/summary",
        )

        assert os.path.exists("test_data/summary/qassoc_mperm_qt-means/summary-q.tsv")
        assert os.path.exists("test_data/summary/qassoc_mperm_qt-means/summary-q-significant.tsv")
        assert os.path.exists("test_data/summary/qassoc_mperm_qt-means/summary-qt_means.tsv")
        assert os.path.exists("test_data/summary/qassoc_mperm_qt-means/summary-qt_means-significant.tsv")

    @timing_decorator
    def test_02_qassoc_mperm_summary(self):
        from myutil.summarization import generate_quantitative_summary, QassocResult

        os.makedirs("test_data/summary/qassoc_mperm", exist_ok=True)

        generate_quantitative_summary(
            [
                QassocResult(
                    qassoc_path="test_data/assoc_mperm.qassoc",
                    mperm_path="test_data/assoc_mperm.qassoc.mperm",
                    qt_means_path=None,
                    gender=Gender.MALE,
                    ethnic_name="British",
                    phenotype_name="f.32820.0.0",
                )
            ],
            bonferroni_n=INDEPENDENT_SNP_NUMs,
            alpha=0.05,
            output_prefix="test_data/summary/qassoc_mperm/summary",
        )

        assert os.path.exists("test_data/summary/qassoc_mperm/summary-q.tsv")
        assert os.path.exists("test_data/summary/qassoc_mperm/summary-q-significant.tsv")

    @timing_decorator
    def test_03_qassoc_qt_means_summary(self):
        from myutil.summarization import generate_quantitative_summary, QassocResult

        os.makedirs("test_data/summary/qassoc_qt-means", exist_ok=True)

        generate_quantitative_summary(
            [
                QassocResult(
                    qassoc_path="test_data/assoc_mperm.qassoc",
                    mperm_path=None,
                    qt_means_path="test_data/assoc_mperm.qassoc.means",
                    gender=Gender.MALE,
                    ethnic_name="British",
                    phenotype_name="f.32820.0.0",
                )
            ],
            bonferroni_n=INDEPENDENT_SNP_NUMs,
            alpha=0.05,
            output_prefix="test_data/summary/qassoc_qt-means/summary",
        )

        assert os.path.exists("test_data/summary/qassoc_qt-means/summary-q.tsv")
        assert os.path.exists("test_data/summary/qassoc_qt-means/summary-q-significant.tsv")
        assert os.path.exists("test_data/summary/qassoc_qt-means/summary-qt_means.tsv")
        assert os.path.exists("test_data/summary/qassoc_qt-means/summary-qt_means-significant.tsv")

    @classmethod
    def tearDownClass(cls) -> None:
        if CLEAN_UP:
            shutil.rmtree(os.path.join("test_data", "summary"))


if __name__ == "__main__":

    # CLEAN_UP = True

    unittest.main()
