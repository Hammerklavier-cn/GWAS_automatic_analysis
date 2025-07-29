import os, unittest
import shutil
import time
import functools
from typing import Callable, Any

from Classes import Gender
from myutil.small_tools import count_line
from myutil.visualisations import assoc_mperm_visualisation
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
CLEAN_UP: bool = False


class Test00EnvironmentSetup(unittest.TestCase):

    @staticmethod
    def find_plink_path() -> str | None:
        res = shutil.which("plink")
        if res is None:
            raise FileNotFoundError("plink not found")
        return res

    def setUp(self):
        super().setUp()

    def test_find_plink(self):
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
            Gender.MALE,
            "White",
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
        global CLEAN_UP
        if CLEAN_UP:
            shutil.rmtree(os.path.join("test_data", "ld_prune"))


class Test03Visualisation(unittest.TestCase):
    def setUp(self):

        # check file existence
        if not os.path.exists("test_data"):
            raise FileNotFoundError("test_data folder does not exist")

        if not os.path.exists(os.path.join("test_data", "visualisation")):
            os.mkdir(os.path.join("test_data", "visualisation"))

    def test_10_mperm_visualisation(self):
        global INDEPENDENT_SNP_NUMs

        if INDEPENDENT_SNP_NUMs == 0:
            self.fail("More than one independent SNP is expected!")

        assoc_mperm_visualisation(
            os.path.join("test_data", "assoc_mperm"),
            os.path.join("test_data", "visualisation", "assoc_mperm"),
            gender=Gender.MALE,
            ethnic_name="British",
            phenotype_name="f.32820.0.0",
            n=INDEPENDENT_SNP_NUMs,  # find this out before this function!
        )
        pass

    def tearDown(self):
        global CLEAN_UP
        if CLEAN_UP:
            shutil.rmtree(os.path.join("test_data", "visualisation"))


if __name__ == "__main__":

    # CLEAN_UP = True

    unittest.main()
