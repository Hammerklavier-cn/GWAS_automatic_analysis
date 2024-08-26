import unittest, os

class MyTestSuite(unittest.TestSuite):
    def __init__(self) -> None:
        super().__init__()
        self.addTest(Test("test_working_directory"))
        self.addTest(Test("test_files_for_tests"))
        self.addTest(Test("test_files_for_tests_deprecated"))
        self.addTest(Test("test_source_file_standardisation"))
        self.addTest(Test("test_ethnic_grouping"))

class Test(unittest.TestCase):
    def setUp(self) -> None:
        # set working directory
        os.chdir(os.path.join(os.path.dirname(__file__), "../test"))
        pass

    def test_working_directory(self):
        self.assertEqual(
            os.getcwd(), 
            os.path.realpath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)), "test")
                )
            )

    def test_files_for_tests_deprecated(self):
        # at present, which shall be replaced:
        self.assertTrue(os.path.exists("../testsuite/Ethnic background.xlsx"))
        self.assertTrue(os.path.exists("../testsuite/Red blood cell count.xlsx"))

    @unittest.expectedFailure
    def test_files_for_tests(self):
        # check files
        self.assertTrue(os.path.exists("../testsuit/hashsum.csv"))
        self.assertTrue(os.path.exists("../testsuit/test_data.vcf.gz"))
        self.assertTrue(os.path.exists("../testsuit/test_data.bed"))
        self.assertTrue(os.path.exists("../testsuit/test_data.bim"))
        self.assertTrue(os.path.exists("../testsuit/test_data.fam"))
        self.assertTrue(os.path.exists("../testsuit/test_data.ped"))
        self.assertTrue(os.path.exists("../testsuit/test_data.map"))
        self.assertTrue(os.path.exists("../testsuit/test_data_with-phenotype.bed"))
        self.assertTrue(os.path.exists("../testsuit/test_data_with-phenotype.bim"))
        self.assertTrue(os.path.exists("../testsuit/test_data_with-phenotype.fam"))
        self.assertTrue(os.path.exists("../testsuit/test_data_with-phenotype.ped"))
        self.assertTrue(os.path.exists("../testsuit/test_data_with-phenotype.map"))
        self.assertTrue(os.path.exists("../testsuit/ethnics_background.xlsx"))
        self.assertTrue(os.path.exists("../testsuit/ethnic_serial_reference.tsv"))
        self.assertTrue(os.path.exists("../testsuit/phenotype.xlsx"))

    @unittest.skipUnless(os.path.exists("../testsuit/test_data.vcf.gz"), "VCF file not found")
    def test_source_file_standardisation(self):
        pass
    
    def test_ethnic_grouping(self):
        pass
        
if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(MyTestSuite())
    # unittest.main(verbosity=2)