import unittest, os

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
        
if __name__ == '__main__':
    unittest.main(verbosity=2)