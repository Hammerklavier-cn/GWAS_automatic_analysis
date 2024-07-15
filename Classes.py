import os, logging
import subprocess


class FileManagement(object):
    def __init__(self, file_path: str, plink_path: str) -> None:
        """Get general information of files

        Args:
            file_path (str): relative or absolute path of source file.
        """
        self.absolute_path = os.path.abspath(file_path)
        self.file_dir = os.path.dirname(self.absolute_path)
        self.file_name, self.original_format = os.path.splitext(self.absolute_path)
        logging.debug(
            "File is located in %s. File name is %s, format is %s", 
                self.file_dir, self.file_name, self.original_format
        )
        
        self.plink = os.path.abspath(plink_path)
        pass
    
    def source_standardisation(self):
        # convert .vcf to plink binary format
        if self.original_format in [".vcf", ".vcf.gz"]:
            process = subprocess.run(
                [
                    self.plink, 
                    "--vcf", self.file_name+self.original_format,
                    "--make bed", 
                    "--out", self.file_name + "standardised"
                ],
                check=True,
                capture_output=True
            )
        elif self.original_format == ".bed":
            try:
                for i in [".bed", ".fam", ".bim"]:
                    os.symlink(self.file_name+i,
                            self.file_name+"standardised"+i)
                    if not os.path.isfile(os.path.realpath()):
                        raise Exception("The symlink created just now is invalid!")
            except Exception as err:
                logging.error(
                    "An error occurred when creating symlink for standardised plink binary file: %s",
                    err
                )
                os._exit(-1)
        elif self.original_format == ".ped":
            pass
        else:
            logging.fatal("Unsupported format! Format check should be done earlier. \
                Please report the defect to \
                    <https://gitcode.com/hammerklavier/GWAS_automatic_analysis/issues>.")
            os._exit(-1)
    