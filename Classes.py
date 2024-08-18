import os, logging
import subprocess


class FileManagement(object):
    def __init__(self, file_path: str, plink_path: str) -> None:
        """Get general information of files

        Args:
            file_path (str): relative or absolute path of source file.
        """
        self.relative_path = os.path.abspath(file_path)
        self.file_dir = os.path.dirname(self.relative_path)
        self.file_name_root, self.original_ext = os.path.splitext(self.relative_path)
        self.output_name_root = os.path.basename(self.file_name_root)
        if self.original_ext == ".gz":
            if self.file_name_root.endswith(".vcf"):
                self.file_name_root = os.path.splitext(self.file_name_root)[0]
                self.original_ext = ".vcf.gz"
            else:
                logging.error("This input file is not a compressed .vcf file!")
                raise Exception(f"expected a `.vcf.gz` file. Input file is {file_path}")
        logging.debug(
            "File is located in %s. File name is %s, format is %s", 
                self.file_dir, self.file_name_root, self.original_ext
        )
        
        self.plink = os.path.abspath(plink_path)
        pass
    
    def source_standardisation(self):
        """
        Convert .vcf to plink binary format.
        It will generate a `.bed`, a `.fam` and a `.bim` file (or symlink).
        """
        
        if self.original_ext in [".vcf", ".vcf.gz"]:
            command = [
                self.plink, 
                "--vcf", self.file_name_root+self.original_ext,
                "--make-bed", 
                "--vcf-half-call", "missing",
                "--out", self.output_name_root + "_standardised"
            ]
            process = subprocess.run(
                command,
                check=True,
                stdout=True,
                stderr=True,
                text=True
            )
        elif self.original_ext == ".bed":
            try:
                for i in [".bed", ".fam", ".bim"]:
                    if not os.path.isfile(os.path.realpath(self.file_name_root + ".fam")):
                        raise Exception(f"`{self.file_name_root}.fam` does not exist!")
                    os.symlink(self.file_name_root + i,
                            self.output_name_root + "_standardised" + i)
                    if not os.path.isfile(os.path.realpath(f"{self.file_name_root}_standardised{i}")):
                        raise Exception(f"The symlink `{self.output_name_root}_standardised{i}` is invalid!")
            except Exception as err:
                logging.error(
                    "An error occurred when creating symlink for standardised plink binary file: %s",
                    err
                )
                os.exit(-2)
        elif self.original_ext == ".ped":
            if os.path.isfile(f"{self.file_name_root}.map"):
                process = subprocess.run(
                    [
                        self.plink,
                        "--file", self.file_name_root + self.original_ext,
                        "--make-bed",
                        "--out", self.output_name_root
                    ],
                    check=True,
                    capture_output=True
                )
            else:
                logging.error(
                    f"Expecting a `.map` file with the same root of {self.relative_path}!"
                )
                os.exit(1)
            pass
        else:
            logging.fatal("Unsupported format! Format check should be done earlier. \
                Please report the defect to \
                    <https://gitcode.com/hammerklavier/GWAS_automatic_analysis/issues>.")
            os.exit(-1)
