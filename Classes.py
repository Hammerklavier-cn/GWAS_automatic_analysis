from argparse import Namespace
from ast import Call
import os, logging, sys
import subprocess


class FileManagement(object):
    def __init__(self, args: Namespace) -> None:
        """Get general information of files

        Args:
            args (Namespace): Namespace object, which contains all arguments.
        """
        file_path = args.file_name
        plink_path = args.plink_path
        self.absolute_path = os.path.abspath(file_path)
        self.file_dir = os.path.dirname(self.absolute_path)
        self.file_name_root, self.original_ext = os.path.splitext(self.absolute_path)
        self.output_name_root = os.path.basename(self.file_name_root)
        self.output_name_temp_root = os.path.join("./temp",self.output_name_root)

        os.makedirs("./temp", exist_ok=True)

        if self.original_ext == ".gz":
            if self.file_name_root.endswith(".vcf"):
                self.file_name_root = os.path.splitext(self.file_name_root)[0]
                self.original_ext = ".vcf.gz"
                self.output_name_root = os.path.splitext(self.output_name_root)[0]
                self.output_name_temp_root = os.path.splitext(self.output_name_temp_root)[0]
            else:
                logging.error("This input file is not a compressed .vcf file!")
                raise Exception(f"expected a `.vcf.gz` file. Input file is {file_path}")
        logging.debug(
            "File is located in %s. File name is %s, format is %s",
                self.file_dir, self.file_name_root, self.original_ext
        )

        self.plink: str = plink_path

        self.phenotype_file_path: str = os.path.realpath(args.phenotype)
        self.ethnic_info_file_path: str = os.path.realpath(args.ethnic)
        self.ethnic_reference_path: str = os.path.realpath(args.ethnic_reference)
        self.gender_info_file_path: str = os.path.realpath(args.gender)
        self.gender_reference_path: str = os.path.realpath(args.gender_reference)
        self.loose_ethnic_filter: bool = args.loose_ethnic_filter
        pass

    def source_standardisation(self) -> str:
        """
        Convert .vcf to plink binary format.
        It will generate a `.bed`, a `.fam` and a `.bim` file (or symlink).
        """

        if self.original_ext in [".vcf", ".vcf.gz"]:
            logging.info("Converting .vcf to plink binary format... This may take a long time.")
            command = [
                self.plink,
                "--vcf", self.file_name_root + self.original_ext,
                "--make-bed",
                "--vcf-half-call", "missing",
                "--out", self.output_name_temp_root + "_standardised"
            ]
            try:
                process = subprocess.run(
                    command,
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT
                )
            except subprocess.CalledProcessError as err:
                logging.error(
                    "An error occurred when converting .vcf to plink binary file: %s",
                    err
                )
                sys.exit(-2)
            except Exception as err:
                logging.error(
                    "Unexpected error occurred when converting .vcf to plink binary file: %s",
                    err
                )
                sys.exit(-3)
            logging.info("Conversion completed.")

        elif self.original_ext == ".bed":
            try:
                for ext in [".bed", ".fam", ".bim"]:
                    if not os.path.isfile(os.path.realpath(self.file_name_root + ext)):
                        raise Exception(f"`{self.file_name_root}{ext}` does not exist!")
                    os.symlink(self.file_name_root + ext,
                            self.output_name_temp_root + "_standardised" + ext)
                    if not os.path.isfile(os.path.realpath(f"{self.output_name_temp_root}_standardised{ext}")):
                        raise Exception(f"The symlink `{self.output_name_temp_root}_standardised{ext}` is invalid!")
            except Exception as err:
                logging.error(
                    "An error occurred when creating symlink for standardised plink binary file: %s",
                    err
                )
                sys.exit(-2)

        elif self.original_ext == ".ped":
            if os.path.isfile(f"{self.file_name_root}.map"):
                try:
                    process = subprocess.run(
                        [
                            self.plink,
                            "--file", self.file_name_root + self.original_ext,
                            "--make-bed",
                            "--out", self.output_name_temp_root
                        ],
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.STDOUT,
                    )
                except subprocess.CalledProcessError as err:
                    logging.error(
                        "An error occurred when converting .ped to plink binary file: %s",
                        err
                    )
                    sys.exit(-2)
                except Exception as err:
                    logging.error(
                        "Unexpected error occurred when converting .ped to plink binary file: %s",
                        err
                    )
                    sys.exit(-3)
            else:
                logging.error(
                    f"Expecting a `.map` file with the same root of {self.absolute_path}!"
                )
                sys.exit(1)
            pass

        else:
            logging.fatal("Unsupported format! Format check should be done earlier. \
                Please report the defect to \
                    <https://gitcode.com/hammerklavier/GWAS_automatic_analysis/issues>.")
            sys.exit(-1)

        self.set_working_file(f"{self.output_name_temp_root}_standardised")
        return f"{self.output_name_temp_root}_standardised"
    def phenotype_standardisation(self):
        pass

    def ethnic_grouping(self):

        pass

    def quality_control(self):
        pass

    def set_working_file(self, file_root: str) -> None:
        # check
        if not os.path.exists(f"{file_root}.bed"):
            logging.error("The file %s does not exist!", file_root)
            sys.exit(-1)
        self.working_file = file_root
