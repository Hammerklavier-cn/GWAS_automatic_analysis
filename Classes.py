import os, logging


class FileManagement(object):
    def __init__(self, file_path: str) -> None:
        """Get general information of files

        Args:
            file_path (str): relative or absolute path of source file.
        """
        self.absolute_path = os.path.abspath(file_path)
        self.file_dir = os.path.dirname(self.absolute_path)
        self.file_name, self.format = os.path.splitext(self.absolute_path)
        logging.debug("File is located in %s. File name is %s, format is %s", self.file_dir, self.file_name, self.format)
        pass
    