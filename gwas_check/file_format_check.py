import os, logging

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s -- %(levelname)s -- %(message)s"
)

'''def source_file_check(file_path: str):
    logging.debug(f"{os.getcwd()=}")
    absolute_path = os.path.realpath(file_path)
    path_head, file_format = os.path.split(absolute_path)
    logging.info(f"Head of path is {path_head}, tail of path is {file_format}")
    return absolute_path path_head, file_format'''