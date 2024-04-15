#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

from io import BytesIO
from typing import Tuple, Dict
from zipfile import ZipFile, ZIP_BZIP2


def extract_zip_content(zip_file: bytes) -> Tuple[str, str]:
    """
    Extracts the content of a zip file.
    :param zip_file:  The content of the zip file.
    :return:  A tuple containing the name and content of each file in the zip file.
    """ """
    """
    with ZipFile(BytesIO(zip_file)) as zip_file:
        for name in zip_file.namelist():
            content = zip_file.read(name).decode("utf-8")
            yield name, content


def zip_to_dict(zip_file: bytes) -> Dict[str, str]:
    """
    Converts a zip file to a dictionary.
    zip_file: The content of the zip file.
    return: A dictionary containing the content of the zip file.
    """
    output = {}

    for name, content in extract_zip_content(zip_file):
        output[name] = content

    return output


def dict_to_zip(data: Dict[str, str]) -> bytes:
    """
    Converts a dictionary to a gzip file.
    :param data: The dictionary to convert.
    :return: The content of the gzip file.
    """
    with BytesIO() as output:
        with ZipFile(output, "w", compression=ZIP_BZIP2) as zip_file:
            for name, content in data.items():
                zip_file.writestr(name, content)

        return output.getvalue()
