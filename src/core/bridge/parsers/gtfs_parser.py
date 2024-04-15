#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.
from collections import defaultdict
from typing import List, Dict

import pandas as pd

from src.core.bridge.parsers.base import BaseParser, MissMatchingTypesException
from src.core.utils.zip_tools import zip_to_dict, dict_to_zip


def string_line_to_typed_list(line: str, types: list) -> list:
    """
    Convert a string line to a typed list.
    :param types: The types to convert the line to a list
    :param line: The line to convert
    :return: The typed list
    """
    return [parser(value) for value, parser in zip(line.split(","), types)]


def infer_types(content: List[str]) -> List[type]:
    """
    Infer the types of the content.
    :param content: The content to infer the types from
    :return: The types of the content
    """

    # noinspection PyTypeChecker
    types: Dict[int, type] = defaultdict(str)

    for line in content:
        for i, value in enumerate(line.split(",")):
            if value.isdigit() and types[i] != str and types[i] != float:
                types[i] = int
            elif value.replace(".", "").isdigit() and types[i] != str:
                types[i] = float
            else:
                types[i] = str

    return [types[i] for i in range(len(types))]


class GTFSParser(BaseParser):
    def parse(self, data: bytes) -> bytes | str | object | None:
        try:
            files = zip_to_dict(data)
            out = {}

            for file, content in files.items():
                content_as_lines = (
                    content.splitlines()
                )  # More efficient than split("\n")
                header, *content_lines = content_as_lines
                header = header.split(",")
                output = []
                types = infer_types(content_lines)

                for line in content_lines:
                    if (
                        not line.strip()
                    ):  # Simplify the check for empty or whitespace-only lines
                        continue
                    try:
                        line_as_dict = dict(
                            zip(header, string_line_to_typed_list(line, types))
                        )
                        output.append(line_as_dict)
                    except ValueError:
                        continue

                out[file] = {"header": header, "content": output}

            return out
        except Exception as e:
            raise MissMatchingTypesException()

    def serialize(self, data: dict) -> bytes:
        files = {}
        for file in data:
            out = ""
            out += ",".join(data[file]["header"]) + "\n"
            out += pd.DataFrame(data[file]["content"]).to_csv(index=False)
            files[file] = out

        return dict_to_zip(files)
