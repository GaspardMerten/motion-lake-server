#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

from typing import Protocol


class MissMatchingTypesException(Exception):
    pass


class BaseParser(Protocol):
    async def parse(self, data: bytes) -> bytes | str | object | None:
        """
        Parse the data from the byte stream to the desired format.
        :param data: The data to parse
        :return: The parsed data
        :raises MissMatchingTypes: If the data type does not match the expected data type
        """
        ...

    async def serialize(self, data: bytes | str | object | dict) -> bytes:
        """
        Serialize the data to the byte stream.
        :param data: The data to serialize
        :return: The serialized data
        :raises MissMatchingTypes: If the data type does not match the expected data type
        """
        ...
