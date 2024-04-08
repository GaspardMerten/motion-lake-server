#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

from datetime import datetime
from io import BytesIO
from typing import Tuple, List, Protocol


class InternalStorageManager(Protocol):
    """
    The InternalStorageManager is responsible for reading and writing the data in a byte stream. It does not
    handle the data representation or storage, only the reading and writing of the data to a byte stream.

    As such it is a protocol that only defines the read and write methods.
    """

    def write(
        self,
        data: List[Tuple[bytes, int]],
        output: BytesIO,
        content_type: List[str] = None,
    ) -> dict:
        """
        Write the data to the output stream.
        :param data: The data to write as a list of tuples of bytes and datetime
        :param output: The output stream to write the data to
        :param content_type: The content type of the data to write
        :return: A dictionary with the metadata of the written data
        """
        ...

    def read(
        self,
        reader: BytesIO,
        metadata: dict,
        where: {},
        order_by: List[str] = None,
        limit: int = None,
    ) -> List[Tuple[bytes, datetime]]:
        """
        Read the data from the input stream.
        :param reader: The input stream to read the data from
        :param metadata: The metadata of the data to read
        :param where: The where clause to filter the data
        :param order_by: The order by clause to order the data
        :param limit: The limit clause to limit the data
        :return: The data read as a list of tuples of bytes and datetime
        """
        ...
