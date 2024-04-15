#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

from datetime import datetime
from io import BytesIO
from typing import List, Tuple, Iterable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import ArrowInvalid

from src.core.bridge.parsers.base import MissMatchingTypesException
from src.core.bridge.parsers.bytes_parser import BytesParser
from src.core.bridge.parsers.gtfs_rt_parser import GTFSRTParser
from src.core.bridge.parsers.json_parser import JSONParser
from src.core.mixins.loggable import LoggableComponent
from src.core.models import ContentType, BridgeResult
from src.core.utils.exception import AnotherWorldException

MAPPING = {
    ContentType.JSON: JSONParser(),
    ContentType.GTFS_RT: GTFSRTParser(),
    ContentType.RAW: BytesParser(),
    # ContentType.GTFS: GTFSParser(),
}


def get_parser(content_type: ContentType):
    return MAPPING.get(content_type, BytesParser())


class ParquetBridge(LoggableComponent):
    """
    Parquet bridge that can handle multiple data types.
    """

    def __init__(self, **settings):
        """
        Initialize the Parquet bridge with the given settings.
        :param settings: The settings to use
            - compression: The compression to use (default: snappy)
        """
        super().__init__()
        self.compression = settings.get("compression", "gzip")
        self.compression_level = settings.get("compression_level", "9")

    def _table_to_bytes(self, table: pa.Table, output: BytesIO) -> BytesIO:
        """
        Convert a table to a bytes object.
        :param output: The bytes buffer to write to
        :param table: The table to convert
        :return: The table as bytes
        """
        try:
            pq.write_table(
                table,
                output,
                compression=self.compression,
                compression_level=self.compression_level,
            )
        except Exception as e:
            self.log_error(f"Error while writing parquet file: {e}")
            raise AnotherWorldException(f"Error while writing parquet file: {e}")
        return output

    def write_single(
        self,
        bytes_data: bytes,
        timestamp: datetime,
        output: BytesIO,
        content_type: ContentType = None,
    ) -> BridgeResult:
        """
        Write the data to the output stream, with the given data type.

        The objective of this method is to write the data to the output stream using Parquet format.
        To best handle the data, the method will try to infer the data type if not provided.
        Then, it will try to parse the data using the parser associated with the data type.
        If the parsing fails, it will try to write the data with the raw data type.

        :param bytes_data: The data to write as bytes
        :param timestamp: The timestamp of the data
        :param output: The output stream to write the data to
        :param content_type: The data type of the data to write
        :return: A dictionary with the metadata of the written data

        :raises AnotherWorldException: If the data type is RAW and the data cannot be written
        """
        content_type = content_type if content_type is not None else ContentType.RAW
        parser = get_parser(content_type)

        # Try to parse the data, and if it fails, write it as raw data
        try:
            representation = parser.parse(bytes_data)
        except MissMatchingTypesException:
            # If the data type is RAW, and the parsing fails, raise an exception
            if content_type == ContentType.RAW:
                self.log_error(
                    "Cannot write raw data to parquet, even if it's not parsed"
                )
                raise AnotherWorldException(
                    "Cannot write raw data to parquet, even if it's not parsed"
                )

            return self.write_single(bytes_data, timestamp, output, ContentType.RAW)

        if not representation:
            representation = str(representation)

        try:
            # noinspection PyArgumentList
            table = pa.Table.from_pydict(
                {
                    "data": [representation],
                    "timestamp": [timestamp.timestamp()],
                },
                schema=pa.schema(
                    [
                        pa.field("data", pa.infer_type([representation])),
                        pa.field("timestamp", pa.int64()),
                    ]
                ),
            )
        except ArrowInvalid as e:
            self.log_error(f"Error while creating parquet table: {e}")
            return self.write_single(bytes_data, timestamp, output, ContentType.RAW)

        try:
            output = self._table_to_bytes(table, output)
        except AnotherWorldException as e:
            raise e

        # Return number of bytes and final data type
        return BridgeResult(
            content_type=content_type.value,
            size=output.tell(),
            original_size=len(bytes_data),
        )

    def merge(
        self, generator: Iterable[Tuple[bytes, str]]
    ) -> Tuple[BytesIO | None, List[str]]:
        """
        Merge the data from the given generator.

        The objective of this method is to merge the data from the given generator into a single parquet file.
        The method will try to merge the data of the same data type into a single table.
        If the data types are different, the method will create a new table for the new data type.
        The method will then write the tables to the output stream.

        :param generator: The generator of data to merge
        :return: A tuple with the list of merged data and the list of skipped files
        """

        table = None

        skipped = []

        for data, file_id in generator:
            try:
                current_table = pq.read_table(BytesIO(data))
                table = (
                    table and pa.concat_tables([table, current_table])
                ) or current_table
            except Exception as e:
                self.log_error(f"Error while merging parquet files: {e}")
                skipped.append(file_id)

        return table and self._table_to_bytes(table, BytesIO()), skipped

    @staticmethod
    def read(
        reader: BytesIO,
        content_type: ContentType,
        where: {},
        order_by: List[str] = None,
        limit: int = None,
    ) -> List[Tuple[bytes, datetime]]:
        """
        Read the data from the input stream. The data is filtered using the where clause, ordered using the order by
        clause, and limited using the limit clause.

        The objective of this method is to read the data from the input stream using Parquet format. It will try to
        filter, order, and limit the data as requested. It will then parse the data using the parser associated with the
        data type.

        :param reader: The input stream to read the data from
        :param content_type: The data type of the data to read
        :param where: The where clause to filter the data
        :param order_by: The order by clause to order the data
        :param limit: The limit clause to limit the data
        :return: The data read as a list of tuples of bytes and datetime
        """
        content_type = ContentType(content_type)
        serialize = MAPPING.get(content_type, BytesParser()).serialize
        reader = BytesIO(reader.read())

        table_filters = []

        if "min_timestamp" in where:
            table_filters.append(
                ("timestamp", ">=", int(where["min_timestamp"].timestamp()))
            )
        if "max_timestamp" in where:
            table_filters.append(
                ("timestamp", "<=", int(where["max_timestamp"].timestamp()))
            )

        table = pq.read_table(reader, filters=table_filters)

        data = table.to_pandas()

        filtered_data = []

        if order_by and "timestamp" in order_by:
            data.sort_values("timestamp")
        elif order_by and "timestamp desc" in order_by:
            data = data.sort_values("timestamp", ascending=False)

        if limit:
            data = data[:limit]
        for i, row in data.iterrows():
            filtered_data.append(
                (
                    serialize(row["data"]),
                    row["timestamp"],
                )
            )

        return filtered_data

    @staticmethod
    def advanced_query(files: List[str], query: str, *args, **kwargs):
        """
        Perform an advanced query on the given files.
        :param files: The files to query
        :param query: The query to perform
        :return: The result of the query
        """

        con = duckdb.connect()

        files_str = ", ".join([f"'{file}'" for file in files])

        table = f"(SELECT * FROM read_parquet([{files_str}], union_by_name=true))"

        try:
            return con.execute(query.replace("[table]", table)).fetchall()
        except Exception as e:
            raise AnotherWorldException(f"Query failed: {e}")
