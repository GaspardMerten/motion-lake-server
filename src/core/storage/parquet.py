#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

from datetime import datetime
from io import BytesIO
from typing import List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import ArrowInvalid, ArrowNotImplementedError

from src.core.models import DataType
from src.core.storage.parsers.base import MissMatchingTypes
from src.core.storage.parsers.bytes_parser import BytesParser
from src.core.storage.parsers.gtfs_rt_parser import GTFSRTParser
from src.core.storage.parsers.json_parser import JSONParser
from src.core.utils.exception import AnotherWorldException

MAPPING = {
    DataType.RAW: BytesParser(),
    DataType.JSON: JSONParser(),
    DataType.GTFS_RT: GTFSRTParser(),
    # DataType.GTFS: GTFSParser(),
}


def _infer_type(data: bytes) -> DataType:
    for data_type, parser in MAPPING.items():
        # noinspection PyBroadException
        try:
            parser.parse(data)
            return data_type
        except Exception:
            pass
    return DataType.RAW


class ParquetDynamicStorage:
    """
    Parquet storage that can handle multiple data types.
    """

    def __init__(self, **settings):
        """
        Initialize the Parquet storage with the given settings.
        :param settings: The settings to use
            - compression: The compression to use (default: snappy)
        """
        self.compression = settings.get("compression", "snappy")
        self.compression_level = settings.get("compression_level", "snappy")

    def write(
        self,
        data: List[Tuple[bytes, datetime]],
        output: BytesIO,
        data_type: DataType = None,
    ) -> dict:
        """
        Write the data to the output stream, with the given data type.

        The objective of this method is to write the data to the output stream using Parquet format.
        To best handle the data, the method will try to infer the data type if not provided.
        Then, it will try to parse the data using the parser associated with the data type.
        If the parsing fails, it will try to write the data with the raw data type.

        :param data: The data to write as a list of tuples of bytes and datetime
        :param output: The output stream to write the data to
        :param data_type: The data type of the data to write
        :return: A dictionary with the metadata of the written data
        """
        assert len(data) > 0, "Data must not be empty"

        # Infer data type if not provided (try to parse the first item, and if it fails, return raw)
        if data_type is None:
            data_type = _infer_type(data[0][0])

        # Then determine the parser to use
        parser = MAPPING.get(data_type, BytesParser()).parse

        # This is a temporary list to store the parsed data
        parsed_data = []

        # For each item in the data, try to parse it, and if it fails,
        # run the write method again with the raw data type for ALL the data
        # This is to avoid writing a mix of data types in the same parquet file.
        for item in data:
            try:
                representation = parser(item[0])
                parsed_data.append((representation, item[1]))
            except MissMatchingTypes:
                return self.write(data, output, DataType.RAW)

        try:
            table = pa.Table.from_arrays(
                arrays=[
                    pa.array([item[0] for item in parsed_data]),
                    pa.array([int(item[1]) for item in parsed_data]),
                ],
                schema=pa.schema(
                    [
                        pa.field(
                            "data", pa.infer_type([item[0] for item in parsed_data])
                        ),
                        pa.field("timestamp", pa.timestamp("ns")),
                    ]
                ),
            )
        except (ArrowInvalid, ArrowNotImplementedError) as e:
            if data_type == DataType.RAW:
                raise AnotherWorldException(
                    "Cannot write raw data to parquet, even if it's not parsed"
                )
            return self.write(data, output, DataType.RAW)

        local_output = BytesIO()

        # Write the table to the output stream
        pq.write_table(
            table,
            local_output,
            compression=self.compression,
            use_dictionary=True,
            compression_level=self.compression_level,
        )

        output.write(local_output.getvalue())

        pq_file = pq.ParquetFile(BytesIO(local_output.getvalue()))
        return {
            "data_type": data_type.value,
            "schema": f"{pq_file.schema}",
        }

    def read(
        self,
        reader: BytesIO,
        metadata: dict,
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
        :param metadata: The metadata of the data to read
        :param where: The where clause to filter the data
        :param order_by: The order by clause to order the data
        :param limit: The limit clause to limit the data
        :return: The data read as a list of tuples of bytes and datetime
        """
        data_type = DataType(metadata["data_type"])
        serialize = MAPPING.get(data_type, BytesParser()).serialize
        reader = BytesIO(reader.read())

        table_filters = []

        if "min_timestamp" in where:
            table_filters.append(("timestamp", ">=", where["min_timestamp"]))
        if "max_timestamp" in where:
            table_filters.append(("timestamp", "<=", where["max_timestamp"]))

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
