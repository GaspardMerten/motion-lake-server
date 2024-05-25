#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

import json

from google.protobuf import json_format
from google.protobuf.json_format import MessageToDict, SerializeToJsonError
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from src.core.bridge.parsers.base import BaseParser
from src.core.utils.exception import AnotherWorldException
from src.core.utils.numpy_json import NumpyEncoder


class GTFSRTParser(BaseParser):
    async def parse(self, data: bytes) -> bytes | str | object | None:
        try:
            # noinspection PyUnresolvedReferences
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(data)
            return MessageToDict(feed)
        except DecodeError | SerializeToJsonError:
            raise AnotherWorldException("Failed to parse GTFS-RT data.")

    def recursively_remove_dict_with_only_nulls(self, data):
        for key, value in list(data.items()):
            if isinstance(value, dict):
                self.recursively_remove_dict_with_only_nulls(value)
            elif isinstance(value, list):
                for item in value:
                    self.recursively_remove_dict_with_only_nulls(item)
            if not value:
                print(f"Removing {key}")
                del data[key]
        return data

    async def serialize(self, data: bytes | str | object) -> bytes:
        # noinspection PyUnresolvedReferences
        feed = gtfs_realtime_pb2.FeedMessage()

        self.recursively_remove_dict_with_only_nulls(data)

        json_format.Parse(json.dumps(data, cls=NumpyEncoder), feed)

        return feed.SerializeToString()
