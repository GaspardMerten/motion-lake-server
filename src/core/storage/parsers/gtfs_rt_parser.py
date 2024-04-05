#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

import json

from google.protobuf import json_format
from google.protobuf.json_format import MessageToDict, SerializeToJsonError
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from src.core.storage.parsers.base import BaseParser, MissMatchingTypes
from src.core.utils.numpy_json import NumpyEncoder


class GTFSRTParser(BaseParser):
    def parse(self, data: bytes) -> bytes | str | object | None:
        try:
            # noinspection PyUnresolvedReferences
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(data)
            return MessageToDict(feed)
        except DecodeError | SerializeToJsonError:
            raise MissMatchingTypes()

    def serialize(self, data: bytes | str | object) -> bytes:
        # noinspection PyUnresolvedReferences
        feed = gtfs_realtime_pb2.FeedMessage()
        json_format.Parse(json.dumps(data, cls=NumpyEncoder), feed)
        return feed.SerializeToString()
