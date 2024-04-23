#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

import os
import tempfile

import ujson
from google.protobuf import json_format
from google.protobuf.json_format import SerializeToJsonError
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from src.core.bridge.parsers.base import BaseParser, MissMatchingTypesException


class GTFSRTParser(BaseParser):
    async def parse(self, data: bytes) -> bytes | str | object | None:
        try:
            # create a named temporary file to store the data
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                temp.write(data)
                temp.seek(0)

                read = os.popen(f"./bin/gtfs_rt_to_json {temp.name}")
            return ujson.loads(read.read())
        except (DecodeError, SerializeToJsonError, ujson.JSONDecodeError) as e:
            raise MissMatchingTypesException(f"Error while parsing the data: {e}")

    async def serialize(self, data: bytes | str | object) -> bytes:
        # noinspection PyUnresolvedReferences
        feed = gtfs_realtime_pb2.FeedMessage()
        json_format.Parse(ujson.dumps(data), feed)
        return feed.SerializeToString()
