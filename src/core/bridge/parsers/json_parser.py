#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

import ujson

from src.core.bridge.parsers.base import BaseParser, MissMatchingTypesException


class JSONParser(BaseParser):
    async def parse(self, data: bytes) -> bytes | str | object | None:
        try:
            return ujson.loads(data)
        except ujson.JSONDecodeError:
            raise MissMatchingTypesException()
        except UnicodeError:
            raise MissMatchingTypesException()

    async def serialize(self, data: dict) -> bytes:
        return ujson.dumps(data, reject_bytes=False).encode()
