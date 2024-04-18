#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

import json

from src.core.bridge.parsers.base import BaseParser, MissMatchingTypesException


class JSONParser(BaseParser):
    def parse(self, data: bytes) -> bytes | str | object | None:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            raise MissMatchingTypesException()
        except UnicodeError:
            raise MissMatchingTypesException()

    def serialize(self, data: dict) -> bytes:
        return json.dumps(data, reject_bytes=False).encode()
