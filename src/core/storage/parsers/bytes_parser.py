#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.
from src.core.storage.parsers.base import BaseParser, MissMatchingTypes


class BytesParser(BaseParser):
    def parse(self, data: bytes) -> bytes | str | object | None:
        if isinstance(data, bytes):
            return data

        raise MissMatchingTypes()

    def serialize(self, data: bytes | str | object) -> bytes:
        return data
