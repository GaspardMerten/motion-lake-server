#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.
import itertools
import os
from collections import defaultdict
from datetime import datetime
from hashlib import md5
from typing import List, Tuple, Union
from uuid import uuid4

from filelock import FileLock

from src.core.bridge.parquet_bridge import ParquetBridge
from src.core.io_manager.base import IOManager
from src.core.mixins.loggable import LoggableComponent
from src.core.models import ContentType
from src.core.persistence.persistence import PersistenceManager
from src.core.persistence.tables import Collection
from src.core.utils.exception import AnotherWorldException

DEFAULT_BUFFER_SIZE = 6  # 6 MB


class Engine(LoggableComponent):
    def __init__(
        self,
        io_manager: IOManager,
        persistence_manager: PersistenceManager,
        bridge: ParquetBridge,
    ):
        super().__init__()
        self.bridge = bridge
        self.io_manager = io_manager
        self.persistence_manager = persistence_manager
        self.startup_lock = FileLock("startup.lock")
        self.hash_cache = {}

    def create_collection(
        self, collection_name: str, allow_existing: bool = False
    ) -> Collection:
        """f
        Create a new collection with the given name.
        :param collection_name: The name of the collection to create
        :param allow_existing: Whether to allow the creation of an existing collection
        :return: None
        """
        self.io_manager.create_collection(collection_name)
        # noinspection PyTypeChecker
        collection = self.persistence_manager.create_collection(
            collection_name, allow_existing
        )
        self.log(f"Collection {collection_name} created")

        return collection

    def list_collections(self) -> List[dict]:
        """
        List all the collections in the bridge.
        :return: A list of collection names
        """

        return self.persistence_manager.get_collections()

    def store(
        self,
        collection_name: str,
        timestamp: datetime,
        data: bytes,
        content_type=None,
        create_collection: bool = False,
    ):
        """
        Store the given data in the collection with the given name. The data will be stored in a
        buffered fragment until it is flushed to a new fragment. The data will be associated with
        the given timestamp. An optional data type can be provided to specify the type of the data,
        this will be used as a hint for the internal bridge manager (if it supports it, otherwise
        fallback to the default behavior).

        :param collection_name: The name of the collection to store the data in
        :param timestamp: The timestamp to associate with the data
        :param data: The data to store
        :param content_type: (Optional) The type of the data
        :param create_collection: Whether to create the collection if it does not exist
        :return: None
        """
        collection = self.check_collection(collection_name, create_collection)
        data_hash = md5(data).hexdigest()

        if collection_name not in self.hash_cache:
            self.hash_cache[collection_name] = self.persistence_manager.get_last_hash(
                collection_name
            )

        # Prevent data duplication
        if self.hash_cache.get(collection_name) == data_hash:
            print("Skipping")
            return
        else:
            self.hash_cache[collection_name] = data_hash

        buffer_uuid = str(uuid4())

        with self.io_manager.get_write_context(collection_name, buffer_uuid) as output:
            result = self.bridge.write_single(
                collection_name=collection_name,
                bytes_data=data,
                timestamp=timestamp,
                output=output,
                content_type=content_type,
            )

        self.persistence_manager.log_buffer(
            collection.id,
            timestamp,
            buffer_uuid,
            result.size,
            result.original_size,
            result.content_type,
            data_hash,
        )

        if (
            self.persistence_manager.get_unlocked_buffers_size(collection_name)
            > int(os.getenv("BUFFER_SIZE", str(DEFAULT_BUFFER_SIZE))) * 1024 * 1024
        ):
            self.log("Flushing collection: " + collection_name)
            self.flush(collection_name)

    def check_collection(self, collection_name: str, create_collection: bool):
        """
        Create a collection if it does not exist.

        :param collection_name: The name of the collection to create
        :param create_collection: Whether to create the collection if it does not exist
        """
        try:
            return self.persistence_manager.get_collection_by_name(collection_name)
        except AnotherWorldException:
            if create_collection:
                return self.create_collection(collection_name)
            else:
                raise AnotherWorldException(
                    f"Collection {collection_name} does not exist"
                )

    def flush(self, collection_name: str) -> bool:
        """
        Flush the buffered data to a new fragment. The data will be written to a new fragment and
        the buffered fragment will be removed. An optional data type can be provided to specify the
        type of the data, this will be used as a hint for the internal bridge manager (if it
        supports it, otherwise fallback to the default behavior).

        :param collection_name: The name of the collection to flush
        :return: True if the data was flushed, False otherwise
        """

        collection = self.persistence_manager.get_collection_by_name(collection_name)
        buffers = self.persistence_manager.get_and_lock_buffers(collection)

        data = defaultdict(list)
        uuid_to_timestamp = {}

        for buffer in buffers:
            data[buffer.content_type].append(
                (
                    self._get_bytes_for_buffer(collection_name, buffer.uuid),
                    buffer.uuid,
                )
            )
            uuid_to_timestamp[buffer.uuid] = buffer.timestamp

        for content_type, items in data.items():
            try:
                result_bytes, skipped = self.bridge.merge(items)
            except Exception as e:
                self.log_error(
                    f"Error while merging buffers for collection {collection_name}", e
                )
                # Flush all fragment buffers as skipped
                self.persistence_manager.flush_skipped_buffers(
                    collection, [item[1] for item in items]
                )
                continue

            self.log(
                f"Flushing {len(skipped)} skipped buffers for collection {collection_name}"
            )
            self.persistence_manager.flush_skipped_buffers(collection, skipped)

            if result_bytes:
                new_fragment_uuid = str(uuid4())
                not_skipped = [item[1] for item in items if item[1] not in skipped]

                with self.io_manager.get_write_context(
                    collection_name, new_fragment_uuid
                ) as output:
                    output.write(result_bytes.getvalue())

                self.log(
                    f"Flushing {len(not_skipped)} buffers for collection {collection_name}"
                )
                self.persistence_manager.flush_buffer(
                    collection, new_fragment_uuid, content_type, not_skipped
                )
                self.io_manager.remove_fragments(collection_name, not_skipped)

        return True

    def _get_bytes_for_buffer(self, collection_name: str, buffer_uuid: str):
        with self.io_manager.get_read_context(collection_name, buffer_uuid) as f:
            return f.read()

    def query(
        self,
        collection_name: str,
        min_timestamp: datetime,
        max_timestamp: datetime,
        ascending: bool = True,
        limit: int = None,
        offset: int = None,
        skip_data: bool = False,
    ) -> List[Tuple[Union[bytes | None], datetime]]:
        """
        Query the data in the collection with the given name. The data will be filtered using the
        :param collection_name: The name of the collection to query
        :param min_timestamp: The minimum timestamp to filter the data
        :param max_timestamp: The maximum timestamp to filter the data
        :param ascending: Whether to sort the data in ascending order
        :param limit: The limit of the data to retrieve
        :param offset: The offset of the data to retrieve
        :param skip_data: Whether to skip the data in the results (data will be None)
        :return: The data in the collection as a list of tuples of bytes and datetime
        """

        try:
            collection = self.persistence_manager.get_collection_by_name(
                collection_name
            )
        except AnotherWorldException:
            return []

        # noinspection PyTypeChecker
        fragments = self.persistence_manager.query(
            collection, min_timestamp, max_timestamp, ascending, limit
        )
        buffers = self.persistence_manager.query_buffers_do_not_lock(
            collection, min_timestamp, max_timestamp, ascending, limit
        )
        if skip_data:
            items = self.persistence_manager.get_items_from_fragments(fragments)
            data = [
                (None, int(item.timestamp.timestamp()))
                for item in itertools.chain(items, buffers)
                if min_timestamp <= item.timestamp <= max_timestamp
            ]

            data.sort(key=lambda x: x[1], reverse=not ascending)
            data = data[offset:limit] if limit else data[offset:]
            return data

        result = []

        for item in itertools.chain(fragments, buffers):
            result.extend(
                self._get_fragment_items(
                    collection,
                    item.uuid,
                    item.content_type,
                    min_timestamp,
                    max_timestamp,
                    ascending,
                    limit,
                )
            )
        # Sort the result by timestamp
        result.sort(key=lambda x: x[1], reverse=not ascending)

        self.log(
            f"Querying data in collection {collection_name}, found {len(result)} items, ascending={ascending}, limit="
            f"{limit}, min_timestamp={min_timestamp}, max_timestamp={max_timestamp}"
        )

        if offset:
            result = result[offset:]

        if limit:
            result = result[:limit]

        return result

    def _get_fragment_items(
        self,
        collection,
        fragment_uuid,
        content_type,
        min_timestamp,
        max_timestamp,
        ascending,
        limit,
    ) -> List[Tuple[bytes, datetime]]:
        with self.io_manager.get_read_context(collection.name, fragment_uuid) as f:
            result = self.bridge.read(
                f,
                content_type=content_type,
                where={"min_timestamp": min_timestamp, "max_timestamp": max_timestamp},
                order_by=["timestamp" if ascending else "timestamp desc"],
                limit=limit,
            )
            return result

    def advanced_query(
        self,
        collection_name: str,
        query: str,
        min_timestamp: datetime,
        max_timestamp: datetime,
    ):
        """
        Perform an advanced query on the given collection.
        :param collection_name: The name of the collection to query
        :param query: The query to perform
        :param min_timestamp: The minimum timestamp to filter the data
        :param max_timestamp: The maximum timestamp to filter the data
        :return: The data in the collection as a list of tuples of bytes and datetime
        """

        collection = self.persistence_manager.get_collection_by_name(collection_name)
        fragments = self.persistence_manager.query(
            collection,
            min_timestamp,
            max_timestamp,
            True,
            content_types=[ContentType.JSON, ContentType.GTFS_RT],
        )
        buffers = self.persistence_manager.query_buffers_do_not_lock(
            collection, min_timestamp, max_timestamp
        )

        paths = [
            self.io_manager.get_duck_db_fragment_path(collection_name, item.uuid)
            for item in itertools.chain(fragments, buffers)
        ]

        if not paths:
            return []

        duck_db_connection = self.io_manager.get_duck_db_connection()

        return self.bridge.advanced_query(
            duck_db_connection, paths, query, min_timestamp, max_timestamp
        )

    def delete_collection(self, collection_name: str):
        """
        Delete the collection with the given name.
        :param collection_name: The name of the collection to delete
        :return: None
        """

        self.persistence_manager.delete_collection(collection_name)
        self.io_manager.remove_collection(collection_name)
        self.log(f"Collection {collection_name} deleted")

    def get_collection_size(self, collection_name: str) -> int:
        """
        Get the size of the collection with the given name.
        :param collection_name: The name of the collection
        :return: The size of the collection
        """

        return self.io_manager.get_collection_size(collection_name)
