#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

import os
from datetime import datetime
from typing import List, Tuple

from filelock import FileLock
from pandas import Timestamp

from src.core.constants import BUFFER
from src.core.io_manager.base import IOManager
from src.core.mixins.loggable import LoggableComponent
from src.core.models import DataType
from src.core.persistence import PersistenceManager
from src.core.storage.base import InternalStorageManager
from src.core.tables import Collection
from src.core.utils.exception import AnotherWorldException


class Engine(LoggableComponent):
    def __init__(
            self,
            io_manager: IOManager,
            persistence_manager: PersistenceManager,
            internal_storage: InternalStorageManager,
    ):
        super().__init__()
        self.internal_storage = internal_storage
        self.io_manager = io_manager
        self.persistence_manager = persistence_manager
        self.startup_lock = FileLock("startup.lock")

    def check_for_storage_integrity(self):
        """
        On startup, the Lake should check the storage integrity and fix any inconsistency.
        """
        try:
            with self.startup_lock.acquire(blocking=False):
                self.log("Checking for storage integrity, acquiring lock")
                for (
                        collection
                ) in self.persistence_manager.get_collections_with_active_buffer():
                    self.log_warning(
                        f"Collection {collection.name} has an active buffer, flushing it"
                    )
                    try:
                        self.flush(collection.name)
                    except Exception as e:
                        self.log_error(
                            f"Error flushing buffer for collection {collection.name}: {e}"
                        )
        except TimeoutError:
            self.log_warning(
                "Startup lock already acquired, skipping storage integrity check"
            )

    def create_collection(self, collection_name: str, allow_existing: bool = False):
        """
        Create a new collection with the given name.
        :param collection_name: The name of the collection to create
        :param allow_existing: Whether to allow the creation of an existing collection
        :return: None
        """

        self.io_manager.create_collection(collection_name)
        self.persistence_manager.create_collection(collection_name, allow_existing)
        self.log(f"Collection {collection_name} created")

    def list_collections(self) -> List[dict]:
        """
        List all the collections in the storage.
        :return: A list of collection names
        """

        return self.persistence_manager.get_collections()

    def store(
            self, collection_name: str, timestamp: datetime, data: bytes, data_type=None,
            create_collection: bool = False
    ):
        """
        Store the given data in the collection with the given name. The data will be stored in a
        buffered fragment until it is flushed to a new fragment. The data will be associated with
        the given timestamp. An optional data type can be provided to specify the type of the data,
        this will be used as a hint for the internal storage manager (if it supports it, otherwise
        fallback to the default behavior).

        :param collection_name: The name of the collection to store the data in
        :param timestamp: The timestamp to associate with the data
        :param data: The data to store
        :param data_type: (Optional) The type of the data
        :param create_collection: Whether to create the collection if it does not exist
        :return: None
        """

        if create_collection:
            try:
                self.persistence_manager.get_collection_by_name(collection_name)
            except AnotherWorldException:
                self.create_collection(collection_name)

        # Append the segment to the buffered fragment
        current_size = self.io_manager.get_size(collection_name, BUFFER)

        # Append the data to the buffered fragment
        with self.io_manager.get_append_context(collection_name, BUFFER) as f:
            f.write(data)

        self.persistence_manager.append_segments_to_buffer_fragment(
            collection_name,
            (
                current_size,
                current_size + len(data),
                timestamp.isoformat(),
            ),
        )

        if (
                self.io_manager.get_size(collection_name, BUFFER) > 500 * 1024 * 1024
        ):  # 500MB
            self.flush(collection_name, data_type)

    def flush(self, collection_name: str, data_type: DataType = None) -> bool:
        """
        Flush the buffered data to a new fragment. The data will be written to a new fragment and
        the buffered fragment will be removed. An optional data type can be provided to specify the
        type of the data, this will be used as a hint for the internal storage manager (if it
        supports it, otherwise fallback to the default behavior).

        :param collection_name: The name of the collection to flush
        :param data_type: (Optional) The type of the data
        :return: True if the data was flushed, False otherwise
        """

        if not self.persistence_manager.has_buffered_fragment(collection_name):
            return False

        # Create a new fragment from the buffered fragment
        segments, associated_fragment_uuid = (
            self.persistence_manager.associate_new_fragment_to_buffer(collection_name)
        )

        # Write the data in the buffered fragment to the new fragment
        with self.io_manager.get_read_context(collection_name, BUFFER) as f:
            with self.io_manager.get_write_context(
                    collection_name, associated_fragment_uuid
            ) as output:
                # Read and Split the data
                data = f.read()
                items = [
                    (data[segment[0]: segment[1]], segment[2]) for segment in segments
                ]
                # Write the data
                metadata = self.internal_storage.write(items, output, data_type)

        # Remove the buffered fragment and create items
        self.persistence_manager.remove_buffered_fragment_and_create_items(
            collection_name, metadata
        )

        # Remove the buffer file
        os.remove(os.path.join(self.io_manager.storage_folder, collection_name, BUFFER))

        self.log(
            f"Buffered data flushed to new fragment in collection {collection_name}"
        )

        return True

    def query(
            self,
            collection_name: str,
            min_timestamp: datetime,
            max_timestamp: datetime,
            ascending: bool = True,
            limit: int = None,
            offset: int = None,
    ) -> List[Tuple[bytes, datetime]]:
        """
        Query the data in the collection with the given name. The data will be filtered using the
        :param collection_name: The name of the collection to query
        :param min_timestamp: The minimum timestamp to filter the data
        :param max_timestamp: The maximum timestamp to filter the data
        :param ascending: Whether to sort the data in ascending order
        :param limit: The limit of the data to retrieve
        :param offset: The offset of the data to retrieve
        :return: The data in the collection as a list of tuples of bytes and datetime
        """

        collection = self.persistence_manager.get_collection_by_name(collection_name)

        fragments = self.persistence_manager.query(
            collection, min_timestamp, max_timestamp, ascending, limit
        )
        result = []
        for fragment in fragments:
            result.extend(
                self._get_fragment_items(
                    collection, fragment, min_timestamp, max_timestamp, ascending, limit
                )
            )

        result.extend(
            self._get_data_from_buffer(collection, min_timestamp, max_timestamp)
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

    def _get_data_from_buffer(self, collection, min_timestamp, max_timestamp):
        buffer = self.persistence_manager.get_buffered_fragment(collection)

        if buffer is None:
            return []

        segments = buffer.segments

        with self.io_manager.get_read_context(collection.name, BUFFER) as f:
            # Split the data
            data = f.read()

            items = [
                (data[segment[0]: segment[1]], Timestamp(segment[2]))
                for segment in segments
                if min_timestamp <= datetime.fromisoformat(segment[2]) <= max_timestamp
            ]

        return items

    def _get_fragment_items(
            self, collection, fragment, min_timestamp, max_timestamp, ascending, limit
    ) -> List[Tuple[bytes, datetime]]:
        with self.io_manager.get_read_context(collection.name, fragment.uuid) as f:
            result = self.internal_storage.read(
                f,
                fragment.internal_metadata,
                where={"min_timestamp": min_timestamp, "max_timestamp": max_timestamp},
                order_by=["timestamp" if ascending else "timestamp desc"],
                limit=limit,
            )

            return result
