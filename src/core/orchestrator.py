#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

from datetime import datetime
from typing import List, Tuple

from src.core.bridge.parquet_bridge import ParquetBridge
from src.core.engine import Engine
from src.core.io_manager.base import IOManager
from src.core.mixins.loggable import LoggableComponent
from src.core.persistence.persistence import PersistenceManager
from src.core.utils.exception import AnotherWorldException


class Orchestrator(LoggableComponent):
    """
    The Orchestrator is the main class of the system. It is responsible for managing the bridge and the
    persistence of the data. It also manages the internal bridge, which is responsible for
    reading and writing the data in the bridge.

    The Orchestrator is the entry point for the following operations:
    - Store data
    - Retrieve data
    - Flush the buffered data
    - Check the bridge integrity
    """

    def __init__(
        self,
        persistence_manager: PersistenceManager,
        io_manager: IOManager,
        bridge: ParquetBridge,
    ) -> None:
        """
        Initialize the Orchestrator with the given parameters.
        :type bridge: object
        :param persistence_manager: The persistence manager to use for managing the data
        :param io_manager: The IO manager to use for reading and writing the data
        :param bridge: The bridge to use for reading and writing the data
        """
        super().__init__()

        self._engine = Engine(
            io_manager=io_manager,
            persistence_manager=persistence_manager,
            bridge=bridge,
        )

    def create_collection(self, collection_name: str, allow_existing: bool = False):
        """
        Create a new collection with the given name.
        :param collection_name: The name of the collection to create
        :param allow_existing: Whether to allow the creation of an existing collection
        :return: None
        """

        return self._engine.create_collection(collection_name, allow_existing)

    def list_collections(self) -> List[dict]:
        """
        List all the collections in the bridge.
        :return: A list of collection names
        """

        return self._engine.list_collections()

    async def store(
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
        this will be used as a hint for the bridge (if it supports it, otherwise
        fallback to the default behavior).

        :param collection_name: The name of the collection to store the data in
        :param timestamp: The timestamp to associate with the data
        :param data: The data to store
        :param content_type: (Optional) The type of the data
        :param create_collection: Whether to create the collection if it does not exist
        :return: None
        """

        return await self._engine.store(
            collection_name, timestamp, data, content_type, create_collection
        )

    async def query(
        self,
        collection_name: str,
        min_timestamp: datetime = datetime(1970, 1, 1),
        max_timestamp: datetime = datetime(2100, 1, 1),
        ascending: bool = True,
        limit: int = None,
        offset: int = None,
        skip_data: bool = False,
    ) -> List[Tuple[bytes, datetime]]:
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

        return await self._engine.query(
            collection_name,
            min_timestamp,
            max_timestamp,
            ascending,
            limit,
            offset,
            skip_data,
        )

    def flush(self, collection_name: str):
        """
        Flush the buffered data in the collection with the given name.
        :param collection_name: The name of the collection to flush
        :return: None
        """

        return self._engine.flush(collection_name)

    def advanced_query(
        self,
        collection_name: str,
        query: str,
        min_timestamp: datetime,
        max_timestamp: datetime,
        limit: int = None,
        ascending: bool = True,
        offset: int = None,
    ):
        """
        Perform an advanced query on the given collection.
        :param collection_name: The name of the collection to query
        :param query: The query to perform
        :param min_timestamp: The minimum timestamp to filter the data
        :param max_timestamp: The maximum timestamp to filter the data
        :param offset: The offset of the data to retrieve
        :param limit: The limit of the data to retrieve
        :param ascending: Whether to sort the data in ascending order
        :return: The data in the collection as a list of tuples of bytes and datetime
        """

        # Max difference between timestamps is 1 day
        if (max_timestamp - min_timestamp).days > 7:
            raise AnotherWorldException("Max difference between timestamps is 7 day")

        return self._engine.advanced_query(
            collection_name,
            query,
            min_timestamp,
            max_timestamp,
            limit,
            ascending,
            offset,
        )

    def delete_collection(self, collection_name: str):
        """
        Delete the collection with the given name.
        :param collection_name: The name of the collection to delete
        :return: None
        """

        return self._engine.delete_collection(collection_name)

    def get_collection_size(self, collection_name: str) -> int:
        """
        Get the size of the collection with the given name.
        :param collection_name: The name of the collection
        :return: The size of the collection
        """

        return self._engine.get_collection_size(collection_name)
