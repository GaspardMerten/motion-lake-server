#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

from datetime import datetime
from typing import List, Tuple

from src.core.engine import Engine
from src.core.io_manager.base import IOManager
from src.core.mixins.loggable import LoggableComponent
from src.core.persistence import PersistenceManager
from src.core.storage.base import InternalStorageManager


class Orchestrator(LoggableComponent):
    """
    The Orchestrator is the main class of the system. It is responsible for managing the storage and the
    persistence of the data. It also manages the internal storage, which is responsible for
    reading and writing the data in the storage.

    The Orchestrator is the entry point for the following operations:
    - Store data
    - Retrieve data
    - Flush the buffered data
    - Check the storage integrity
    """

    def __init__(
        self,
        persistence_manager: PersistenceManager,
        io_manager: IOManager,
        internal_storage: InternalStorageManager,
    ) -> None:
        """
        Initialize the Orchestrator with the given parameters.
        :type internal_storage: object
        :param persistence_manager: The persistence manager to use for managing the data
        :param io_manager: The IO manager to use for reading and writing the data
        :param internal_storage: The internal storage manager to use for reading and writing the data
        """
        super().__init__()
        self._engine = Engine(
            io_manager=io_manager,
            persistence_manager=persistence_manager,
            internal_storage=internal_storage,
        )
        self.log("Orchestrator initialized, launching storage integrity check")
        self._engine.check_for_storage_integrity()
        self.log("Storage integrity check completed")

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
        List all the collections in the storage.
        :return: A list of collection names
        """

        return self._engine.list_collections()

    def store(
        self,
        collection_name: str,
        timestamp: datetime,
        data: bytes,
        data_type=None,
        create_collection: bool = False,
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

        return self._engine.store(
            collection_name, timestamp, data, data_type, create_collection
        )

    def query(
        self,
        collection_name: str,
        min_timestamp: datetime = datetime(1970, 1, 1),
        max_timestamp: datetime = datetime(2100, 1, 1),
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

        return self._engine.query(
            collection_name, min_timestamp, max_timestamp, ascending, limit, offset
        )

    def flush(self, collection_name: str):
        """
        Flush the buffered data in the collection with the given name.
        :param collection_name: The name of the collection to flush
        :return: None
        """

        return self._engine.flush(collection_name)
