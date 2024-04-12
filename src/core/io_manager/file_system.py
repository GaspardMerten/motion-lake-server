#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

import contextlib
import os
import re
from collections import defaultdict
from multiprocessing import Lock

from src.core.io_manager.base import IOManager


# noinspection PyArgumentList
class FileSystemIOManager(IOManager):
    def __init__(self, storage_folder: str) -> None:
        """
        This class implements the IOManager interface using the fragment system.
        :param storage_folder: The folder where the data will be stored
        """
        self.fragment_locks = defaultdict(Lock)
        self.wait_queue = defaultdict(list)
        self.storage_folder = storage_folder
        # Ensure the storage folder exists
        os.makedirs(storage_folder, exist_ok=True)
        self.global_lock = Lock()

    def _acquire_lock(self, fragment_identifier: str) -> None:
        """
        Acquire the lock for the given fragment identifier.
        :param fragment_identifier: The identifier of the fragment to acquire the lock for
        :return: None
        """
        with self.global_lock:
            fragment_lock = self.fragment_locks[fragment_identifier]
            fragment_lock.acquire()

    def _release_lock(self, fragment_identifier: str) -> None:
        """
        Release the lock for the given fragment identifier.
        :param fragment_identifier: The identifier of the fragment to release the lock for
        :return: None
        """

        with self.global_lock:
            fragment_lock = self.fragment_locks[fragment_identifier]
            fragment_lock.release()
            # Check and notify the first operation in the wait queue, if any
            if self.wait_queue[fragment_identifier]:
                next_operation = self.wait_queue[fragment_identifier].pop(0)
                next_operation.set()

    @contextlib.contextmanager
    def get_fragment_context(self, collection_name: str, identifier: str, mode: str):
        fragment_identifier = os.path.join(collection_name, identifier)
        try:
            if "w" in mode or "a" in mode:
                self._acquire_lock(fragment_identifier)
            if not re.search(r"[a-zA-Z0-9_-]", identifier):
                raise ValueError(
                    f"Invalid identifier: {identifier}, must match [^a-zA-Z0-9_-]"
                )

            with open(
                os.path.join(self.storage_folder, collection_name, identifier), mode
            ) as f:
                yield f
        finally:
            if "w" in mode or "a" in mode:
                self._release_lock(fragment_identifier)

    def get_size(self, collection_name: str, fragment_uuid: str) -> int:
        if os.path.exists(
            os.path.join(self.storage_folder, collection_name, fragment_uuid)
        ):
            return os.path.getsize(
                os.path.join(self.storage_folder, collection_name, fragment_uuid)
            )
        return 0

    def create_collection(self, collection_name: str):
        os.makedirs(os.path.join(self.storage_folder, collection_name), exist_ok=True)

    @contextlib.contextmanager
    def get_read_context(self, collection_name: str, fragment_uuid: str):
        with self.get_fragment_context(collection_name, fragment_uuid, "rb") as context:
            yield context

    @contextlib.contextmanager
    def get_write_context(self, collection_name: str, fragment_uuid: str):
        with self.get_fragment_context(collection_name, fragment_uuid, "wb") as context:
            yield context

    @contextlib.contextmanager
    def get_append_context(self, collection_name: str, fragment_uuid: str):
        with self.get_fragment_context(
            collection_name, fragment_uuid, "ab+"
        ) as context:
            yield context

    def get_fragment_path(self, collection_name: str, fragment_uuid: str) -> str:
        return os.path.join(self.storage_folder, collection_name, fragment_uuid)