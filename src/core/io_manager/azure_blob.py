#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

import contextlib
import os
import re
from io import BytesIO

from azure.storage.blob import BlobServiceClient

from src.core.io_manager.base import IOManager
from src.core.mixins.loggable import LoggableComponent


# noinspection PyArgumentList
class AzureBlobIOManager(LoggableComponent, IOManager):
    def __init__(self) -> None:
        """
        This class implements the IOManager interface using the Azure Blob Storage system.
        """
        super().__init__()
        self.container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        assert connection_string, "AZURE_STORAGE_CONNECTION_STRING is not set"
        assert self.container_name, "AZURE_STORAGE_CONTAINER_NAME is not set"
        self.client = BlobServiceClient.from_connection_string(connection_string)

    @staticmethod
    def _sanitize_collection_name(collection_name: str) -> str:
        return collection_name.replace("_", "-")

    @contextlib.contextmanager
    def get_fragment_context(self, collection_name: str, identifier: str, mode: str):
        collection_name = self._sanitize_collection_name(collection_name)

        assert mode in [
            "rb",
            "wb",
        ], f"Invalid mode: {mode}, must be one of ['rb', 'wb']"
        if not re.search(r"[a-zA-Z0-9_-]", identifier):
            raise ValueError(
                f"Invalid identifier: {identifier}, must match [^a-zA-Z0-9_-]"
            )

        if mode == "rb":
            blob = self.client.get_blob_client(
                self.container_name, f"{collection_name}/{identifier}"
            )
            data = blob.download_blob()

            with BytesIO(data.readall()) as buffer:
                yield buffer

        if mode == "wb":
            io = BytesIO()
            with io as buffer:
                yield buffer
                # Upload the buffer to the blob
                blob = self.client.get_blob_client(
                    self.container_name, f"{collection_name}/{identifier}"
                )
                blob.upload_blob(buffer.getvalue(), overwrite=True)

    def get_size(self, collection_name: str, fragment_uuid: str) -> int:
        blob = self.client.get_blob_client(
            self.container_name, f"{collection_name}/{fragment_uuid}"
        )
        if not blob.exists():
            return 0
        properties = blob.get_blob_properties()
        return properties.size

    def create_collection(self, collection_name: str):
        pass

    @contextlib.contextmanager
    def get_read_context(self, collection_name: str, fragment_uuid: str):
        with self.get_fragment_context(collection_name, fragment_uuid, "rb") as context:
            yield context

    @contextlib.contextmanager
    def get_write_context(self, collection_name: str, fragment_uuid: str):
        with self.get_fragment_context(collection_name, fragment_uuid, "wb") as context:
            yield context

    def get_fragment_path(self, collection_name: str, fragment_uuid: str) -> str:
        collection_name = self._sanitize_collection_name(collection_name)

        return os.path.join(collection_name, fragment_uuid)

    def remove_fragment(self, collection_name: str, fragment_uuid: str):
        collection_name = self._sanitize_collection_name(collection_name)

        fragment_path = self.get_fragment_path(collection_name, fragment_uuid)
        blob = self.client.get_blob_client(self.container_name, fragment_path)
        try:
            blob.delete_blob()
        except Exception as e:
            self.log_error(
                f"Failed to remove fragment {fragment_path} from collection {collection_name}",
                e,
            )
            return False
        return True

    def remove_fragments(self, collection_name: str, fragment_uuids: list):
        collection_name = self._sanitize_collection_name(collection_name)

        for fragment_uuid in fragment_uuids:
            self.remove_fragment(collection_name, fragment_uuid)
        return True

    def remove_collection(self, collection_name: str):
        collection_name = self._sanitize_collection_name(collection_name)

        container = self.client.get_container_client(self.container_name)
        # Delete all blob starting with the collection name followed by a /
        for blob in container.list_blobs(name_starts_with=collection_name + "/"):
            container.delete_blob(blob)
