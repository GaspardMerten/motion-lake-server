#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.
from typing import Protocol


# noinspection PyArgumentList
class IOManager(Protocol):

    def get_fragment_context(self, collection_name: str, identifier: str, mode: str):
        """
        This method should return a context manager that yields a fragment-like object.
        The fragment-like object should be opened in the specified mode.

        :param collection_name: The name of the collection
        :param identifier: The identifier of the fragment
        :param mode: The mode to open the fragment in
        :return: A context manager that yields a fragment-like object
        """
        ...

    def get_size(self, collection_name: str, fragment_uuid: str) -> int:
        """
        This method should return the size of the fragment corresponding to the given fragment UUID.

        :param collection_name: The name of the collection
        :param fragment_uuid: The UUID of the fragment
        :return: The size of the fragment
        """
        ...

    def create_collection(self, collection_name: str):
        """
        This method should create a folder for the given collection.

        :param collection_name: The name of the collection
        :return: None
        """
        ...

    def get_read_context(self, collection_name: str, fragment_uuid: str):
        """
        This method should return a context manager that yields a fragment-like object for reading.

        :param collection_name: The name of the collection
        :param fragment_uuid: The UUID of the fragment
        :return: A context manager that yields a fragment-like object
        """
        ...

    def get_write_context(self, collection_name: str, fragment_uuid: str):
        """
        This method should return a context manager that yields a fragment-like object for writing.

        :param collection_name: The name of the collection
        :param fragment_uuid: The UUID of the fragment
        :return: A context manager that yields a fragment-like object
        """
        ...

    def get_fragment_path(self, collection_name: str, fragment_uuid: str) -> str:
        """
        This method should return the path to the fragment corresponding to the given fragment UUID.

        :param collection_name: The name of the collection
        :param fragment_uuid: The UUID of the fragment
        :return: The path to the fragment
        """
        ...

    def remove_fragment(self, collection_name: str, fragment_uuid: str):
        """
        This method should remove the fragment corresponding to the given fragment UUID.

        :param collection_name: The name of the collection
        :param fragment_uuid: The UUID of the fragment
        :return: None
        """
        ...

    def remove_fragments(self, collection_name: str, fragment_uuids: list):
        """
        This method should remove the fragments corresponding to the given fragment UUIDs.

        :param collection_name: The name of the collection
        :param fragment_uuids: The UUIDs of the fragments
        :return: None
        """
        ...

    def remove_collection(self, collection_name: str):
        """
        This method should delete the collection corresponding to the given collection name.

        :param collection_name: The name of the collection
        :return: None
        """
        ...
