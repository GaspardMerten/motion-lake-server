#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

import uuid
from datetime import datetime
from functools import lru_cache
from typing import Tuple, List

from sqlalchemy import create_engine, func
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm import Session

from src.core.tables import Base, Collection, BufferedFragment, Fragment, Item
from src.core.utils.exception import AnotherWorldException


class PersistenceManager:
    """
    The PersistenceManager is responsible for managing the database and the persistence of the data.
    It is responsible for creating the tables, creating collections, and storing the metadata of the
    data.
    """

    def __init__(self, db_url: str) -> None:
        # Create sqlite engine (make it read and write (force it to be read and write))
        self.engine = create_engine(db_url)
        self._create_tables()
        self.session = Session(self.engine, expire_on_commit=False, autocommit=False)

    def _create_tables(self) -> None:
        """
        Create the tables in the database if they do not exist.
        """

        Base.metadata.create_all(self.engine)

    def create_collection(
        self, collection_name: str, allow_existing: bool = False
    ) -> None:
        """
        Create a new collection with the given name.
        :param collection_name: The name of the collection to create
        :param allow_existing: Whether to allow the creation of an existing collection
        :return: None
        :raises AnotherWorldException: If the collection already exists and allow_existing is False
        """

        try:
            collection = Collection(name=collection_name)
            self.session.add(collection)
            self.session.commit()
        except IntegrityError:
            # If the collection already exists, rollback the transaction
            self.session.rollback()

            if not allow_existing:
                # If the collection already exists, and we do not allow it, raise an exception
                raise AnotherWorldException(
                    f"Collection {collection_name} already exists"
                )

    def get_collections(self) -> List[dict]:
        """
        Get all collections in the database.
        :return: A list of collections
        """

        # Create an aggregate query to get all collections + add min/max timestamp and count
        # noinspection PyTypeChecker
        results = (
            self.session.query(
                Collection,
                func.min(Item.timestamp).label("min_timestamp"),
                func.max(Item.timestamp).label("max_timestamp"),
                func.count(Item.timestamp).label("count"),
            )
            .outerjoin(Item)
            .group_by(Collection.id)
            .all()
        )
        return [
            {
                "name": collection.name,
                "min_timestamp": min_timestamp,
                "max_timestamp": max_timestamp,
                "count": count,
            }
            for collection, min_timestamp, max_timestamp, count in results
        ]

    def get_collections_with_active_buffer(self) -> List[Collection]:
        """
        Get all collections with an active buffer.
        :return: A list of collections with an active buffer
        """

        # noinspection PyTypeChecker
        return (
            self.session.query(Collection)
            .join(BufferedFragment)
            .filter(BufferedFragment.collection_id == Collection.id)
            .all()
        )

    @lru_cache(maxsize=128)
    def get_collection_by_name(self, collection_name: str) -> Collection:
        """
        Get a collection by its name.
        :param collection_name: The name of the collection to get
        :return: The collection with the given name
        """

        try:
            # noinspection PyTypeChecker
            return self.session.query(Collection).filter_by(name=collection_name).one()
        except NoResultFound:
            raise AnotherWorldException(f"Collection {collection_name} does not exist")

    def get_buffered_fragment(self, collection: Collection | str) -> BufferedFragment:
        """
        Get the buffered fragment for the given collection.
        :param collection: The collection to get the buffered fragment for
        :return: The buffered fragment for the given collection
        """

        if isinstance(collection, str):
            collection = self.get_collection_by_name(collection)

        # noinspection PyTypeChecker
        return (
            self.session.query(BufferedFragment)
            .filter_by(collection_id=collection.id)
            .one_or_none()
        )

    def append_segments_to_buffer_fragment(
        self, collection_name: str, segments: Tuple[int, int, str]
    ) -> int:
        """
        Append segments to the buffered fragment for the given collection.
        :param collection_name: The name of the collection to append the segments to
        :param segments: The segments to append to the buffered fragment
        :return: The number of segments in the buffered fragment
        """

        collection = self.get_collection_by_name(collection_name)

        # Get or create the buffered fragment
        buffered_fragment = self.get_buffered_fragment(collection)

        # Create a new buffered fragment if it does not exist
        if buffered_fragment is None:
            collection = self.get_collection_by_name(collection_name)
            buffered_fragment = BufferedFragment(
                collection_id=collection.id, segments=[]
            )

            self.session.add(buffered_fragment)

        # Append the segments to the buffered fragment
        buffered_fragment.segments = buffered_fragment.segments + [segments]

        # Commit the transaction
        self.session.commit()

        return len(buffered_fragment.segments)

    def has_buffered_fragment(self, collection_name: str) -> bool:
        """
        Check if the collection has a buffered fragment.
        :param collection_name: The name of the collection to check
        :return: True if the collection has a buffered fragment, False otherwise
        """

        collection = self.get_collection_by_name(collection_name)
        return (
            self.session.query(BufferedFragment)
            .filter_by(collection_id=collection.id)
            .one_or_none()
            is not None
        )

    def associate_new_fragment_to_buffer(
        self, collection_name: str
    ) -> Tuple[List, str]:
        """
        Associate a new fragment to the buffered fragment for the given collection.
        :param collection_name: The name of the collection to associate the new fragment to
        :return: The segments of the buffered fragment and the UUID of the new fragment
        :raises AnotherWorldException: If there is no buffered fragment for the collection
        """

        collection = self.get_collection_by_name(collection_name)

        # Get the buffered fragment
        buffered_fragment = (
            self.session.query(BufferedFragment)
            .filter_by(collection_id=collection.id)
            .one_or_none()
        )

        # Check if there is a buffered fragment
        if buffered_fragment is None:
            raise AnotherWorldException(
                f"No buffered fragment for {collection_name}, cannot associate a new fragment"
            )

        # Create a new fragment
        fragment = Fragment(
            collection_id=buffered_fragment.collection_id, uuid=str(uuid.uuid4())
        )

        self.session.add(fragment)
        self.session.commit()

        # Associate the new fragment to the buffered fragment
        buffered_fragment.associated_fragment = fragment
        self.session.commit()

        return buffered_fragment.segments, fragment.uuid

    def remove_buffered_fragment_and_create_items(
        self, collection_name: str, metadata: dict
    ):
        """
        Remove the buffered fragment and create items for the given collection.

        :param collection_name: The name of the collection to remove the buffered fragment and create items for
        :param metadata: The metadata of the data
        :return: None
        :raises AnotherWorldException: If there is no buffered fragment for the collection
        """

        collection = self.get_collection_by_name(collection_name)
        # Get the buffered fragment
        buffered_fragment = (
            self.session.query(BufferedFragment)
            .filter_by(collection_id=collection.id)
            .one_or_none()
        )

        if buffered_fragment is None:
            raise AnotherWorldException(f"No buffered fragment for {collection_name}")

        # noinspection PyTypeChecker
        items = [
            Item(
                fragment_id=buffered_fragment.associated_fragment.id,
                collection_id=buffered_fragment.collection_id,
                timestamp=datetime.fromisoformat(segment[2]),
            )
            for segment in buffered_fragment.segments
        ]

        # Add the items to the database
        self.session.add_all(items)

        # Associate the metadata to the fragment
        buffered_fragment.associated_fragment.internal_metadata = metadata

        # Remove the buffered fragment
        self.session.delete(buffered_fragment)

        # Commit the whole transaction
        self.session.commit()

    def query(
        self,
        collection: Collection,
        min_timestamp: datetime,
        max_timestamp: datetime,
        ascending: bool,
        limit: int = None,
    ) -> List[Fragment]:
        """
        Retrieve the data at the given timestamp from the collection with the given name. The data
        will be filtered using the min and max timestamp, ordered using the ascending flag, and limited
        using the limit parameter.
        :param collection: The collection to retrieve the data from
        :param min_timestamp: The minimum timestamp to retrieve the data from
        :param max_timestamp: The maximum timestamp to retrieve the data
        :param ascending: Whether to retrieve the data in ascending order
        :param limit: The maximum number of items to retrieve
        :param offset: The offset of the items to retrieve
        :return: The list of fragments in the collection with the given name
        """
        query = self.session.query(Item).filter(
            Item.collection_id == collection.id,
            Item.timestamp >= min_timestamp,
            Item.timestamp <= max_timestamp,
        )

        if ascending:
            query = query.order_by(Item.timestamp)
        else:
            query = query.order_by(Item.timestamp.desc())

        results = query.limit(limit).all()

        # Query the fragments
        fragments = (
            self.session.query(Fragment)
            .filter(Fragment.id.in_([result.fragment_id for result in results]))
            .all()
        )

        # noinspection PyTypeChecker
        return fragments
