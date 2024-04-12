#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

import uuid
from datetime import datetime
from functools import wraps
from typing import Tuple, List

from sqlalchemy import create_engine, func
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm import Session, sessionmaker

from src.core.models import DataType
from src.core.tables import Base, Collection, BufferedFragment, Fragment, Item
from src.core.utils.exception import AnotherWorldException


def with_session(func_to_wrap):
    @wraps(func_to_wrap)
    def wrapper(self, *args, **kwargs):
        with self.session_maker.begin() as session:
            return func_to_wrap(self, session, *args, **kwargs)

    return wrapper


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
        self.session_maker = sessionmaker(
            self.engine, expire_on_commit=False, autoflush=True
        )

    def _create_tables(self) -> None:
        """
        Create the tables in the database if they do not exist.
        """

        Base.metadata.create_all(self.engine)

    @with_session
    def create_collection(
        self, session: Session, collection_name: str, allow_existing: bool = False
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
            session.add(collection)
            session.commit()
        except IntegrityError:
            # If the collection already exists, rollback the transaction

            if not allow_existing:
                # If the collection already exists, and we do not allow it, raise an exception
                raise AnotherWorldException(
                    f"Collection {collection_name} already exists"
                )

    @with_session
    def get_collections(self, session: Session) -> List[dict]:
        """
        Get all collections in the database.
        :return: A list of collections
        """

        # Create an aggregate query to get all collections + add min/max timestamp and count
        # noinspection PyTypeChecker
        results = (
            session.query(
                Collection,
                func.min(Item.timestamp).label("min_timestamp"),
                func.max(Item.timestamp).label("max_timestamp"),
                func.count(Item.timestamp).label("count"),
            )
            .outerjoin(Item)
            .group_by(Collection.id)
            .all()
        )

        buffer_stat = self.get_collections_buffer_stat()

        # Create a list of dictionaries with the collection name, min/max timestamp, and count
        collections = []

        for collection, min_timestamp, max_timestamp, count in results:
            min_timestamps = [min_timestamp] if min_timestamp is not None else []
            min_timestamps.append(
                buffer_stat.get(collection.id, {}).get("min_timestamp", None)
            )

            max_timestamps = [max_timestamp] if max_timestamp is not None else []
            max_timestamps.append(
                buffer_stat.get(collection.id, {}).get("max_timestamp", None)
            )

            min_timestamp = min(list(filter(None, min_timestamps)) or [None])
            max_timestamp = max(list(filter(None, max_timestamps)) or [None])

            collections.append(
                {
                    "name": collection.name,
                    "min_timestamp": min_timestamp,
                    "max_timestamp": max_timestamp,
                    "count": count + buffer_stat.get(collection.id, {}).get("count", 0),
                }
            )

        return collections

    @with_session
    def get_collections_buffer_stat(self, session: Session) -> dict:
        buffer_stat = {}
        for buffer in session.query(BufferedFragment).all():
            buffer_stat[buffer.collection_id] = {
                "min_timestamp": datetime.fromtimestamp(
                    min([segment[2] for segment in buffer.segments] or [0])
                ),
                "max_timestamp": datetime.fromtimestamp(
                    max([segment[2] for segment in buffer.segments] or [0])
                ),
                "count": len(buffer.segments),
            }

        return buffer_stat

    @with_session
    def get_collections_with_active_buffer(self, session) -> List[Collection]:
        """
        Get all collections with an active buffer.
        :return: A list of collections with an active buffer
        """

        # noinspection PyTypeChecker
        return (
            session.query(Collection)
            .join(BufferedFragment)
            .filter(BufferedFragment.collection_id == Collection.id)
            .all()
        )

    @with_session
    def get_collection_by_name(
        self, session: Session, collection_name: str
    ) -> Collection:
        """
        Get a collection by its name.
        :param collection_name: The name of the collection to get
        :return: The collection with the given name
        """

        try:
            # noinspection PyTypeChecker
            return session.query(Collection).filter_by(name=collection_name).one()
        except NoResultFound:
            raise AnotherWorldException(f"Collection {collection_name} does not exist")

    @with_session
    def get_buffered_fragment(
        self, session: Session, collection: Collection | str
    ) -> BufferedFragment:
        """
        Get the buffered fragment for the given collection.
        :param collection: The collection to get the buffered fragment for
        :return: The buffered fragment for the given collection
        """

        if isinstance(collection, str):
            collection = self.get_collection_by_name(collection)

        # noinspection PyTypeChecker
        return (
            session.query(BufferedFragment)
            .filter_by(collection_id=collection.id)
            .one_or_none()
        )

    @with_session
    def append_segments_to_buffer_fragment(
        self, session: Session, collection_name: str, segments: Tuple[int, int, int]
    ) -> int:
        """
        Append segments to the buffered fragment for the given collection.
        :param session: The session to use
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

            session.add(buffered_fragment)

        # Append the segments to the buffered fragment
        buffered_fragment.segments = buffered_fragment.segments + [segments]

        # Associate the buffered fragment to the current session
        session.add(buffered_fragment)

        return len(buffered_fragment.segments)

    @with_session
    def has_buffered_fragment(self, session: Session, collection_name: str) -> bool:
        """
        Check if the collection has a buffered fragment.
        :param collection_name: The name of the collection to check
        :return: True if the collection has a buffered fragment, False otherwise
        """

        collection = self.get_collection_by_name(collection_name)
        return (
            session.query(BufferedFragment)
            .filter_by(collection_id=collection.id)
            .one_or_none()
            is not None
        )

    @with_session
    def associate_new_fragment_to_buffer(
        self, session: Session, collection_name: str
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
            session.query(BufferedFragment)
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

        session.add(fragment)
        # Associate the new fragment to the buffered fragment
        buffered_fragment.associated_fragment = fragment
        session.commit()

        return buffered_fragment.segments, fragment.uuid

    @with_session
    def remove_buffered_fragment_and_create_items(
        self, session: Session, collection_name: str, metadata: dict
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
            session.query(BufferedFragment)
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
                timestamp=datetime.fromtimestamp(segment[2]),
            )
            for segment in buffered_fragment.segments
        ]

        # Add the items to the database
        session.add_all(items)

        # Associate the metadata to the fragment
        buffered_fragment.associated_fragment.internal_metadata = metadata
        buffered_fragment.associated_fragment.data_type = metadata["data_type"]

        # Remove the buffered fragment
        session.delete(buffered_fragment)

        # Commit the whole transaction
        session.commit()

    @with_session
    def query(
        self,
        session: Session,
        collection: Collection,
        min_timestamp: datetime,
        max_timestamp: datetime,
        ascending: bool,
        limit: int = None,
        data_types: List[DataType] = None,
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
        :param data_types: The data types to filter the data with
        :return: The list of fragments in the collection with the given name
        """
        query = session.query(Item).filter(
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
        fragments = session.query(Fragment).filter(
            Fragment.id.in_([result.fragment_id for result in results])
        )

        if data_types and isinstance(data_types, list):
            fragments = fragments.filter(
                Fragment.data_type.in_([data_type.value for data_type in data_types])
            )

        # noinspection PyTypeChecker
        return fragments.all()
