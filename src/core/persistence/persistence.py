#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

from datetime import datetime
from functools import wraps, lru_cache
from typing import List

from sqlalchemy import create_engine, func, text
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm import Session, sessionmaker

from src.core.models import ContentType
from src.core.persistence.tables import (
    Base,
    Collection,
    BufferedFragment,
    Fragment,
    Item,
)
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
            self.engine, expire_on_commit=False, autoflush=False
        )

        # The protected session is used to perform high-integrity operations
        self.protected_session = self.session_maker(
            autoflush=False,
        )

    def _create_tables(self) -> None:
        """
        Create the tables in the database if they do not exist.
        """

        Base.metadata.create_all(self.engine)

    @with_session
    def log_buffer(
        self,
        session: Session,
        collection_id: int,
        timestamp: datetime,
        buffer_id,
        size: int,
        original_size: int,
        content_type: int,
    ):
        """
        Log the buffer in the database
        :param session: The session to use
        :param collection_id: The id of the collection
        :param timestamp: The timestamp of the buffer
        :param buffer_id: The id of the buffer
        :param size: The size of the buffer
        :param original_size: The original size of the buffer
        :param content_type: The data type of the buffer
        :return: None
        """
        session.execute(
            text(
                "INSERT INTO buffered_fragment (collection_id, timestamp, size, original_size, content_type, uuid,locked) "
                f"VALUES ('{collection_id}', '{timestamp}', {size}, {original_size}, {content_type}, '{buffer_id}', FALSE)"
            ),
        )

    @with_session
    def create_collection(
        self, session: Session, collection_name: str, allow_existing: bool = False
    ) -> Collection:
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
            return collection
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
            if buffer.collection_id not in buffer_stat:
                buffer_stat[buffer.collection_id] = {
                    "min_timestamp": buffer.timestamp,
                    "max_timestamp": buffer.timestamp,
                    "count": 1,
                }
            else:
                buffer_stat[buffer.collection_id]["min_timestamp"] = min(
                    buffer.timestamp, buffer_stat[buffer.collection_id]["min_timestamp"]
                )
                buffer_stat[buffer.collection_id]["max_timestamp"] = max(
                    buffer.timestamp, buffer_stat[buffer.collection_id]["max_timestamp"]
                )
                buffer_stat[buffer.collection_id]["count"] += 1
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
            .distinct()
        )

    @lru_cache(maxsize=128)
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

    def query(
        self,
        collection: Collection,
        min_timestamp: datetime,
        max_timestamp: datetime,
        ascending: bool,
        limit: int = None,
        content_types: List[ContentType] = None,
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
        :param content_types: The data types to filter the data with
        :return: The list of fragments in the collection with the given name
        """
        query = self.protected_session.query(Item).filter(
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
        fragments = self.protected_session.query(Fragment).filter(
            Fragment.uuid.in_([result.fragment_id for result in results])
        )

        if content_types and isinstance(content_types, list):
            fragments = fragments.filter(
                Fragment.content_type.in_(
                    [content_type.value for content_type in content_types]
                )
            )

        # noinspection PyTypeChecker
        return fragments.all()

    @with_session
    def flush_skipped_buffers(
        self, session: Session, collection: Collection, skipped: List[str]
    ):
        """
        Flush the buffered fragments for the given collection.
        :param session: The session to use
        :param collection: The collection to flush the buffered fragments for
        :param skipped: The buffer fragment UUIDs that were skipped (transform them to individual items)
        :return: None
        """

        # Get the skipped buffer fragments
        skipped_fragments = (
            session.query(BufferedFragment)
            .filter(BufferedFragment.uuid.in_(skipped))
            .all()
        )

        # Convert the skipped fragments to individual fragment-item
        for buffer_fragment in skipped_fragments:
            fragment = Fragment(
                collection_id=collection.id,
                uuid=buffer_fragment.uuid,
                content_type=buffer_fragment.content_type,
            )
            session.add(fragment)

            item = Item(
                fragment_id=fragment.uuid,
                collection_id=collection.id,
                timestamp=buffer_fragment.timestamp,
                size=buffer_fragment.size,
                original_size=buffer_fragment.original_size,
            )

            session.add(item)

        # Delete the skipped buffer fragments
        session.query(BufferedFragment).filter(
            BufferedFragment.uuid.in_(skipped)
        ).delete()

        session.commit()

    @with_session
    def flush_buffer(
        self,
        session: Session,
        collection: Collection,
        new_fragment_uuid: str,
        content_type: ContentType,
        buffer_uuids: List[str],
    ):
        """
        Flush the buffered data in the collection with the given name.
        :param session: The session to use
        :param collection: The collection to flush the buffered data for
        :param new_fragment_uuid: The UUID of the new fragment
        :param content_type: The data type of the new fragment
        :param buffer_uuids: The UUIDs of the buffer fragments
        :return: None
        """

        # Get the buffered fragments
        buffered_fragments = (
            session.query(BufferedFragment)
            .filter(BufferedFragment.uuid.in_(buffer_uuids))
            .all()
        )

        # Create a new fragment
        fragment = Fragment(
            collection_id=collection.id,
            uuid=new_fragment_uuid,
            content_type=content_type,
        )

        session.add(fragment)

        # Create items for the buffered fragments
        for buffered_fragment in buffered_fragments:
            item = Item(
                fragment_id=fragment.uuid,
                collection_id=collection.id,
                timestamp=buffered_fragment.timestamp,
                size=buffered_fragment.size,
                content_type=buffered_fragment.content_type,
                original_size=buffered_fragment.original_size,
            )

            session.add(item)

        # Delete the buffered fragments
        session.query(BufferedFragment).filter(
            BufferedFragment.uuid.in_(buffer_uuids)
        ).delete()

        session.commit()

    @with_session
    def get_and_lock_buffers(
        self, session: Session, collection: Collection | str
    ) -> List[BufferedFragment]:
        """
        Get the buffered fragment for the given collection.
        :param session: The session to use
        :param collection: The collection to get the buffered fragment for
        :return: The buffered fragment for the given collection
        """

        if isinstance(collection, str):
            collection = self.get_collection_by_name(collection)

        # In one transaction, get all non-locked buffered fragments for the collection, lock them, and return them
        # noinspection PyTypeChecker
        buffered_fragments = (
            session.query(BufferedFragment)
            .filter_by(collection_id=collection.id, locked=False)
            .with_for_update()
            .all()
        )

        session.execute(
            text(
                f"UPDATE buffered_fragment SET locked = TRUE WHERE collection_id = {collection.id} AND locked = FALSE"
            )
        )

        session.commit()

        # noinspection PyTypeChecker
        return buffered_fragments

    @with_session
    def get_unlocked_buffers_size(
        self, session: Session, collection: Collection | str
    ) -> int:
        """
        Get the size of the buffered fragments for the given collection.
        :param session: The session to use
        :param collection: The collection to get the size of the buffered fragments for
        :return: The size of the buffered fragments for the given collection
        """
        if isinstance(collection, str):
            collection = self.get_collection_by_name(collection)

        # Get the size of the buffered fragments
        return (
            session.query(func.sum(BufferedFragment.original_size))
            .filter_by(collection_id=collection.id, locked=False)
            .scalar()
            or 0
        )

    def query_buffers_no_lock(
        self,
        collection: Collection | str,
        min_timestamp: datetime = None,
        max_timestamp: datetime = None,
        ascending: bool = True,
        limit: int = None,
    ) -> List[BufferedFragment]:
        """
        Get the buffered fragment for the given collection.
        :param session: The session to use
        :param collection: The collection to get the buffered fragment for
        :param min_timestamp: The minimum timestamp to filter the buffered fragments
        :param max_timestamp: The maximum timestamp to filter the buffered fragments
        :return: The buffered fragment for the given collection
        """

        if isinstance(collection, str):
            collection = self.get_collection_by_name(collection)

        # In one transaction, get all non-locked buffered fragments for the collection, lock them, and return them
        # noinspection PyTypeChecker
        buffered_fragments = self.protected_session.query(BufferedFragment).filter_by(
            collection_id=collection.id
        )

        if min_timestamp:
            buffered_fragments = buffered_fragments.filter(
                BufferedFragment.timestamp >= min_timestamp
            )
        if max_timestamp:
            buffered_fragments = buffered_fragments.filter(
                BufferedFragment.timestamp <= max_timestamp
            )

        if ascending:
            buffered_fragments = buffered_fragments.order_by(BufferedFragment.timestamp)
        else:
            buffered_fragments = buffered_fragments.order_by(
                BufferedFragment.timestamp.desc()
            )

        if limit:
            buffered_fragments = buffered_fragments.limit(limit)

        # noinspection PyTypeChecker
        return buffered_fragments.all()

    @with_session
    def delete_collection(self, session: Session, collection_name: str):
        """
        Delete the collection with the given name.
        :param collection_name: The name of the collection to delete
        :return: None
        """

        collection = self.get_collection_by_name(collection_name)
        self.get_collection_by_name.cache_clear()
        # Delete the items
        session.query(Item).filter_by(collection_id=collection.id).delete()

        # Delete the fragments
        session.query(Fragment).filter_by(collection_id=collection.id).delete()

        # Delete the buffered fragments
        session.query(BufferedFragment).filter_by(collection_id=collection.id).delete()

        # Delete the collection
        session.query(Collection).filter_by(id=collection.id).delete()

        session.commit()

    @with_session
    def get_items_from_fragments(self, session: Session, fragments: List[Fragment]):
        """
        Get the items from the fragments.
        :param session: The session to use
        :param fragments: The fragments to get the items from
        :return: The items from the fragments
        """

        # Get the items from the fragments
        return (
            session.query(Item)
            .filter(Item.fragment_id.in_([fragment.uuid for fragment in fragments]))
            .all()
        )
