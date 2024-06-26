#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.


import os
from datetime import datetime

import fastapi
from pydantic import BaseModel

from src.core.io_manager.file_system import FileSystemIOManager
from src.core.models import DataType
from src.core.orchestrator import Orchestrator
from src.core.persistence import PersistenceManager
from src.core.storage.parquet import ParquetDynamicStorage

__all__ = ["app"]

from src.core.utils.exception import AnotherWorldException

app = fastapi.FastAPI()

core = Orchestrator(
    PersistenceManager(os.environ.get("DB_URL", "sqlite:///sqlite3.db")),
    FileSystemIOManager(os.environ.get("STORAGE_PATH", "storage")),
    ParquetDynamicStorage(
        compression=os.environ.get("COMPRESSION", "snappy"),
        compression_level=(os.environ.get("COMPRESSION_LEVEL", None)),
    ),
)


@app.get("/collections/")
async def all_collections():
    try:
        return core.list_collections()
    except AnotherWorldException as e:
        return {"error": str(e)}


@app.get("/query/{collection_name}")
async def query_collection(
    collection_name: str,
    min_timestamp: int,
    max_timestamp: int,
    ascending: bool,
    limit: int,
    offset: int,
):
    """
    Query the data in the collection with the given name. The data will be filtered using the
    :param collection_name: The name of the collection to query
    :param min_timestamp: The minimum timestamp to filter the data (in milliseconds)
    :param max_timestamp: The maximum timestamp to filter the data (in milliseconds)
    :param ascending: Whether to sort the data in ascending order
    :param limit: The limit of the data to retrieve
    :param offset: The offset of the data to retrieve
    :return: The data in the collection as a list of tuples of bytes and datetime
    """
    # Convert timestamps to datetime
    min_timestamp = datetime.fromtimestamp(min_timestamp/1000)
    max_timestamp = datetime.fromtimestamp(max_timestamp/1000)

    try:
        results = core.query(
            collection_name, min_timestamp, max_timestamp, ascending, limit, offset
        )
    except AnotherWorldException as e:
        return {"results": [], "error": str(e)}

    formatted_results = [
        {"data": result[0], "timestamp": result[1]}
        for result in results
    ]

    return {"results": formatted_results}


class CollectionRequest(BaseModel):
    name: str


@app.post("/flush/{collection_name}")
async def flush_buffer(collection_name: str):
    """
    Flush the buffered data in the collection with the given name.
    :param collection_name: The name of the collection to flush
    :return: None
    """
    try:
        core.flush(collection_name)
    except AnotherWorldException as e:
        return {"error": str(e)}

    return {"message": "Buffer flushed successfully"}


@app.post("/collection/")
async def create_collection(collection: CollectionRequest):
    """
    Create a new collection with the given name.
    :param collection: The name of the collection to create
    :return: None
    """
    try:
        core.create_collection(collection.name, allow_existing=True)
    except AnotherWorldException as e:
        return {"error": str(e)}

    return {"message": "Collection created successfully"}


class StoreRequest(BaseModel):
    timestamp: int | float
    data: bytes
    content_type: int | None
    create_collection: bool = False


@app.post("/store/{collection_name}/")
async def store_data(request: StoreRequest, collection_name: str):
    """
    Store the given data in the collection with the given name. The data will be stored in a
    :param request: The request body
    :param collection_name: The name of the collection to store the data in
    :return:
    """
    # Convert timestamp to datetime
    timestamp = datetime.fromtimestamp(request.timestamp)

    try:
        core.store(
            collection_name,
            timestamp,
            bytes.fromhex(request.data.decode()),
            data_type=DataType(request.content_type) if request.content_type is not None else None,
            create_collection=request.create_collection,
        )
    except AnotherWorldException as e:
        return {"error": str(e)}

    return {"message": "Data stored successfully"}
