#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.
import gc
import json
import os
from datetime import datetime

import fastapi
from pydantic import BaseModel
from starlette import status

from src.core.bridge.parquet_bridge import ParquetBridge
from src.core.io_manager.azure_blob import AzureBlobIOManager
from src.core.io_manager.file_system import FileSystemIOManager
from src.core.models import ContentType
from src.core.orchestrator import Orchestrator

__all__ = ["app"]

from src.core.persistence.persistence import PersistenceManager

from src.core.utils.exception import AnotherWorldException

app = fastapi.FastAPI()


core = Orchestrator(
    PersistenceManager(os.environ.get("DB_URL", "sqlite:///another_world.db")),
    (
        AzureBlobIOManager()
        if os.environ.get("IO_MANAGER", "file_system") == "azure_blob"
        else FileSystemIOManager(os.environ.get("STORAGE_PATH", "storage"))
    ),
    ParquetBridge(
        compression=os.environ.get("COMPRESSION", "gzip"),
        compression_level=(os.environ.get("COMPRESSION_LEVEL", None)),
    ),
)


@app.get("/collections/")
async def all_collections(response: fastapi.Response):
    try:
        return core.list_collections()
    except AnotherWorldException as e:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": str(e)}


@app.get("/query/{collection_name}")
async def query_collection(
    response: fastapi.Response,
    collection_name: str,
    min_timestamp: int,
    max_timestamp: int,
    ascending: bool,
    limit: int,
    skip_data: bool = False,
):
    """
    Query the data in the collection with the given name. The data will be filtered using the
    :param collection_name: The name of the collection to query
    :param min_timestamp: The minimum timestamp to filter the data (in milliseconds)
    :param max_timestamp: The maximum timestamp to filter the data (in milliseconds)
    :param ascending: Whether to sort the data in ascending order
    :param limit: The limit of the data to retrieve
    :param skip_data: Whether to skip the data in the results (data will be None)
    :return: The data in the collection as a list of tuples of bytes and datetime
    """
    # Convert timestamps to datetime
    min_timestamp = datetime.fromtimestamp(min_timestamp)
    max_timestamp = datetime.fromtimestamp(max_timestamp)

    try:
        results = core.query(
            collection_name,
            min_timestamp,
            max_timestamp,
            ascending,
            limit,
            None,
            skip_data,
        )
    except AnotherWorldException as e:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"results": [], "error": str(e)}
    formatted_results = [
        {"data": result[0].hex() if result[0] else "", "timestamp": result[1]}
        for result in results
    ]

    return {"results": formatted_results}


class CollectionRequest(BaseModel):
    name: str


@app.post("/flush/{collection_name}")
async def flush_buffer(response: fastapi.Response, collection_name: str):
    """
    Flush the buffered data in the collection with the given name.
    :param collection_name: The name of the collection to flush
    :return: None
    """
    try:
        core.flush(collection_name)
    except AnotherWorldException as e:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": str(e)}

    return {"message": "Buffer flushed successfully"}


@app.post("/collection/")
async def create_collection(response: fastapi.Response, collection: CollectionRequest):
    """
    Create a new collection with the given name.
    :param collection: The name of the collection to create
    :return: None
    """
    try:
        core.create_collection(collection.name, allow_existing=True)
    except AnotherWorldException as e:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": str(e)}

    return {"message": "Collection created successfully"}


@app.post("/store/{collection_name}/")
async def store_data(
    response: fastapi.Response, request: fastapi.Request, collection_name: str
):
    """
    Store the given data in the collection with the given name. The data will be stored in a
    buffered fragment until it is flushed to a new fragment. The data will be associated with
    the given timestamp. An optional data type can be provided to specify the type of the data.
    :param response: The response
    :param request: The request body
    :param collection_name: The name of the collection to store the data in
    :return:
    """
    data = await request.body()

    metadata, bytes_data = data.split(b"\n", 1)
    metadata = json.loads(metadata)
    # Convert timestamp to datetime
    timestamp = datetime.fromtimestamp(metadata["timestamp"])
    try:
        await core.store(
            collection_name,
            timestamp,
            bytes_data,
            content_type=(
                ContentType(metadata["content_type"])
                if "content_type" in metadata
                else None
            ),
            create_collection=metadata.get("create_collection", False),
        )
    except AnotherWorldException as e:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": str(e)}
    finally:
        del data
        gc.collect()

    return {"message": "Data stored successfully"}


class AdvancedQueryRequest(BaseModel):
    min_timestamp: int
    max_timestamp: int
    query: str


@app.post("/advanced/{collection_name}/")
async def advanced_query(
    response: fastapi.Response, request: AdvancedQueryRequest, collection_name: str
):
    """
    Perform an advanced query on the given collection.
    :param request: The request body
    :param collection_name: The name of the collection to query
    :return: The results of the query
    """
    # Convert timestamps to datetime
    min_timestamp = datetime.fromtimestamp(request.min_timestamp)
    max_timestamp = datetime.fromtimestamp(request.max_timestamp)

    try:
        results = core.advanced_query(
            collection_name, request.query, min_timestamp, max_timestamp
        )
    except AnotherWorldException as e:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"results": [], "error": str(e)}

    return {"results": results}


@app.delete("/delete/{collection_name}")
async def delete_collection(response: fastapi.Response, collection_name: str):
    """
    Delete the collection with the given name.
    :param collection_name: The name of the collection to delete
    :return: None
    """
    try:
        core.delete_collection(collection_name)
    except AnotherWorldException as e:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": str(e)}

    return {"message": "Collection deleted successfully"}


@app.get("/size/{collection_name}")
async def get_collection_size(response: fastapi.Response, collection_name: str):
    """
    Get the size of the collection with the given name.
    :param collection_name: The name of the collection
    :return: The size of the collection
    """
    try:
        size = core.get_collection_size(collection_name)
    except AnotherWorldException as e:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": str(e)}

    return {"size": size}
