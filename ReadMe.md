# Motion Lake, a Mobility Data Lake

Motion Lake is a data lake for mobility data. It can
efficiently store and retrieve large amounts of mobility data. It largely
relies on the [Apache Parquet](https://parquet.apache.org/) file format.

## Features

- Efficient storage and retrieval of large amounts of mobility data
- Support for various mobility data formats

## Environment variables for configuration

- `DB_URL`: The URL of the database used for persisting metadata
- `THREADS`: The number of threads to use for the server
- `PORT`: The port on which the server should listen
- `HOST`: The host on which the server should listen
- `IO_MANAGER`: The I/O manager to use for storing data (choices are `file_system`, `azure_blob`), defaults
  to `file_system`
- `STORAGE_PATH`: The path where the data should be stored (if using the file system backend)
- `AZURE_STORAGE_CONNECTION_STRING`: The connection string for the Azure Blob Storage account (if using the Azure Blob
  backend)
- `AZURE_STORAGE_CONTAINER_NAME`: The name of the container in the Azure Blob Storage account (if using the Azure Blob
  backend)
- `COMPRESSION`: The compression algorithm to use for storing data (choices are `gzip`, `snappy`, and any other
  algorithm supported by Apache Parquet), defaults to `gzip`
- `COMPRESSION_LEVEL`: Only applicable if using `gzip` compression, the compression level to use (0-9), defaults to `9`
