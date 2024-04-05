#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

import argparse
import logging
import os

import uvicorn


def main():
    # Setting up the argument parser
    parser = argparse.ArgumentParser(description="Run the FastAPI application")
    parser.add_argument(
        "--db_url",
        type=str,
        default=os.environ.get("DB_URL", "postgresql://postgres:postgres@localhost:5428/postgres"),
        help="Database URL, default is 'postgresql://postgres:postgres@localhost:5428/postgres'",
    )
    parser.add_argument(
        "--storage_folder",
        type=str,
        default=os.environ.get("STORAGE_PATH", "storage"),
        help="Folder for storage, default is 'storage'",
    )
    parser.add_argument(
        "--threads", type=int,
        default=os.environ.get("THREADS", 1),
        help="Number of threads, default is 1"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=os.environ.get("HOST_PORT", 8000),
        help="Port to run the FastAPI app on, default is 8000",
    )
    parser.add_argument(
        "--ip",
        type=str,
        default=os.environ.get("HOST_IP", "127.0.0.1"),
        help="IP address to run the FastAPI app, default is 127.0.0.1",
    )

    args = parser.parse_args()

    os.environ["DB_URL"] = args.db_url
    os.environ["STORAGE_PATH"] = args.storage_folder

    # set global logging level
    logging.basicConfig(level=logging.INFO)

    logging.info(f"Running FastAPI app on {args.ip}:{args.port} with {args.threads} threads using {args.db_url} and {args.storage_folder}")

    # Running the FastAPI app with Uvicorn
    uvicorn.run(
        "src.runner.server:app", host=args.ip, port=args.port, workers=args.threads
    )


if __name__ == "__main__":
    main()
