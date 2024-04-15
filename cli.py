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
        "--threads",
        type=int,
        default=os.environ.get("THREADS", 1),
        help="Number of threads, default is 1",
    )
    parser.add_argument(
        "--ip",
        type=str,
        default=os.environ.get("HOST", "127.0.0.1"),
        help="IP address to run the FastAPI app, default is 127.0.0.1",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=os.environ.get("PORT", 8000),
        help="Port to run the FastAPI app on, default is 8000",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Logging level, default is INFO",
    )

    args = parser.parse_args()

    # set global logging level
    logging.basicConfig(level=logging.getLevelName(args.log_level))

    logging.info(
        f"Running FastAPI app on {args.ip}:{args.port} with {args.threads} threads using {args.db_url} and {args.storage_folder}"
    )

    # Running the FastAPI app with Uvicorn
    uvicorn.run(
        "src.runner.server:app", host=args.ip, port=args.port, workers=args.threads
    )


if __name__ == "__main__":
    main()
