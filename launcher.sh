#!/bin/bash

# Copyright (c) 2024. Gaspard Merten
# All rights reserved.

# Parse command line arguments (if any)
while getopts ":t:h:p:l:d:" opt; do
  case $opt in
    t) THREADS=$OPTARG ;;
    h) HOST=$OPTARG ;;
    p) PORT=$OPTARG ;;
    l) LOG_LEVEL=$OPTARG ;;
    d) DB_URL=$OPTARG ;;
    \?) echo "Invalid option -$OPTARG" >&2 ;;
  esac
done


# Define default values for environment variables
THREADS="${THREADS:-1}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"
LOG_LEVEL="${LOG_LEVEL:-WARNING}"
DB_URL="${DB_URL:-postgresql://postgres:postgres@localhost:5432/postgres}"

# Logging
echo "Running FastAPI app on $HOST:$PORT with $THREADS threads"

# Running the FastAPI app with Uvicorn
gunicorn "src.runner.server:app" --workers $THREADS --worker-class uvicorn.workers.UvicornWorker --bind $HOST:$PORT \
   --max-requests 10000 --timeout 120