#!/bin/bash

# Path to the Python script
SCRIPT="cli.py"

# Maximum allowed memory size in kilobytes (e.g., 100000 KB for 100MB)
MAX_MEM=2000000000

# Function to get current memory usage of the Python script
get_mem_usage() {
    echo $(ps aux | grep "[p]ython $SCRIPT" | awk '{print $6}')
}

# Start the Python script in the background
python $SCRIPT &
SCRIPT_PID=$!

echo "Script launched, PID: $SCRIPT_PID"

# Monitor loop
while true; do
    # Check memory usage
    MEM_USAGE=$(get_mem_usage)
    if [[ ! -z $MEM_USAGE ]]; then
        echo "Current memory usage: $MEM_USAGE KB"
        if [ $MEM_USAGE -gt $MAX_MEM ]; then
            echo "Memory limit exceeded. Restarting script..."
            kill $SCRIPT_PID
            python $SCRIPT &
            SCRIPT_PID=$!
            echo "Script relaunched, PID: $SCRIPT_PID"
        fi
    else
        echo "Script is not running. Restarting..."
        python $SCRIPT &
        SCRIPT_PID=$!
        echo "Script relaunched, PID: $SCRIPT_PID"
    fi
    sleep 5 # Check every 5 seconds
done
