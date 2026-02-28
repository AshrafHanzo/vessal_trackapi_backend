#!/bin/bash
# Wrapper script to prevent overlapping execution of the orchestrator

# Define the lock file
LOCK_FILE="/tmp/orchestrator.lock"

# Try to acquire the lock (non-blocking)
# file descriptor 200 is used for the lock
(
    flock -n 200 || {
        echo "$(date): Orchestrator is already running. Skipping this schedule."
        exit 0
    }

    echo "$(date): Starting Orchestrator..."
    
    # Activate venv and run the script
    # Adjust path if needed: /root/track_container/venv/bin/python
    ./venv/bin/python main_orchestrator.py

) 200>"$LOCK_FILE"
