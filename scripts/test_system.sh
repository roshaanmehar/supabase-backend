#!/bin/bash

echo "Testing the Redis Job Queue System..."

# Wait for server to start
sleep 2

# Run the test client
python test_client.py
