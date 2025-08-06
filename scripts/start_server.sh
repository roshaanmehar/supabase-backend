#!/bin/bash

# Start Redis server (if not already running)
echo "Starting Redis server..."
redis-server --daemonize yes

# Install Python dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Set environment variables (update these with your actual values)
export SUPABASE_URL="your-supabase-url"
export SUPABASE_KEY="your-supabase-anon-key"
export REDIS_HOST="localhost"
export REDIS_PORT="6379"

# Start the Flask server
echo "Starting Flask Redis Job Queue Server..."
python app.py
