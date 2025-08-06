import os
from typing import List

# Worker Configuration
MAX_CONCURRENT_WORKERS = 10
GOOGLE_MAPS_QUEUE_NAME = "google_maps_scraper"
EMAIL_QUEUE_NAME = "email_scraper"

# Retry Configuration  
MAX_RETRIES = 3
RETRY_DELAYS = [10, 30, 60]  # seconds (exponential backoff)

# Database Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://knlmnpoqcfktixnusbmu.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtubG1ucG9xY2ZrdGl4bnVzYm11Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTQxNDIyMDEsImV4cCI6MjA2OTcxODIwMX0.n9tlMfrKVoXPldYxj5bK5bADX8KJzLjVawrUYPZ2seA')

# Redis Configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

# Flask Configuration
FLASK_HOST = '0.0.0.0'
FLASK_PORT = 5000
DEBUG = True

# Logging Configuration
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
