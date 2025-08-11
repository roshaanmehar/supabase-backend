import os
from dotenv import load_dotenv
from typing import List

# Load environment variables from .env file
load_dotenv()

# Worker Configuration
MAX_CONCURRENT_WORKERS = int(os.getenv('MAX_CONCURRENT_WORKERS', '1'))
GOOGLE_MAPS_QUEUE_NAME = os.getenv('GOOGLE_MAPS_QUEUE_NAME', 'google_maps_scraper')
EMAIL_QUEUE_NAME = os.getenv('EMAIL_QUEUE_NAME', 'email_scraper')

# Retry Configuration  
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))
RETRY_DELAYS = [10, 30, 60]  # seconds (exponential backoff)

# Database Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")

# Redis Configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '0'))

# Flask Configuration
FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
FLASK_PORT = int(os.getenv('FLASK_PORT', '5000'))
DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Scraper Configuration (CONFIG FILE ONLY - NOT FROM ENV)
HEADLESS_MODE = False  # Set to True only if you absolutely need headless mode
SCRAPER_DEBUG = os.getenv('SCRAPER_DEBUG', 'False').lower() == 'true'

# Default scraping strategy: 'hybrid', 'card', or 'tile'
DEFAULT_SCRAPING_STRATEGY = 'hybrid'  # Hybrid automatically chooses best approach
