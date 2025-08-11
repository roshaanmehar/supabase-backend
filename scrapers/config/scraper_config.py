"""
scraper_config.py - Configuration settings for scrapers
------------------------------------------------------
Contains all configuration constants and settings.
"""

# Import headless mode from main config (NOT from environment)
from config import HEADLESS_MODE, SCRAPER_DEBUG

# Browser Configuration - CONTROLLED BY CONFIG FILE ONLY
# HEADLESS_MODE is imported from config.py
# SCRAPER_DEBUG is imported from config.py

# ─────────────────── Tunables & Delays ─────────────────
# Search and timing configuration
SEARCH_DELAY_MIN = 0.3
SEARCH_DELAY_MAX = 0.7
CLICK_WAIT_MIN = 0.5
CLICK_WAIT_MAX = 1.0
CLOSE_WAIT_MIN = 0.3
CLOSE_WAIT_MAX = 0.6
SCROLL_WAIT_MIN = 1.0
SCROLL_WAIT_MAX = 1.5

# Element wait times
PHONE_WAIT_TIME = 1.0
ADDRESS_WAIT_TIME = 1.0
WEBSITE_WAIT_TIME = 1.0

# Scraping limits and thresholds
MAX_SCROLL_ATTEMPTS = 10
RESULT_LIMIT = 120
MAX_STALE_RETRIES = 3
PAGE_REFRESH_THRESHOLD = 3
DRIVER_RESET_THRESHOLD = 10

# Card processing delay (in seconds)
CARD_PROCESSING_DELAY = 6.0

# ───────────── CSS Selectors & XPaths ─────────────
# Business name
NAME_CSS = "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div.TIHn2 > div > div.lMbq3e > div:nth-child(1) > h1"
NAME_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[3]/div/div[1]/div/div/div[2]/div[2]/div/div[1]/div[1]/h1'

# Rating
RATING_CSS = "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div.TIHn2 > div > div.lMbq3e > div.LBgpqf > div > div.fontBodyMedium.dmRWX > div.F7nice > span:nth-child(1) > span:nth-child(1)"
RATING_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[3]/div/div[1]/div/div/div[2]/div[2]/div/div[1]/div[2]/div/div[1]/div[2]/span[1]/span[1]'

# Number of reviews
REVIEWS_CSS = "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div.TIHn2 > div > div.lMbq3e > div.LBgpqf > div > div.fontBodyMedium.dmRWX > div.F7nice > span:nth-child(2) > span > span"
REVIEWS_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[3]/div/div[1]/div/div/div[2]/div[2]/div/div[1]/div[2]/div/div[1]/div[2]/span[2]/span/span'

# Address - multiple selectors for better reliability
ADDRESS_SELECTORS = [
    "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div:nth-child(9) > div:nth-child(3) > button > div > div.rogA2c > div.Io6YTe.fontBodyMedium.kR99db.fdkmkc",
    '//*[@id="QA0Szd"]/div/div/div[1]/div[3]/div/div[1]/div/div/div[2]/div[9]/div[3]/button/div/div[2]/div[1]',
    "button[data-item-id='address'] div.Io6YTe",
    "button[aria-label*='address'] div.Io6YTe",
    "button[data-tooltip*='address'] div.Io6YTe",
    "div[role='button'][data-item-id*='address'] div.Io6YTe",
    "div.rogA2c div.Io6YTe.fontBodyMedium",
    "div.Io6YTe.fontBodyMedium:not(:empty)"
]

# Website - multiple selectors for better reliability
WEBSITE_SELECTORS = [
    "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div:nth-child(9) > div:nth-child(8) > a > div > div.rogA2c.ITvuef > div.Io6YTe.fontBodyMedium.kR99db.fdkmkc",
    '//*[@id="QA0Szd"]/div/div/div[1]/div[3]/div/div[1]/div/div/div[2]/div[9]/div[8]/a/div/div[2]/div[1]',
    "a[data-item-id='authority']",
    "a[aria-label*='website']",
    "a[data-tooltip*='website']",
    "a[href^='https']:not([href*='google'])",
    "div.m6QErb a[target='_blank']"
]

# Phone number - prioritized selectors (most reliable first)
PHONE_SELECTORS = [
    "button[data-item-id='phone:tel'] div.Io6YTe",
    "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div:nth-child(9) > div:nth-child(9) > button > div > div.rogA2c > div.Io6YTe.fontBodyMedium.kR99db.fdkmkc",
    "button[aria-label*='phone'] div.Io6YTe",
    "button[data-tooltip='Copy phone number'] div.Io6YTe"
]

# Tile name selector - to get name from tile before clicking
TILE_NAME_CSS = "div.qBF1Pd.fontHeadlineSmall"

# Fallback selectors
FALLBACK_NAME = "h1.DUwDvf"
FALLBACK_STARS = "span.Aq14fc"
FALLBACK_REVIEWS = "span.z5jxId"

# Search results selectors (for detection)
SEARCH_RESULTS_SELECTORS = {
    'phone': 'span.UsdlK',
    'address': 'div.W4Efsd span:nth-of-type(2)',
    'website': 'a.lcr4fd',
    'rating': 'span.MW4etd',
    'reviews': 'span.UY7F9',
    'name': 'div.qBF1Pd'
}

# Detection thresholds
COMPLETENESS_THRESHOLD = 0.8  # 80% of tiles must have all required data
SAMPLE_SIZE = 5  # Check first 5 tiles for detection
PHONE_REQUIRED = False  # Phone number is not required for storage, only for strategy detection
