"""
browser_manager.py - Browser management
-------------------------------------
Functions for managing Selenium browser instances.
"""
import random
import os
from selenium import webdriver
from ..config.scraper_config import HEADLESS_MODE

def make_driver(headless: bool = None) -> webdriver.Chrome:
    """
    Create a Selenium WebDriver.
    
    Args:
        headless: Whether to run Chrome in headless mode 
                 (overrides config if provided, otherwise uses config.py setting)
        
    Returns:
        Selenium WebDriver
    """
    # Use parameter if provided, otherwise use config file setting
    if headless is None:
        headless = HEADLESS_MODE
    
    ua = random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36",
    ])
    
    opts = webdriver.ChromeOptions()
    opts.add_argument(f"user-agent={ua}")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    
    # Performance optimizations
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-sandbox")
    
    # Disable WebGL to prevent GPU errors
    opts.add_argument("--disable-webgl")
    opts.add_argument("--disable-3d-apis")
    
    # Disable software rendering fallback warnings
    opts.add_argument("--disable-software-rasterizer")
    
    # Disable logging
    opts.add_argument("--log-level=3")
    opts.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--blink-settings=imagesEnabled=false")
        print("🤖 Running in headless mode (config.py HEADLESS_MODE=True)")
    else:
        print("🖥️  Running in visible mode (config.py HEADLESS_MODE=False)")
    
    # Set page load strategy to eager for faster loading
    opts.page_load_strategy = 'eager'
    
    return webdriver.Chrome(options=opts)
