"""
strategy_detector.py - Strategy detection and selection
-----------------------------------------------------
Detects which scraping strategy (card vs tile) should be used
based on the completeness of data available in search results.
"""
import logging
import time
from typing import Dict, Any, Tuple, Optional
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ..core.browser_manager import make_driver
from ..config.scraper_config import *
from config import SCRAPER_DEBUG

# ANSI color codes
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"
ARROW = "→"

class StrategyDetector:
    """
    Detects which scraping strategy should be used based on
    the completeness of data available in search results.
    """
    
    def __init__(self, debug: bool = None):
        if debug is None:
            debug = SCRAPER_DEBUG
        
        self.debug = debug
        self.log = logging.getLogger("strategy_detector")
    
    def detect_best_strategy(
        self, 
        job_part: Dict[str, Any], 
        driver: webdriver.Chrome = None
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Detect the best scraping strategy for a given job part.
        
        Args:
            job_part: Job part data containing search parameters
        driver: Optional existing WebDriver instance
        
        Returns:
            Tuple of (strategy_name, detection_data)
            strategy_name: 'card' or 'tile'
            detection_data: Dictionary with detection statistics
        """
        # Extract job part information
        part_id = job_part.get('part_id', 'unknown')
        postcode = job_part.get('postcode', '')
        keyword = job_part.get('keyword', 'restaurants')
        city = job_part.get('city', '')
        state = job_part.get('state', '')
        country = job_part.get('country', 'USA')
        
        # Create search query
        if country == 'USA':
            query = f"{keyword} in {postcode} {state}"
        else:
            query = f"{keyword} in {postcode} {city}"
        
        self.log.info(f"{part_id} {ARROW} {BLUE+BOLD}Detecting best strategy for: {query}{RESET}")
        
        # Create driver only if not provided
        driver_created = False
        if driver is None:
            driver = make_driver()
            driver_created = True
        
        try:
            detection_data = self._analyze_search_results(driver, query, part_id)
        
            # Determine strategy based on detection data
            strategy = self._choose_strategy(detection_data, part_id)
        
            self.log.info(f"{part_id} {ARROW} {GREEN+BOLD}Selected strategy: {strategy.upper()}{RESET}")
        
            return strategy, detection_data
        
        except Exception as e:
            self.log.error(f"{part_id} {ARROW} Error during strategy detection: {e}")
            # Default to card strategy on error
            return 'card', {'error': str(e), 'default_strategy': True}
        finally:
            # Only close driver if we created it (not if it was passed to us)
            if driver_created:
                try:
                    driver.quit()
                except:
                    pass
    
    def _analyze_search_results(
        self, 
        driver: webdriver.Chrome, 
        query: str, 
        part_id: str
    ) -> Dict[str, Any]:
        """
        Analyze search results to determine data completeness.
        """
        # Navigate to Google Maps and perform search
        driver.get("https://www.google.com/maps")
        self._dismiss_banners(driver)
        
        # Perform search
        search_box = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "searchboxinput"))
        )
        time.sleep(1)
        search_box.clear()
        search_box.send_keys(query)
        time.sleep(1)
        search_box.send_keys(Keys.ENTER)
        
        # Wait for results
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.Nv2PK"))
            )
        except TimeoutException:
            self.log.warning(f"{part_id} {ARROW} No search results found")
            return {
                'total_tiles': 0,
                'tiles_with_phone': 0,
                'tiles_with_address': 0,
                'tiles_with_website': 0,
                'completeness_score': 0.0,
                'phone_percentage': 0.0,
                'recommendation': 'card'
            }
        
        # Get sample of tiles for analysis
        tiles = driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
        sample_size = min(SAMPLE_SIZE, len(tiles))
        sample_tiles = tiles[:sample_size]
        
        self.log.info(f"{part_id} {ARROW} Analyzing {sample_size} tiles out of {len(tiles)} total")
        
        # Analyze each tile
        analysis_data = {
            'total_tiles': len(tiles),
            'sample_size': sample_size,
            'tiles_with_phone': 0,
            'tiles_with_address': 0,
            'tiles_with_website': 0,
            'tiles_with_name': 0,
            'tiles_with_rating': 0,
            'tiles_with_reviews': 0
        }
        
        for idx, tile in enumerate(sample_tiles):
            tile_data = self._analyze_single_tile(tile, part_id, idx + 1)
            
            if tile_data['has_name']:
                analysis_data['tiles_with_name'] += 1
            if tile_data['has_phone']:
                analysis_data['tiles_with_phone'] += 1
            if tile_data['has_address']:
                analysis_data['tiles_with_address'] += 1
            if tile_data['has_website']:
                analysis_data['tiles_with_website'] += 1
            if tile_data['has_rating']:
                analysis_data['tiles_with_rating'] += 1
            if tile_data['has_reviews']:
                analysis_data['tiles_with_reviews'] += 1
        
        # Calculate percentages and completeness score
        if sample_size > 0:
            analysis_data['phone_percentage'] = (analysis_data['tiles_with_phone'] / sample_size) * 100
            analysis_data['address_percentage'] = (analysis_data['tiles_with_address'] / sample_size) * 100
            analysis_data['website_percentage'] = (analysis_data['tiles_with_website'] / sample_size) * 100
            analysis_data['name_percentage'] = (analysis_data['tiles_with_name'] / sample_size) * 100
            analysis_data['rating_percentage'] = (analysis_data['tiles_with_rating'] / sample_size) * 100
            analysis_data['reviews_percentage'] = (analysis_data['tiles_with_reviews'] / sample_size) * 100
            
            # Calculate overall completeness score
            # Phone is most important, so it gets higher weight
            completeness_score = (
                analysis_data['phone_percentage'] * 0.4 +  # 40% weight for phone
                analysis_data['name_percentage'] * 0.2 +   # 20% weight for name
                analysis_data['address_percentage'] * 0.15 + # 15% weight for address
                analysis_data['website_percentage'] * 0.15 + # 15% weight for website
                analysis_data['rating_percentage'] * 0.05 +  # 5% weight for rating
                analysis_data['reviews_percentage'] * 0.05   # 5% weight for reviews
            ) / 100
            
            analysis_data['completeness_score'] = completeness_score
        else:
            analysis_data['completeness_score'] = 0.0
            analysis_data['phone_percentage'] = 0.0
        
        return analysis_data
    
    def _analyze_single_tile(self, tile, part_id: str, tile_idx: int) -> Dict[str, bool]:
        """
        Analyze a single tile for data completeness.
        """
        tile_data = {
            'has_name': False,
            'has_phone': False,
            'has_address': False,
            'has_website': False,
            'has_rating': False,
            'has_reviews': False
        }
        
        try:
            # Check for business name
            try:
                name_element = tile.find_element(By.CSS_SELECTOR, "div.qBF1Pd")
                if name_element.text.strip():
                    tile_data['has_name'] = True
            except NoSuchElementException:
                pass
            
            # Check for phone number
            try:
                phone_element = tile.find_element(By.CSS_SELECTOR, "span.UsdlK")
                if phone_element.text.strip():
                    tile_data['has_phone'] = True
            except NoSuchElementException:
                pass
            
            # Check for address
            try:
                address_element = tile.find_element(
                    By.CSS_SELECTOR,
                    "div.W4Efsd div.W4Efsd span:nth-of-type(2) span:nth-of-type(2)"
                )
                if address_element.text.strip():
                    tile_data['has_address'] = True
            except NoSuchElementException:
                pass
            
            # Check for website
            try:
                website_element = tile.find_element(By.CSS_SELECTOR, "a.lcr4fd")
                if website_element.get_attribute("href"):
                    tile_data['has_website'] = True
            except NoSuchElementException:
                pass
            
            # Check for rating
            try:
                rating_element = tile.find_element(By.CSS_SELECTOR, "span.MW4etd")
                if rating_element.text.strip():
                    tile_data['has_rating'] = True
            except NoSuchElementException:
                pass
            
            # Check for reviews
            try:
                reviews_element = tile.find_element(By.CSS_SELECTOR, "span.UY7F9")
                if reviews_element.text.strip():
                    tile_data['has_reviews'] = True
            except NoSuchElementException:
                pass
            
        except Exception as e:
            self.log.debug(f"{part_id} {ARROW} Error analyzing tile {tile_idx}: {e}")
        
        return tile_data
    
    def _choose_strategy(self, detection_data: Dict[str, Any], part_id: str) -> str:
        """
        Choose the best strategy based on detection data.
        Simple rule: If 60% or more tiles have phone numbers, use TILE strategy.
        Otherwise, use CARD strategy.
        """
        phone_percentage = detection_data.get('phone_percentage', 0.0)
        completeness_score = detection_data.get('completeness_score', 0.0)
        
        self.log.info(f"{part_id} {ARROW} Detection results:")
        self.log.info(f"{part_id} {ARROW}   Completeness score: {completeness_score:.2f}")
        self.log.info(f"{part_id} {ARROW}   Phone percentage: {phone_percentage:.1f}%")
        
        # Simple decision logic:
        # If 60% or more tiles have phone numbers → use TILE strategy
        # If less than 60% tiles have phone numbers → use CARD strategy
        
        if phone_percentage >= 60.0:
            strategy = 'tile'
            reason = f"High phone availability ({phone_percentage:.1f}%) - using tile extraction"
        else:
            strategy = 'card'
            reason = f"Low phone availability ({phone_percentage:.1f}%) - using detailed card extraction"
        
        self.log.info(f"{part_id} {ARROW} Strategy decision: {strategy.upper()} - {reason}")
        
        detection_data['strategy'] = strategy
        detection_data['reason'] = reason
        
        return strategy
    
    def _dismiss_banners(self, driver: webdriver.Chrome):
        """Close GDPR or consent banners if present."""
        for label in ("Reject all", "Accept all", "I agree", "Dismiss"):
            try:
                btn = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, f'button[aria-label="{label}"]')
                    )
                )
                btn.click()
                self.log.debug("✕ dismissed popup (%s)", label)
                return
            except TimeoutException:
                continue
