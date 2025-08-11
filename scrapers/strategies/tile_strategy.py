"""
tile_strategy.py - Tile-based scraping strategy
----------------------------------------------
Implements the tile-based approach that extracts business information
directly from search result tiles without clicking on individual cards.
"""
import logging
import random
import re
import time
import unicodedata
import html
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional, Set, Callable

from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ..database.supabase_adapter import SupabaseAdapter
from ..core.browser_manager import make_driver
from ..config.scraper_config import *
from config import SUPABASE_URL, SUPABASE_KEY, SCRAPER_DEBUG

# ANSI color codes for console output
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
RED = "\033[91m"
BOLD = "\033[1m"
UNDERLINE = "\033[4m"
RESET = "\033[0m"
ARROW = "→"

class TileStrategy:
    """
    Tile-based scraping strategy that extracts business information
    directly from search result tiles without clicking on individual cards.
    """
    
    def __init__(self, supabase_adapter: SupabaseAdapter = None, debug: bool = None):
        if supabase_adapter is None:
            supabase_adapter = SupabaseAdapter(SUPABASE_URL, SUPABASE_KEY)
        
        if debug is None:
            debug = SCRAPER_DEBUG
        
        self.db = supabase_adapter
        self.debug = debug
        self.log = logging.getLogger("tile_strategy")
        
        # Tracking sets for deduplication
        self.processed_phones: Set[int] = set()
        
        # Statistics
        self.total_tiles_processed = 0
        self.total_errors = 0
    
    def scrape_job_part(
        self, 
        job_part: Dict[str, Any], 
        driver: webdriver.Chrome = None,
        termination_check: Optional[Callable[[], bool]] = None
    ) -> Tuple[List[Dict], int, bool]:
        """
        Scrape a single job part using tile-based approach.
        
        Args:
            job_part: Job part data containing postcode, keyword, city, state, country
            driver: Optional existing WebDriver instance
            termination_check: Optional function to check if scraping should be terminated
            
        Returns:
            Tuple of (records, tiles_processed, success)
        """
        # Extract job part information
        part_id = job_part.get('part_id')
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
        
        self.log.info("=" * 60)
        self.log.info(f"Starting tile scrape for part: {part_id}")
        self.log.info(f"Query: {query}")
        self.log.info("=" * 60)
        
        start_time = datetime.now()
        
        # Create driver if not provided - USES CONFIG FILE SETTING
        driver_created = False
        if driver is None:
            driver = make_driver()  # Uses HEADLESS_MODE from config.py
            driver_created = True
        
        try:
            # Reset tracking for this job part
            self._reset_tracking()
            
            # Perform the scraping
            records = self._scrape_with_tiles(
                driver, query, part_id, termination_check
            )
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.log.info("=" * 60)
            self.log.info(f"{part_id} {ARROW} {GREEN+BOLD}Tile scraping completed in {duration}{RESET}")
            self.log.info(f"{part_id} {ARROW} {GREEN+BOLD}Total tiles processed: {self.total_tiles_processed}{RESET}")
            self.log.info(f"{part_id} {ARROW} {GREEN+BOLD}Unique businesses found: {len(records)}{RESET}")
            self.log.info("=" * 60)
            
            return records, self.total_tiles_processed, True
            
        except Exception as e:
            self.log.error(f"{part_id} {ARROW} Error during tile scraping: {e}")
            return [], self.total_tiles_processed, False
        finally:
            if driver_created:
                try:
                    driver.quit()
                except:
                    pass
    
    def _reset_tracking(self):
        """Reset tracking variables for a new job part."""
        self.processed_phones.clear()
        self.total_tiles_processed = 0
        self.total_errors = 0
    
    def _scrape_with_tiles(
        self, 
        driver: webdriver.Chrome, 
        query: str, 
        part_id: str,
        termination_check: Optional[Callable[[], bool]] = None
    ) -> List[Dict]:
        """
        Main scraping logic using tile-based approach.
        """
        records = []
        
        try:
            # Navigate to Google Maps and perform search
            driver.get("https://www.google.com/maps")
            self._dismiss_banners(driver)
            
            if termination_check and termination_check():
                self.log.info(f"{part_id} {ARROW} Termination requested during search setup")
                return records
            
            # Perform search
            search_box = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "searchboxinput"))
            )
            self._rdelay(SEARCH_DELAY_MIN, SEARCH_DELAY_MAX)
            search_box.clear()
            search_box.send_keys(query)
            self._rdelay(SEARCH_DELAY_MIN, SEARCH_DELAY_MAX)
            search_box.send_keys(Keys.ENTER)
            self.log.info(f"{part_id} {ARROW} Search query launched: {query}")
            
            # Wait for results container
            try:
                results_container = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[aria-label*='Results']"))
                )
            except TimeoutException:
                self.log.error(f"{part_id} {ARROW} No results container found")
                return records
            
            # Scroll to load all results
            did_hit_limit, ended_due_to_max_attempts = self._scroll_until_loaded(
                driver, results_container, part_id, termination_check
            )
            
            if termination_check and termination_check():
                self.log.info(f"{part_id} {ARROW} Termination requested after scrolling")
                return records
            
            # Extract data from all tiles
            businesses = driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
            self.log.info(f"{part_id} {ARROW} Found {len(businesses)} business tiles to process")
            
            for idx, tile in enumerate(businesses):
                if termination_check and termination_check():
                    self.log.info(f"{part_id} {ARROW} Termination requested during tile processing")
                    break
                
                if self.total_tiles_processed >= RESULT_LIMIT:
                    self.log.info(f"{part_id} {ARROW} Reached result limit of {RESULT_LIMIT}")
                    break
                
                record = self._extract_tile_data(driver, tile, part_id, idx + 1, len(businesses))
                
                if record:
                    # Check for phone number duplicates
                    phone_num = record.get('phonenumber')
                    if phone_num and phone_num not in self.processed_phones:
                        records.append(record)
                        self.processed_phones.add(phone_num)
                        self.log.info(f"{part_id} {ARROW} Added: {record['businessname']} (phone: {phone_num})")
                    elif phone_num:
                        self.log.debug(f"{part_id} {ARROW} Skipping duplicate phone: {phone_num}")
                
                self.total_tiles_processed += 1
            
        except Exception as e:
            self.log.error(f"{part_id} {ARROW} Error in main tile scraping loop: {e}")
        
        return records
    
    def _scroll_until_loaded(
        self, 
        driver: webdriver.Chrome, 
        container, 
        part_id: str,
        termination_check: Optional[Callable[[], bool]] = None,
        result_limit: int = RESULT_LIMIT, 
        max_attempts: int = MAX_SCROLL_ATTEMPTS
    ) -> Tuple[bool, bool]:
        """
        Scroll until results are fully loaded or certain conditions are met.
        Returns a tuple (did_hit_limit, ended_due_to_max_attempts).
        """
        last_results_count = 0
        attempts = 0
        
        while attempts < max_attempts:
            if termination_check and termination_check():
                self.log.info(f"{part_id} {ARROW} Termination requested during scrolling")
                return False, False
            
            current_results_count = len(driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK"))
            
            if current_results_count >= result_limit:
                self.log.info(f"{part_id} {ARROW} Reached the result limit of {result_limit}. Stopping scroll.")
                return True, False
            
            # Check if "You've reached the end of the list" is visible
            try:
                container.find_element(By.XPATH, "//*[contains(text(), \"You've reached the end of the list\")]")
                self.log.info(f"{part_id} {ARROW} End of the list detected. Stopping scroll.")
                return True, False
            except NoSuchElementException:
                pass
            
            if current_results_count > last_results_count:
                self.log.info(f"{part_id} {ARROW} Loaded {current_results_count} results so far...")
                last_results_count = current_results_count
                attempts = 0  # Reset attempts counter
            else:
                attempts += 1
                self.log.info(f"{part_id} {ARROW} No new results loaded. Attempt {attempts} of {max_attempts}...")
            
            # Scroll down
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", container)
            self._rdelay(SCROLL_WAIT_MIN, SCROLL_WAIT_MAX)
        
        self.log.warning(f"{part_id} {ARROW} Max scroll attempts reached; forcibly stopped scrolling.")
        return False, True
    
    def _extract_tile_data(
        self, 
        driver: webdriver.Chrome, 
        tile, 
        part_id: str, 
        tile_idx: int, 
        total_tiles: int
    ) -> Optional[Dict]:
        """Extract business data from a single tile."""
        
        try:
            # Business name
            try:
                businessname = tile.find_element(By.CSS_SELECTOR, "div.qBF1Pd").text.strip()
                businessname = self._normalize_text(businessname)
            except NoSuchElementException:
                businessname = "N/A"
            
            if not businessname or businessname == "N/A":
                return None
            
            # Address
            try:
                address_element = tile.find_element(
                    By.CSS_SELECTOR,
                    "div.W4Efsd div.W4Efsd span:nth-of-type(2) span:nth-of-type(2)"
                )
                address = self._normalize_text(address_element.text.strip())
            except NoSuchElementException:
                address = "N/A"
            
            # Stars/Rating
            try:
                stars = tile.find_element(By.CSS_SELECTOR, "span.MW4etd").text.strip()
                stars = self._normalize_text(stars)
            except NoSuchElementException:
                stars = "N/A"
            
            # Number of reviews
            try:
                num_reviews_raw = tile.find_element(By.CSS_SELECTOR, "span.UY7F9").text.strip("()")
                num_reviews_str = re.sub(r"[^\d]", "", num_reviews_raw)
                numberofreviews = int(num_reviews_str) if num_reviews_str else 0
            except NoSuchElementException:
                numberofreviews = 0
            
            # Phone number (optional now)
            try:
                phone_raw = tile.find_element(By.CSS_SELECTOR, "span.UsdlK").text.strip()
                phone_raw = self._normalize_text(phone_raw)
            except NoSuchElementException:
                phone_raw = ""

            # Normalize phone number
            phone_digits = self._normalize_phonenumber(phone_raw)
            if phone_digits:
                phonenumber = int(phone_digits)
            else:
                phonenumber = None  # Allow None instead of skipping

            # Extract coordinates from tile
            longitude, latitude = self._extract_tile_coordinates(driver, tile, businessname)

            # Website
            try:
                website_element = tile.find_element(By.CSS_SELECTOR, "a.lcr4fd")
                website = website_element.get_attribute("href")
                if not website:
                    website = "N/A"
            except NoSuchElementException:
                website = "N/A"

            # Clean up address if it matches phone
            if phone_raw and address == phone_raw.strip():
                address = "N/A"

            # Set email status based on website availability
            emailstatus = "pending" if website != "N/A" else "nowebsite"
            email = "N/A"

            # Build record
            record = {
                "businessname": businessname,
                "address": address,
                "stars": stars,
                "numberofreviews": numberofreviews,
                "website": website,
                "phonenumber": phonenumber,
                "longitude": longitude,
                "latitude": latitude,
                "emailstatus": emailstatus,
                "email": email,
                "scraped_at": datetime.now(),
                "source": "google_maps_tile",
                "outreach": {
                    "status": "idle",
                    "campaignType": None,
                    "lastUpdatedAt": None,
                    "contactInfo": {"phoneNumbers": [], "emails": []},
                    "alignment": {"status": "unknown"},
                    "call": {
                        "isActive": False,
                        "nextScheduledCallAt": None,
                        "overallAttemptNumber": 0,
                        "retriesMadeForCurrentOverallAttempt": 0
                    },
                    "email": {
                        "isActive": False,
                        "nextScheduledEmailAt": None,
                        "emailsSentCount": 0
                    }
                }
            }

            self.log.debug(f"{part_id} {ARROW} Tile {tile_idx}/{total_tiles}: {businessname} - {phonenumber} - ({latitude}, {longitude})")
            return record
            
        except Exception as e:
            self.log.error(f"{part_id} {ARROW} Error extracting tile {tile_idx}: {e}")
            self.total_errors += 1
            return None
    
    # Utility methods
    
    def _rdelay(self, a: float, b: float, fast_mode: bool = False):
        """Random delay with option for fast mode"""
        if fast_mode:
            time.sleep(random.uniform(a * 0.5, b * 0.5))
        else:
            time.sleep(random.uniform(a, b))
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text to handle non-English characters properly."""
        if not text:
            return ""
        normalized = unicodedata.normalize('NFC', text)
        return html.unescape(normalized)
    
    def _normalize_phonenumber(self, phonenumber: str) -> str:
        """
        Remove all non-digit characters from the phone number.
        Returns only digits as a string, or '' if none exist.
        """
        if not phonenumber:
            return ""
        return re.sub(r"\D", "", phonenumber)
    
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

    def _extract_tile_coordinates(self, driver: webdriver.Chrome, tile, business_name: str) -> Tuple[Optional[float], Optional[float]]:
        """Extract coordinates from a tile element."""
        try:
            # Try to get coordinates from tile data attributes
            try:
                lat_attr = tile.get_attribute('data-lat')
                lng_attr = tile.get_attribute('data-lng')
                if lat_attr and lng_attr:
                    return float(lng_attr), float(lat_attr)
            except:
                pass
            
            # Try to find a link in the tile and extract coordinates from href
            try:
                links = tile.find_elements(By.TAG_NAME, 'a')
                for link in links:
                    href = link.get_attribute('href')
                    if href and 'maps' in href:
                        import re
                        coord_match = re.search(r'/@(-?\d+\.\d+),(-?\d+\.\d+),', href)
                        if coord_match:
                            latitude = float(coord_match.group(1))
                            longitude = float(coord_match.group(2))
                            return longitude, latitude
            except:
                pass
            
            # Try JavaScript extraction from tile
            try:
                coords = driver.execute_script("""
                    var tile = arguments[0];
                    var coords = null;
                    
                    // Look for coordinate data in tile attributes
                    if (tile.dataset && tile.dataset.lat && tile.dataset.lng) {
                        coords = {
                            lat: parseFloat(tile.dataset.lat),
                            lng: parseFloat(tile.dataset.lng)
                        };
                    }
                    
                    // Look for links with coordinate data
                    if (!coords) {
                        var links = tile.querySelectorAll('a[href*="maps"]');
                        for (var i = 0; i < links.length; i++) {
                            var href = links[i].href;
                            var match = href.match(/@(-?\\d+\\.\\d+),(-?\\d+\\.\\d+),/);
                            if (match) {
                                coords = {
                                    lat: parseFloat(match[1]),
                                    lng: parseFloat(match[2])
                                };
                                break;
                            }
                        }
                    }
                    
                    return coords;
                """, tile)
                
                if coords and 'lat' in coords and 'lng' in coords:
                    return float(coords['lng']), float(coords['lat'])
                    
            except Exception as e:
                self.log.debug(f"JavaScript tile coordinate extraction failed: {e}")
            
            self.log.debug(f"Could not extract coordinates from tile for {business_name}")
            return None, None
            
        except Exception as e:
            self.log.error(f"Error extracting tile coordinates for {business_name}: {e}")
            return None, None
