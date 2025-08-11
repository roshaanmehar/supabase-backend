"""
card_strategy.py - Card-based scraping strategy
----------------------------------------------
Implements the card-clicking approach for businesses that don't show
complete information in search results.
"""
import logging
import random
import re
import time
import unicodedata
import html
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional, Set, Callable

from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
    ElementClickInterceptedException,
    ElementNotInteractableException
)
from selenium.webdriver import ActionChains
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

class CardStrategy:
    """
    Card-based scraping strategy that clicks on business tiles to extract
    detailed information from the business detail cards.
    """
    
    def __init__(self, supabase_adapter: SupabaseAdapter = None, debug: bool = None):
        if supabase_adapter is None:
            supabase_adapter = SupabaseAdapter(SUPABASE_URL, SUPABASE_KEY)
        
        if debug is None:
            debug = SCRAPER_DEBUG
        
        self.db = supabase_adapter
        self.debug = debug
        self.log = logging.getLogger("card_strategy")
        
        # Tracking sets for deduplication
        self.processed_businesses: Set[str] = set()
        self.processed_phones: Set[int] = set()
        self.processed_tile_ids: Set[str] = set()
        
        # Statistics
        self.total_cards_processed = 0
        self.total_errors = 0
        self.consecutive_stale_errors = 0
        
    def scrape_job_part(
        self, 
        job_part: Dict[str, Any], 
        driver: webdriver.Chrome = None,
        termination_check: Optional[Callable[[], bool]] = None
    ) -> Tuple[List[Dict], int, bool]:
        """
        Scrape a single job part using card-based approach.
        
        Args:
            job_part: Job part data containing postcode, keyword, city, state, country
            driver: Optional existing WebDriver instance
            termination_check: Optional function to check if scraping should be terminated
            
        Returns:
            Tuple of (records, cards_processed, success)
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
        self.log.info(f"Starting card scrape for part: {part_id}")
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
            records = self._scrape_with_cards(
                driver, query, part_id, termination_check
            )
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.log.info("=" * 60)
            self.log.info(f"{part_id} {ARROW} {GREEN+BOLD}Card scraping completed in {duration}{RESET}")
            self.log.info(f"{part_id} {ARROW} {GREEN+BOLD}Total cards processed: {self.total_cards_processed}{RESET}")
            self.log.info(f"{part_id} {ARROW} {GREEN+BOLD}Unique businesses found: {len(records)}{RESET}")
            self.log.info("=" * 60)
            
            return records, self.total_cards_processed, True
            
        except Exception as e:
            self.log.error(f"{part_id} {ARROW} Error during card scraping: {e}")
            return [], self.total_cards_processed, False
        finally:
            if driver_created:
                try:
                    driver.quit()
                except:
                    pass
    
    def _reset_tracking(self):
        """Reset tracking variables for a new job part."""
        self.processed_businesses.clear()
        self.processed_phones.clear()
        self.processed_tile_ids.clear()
        self.total_cards_processed = 0
        self.total_errors = 0
        self.consecutive_stale_errors = 0
    
    def _scrape_with_cards(
        self, 
        driver: webdriver.Chrome, 
        query: str, 
        part_id: str,
        termination_check: Optional[Callable[[], bool]] = None
    ) -> List[Dict]:
        """
        Main scraping logic using card-based approach.
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
            
            # Wait for results
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.Nv2PK"))
                )
            except TimeoutException:
                self.log.error(f"{part_id} {ARROW} No results found")
                return records
            
            # Main scraping loop
            scroll_attempts = 0
            consecutive_no_new_data = 0
            
            while (self.total_cards_processed < RESULT_LIMIT and 
                   scroll_attempts < MAX_SCROLL_ATTEMPTS):
                
                if termination_check and termination_check():
                    self.log.info(f"{part_id} {ARROW} Termination requested during scraping loop")
                    break
                
                # Ensure no card is open
                if not self._ensure_no_card_open(driver, part_id):
                    self.log.error(f"{part_id} {ARROW} Unable to close card, refreshing page")
                    try:
                        driver.refresh()
                        time.sleep(3.0)
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.Nv2PK"))
                        )
                    except Exception as e:
                        self.log.error(f"{part_id} {ARROW} Error refreshing page: {e}")
                        break
                
                # Check for end of results
                if self._check_end_of_results(driver):
                    self.log.info(f"{part_id} {ARROW} Reached end of search results")
                    break
                
                # Get unprocessed tiles
                unprocessed_tiles = self._get_unprocessed_tiles(driver, part_id)
                
                if not unprocessed_tiles:
                    self.log.info(f"{part_id} {ARROW} No new tiles, scrolling")
                    if termination_check and termination_check():
                        break
                    
                    _, scroll_successful = self._scroll_results_feed(driver, part_id)
                    
                    if not scroll_successful:
                        scroll_attempts += 1
                        self.log.info(f"{part_id} {ARROW} Scroll unsuccessful (attempt {scroll_attempts}/{MAX_SCROLL_ATTEMPTS})")
                    else:
                        consecutive_no_new_data += 1
                        if consecutive_no_new_data >= 3:
                            scroll_attempts += 1
                            self.log.info(f"{part_id} {ARROW} Multiple scrolls with no new data")
                    continue
                
                consecutive_no_new_data = 0
                new_tiles_processed = 0
                
                # Process tiles
                for tile_idx, (tile_el, tile_id) in enumerate(unprocessed_tiles):
                    if termination_check and termination_check():
                        break
                    if self.total_cards_processed >= RESULT_LIMIT:
                        break
                    
                    # Process single tile
                    record = self._process_single_tile(
                        driver, tile_el, tile_id, part_id, tile_idx, len(unprocessed_tiles)
                    )
                    
                    if record:
                        records.append(record)
                        new_tiles_processed += 1
                        self.total_cards_processed += 1
                        
                        if termination_check and termination_check():
                            break
                    
                    # Break after processing one tile to maintain order
                    break
                
                # Handle scrolling logic
                if new_tiles_processed == 0 and unprocessed_tiles:
                    self.log.info(f"{part_id} {ARROW} No tiles processed, scrolling")
                    _, scroll_successful = self._scroll_results_feed(driver, part_id)
                    if not scroll_successful:
                        scroll_attempts += 1
                
                if scroll_attempts >= MAX_SCROLL_ATTEMPTS:
                    self.log.warning(f"{part_id} {ARROW} Max scroll attempts reached")
                    break
            
        except Exception as e:
            self.log.error(f"{part_id} {ARROW} Error in main scraping loop: {e}")
        
        return records
    
    def _process_single_tile(
        self, 
        driver: webdriver.Chrome, 
        tile_el, 
        tile_id: str, 
        part_id: str, 
        tile_idx: int, 
        total_tiles: int
    ) -> Optional[Dict]:
        """Process a single business tile and extract data from its card."""
        
        # Get tile name before clicking
        tile_name = self._get_tile_name(tile_el)
        self.log.info(f"{part_id} {ARROW} {CYAN+BOLD}Clicking tile {tile_idx + 1}/{total_tiles}: {tile_name}{RESET}")
        
        # Skip if already processed
        if tile_name and tile_name in self.processed_businesses:
            self.log.debug(f"{part_id} {ARROW} Skipping already processed: {tile_name}")
            self.processed_tile_ids.add(tile_id)
            return None
        
        self.processed_tile_ids.add(tile_id)
        
        # Click tile
        if not self._safe_click_tile(driver, tile_el, part_id, tile_idx, total_tiles):
            self.consecutive_stale_errors += 1
            self.total_errors += 1
            return None
        
        self.consecutive_stale_errors = 0
        
        try:
            # Wait for card to load
            WebDriverWait(driver, 12).until(
                lambda d: (d.find_elements(By.CSS_SELECTOR, NAME_CSS) or 
                          d.find_elements(By.XPATH, NAME_XPATH) or 
                          d.find_elements(By.CSS_SELECTOR, FALLBACK_NAME))
            )
            
            # Extract data from card
            record = self._extract_card_data(driver, part_id)
            
            if record:
                # Check for duplicates
                if record['businessname'] in self.processed_businesses:
                    self.log.debug(f"{part_id} {ARROW} Skipping duplicate business: {record['businessname']}")
                    self._safe_close_card(driver)
                    return None
                
                if record.get('phonenumber') and record['phonenumber'] in self.processed_phones:
                    self.log.debug(f"{part_id} {ARROW} Skipping duplicate phone: {record['phonenumber']}")
                    self._safe_close_card(driver)
                    return None
                
                # Add to processed sets
                self.processed_businesses.add(record['businessname'])
                if record.get('phonenumber'):
                    self.processed_phones.add(record['phonenumber'])
                
                self.log.info(f"{part_id} {ARROW} Scraped: {record['businessname']} (phone: {record.get('phonenumber', 'none')})")
            
            # Close card
            self._safe_close_card(driver)
            time.sleep(2.0)
            
            return record
            
        except TimeoutException:
            self.log.debug(f"{part_id} {ARROW} Card timeout, closing")
            self._safe_close_card(driver)
            return None
        except Exception as e:
            self.log.debug(f"{part_id} {ARROW} Error processing card: {e}")
            self._safe_close_card(driver)
            self.total_errors += 1
            return None
    
    def _extract_card_data(self, driver: webdriver.Chrome, part_id: str) -> Optional[Dict]:
        """Extract business data from the opened card."""
        
        # Wait for card processing
        self._rdelay(CLICK_WAIT_MIN, CLICK_WAIT_MAX)
        
        # Extract basic information
        name = self._safe_text_with_fallbacks(driver, NAME_CSS, NAME_XPATH, FALLBACK_NAME)
        if not name:
            return None
        
        stars = self._safe_text_with_fallbacks(driver, RATING_CSS, RATING_XPATH, FALLBACK_STARS) or "N/A"
        rev_raw = self._safe_text_with_fallbacks(driver, REVIEWS_CSS, REVIEWS_XPATH, FALLBACK_REVIEWS)
        reviews = int(re.sub(r"[^\d]", "", rev_raw)) if rev_raw else 0
        
        # Extract contact information
        address = self._extract_address(driver) or "N/A"
        website = self._extract_website(driver, name) or "N/A"
        phone_int = self._extract_phone_number(driver, name)

        # Extract coordinates
        longitude, latitude = self._extract_coordinates(driver, name)

        # Clean up address if it matches phone
        if phone_int and address == str(phone_int):
            address = "N/A"

        # Log extraction results
        self.log.info(f"{part_id} {ARROW} EXTRACTION: Name: {name}, Stars: {stars}, Reviews: {reviews}, Address: {address[:30]}, Website: {website[:30]}, Phone: {phone_int}, Coords: ({latitude}, {longitude})")

        # Build record
        record = {
            "businessname": name,
            "address": address,
            "stars": stars,
            "numberofreviews": reviews,
            "website": website,
            "phonenumber": phone_int,
            "longitude": longitude,
            "latitude": latitude,
            "emailstatus": "pending" if website != "N/A" else "nowebsite",
            "email": "N/A",
            "scraped_at": datetime.now(),
            "source": "google_maps_card",
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

        return record
    
    # Utility methods (adapted from original scraper.py)
    
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
    
    def _safe_text_with_fallbacks(self, driver: webdriver.Chrome, css: str, xpath: str, fallback: str = None) -> str:
        """Try to get text using CSS, then XPath, then fallback selector."""
        for attempt in range(MAX_STALE_RETRIES):
            try:
                try:
                    text = driver.find_element(By.CSS_SELECTOR, css).text.strip()
                    return self._normalize_text(text)
                except NoSuchElementException:
                    try:
                        text = driver.find_element(By.XPATH, xpath).text.strip()
                        return self._normalize_text(text)
                    except NoSuchElementException:
                        if fallback:
                            try:
                                text = driver.find_element(By.CSS_SELECTOR, fallback).text.strip()
                                return self._normalize_text(text)
                            except NoSuchElementException:
                                return ""
                        return ""
            except StaleElementReferenceException:
                if attempt < MAX_STALE_RETRIES - 1:
                    time.sleep(0.5)
                    continue
                else:
                    return ""
            except Exception:
                return ""
        return ""
    
    def _get_tile_name(self, tile) -> str:
        """Extract the name from a tile element before clicking it."""
        for attempt in range(MAX_STALE_RETRIES):
            try:
                name_element = tile.find_element(By.CSS_SELECTOR, TILE_NAME_CSS)
                return self._normalize_text(name_element.text.strip())
            except (NoSuchElementException, StaleElementReferenceException):
                if attempt < MAX_STALE_RETRIES - 1:
                    time.sleep(0.5)
                    continue
                else:
                    return ""
        return ""
    
    def _extract_address(self, driver: webdriver.Chrome) -> str:
        """Extract address using multiple selectors and methods."""
        time.sleep(ADDRESS_WAIT_TIME)
        
        for selector in ADDRESS_SELECTORS:
            try:
                by_method = By.XPATH if selector.startswith('/') else By.CSS_SELECTOR
                elements = driver.find_elements(by_method, selector)
                for element in elements:
                    text = self._normalize_text(element.text.strip())
                    if re.search(r'\d', text) and len(text) > 5:
                        return text
            except Exception:
                continue
        
        # JavaScript fallback
        try:
            address = driver.execute_script("""
                var addressElements = [
                    ...Array.from(document.querySelectorAll('button[data-item-id="address"] div.Io6YTe')),
                    ...Array.from(document.querySelectorAll('button[aria-label*="address"] div.Io6YTe')),
                    ...Array.from(document.querySelectorAll('div.Io6YTe.fontBodyMedium'))
                ];
                
                for (let el of addressElements) {
                    if (el && el.textContent && el.textContent.trim().length > 5 && /\\d/.test(el.textContent)) {
                        return el.textContent.trim();
                    }
                }
                return "";
            """)
            
            if address:
                return self._normalize_text(address)
        except Exception:
            pass
        
        return "N/A"
    
    def _extract_website(self, driver: webdriver.Chrome, business_name: str) -> str:
        """Extract website URL using multiple selectors and methods."""
        time.sleep(WEBSITE_WAIT_TIME)
        
        for selector in WEBSITE_SELECTORS:
            try:
                by_method = By.XPATH if selector.startswith('/') else By.CSS_SELECTOR
                elements = driver.find_elements(by_method, selector)
                for element in elements:
                    if element.tag_name == 'a':
                        href = element.get_attribute('href')
                        if href and 'google.com' not in href and href.startswith('http'):
                            return href
                    
                    text = self._normalize_text(element.text.strip())
                    if text and ('.' in text) and ('http' in text or 'www' in text):
                        return text
            except Exception:
                continue
        
        # JavaScript fallback
        try:
            website = driver.execute_script("""
                var websiteElements = [
                    ...Array.from(document.querySelectorAll('a[data-item-id="authority"]')),
                    ...Array.from(document.querySelectorAll('a[aria-label*="website"]')),
                    ...Array.from(document.querySelectorAll('a[target="_blank"]'))
                ];
                
                for (let el of websiteElements) {
                    if (el && el.href && el.href.startsWith('http') && !el.href.includes('google.com')) {
                        return el.href;
                    }
                }
                return "";
            """)
            
            if website:
                return website
        except Exception:
            pass
        
        return "N/A"
    
    def _extract_phone_number(self, driver: webdriver.Chrome, business_name: str) -> Optional[int]:
        """Extract phone number using optimized selectors."""
        time.sleep(PHONE_WAIT_TIME)
        
        for selector in PHONE_SELECTORS:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = self._normalize_text(element.text.strip())
                    if re.search(r'\d', text):
                        digits_only = re.sub(r"\D", "", text)
                        if digits_only and len(digits_only) >= 5:
                            return int(digits_only)
            except Exception:
                continue
        
        # JavaScript fallback
        try:
            phone_text = driver.execute_script("""
                var phoneElements = [
                    ...Array.from(document.querySelectorAll('button[data-item-id="phone:tel"] div.Io6YTe')),
                    ...Array.from(document.querySelectorAll('button[aria-label*="phone"] div.Io6YTe')),
                    ...Array.from(document.querySelectorAll('div.Io6YTe.fontBodyMedium'))
                ];
                
                for (let el of phoneElements) {
                    if (el && el.textContent && /\\d/.test(el.textContent)) {
                        return el.textContent.trim();
                    }
                }
                return "";
            """)
            
            if phone_text:
                phone_text = self._normalize_text(phone_text)
                digits_only = re.sub(r"\D", "", phone_text)
                if digits_only and len(digits_only) >= 5:
                    return int(digits_only)
        except Exception:
            pass
        
        return None
    
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
    
    def _check_end_of_results(self, driver: webdriver.Chrome) -> bool:
        """Check if we've reached the end of search results."""
        try:
            end_markers = [
                "You've reached the end of the list",
                "No more results",
                "End of results",
                "No additional results found"
            ]
            
            for marker in end_markers:
                try:
                    if driver.find_element(By.XPATH, f"//*[contains(text(), '{marker}')]"):
                        return True
                except NoSuchElementException:
                    continue
            
            return False
        except Exception:
            return False
    
    def _scroll_results_feed(self, driver: webdriver.Chrome, part_id: str) -> Tuple[int, bool]:
        """Scroll the results feed down in a controlled manner."""
        scroll_distance = 300
        
        for attempt in range(MAX_STALE_RETRIES):
            try:
                try:
                    feed = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]'))
                    )
                except TimeoutException:
                    driver.execute_script(f"window.scrollBy({{top: {scroll_distance}, left: 0, behavior: 'smooth'}});")
                    time.sleep(1.0)
                    return len(driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")), True
                
                current_scroll_position = driver.execute_script("return arguments[0].scrollTop", feed)
                
                driver.execute_script(f"""
                    arguments[0].scrollBy({{
                        top: {scroll_distance},
                        left: 0,
                        behavior: 'smooth'
                    }});
                """, feed)
                
                time.sleep(1.5)
                
                new_scroll_position = driver.execute_script("return arguments[0].scrollTop", feed)
                
                if new_scroll_position - current_scroll_position < 50:
                    driver.execute_script(f"arguments[0].scrollTop = {current_scroll_position + scroll_distance};", feed)
                    time.sleep(1.0)
                    
                    final_scroll_position = driver.execute_script("return arguments[0].scrollTop", feed)
                    if final_scroll_position - current_scroll_position < 50:
                        return len(driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")), False
                
                count = len(driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK"))
                self.log.info(f"{part_id} {ARROW} scrolled feed (tiles now {count})")
                return count, True
                
            except StaleElementReferenceException:
                if attempt < MAX_STALE_RETRIES - 1:
                    time.sleep(1)
                    continue
                else:
                    try:
                        driver.execute_script(f"""
                            var feeds = document.querySelectorAll('div[role="feed"]');
                            if (feeds.length > 0) {{
                                feeds[0].scrollTop += {scroll_distance};
                            }} else {{
                                window.scrollBy(0, {scroll_distance});
                            }}
                        """)
                        time.sleep(1.0)
                        return len(driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")), True
                    except Exception as e:
                        self.log.error(f"{part_id} {ARROW} JavaScript scroll error: {e}")
                        return 0, False
            except Exception as e:
                self.log.error(f"{part_id} {ARROW} scroll error: {e}")
                return 0, False
        
        return 0, False
    
    def _get_unprocessed_tiles(self, driver: webdriver.Chrome, part_id: str) -> List[Tuple[Any, str]]:
        """Get all visible tiles that haven't been processed yet."""
        try:
            all_tiles = driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
            
            unprocessed_tiles = []
            for tile in all_tiles:
                try:
                    tile_id = self._get_tile_identifier(driver, tile)
                    if tile_id not in self.processed_tile_ids:
                        position = self._get_tile_position(driver, tile)
                        unprocessed_tiles.append((tile, tile_id, position))
                except Exception:
                    continue
            
            unprocessed_tiles.sort(key=lambda x: x[2])
            result = [(t[0], t[1]) for t in unprocessed_tiles]
            self.log.info(f"{part_id} {ARROW} Found {len(result)} unprocessed tiles")
            return result
        except Exception as e:
            self.log.error(f"{part_id} {ARROW} Error getting unprocessed tiles: {e}")
            return []
    
    def _get_tile_identifier(self, driver: webdriver.Chrome, tile) -> str:
        """Get a unique identifier for a tile."""
        try:
            name = self._get_tile_name(tile)
            data_cid = tile.get_attribute("data-cid") or ""
            data_index = tile.get_attribute("data-result-index") or ""
            data_item_id = tile.get_attribute("data-item-id") or ""
            
            if data_cid or data_index or data_item_id:
                return f"{name}|{data_cid}|{data_index}|{data_item_id}"
            
            inner_html = driver.execute_script("""
                var html = arguments[0].innerHTML;
                return html.substring(0, 100);
            """, tile)
            
            html_hash = hash(inner_html)
            return f"{name}|{html_hash}"
        except Exception:
            try:
                return self._get_tile_name(tile)
            except:
                return f"unknown_tile_{time.time()}"
    
    def _get_tile_position(self, driver: webdriver.Chrome, tile) -> int:
        """Get the vertical position of a tile element."""
        try:
            return driver.execute_script("return arguments[0].getBoundingClientRect().top", tile)
        except Exception:
            return 0
    
    def _safe_click_tile(self, driver: webdriver.Chrome, tile, part_id: str, tile_idx: int, total_tiles: int) -> bool:
        """Safely click a tile with improved reliability."""
        if self._is_card_open(driver):
            self.log.warning(f"{part_id} {ARROW} A card appears to be already open before clicking new tile")
            return False
        
        tile_name = self._get_tile_name(tile)
        
        for attempt in range(MAX_STALE_RETRIES):
            try:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tile)
                time.sleep(0.5)
                
                try:
                    if not tile.is_displayed():
                        return False
                except Exception:
                    if attempt < MAX_STALE_RETRIES - 1:
                        time.sleep(0.5)
                        continue
                    else:
                        return False
                
                try:
                    WebDriverWait(driver, 3).until(EC.element_to_be_clickable(tile))
                    tile.click()
                    return True
                except (ElementClickInterceptedException, ElementNotInteractableException):
                    driver.execute_script("arguments[0].click();", tile)
                    return True
                    
            except StaleElementReferenceException:
                if attempt < MAX_STALE_RETRIES - 1:
                    time.sleep(0.5)
                else:
                    return False
            except Exception as e:
                if attempt < MAX_STALE_RETRIES - 1:
                    time.sleep(0.5)
                else:
                    return False
        
        return False
    
    def _safe_close_card(self, driver: webdriver.Chrome) -> bool:
        """Safely close the card with retry logic."""
        for attempt in range(MAX_STALE_RETRIES):
            try:
                close_button_selectors = [
                    "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.BHymgf.eiJcBe.bJUD0c > div > div > div:nth-child(3) > span > button",
                    "button[aria-label='Close']",
                    "button[jsaction*='closeButton']",
                    "button.VfPpkd-icon-LgbsSe[data-disable-idom='true']",
                    "[role='button'][aria-label='Close']",
                    "button.mL3xi"
                ]
                
                for selector in close_button_selectors:
                    try:
                        close_buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                        if close_buttons and len(close_buttons) > 0:
                            for btn in close_buttons:
                                if btn.is_displayed():
                                    try:
                                        btn.click()
                                        time.sleep(1.0)
                                        return True
                                    except:
                                        driver.execute_script("arguments[0].click();", btn)
                                        time.sleep(1.0)
                                        return True
                    except Exception:
                        continue
                
                # Try Escape key
                actions = ActionChains(driver)
                actions.send_keys(Keys.ESCAPE).perform()
                time.sleep(1.0)
                
                if not self._is_card_open(driver):
                    return True
                    
            except Exception as e:
                if attempt < MAX_STALE_RETRIES - 1:
                    time.sleep(0.5)
                else:
                    try:
                        driver.execute_script("""
                            var closeButtons = [
                                ...document.querySelectorAll('button[aria-label="Close"]'),
                                ...document.querySelectorAll('button[jsaction*="closeButton"]'),
                                ...document.querySelectorAll('[role="button"][aria-label="Close"]'),
                                ...document.querySelectorAll('button.mL3xi')
                            ];
                            
                            for (let btn of closeButtons) {
                                if (btn && btn.offsetParent !== null) {
                                    btn.click();
                                    return;
                                }
                            }
                            
                            document.dispatchEvent(new KeyboardEvent('keydown', {'key': 'Escape'}));
                        """)
                        time.sleep(1.5)
                        return not self._is_card_open(driver)
                    except:
                        return False
        
        return False
    
    def _is_card_open(self, driver: webdriver.Chrome) -> bool:
        """Check if a details card is currently open."""
        try:
            # Check for search box and search results
            try:
                search_box = driver.find_element(By.ID, "searchboxinput")
                search_results = driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
                
                if search_box and search_results and driver.current_url.startswith("https://www.google.com/maps/search/"):
                    return False
            except NoSuchElementException:
                pass
            
            # Look for business name in card context
            try:
                name_elements = driver.find_elements(By.CSS_SELECTOR, NAME_CSS) or driver.find_elements(By.XPATH, NAME_XPATH)
                if name_elements:
                    for element in name_elements:
                        if element.is_displayed():
                            parent_card = driver.execute_script("""
                                var el = arguments[0];
                                var parent = el;
                                for (var i = 0; i < 10; i++) {
                                    if (!parent) return null;
                                    if (parent.classList && 
                                        (parent.classList.contains('m6QErb') || 
                                         parent.classList.contains('DxyBCb') ||
                                         parent.classList.contains('kA9KIf'))) {
                                        return true;
                                    }
                                    parent = parent.parentElement;
                                }
                                return false;
                            """, element)
                            
                            if parent_card:
                                return True
            except Exception:
                pass
            
            return False
            
        except Exception:
            return False
    
    def _ensure_no_card_open(self, driver: webdriver.Chrome, part_id: str, max_attempts: int = 3) -> bool:
        """Ensure no card is open by checking and closing if necessary."""
        for attempt in range(max_attempts):
            if not self._is_card_open(driver):
                return True
                
            self.log.warning(f"{part_id} {ARROW} Card still open, attempting to close (attempt {attempt + 1}/{max_attempts})")
            
            self._safe_close_card(driver)
            time.sleep(1.5)
            
            if self._is_card_open(driver) and attempt >= 1:
                try:
                    back_buttons = driver.find_elements(By.CSS_SELECTOR, "button[aria-label='Back']")
                    if back_buttons and len(back_buttons) > 0:
                        for btn in back_buttons:
                            if btn.is_displayed():
                                try:
                                    btn.click()
                                except:
                                    driver.execute_script("arguments[0].click();", btn)
                                time.sleep(1.0)
                                
                    if self._is_card_open(driver):
                        driver.execute_script("""
                            var buttons = [
                                ...document.querySelectorAll('button[aria-label="Close"]'),
                                ...document.querySelectorAll('button[aria-label="Back"]'),
                                ...document.querySelectorAll('button[jsaction*="closeButton"]'),
                                ...document.querySelectorAll('[role="button"][aria-label="Close"]')
                            ];
                            
                            for (let btn of buttons) {
                                if (btn && btn.offsetParent !== null) {
                                    btn.click();
                                }
                            }
                            
                            document.dispatchEvent(new KeyboardEvent('keydown', {'key': 'Escape'}));
                        """)
                        time.sleep(1.5)
                except Exception as e:
                    self.log.warning(f"{part_id} {ARROW} Error during aggressive card closing: {e}")
        
        return not self._is_card_open(driver)

    def _extract_coordinates(self, driver: webdriver.Chrome, business_name: str) -> Tuple[Optional[float], Optional[float]]:
        """Extract longitude and latitude coordinates from the business page."""
        try:
            # Wait a moment for the page to fully load
            time.sleep(1.0)
            
            # Try to extract coordinates from the URL
            current_url = driver.current_url
            
            # Google Maps URLs often contain coordinates in format: /@lat,lng,zoom
            import re
            coord_match = re.search(r'/@(-?\d+\.\d+),(-?\d+\.\d+),', current_url)
            if coord_match:
                latitude = float(coord_match.group(1))
                longitude = float(coord_match.group(2))
                self.log.debug(f"Extracted coordinates from URL: {latitude}, {longitude}")
                return longitude, latitude
            
            # Try JavaScript method to get coordinates
            try:
                coords = driver.execute_script("""
                    // Look for coordinate data in various places
                    var coords = null;
                    
                    // Method 1: Check for data attributes
                    var elements = document.querySelectorAll('[data-lat][data-lng]');
                    if (elements.length > 0) {
                        coords = {
                            lat: parseFloat(elements[0].getAttribute('data-lat')),
                            lng: parseFloat(elements[0].getAttribute('data-lng'))
                        };
                    }
                    
                    // Method 2: Check window object for coordinate data
                    if (!coords && window.APP_INITIALIZATION_STATE) {
                        try:
                            var state = window.APP_INITIALIZATION_STATE;
                            // This is a simplified check - actual structure may vary
                            if (state && state[3] && state[3][6]) {
                                var coordData = state[3][6];
                                if (coordData && coordData.length >= 2) {
                                    coords = {
                                        lat: coordData[0],
                                        lng: coordData[1]
                                    };
                                }
                            }
                        } catch (e) {
                            // Ignore parsing errors
                        }
                    }
                    
                    return coords;
                """)
                
                if coords and 'lat' in coords and 'lng' in coords:
                    latitude = float(coords['lat'])
                    longitude = float(coords['lng'])
                    self.log.debug(f"Extracted coordinates via JavaScript: {latitude}, {longitude}")
                    return longitude, latitude
                    
            except Exception as e:
                self.log.debug(f"JavaScript coordinate extraction failed: {e}")
            
            # Method 3: Try to find coordinates in page source
            try:
                page_source = driver.page_source
                coord_patterns = [
                    r'"lat":(-?\d+\.\d+),"lng":(-?\d+\.\d+)',
                    r'\[(-?\d+\.\d+),(-?\d+\.\d+)\]',
                    r'center:\s*\{\s*lat:\s*(-?\d+\.\d+),\s*lng:\s*(-?\d+\.\d+)'
                ]
                
                for pattern in coord_patterns:
                    matches = re.findall(pattern, page_source)
                    if matches:
                        latitude = float(matches[0][0])
                        longitude = float(matches[0][1])
                        self.log.debug(f"Extracted coordinates from page source: {latitude}, {longitude}")
                        return longitude, latitude
                        
            except Exception as e:
                self.log.debug(f"Page source coordinate extraction failed: {e}")
            
            self.log.debug(f"Could not extract coordinates for {business_name}")
            return None, None
            
        except Exception as e:
            self.log.error(f"Error extracting coordinates for {business_name}: {e}")
            return None, None
