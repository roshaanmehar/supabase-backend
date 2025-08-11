"""
hybrid_strategy.py - Hybrid scraping strategy
--------------------------------------------
Combines tile and card strategies by first detecting which approach
is best suited for the current search results, then using that strategy.
"""
import logging
from typing import Dict, Any, Tuple, List, Optional, Callable

from ..database.supabase_adapter import SupabaseAdapter
from .strategy_detector import StrategyDetector
from .tile_strategy import TileStrategy
from .card_strategy import CardStrategy
from config import SUPABASE_URL, SUPABASE_KEY, SCRAPER_DEBUG

# ANSI color codes
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"
ARROW = "→"

class HybridStrategy:
    """
    Hybrid scraping strategy that automatically selects between
    tile-based and card-based approaches based on data availability.
    """
    
    def __init__(self, supabase_adapter: SupabaseAdapter = None, debug: bool = None):
        if supabase_adapter is None:
            supabase_adapter = SupabaseAdapter(SUPABASE_URL, SUPABASE_KEY)
        
        if debug is None:
            debug = SCRAPER_DEBUG
        
        self.db = supabase_adapter
        self.debug = debug
        self.log = logging.getLogger("hybrid_strategy")
        
        # Initialize sub-strategies
        self.detector = StrategyDetector(debug=debug)
        self.tile_strategy = TileStrategy(supabase_adapter, debug=debug)
        self.card_strategy = CardStrategy(supabase_adapter, debug=debug)
    
    def scrape_job_part(
        self, 
        job_part: Dict[str, Any], 
        driver = None,
        termination_check: Optional[Callable[[], bool]] = None
    ) -> Tuple[List[Dict], int, bool]:
        """
        Scrape a single job part using hybrid approach.
        
        Args:
            job_part: Job part data containing postcode, keyword, city, state, country
            driver: Optional existing WebDriver instance
            termination_check: Optional function to check if scraping should be terminated
        
        Returns:
            Tuple of (records, items_processed, success)
        """
        part_id = job_part.get('part_id', 'unknown')
        
        self.log.info("=" * 60)
        self.log.info(f"{part_id} {ARROW} {MAGENTA+BOLD}Starting hybrid scraping{RESET}")
        self.log.info("=" * 60)
        
        # Create driver if not provided - this will be reused throughout
        driver_created = False
        if driver is None:
            from ..core.browser_manager import make_driver
            driver = make_driver()
            driver_created = True
    
        try:
            # Step 1: Detect best strategy using the same driver
            if termination_check and termination_check():
                self.log.info(f"{part_id} {ARROW} Termination requested before strategy detection")
                return [], 0, False
            
            strategy, detection_data = self.detector.detect_best_strategy(job_part, driver)
            
            self.log.info(f"{part_id} {ARROW} {CYAN+BOLD}Strategy selected: {strategy.upper()}{RESET}")
            
            # Step 2: Execute chosen strategy using the SAME driver instance
            if termination_check and termination_check():
                self.log.info(f"{part_id} {ARROW} Termination requested before strategy execution")
                return [], 0, False
            
            if strategy == 'tile':
                records, items_processed, success = self.tile_strategy.scrape_job_part(
                    job_part, driver, termination_check  # Pass the same driver
                )
                strategy_used = 'tile'
            else:  # strategy == 'card' or fallback
                records, items_processed, success = self.card_strategy.scrape_job_part(
                    job_part, driver, termination_check  # Pass the same driver
                )
                strategy_used = 'card'
        
            # Add strategy metadata to records
            for record in records:
                record['scraping_strategy'] = strategy_used
                record['detection_data'] = detection_data
        
            self.log.info("=" * 60)
            if success:
                self.log.info(f"{part_id} {ARROW} {GREEN+BOLD}Hybrid scraping completed successfully{RESET}")
                self.log.info(f"{part_id} {ARROW} {GREEN+BOLD}Strategy used: {strategy_used.upper()}{RESET}")
                self.log.info(f"{part_id} {ARROW} {GREEN+BOLD}Records found: {len(records)}{RESET}")
                self.log.info(f"{part_id} {ARROW} {GREEN+BOLD}Items processed: {items_processed}{RESET}")
            else:
                self.log.error(f"{part_id} {ARROW} {RED+BOLD}Hybrid scraping failed{RESET}")
            self.log.info("=" * 60)
            
            return records, items_processed, success
        
        except Exception as e:
            self.log.error(f"{part_id} {ARROW} Error in hybrid strategy: {e}")
            return [], 0, False
        finally:
            # Only close the driver if we created it
            if driver_created:
                try:
                    driver.quit()
                except:
                    pass
