"""
redis_integration.py - Integration with Redis job queue
------------------------------------------------------
Provides the interface between Redis workers and the scraping strategies.
"""
import logging
from typing import Dict, Any, Tuple, List
from datetime import datetime
from config import SUPABASE_URL, SUPABASE_KEY, SCRAPER_DEBUG

from .strategies.hybrid_strategy import HybridStrategy
from .strategies.card_strategy import CardStrategy
from .strategies.tile_strategy import TileStrategy
from .database.supabase_adapter import SupabaseAdapter

class RedisCardScraper:
    """
    Redis-compatible wrapper for scraping strategies.
    This class provides the interface that Redis workers can call.
    """
    
    def __init__(self, supabase_url: str = None, supabase_key: str = None, debug: bool = None, strategy: str = 'hybrid'):
        if supabase_url is None:
            supabase_url = SUPABASE_URL
        if supabase_key is None:
            supabase_key = SUPABASE_KEY
        if debug is None:
            debug = SCRAPER_DEBUG
        
        self.supabase_adapter = SupabaseAdapter(supabase_url, supabase_key)
        self.strategy_name = strategy
        self.log = logging.getLogger("redis_scraper")
        
        # Initialize the chosen strategy
        if strategy == 'hybrid':
            self.scraping_strategy = HybridStrategy(self.supabase_adapter, debug=debug)
        elif strategy == 'card':
            self.scraping_strategy = CardStrategy(self.supabase_adapter, debug=debug)
        elif strategy == 'tile':
            self.scraping_strategy = TileStrategy(self.supabase_adapter, debug=debug)
        else:
            self.log.warning(f"Unknown strategy '{strategy}', defaulting to hybrid")
            self.scraping_strategy = HybridStrategy(self.supabase_adapter, debug=debug)
            self.strategy_name = 'hybrid'
    
    def process_job_part(self, job_part_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Process a single job part from Redis queue.
        This is the main entry point for Redis workers.
        
        Args:
            job_part_data: Job part data from Redis containing:
                - job_id: ID of the parent job
                - profile_id: ID of the user
                - scraper_engine: Engine type (should be 'google_maps')
                - part_data: The actual job part data
                - created_at: When the job was created
                - retry_count: Number of retries attempted
        
        Returns:
            Tuple of (success, result_data)
        """
        job_id = job_part_data.get('job_id')
        profile_id = job_part_data.get('profile_id')  # This becomes user_id
        part_data = job_part_data.get('part_data', {})
        part_id = part_data.get('part_id')  # This becomes scrape_job_part_id
        
        self.log.info(f"Processing job part {part_id} for job {job_id} using {self.strategy_name} strategy")
        
        try:
            # Update job part status to ongoing
            if part_id:
                self.supabase_adapter.update_job_part_status(part_id, 'ongoing')
            
            # Process with chosen strategy
            records, items_processed, success = self.scraping_strategy.scrape_job_part(
                part_data,
                termination_check=None  # Could add termination logic here
            )
            
            if success and records:
                # Enrich records with job metadata
                enriched_records = self._enrich_records(records, job_part_data)
                
                # Insert into database with user_id and scrape_job_part_id
                insert_success = self.supabase_adapter.insert_scraped_data(
                    enriched_records, 
                    user_id=profile_id,  # Pass profile_id as user_id
                    scraped_job_part_id=part_id  # Pass part_id as scrape_job_part_id
                )
                
                if insert_success:
                    # Update job part status to done
                    if part_id:
                        self.supabase_adapter.update_job_part_status(part_id, 'done')
                    
                    # Check if entire job is complete
                    if job_id:
                        self.supabase_adapter.check_job_completion(job_id)
                    
                    result_data = {
                        'records_found': len(records),
                        'items_processed': items_processed,
                        'database_inserted': True,
                        'part_id': part_id,
                        'job_id': job_id,
                        'strategy_used': self.strategy_name
                    }
                    
                    self.log.info(f"Successfully processed job part {part_id}: {len(records)} records using {self.strategy_name}")
                    return True, result_data
                else:
                    # Database insert failed (but this now handles duplicates gracefully)
                    if part_id:
                        self.supabase_adapter.update_job_part_status(part_id, 'failed')
                    
                    self.log.error(f"Database insert failed for job part {part_id}")
                    return False, {'error': 'Database insert failed', 'part_id': part_id}
            
            elif success and not records:
                # No records found but scraping was successful
                if part_id:
                    self.supabase_adapter.update_job_part_status(part_id, 'done')
                
                if job_id:
                    self.supabase_adapter.check_job_completion(job_id)
                
                result_data = {
                    'records_found': 0,
                    'items_processed': items_processed,
                    'database_inserted': False,
                    'part_id': part_id,
                    'job_id': job_id,
                    'strategy_used': self.strategy_name
                }
                
                self.log.info(f"Job part {part_id} completed with no records found using {self.strategy_name}")
                return True, result_data
            
            else:
                # Scraping failed
                if part_id:
                    self.supabase_adapter.update_job_part_status(part_id, 'failed')
                
                self.log.error(f"Scraping failed for job part {part_id} using {self.strategy_name}")
                return False, {'error': 'Scraping failed', 'part_id': part_id}
        
        except Exception as e:
            # Mark as failed on exception
            if part_id:
                try:
                    self.supabase_adapter.update_job_part_status(part_id, 'failed')
                except:
                    pass
            
            self.log.error(f"Exception processing job part {part_id}: {e}")
            return False, {'error': str(e), 'part_id': part_id}
    
    def _enrich_records(self, records: List[Dict[str, Any]], job_part_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Enrich scraped records with job metadata.
        
        Args:
            records: List of scraped business records
            job_part_data: Job part data from Redis
            
        Returns:
            Enriched records list
        """
        profile_id = job_part_data.get('profile_id')
        part_data = job_part_data.get('part_data', {})
        
        enriched_records = []
        for record in records:
            enriched_record = record.copy()
            
            # Add job metadata
            enriched_record['profile_id'] = profile_id
            enriched_record['keyword'] = part_data.get('keyword', '')
            enriched_record['city'] = part_data.get('city', '')
            enriched_record['postcode'] = part_data.get('postcode', '')
            enriched_record['state'] = part_data.get('state', '')
            enriched_record['country'] = part_data.get('country', '')
            
            enriched_records.append(enriched_record)
        
        return enriched_records


# Factory functions for Redis workers
def create_card_scraper(supabase_url: str = None, supabase_key: str = None, debug: bool = None) -> RedisCardScraper:
    """Create a Redis-compatible card scraper (legacy function name for compatibility)."""
    return RedisCardScraper(supabase_url, supabase_key, debug, strategy='card')

def create_tile_scraper(supabase_url: str = None, supabase_key: str = None, debug: bool = None) -> RedisCardScraper:
    """Create a Redis-compatible tile scraper."""
    return RedisCardScraper(supabase_url, supabase_key, debug, strategy='tile')

def create_hybrid_scraper(supabase_url: str = None, supabase_key: str = None, debug: bool = None) -> RedisCardScraper:
    """Create a Redis-compatible hybrid scraper (recommended)."""
    return RedisCardScraper(supabase_url, supabase_key, debug, strategy='hybrid')

def create_scraper(strategy: str = 'hybrid', supabase_url: str = None, supabase_key: str = None, debug: bool = None) -> RedisCardScraper:
    """
    Create a Redis-compatible scraper with specified strategy.
    
    Args:
        strategy: 'hybrid', 'card', or 'tile'
        supabase_url: Supabase project URL (optional, uses env var if not provided)
        supabase_key: Supabase API key (optional, uses env var if not provided)
        debug: Enable debug logging (optional, uses env var if not provided)
        
    Returns:
        RedisCardScraper instance
    """
    return RedisCardScraper(supabase_url, supabase_key, debug, strategy)
