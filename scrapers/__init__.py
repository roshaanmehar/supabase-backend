"""
scrapers package - Google Maps scraping strategies
"""
from .redis_integration import (
    create_card_scraper, 
    create_tile_scraper, 
    create_hybrid_scraper, 
    create_scraper
)
from .strategies.card_strategy import CardStrategy
from .strategies.tile_strategy import TileStrategy
from .strategies.hybrid_strategy import HybridStrategy
from .strategies.strategy_detector import StrategyDetector
from .database.supabase_adapter import SupabaseAdapter

__all__ = [
    'create_card_scraper', 
    'create_tile_scraper', 
    'create_hybrid_scraper', 
    'create_scraper',
    'CardStrategy', 
    'TileStrategy', 
    'HybridStrategy',
    'StrategyDetector',
    'SupabaseAdapter'
]
