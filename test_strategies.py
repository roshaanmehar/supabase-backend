"""
test_strategies.py - Test different scraping strategies
-----------------------------------------------------
Test script to compare card, tile, and hybrid strategies.
"""
import uuid
from datetime import datetime
from scrapers.redis_integration import create_card_scraper, create_tile_scraper, create_hybrid_scraper

def test_strategy(strategy_name: str, scraper, job_part_data: dict):
    """Test a specific scraping strategy"""
    print(f"\n{'='*60}")
    print(f"🧪 Testing {strategy_name.upper()} Strategy")
    print(f"{'='*60}")
    
    try:
        success, result = scraper.process_job_part(job_part_data)
        
        if success:
            print(f"✅ {strategy_name} strategy completed successfully!")
            print(f"📊 Records found: {result.get('records_found', 0)}")
            print(f"🔧 Items processed: {result.get('items_processed', 0)}")
            print(f"💾 Database inserted: {result.get('database_inserted', False)}")
            
            if 'strategy_used' in result:
                print(f"🎯 Actual strategy used: {result['strategy_used']}")
        else:
            print(f"❌ {strategy_name} strategy failed!")
            print(f"Error: {result.get('error', 'Unknown error')}")
        
        return success, result
        
    except Exception as e:
        print(f"❌ {strategy_name} strategy crashed: {e}")
        return False, {'error': str(e)}

def main():
    """Test all scraping strategies"""
    print("🚀 Testing All Scraping Strategies")
    print("=" * 60)
    
    # Create test job part data with proper UUIDs
    job_id = str(uuid.uuid4())
    part_id = str(uuid.uuid4())
    profile_id = str(uuid.uuid4())
    
    job_part_data = {
        'job_id': job_id,
        'profile_id': profile_id,
        'scraper_engine': 'google_maps',
        'part_data': {
            'part_id': part_id,
            'postcode': 'HU1',
            'keyword': 'supermarkets',
            'city': 'Hull',
            'state': '',
            'country': 'UK'
        },
        'created_at': datetime.now().isoformat(),
        'retry_count': 0
    }
    
    print(f"🎯 Test Query: {job_part_data['part_data']['keyword']} in {job_part_data['part_data']['postcode']}")
    print(f"🆔 Job ID: {job_id}")
    print(f"🆔 Part ID: {part_id}")
    
    # Test strategies
    strategies = [
        ('Hybrid', create_hybrid_scraper()),
        ('Card', create_card_scraper()),
        ('Tile', create_tile_scraper())
    ]
    
    results = {}
    
    for strategy_name, scraper in strategies:
        success, result = test_strategy(strategy_name, scraper, job_part_data)
        results[strategy_name] = {'success': success, 'result': result}
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 STRATEGY COMPARISON SUMMARY")
    print(f"{'='*60}")
    
    for strategy_name, data in results.items():
        success = data['success']
        result = data['result']
        
        status = "✅ SUCCESS" if success else "❌ FAILED"
        records = result.get('records_found', 0) if success else 0
        items = result.get('items_processed', 0) if success else 0
        
        print(f"{strategy_name:8} | {status:10} | Records: {records:3} | Items: {items:3}")
        
        if strategy_name == 'Hybrid' and success and 'strategy_used' in result:
            print(f"         | Hybrid chose: {result['strategy_used'].upper()}")
    
    print(f"\n💡 Recommendation: Use HYBRID strategy for automatic optimization")

if __name__ == "__main__":
    main()
