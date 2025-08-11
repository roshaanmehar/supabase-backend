"""
simple_test.py - Simple test without complex imports
--------------------------------------------------
Direct test of individual strategies without going through the full import chain.
"""
import uuid
from datetime import datetime

def test_card_strategy_direct():
    """Test card strategy directly"""
    print("🧪 Testing Card Strategy Directly")
    print("=" * 50)
    
    try:
        # Import directly
        from scrapers.strategies.card_strategy import CardStrategy
        from scrapers.database.supabase_adapter import SupabaseAdapter
        from config import SUPABASE_URL, SUPABASE_KEY
        
        # Create adapter and strategy
        adapter = SupabaseAdapter(SUPABASE_URL, SUPABASE_KEY)
        strategy = CardStrategy(adapter, debug=True)
        
        # Test job part data
        job_part = {
            'part_id': str(uuid.uuid4()),
            'postcode': '10001',
            'keyword': 'restaurants',
            'city': 'New York',
            'state': 'NY',
            'country': 'USA'
        }
        
        print(f"🎯 Testing: {job_part['keyword']} in {job_part['postcode']}")
        
        # Run the test
        records, cards_processed, success = strategy.scrape_job_part(job_part)
        
        if success:
            print(f"✅ Card strategy successful!")
            print(f"📊 Records found: {len(records)}")
            print(f"🎴 Cards processed: {cards_processed}")
        else:
            print(f"❌ Card strategy failed!")
        
        return success
        
    except Exception as e:
        print(f"❌ Card strategy test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_tile_strategy_direct():
    """Test tile strategy directly"""
    print("\n🧪 Testing Tile Strategy Directly")
    print("=" * 50)
    
    try:
        # Import directly
        from scrapers.strategies.tile_strategy import TileStrategy
        from scrapers.database.supabase_adapter import SupabaseAdapter
        from config import SUPABASE_URL, SUPABASE_KEY
        
        # Create adapter and strategy
        adapter = SupabaseAdapter(SUPABASE_URL, SUPABASE_KEY)
        strategy = TileStrategy(adapter, debug=True)
        
        # Test job part data
        job_part = {
            'part_id': str(uuid.uuid4()),
            'postcode': '10001',
            'keyword': 'restaurants',
            'city': 'New York',
            'state': 'NY',
            'country': 'USA'
        }
        
        print(f"🎯 Testing: {job_part['keyword']} in {job_part['postcode']}")
        
        # Run the test
        records, tiles_processed, success = strategy.scrape_job_part(job_part)
        
        if success:
            print(f"✅ Tile strategy successful!")
            print(f"📊 Records found: {len(records)}")
            print(f"🔲 Tiles processed: {tiles_processed}")
        else:
            print(f"❌ Tile strategy failed!")
        
        return success
        
    except Exception as e:
        print(f"❌ Tile strategy test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_strategy_detector_direct():
    """Test strategy detector directly"""
    print("\n🧪 Testing Strategy Detector Directly")
    print("=" * 50)
    
    try:
        # Import directly
        from scrapers.strategies.strategy_detector import StrategyDetector
        
        # Create detector
        detector = StrategyDetector(debug=True)
        
        # Test job part data
        job_part = {
            'part_id': str(uuid.uuid4()),
            'postcode': '10001',
            'keyword': 'restaurants',
            'city': 'New York',
            'state': 'NY',
            'country': 'USA'
        }
        
        print(f"🎯 Testing detection for: {job_part['keyword']} in {job_part['postcode']}")
        
        # Run detection
        strategy, detection_data = detector.detect_best_strategy(job_part)
        
        print(f"✅ Detection successful!")
        print(f"🎯 Recommended strategy: {strategy.upper()}")
        print(f"📊 Phone percentage: {detection_data.get('phone_percentage', 0):.1f}%")
        print(f"🌐 Website percentage: {detection_data.get('website_percentage', 0):.1f}%")
        print(f"💡 Reason: {detection_data.get('reason', 'N/A')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Strategy detector test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Simple Strategy Testing")
    print("=" * 60)
    
    # Test each component individually
    tests = [
        ("Strategy Detector", test_strategy_detector_direct),
        ("Card Strategy", test_card_strategy_direct),
        ("Tile Strategy", test_tile_strategy_direct)
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} crashed: {e}")
            results[test_name] = False
    
    # Summary
    print(f"\n📊 Test Results Summary")
    print("=" * 30)
    for test_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {test_name}: {status}")
