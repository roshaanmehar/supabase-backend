"""
quick_test.py - Quick testing script for card scraper
---------------------------------------------------
Simple script to test the card scraper functionality.
"""
import os
import sys
import uuid
from datetime import datetime

def test_card_scraper():
    """Quick test of the card scraper"""
    print("🧪 Testing Card Scraper...")
    print("=" * 50)
    
    # Test imports first
    try:
        from scrapers.redis_integration import create_card_scraper
        print("✅ Imports successful")
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        print("Make sure you've installed requirements: pip install -r requirements.txt")
        return False
    
    # Create scraper (uses environment variables)
    try:
        scraper = create_card_scraper()
        print("✅ Scraper created successfully")
    except Exception as e:
        print(f"❌ Failed to create scraper: {e}")
        print("Make sure your .env file has SUPABASE_URL and SUPABASE_KEY")
        return False
    
    # Test job part data with proper UUIDs
    job_id = str(uuid.uuid4())
    part_id = str(uuid.uuid4())
    profile_id = str(uuid.uuid4())  # Generate a proper UUID for profile
    
    job_part_data = {
        'job_id': job_id,
        'profile_id': profile_id,  # Use proper UUID
        'scraper_engine': 'google_maps',
        'part_data': {
            'part_id': part_id,
            'postcode': '10001',
            'keyword': 'restaurants',
            'city': 'New York',
            'state': 'NY',
            'country': 'USA'
        },
        'created_at': datetime.now().isoformat(),
        'retry_count': 0
    }
    
    print(f"🎯 Testing with: {job_part_data['part_data']['keyword']} in {job_part_data['part_data']['postcode']}")
    print(f"🆔 Job ID: {job_id}")
    print(f"🆔 Part ID: {part_id}")
    print(f"🆔 Profile ID: {profile_id}")
    print("⏳ Processing (this may take a few minutes)...")
    
    # Process the job part
    try:
        success, result = scraper.process_job_part(job_part_data)
        
        if success:
            print("✅ Test completed successfully!")
            print(f"📊 Records found: {result.get('records_found', 0)}")
            print(f"🎴 Cards processed: {result.get('cards_processed', 0)}")
            print(f"💾 Database inserted: {result.get('database_inserted', False)}")
        else:
            print("❌ Test failed!")
            print(f"Error: {result.get('error', 'Unknown error')}")
        
        return success
        
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_card_scraper()
