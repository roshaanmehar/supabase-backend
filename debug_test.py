"""
debug_test.py - Debug the scraper issues
---------------------------------------
Helps identify and fix configuration issues.
"""
import os
import sys
import uuid
from pathlib import Path

def debug_environment():
    """Debug environment configuration"""
    print("🔍 Environment Debug")
    print("=" * 50)
    
    # Check .env file
    env_path = Path('.env')
    if env_path.exists():
        print("✅ .env file exists")
        with open('.env', 'r') as f:
            content = f.read()
            print("📄 .env contents:")
            for line in content.split('\n'):
                if line.strip() and not line.startswith('#'):
                    key = line.split('=')[0]
                    if 'KEY' in key or 'URL' in key:
                        print(f"  {key}=***hidden***")
                    else:
                        print(f"  {line}")
    else:
        print("❌ .env file not found")
        return False
    
    # Check environment variables
    print("\n🌍 Environment Variables:")
    env_vars = ['SUPABASE_URL', 'SUPABASE_KEY', 'SCRAPER_DEBUG']
    for var in env_vars:
        value = os.getenv(var)
        if value:
            if 'KEY' in var or 'URL' in var:
                print(f"  {var}=***hidden***")
            else:
                print(f"  {var}={value}")
        else:
            print(f"  {var}=NOT SET")
    
    return True

def debug_config():
    """Debug config file settings"""
    print("\n🔍 Config File Debug")
    print("=" * 30)
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✅ dotenv loaded")
    except Exception as e:
        print(f"❌ dotenv failed: {e}")
        return False
    
    try:
        from config import SUPABASE_URL, SUPABASE_KEY, HEADLESS_MODE, SCRAPER_DEBUG
        print("✅ config imported")
        print(f"  HEADLESS_MODE from config.py: {HEADLESS_MODE}")
        print(f"  SCRAPER_DEBUG from config.py: {SCRAPER_DEBUG}")
        
        # Check scraper config
        from scrapers.config.scraper_config import HEADLESS_MODE as SCRAPER_HEADLESS
        print(f"  HEADLESS_MODE from scraper_config.py: {SCRAPER_HEADLESS}")
        
        if HEADLESS_MODE != SCRAPER_HEADLESS:
            print("⚠️  HEADLESS_MODE mismatch between config files!")
        else:
            print("✅ HEADLESS_MODE consistent across configs")
            
    except Exception as e:
        print(f"❌ config failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def debug_imports():
    """Debug import issues"""
    print("\n🔍 Import Debug")
    print("=" * 30)
    
    try:
        from scrapers.redis_integration import create_card_scraper
        print("✅ scrapers imported")
    except Exception as e:
        print(f"❌ scrapers failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def debug_database():
    """Debug database connection"""
    print("\n🔍 Database Debug")
    print("=" * 30)
    
    try:
        from scrapers.database.supabase_adapter import SupabaseAdapter
        from config import SUPABASE_URL, SUPABASE_KEY
        
        adapter = SupabaseAdapter(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Database adapter created")
        
        # Test with proper UUID
        test_uuid = str(uuid.uuid4())
        print(f"🧪 Testing with UUID: {test_uuid}")
        
        # This should fail gracefully if the UUID doesn't exist
        result = adapter.get_job_stats(test_uuid)
        print(f"✅ Database query successful (empty result expected): {result}")
        
        return True
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def debug_browser():
    """Debug browser configuration"""
    print("\n🔍 Browser Debug")
    print("=" * 30)
    
    try:
        from scrapers.core.browser_manager import make_driver
        from config import HEADLESS_MODE
        
        print(f"📊 HEADLESS_MODE from config.py: {HEADLESS_MODE}")
        
        # Test browser creation with config setting
        print("🌐 Creating browser with config setting...")
        driver = make_driver()  # Uses config.py HEADLESS_MODE
        print("✅ Browser created successfully")
        
        # Test navigation
        print("🔗 Testing navigation...")
        driver.get("https://www.google.com")
        print(f"✅ Navigation successful: {driver.title}")
        
        # Close browser
        driver.quit()
        print("✅ Browser closed")
        
        return True
    except Exception as e:
        print(f"❌ Browser test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def debug_scraper():
    """Debug scraper with proper UUIDs"""
    print("\n🔍 Scraper Debug")
    print("=" * 30)
    
    try:
        from scrapers.redis_integration import create_card_scraper
        
        scraper = create_card_scraper()
        print("✅ Scraper created")
        
        # Create proper UUIDs
        job_id = str(uuid.uuid4())
        part_id = str(uuid.uuid4())
        profile_id = str(uuid.uuid4())
        
        job_part_data = {
            'job_id': job_id,
            'profile_id': profile_id,
            'scraper_engine': 'google_maps',
            'part_data': {
                'part_id': part_id,
                'postcode': '90210',  # Try a different postcode
                'keyword': 'coffee shops',
                'city': 'Beverly Hills',
                '': '90210',  # Try a different postcode
                'keyword': 'coffee shops',
                'city': 'Beverly Hills',
                'state': 'CA',
                'country': 'USA'
            },
            'created_at': '2024-01-01T10:00:00Z',
            'retry_count': 0
        }
        
        print(f"🎯 Testing with proper UUIDs:")
        print(f"  Job ID: {job_id}")
        print(f"  Part ID: {part_id}")
        print(f"  Profile ID: {profile_id}")
        print(f"  Query: {job_part_data['part_data']['keyword']} in {job_part_data['part_data']['postcode']}")
        
        # Test the scraper
        success, result = scraper.process_job_part(job_part_data)
        
        if success:
            print("✅ Scraper test successful!")
            print(f"📊 Records found: {result.get('records_found', 0)}")
        else:
            print("❌ Scraper test failed!")
            print(f"Error: {result.get('error', 'Unknown error')}")
        
        return success
        
    except Exception as e:
        print(f"❌ Scraper debug failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main debug function"""
    print("🐛 Card Scraper Debug Tool")
    print("=" * 50)
    
    # Run all debug checks
    checks = [
        ("Environment", debug_environment),
        ("Config", debug_config),
        ("Imports", debug_imports),
        ("Database", debug_database),
        ("Browser", debug_browser),
        ("Scraper", debug_scraper)
    ]
    
    results = {}
    for name, check_func in checks:
        try:
            results[name] = check_func()
        except Exception as e:
            print(f"❌ {name} debug crashed: {e}")
            results[name] = False
    
    # Summary
    print("\n📊 Debug Summary")
    print("=" * 30)
    for name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {name}: {status}")
    
    # Recommendations
    print("\n💡 Recommendations:")
    if not results.get("Environment"):
        print("  - Check your .env file configuration")
    if not results.get("Config"):
        print("  - Check config.py HEADLESS_MODE setting")
    if not results.get("Database"):
        print("  - Verify your Supabase credentials")
        print("  - Check if your database tables exist")
    if not results.get("Browser"):
        print("  - Install Chrome/Chromium browser")
        print("  - Check if ChromeDriver is accessible")
    
    # Show current headless setting
    try:
        from config import HEADLESS_MODE
        print(f"\n🎛️  Current Settings:")
        print(f"  HEADLESS_MODE (config.py): {HEADLESS_MODE}")
        if HEADLESS_MODE:
            print("  ⚠️  Browser will run in HEADLESS mode (invisible)")
            print("  💡 To see browser, edit config.py and set HEADLESS_MODE = False")
        else:
            print("  ✅ Browser will run in VISIBLE mode")
    except:
        pass

if __name__ == "__main__":
    main()
