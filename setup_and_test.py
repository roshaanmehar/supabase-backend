"""
setup_and_test.py - Setup and test the card scraper
--------------------------------------------------
Complete setup and testing without shell scripts.
"""
import os
import sys
import subprocess
from pathlib import Path

def check_requirements():
    """Check if all requirements are met"""
    print("🔍 Checking requirements...")
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ required")
        return False
    print("✅ Python version OK")
    
    # Check if .env exists
    if not Path('.env').exists():
        print("❌ .env file not found!")
        print("Please create .env file with your Supabase credentials:")
        print("SUPABASE_URL=https://your-project.supabase.co")
        print("SUPABASE_KEY=your-supabase-anon-key")
        return False
    print("✅ .env file found")
    
    # Check if virtual environment is active
    if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("⚠️  Virtual environment not detected")
        print("Consider running: python -m venv venv && source venv/bin/activate")
    else:
        print("✅ Virtual environment active")
    
    return True

def install_requirements():
    """Install Python requirements"""
    print("📦 Installing requirements...")
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
        print("✅ Requirements installed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install requirements: {e}")
        return False

def test_imports():
    """Test if all imports work"""
    print("🧪 Testing imports...")
    try:
        from scrapers.redis_integration import create_card_scraper
        from config import SUPABASE_URL, SUPABASE_KEY
        print("✅ Imports successful")
        return True
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_scraper():
    """Test the card scraper"""
    print("🎯 Testing card scraper...")
    try:
        from scrapers.redis_integration import create_card_scraper
        from datetime import datetime
        
        # Create scraper
        scraper = create_card_scraper()
        print("✅ Scraper created")
        
        # Test job part
        job_part_data = {
            'job_id': f'test-job-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
            'profile_id': 'test-profile',
            'scraper_engine': 'google_maps',
            'part_data': {
                'part_id': f'test-part-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
                'postcode': '10001',
                'keyword': 'restaurants',
                'city': 'New York',
                'state': 'NY',
                'country': 'USA'
            },
            'created_at': datetime.now().isoformat(),
            'retry_count': 0
        }
        
        print("⏳ Running scraper test (this may take a few minutes)...")
        success, result = scraper.process_job_part(job_part_data)
        
        if success:
            print("✅ Scraper test successful!")
            print(f"📊 Records found: {result.get('records_found', 0)}")
            print(f"🎴 Cards processed: {result.get('cards_processed', 0)}")
        else:
            print("❌ Scraper test failed!")
            print(f"Error: {result.get('error', 'Unknown error')}")
        
        return success
        
    except Exception as e:
        print(f"❌ Scraper test failed: {e}")
        return False

def main():
    """Main setup and test function"""
    print("🚀 Card Scraper Setup and Test")
    print("=" * 50)
    
    # Check requirements
    if not check_requirements():
        return False
    
    # Install requirements
    if not install_requirements():
        return False
    
    # Test imports
    if not test_imports():
        return False
    
    # Ask user if they want to run the scraper test
    response = input("\n🤔 Run scraper test? This will open a browser and scrape Google Maps (y/n): ").lower()
    if response == 'y':
        test_scraper()
    
    print("\n✅ Setup complete!")
    print("\nNext steps:")
    print("1. Start Redis: redis-server")
    print("2. Start Flask app: python app.py")
    print("3. Test with: python test_client.py")
    
    return True

if __name__ == "__main__":
    main()
