import requests
import json
import time
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:5000"

def test_job_submission():
    """Test submitting a job to the Redis server"""
    
    # Sample job data (similar to what edge function would send)
    job_data = {
        "job_id": "550e8400-e29b-41d4-a716-446655440001",
        "profile_id": "550e8400-e29b-41d4-a716-446655440000",
        "scraper_engine": "google_maps",
        "job_parts": [
            {
                "part_id": "550e8400-e29b-41d4-a716-446655440002",
                "postcode": "10001",
                "keyword": "restaurants",
                "city": "New York",
                "state": "NY",
                "country": "USA"
            },
            {
                "part_id": "550e8400-e29b-41d4-a716-446655440003",
                "postcode": "10002",
                "keyword": "restaurants",
                "city": "New York",
                "state": "NY",
                "country": "USA"
            },
            {
                "part_id": "550e8400-e29b-41d4-a716-446655440004",
                "postcode": "10003",
                "keyword": "restaurants",
                "city": "New York",
                "state": "NY",
                "country": "USA"
            }
        ],
        "created_at": datetime.now().isoformat()
    }
    
    print("Testing job submission...")
    response = requests.post(f"{BASE_URL}/api/jobs/submit", json=job_data)
    print(f"Response: {response.status_code} - {response.json()}")
    
    return job_data["job_id"]

def test_system_status():
    """Test various status endpoints"""
    
    print("\n=== System Health ===")
    response = requests.get(f"{BASE_URL}/api/system/health")
    print(f"Health: {response.json()}")
    
    print("\n=== Queue Status ===")
    response = requests.get(f"{BASE_URL}/api/queues/status")
    print(f"Queues: {response.json()}")
    
    print("\n=== Workers Status ===")
    response = requests.get(f"{BASE_URL}/api/workers/status")
    print(f"Workers: {response.json()}")

def test_job_status(job_id: str):
    """Test job status endpoint"""
    print(f"\n=== Job Status for {job_id} ===")
    response = requests.get(f"{BASE_URL}/api/jobs/status/{job_id}")
    print(f"Job Status: {response.json()}")

def run_full_test():
    """Run a complete test scenario"""
    print("Starting full test scenario...")
    
    # Test system health first
    test_system_status()
    
    # Submit a test job
    job_id = test_job_submission()
    
    # Monitor job progress
    for i in range(10):
        time.sleep(5)
        print(f"\n--- Check {i+1} (after {(i+1)*5} seconds) ---")
        test_job_status(job_id)
        test_system_status()
    
    print("\nTest completed!")

if __name__ == "__main__":
    run_full_test()
