import requests
import json
import time
import uuid
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:5000"

def test_job_submission():
    """Test submitting a job to the Redis server with proper UUIDs"""
    
    # Generate proper UUIDs
    job_id = str(uuid.uuid4())
    profile_id = str(uuid.uuid4())  # This should be a real profile_id from your database
    
    # Sample job data with proper UUIDs
    job_data = {
        "job_id": job_id,
        "profile_id": profile_id,  # Real profile UUID
        "scraper_engine": "google_maps",
        "job_parts": [
            {
                "part_id": str(uuid.uuid4()),  # Real part UUID
                "postcode": "10001",
                "keyword": "restaurants",
                "city": "New York",
                "state": "NY",
                "country": "USA"
            },
            {
                "part_id": str(uuid.uuid4()),  # Real part UUID
                "postcode": "10002",
                "keyword": "restaurants",
                "city": "New York",
                "state": "NY",
                "country": "USA"
            },
            {
                "part_id": str(uuid.uuid4()),  # Real part UUID
                "postcode": "10003",
                "keyword": "restaurants",
                "city": "New York",
                "state": "NY",
                "country": "USA"
            }
        ],
        "created_at": datetime.now().isoformat()
    }
    
    print("Testing job submission with proper UUIDs...")
    print(f"Job ID: {job_id}")
    print(f"Profile ID: {profile_id}")
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
