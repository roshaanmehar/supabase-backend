import requests
import json
import time
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:5000"

def test_job_submission():
    """Test job submission endpoint"""
    print("Testing job submission...")
    
    test_job = {
        "job_id": "550e8400-e29b-41d4-a716-446655440001",
        "profile_id": "550e8400-e29b-41d4-a716-446655440000",
        "scraper_engine": "google_maps",
        "created_at": datetime.now().isoformat(),
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
        ]
    }
    
    response = requests.post(f"{BASE_URL}/api/jobs/submit", json=test_job)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    print()

def test_system_status():
    """Test system status endpoints"""
    print("Testing system status...")
    
    # Test worker status
    response = requests.get(f"{BASE_URL}/api/workers/status")
    print(f"Worker Status: {response.json()}")
    
    # Test queue status
    response = requests.get(f"{BASE_URL}/api/queues/status")
    print(f"Queue Status: {response.json()}")
    
    # Test health check
    response = requests.get(f"{BASE_URL}/health")
    print(f"Health Check: {response.json()}")
    print()

def test_multiple_jobs():
    """Test multiple job submissions for fair distribution"""
    print("Testing multiple jobs for fair distribution...")
    
    # Customer A job (15 parts)
    job_a = {
        "job_id": "customer-a-job",
        "profile_id": "customer-a",
        "scraper_engine": "google_maps",
        "created_at": datetime.now().isoformat(),
        "job_parts": [
            {
                "part_id": f"customer-a-part-{i}",
                "postcode": f"1000{i}",
                "keyword": "restaurants",
                "city": "New York",
                "state": "NY",
                "country": "USA"
            }
            for i in range(15)
        ]
    }
    
    # Customer B job (7 parts)
    job_b = {
        "job_id": "customer-b-job",
        "profile_id": "customer-b",
        "scraper_engine": "google_maps",
        "created_at": (datetime.now()).isoformat(),
        "job_parts": [
            {
                "part_id": f"customer-b-part-{i}",
                "postcode": f"2000{i}",
                "keyword": "cafes",
                "city": "Los Angeles",
                "state": "CA",
                "country": "USA"
            }
            for i in range(7)
        ]
    }
    
    # Submit both jobs
    print("Submitting Customer A job (15 parts)...")
    response_a = requests.post(f"{BASE_URL}/api/jobs/submit", json=job_a)
    print(f"Response A: {response_a.json()}")
    
    print("Submitting Customer B job (7 parts)...")
    response_b = requests.post(f"{BASE_URL}/api/jobs/submit", json=job_b)
    print(f"Response B: {response_b.json()}")
    
    # Monitor queue status
    for i in range(10):
        time.sleep(2)
        response = requests.get(f"{BASE_URL}/api/queues/status")
        queue_status = response.json()
        print(f"Queue status after {(i+1)*2}s: {queue_status}")
        
        if queue_status.get("total_pending", 0) == 0:
            print("All jobs completed!")
            break

def clear_queues():
    """Clear all queues"""
    print("Clearing all queues...")
    response = requests.post(f"{BASE_URL}/api/system/clear-queues")
    print(f"Response: {response.json()}")
    print()

if __name__ == "__main__":
    print("=== Flask Redis Job Queue System Test ===\n")
    
    # Clear queues first
    clear_queues()
    
    # Test basic functionality
    test_job_submission()
    test_system_status()
    
    # Wait a bit for processing
    print("Waiting 10 seconds for processing...")
    time.sleep(10)
    test_system_status()
    
    # Test fair distribution
    clear_queues()
    test_multiple_jobs()
