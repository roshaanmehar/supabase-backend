import threading
import time
import logging
from typing import Dict, List
from queue_manager import queue_manager
from database import db_client
from scrapers import SCRAPERS
from config import MAX_CONCURRENT_WORKERS, MAX_RETRIES, RETRY_DELAYS, GOOGLE_MAPS_QUEUE_NAME, EMAIL_QUEUE_NAME

logger = logging.getLogger(__name__)

class WorkerSystem:
    def __init__(self):
        self.workers = []
        self.running = False
        self.worker_stats = {
            "active_workers": 0,
            "total_processed": 0,
            "total_failed": 0
        }
        self.lock = threading.Lock()
    
    def start_workers(self):
        """Start the worker threads"""
        if self.running:
            logger.warning("Workers already running")
            return
        
        self.running = True
        logger.info(f"Starting {MAX_CONCURRENT_WORKERS} workers")
        
        for i in range(MAX_CONCURRENT_WORKERS):
            worker = threading.Thread(target=self._worker_loop, args=(i,), daemon=True)
            worker.start()
            self.workers.append(worker)
    
    def stop_workers(self):
        """Stop all worker threads"""
        self.running = False
        logger.info("Stopping all workers")
    
    def _worker_loop(self, worker_id: int):
        """Main worker loop that processes jobs from queues"""
        logger.info(f"Worker {worker_id} started")
        
        queues = [GOOGLE_MAPS_QUEUE_NAME, EMAIL_QUEUE_NAME]
        current_queue_index = 0
        
        while self.running:
            try:
                with self.lock:
                    self.worker_stats["active_workers"] = sum(1 for w in self.workers if w.is_alive())
                
                # Fair distribution: alternate between queues
                queue_name = queues[current_queue_index]
                current_queue_index = (current_queue_index + 1) % len(queues)
                
                # Get next job part
                job_part = queue_manager.get_next_job_part(queue_name)
                
                if job_part is None:
                    # No jobs in current queue, sleep briefly
                    time.sleep(1)
                    continue
                
                # Process the job part
                self._process_job_part(worker_id, job_part, queue_name)
                
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                time.sleep(5)  # Sleep on error to prevent tight loop
        
        logger.info(f"Worker {worker_id} stopped")
    
    def _process_job_part(self, worker_id: int, job_part: Dict, queue_name: str):
        """Process a single job part"""
        part_id = job_part["part_data"]["part_id"]
        scraper_engine = job_part["scraper_engine"]
        
        logger.info(f"Worker {worker_id} processing part {part_id} with {scraper_engine}")
        
        try:
            # Update part status to 'ongoing'
            db_client.update_job_part_status(part_id, "ongoing")
            
            # Get the appropriate scraper function
            scraper_func = SCRAPERS.get(scraper_engine)
            if not scraper_func:
                logger.error(f"No scraper found for engine: {scraper_engine}")
                self._handle_job_failure(job_part, queue_name, "No scraper available")
                return
            
            # Add profile_id to part_data for scraper
            job_part["part_data"]["profile_id"] = job_part["profile_id"]
            
            # Run the scraper
            result = scraper_func(job_part["part_data"])
            
            if result["success"]:
                # Scraping successful
                logger.info(f"Worker {worker_id} completed part {part_id}")
                
                # Insert scraped data if available
                if result.get("data") and isinstance(result["data"], list):
                    for item in result["data"]:
                        db_client.insert_scraped_data(item)
                
                # Update part status to 'done'
                db_client.update_job_part_status(part_id, "done")
                
                # Check if job is complete
                if db_client.check_job_completion(job_part["job_id"]):
                    db_client.update_job_status(job_part["job_id"], "done")
                    logger.info(f"Job {job_part['job_id']} completed")
                
                with self.lock:
                    self.worker_stats["total_processed"] += 1
            else:
                # Scraping failed
                self._handle_job_failure(job_part, queue_name, result.get("message", "Unknown error"))
                
        except Exception as e:
            logger.error(f"Worker {worker_id} exception processing part {part_id}: {e}")
            self._handle_job_failure(job_part, queue_name, str(e))
    
    def _handle_job_failure(self, job_part: Dict, queue_name: str, error_message: str):
        """Handle job part failure with retry logic"""
        part_id = job_part["part_data"]["part_id"]
        retry_count = job_part.get("retry_count", 0)
        
        if retry_count < MAX_RETRIES:
            # Retry the job
            delay = RETRY_DELAYS[min(retry_count, len(RETRY_DELAYS) - 1)]
            logger.info(f"Retrying part {part_id} in {delay} seconds (attempt {retry_count + 1})")
            
            # Add delay (in a real system, you'd use a delayed queue)
            time.sleep(delay)
            queue_manager.add_failed_job_for_retry(queue_name, job_part, delay)
        else:
            # Max retries reached, mark as failed
            logger.error(f"Part {part_id} failed permanently after {MAX_RETRIES} attempts: {error_message}")
            db_client.update_job_part_status(part_id, "failed")
            
            # Check if job should be marked as failed
            if db_client.check_job_completion(job_part["job_id"]):
                db_client.update_job_status(job_part["job_id"], "done")  # Still mark as done even with some failures
            
            with self.lock:
                self.worker_stats["total_failed"] += 1
    
    def get_stats(self) -> Dict:
        """Get worker statistics"""
        with self.lock:
            return self.worker_stats.copy()

# Global worker system instance
worker_system = WorkerSystem()
