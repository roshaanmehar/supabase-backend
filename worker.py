import threading
import time
import logging
from typing import Dict, List
from redis_manager import redis_manager
from database import db
from scrapers import get_scraper
from config import MAX_RETRIES, RETRY_DELAYS, GOOGLE_MAPS_QUEUE_NAME, EMAIL_QUEUE_NAME

logger = logging.getLogger(__name__)

class JobWorker:
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.is_running = False
        self.current_job = None
        self.processed_count = 0
        self.failed_count = 0
    
    def start(self):
        """Start the worker thread"""
        self.is_running = True
        self.thread = threading.Thread(target=self._work_loop, daemon=True)
        self.thread.start()
        logger.info(f"Worker {self.worker_id} started")
    
    def stop(self):
        """Stop the worker thread"""
        self.is_running = False
        logger.info(f"Worker {self.worker_id} stopping")
    
    def _work_loop(self):
        """Main work loop for the worker"""
        queues = [GOOGLE_MAPS_QUEUE_NAME, EMAIL_QUEUE_NAME]
        
        while self.is_running:
            job_part = None
            
            # Try to get work from any queue (round-robin)
            for queue_name in queues:
                job_part = redis_manager.get_next_job_part(queue_name, timeout=1)
                if job_part:
                    break
            
            if job_part:
                self._process_job_part(job_part)
            else:
                # No work available, sleep briefly
                time.sleep(0.1)
    
    def _process_job_part(self, job_part: Dict):
        """Process a single job part"""
        self.current_job = job_part
        part_id = job_part['part_data']['part_id']
        job_id = job_part['job_id']
        scraper_engine = job_part['scraper_engine']
        
        logger.info(f"Worker {self.worker_id} processing part {part_id}")
        
        try:
            # Update part status to ongoing
            db.update_job_part_status(part_id, 'ongoing')
            
            # Get the appropriate scraper
            scraper = get_scraper(scraper_engine)
            
            # Perform the scraping
            result = scraper.scrape(job_part['part_data'])
            
            # Mark as completed
            db.update_job_part_status(part_id, 'done')
            self.processed_count += 1
            
            # Check if the entire job is completed
            db.check_job_completion(job_id)
            
            logger.info(f"Worker {self.worker_id} completed part {part_id}")
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id} failed to process part {part_id}: {e}")
            self._handle_job_failure(job_part, str(e))
        
        finally:
            self.current_job = None
    
    def _handle_job_failure(self, job_part: Dict, error_msg: str):
        """Handle job part failure with retry logic"""
        part_id = job_part['part_data']['part_id']
        retry_count = job_part.get('retry_count', 0)
        
        if retry_count < MAX_RETRIES:
            # Schedule for retry
            delay = RETRY_DELAYS[min(retry_count, len(RETRY_DELAYS) - 1)]
            logger.info(f"Scheduling part {part_id} for retry in {delay} seconds (attempt {retry_count + 1})")
            
            # Add back to queue for retry (in a real system, you'd use delayed queues)
            time.sleep(delay)  # Simple delay - in production use proper delayed queues
            redis_manager.add_failed_job_for_retry(job_part, delay)
            
        else:
            # Max retries reached, mark as failed
            logger.error(f"Part {part_id} failed permanently after {MAX_RETRIES} attempts")
            db.update_job_part_status(part_id, 'failed')
            self.failed_count += 1
            
            # Still check if job is completed (other parts might be done)
            db.check_job_completion(job_part['job_id'])
    
    def get_status(self) -> Dict:
        """Get worker status"""
        return {
            'worker_id': self.worker_id,
            'is_running': self.is_running,
            'processed_count': self.processed_count,
            'failed_count': self.failed_count,
            'current_job': self.current_job['part_data']['part_id'] if self.current_job else None
        }

class WorkerManager:
    def __init__(self, num_workers: int):
        self.num_workers = num_workers
        self.workers: List[JobWorker] = []
    
    def start_workers(self):
        """Start all workers"""
        for i in range(self.num_workers):
            worker = JobWorker(i + 1)
            worker.start()
            self.workers.append(worker)
        
        logger.info(f"Started {self.num_workers} workers")
    
    def stop_workers(self):
        """Stop all workers"""
        for worker in self.workers:
            worker.stop()
        
        logger.info("All workers stopped")
    
    def get_workers_status(self) -> List[Dict]:
        """Get status of all workers"""
        return [worker.get_status() for worker in self.workers]
    
    def get_summary_stats(self) -> Dict:
        """Get summary statistics"""
        total_processed = sum(w.processed_count for w in self.workers)
        total_failed = sum(w.failed_count for w in self.workers)
        active_workers = sum(1 for w in self.workers if w.current_job is not None)
        
        return {
            'total_workers': len(self.workers),
            'active_workers': active_workers,
            'total_processed': total_processed,
            'total_failed': total_failed
        }

# Global worker manager instance
worker_manager = None
