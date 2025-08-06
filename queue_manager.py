import redis
import json
import logging
from typing import Dict, List, Optional
from config import REDIS_HOST, REDIS_PORT, REDIS_DB, GOOGLE_MAPS_QUEUE_NAME, EMAIL_QUEUE_NAME

logger = logging.getLogger(__name__)

class QueueManager:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
        self.queues = {
            "google_maps": GOOGLE_MAPS_QUEUE_NAME,
            "manta": EMAIL_QUEUE_NAME
        }
    
    def add_job_to_queue(self, job_data: Dict) -> bool:
        """Add a job with its parts to the appropriate queue"""
        try:
            scraper_engine = job_data.get("scraper_engine")
            queue_name = self.queues.get(scraper_engine)
            
            if not queue_name:
                logger.error(f"Unknown scraper engine: {scraper_engine}")
                return False
            
            # Add each job part to the queue with job metadata
            for part in job_data.get("job_parts", []):
                queue_item = {
                    "job_id": job_data["job_id"],
                    "profile_id": job_data["profile_id"],
                    "scraper_engine": scraper_engine,
                    "created_at": job_data["created_at"],
                    "part_data": part,
                    "retry_count": 0
                }
                
                # Use LPUSH to add to the left (FIFO with RPOP)
                self.redis_client.lpush(queue_name, json.dumps(queue_item))
            
            logger.info(f"Added {len(job_data.get('job_parts', []))} parts to {queue_name} queue")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add job to queue: {e}")
            return False
    
    def get_next_job_part(self, queue_name: str) -> Optional[Dict]:
        """Get the next job part from the specified queue"""
        try:
            # Use RPOP to get from the right (FIFO)
            item = self.redis_client.rpop(queue_name)
            if item:
                return json.loads(item)
            return None
        except Exception as e:
            logger.error(f"Failed to get job part from {queue_name}: {e}")
            return None
    
    def add_failed_job_for_retry(self, queue_name: str, job_part: Dict, delay: int) -> bool:
        """Add a failed job part back to queue for retry after delay"""
        try:
            job_part["retry_count"] += 1
            
            # For simplicity, we'll just add it back to the queue
            # In production, you might want to use Redis delayed queues
            self.redis_client.lpush(queue_name, json.dumps(job_part))
            
            logger.info(f"Added job part {job_part['part_data']['part_id']} for retry #{job_part['retry_count']}")
            return True
        except Exception as e:
            logger.error(f"Failed to add job for retry: {e}")
            return False
    
    def get_queue_lengths(self) -> Dict[str, int]:
        """Get the length of all queues"""
        try:
            return {
                name: self.redis_client.llen(queue_name)
                for name, queue_name in self.queues.items()
            }
        except Exception as e:
            logger.error(f"Failed to get queue lengths: {e}")
            return {}
    
    def clear_all_queues(self) -> bool:
        """Clear all queues (useful for testing)"""
        try:
            for queue_name in self.queues.values():
                self.redis_client.delete(queue_name)
            logger.info("Cleared all queues")
            return True
        except Exception as e:
            logger.error(f"Failed to clear queues: {e}")
            return False

# Global queue manager instance
queue_manager = QueueManager()
