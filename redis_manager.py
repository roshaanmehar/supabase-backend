import redis
import json
import logging
from typing import Dict, List, Optional
from config import REDIS_HOST, REDIS_PORT, REDIS_DB, GOOGLE_MAPS_QUEUE_NAME, EMAIL_QUEUE_NAME

logger = logging.getLogger(__name__)

class RedisManager:
    def __init__(self):
        self.client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
        self.queues = {
            'google_maps': GOOGLE_MAPS_QUEUE_NAME,
            'email': EMAIL_QUEUE_NAME
        }
    
    def add_job_to_queue(self, job_data: Dict) -> bool:
        """Add a job with its parts to the appropriate queue"""
        try:
            scraper_engine = job_data.get('scraper_engine', 'google_maps')
            queue_name = self.queues.get(scraper_engine, GOOGLE_MAPS_QUEUE_NAME)
            
            # Add each job part to the queue with job metadata
            for part in job_data['job_parts']:
                queue_item = {
                    'job_id': job_data['job_id'],
                    'profile_id': job_data['profile_id'],
                    'scraper_engine': scraper_engine,
                    'part_data': part,
                    'created_at': job_data['created_at'],
                    'retry_count': 0
                }
                
                self.client.lpush(queue_name, json.dumps(queue_item))
            
            logger.info(f"Added {len(job_data['job_parts'])} parts to {queue_name} queue")
            return True
        except Exception as e:
            logger.error(f"Failed to add job to queue: {e}")
            return False
    
    def get_next_job_part(self, queue_name: str, timeout: int = 1) -> Optional[Dict]:
        """Get the next job part from queue (blocking pop)"""
        try:
            result = self.client.brpop(queue_name, timeout=timeout)
            if result:
                _, job_data = result
                return json.loads(job_data)
            return None
        except Exception as e:
            logger.error(f"Failed to get job from {queue_name}: {e}")
            return None
    
    def add_failed_job_for_retry(self, job_part: Dict, delay: int) -> bool:
        """Add a failed job part back to queue for retry after delay"""
        try:
            job_part['retry_count'] += 1
            scraper_engine = job_part['scraper_engine']
            queue_name = self.queues.get(scraper_engine, GOOGLE_MAPS_QUEUE_NAME)
            
            # For simplicity, we'll add it back to the front of the queue
            # In production, you might want to use Redis delayed queues
            self.client.rpush(queue_name, json.dumps(job_part))
            
            logger.info(f"Added job part for retry (attempt {job_part['retry_count']})")
            return True
        except Exception as e:
            logger.error(f"Failed to add job for retry: {e}")
            return False
    
    def get_queue_lengths(self) -> Dict[str, int]:
        """Get the length of all queues"""
        try:
            lengths = {}
            for engine, queue_name in self.queues.items():
                lengths[engine] = self.client.llen(queue_name)
            return lengths
        except Exception as e:
            logger.error(f"Failed to get queue lengths: {e}")
            return {}
    
    def clear_all_queues(self) -> bool:
        """Clear all queues (for testing)"""
        try:
            for queue_name in self.queues.values():
                self.client.delete(queue_name)
            logger.info("Cleared all queues")
            return True
        except Exception as e:
            logger.error(f"Failed to clear queues: {e}")
            return False

# Global instance
redis_manager = RedisManager()
