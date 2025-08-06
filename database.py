from supabase import create_client, Client
from typing import Dict, List, Optional
import logging
from config import SUPABASE_URL, SUPABASE_KEY

logger = logging.getLogger(__name__)

class SupabaseManager:
    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    def update_job_part_status(self, part_id: str, status: str) -> bool:
        """Update the status of a scrape job part"""
        try:
            result = self.client.table('scrape_job_parts').update({
                'status': status
            }).eq('id', part_id).execute()
            
            logger.info(f"Updated part {part_id} status to {status}")
            return True
        except Exception as e:
            logger.error(f"Failed to update part {part_id} status: {e}")
            return False
    
    def update_job_status(self, job_id: str, status: str) -> bool:
        """Update the status of a scrape job"""
        try:
            result = self.client.table('scrape_jobs').update({
                'status': status
            }).eq('id', job_id).execute()
            
            logger.info(f"Updated job {job_id} status to {status}")
            return True
        except Exception as e:
            logger.error(f"Failed to update job {job_id} status: {e}")
            return False
    
    def check_job_completion(self, job_id: str) -> bool:
        """Check if all parts of a job are completed"""
        try:
            # Get all parts for this job
            result = self.client.table('scrape_job_parts').select('status').eq('job_id', job_id).execute()
            
            if not result.data:
                return False
            
            # Check if all parts are either 'done' or 'failed'
            completed_statuses = ['done', 'failed']
            all_completed = all(part['status'] in completed_statuses for part in result.data)
            
            if all_completed:
                # Update job status to done
                self.update_job_status(job_id, 'done')
                logger.info(f"Job {job_id} completed - all parts finished")
                return True
            
            return False
        except Exception as e:
            logger.error(f"Failed to check job completion for {job_id}: {e}")
            return False
    
    def get_job_stats(self, job_id: str) -> Dict:
        """Get statistics for a job"""
        try:
            result = self.client.table('scrape_job_parts').select('status').eq('job_id', job_id).execute()
            
            if not result.data:
                return {}
            
            stats = {
                'total': len(result.data),
                'undone': 0,
                'ongoing': 0,
                'done': 0,
                'failed': 0,
                'locked': 0
            }
            
            for part in result.data:
                status = part['status']
                if status in stats:
                    stats[status] += 1
            
            return stats
        except Exception as e:
            logger.error(f"Failed to get job stats for {job_id}: {e}")
            return {}

# Global instance
db = SupabaseManager()
