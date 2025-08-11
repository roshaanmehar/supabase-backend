"""
supabase_adapter.py - Supabase database operations
-------------------------------------------------
Handles all database operations for the scraper using Supabase.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from supabase import create_client, Client

class SupabaseAdapter:
    """
    Adapter for Supabase database operations.
    """
    
    def __init__(self, supabase_url: str, supabase_key: str):
        self.client: Client = create_client(supabase_url, supabase_key)
        self.log = logging.getLogger("supabase_adapter")
    
    def insert_scraped_data(self, records: List[Dict[str, Any]], user_id: str = None, scraped_job_part_id: str = None) -> bool:
        """
        Insert scraped business data into the scraped_data table.
        Handles unique phone number constraints gracefully.
        
        Args:
            records: List of business records to insert
            user_id: User ID (profile_id from the job)
            scraped_job_part_id: ID of the scrape job part
        
        Returns:
            True if at least some records were inserted successfully, False if all failed
        """
        if not records:
            return True
        
        successful_inserts = 0
        duplicate_phone_skips = 0
        other_errors = 0
        
        # Prepare records for insertion
        prepared_records = []
        for record in records:
            prepared_record = {
                "source": "google_maps",
                "keyword": record.get("keyword", ""),
                "name": record.get("businessname", ""),
                "phone_no": record.get("phonenumber") if record.get("phonenumber") else None,
                "address": record.get("address", "N/A"),
                "city": record.get("city", ""),
                "postcode": record.get("postcode", ""),
                "website": record.get("website", "N/A"),
                "stars": self._parse_stars(record.get("stars", "N/A")),
                "number_of_reviews": record.get("numberofreviews", 0),
                "longitude": record.get("longitude"),
                "latitude": record.get("latitude"),
                "email_scraper_run": False,
                "email_found": False,
                "scraped_at": datetime.now().isoformat(),
                "in_campaign": False,
                "visit_successful": False,
                "user_id": user_id,  # Add user_id from profile_id
                "scraped_job_part_id": scraped_job_part_id  # Correct column name
            }
            prepared_records.append(prepared_record)
        
        # Try to insert records one by one to handle unique constraint violations
        for record in prepared_records:
            try:
                result = self.client.table('scraped_data').insert(record).execute()
                successful_inserts += 1
                self.log.debug(f"Successfully inserted record: {record.get('name', 'Unknown')} (phone: {record.get('phone_no', 'None')})")
                
            except Exception as e:
                error_message = str(e).lower()
                
                # Check if it's a unique constraint violation on phone_no
                if 'unique' in error_message and ('phone_no' in error_message or 'phone' in error_message):
                    duplicate_phone_skips += 1
                    self.log.debug(f"Skipped duplicate phone number: {record.get('phone_no', 'None')} for {record.get('name', 'Unknown')}")
                else:
                    other_errors += 1
                    self.log.error(f"Error inserting record {record.get('name', 'Unknown')}: {e}")
        
        # Log summary
        total_records = len(prepared_records)
        self.log.info(f"Insert summary: {successful_inserts} successful, {duplicate_phone_skips} duplicate phones skipped, {other_errors} other errors out of {total_records} total records")
        
        # Consider it successful if we inserted at least some records or only had duplicate phone issues
        if successful_inserts > 0 or (duplicate_phone_skips > 0 and other_errors == 0):
            return True
        else:
            return False
    
    def update_job_part_status(self, part_id: str, status: str) -> bool:
        """
        Update the status of a scrape job part.
        
        Args:
            part_id: ID of the job part
            status: New status ('ongoing', 'done', 'failed')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            result = self.client.table('scrape_job_parts').update({
                'status': status
            }).eq('id', part_id).execute()
            
            self.log.info(f"Updated job part {part_id} status to {status}")
            return True
        except Exception as e:
            self.log.error(f"Failed to update job part {part_id} status: {e}")
            return False
    
    def update_job_status(self, job_id: str, status: str) -> bool:
        """
        Update the status of a scrape job.
        
        Args:
            job_id: ID of the job
            status: New status ('ongoing', 'done', 'failed')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            result = self.client.table('scrape_jobs').update({
                'status': status
            }).eq('id', job_id).execute()
            
            self.log.info(f"Updated job {job_id} status to {status}")
            return True
        except Exception as e:
            self.log.error(f"Failed to update job {job_id} status: {e}")
            return False
    
    def check_job_completion(self, job_id: str) -> bool:
        """
        Check if all parts of a job are completed and update job status if so.
        
        Args:
            job_id: ID of the job to check
            
        Returns:
            True if job is completed, False otherwise
        """
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
                self.log.info(f"Job {job_id} completed - all parts finished")
                return True
            
            return False
        except Exception as e:
            self.log.error(f"Failed to check job completion for {job_id}: {e}")
            return False
    
    def get_job_stats(self, job_id: str) -> Dict:
        """
        Get statistics for a job.
        
        Args:
            job_id: ID of the job
            
        Returns:
            Dictionary with job statistics
        """
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
            self.log.error(f"Failed to get job stats for {job_id}: {e}")
            return {}
    
    def _parse_stars(self, stars_str: str) -> Optional[float]:
        """Parse stars string to float."""
        if not stars_str or stars_str == "N/A":
            return None
        
        try:
            # Extract numeric value from string like "4.5" or "4.5 stars"
            import re
            match = re.search(r'(\d+\.?\d*)', str(stars_str))
            if match:
                return float(match.group(1))
        except (ValueError, AttributeError):
            pass
        
        return None
