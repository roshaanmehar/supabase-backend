from flask import Flask, request, jsonify
import logging
from typing import Dict
from config import *
from redis_manager import redis_manager
from database import db
from worker import WorkerManager

# Configure logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global worker manager
worker_manager = None

@app.route('/api/jobs/submit', methods=['POST'])
def submit_job():
    """Receive job from edge function and add to queue"""
    try:
        job_data = request.get_json()
        
        # Validate required fields
        required_fields = ['job_id', 'profile_id', 'scraper_engine', 'job_parts']
        for field in required_fields:
            if field not in job_data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Add job to Redis queue
        success = redis_manager.add_job_to_queue(job_data)
        
        if success:
            logger.info(f"Job {job_data['job_id']} submitted successfully with {len(job_data['job_parts'])} parts")
            return jsonify({
                'success': True,
                'message': f"Job submitted with {len(job_data['job_parts'])} parts",
                'job_id': job_data['job_id']
            })
        else:
            return jsonify({'error': 'Failed to submit job to queue'}), 500
            
    except Exception as e:
        logger.error(f"Error submitting job: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/jobs/status/<job_id>', methods=['GET'])
def get_job_status(job_id: str):
    """Get status of a specific job"""
    try:
        stats = db.get_job_stats(job_id)
        if not stats:
            return jsonify({'error': 'Job not found'}), 404
        
        return jsonify({
            'job_id': job_id,
            'stats': stats
        })
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/workers/status', methods=['GET'])
def get_workers_status():
    """Get status of all workers"""
    try:
        if not worker_manager:
            return jsonify({'error': 'Workers not initialized'}), 500
        
        workers_status = worker_manager.get_workers_status()
        summary_stats = worker_manager.get_summary_stats()
        
        return jsonify({
            'summary': summary_stats,
            'workers': workers_status
        })
    except Exception as e:
        logger.error(f"Error getting workers status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/queues/status', methods=['GET'])
def get_queues_status():
    """Get status of all queues"""
    try:
        queue_lengths = redis_manager.get_queue_lengths()
        return jsonify({
            'queues': queue_lengths
        })
    except Exception as e:
        logger.error(f"Error getting queue status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/system/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Test Redis connection
        redis_manager.client.ping()
        
        # Test Supabase connection (simple query)
        db.client.table('profiles').select('id').limit(1).execute()
        
        return jsonify({
            'status': 'healthy',
            'redis': 'connected',
            'database': 'connected'
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500

@app.route('/api/system/clear-queues', methods=['POST'])
def clear_queues():
    """Clear all queues (for testing)"""
    try:
        success = redis_manager.clear_all_queues()
        if success:
            return jsonify({'message': 'All queues cleared'})
        else:
            return jsonify({'error': 'Failed to clear queues'}), 500
    except Exception as e:
        logger.error(f"Error clearing queues: {e}")
        return jsonify({'error': str(e)}), 500

def initialize_workers():
    """Initialize and start worker threads"""
    global worker_manager
    worker_manager = WorkerManager(MAX_CONCURRENT_WORKERS)
    worker_manager.start_workers()

if __name__ == '__main__':
    logger.info("Starting Flask Redis Job Queue Server")
    
    # Initialize workers
    initialize_workers()
    
    try:
        app.run(host=FLASK_HOST, port=FLASK_PORT, debug=DEBUG)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        if worker_manager:
            worker_manager.stop_workers()
