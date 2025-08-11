from flask import Flask, request, jsonify
import logging
import subprocess
import signal
import atexit
import psutil
import time
import sys
import os
from typing import Dict
from config import *
from redis_manager import redis_manager
from database import db
from worker import WorkerManager

# Configure logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global system components
worker_manager = None
redis_process = None

class SystemManager:
    def __init__(self):
        self.redis_process = None
        self.worker_manager = None
        self.is_shutting_down = False
    
    def check_redis_installed(self):
        """Check if Redis is installed"""
        try:
            subprocess.run(['redis-server', '--version'], 
                         capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    
    def is_redis_running(self):
        """Check if Redis is already running"""
        try:
            import redis
            client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
            client.ping()
            return True
        except:
            return False
    
    def start_redis(self):
        """Start Redis server if not already running"""
        if self.is_redis_running():
            logger.info("✅ Redis server already running")
            return True
        
        if not self.check_redis_installed():
            logger.error("❌ Redis not installed!")
            logger.error("Install Redis:")
            logger.error("  Windows: Download from https://redis.io/download")
            logger.error("  macOS: brew install redis")
            logger.error("  Linux: sudo apt-get install redis-server")
            return False
        
        logger.info("🚀 Starting Redis server...")
        try:
            # Try to start Redis as daemon first
            try:
                self.redis_process = subprocess.Popen(
                    ['redis-server', '--daemonize', 'yes', '--port', str(REDIS_PORT)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                time.sleep(2)
            except:
                # Fallback to regular mode
                self.redis_process = subprocess.Popen(
                    ['redis-server', '--port', str(REDIS_PORT)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                time.sleep(3)
            
            # Test connection
            if self.is_redis_running():
                logger.info("✅ Redis server started successfully")
                return True
            else:
                logger.error("❌ Redis server failed to start")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to start Redis: {e}")
            return False
    
    def start_workers(self):
        """Initialize and start worker threads"""
        logger.info("🔧 Starting worker threads...")
        self.worker_manager = WorkerManager(MAX_CONCURRENT_WORKERS)
        self.worker_manager.start_workers()
        logger.info(f"✅ Started {MAX_CONCURRENT_WORKERS} workers")
    
    def stop_system(self):
        """Stop all system components"""
        if self.is_shutting_down:
            return
        
        self.is_shutting_down = True
        logger.info("🛑 Shutting down system...")
        
        # Stop workers
        if self.worker_manager:
            logger.info("Stopping workers...")
            self.worker_manager.stop_workers()
        
        # Stop Redis if we started it
        if self.redis_process:
            logger.info("Stopping Redis...")
            try:
                # Try graceful shutdown first
                subprocess.run(['redis-cli', '-p', str(REDIS_PORT), 'shutdown'], 
                             capture_output=True, timeout=5)
            except:
                # Force kill if graceful shutdown fails
                try:
                    self.redis_process.terminate()
                    self.redis_process.wait(timeout=5)
                except:
                    self.redis_process.kill()
        
        logger.info("✅ System shutdown complete")

# Global system manager
system_manager = SystemManager()

# Register cleanup handlers
def cleanup_handler():
    system_manager.stop_system()

atexit.register(cleanup_handler)
signal.signal(signal.SIGINT, lambda s, f: cleanup_handler())
signal.signal(signal.SIGTERM, lambda s, f: cleanup_handler())

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
        if not system_manager.worker_manager:
            return jsonify({'error': 'Workers not initialized'}), 500
        
        workers_status = system_manager.worker_manager.get_workers_status()
        summary_stats = system_manager.worker_manager.get_summary_stats()
        
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
            'database': 'connected',
            'workers': len(system_manager.worker_manager.workers) if system_manager.worker_manager else 0
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

@app.route('/api/system/status', methods=['GET'])
def get_system_status():
    """Get overall system status"""
    try:
        return jsonify({
            'redis_running': system_manager.is_redis_running(),
            'workers_active': len(system_manager.worker_manager.workers) if system_manager.worker_manager else 0,
            'queue_lengths': redis_manager.get_queue_lengths(),
            'system_healthy': True
        })
    except Exception as e:
        return jsonify({
            'system_healthy': False,
            'error': str(e)
        }), 500

def main():
    """Main entry point - starts entire system"""
    logger.info("🚀 Starting Complete Flask Redis Job Queue System")
    logger.info("=" * 60)
    
    try:
        # Check environment
        if not os.path.exists('.env'):
            logger.error("❌ .env file not found!")
            logger.error("Create .env with your Supabase credentials")
            return False
        
        # Start Redis
        if not system_manager.start_redis():
            logger.error("❌ Failed to start Redis server")
            return False
        
        # Start workers
        system_manager.start_workers()
        
        # Display system info
        logger.info("=" * 60)
        logger.info("✅ System started successfully!")
        logger.info(f"🌐 Flask server: http://localhost:{FLASK_PORT}")
        logger.info(f"🔧 Workers: {MAX_CONCURRENT_WORKERS}")
        logger.info(f"📡 Redis: {REDIS_HOST}:{REDIS_PORT}")
        logger.info("")
        logger.info("Available endpoints:")
        logger.info(f"  Health check: http://localhost:{FLASK_PORT}/api/system/health")
        logger.info(f"  System status: http://localhost:{FLASK_PORT}/api/system/status")
        logger.info(f"  Submit job: http://localhost:{FLASK_PORT}/api/jobs/submit")
        logger.info(f"  Worker status: http://localhost:{FLASK_PORT}/api/workers/status")
        logger.info("")
        logger.info("Press Ctrl+C to stop the system")
        logger.info("=" * 60)
        
        # Start Flask app
        app.run(host=FLASK_HOST, port=FLASK_PORT, debug=DEBUG)
        
    except KeyboardInterrupt:
        logger.info("🛑 Shutdown requested by user")
    except Exception as e:
        logger.error(f"❌ System error: {e}")
    finally:
        cleanup_handler()

if __name__ == '__main__':
    main()
