"""
start_system.py - Start the complete Redis job system
----------------------------------------------------
Starts Redis server and Flask app with proper error handling.
"""
import os
import sys
import subprocess
import time
import signal
from pathlib import Path

class SystemManager:
    def __init__(self):
        self.redis_process = None
        self.flask_process = None
    
    def check_redis_installed(self):
        """Check if Redis is installed"""
        try:
            subprocess.run(['redis-server', '--version'], 
                         capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    
    def start_redis(self):
        """Start Redis server"""
        if not self.check_redis_installed():
            print("❌ Redis not installed!")
            print("Install Redis:")
            print("  Windows: Download from https://redis.io/download")
            print("  macOS: brew install redis")
            print("  Linux: sudo apt-get install redis-server")
            return False
        
        print("🚀 Starting Redis server...")
        try:
            # Start Redis in background
            self.redis_process = subprocess.Popen(
                ['redis-server', '--daemonize', 'yes'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            time.sleep(2)  # Give Redis time to start
            
            # Test Redis connection
            result = subprocess.run(['redis-cli', 'ping'], 
                                  capture_output=True, text=True)
            if result.stdout.strip() == 'PONG':
                print("✅ Redis server started successfully")
                return True
            else:
                print("❌ Redis server failed to start")
                return False
        except Exception as e:
            print(f"❌ Failed to start Redis: {e}")
            return False
    
    def start_flask(self):
        """Start Flask application"""
        print("🚀 Starting Flask application...")
        try:
            # Check if app.py exists
            if not Path('app.py').exists():
                print("❌ app.py not found!")
                return False
            
            # Start Flask app
            self.flask_process = subprocess.Popen(
                [sys.executable, 'app.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Wait a bit and check if it's running
            time.sleep(3)
            if self.flask_process.poll() is None:
                print("✅ Flask application started successfully")
                print("🌐 Server running at http://localhost:5000")
                return True
            else:
                print("❌ Flask application failed to start")
                # Print any error output
                output, _ = self.flask_process.communicate()
                if output:
                    print(f"Error output: {output}")
                return False
        except Exception as e:
            print(f"❌ Failed to start Flask: {e}")
            return False
    
    def stop_services(self):
        """Stop all services"""
        print("\n🛑 Stopping services...")
        
        # Stop Flask
        if self.flask_process and self.flask_process.poll() is None:
            self.flask_process.terminate()
            try:
                self.flask_process.wait(timeout=5)
                print("✅ Flask stopped")
            except subprocess.TimeoutExpired:
                self.flask_process.kill()
                print("🔪 Flask force killed")
        
        # Stop Redis
        try:
            subprocess.run(['redis-cli', 'shutdown'], 
                         capture_output=True, check=True)
            print("✅ Redis stopped")
        except subprocess.CalledProcessError:
            print("⚠️  Redis may already be stopped")
    
    def run(self):
        """Run the complete system"""
        print("🚀 Starting Complete Redis Job System")
        print("=" * 50)
        
        # Check environment
        if not Path('.env').exists():
            print("❌ .env file not found!")
            print("Create .env with your Supabase credentials")
            return False
        
        try:
            # Start Redis
            if not self.start_redis():
                return False
            
            # Start Flask
            if not self.start_flask():
                self.stop_services()
                return False
            
            print("\n✅ System started successfully!")
            print("\nAvailable endpoints:")
            print("  Health check: http://localhost:5000/api/system/health")
            print("  Submit job: http://localhost:5000/api/jobs/submit")
            print("  Worker status: http://localhost:5000/api/workers/status")
            print("\nPress Ctrl+C to stop the system")
            
            # Keep running until interrupted
            try:
                while True:
                    time.sleep(1)
                    # Check if Flask is still running
                    if self.flask_process.poll() is not None:
                        print("❌ Flask process died")
                        break
            except KeyboardInterrupt:
                print("\n🛑 Shutdown requested")
            
        except Exception as e:
            print(f"❌ System error: {e}")
        finally:
            self.stop_services()
        
        return True

def main():
    """Main function"""
    manager = SystemManager()
    
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print("\n🛑 Interrupt received")
        manager.stop_services()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Run the system
    manager.run()

if __name__ == "__main__":
    main()
