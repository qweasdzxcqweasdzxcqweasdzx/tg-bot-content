#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Health check script for Telegram Content Grabber Bot
"""

import sys
import os

# Add the bot directory to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
bot_dir = os.path.join(project_root, 'bot')
sys.path.insert(0, project_root)
sys.path.insert(0, bot_dir)

def check_environment():
    """Check if the environment is properly set up"""
    print("🔍 Checking environment...")
    
    # Check Python version
    print(f"✅ Python version: {sys.version}")
    
    # Check if we're in the right directory
    print(f"✅ Working directory: {os.getcwd()}")
    
    return True

def check_dependencies():
    """Check if all required dependencies are installed"""
    print("\n🔍 Checking dependencies...")
    
    dependencies = [
        ("structlog", "Logging framework"),
        ("aiosqlite", "Async SQLite database driver"),
        ("telethon", "Telegram client library"),
        ("apscheduler", "Async task scheduler"),
        ("bs4", "BeautifulSoup for HTML parsing"),
        ("aiohttp", "Async HTTP client"),
        ("PIL", "Python Imaging Library"),
        ("psutil", "System utilities"),
        ("pytz", "Timezone handling"),
        ("dotenv", "Environment variable loader")
    ]
    
    missing = []
    for module, description in dependencies:
        try:
            __import__(module)
            print(f"✅ {module} - {description}")
        except ImportError:
            print(f"❌ {module} - {description} (MISSING)")
            missing.append(module)
    
    if missing:
        print(f"\n⚠️  Missing {len(missing)} dependencies: {', '.join(missing)}")
        print("Run install_requirements.bat to install missing dependencies")
        return False
    else:
        print("✅ All dependencies are installed")
        return True

def check_configuration():
    """Check if configuration is properly set up"""
    print("\n🔍 Checking configuration...")
    
    try:
        from bot.config import config
        print("✅ Configuration module loaded")
        
        # Check essential config values
        print(f"✅ Database path: {config.DATABASE_PATH}")
        print(f"✅ Media directory: {config.MEDIA_DIR}")
        print(f"✅ Cache directory: {config.CACHE_DIR}")
        
        # Check if .env file exists
        env_path = os.path.join(project_root, '.env')
        if os.path.exists(env_path):
            print("✅ .env file exists")
        else:
            print("⚠️  .env file not found (will use default values)")
        
        return True
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def check_database():
    """Check if database is properly initialized"""
    print("\n🔍 Checking database...")
    
    try:
        from bot.config import config
        
        if os.path.exists(config.DATABASE_PATH):
            print("✅ Database file exists")
            
            # Try to connect to database
            import asyncio
            from bot.database.manager import DatabaseManager
            
            async def test_db():
                db = DatabaseManager(config.DATABASE_PATH)
                await db.connect()
                # Try a simple query
                try:
                    result = await db.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
                    print("✅ Database connection successful")
                except Exception as e:
                    print(f"⚠️  Database query error: {e}")
                finally:
                    await db.close()
            
            asyncio.run(test_db())
        else:
            print("⚠️  Database file not found (run bot/init_db.py to create it)")
        
        return True
    except Exception as e:
        print(f"❌ Database check error: {e}")
        return False

def main():
    """Run all health checks"""
    print("Telegram Content Grabber Bot - Health Check")
    print("=" * 45)
    
    checks = [
        ("Environment", check_environment),
        ("Dependencies", check_dependencies),
        ("Configuration", check_configuration),
        ("Database", check_database)
    ]
    
    results = []
    for name, check_func in checks:
        try:
            result = check_func()
            results.append(result)
        except Exception as e:
            print(f"❌ {name} check failed: {e}")
            results.append(False)
    
    print("\n" + "=" * 45)
    if all(results):
        print("🎉 All checks passed! The bot is ready to run.")
        print("\nTo start the bot, run:")
        print("  start_bot.bat")
        print("  or")
        print("  python launch_bot.py")
        return 0
    else:
        print("❌ Some checks failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())