#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script to verify the installation and configuration
"""

import os
import sys
import asyncio

# Add the bot directory to the Python path
bot_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot')
sys.path.insert(0, bot_dir)

def test_imports():
    """Test that all required modules can be imported"""
    try:
        import structlog
        print("✅ structlog import successful")
        
        import aiosqlite
        print("✅ aiosqlite import successful")
        
        from config import config
        print("✅ config import successful")
        
        from database.manager import DatabaseManager
        print("✅ DatabaseManager import successful")
        
        print("\n🎉 All imports successful!")
        return True
        
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False

def test_config():
    """Test that configuration loads correctly"""
    try:
        from config import config
        print(f"✅ Database path: {config.DATABASE_PATH}")
        print(f"✅ Media directory: {config.MEDIA_DIR}")
        print(f"✅ Cache directory: {config.CACHE_DIR}")
        print("\n🎉 Configuration loaded successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def test_database():
    """Test database connection"""
    try:
        from config import config
        from database.manager import DatabaseManager
        
        db_manager = DatabaseManager(config.DATABASE_PATH)
        print("✅ DatabaseManager created successfully")
        
        # Test connecting to database
        async def test_connection():
            await db_manager.connect()
            print("✅ Database connection successful")
            await db_manager.close()
            print("✅ Database closed successfully")
        
        asyncio.run(test_connection())
        print("\n🎉 Database tests successful!")
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

def main():
    """Run all tests"""
    print("Testing Telegram Content Grabber Bot Installation")
    print("=" * 50)
    
    tests = [
        ("Imports", test_imports),
        ("Configuration", test_config),
        ("Database", test_database)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\nRunning {test_name} test...")
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"❌ {test_name} test failed: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    if all(results):
        print("🎉 All tests passed! The bot is ready to run.")
        print("\nNext steps:")
        print("1. Edit the .env file with your Telegram credentials")
        print("2. Run start_bot.bat to start the bot")
    else:
        print("❌ Some tests failed. Please check the errors above.")
        print("\nTroubleshooting tips:")
        print("- Make sure all dependencies are installed")
        print("- Check that the .env file exists and is properly formatted")
        print("- Ensure the database file is accessible")

if __name__ == "__main__":
    main()