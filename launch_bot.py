#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Launcher script for Telegram Content Grabber Bot
This script properly sets up the Python path and handles startup gracefully.
"""

import sys
import os
import logging

# Add the bot directory to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
bot_dir = os.path.join(project_root, 'bot')
sys.path.insert(0, project_root)
sys.path.insert(0, bot_dir)

def check_dependencies():
    """Check if all required dependencies are available"""
    try:
        import structlog
        import aiosqlite
        import telethon
        import apscheduler
        import bs4
        import aiohttp
        import PIL
        import psutil
        import pytz
        import dotenv
        print("✅ All dependencies are available")
        return True
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Please run install_requirements.bat to install all dependencies")
        return False

def check_config():
    """Check if configuration is properly set up"""
    try:
        from bot.config import config
        print("✅ Configuration loaded successfully")
        
        # Check if Telegram credentials are configured
        has_valid_creds = (
            config.TELEGRAM_API_ID and 
            config.TELEGRAM_API_HASH and 
            str(config.TELEGRAM_API_ID) != '0' and
            config.BOT_TOKEN and 
            len(config.BOT_TOKEN) > 10
        )
        
        if has_valid_creds:
            print("✅ Telegram credentials configured")
        else:
            print("⚠️  Telegram credentials not configured")
            print("   The bot will run in limited mode without Telegram connectivity")
        
        return True, has_valid_creds
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False, False

def check_database():
    """Check if database is properly initialized"""
    try:
        from bot.config import config
        if os.path.exists(config.DATABASE_PATH):
            print("✅ Database file exists")
            return True
        else:
            print("⚠️  Database file not found. Please run bot/init_db.py to initialize the database")
            return False
    except Exception as e:
        print(f"❌ Database check error: {e}")
        return False

def launch_gui_if_needed():
    """Launch GUI to configure environment variables if they're missing"""
    try:
        from bot.config import config
        
        # Check if we have valid credentials
        has_valid_creds = (
            config.TELEGRAM_API_ID and 
            config.TELEGRAM_API_HASH and 
            str(config.TELEGRAM_API_ID) != '0' and
            config.BOT_TOKEN and 
            len(config.BOT_TOKEN) > 10
        )
        
        if not has_valid_creds:
            print("\n📝 Telegram credentials are missing!")
            response = input("Would you like to configure them now using the GUI? (y/N): ").strip().lower()
            
            if response in ['y', 'yes']:
                try:
                    from bot.utils.gui import run_env_creator_gui
                    env_file = run_env_creator_gui()
                    if env_file:
                        print(f"\n✅ Configuration saved to {env_file}")
                        print("🔄 Please restart the bot to use the new configuration")
                        return True
                    else:
                        print("\n❌ Configuration was not saved")
                except Exception as e:
                    print(f"❌ Error launching GUI: {e}")
                    print("Fallback to manual configuration...")
                    return False
            else:
                print("\nℹ️  You can configure credentials later by editing the .env file")
        
        return False
    except Exception as e:
        print(f"❌ Error checking configuration: {e}")
        return False

def main():
    """Main function to start the bot"""
    print("Telegram Content Grabber Bot - Startup Checker")
    print("=" * 50)
    
    # Check dependencies
    if not check_dependencies():
        return 1
    
    # Check configuration
    config_ok, has_valid_creds = check_config()
    if not config_ok:
        return 1
    
    # Check database
    check_database()  # This is not critical for startup
    
    # If credentials are missing, offer to launch GUI
    if not has_valid_creds:
        if launch_gui_if_needed():
            return 0  # Exit after GUI configuration
    
    print("\n🚀 Starting bot...")
    print("💡 Press Ctrl+C to stop the bot")
    print("=" * 50)
    
    try:
        # Import and run the main module
        from bot.main import main as bot_main
        import asyncio
        asyncio.run(bot_main())
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
        return 0
    except Exception as e:
        print(f"❌ Error starting bot: {e}")
        logging.exception("Error starting bot")
        return 1

if __name__ == "__main__":
    sys.exit(main())