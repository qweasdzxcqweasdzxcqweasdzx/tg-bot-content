# Telegram Content Grabber Bot - Startup Guide

## How to Start the Bot

### Method 1: Using the Batch File (Recommended for Windows)
```
start_bot.bat
```

This will:
1. Show startup instructions
2. Check if dependencies are installed
3. Start the bot with proper configuration

### Method 2: Using the Launcher Script
```
python launch_bot.py
```

This will:
1. Perform health checks
2. Verify all dependencies
3. Start the bot

### Method 3: Direct Execution
```
cd bot
python main.py
```

## Health Check

Before starting the bot, you can run a health check to verify everything is properly configured:

```
python health_check.py
```

This will check:
- Environment setup
- Dependencies installation
- Configuration files
- Database connectivity

## Prerequisites

1. **Python 3.8+** must be installed and available in PATH
2. **Dependencies** must be installed:
   ```
   install_requirements.bat
   ```
3. **Configuration** must be set up in `.env` file
4. **Database** must be initialized:
   ```
   cd bot
   python init_db.py
   ```

## Configuration

Make sure to edit the `.env` file with your:
- Telegram API ID and Hash (from https://my.telegram.org)
- Bot Token (from @BotFather)
- Admin User ID (your Telegram user ID)

## Troubleshooting

### Common Issues

1. **Import Errors**: Make sure you're using the launcher script or batch file
2. **Missing Dependencies**: Run `install_requirements.bat`
3. **Database Issues**: Run `cd bot && python init_db.py`
4. **Configuration Errors**: Check the `.env` file format

### Stopping the Bot

To stop the bot, press `Ctrl+C` in the terminal where it's running.

## File Structure

```
├── start_bot.bat          # Main startup script (Windows)
├── launch_bot.py          # Python launcher script
├── health_check.py        # Health verification script
├── .env                   # Configuration file
├── bot/                   # Bot source code
│   ├── main.py           # Main entry point
│   ├── config.py         # Configuration management
│   ├── init_db.py        # Database initialization
│   └── ...               # Other modules
```

## Verification

When the bot starts successfully, you should see:
```
🚀 Starting Telegram Content Grabber Bot
🔧 Optimization: Disabled
Кэширование: 1000 entries
👷 Workers: 3
✅ Bot components initialized successfully
✅ Database pool created successfully
🚀 Bot started successfully
```

The bot will continue running until stopped with `Ctrl+C`.