# Telegram Content Grabber Bot - Configuration Guide

## Overview

This guide explains how the Telegram Content Grabber Bot handles configuration and credentials setup. The bot now includes an integrated GUI for easy configuration when required environment variables are missing.

## Required Environment Variables

The bot requires these environment variables to function fully:

1. **BOT_TOKEN** - Telegram bot token from @BotFather
2. **ADMIN_ID** - Your Telegram user ID
3. **TELEGRAM_API_ID** - API ID from https://my.telegram.org
4. **TELEGRAM_API_HASH** - API Hash from https://my.telegram.org

## Configuration Methods

### Method 1: GUI Configuration (Recommended)

When the bot starts and detects missing credentials, it will automatically prompt you to configure them using a graphical interface:

```
📝 Telegram credentials are missing!
Would you like to configure them now using the GUI? (y/N):
```

If you answer 'y' or 'yes', a GUI window will open allowing you to:

1. Enter all required credentials
2. Load an existing .env file
3. Save the configuration to a new .env file

The GUI includes:
- Input validation
- Placeholder text for guidance
- Load/Save functionality
- Keyboard shortcuts (Enter to save, Escape to cancel)

### Method 2: Manual Configuration

You can manually create or edit the `.env` file in the project root directory:

```env
# Telegram Content Grabber Bot Configuration
BOT_TOKEN=123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ1234567890
ADMIN_ID=987654321
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=1234567890abcdef1234567890abcdef
```

## Startup Process

The bot startup process now includes:

1. **Dependency Check** - Verifies all required packages are installed
2. **Configuration Check** - Validates environment variables
3. **GUI Prompt** - Offers to launch GUI if credentials are missing
4. **Database Check** - Verifies database file exists
5. **Bot Launch** - Starts the bot in appropriate mode

## Bot Modes

### Full Mode
When all credentials are provided, the bot runs with full functionality:
- Connects to Telegram API
- Can send and receive messages
- Can parse content from sources
- Can publish to channels

### Limited Mode
When credentials are missing, the bot runs in limited mode:
- Database functionality works
- Can perform local operations
- Cannot connect to Telegram
- Shows warnings about missing credentials

## Security Notes

- Keep your credentials secure
- Don't share your .env file
- The .env file is in .gitignore to prevent accidental sharing
- Regenerate credentials if you suspect they're compromised

## Troubleshooting

### GUI Not Launching
If the GUI doesn't launch:
1. Ensure you're on a system with a graphical interface
2. Check that tkinter is available (usually included with Python)
3. Try manual configuration as a fallback

### Invalid Credentials
If credentials are rejected:
1. Verify API ID is numeric (not the app ID)
2. Verify API Hash is the full string
3. Verify Bot Token includes the colon
4. Verify Admin ID is your numeric Telegram user ID

### Configuration Not Saved
If configuration isn't saved:
1. Check file permissions in the project directory
2. Ensure the directory is writable
3. Try running as administrator (Windows) or with sudo (Linux/Mac)

## Getting Help

For detailed instructions on obtaining Telegram credentials, see [TELEGRAM_SETUP.md](TELEGRAM_SETUP.md).

If you encounter issues, you can:
1. Check the console output for error messages
2. Verify your .env file format
3. Run the health check script: `python health_check.py`