# Telegram Content Grabber Bot

A powerful bot for grabbing and republishing content from various sources to Telegram channels.

## Setup Instructions

### 1. Install Dependencies

Run the installation script:
```
install_requirements.bat
```

Or manually install dependencies:
```
pip install -r bot/requirements.txt
```

### 2. Configure Environment Variables

You have two options:

**Option A: Use the GUI (Recommended)**
When you start the bot, if credentials are missing, it will automatically offer to launch a GUI configurator.

**Option B: Manual Configuration**
Edit the `.env` file with your credentials:

1. **Telegram API Credentials**:
   - Go to https://my.telegram.org and create an application
   - Fill in `TELEGRAM_API_ID` and `TELEGRAM_API_HASH`

2. **Bot Token**:
   - Talk to @BotFather on Telegram to create a bot
   - Fill in `BOT_TOKEN` with your bot's token

3. **Admin ID**:
   - Fill in `ADMIN_ID` with your Telegram user ID

See [TELEGRAM_SETUP.md](TELEGRAM_SETUP.md) for detailed instructions on getting these credentials.

### 3. Initialize Database

Run the database initialization script:
```
cd bot
python init_db.py
```

### 4. Run the Bot

Start the bot using one of these methods:

1. Using the batch file (recommended):
   ```
   start_bot.bat
   ```

2. Using the launcher script:
   ```
   python launch_bot.py
   ```

3. Directly from the bot directory:
   ```
   cd bot
   python main.py
   ```

When starting the bot for the first time, if credentials are missing, you'll be prompted to configure them using the GUI.

## Features

- Content parsing from websites, TikTok, and Telegram channels
- Automatic content republishing to target channels
- Scheduled content posting
- Media downloading and processing
- Content filtering and customization
- Web interface for management (coming soon)
- GUI configuration tool for easy setup

## Requirements

- Python 3.8+
- Telegram API credentials
- Telegram bot token
- Internet connection

## Troubleshooting

### Common Issues

1. **ModuleNotFoundError**: Make sure all dependencies are installed:
   ```
   pip install -r bot/requirements.txt
   ```

2. **Database errors**: Reinitialize the database:
   ```
   cd bot
   python init_db.py
   ```

3. **Import errors**: Use the provided launcher script or batch files to run the bot.

4. **Bot hangs on startup**: This usually means missing or invalid Telegram credentials. The bot will now prompt you to configure them.

### Getting Telegram Credentials

See the detailed guide in [TELEGRAM_SETUP.md](TELEGRAM_SETUP.md) for step-by-step instructions.

## Project Structure

```
bot/
├── core/          # Core bot functionality
├── database/      # Database management
├── handlers/      # Command and message handlers
├── parsers/       # Content parsers for different sources
├── utils/         # Utility functions
├── config.py      # Configuration management
├── main.py        # Main entry point
├── init_db.py     # Database initialization
└── requirements.txt # Python dependencies
```

## License

This project is licensed under the MIT License.