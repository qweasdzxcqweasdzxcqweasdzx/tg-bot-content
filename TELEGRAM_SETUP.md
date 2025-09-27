# Telegram API Credentials Setup Guide

To enable full functionality of the Telegram Content Grabber Bot, you need to configure your Telegram API credentials.

## Getting Telegram API Credentials

### Step 1: Get API ID and API Hash

1. Visit [https://my.telegram.org](https://my.telegram.org)
2. Log in with your phone number
3. Click on "API development tools"
4. Fill in the form:
   - **App title**: Telegram Content Grabber Bot
   - **Short name**: tgcontentbot
   - **URL**: (you can leave this blank or put any URL)
   - **Platform**: (select any or leave blank)
5. Click "Create application"
6. You'll receive:
   - **api_id** (a number)
   - **api_hash** (a long string)

### Step 2: Create a Bot and Get Token

1. Open Telegram
2. Search for [@BotFather](https://t.me/BotFather)
3. Start a chat with BotFather
4. Send `/newbot` command
5. Follow the instructions:
   - Enter a name for your bot (e.g., "Content Grabber Bot")
   - Enter a username for your bot (must end with "bot", e.g., "MyContentGrabberBot")
6. BotFather will provide you with a **token** (a long string with colons)

### Step 3: Get Your User ID

1. Open Telegram
2. Search for [@userinfobot](https://t.me/userinfobot) or [@getmyid_bot](https://t.me/getmyid_bot)
3. Start a chat and send any message
4. The bot will reply with your user ID (a number)

## Configuring the Bot

Edit the `.env` file in the project root directory:

```env
# Replace these with your actual credentials
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=your_api_hash_here
BOT_TOKEN=123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ1234567890
ADMIN_ID=987654321
```

### Example Configuration:

```env
# Telegram API credentials (get these from https://my.telegram.org)
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=1234567890abcdef1234567890abcdef
BOT_TOKEN=123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ1234567890
ADMIN_ID=987654321
```

## Testing the Configuration

After updating the `.env` file:

1. Save the file
2. Restart the bot:
   ```
   start_bot.bat
   ```
   or
   ```
   python launch_bot.py
   ```

The bot should now show that it has valid Telegram credentials and can connect to Telegram services.

## Troubleshooting

### Common Issues:

1. **Invalid API ID**: Make sure you're using the numeric API ID, not the app ID
2. **Invalid API Hash**: Make sure you're using the full API hash string
3. **Invalid Bot Token**: Make sure you're using the complete token provided by BotFather
4. **Invalid User ID**: Make sure you're using the numeric user ID

### Verification:

If the bot starts successfully with valid credentials, you should see:
```
✅ Valid Telegram credentials found, initializing clients
📱 Starting user client...
✅ User client started
🤖 Starting bot client...
✅ Bot client started
⏰ Scheduler started
🚀 Bot started successfully (waiting for commands)
```

Instead of:
```
⚠️  Telegram credentials not configured!
```

## Security Notes

- Keep your API credentials secret
- Don't share your `.env` file
- If you suspect your credentials are compromised, regenerate them on Telegram