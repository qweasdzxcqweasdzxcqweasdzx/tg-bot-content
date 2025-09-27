@echo off
title Telegram Content Grabber Bot
echo Telegram Content Grabber Bot
echo ===========================
echo.
echo This script will:
echo 1. Check if all dependencies are installed
echo 2. Verify Telegram credentials configuration
echo 3. Launch the GUI if credentials are missing
echo 4. Start the bot
echo.
echo Press any key to continue...
pause >nul
echo.
echo Checking system...
echo.

cd /d "%~dp0"

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python is not installed or not in PATH
    echo Please install Python 3.8+ and make sure it's in your PATH
    echo.
    pause
    exit /b 1
)

REM Run the launcher script
python launch_bot.py

echo.
echo Bot session ended.
pause