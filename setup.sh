#!/bin/bash

# Exit on error
set -e

echo "=================================================="
echo "Vessel TrackAPI - Linux Setup Script"
echo "=================================================="

# 1. Remove existing venv if it exists
if [ -d "venv" ]; then
    echo "Removing existing virtual environment..."
    rm -rf venv
fi

# 2. Create new virtual environment
echo "Creating new virtual environment..."
python3 -m venv venv

# 3. Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/bin/activate || source venv/bin/activate

# 4. Upgrade pip
echo "Installing all dependencies..."
pip install --upgrade pip
pip install playwright fastapi uvicorn beautifulsoup4 lxml opencv-python-headless easyocr Pillow numpy onnxruntime
pip install -r requirements.txt

# 6. Install Playwright browsers and system dependencies
echo "Installing Playwright browsers..."
playwright install chromium

echo "Installing Playwright system dependencies..."
# This might require sudo if not already installed
if command -v sudo &> /dev/null; then
    sudo playwright install-deps chromium
else
    playwright install-deps chromium
fi

echo "=================================================="
echo "Setup Complete!"
echo "To activate the environment: source venv/bin/activate"
echo "To run the server: python main.py"
echo "=================================================="
and you need to do one thing also in get you will get the status also 