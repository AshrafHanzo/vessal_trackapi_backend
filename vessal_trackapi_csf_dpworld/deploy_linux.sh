#!/bin/bash

# Configuration
APP_DIR="/root/api_special"
PORT=30016
SERVICE_NAME="special-doc-api"

echo "=================================================="
echo "DEPLOYING SPECIAL DOCUMENT FINDER API"
echo "=================================================="

# 1. Update and Navigate to directory
cd $APP_DIR || exit
echo "Working in: $APP_DIR"

# 2. Create Virtual Environment
echo "Creating Virtual Environment..."
python3 -m venv venv

# 3. Install Dependencies
echo "Installing dependencies (FastAPI, Uvicorn, OpenAI)..."
./venv/bin/pip install --upgrade pip
./venv/bin/pip install fastapi uvicorn openai

# 4. Open Port in Firewall (UFW)
echo "Opening port $PORT in UFW..."
ufw allow $PORT/tcp

# 5. Create systemd Service File
echo "Creating systemd service file..."
cat <<EOF > /etc/systemd/system/$SERVICE_NAME.service
[Unit]
Description=Special Import Document Finder API
After=network.target

[Service]
User=root
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/python $APP_DIR/special_doc_api.py
Restart=always

[Install]
WantedBy=multi-user.target
EOF

# 6. Reload and Start Service
echo "Reloading systemd and starting service..."
systemctl daemon-reload
systemctl enable $SERVICE_NAME
systemctl restart $SERVICE_NAME

echo "=================================================="
echo "DEPLOYMENT COMPLETE!"
echo "Service status: systemctl status $SERVICE_NAME"
echo "API is running on port: $PORT"
echo "=================================================="
