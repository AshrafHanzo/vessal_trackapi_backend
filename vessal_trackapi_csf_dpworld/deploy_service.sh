#!/bin/bash

# Vessel TrackAPI - Systemd Deployment Script
# This script handles the automation of starting the service and keeping it alive.

# Configuration
SERVICE_NAME="vessel-tracking"
APP_DIR="/root/vessal_trackapi"
PYTHON_BIN="$APP_DIR/venv/bin/python"
MAIN_SCRIPT="main.py"
SERVICE_FILE="/etc/systemd/system/$SERVICE_NAME.service"

echo "=================================================="
echo "Vessel TrackAPI - Systemd Service Deployment"
echo "=================================================="

# 1. Create the systemd service file
echo "Creating service file at $SERVICE_FILE..."

cat <<EOF > $SERVICE_FILE
[Unit]
Description=Vessel Tracking API Service
After=network.target

[Service]
User=root
WorkingDirectory=$APP_DIR
ExecStart=$PYTHON_BIN $MAIN_SCRIPT
Restart=always
RestartSec=5
StandardOutput=append:$APP_DIR/service_stdout.log
StandardError=append:$APP_DIR/service_stderr.log

[Install]
WantedBy=multi-user.target
EOF

# 2. Reload systemd to recognize the new service
echo "Reloading systemd daemon..."
systemctl daemon-reload

# 3. Enable the service to start on boot
echo "Enabling service to start on boot..."
systemctl enable $SERVICE_NAME

# 4. Start the service
echo "Starting service..."
systemctl restart $SERVICE_NAME

# 5. Show status
echo "Checking service status..."
systemctl status $SERVICE_NAME --no-pager

echo "=================================================="
echo "DEPLOYMENT COMPLETE!"
echo "Service: $SERVICE_NAME"
echo "Logs: tail -f $APP_DIR/service_stdout.log"
echo "Command to stop: systemctl stop $SERVICE_NAME"
echo "Command to logs: journalctl -u $SERVICE_NAME -f"
echo "=================================================="
