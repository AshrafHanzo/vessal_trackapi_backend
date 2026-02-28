#!/bin/bash
# ============================================================
# Deploy All Agent Workers — Run on Server
# ============================================================
# This script:
#   1. Installs redis package in each service's venv
#   2. Creates systemctl service files for each agent
#   3. Starts all agent services
#
# Usage: bash deploy_agents.sh
# ============================================================

set -e

# Project root — UPDATE THIS to your server path
PROJECT_DIR="/root/track_container_backend"

# Color helpers
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  TRACK CONTAINER — AGENT DEPLOYMENT${NC}"
echo -e "${GREEN}========================================${NC}\n"

# ============================================================
# STEP 1: Verify Redis is running
# ============================================================
echo -e "${YELLOW}[1/4] Checking Redis...${NC}"
if redis-cli ping 2>/dev/null | grep -q PONG; then
    echo -e "  ${GREEN}✓ Redis is running${NC}"
else
    echo -e "  ${RED}✗ Redis not running! Installing & starting...${NC}"
    apt-get update && apt-get install -y redis-server
    systemctl enable redis-server
    systemctl start redis-server
    echo -e "  ${GREEN}✓ Redis installed and started${NC}"
fi

# ============================================================
# STEP 2: Install redis package in each venv
# ============================================================
echo -e "\n${YELLOW}[2/4] Installing redis package in all venvs...${NC}"

SERVICES=(
    "Sealion"
    "icegate"
    "vessal_trackapi_Port"
    "vessal_trackapi_cfs"
    "vessal_trackapi_csf_dpworld"
    "vessal_trackapi_adaniports_katu"
    "vessal_trackapi_adaniports_ennore"
)

for svc in "${SERVICES[@]}"; do
    SVC_DIR="$PROJECT_DIR/$svc"
    VENV_PIP="$SVC_DIR/venv/bin/pip"
    
    if [ -f "$VENV_PIP" ]; then
        echo -e "  Installing in $svc..."
        $VENV_PIP install redis>=5.0.0 requests 2>&1 | tail -1
        echo -e "  ${GREEN}✓ $svc${NC}"
    else
        echo -e "  ${RED}✗ $svc — venv not found at $SVC_DIR/venv${NC}"
    fi
done

# Also install redis in root venv (for main_orchestrator)
ROOT_VENV_PIP="$PROJECT_DIR/venv/bin/pip"
if [ -f "$ROOT_VENV_PIP" ]; then
    echo -e "  Installing in root venv..."
    $ROOT_VENV_PIP install redis>=5.0.0 2>&1 | tail -1
    echo -e "  ${GREEN}✓ root venv${NC}"
fi

# ============================================================
# STEP 3: Create systemctl service files for agents
# ============================================================
echo -e "\n${YELLOW}[3/4] Creating systemctl service files for agents...${NC}"

# Agent configs: service_name | folder | agent_script
declare -A AGENTS
AGENTS=(
    ["sealion-agent"]="Sealion|sealion_agent.py"
    ["icegate-agent"]="icegate|icegate_agent.py"
    ["ldb-agent"]="vessal_trackapi_Port|ldb_agent.py"
    ["cfs-agent"]="vessal_trackapi_cfs|cfs_agent.py"
    ["dpw-agent"]="vessal_trackapi_csf_dpworld|dpw_agent.py"
    ["adani-katu-agent"]="vessal_trackapi_adaniports_katu|adani_katu_agent.py"
    ["adani-ennore-agent"]="vessal_trackapi_adaniports_ennore|adani_ennore_agent.py"
)

for svc_name in "${!AGENTS[@]}"; do
    IFS='|' read -r folder script <<< "${AGENTS[$svc_name]}"
    SVC_DIR="$PROJECT_DIR/$folder"
    PYTHON="$SVC_DIR/venv/bin/python"
    SCRIPT="$SVC_DIR/$script"
    
    SERVICE_FILE="/etc/systemd/system/${svc_name}.service"
    
    cat > "$SERVICE_FILE" << EOF
[Unit]
Description=Track Container ${svc_name}
After=network.target redis-server.service

[Service]
Type=simple
User=root
WorkingDirectory=${SVC_DIR}
ExecStart=${PYTHON} ${SCRIPT}
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
Environment=REDIS_HOST=localhost
Environment=REDIS_PORT=6379
Environment=API_BASE_URL=https://uat.trackcontainer.in/api/external

[Install]
WantedBy=multi-user.target
EOF

    echo -e "  ${GREEN}✓ Created ${SERVICE_FILE}${NC}"
done

# Reload systemd
systemctl daemon-reload
echo -e "  ${GREEN}✓ systemd reloaded${NC}"

# ============================================================
# STEP 4: Enable and start all agent services
# ============================================================
echo -e "\n${YELLOW}[4/4] Starting all agent services...${NC}"

for svc_name in "${!AGENTS[@]}"; do
    systemctl enable "$svc_name" 2>/dev/null
    systemctl restart "$svc_name"
    
    # Check status
    if systemctl is-active --quiet "$svc_name"; then
        echo -e "  ${GREEN}✓ $svc_name — running${NC}"
    else
        echo -e "  ${RED}✗ $svc_name — failed to start${NC}"
        echo -e "    Check logs: journalctl -u $svc_name -n 20"
    fi
done

# ============================================================
# SUMMARY
# ============================================================
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  DEPLOYMENT COMPLETE${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "  Useful commands:"
echo "  ────────────────────────────────────────"
echo "  Check all agents:  systemctl list-units --type=service | grep agent"
echo "  View logs:         journalctl -u sealion-agent -f"
echo "  Queue monitor:     python $PROJECT_DIR/check_all_queues.py"
echo "  Queue dashboard:   python $PROJECT_DIR/check_all_queues.py --watch"
echo ""
echo "  Run orchestrator:  python $PROJECT_DIR/main_orchestrator.py"
echo ""
echo "  Stop all agents:"
echo "    for s in sealion-agent icegate-agent ldb-agent cfs-agent dpw-agent adani-katu-agent adani-ennore-agent; do systemctl stop \$s; done"
echo ""
