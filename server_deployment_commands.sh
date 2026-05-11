# ==========================================
# TRACK CONTAINER BACKEND - LINUX SERVER SETUP COMMANDS
# ==========================================
# Server IP: 103.14.121.28
# Port: 2244

# 1. Install System Dependencies & Redis
sudo apt update -y
sudo apt install -y python3 python3-pip python3-venv redis-server npm

# 2. Install PM2
sudo npm install -g pm2

# 3. Create Virtual Environment
cd /root/track_container
python3 -m venv venv
source venv/bin/activate

# 4. Install Python Packages
pip install --upgrade pip
pip install -r requirements_unified.txt
playwright install --with-deps chromium
playwright install chrome

# ==========================================
# CREATE LINUX AGENT SERVICES (systemctl)
# ==========================================
# NOTE: HMM, Hapag, RCL, Cosco run on WINDOWS SERVER (see below)
# Linux agents: Sealion, KMTC, ONE LINE, InterAsia, ESL, Wan Hai,
#               ICEGate, CFS, DPW, Adani Katu, Adani Ennore

PROJECT="/root/track_container"
PYTHON="$PROJECT/venv/bin/python"

cat > /etc/systemd/system/sealion-agent.service << EOF
[Unit]
Description=Track Container Sealion Agent
After=network.target redis-server.service
[Service]
WorkingDirectory=$PROJECT/Sealion
ExecStart=$PYTHON sealion_agent.py
Restart=always
Environment=REDIS_HOST=localhost
Environment=REDIS_PORT=6379
Environment=API_BASE_URL=https://trackcontainer.in/api/external
[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/kmtc-agent.service << EOF
[Unit]
Description=TC KMTC Agent
After=network.target redis-server.service
[Service]
WorkingDirectory=$PROJECT/vessel_trackapi_kmtc/kmtc
ExecStart=$PYTHON kmtc_agent.py
Restart=always
Environment=REDIS_HOST=localhost
Environment=REDIS_PORT=6379
Environment=API_BASE_URL=https://trackcontainer.in/api/external
[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/one-line-agent.service << EOF
[Unit]
Description=TC ONE Line Agent
After=network.target redis-server.service
[Service]
WorkingDirectory=$PROJECT/vessal_trackapi_one_line/vessal_trackapi_one_line
ExecStart=$PYTHON one_line_agent.py
Restart=always
Environment=REDIS_HOST=localhost
Environment=REDIS_PORT=6379
[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/interasia-agent.service << EOF
[Unit]
Description=TC InterAsia Agent
After=network.target
[Service]
WorkingDirectory=$PROJECT/interasia
ExecStart=$PYTHON interasia_agent.py
Restart=always
Environment=REDIS_HOST=localhost
Environment=REDIS_PORT=6379
[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/esl-agent.service << EOF
[Unit]
Description=TC ESL Agent
After=network.target
[Service]
WorkingDirectory=$PROJECT/ESL
ExecStart=$PYTHON esl_agent.py
Restart=always
Environment=REDIS_HOST=localhost
Environment=REDIS_PORT=6379
[Install]
WantedBy=multi-user.target
EOF

# HMM agent runs on WINDOWS SERVER (not Linux) — see Windows section below

cat > /etc/systemd/system/wan-hai-agent.service << EOF
[Unit]
Description=TC Wan Hai Agent
After=network.target
[Service]
WorkingDirectory=$PROJECT/wan_hai
ExecStart=$PYTHON wan_hai_agent.py
Restart=always
Environment=REDIS_HOST=localhost
Environment=REDIS_PORT=6379
[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/icegate-agent.service << EOF
[Unit]
Description=TC ICEGate Agent
After=network.target
[Service]
WorkingDirectory=$PROJECT/icegate
ExecStart=$PYTHON icegate_agent.py
Restart=always
Environment=REDIS_HOST=localhost
Environment=REDIS_PORT=6379
[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/cfs-agent.service << EOF
[Unit]
Description=TC CFS CITPL Agent
After=network.target
[Service]
WorkingDirectory=$PROJECT/vessal_trackapi_cfs
ExecStart=$PYTHON cfs_agent.py
Restart=always
Environment=REDIS_HOST=localhost
Environment=REDIS_PORT=6379
[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/dpw-agent.service << EOF
[Unit]
Description=TC DP World Agent
After=network.target
[Service]
WorkingDirectory=$PROJECT/vessal_trackapi_csf_dpworld
ExecStart=$PYTHON dpw_agent.py
Restart=always
Environment=REDIS_HOST=localhost
Environment=REDIS_PORT=6379
[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/adani-katu-agent.service << EOF
[Unit]
Description=TC Adani Kattupalli Agent
After=network.target
[Service]
WorkingDirectory=$PROJECT/vessal_trackapi_adaniports_katu
ExecStart=$PYTHON adani_katu_agent.py
Restart=always
Environment=REDIS_HOST=localhost
Environment=REDIS_PORT=6379
[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/adani-ennore-agent.service << EOF
[Unit]
Description=TC Adani Ennore Agent
After=network.target
[Service]
WorkingDirectory=$PROJECT/vessal_trackapi_adaniports_ennore
ExecStart=$PYTHON adani_ennore_agent.py
Restart=always
Environment=REDIS_HOST=localhost
Environment=REDIS_PORT=6379
[Install]
WantedBy=multi-user.target
EOF

# Reload systemd to recognize new files
systemctl daemon-reload

# Enable and Start all LINUX agents (HMM/Hapag/RCL/Cosco are on Windows)
for svc in sealion-agent kmtc-agent one-line-agent interasia-agent esl-agent \
           wan-hai-agent icegate-agent cfs-agent dpw-agent \
           adani-katu-agent adani-ennore-agent; do
    systemctl enable $svc
    systemctl start $svc
done

# ==========================================
# CREATE ORCHESTRATOR PM2 CONFIG
# ==========================================
cd /root/track_container

cat > ecosystem.config.js << 'EOF'
module.exports = {
    apps: [{
        name: "unified-orchestrator",
        script: "/root/track_container/venv/bin/python",
        args: "unified_orchestrator.py",
        cwd: "/root/track_container",
        restart_delay: 60000,
        autorestart: true,
        watch: false,
        env: {
            REDIS_HOST: "localhost",
            REDIS_PORT: "6379",
            API_BASE_URL: "https://trackcontainer.in/api/external"
        }
    }]
};
EOF

# Start PM2
pm2 start ecosystem.config.js
pm2 save
pm2 startup

# ==========================================
# USEFUL COMMANDS — LINUX SERVER
# ==========================================

# Check if orchestrator is running
pm2 status
pm2 logs unified-orchestrator

# Restart an individual agent (e.g., if you update the code)
systemctl restart wan-hai-agent

# Restart all stuck Linux agents at once
for svc in sealion-agent kmtc-agent one-line-agent interasia-agent esl-agent \
           wan-hai-agent icegate-agent cfs-agent dpw-agent \
           adani-katu-agent adani-ennore-agent; do
    systemctl restart $svc
done

# Check status of all Linux agents
for svc in sealion-agent kmtc-agent one-line-agent interasia-agent esl-agent \
           wan-hai-agent icegate-agent cfs-agent dpw-agent \
           adani-katu-agent adani-ennore-agent; do
    echo "=== $svc ==="
    systemctl is-active $svc
done

# View live logs for an agent
journalctl -u wan-hai-agent -f -n 100

# View queue dashboard
cd /root/track_container
source venv/bin/activate
python check_all_queues.py --watch

# ==========================================
# WINDOWS SERVER SETUP & COMMANDS
# ==========================================
# Server Path: C:\Users\Administrator\Desktop\windows_track_container
# Agents on Windows: Hapag, Cosco, RCL, HMM
# Managed by: windows_service_manager.py via NSSM (TC-WindowsManager)
#
# The windows_service_manager.py automatically starts and monitors:
#   - Windows Orchestrator (windows_orchestrator_runner.py)
#   - Cosco Agent (cosco/cosco_agent.py)
#   - Hapag Agent (hapag/hapag_agent.py)
#   - RCL Agent (rcl/rcl_agent.py)
#   - HMM Agent (vessal_trackapi_hmm/hmm/hmm_agent.py)
#
# Redis: Connects to Linux server at api.trackcontainer.in:30093

# --- NSSM Commands (run in Windows PowerShell as Administrator) ---

# Check service status
# .\nssm status TC-WindowsManager

# Stop all Windows agents
# .\nssm stop TC-WindowsManager

# Start all Windows agents
# .\nssm start TC-WindowsManager

# Restart all Windows agents
# .\nssm restart TC-WindowsManager

# View live logs
# Get-Content .\service.log -Tail 50 -Wait

# Filter logs for a specific agent
# Get-Content .\service.log -Tail 200 | Select-String "hapag|cosco|rcl|hmm"
