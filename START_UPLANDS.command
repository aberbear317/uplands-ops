#!/bin/bash
# Move to the project folder
cd "$(dirname "$0")"
# Clear any old tunnel ghosts
pkill cloudflared > /dev/null 2>&1 || true
pkill -f "http.server 8502" > /dev/null 2>&1 || true
pkill -f "gps_server.py" > /dev/null 2>&1 || true
# Start the GPS capture page server
nohup python3 gps_server.py </dev/null >> gps.log 2>&1 &
GPS_PID=$!
disown || true
# Start the secure bridge
nohup cloudflared tunnel --config "$HOME/.cloudflared/config.yml" run uplands-site-induction </dev/null >> tunnel.log 2>&1 &
TUNNEL_PID=$!
disown || true
# Fire up the Command Centre
python3 -m streamlit run app.py
# Close the bridge when the app shuts down
kill "$GPS_PID" > /dev/null 2>&1 || true
kill "$TUNNEL_PID" > /dev/null 2>&1 || true
