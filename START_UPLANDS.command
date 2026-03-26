#!/bin/bash
set -u
export PATH="/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin:${PATH:-}"

wait_for_http() {
  local url="$1"
  local seconds="${2:-15}"
  local i
  for ((i=1; i<=seconds; i++)); do
    if /usr/bin/curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_tunnel() {
  local seconds="${1:-15}"
  local i
  for ((i=1; i<=seconds; i++)); do
    if cloudflared tunnel info uplands-site-induction 2>/dev/null | grep -Eq '^[0-9a-f-]{36}[[:space:]]'; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Move to the project folder
cd "$(dirname "$0")"
# Clear any old tunnel ghosts
pkill cloudflared > /dev/null 2>&1 || true
pkill -f "http.server 8502" > /dev/null 2>&1 || true
pkill -f "gps_server.py" > /dev/null 2>&1 || true
# Start the GPS capture page server
echo "Starting helper server..."
nohup python3 gps_server.py </dev/null >> gps.log 2>&1 &
GPS_PID=$!
disown || true
if wait_for_http "http://127.0.0.1:8502/health" 15; then
  echo "Helper OK: http://127.0.0.1:8502/health"
else
  echo "Warning: helper server did not verify on port 8502. Check gps.log"
fi
# Start the secure bridge
echo "Starting Cloudflare tunnel..."
nohup cloudflared tunnel --config "$HOME/.cloudflared/config.yml" run uplands-site-induction </dev/null >> tunnel.log 2>&1 &
TUNNEL_PID=$!
disown || true
if wait_for_tunnel 20; then
  echo "Tunnel OK: uplands-site-induction"
  echo "Public URL: https://uplands-site-induction.omegaleague.win"
else
  echo "Warning: tunnel did not verify. Check tunnel.log"
fi
# Fire up the Command Centre
python3 -m streamlit run app.py
# Close the bridge when the app shuts down
kill "$GPS_PID" > /dev/null 2>&1 || true
kill "$TUNNEL_PID" > /dev/null 2>&1 || true
