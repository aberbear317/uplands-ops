#!/bin/bash
cd /Users/ceriedwards/uplands-ops
pkill -f "streamlit run app.py" >/dev/null 2>&1 || true
exec python3 -m streamlit run app.py
