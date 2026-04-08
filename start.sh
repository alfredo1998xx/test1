#!/bin/bash
# Seed production database with users if empty
python seed_production.py

# Start FastAPI backend in background, then Streamlit in foreground
uvicorn main:app --host 0.0.0.0 --port 8000 &
streamlit run app.py --server.port 5000 --server.address 0.0.0.0
