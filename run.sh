# docker-compose up -d
source .venv/Scripts/activate
py src/main.py >logs/$(date +"%Y%m%d-%H%M").log 2>&1
