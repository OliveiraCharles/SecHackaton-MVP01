# MVP 01

## Pre-requisites

- Docker
- Docker Compose
- Python

## Running Application

### 1. Infra

```sh
# Up containers
docker-compose up -d
```

### 2. Python Environment

```sh
## Create an virtual environment
# Windows:
python -m venv .venv
# Linux:
# python3 -m venv .venv

## Activate the virtual environment
# WSL 2:
source .venv/Scripts/activate
# Linux: 
# source .venv/bin/activate
# Windows: 
# .venv/bin/activate

## Update pip 
python -m pip install -U pip 

## install Requirements
pip install -r requirements.txt

## (Optional) Install dev dependencies
# pip install isort blue pytest bandit
```

### 3. Application

```sh
# Run application
python src/main.py
```

## References
