# Dev Container Setup

This directory contains the VS Code Dev Container configuration for the nvv-graph project.

## Features

- **Python 3.12** - Latest Python version for optimal performance
- **Pre-installed packages**:
  - FastAPI (>=0.115.0) - Modern, fast web framework
  - Uvicorn (>=0.32.0) - Lightning-fast ASGI server
  - DuckDB (>=1.1.0) - High-performance analytical database
  - SQLAlchemy (>=2.0.35) - Database toolkit and ORM
  - Pandas & PyArrow - Data processing optimization
  - Pydantic - Type validation

- **Data Volume Mount**: Host's `/data` directory is automatically mounted to `/workspaces/nvv-graph/data`
- **Port Forwarding**: Port 8000 is forwarded for FastAPI application
- **Optimized Environment**: 
  - `PYTHONUNBUFFERED=1` for real-time logging
  - `DUCKDB_PYTHON_VERSION=3.12` for version compatibility

## Usage

1. Install [VS Code](https://code.visualstudio.com/) and the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
2. Open this repository in VS Code
3. Press `F1` and select "Dev Containers: Reopen in Container"
4. Wait for the container to build and dependencies to install
5. Start developing!

## Data Directory

Make sure you have a `/data` directory on your host machine. This will be mounted into the container at `/workspaces/nvv-graph/data` for DuckDB/SQLite data processing.

If you don't have a `/data` directory, create it first:
```bash
sudo mkdir -p /data
sudo chmod 755 /data
```

Alternatively, you can modify the mount path in `devcontainer.json` to point to a different location.
