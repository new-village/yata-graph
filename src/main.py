import contextlib
from fastapi import FastAPI
from src.loader import load_data
from src.api import router as api_router
from src import deps

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    print("Startup: Loading Data...")
    # Initialize the singleton connection
    # In simpler model, load_data returns connection, we store it in deps? 
    # Actually deps.py has lazy loading. 
    # But for startup verification:
    
    # We can force load here. 
    # But deps.py handles singleton `get_db`.
    # Let's ensure it's loaded.
    deps.get_db()
    
    yield
    print("Shutdown: Closing connection...")
    if deps._db_connection:
        deps._db_connection.close()

app = FastAPI(title="Yata Graph API", description="Parquet-backed Graph API", version="0.1.0", lifespan=lifespan)

app.include_router(api_router, prefix="/api/v1")

