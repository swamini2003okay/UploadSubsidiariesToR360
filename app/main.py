from fastapi import FastAPI
from .routes import route

app = FastAPI(
    title="PSD API",
    description="API for extracting PSD numbers and fetching parent IDs",
    version="1.0.0"
)

# Include the router
app.include_router(route.router, prefix="/api")