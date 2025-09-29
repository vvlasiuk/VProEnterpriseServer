from fastapi import APIRouter
from datetime import datetime
from app.core.config import settings

router = APIRouter()

@router.get("/")
async def health_check():
    """Health check для MS SQL (заглушка)"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "service": settings.APP_NAME,
        "database": {
            "type": "MS SQL Server",
            "server": settings.DB_SERVER,
            "database": settings.DB_DATABASE,
            "status": "mock_connection_ok"
        }
    }

@router.get("/db")
async def health_check_db():
    """Детальна перевірка MS SQL (заглушка)"""
    return {
        "status": "healthy",
        "database": {
            "type": "Microsoft SQL Server",
            "connection": "mock_active",
            "driver": settings.DB_DRIVER,
            "pool_status": "mock_ready"
        },
        "timestamp": datetime.utcnow()
    }