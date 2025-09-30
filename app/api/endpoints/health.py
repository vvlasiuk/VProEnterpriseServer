from fastapi import APIRouter
from datetime import datetime
from app.core.config import settings
from app.core.server_setup import get_ssl_config

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

@router.get("/ssl")
async def ssl_status():
    """Статус SSL конфігурації"""
    ssl_config = get_ssl_config()
    
    return {
        "ssl_enabled": bool(ssl_config),
        "ssl_config": {
            "has_keyfile": "ssl_keyfile" in ssl_config,
            "has_certfile": "ssl_certfile" in ssl_config,
            "has_password": "ssl_keyfile_password" in ssl_config
        } if ssl_config else {},
        "settings": {
            "USE_SSL": getattr(settings, 'USE_SSL', False),
            "SSL_KEYFILE": getattr(settings, 'SSL_KEYFILE', None),
            "SSL_CERTFILE": getattr(settings, 'SSL_CERTFILE', None)
        }
    }