from fastapi import APIRouter
from datetime import datetime
from app.core.config import settings
from app.core.server_setup import get_ssl_config
from app.services.database_service import DatabaseService
# import sys
from fastapi import Body


router = APIRouter()

@router.get("/")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "service": settings.APP_NAME,
        "database": {
            "type": "MS SQL Server",
            "server": settings.DB_SERVER,
            "database": settings.DB_DATABASE            
        }
    }

@router.get("/db")
async def health_check_db():
    try:
        result = await DatabaseService.execute_scalar("SELECT 1")
        return {
            "status": "healthy",
            "database": "connected",
            "server": f"{settings.DB_SERVER}:{settings.DB_PORT}",
            "test_result": result
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
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

@router.get("/db_empty")
async def db_empty():
    sys_tables = [
        'sys_users'
    ]
    missing_tables = []
    try:
        for table in sys_tables:
            check_sql = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}'"
            count = await DatabaseService.execute_scalar(check_sql)
            if count == 0:
                missing_tables.append(table)
        is_empty = len(missing_tables) == len(sys_tables)
        return {
            "db_empty": is_empty,
            "missing_sys_tables": missing_tables
        }
    except Exception as e:
        return {
            "db_empty": None,
            "error": str(e)
        }
    
@router.post("/db_create_table")
async def db_create_table():

    from app.db.migration_service import MigrationService
    from app.db.database import db_manager

    try:
        await db_manager.create_pool()
        migration_service = MigrationService()
        results = await migration_service.create_all_tables()
        return results
    except Exception as e:
        await db_manager.close_pool()
        return {"error": str(e)}