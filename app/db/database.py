import aioodbc
import logging
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from app.core.config import settings

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.pool: Optional[aioodbc.Pool] = None
        
    async def create_pool(self):
        dsn = (
            f"DRIVER={{{settings.DB_DRIVER}}};"
            f"SERVER={settings.DB_SERVER},{settings.DB_PORT};"
            f"DATABASE={settings.DB_DATABASE};"
            f"UID={settings.DB_USERNAME};"
            f"PWD={settings.DB_PASSWORD};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=yes;"
        )
        
        self.pool = await aioodbc.create_pool(
            dsn=dsn,
            minsize=5,
            maxsize=20,
            echo=settings.DEBUG,
            autocommit=False,
            timeout=30
        )
        logger.info("Database pool created")
    
    async def close_pool(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
    
    @asynccontextmanager
    async def get_connection(self):
        async with self.pool.acquire() as conn:
            try:
                yield conn
            except Exception as e:
                await conn.rollback()
                raise
    
    @asynccontextmanager
    async def get_transaction(self):
        async with self.get_connection() as conn:
            async with conn.cursor() as cursor:
                try:
                    yield cursor
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise

db_manager = DatabaseManager()