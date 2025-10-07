from typing import List, Dict, Any, Optional
from app.db.database import db_manager
import logging

logger = logging.getLogger(__name__)

class DatabaseService:
    @staticmethod
    async def execute_query(query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """SELECT запити"""
        async with db_manager.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params or ())
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                rows = await cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
    
    @staticmethod
    async def execute_non_query(query: str, params: tuple = None) -> int:
        """INSERT/UPDATE/DELETE запити"""
        async with db_manager.get_transaction() as cursor:
            await cursor.execute(query, params or ())
            return cursor.rowcount
    
    @staticmethod
    async def execute_scalar(query: str, params: tuple = None) -> Any:
        """Запити що повертають одне значення"""
        async with db_manager.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params or ())
                row = await cursor.fetchone()
                return row[0] if row else None
    
    @staticmethod
    async def execute_procedure(proc_name: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Виконання збережених процедур"""
        async with db_manager.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.callproc(proc_name, params or ())
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                rows = await cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
    
    @staticmethod
    def get_transaction():
        """Отримати транзакцію для виконання множинних операцій"""
        return db_manager.get_transaction()

    @staticmethod 
    async def execute_in_transaction(queries_with_params):
        """Виконати список запитів в одній транзакції"""
        async with db_manager.get_transaction() as cursor:
            for query, params in queries_with_params:
                await cursor.execute(query, params or ())
            return cursor.rowcount