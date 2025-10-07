import logging
from typing import List, Dict, Any, Optional
from app.db.schema_manager import SchemaManager
from app.services.database_service import DatabaseService
from app.db.schema_comparator import SchemaComparator
from app.db.alter_table_generator import AlterTableGenerator

logger = logging.getLogger(__name__)

class MigrationService:
    """Сервіс для міграцій бази даних"""
    
    def __init__(self):
        self.schema_manager = SchemaManager()
        
    async def create_all_tables(self) -> Dict[str, Any]:
        """Створити всі таблиці згідно схеми в транзакції"""
        results = {
            "created_tables": [],
            "errors": [],
            "skipped_tables": []
        }
        
        try:
            # Завантажуємо схеми
            self.schema_manager.load_all_schemas()
            
            # Отримуємо порядок створення таблиць
            creation_order = self.schema_manager.get_table_creation_order()
            resolved_tables = self.schema_manager.get_all_tables()
            
            # Виконуємо в одній транзакції
            async with DatabaseService.get_transaction() as cursor:
                for table_name in creation_order:
                    try:
                        # Перевіряємо чи таблиця вже існує
                        exists = await self._table_exists_in_transaction(cursor, table_name)
                        if exists:
                            results["skipped_tables"].append(f"{table_name} (already exists)")
                            continue
                        
                        # Генеруємо SQL
                        table_def = resolved_tables[table_name]
                        create_sql = self.schema_manager.generate_create_table_sql(table_name, table_def)
                        
                        # Виконуємо CREATE TABLE в транзакції
                        await cursor.execute(create_sql)
                        logger.info(f"Created table: {table_name}")
                        results["created_tables"].append(table_name)
                        
                        # Створюємо індекси в тій же транзакції
                        indexes = table_def.get('indexes', [])
                        if indexes:
                            index_sqls = self.schema_manager.generate_indexes_sql(table_name, indexes)
                            for index_sql in index_sqls:
                                await cursor.execute(index_sql)
                            logger.info(f"Created {len(index_sqls)} indexes for {table_name}")
                    
                    except Exception as e:
                        # При помилці транзакція автоматично відкатиться
                        error_msg = f"Failed to create table {table_name}: {str(e)}"
                        logger.error(error_msg)
                        results["errors"].append(error_msg)
                        # Виходимо з циклу при помилці
                        raise
            
            logger.info("All tables created successfully in transaction")
            
        except Exception as e:
            logger.error(f"Migration transaction failed: {e}")
            results["errors"].append(f"Transaction error: {str(e)}")
        
        return results
    
    async def _table_exists(self, table_name: str) -> bool:
        """Перевірити чи існує таблиця"""
        query = """
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE LOWER(TABLE_NAME) = LOWER(?) AND TABLE_TYPE = 'BASE TABLE'
        """
        result = await DatabaseService.execute_scalar(query, (table_name,))
        return result > 0
    
    async def _table_exists_in_transaction(self, cursor, table_name: str) -> bool:
        """Перевірити чи існує таблиця в рамках транзакції"""
        query = """
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE LOWER(TABLE_NAME) = LOWER(?) AND TABLE_TYPE = 'BASE TABLE'
        """
        await cursor.execute(query, (table_name,))
        result = await cursor.fetchone()
        return result[0] > 0
    
    async def get_database_info(self) -> Dict[str, Any]:
        """Отримати інформацію про поточну БД"""
        info = {
            "existing_tables": [],
            "schema_tables": [],
            "missing_tables": [],
            "validation_errors": []
        }
        
        try:
            # Існуючі таблиці в БД
            existing_query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """
            existing_tables_result = await DatabaseService.execute_query(existing_query)
            info["existing_tables"] = [row["TABLE_NAME"] for row in existing_tables_result]
            
            # Додаємо версії в нижньому регістрі для порівняння
            info["existing_tables_lower"] = [t.lower() for t in info["existing_tables"]]
            
            # Таблиці зі схеми
            self.schema_manager.load_all_schemas()
            schema_tables = list(self.schema_manager.get_all_tables().keys())
            info["schema_tables"] = schema_tables
            
            # Відсутні таблиці (регістронезалежне порівняння)
            info["missing_tables"] = [
                table for table in schema_tables 
                if table.lower() not in info["existing_tables_lower"]
            ]
            
            # Валідація foreign keys
            validation_errors = self.schema_manager.validate_foreign_keys()
            info["validation_errors"] = validation_errors
            
        except Exception as e:
            logger.error(f"Failed to get database info: {e}")
            info["validation_errors"].append(f"Error getting DB info: {str(e)}")
        
        return info
    
    async def update_existing_tables(self, dry_run: bool = False) -> Dict[str, Any]:
        """Оновити існуючі таблиці згідно схеми"""
        results = {
            "updated_tables": [],
            "changes_planned": [],
            "errors": []
        }
        
        try:
            self.schema_manager.load_all_schemas()
            resolved_tables = self.schema_manager.get_all_tables()
            
            comparator = SchemaComparator()
            generator = AlterTableGenerator()
            
            for table_name, yaml_structure in resolved_tables.items():
                try:
                    # Перевірити чи таблиця існує
                    if not await self._table_exists(table_name):
                        continue
                    
                    # Отримати структуру з БД
                    db_structure = await comparator.get_table_structure(table_name)
                    
                    # Порівняти структури
                    differences = comparator.compare_table_structures(db_structure, yaml_structure)
                    
                    # Якщо є різниці
                    if any(differences.values()):
                        # Згенерувати команди
                        alter_commands = generator.generate_alter_commands(table_name, differences)
                        
                        if dry_run:
                            results["changes_planned"].append({
                                "table": table_name,
                                "commands": alter_commands,
                                "differences": differences
                            })
                        else:
                            # Виконати команди
                            for cmd in alter_commands:
                                await DatabaseService.execute_non_query(cmd)
                                logger.info(f"Executed: {cmd}")
                            
                            results["updated_tables"].append(table_name)
                
                except Exception as e:
                    error_msg = f"Failed to update table {table_name}: {str(e)}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
        
        except Exception as e:
            logger.error(f"Update tables failed: {e}")
            results["errors"].append(f"General error: {str(e)}")
        
        return results