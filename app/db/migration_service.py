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
        """Створити всі таблиці і зареєструвати їх в sys_data_types"""
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
            
            # Після створення кожної таблиці
            for table_name in results["created_tables"]:  # Тільки створені таблиці!
                try:
                    schema_info = self.schema_manager.get_schema_info_for_table(table_name)
                    await self._register_table_in_data_types(table_name, resolved_tables[table_name], schema_info)
                except Exception as e:
                    logger.warning(f"Could not register table {table_name} in sys_data_types: {e}")
            
            # Після транзакції створення, але перед реєстрацією:
            if 'sys_data_types' not in results["created_tables"] and 'sys_data_types' not in results["skipped_tables"]:
                # logger.warning("sys_data_types was not created - skipping table registration")
                return results
            
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
    
    async def _register_table_in_data_types(self, table_name: str, table_def: dict, schema_info: dict):
        """Register table in sys_data_types after creation"""
        
        # Check if registration is needed
        if not self._should_register_table(table_name):
            return
        
        # Check if already registered
        if await self._is_table_registered(table_name):
            return
        
        # Extract type_name from table description (NEW LOCATION)
        table_description = table_def.get('description', '')  # ← Змінити тут
        type_name = self._extract_type_name_from_description(table_description)
        
        # Determine supports_mapping
        supports_mapping = self._table_supports_mapping(table_name)
        
        # Insert record
        await self._insert_data_type_record(table_name, type_name, supports_mapping)
    
    def _should_register_table(self, table_name: str) -> bool:
        """Визначити чи потрібно реєструвати таблицю"""
        return True
        
        # # Включаємо бізнес таблиці
        # include_prefixes = ['cat_', 'doc_', 'trn_']
        
        # # Виключаємо системні та службові
        # exclude_prefixes = ['sys_', 'temp_', 'migration_']
        # exclude_tables = ['parent_tables', 'schema_versions']
        
        # if table_name in exclude_tables:
        #     return False
        
        # for prefix in exclude_prefixes:
        #     if table_name.startswith(prefix):
        #         return False
        
        # for prefix in include_prefixes:
        #     if table_name.startswith(prefix):
        #         return True
        
        # return False
    
    def _extract_type_name_from_description(self, description: str) -> str:
        """Витягнути назву типу з description"""
        
        if not description:
            return "Unknown Type"
        
        # Якщо є дефіс - взяти частину до дефіса
        if ' - ' in description:
            type_name = description.split(' - ')[0].strip()
        else:
            type_name = description.strip()
        
        # Обмежити довжину
        return type_name[:100] if type_name else "Unknown Type"
    
    async def _is_table_registered(self, table_name: str) -> bool:
        """Check if table is already registered in sys_data_types"""
        try:
            query = "SELECT COUNT(*) FROM sys_data_types WHERE table_name = ?"
            result = await DatabaseService.execute_scalar(query, (table_name,))
            return result > 0
        except Exception as e:
            # If sys_data_types table doesn't exist yet, consider as not registered
            logger.debug(f"Table sys_data_types might not exist yet: {e}")
            return False
    
    def _table_supports_mapping(self, table_name: str) -> bool:
        """Determine if table supports external mapping"""
        
        # System tables don't support mapping
        if table_name.startswith('sys_'):
            return False
        
        # Parent tables don't support mapping
        if 'parent' in table_name.lower():
            return False
        
        # Business tables support mapping
        business_prefixes = ['cat_', 'doc_', 'trn_']
        for prefix in business_prefixes:
            if table_name.startswith(prefix):
                return True
        
        return False
    
    async def _insert_data_type_record(self, table_name: str, type_name: str, supports_mapping: bool):
        """Insert record into sys_data_types"""
        try:
            query = """
            INSERT INTO sys_data_types (type_name, table_name, is_active, supports_mapping, created_at)
            VALUES (?, ?, 1, ?, GETDATE())
            """
            await DatabaseService.execute_non_query(query, (type_name, table_name, supports_mapping))
            logger.info(f"Registered table {table_name} as '{type_name}' in sys_data_types")
        except Exception as e:
            logger.warning(f"Failed to register table {table_name} in sys_data_types: {e}")
        
        # # Замінити перевірку на:
        # try:
        #     # Перевірити чи sys_data_types існує в БД (а не в результатах міграції)
        #     exists_query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'sys_data_types'"
        #     sys_data_types_exists = await DatabaseService.execute_scalar(exists_query)
            
        #     if sys_data_types_exists == 0:
        #         logger.warning("sys_data_types table does not exist - skipping table registration")
        #         return results
                
        # except Exception as e:
        #     logger.warning(f"Could not check sys_data_types existence: {e}")
        #     return results