import yaml
import os
from typing import Dict, List, Any, Optional
from pathlib import Path
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)

class SchemaManager:
    """Менеджер для роботи зі схемами бази даних"""
    
    def __init__(self):
        self.schemas_dir = Path(settings.DB_SCHEMAS_DIR)  # Перетворити в Path
        self.parent_tables = {}
        self.tables = {}
        self.core_schema: Dict = {}
        self.plugin_schemas: Dict[str, Dict] = {}
        self.resolved_tables: Dict[str, Dict] = {}
        self._loaded = False
    
    def load_all_schemas(self):
        """Завантажити всі схеми з усіх тек"""
        schema_files = self.discover_schema_files()
        
        # Завантажити parent схеми
        for parent_file in schema_files.get('parents', []):
            self._load_parent_schema(parent_file)
        
        # Завантажити схеми таблиць (всі теки окрім parents)
        for category in schema_files:
            if category == 'parents':
                continue  # parents вже завантажені вище
            for schema_file in schema_files[category]:
                self._load_table_schema(schema_file)
        
        # ДОДАТИ: Розв'язати наслідування
        self._resolve_all_tables()
        self._loaded = True
    
    def load_parent_tables(self) -> Dict:
        """Завантажити батьківські таблиці"""
        file_path = settings.DB_PARENT_CORE_SCHEMA
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                self.parent_tables = data.get('parent_tables', {})
                logger.info(f"Loaded {len(self.parent_tables)} parent tables")
        else:
            logger.warning(f"Parent tables file not found: {file_path}")
        return self.parent_tables
    
    def load_core_schema(self) -> Dict:
        """Завантажити основну схему"""
        file_path = settings.DB_CORE_SCHEMA
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                self.core_schema = yaml.safe_load(f)
                logger.info(f"Loaded core schema with {len(self.core_schema.get('tables', {}))} tables")
        else:
            logger.warning(f"Core schema file not found: {file_path}")
        return self.core_schema
    
    def load_plugin_schemas(self) -> Dict:
        """Завантажити схеми плагінів"""
        plugins_dir = settings.DB_PLUGINS_DIR
        enabled_plugins = settings.ENABLED_PLUGINS
        
        if not os.path.exists(plugins_dir):
            logger.info(f"Plugins directory not found: {plugins_dir}")
            return self.plugin_schemas
        
        for plugin_name in enabled_plugins:
            plugin_schema_path = os.path.join(plugins_dir, plugin_name, "schema.yaml")
            if os.path.exists(plugin_schema_path):
                try:
                    with open(plugin_schema_path, 'r', encoding='utf-8') as f:
                        schema = yaml.safe_load(f)
                        self.plugin_schemas[plugin_name] = schema
                        logger.info(f"Loaded plugin schema: {plugin_name}")
                except Exception as e:
                    logger.error(f"Failed to load plugin schema {plugin_name}: {e}")
            else:
                logger.warning(f"Plugin schema not found: {plugin_schema_path}")
        
        return self.plugin_schemas
    
    def _resolve_all_tables(self) -> None:
        """Розв'язати наслідування для всіх таблиць"""
        # Тепер таблиці в self.tables замість self.core_schema
        for table_name, table_def in self.tables.items():
            self.resolved_tables[table_name] = self.resolve_table_inheritance(table_name, table_def)
        
        # Потім plugin таблиці
        for plugin_name, plugin_schema in self.plugin_schemas.items():
            plugin_tables = plugin_schema.get('tables', {})
            for table_name, table_def in plugin_tables.items():
                # Додаємо префікс плагіна до назви таблиці якщо конфлікт
                full_table_name = f"{plugin_name}_{table_name}" if table_name in self.resolved_tables else table_name
                self.resolved_tables[full_table_name] = self.resolve_table_inheritance(table_name, table_def)
    
    def resolve_table_inheritance(self, table_name: str, table_def: Dict) -> Dict:
        """Розв'язати наслідування для таблиці"""
        resolved_table = table_def.copy()
        
        # Якщо є parent, об'єднуємо колонки
        if 'parent' in table_def:
            parent_name = table_def['parent']
            if parent_name in self.parent_tables:
                parent_columns = self.parent_tables[parent_name].copy()
                table_columns = table_def.get('columns', {})
                
                # Об'єднуємо колонки (дочірні перевизначають батьківські)
                merged_columns = self.merge_parent_columns(parent_columns, table_columns)
                resolved_table['columns'] = merged_columns
                
                # Видаляємо parent з resolved таблиці
                resolved_table.pop('parent', None)
            else:
                logger.error(f"Parent table '{parent_name}' not found for table '{table_name}'")
                raise ValueError(f"Parent table '{parent_name}' not found")
        
        return resolved_table
    
    def merge_parent_columns(self, parent_columns: Dict, table_columns: Dict) -> Dict:
        """Об'єднати колонки батьківської та дочірньої таблиць"""
        merged = parent_columns.copy()
        merged.update(table_columns)
        return merged
    
    def generate_create_table_sql(self, table_name: str, table_def: Dict) -> str:
        """Згенерувати SQL CREATE TABLE"""
        columns = table_def.get('columns', {})
        if not columns:
            raise ValueError(f"No columns defined for table {table_name}")
        
        # Генеруємо колонки
        column_definitions = []
        primary_keys = []
        
        for col_name, col_def in columns.items():
            col_sql = self._generate_column_definition(col_name, col_def)
            column_definitions.append(col_sql)
            
            if col_def.get('primary_key'):
                primary_keys.append(col_name)
        
        # PRIMARY KEY constraint
        if primary_keys:
            pk_constraint = f"CONSTRAINT PK_{table_name} PRIMARY KEY ({', '.join(primary_keys)})"
            column_definitions.append(pk_constraint)
        
        columns_sql = ',\n    '.join(column_definitions)
        
        create_sql = f"""CREATE TABLE {table_name} (
    {columns_sql}
);"""
        
        return create_sql
    
    def _generate_column_definition(self, column_name: str, column_def: dict) -> str:
        """Генерує SQL визначення колонки"""
        column_type = column_def.get('type', 'NVARCHAR(255)')
        
        # IDENTITY для auto_increment
        if column_def.get('auto_increment', False):
            column_type += " IDENTITY(1,1)"
        
        # Primary key колонки завжди NOT NULL
        if column_def.get('primary_key', False):
            nullable = False
        else:
            nullable = column_def.get('nullable', True)
        
        # Додати підтримку ROWVERSION
        if column_type.upper() == "ROWVERSION":
            column_type = "rowversion"
       
        # NULL/NOT NULL
        null_sql = "NOT NULL" if not nullable else "NULL"
        
        # DEFAULT value (не для IDENTITY колонок)
        if 'default' in column_def and not column_def.get('auto_increment', False):
            default_val = column_def['default']
            default_sql = f"DEFAULT {default_val}"
        else:
            default_sql = ""
        
        # UNIQUE constraint
        unique_sql = "UNIQUE" if column_def.get('unique') else ""
        
        # Об'єднуємо частини визначення колонки
        col_definition = f"{column_name} {column_type} {null_sql} {default_sql} {unique_sql}".strip()
        
        return col_definition
    
    def generate_indexes_sql(self, table_name: str, indexes: List[Dict]) -> List[str]:
        """Згенерувати SQL для індексів"""
        index_sqls = []
        
        for index in indexes:
            index_name = index.get('name', f"IX_{table_name}_{'_'.join(index['columns'])}")
            columns = ', '.join(index['columns'])
            unique = "UNIQUE " if index.get('unique') else ""
            
            sql = f"CREATE {unique}INDEX {index_name} ON {table_name} ({columns});"
            index_sqls.append(sql)
        
        return index_sqls
    
    def validate_foreign_keys(self) -> List[str]:
        """Валідувати foreign keys"""
        errors = []
        
        for table_name, table_def in self.resolved_tables.items():
            columns = table_def.get('columns', {})
            for col_name, col_def in columns.items():
                if 'foreign_key' in col_def:
                    fk_ref = col_def['foreign_key']
                    if '.' in fk_ref:
                        ref_table, ref_column = fk_ref.split('.', 1)
                        if ref_table not in self.resolved_tables:
                            errors.append(f"Foreign key in {table_name}.{col_name} references non-existent table: {ref_table}")
        
        return errors
    
    def get_table_dependencies(self) -> Dict[str, List[str]]:
        """Отримати залежності між таблицями"""
        dependencies = {}
        
        for table_name, table_def in self.resolved_tables.items():
            table_deps = []
            columns = table_def.get('columns', {})
            
            for col_name, col_def in columns.items():
                if 'foreign_key' in col_def:
                    fk_ref = col_def['foreign_key']
                    if '.' in fk_ref:
                        ref_table = fk_ref.split('.')[0]
                        if ref_table != table_name:  # Не самопосилання
                            table_deps.append(ref_table)
            
            dependencies[table_name] = table_deps
        
        return dependencies
    
    def get_all_tables(self) -> Dict[str, Dict]:
        """Отримати всі розв'язані таблиці"""
        if not self._loaded:
            self.load_all_schemas()
        return self.resolved_tables
    
    def get_table_creation_order(self) -> List[str]:
        """Get table creation order (topological sorting with sys_data_types first)"""
        dependencies = self.get_table_dependencies()
        ordered = []
        remaining = set(self.resolved_tables.keys())
        
        # Always create sys_data_types first if it exists
        if 'sys_data_types' in remaining:
            ordered.append('sys_data_types')
            remaining.remove('sys_data_types')
        
        # Then create other system tables
        system_tables = [t for t in remaining if t.startswith('sys_')]
        if system_tables:
            ordered.extend(sorted(system_tables))
            remaining -= set(system_tables)
        
        # Then use regular dependency resolution for business tables
        while remaining:
            # Find tables without dependencies
            no_deps = []
            for table in remaining:
                table_deps = [dep for dep in dependencies.get(table, []) if dep in remaining]
                if not table_deps:
                    no_deps.append(table)
            
            if not no_deps:
                # Circular dependencies - add remaining
                no_deps = list(remaining)
                logger.warning("Possible circular dependencies detected")
            
            ordered.extend(sorted(no_deps))
            remaining -= set(no_deps)
        
        return ordered
    
    def discover_schema_files(self) -> Dict[str, List[Path]]:
        """Discover all schema files in subdirectories (including nested)"""
        schema_files = {}
        
        for category_path in self.schemas_dir.iterdir():
            if category_path.is_dir():
                category_name = category_path.name
                schema_files[category_name] = []
                
                # Рекурсивний пошук в підпапках
                for schema_file in category_path.glob("**/*.yaml"):
                    schema_files[category_name].append(schema_file)
        
        return schema_files
    
    def _load_parent_schema(self, file_path: str):
        """Завантажити parent схему з файлу"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = yaml.safe_load(file)
                if data and 'parent_tables' in data:
                    parent_tables = data['parent_tables']
                    if parent_tables:  # Перевірка на None
                        self.parent_tables.update(parent_tables)
                        logger.info(f"Loaded {len(parent_tables)} parent tables from {file_path}")
                    else:
                        logger.warning(f"Empty parent_tables in {file_path}")
                else:
                    logger.warning(f"No parent_tables found in {file_path}")
        except Exception as e:
            logger.error(f"Failed to load parent schema from {file_path}: {e}")

    def _load_table_schema(self, file_path: str):
        """Завантажити схему таблиць з файлу"""
        with open(file_path, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file)
            if 'tables' in data:
                self.tables.update(data['tables'])
    
    def generate_alter_commands(self, table_name: str, differences: Dict[str, List]) -> List[str]:
        """Згенерувати команди ALTER TABLE"""
        commands = []
        
        # Логування для діагностики
        logger.info(f"Generating ALTER commands for table {table_name}")
        logger.info(f"Add columns: {[col[0] for col in differences.get('add_columns', [])]}")
        logger.info(f"Modify columns: {[col[0] for col in differences.get('modify_columns', [])]}")
        logger.info(f"Drop columns: {differences.get('drop_columns', [])}")
        
        # Додати колонки
        for col_name, col_def in differences.get('add_columns', []):
            cmd = self._generate_add_column(table_name, col_name, col_def)
            logger.info(f"ADD COLUMN: {cmd}")
            commands.append(cmd)
        
        # Змінити колонки
        for col_name, col_def in differences.get('modify_columns', []):
            cmd = self._generate_modify_column(table_name, col_name, col_def)
            logger.info(f"MODIFY COLUMN: {cmd}")
            commands.append(cmd)
        
        # Видалити колонки
        for col_name in differences.get('drop_columns', []):
            cmd = self._generate_drop_column(table_name, col_name)
            logger.info(f"DROP COLUMN: {cmd}")
            commands.append(cmd)
        
        return commands
    
    def _generate_add_column(self, table_name: str, column_name: str, column_def: Dict) -> str:
        """Генерує SQL команду для додавання колонки"""
        column_type = column_def.get('type', 'NVARCHAR(255)')
        nullable = "NULL" if column_def.get('nullable', True) else "NOT NULL"
        default = f"DEFAULT {column_def['default']}" if 'default' in column_def else ""
        unique = "UNIQUE" if column_def.get('unique') else ""
        
        add_column_sql = f"ALTER TABLE {table_name} ADD {column_name} {column_type} {nullable} {default} {unique};"
        
        return add_column_sql
    
    def _generate_modify_column(self, table_name: str, column_name: str, column_def: Dict) -> str:
        """Генерує SQL команду для зміни колонки"""
        column_type = column_def.get('type', 'NVARCHAR(255)')
        nullable = "NULL" if column_def.get('nullable', True) else "NOT NULL"
        default = f"DEFAULT {column_def['default']}" if 'default' in column_def else ""
        unique = "UNIQUE" if column_def.get('unique') else ""
        
        modify_column_sql = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} {column_type} {nullable} {default} {unique};"
        
        return modify_column_sql
    
    def _generate_drop_column(self, table_name: str, column_name: str) -> str:
        """Генерує SQL команду для видалення колонки"""
        drop_column_sql = f"ALTER TABLE {table_name} DROP COLUMN {column_name};"
        return drop_column_sql
    
    def get_schema_info_for_table(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for specific table including description"""
        
        # Search through loaded schema files to find table definition
        schema_files = self.discover_schema_files()
        
        for category in schema_files:
            if category == 'parents':
                continue
            
            for schema_file in schema_files[category]:
                try:
                    with open(schema_file, 'r', encoding='utf-8') as file:
                        data = yaml.safe_load(file)
                        
                        # Check if this file contains our table
                        tables = data.get('tables', {})
                        if table_name in tables:
                            return {
                                'description': data.get('description', ''),
                                'version': data.get('version', '1.0.0'),
                                'table_definition': tables[table_name]
                            }
                except Exception as e:
                    logger.warning(f"Could not read schema file {schema_file}: {e}")
                    continue
        
        # If not found, return empty info
        logger.warning(f"Schema info not found for table: {table_name}")
        return {
            'description': '',
            'version': '1.0.0',
            'table_definition': {}
        }