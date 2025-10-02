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
        self.parent_tables: Dict[str, Dict] = {}
        self.core_schema: Dict = {}
        self.plugin_schemas: Dict[str, Dict] = {}
        self.resolved_tables: Dict[str, Dict] = {}
        self._loaded = False
    
    def load_all_schemas(self) -> None:
        """Завантажити всі схеми з файлів"""
        try:
            self.load_parent_tables()
            self.load_core_schema()
            self.load_plugin_schemas()
            self._resolve_all_tables()
            self._loaded = True
            logger.info("All schemas loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load schemas: {e}")
            raise
    
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
        # Спочатку core таблиці
        core_tables = self.core_schema.get('tables', {})
        for table_name, table_def in core_tables.items():
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
            col_sql = self.generate_column_definition(col_name, col_def)
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
    
    def generate_column_definition(self, column_name: str, column_def: Dict) -> str:
        """Згенерувати визначення колонки"""
        col_type = column_def.get('type', 'NVARCHAR(255)')
        parts = [f"{column_name} {col_type}"]
        
        # IDENTITY для auto_increment
        if column_def.get('auto_increment'):
            parts.append("IDENTITY(1,1)")
        
        # NULL/NOT NULL
        if column_def.get('nullable', True) is False:
            parts.append("NOT NULL")
        else:
            parts.append("NULL")
        
        # DEFAULT value
        if 'default' in column_def:
            default_val = column_def['default']
            parts.append(f"DEFAULT {default_val}")
        
        # UNIQUE constraint
        if column_def.get('unique'):
            parts.append("UNIQUE")
        
        return ' '.join(parts)
    
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
        """Отримати порядок створення таблиць (топологічне сортування)"""
        dependencies = self.get_table_dependencies()
        ordered = []
        remaining = set(self.resolved_tables.keys())
        
        while remaining:
            # Знайдемо таблиці без залежностей
            no_deps = []
            for table in remaining:
                table_deps = [dep for dep in dependencies.get(table, []) if dep in remaining]
                if not table_deps:
                    no_deps.append(table)
            
            if not no_deps:
                # Циклічні залежності - додаємо що залишилося
                no_deps = list(remaining)
                logger.warning("Possible circular dependencies detected")
            
            ordered.extend(sorted(no_deps))
            remaining -= set(no_deps)
        
        return ordered