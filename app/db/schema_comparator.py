from typing import Dict, List, Tuple, Any
import logging
from app.services.database_service import DatabaseService

logger = logging.getLogger(__name__)

class SchemaComparator:
    """Порівняння схеми БД з YAML описом"""
    
    async def get_table_structure(self, table_name: str) -> Dict[str, Any]:
        """Отримати структуру таблиці з БД"""
        query = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as IS_PRIMARY_KEY,
            CASE WHEN fk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as IS_FOREIGN_KEY
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
            SELECT ku.COLUMN_NAME, ku.TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME AND c.TABLE_NAME = pk.TABLE_NAME
        LEFT JOIN (
            SELECT ku.COLUMN_NAME, ku.TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
        ) fk ON c.COLUMN_NAME = fk.COLUMN_NAME AND c.TABLE_NAME = fk.TABLE_NAME
        WHERE c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        """
        
        result = await DatabaseService.execute_query(query, (table_name,))
        
        columns = {}
        for row in result:
            # Нормалізуємо назву колонки до snake_case
            col_name = self._normalize_column_name(row['COLUMN_NAME'])
            columns[col_name] = {
                'type': self._convert_sql_type_to_yaml(row),
                'nullable': row['IS_NULLABLE'] == 'YES',
                'default': row['COLUMN_DEFAULT'],
                'primary_key': bool(row['IS_PRIMARY_KEY']),
                'foreign_key': bool(row['IS_FOREIGN_KEY'])
            }
        
        return {'columns': columns}
    
    def _convert_sql_type_to_yaml(self, row: Dict) -> str:
        """Конвертувати SQL тип в YAML формат"""
        data_type = row['DATA_TYPE'].upper()
        
        if data_type == 'NVARCHAR':
            length = row['CHARACTER_MAXIMUM_LENGTH']
            if length == -1:
                return 'NVARCHAR(MAX)'
            return f'NVARCHAR({length})'
        elif data_type == 'DECIMAL':
            precision = row['NUMERIC_PRECISION']
            scale = row['NUMERIC_SCALE']
            return f'DECIMAL({precision},{scale})'
        elif data_type == 'UNIQUEIDENTIFIER':
            return 'UNIQUEIDENTIFIER'
        elif data_type == 'DATETIME2':
            return 'DATETIME2'
        elif data_type == 'BIT':
            return 'BIT'
        elif data_type == 'INT':
            return 'INT'
        elif data_type == 'BIGINT':
            return 'BIGINT'
        elif data_type == 'NTEXT':
            return 'NTEXT'
        else:
            return data_type
    
    def compare_table_structures(self, db_structure: Dict, yaml_structure: Dict) -> Dict[str, List]:
        """Порівняти структури таблиць"""
        differences = {
            'add_columns': [],
            'modify_columns': [],
            'drop_columns': []
        }
        
        db_columns = db_structure.get('columns', {})
        yaml_columns = yaml_structure.get('columns', {})
        
        # Колонки для додавання
        for col_name, col_def in yaml_columns.items():
            if col_name not in db_columns:
                differences['add_columns'].append((col_name, col_def))
        
        # Колонки для зміни
        for col_name, yaml_def in yaml_columns.items():
            if col_name in db_columns:
                db_def = db_columns[col_name]
                if self._columns_different(db_def, yaml_def):
                    differences['modify_columns'].append((col_name, db_def, yaml_def))
        
        # Колонки для видалення
        for col_name in db_columns:
            if col_name not in yaml_columns:
                differences['drop_columns'].append(col_name)
        
        return differences
    
    def _columns_different(self, db_def: Dict, yaml_def: Dict) -> bool:
        """Перевірити чи різняться колонки"""
        # Ігнорувати PRIMARY KEY колонки
        if db_def.get('primary_key', False) or yaml_def.get('primary_key', False):
            return False  # Не порівнювати PK колонки
        
        # Порівняти тип
        if db_def['type'] != yaml_def.get('type', 'NVARCHAR(255)'):
            return True
        
        # Порівняти nullable (PK завжди NOT NULL)
        if not yaml_def.get('primary_key', False):
            if db_def['nullable'] != yaml_def.get('nullable', True):
                return True
        
        return False
    
    def _normalize_column_name(self, db_column_name: str) -> str:
        """Конвертувати PascalCase в snake_case"""
        import re
        # PascalCase -> snake_case
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', db_column_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()