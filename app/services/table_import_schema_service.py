# app/services/table_import_schema_service.py
from typing import Dict, List, Any, Optional
import logging
from app.db.schema_manager import SchemaManager

logger = logging.getLogger(__name__)

class TableImportSchemaService:
    """Service for working with table schemas for import operations"""
    
    def __init__(self):
        self.schema_manager = SchemaManager()
        self._schemas_loaded = False
    
    def _ensure_schemas_loaded(self):
        """Ensure schemas are loaded"""
        if not self._schemas_loaded:
            self.schema_manager.load_all_schemas()
            self._schemas_loaded = True
    
    def get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get schema for specific table"""
        self._ensure_schemas_loaded()
        
        all_tables = self.schema_manager.get_all_tables()
        return all_tables.get(table_name)
    
    def get_table_columns(self, table_name: str) -> Dict[str, Dict[str, Any]]:
        """Get columns definition for table"""
        schema = self.get_table_schema(table_name)
        if not schema:
            return {}
        
        return schema.get('columns', {})
    
    def get_required_columns(self, table_name: str) -> List[str]:
        """Get list of required (non-nullable) columns"""
        columns = self.get_table_columns(table_name)
        required = []
        
        for col_name, col_def in columns.items():
            if not col_def.get('nullable', True):
                required.append(col_name)
        
        return required
    
    def get_unique_columns(self, table_name: str) -> List[str]:
        """Get list of unique columns"""
        columns = self.get_table_columns(table_name)
        unique = []
        
        for col_name, col_def in columns.items():
            if col_def.get('unique', False):
                unique.append(col_name)
        
        return unique
    
    def get_primary_key_columns(self, table_name: str) -> List[str]:
        """Get primary key columns"""
        columns = self.get_table_columns(table_name)
        pk_columns = []
        
        for col_name, col_def in columns.items():
            if col_def.get('primary_key', False):
                pk_columns.append(col_name)
        
        return pk_columns
    
    def get_foreign_key_columns(self, table_name: str) -> Dict[str, str]:
        """Get foreign key columns and their references"""
        columns = self.get_table_columns(table_name)
        fk_columns = {}
        
        for col_name, col_def in columns.items():
            if 'foreign_key' in col_def:
                fk_columns[col_name] = col_def['foreign_key']
        
        return fk_columns
    
    def validate_column_exists(self, table_name: str, column_name: str) -> bool:
        """Check if column exists in table schema"""
        columns = self.get_table_columns(table_name)
        return column_name in columns
    
    def get_column_type(self, table_name: str, column_name: str) -> Optional[str]:
        """Get column data type"""
        columns = self.get_table_columns(table_name)
        col_def = columns.get(column_name, {})
        return col_def.get('type')
    
    def is_column_nullable(self, table_name: str, column_name: str) -> bool:
        """Check if column allows NULL values"""
        columns = self.get_table_columns(table_name)
        col_def = columns.get(column_name, {})
        return col_def.get('nullable', True)
    
    def get_column_default(self, table_name: str, column_name: str) -> Any:
        """Get column default value"""
        columns = self.get_table_columns(table_name)
        col_def = columns.get(column_name, {})
        return col_def.get('default')
    
    def get_all_importable_tables(self) -> List[str]:
        """Get list of tables that can be imported to"""
        self._ensure_schemas_loaded()
        
        all_tables = self.schema_manager.get_all_tables()
        importable = []
        
        for table_name in all_tables.keys():
            # Exclude system and parent tables
            if not table_name.startswith('sys_') and 'parent' not in table_name.lower():
                importable.append(table_name)
        
        return sorted(importable)
    
    def get_table_import_info(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive import information for table"""
        schema = self.get_table_schema(table_name)
        if not schema:
            return {}
        
        return {
            'table_name': table_name,
            'description': schema.get('description', ''),
            'columns': self.get_table_columns(table_name),
            'required_columns': self.get_required_columns(table_name),
            'unique_columns': self.get_unique_columns(table_name),
            'primary_key_columns': self.get_primary_key_columns(table_name),
            'foreign_key_columns': self.get_foreign_key_columns(table_name),
            'total_columns': len(self.get_table_columns(table_name)),
            'parent_table': schema.get('parent'),
            'indexes': schema.get('indexes', [])
        }
    
    def validate_import_columns(self, table_name: str, column_mapping: Dict[str, str]) -> Dict[str, Any]:
        """Validate column mapping for import"""
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'missing_required': [],
            'invalid_columns': [],
            'suggestions': []
        }
        
        # Check if table exists
        if not self.get_table_schema(table_name):
            result['valid'] = False
            result['errors'].append(f"Table '{table_name}' not found in schema")
            return result
        
        # Get table info
        table_columns = self.get_table_columns(table_name)
        required_columns = self.get_required_columns(table_name)
        
        # Check mapped columns exist
        mapped_table_columns = list(column_mapping.values())
        for table_col in mapped_table_columns:
            if table_col and not self.validate_column_exists(table_name, table_col):
                result['invalid_columns'].append(table_col)
                result['errors'].append(f"Column '{table_col}' does not exist in table '{table_name}'")
        
        # Check required columns are mapped
        for required_col in required_columns:
            if required_col not in mapped_table_columns:
                result['missing_required'].append(required_col)
                result['errors'].append(f"Required column '{required_col}' is not mapped")
        
        # Generate suggestions for unmapped columns
        unmapped_table_columns = [col for col in table_columns.keys() if col not in mapped_table_columns]
        if unmapped_table_columns:
            result['suggestions'].append(f"Consider mapping these columns: {', '.join(unmapped_table_columns)}")
        
        if result['errors']:
            result['valid'] = False
        
        return result
    
    def get_column_validation_rules(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """Get validation rules for specific column"""
        columns = self.get_table_columns(table_name)
        col_def = columns.get(column_name, {})
        
        rules = {
            'required': not col_def.get('nullable', True),
            'type': col_def.get('type', 'NVARCHAR(255)'),
            'unique': col_def.get('unique', False),
            'foreign_key': col_def.get('foreign_key'),
            'default': col_def.get('default'),
            'max_length': None
        }
        
        # Extract max length from type if applicable
        if 'NVARCHAR' in rules['type'] and '(' in rules['type']:
            try:
                max_length_str = rules['type'].split('(')[1].split(')')[0]
                rules['max_length'] = int(max_length_str)
            except (ValueError, IndexError):
                pass
        
        return rules
    
    def suggest_column_mapping(self, table_name: str, excel_columns: List[str]) -> Dict[str, Optional[str]]:
        """Suggest column mapping between Excel and table columns"""
        table_columns = list(self.get_table_columns(table_name).keys())
        mapping = {}
        
        for excel_col in excel_columns:
            best_match = None
            excel_col_clean = excel_col.lower().replace('_', '').replace(' ', '').replace('-', '')
            
            for table_col in table_columns:
                table_col_clean = table_col.lower().replace('_', '').replace(' ', '').replace('-', '')
                
                # Exact match
                if excel_col_clean == table_col_clean:
                    best_match = table_col
                    break
                
                # Partial match
                if excel_col_clean in table_col_clean or table_col_clean in excel_col_clean:
                    if best_match is None:
                        best_match = table_col
                
                # Common synonyms
                synonyms = {
                    'id': ['identifier', 'key'],
                    'name': ['title', 'label', 'description'],
                    'code': ['shortcode', 'abbreviation'],
                    'created': ['date', 'time'],
                }
                
                for standard, aliases in synonyms.items():
                    if standard in table_col_clean:
                        for alias in aliases:
                            if alias in excel_col_clean:
                                if best_match is None:
                                    best_match = table_col
            
            mapping[excel_col] = best_match
        
        return mapping