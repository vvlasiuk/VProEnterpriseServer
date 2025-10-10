# app/services/enumeration_service.py
from typing import Dict, List, Any, Optional
from pathlib import Path
import yaml
import logging
from app.core.config import settings
from app.services.database_service import DatabaseService

logger = logging.getLogger(__name__)

class EnumerationService:
    """Service for managing system enumerations"""
    
    def __init__(self):
        self.enumerations_dir = Path(settings.DB_ENUMERATIONS_DIR)
    
    def discover_enumeration_files(self) -> List[Path]:
        """Discover all enumeration YAML files"""
        if not self.enumerations_dir.exists():
            logger.warning(f"Enumerations directory not found: {self.enumerations_dir}")
            return []
        
        files = list(self.enumerations_dir.glob("*.yaml"))
        logger.info(f"Found {len(files)} enumeration files")
        return files
    
    def load_enumeration_file(self, file_path: Path) -> Dict[str, Any]:
        """Load and parse enumeration YAML file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = yaml.safe_load(file)
                
            # Extract enumeration name from file structure
            enum_name = list(data.keys())[0] if data else None
            if not enum_name:
                raise ValueError(f"No enumeration found in {file_path}")
            
            enum_data = data[enum_name]
            enum_data['enum_name'] = enum_name
            
            return enum_data
            
        except Exception as e:
            logger.error(f"Failed to load enumeration file {file_path}: {e}")
            raise
    
    def load_all_enumerations(self) -> Dict[str, Dict[str, Any]]:
        """Load all enumeration files"""
        enumerations = {}
        
        for file_path in self.discover_enumeration_files():
            try:
                enum_data = self.load_enumeration_file(file_path)
                type_code = enum_data.get('type_code')
                
                if type_code:
                    enumerations[type_code] = enum_data
                    logger.debug(f"Loaded enumeration: {type_code}")
                    
            except Exception as e:
                logger.error(f"Skipping file {file_path}: {e}")
                continue
        
        return enumerations
    
    async def get_existing_enumeration_types(self) -> Dict[str, Dict[str, Any]]:
        """Get existing enumeration types from database"""
        query = """
        SELECT id, type_code, description
        FROM sys_enumeration_type
        """
        
        try:
            rows = await DatabaseService.execute_query(query)
            types = {}
            
            for row in rows:
                types[row['type_code']] = {
                    'id': row['id'],
                    'type_code': row['type_code'],
                    'description': row['description']
                }
            
            return types
            
        except Exception as e:
            logger.error(f"Failed to get enumeration types: {e}")
            # If table doesn't exist, return empty dict
            return {}

    async def get_all_existing_enumeration_values(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Get all existing enumeration values grouped by type_code"""
        query = """
        SELECT 
            t.type_code,
            v.id, v.value_code, v.value_name, v.numeric_value, v.sort_order
        FROM sys_enumeration_type t
        LEFT JOIN sys_enumeration_value v ON t.id = v.enumeration_type_id
        """
        
        try:
            rows = await DatabaseService.execute_query(query)
            result = {}
            
            for row in rows:
                type_code = row['type_code']
                
                if type_code not in result:
                    result[type_code] = {}
                
                # Skip if no values (LEFT JOIN can return NULL)
                if row['value_code'] is not None:
                    result[type_code][row['value_code']] = {
                        'id': row['id'],
                        'value_code': row['value_code'],
                        'value_name': row['value_name'],
                        'numeric_value': row['numeric_value'],
                        'sort_order': row['sort_order']
                    }
        
            return result
            
        except Exception as e:
            logger.error(f"Failed to get enumeration values: {e}")
            # If table doesn't exist, return empty dict
            return {}
    
    async def sync_enumerations(self, file_enumerations: Dict[str, Dict[str, Any]], 
                               db_types: Dict[str, Dict[str, Any]], 
                               db_values: Dict[str, Dict[str, Dict[str, Any]]], 
                               dry_run: bool = False) -> Dict[str, Any]:
        """Synchronize enumeration data from files to database"""
        
        results = {
            'types_added': [],
            'types_updated': [],
            'values_added': {},
            'values_updated': {},
            'errors': []
        }
        
        try:
            from app.db.database import db_manager
            
            if not dry_run:
                async with db_manager.get_transaction() as cursor:
                    await self._sync_types_and_values(cursor, file_enumerations, db_types, db_values, results)
            else:
                # Dry run - just compare without changes
                await self._compare_types_and_values(file_enumerations, db_types, db_values, results)
            
        except Exception as e:
            error_msg = f"Synchronization failed: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
        
        return results
    
    async def _sync_types_and_values(self, cursor, file_enumerations, db_types, db_values, results):
        """Perform actual synchronization with database changes"""
        
        for type_code, file_enum in file_enumerations.items():
            try:
                # Sync enumeration type
                type_id = await self._sync_enumeration_type(cursor, type_code, file_enum, db_types, results)
                
                if type_id:
                    # Sync enumeration values
                    await self._sync_enumeration_values(cursor, type_id, type_code, file_enum, db_values, results)
                    
            except Exception as e:
                error_msg = f"Failed to sync {type_code}: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
    
    async def _compare_types_and_values(self, file_enumerations, db_types, db_values, results):
        """Compare without making changes (dry run)"""
        
        for type_code, file_enum in file_enumerations.items():
            # Check if type needs to be added or updated
            if type_code not in db_types:
                results['types_added'].append(type_code)
            else:
                # Check if type needs update
                if self._type_needs_update(file_enum, db_types[type_code]):
                    results['types_updated'].append(type_code)
            
            # Check values
            file_values = file_enum.get('values', [])
            existing_values = db_values.get(type_code, {})
            
            added_values = []
            updated_values = []
            
            for file_value in file_values:
                value_code = file_value['value_code']
                
                if value_code not in existing_values:
                    added_values.append(value_code)
                else:
                    # Check if value needs update
                    if self._value_needs_update(file_value, existing_values[value_code]):
                        updated_values.append(value_code)
            
            if added_values:
                results['values_added'][type_code] = added_values
            if updated_values:
                results['values_updated'][type_code] = updated_values
    
    async def _sync_enumeration_type(self, cursor, type_code, file_enum, db_types, results):
        """Sync single enumeration type"""
        
        if type_code not in db_types:
            # Insert new type
            query = """
            INSERT INTO sys_enumeration_type (type_code, description)
            VALUES (?, ?)
            """
            
            await cursor.execute(query, (
                type_code,
                file_enum.get('description', '')
            ))
            
            # Get the new ID
            await cursor.execute("SELECT @@IDENTITY")
            type_id = (await cursor.fetchone())[0]
            
            results['types_added'].append(type_code)
            logger.info(f"Added enumeration type: {type_code}")
            
        else:
            # Update existing type if needed
            db_type = db_types[type_code]
            type_id = db_type['id']
            
            if self._type_needs_update(file_enum, db_type):
                query = """
                UPDATE sys_enumeration_type 
                SET description = ?
                WHERE id = ?
                """
                
                await cursor.execute(query, (
                    file_enum.get('description', ''),
                    type_id
                ))
                
                results['types_updated'].append(type_code)
                logger.info(f"Updated enumeration type: {type_code}")
    
        return type_id
    
    async def _sync_enumeration_values(self, cursor, type_id, type_code, file_enum, db_values, results):
        """Sync enumeration values for specific type"""
        
        file_values = file_enum.get('values', [])
        existing_values = db_values.get(type_code, {})
        
        added_values = []
        updated_values = []
        
        for file_value in file_values:
            value_code = file_value['value_code']
            
            if value_code not in existing_values:
                # Insert new value
                query = """
                INSERT INTO sys_enumeration_value (enumeration_type_id, value_code, value_name, numeric_value, sort_order)
                VALUES (?, ?, ?, ?, ?)
                """
                
                await cursor.execute(query, (
                    type_id,
                    value_code,
                    file_value.get('value_name', ''),
                    file_value.get('numeric_value'),
                    file_value.get('sort_order', 0)
                ))
                
                added_values.append(value_code)
                logger.info(f"Added enumeration value: {type_code}.{value_code}")
                
            else:
                # Update existing value if needed
                db_value = existing_values[value_code]
                
                if self._value_needs_update(file_value, db_value):
                    query = """
                    UPDATE sys_enumeration_value 
                    SET value_name = ?, numeric_value = ?, sort_order = ?
                    WHERE id = ?
                    """
                    
                    await cursor.execute(query, (
                        file_value.get('value_name', ''),
                        file_value.get('numeric_value'),
                        file_value.get('sort_order', 0),
                        db_value['id']
                    ))
                    
                    updated_values.append(value_code)
                    logger.info(f"Updated enumeration value: {type_code}.{value_code}")
        
        # Store results
        if added_values:
            results['values_added'][type_code] = added_values
        if updated_values:
            results['values_updated'][type_code] = updated_values
    
    def _type_needs_update(self, file_enum, db_type):
        """Check if enumeration type needs update"""
        return file_enum.get('description', '') != db_type.get('description', '')

    def _value_needs_update(self, file_value, db_value):
        """Check if enumeration value needs update"""
        return (
            file_value.get('value_name', '') != db_value.get('value_name', '') or
            file_value.get('numeric_value') != db_value.get('numeric_value') or
            file_value.get('sort_order', 0) != db_value.get('sort_order', 0)
        )