# # app/services/external_mapping_service.py
# from typing import Dict, List, Any, Optional, Union
# import logging
# from app.services.database_service import DatabaseService

# logger = logging.getLogger(__name__)

# class ExternalMappingService:
#     """Service for managing external ID mappings between external sources and internal records"""
    
#     def __init__(self):
#         self.db = DatabaseService()
    
#     async def get_data_type_id(self, table_name: str) -> Optional[int]:
#         """Get data type ID by table name"""
#         try:
#             query = "SELECT id FROM sys_data_types WHERE table_name = ? AND is_active = 1"
#             result = await self.db.execute_scalar(query, (table_name,))
#             return result
#         except Exception as e:
#             logger.error(f"Error getting data type ID for table {table_name}: {e}")
#             return None
    
#     async def get_all_data_types(self) -> List[Dict[str, Any]]:
#         """Get all active data types"""
#         try:
#             query = """
#                 SELECT id, type_name, table_name, supports_mapping, created_at
#                 FROM sys_data_types 
#                 WHERE is_active = 1
#                 ORDER BY type_name
#             """
#             result = await self.db.execute_query(query)
#             return [dict(row) for row in result]
#         except Exception as e:
#             logger.error(f"Error getting data types: {e}")
#             return []
    
#     async def create_data_type(self, type_name: str, table_name: str, supports_mapping: bool = True) -> Optional[int]:
#         """Create new data type entry"""
#         try:
#             query = """
#                 INSERT INTO sys_data_types (type_name, table_name, supports_mapping, is_active, created_at)
#                 OUTPUT INSERTED.id
#                 VALUES (?, ?, ?, 1, GETDATE())
#             """
#             result = await self.db.execute_scalar(query, (type_name, table_name, supports_mapping))
#             logger.info(f"Created data type '{type_name}' for table '{table_name}' with ID: {result}")
#             return result
#         except Exception as e:
#             logger.error(f"Error creating data type {type_name}: {e}")
#             return None
    
#     async def find_mapping(self, source_id: int, external_id: str, data_type_id: int) -> Optional[int]:
#         """Find internal ID by external ID"""
#         try:
#             query = """
#                 SELECT internal_id 
#                 FROM cat_external_data 
#                 WHERE source_id = ? AND external_id = ? AND internal_type_id = ?
#             """
#             result = await self.db.execute_scalar(query, (source_id, external_id, data_type_id))
#             return result
#         except Exception as e:
#             logger.error(f"Error finding mapping for external_id {external_id}: {e}")
#             return None
    
#     async def find_mapping_by_table(self, source_id: int, external_id: str, table_name: str) -> Optional[int]:
#         """Find internal ID by external ID and table name"""
#         data_type_id = await self.get_data_type_id(table_name)
#         if not data_type_id:
#             return None
        
#         return await self.find_mapping(source_id, external_id, data_type_id)
    
#     async def create_mapping(self, source_id: int, external_id: str, internal_id: int, data_type_id: int) -> bool:
#         """Create new external ID mapping"""
#         try:
#             query = """
#                 INSERT INTO cat_external_data (source_id, external_id, internal_id, internal_type_id, created_at)
#                 VALUES (?, ?, ?, ?, GETDATE())
#             """
#             await self.db.execute_query(query, (source_id, external_id, internal_id, data_type_id))
#             logger.info(f"Created mapping: external_id='{external_id}' -> internal_id={internal_id}")
#             return True
#         except Exception as e:
#             logger.error(f"Error creating mapping for external_id {external_id}: {e}")
#             return False
    
#     async def create_mapping_by_table(self, source_id: int, external_id: str, internal_id: int, table_name: str) -> bool:
#         """Create new external ID mapping by table name"""
#         data_type_id = await self.get_data_type_id(table_name)
#         if not data_type_id:
#             logger.error(f"Data type not found for table: {table_name}")
#             return False
        
#         return await self.create_mapping(source_id, external_id, internal_id, data_type_id)
    
#     async def update_mapping(self, source_id: int, external_id: str, new_internal_id: int, data_type_id: int) -> bool:
#         """Update existing external ID mapping"""
#         try:
#             query = """
#                 UPDATE cat_external_data 
#                 SET internal_id = ?
#                 WHERE source_id = ? AND external_id = ? AND internal_type_id = ?
#             """
#             result = await self.db.execute_query(query, (new_internal_id, source_id, external_id, data_type_id))
#             logger.info(f"Updated mapping: external_id='{external_id}' -> internal_id={new_internal_id}")
#             return True
#         except Exception as e:
#             logger.error(f"Error updating mapping for external_id {external_id}: {e}")
#             return False
    
#     async def delete_mapping(self, source_id: int, external_id: str, data_type_id: int) -> bool:
#         """Delete external ID mapping"""
#         try:
#             query = """
#                 DELETE FROM cat_external_data 
#                 WHERE source_id = ? AND external_id = ? AND internal_type_id = ?
#             """
#             await self.db.execute_query(query, (source_id, external_id, data_type_id))
#             logger.info(f"Deleted mapping for external_id='{external_id}'")
#             return True
#         except Exception as e:
#             logger.error(f"Error deleting mapping for external_id {external_id}: {e}")
#             return False
    
#     async def get_mappings_by_source(self, source_id: int, data_type_id: Optional[int] = None) -> List[Dict[str, Any]]:
#         """Get all mappings for specific source"""
#         try:
#             if data_type_id:
#                 query = """
#                     SELECT m.id, m.external_id, m.internal_id, m.created_at,
#                            dt.type_name, dt.table_name
#                     FROM cat_external_data m
#                     JOIN sys_data_types dt ON m.internal_type_id = dt.id
#                     WHERE m.source_id = ? AND m.internal_type_id = ?
#                     ORDER BY m.created_at DESC
#                 """
#                 result = await self.db.execute_query(query, (source_id, data_type_id))
#             else:
#                 query = """
#                     SELECT m.id, m.external_id, m.internal_id, m.created_at,
#                            dt.type_name, dt.table_name
#                     FROM cat_external_data m
#                     JOIN sys_data_types dt ON m.internal_type_id = dt.id
#                     WHERE m.source_id = ?
#                     ORDER BY dt.type_name, m.created_at DESC
#                 """
#                 result = await self.db.execute_query(query, (source_id,))
            
#             return [dict(row) for row in result]
#         except Exception as e:
#             logger.error(f"Error getting mappings for source {source_id}: {e}")
#             return []
    
#     async def get_mappings_by_table(self, source_id: int, table_name: str) -> List[Dict[str, Any]]:
#         """Get all mappings for specific table"""
#         data_type_id = await self.get_data_type_id(table_name)
#         if not data_type_id:
#             return []
        
#         return await self.get_mappings_by_source(source_id, data_type_id)
    
#     async def get_internal_records_with_mappings(self, source_id: int, table_name: str) -> List[Dict[str, Any]]:
#         """Get internal records with their external mappings"""
#         try:
#             data_type_id = await self.get_data_type_id(table_name)
#             if not data_type_id:
#                 return []
            
#             query = f"""
#                 SELECT t.id as internal_id, t.*, m.external_id, m.created_at as mapped_at
#                 FROM {table_name} t
#                 LEFT JOIN cat_external_data m ON t.id = m.internal_id 
#                     AND m.source_id = ? AND m.internal_type_id = ?
#                 ORDER BY t.id
#             """
#             result = await self.db.execute_query(query, (source_id, data_type_id))
#             return [dict(row) for row in result]
#         except Exception as e:
#             logger.error(f"Error getting records with mappings for table {table_name}: {e}")
#             return []
    
#     async def batch_create_mappings(self, mappings: List[Dict[str, Any]]) -> Dict[str, Any]:
#         """Create multiple mappings in batch"""
#         result = {
#             'created': 0,
#             'errors': [],
#             'skipped': 0
#         }
        
#         for mapping in mappings:
#             try:
#                 source_id = mapping['source_id']
#                 external_id = mapping['external_id']
#                 internal_id = mapping['internal_id']
#                 data_type_id = mapping.get('data_type_id')
#                 table_name = mapping.get('table_name')
                
#                 if not data_type_id and table_name:
#                     data_type_id = await self.get_data_type_id(table_name)
                
#                 if not data_type_id:
#                     result['errors'].append(f"Data type not found for mapping: {mapping}")
#                     continue
                
#                 # Check if mapping already exists
#                 existing = await self.find_mapping(source_id, external_id, data_type_id)
#                 if existing:
#                     result['skipped'] += 1
#                     continue
                
#                 success = await self.create_mapping(source_id, external_id, internal_id, data_type_id)
#                 if success:
#                     result['created'] += 1
#                 else:
#                     result['errors'].append(f"Failed to create mapping: {mapping}")
                    
#             except Exception as e:
#                 result['errors'].append(f"Error processing mapping {mapping}: {e}")
        
#         logger.info(f"Batch mapping result: created={result['created']}, skipped={result['skipped']}, errors={len(result['errors'])}")
#         return result
    
#     async def resolve_external_ids(self, source_id: int, external_ids: List[str], table_name: str) -> Dict[str, Optional[int]]:
#         """Resolve multiple external IDs to internal IDs"""
#         result = {}
#         data_type_id = await self.get_data_type_id(table_name)
        
#         if not data_type_id:
#             return {ext_id: None for ext_id in external_ids}
        
#         try:
#             if not external_ids:
#                 return {}
            
#             # Create placeholders for IN clause
#             placeholders = ','.join(['?' for _ in external_ids])
#             query = f"""
#                 SELECT external_id, internal_id
#                 FROM cat_external_data
#                 WHERE source_id = ? AND internal_type_id = ? AND external_id IN ({placeholders})
#             """
            
#             params = [source_id, data_type_id] + external_ids
#             mappings = await self.db.execute_query(query, params)
            
#             # Convert to dict
#             mapping_dict = {row['external_id']: row['internal_id'] for row in mappings}
            
#             # Fill result with None for unmapped IDs
#             for ext_id in external_ids:
#                 result[ext_id] = mapping_dict.get(ext_id)
                
#         except Exception as e:
#             logger.error(f"Error resolving external IDs: {e}")
#             result = {ext_id: None for ext_id in external_ids}
        
#         return result
    
#     async def get_mapping_statistics(self, source_id: Optional[int] = None) -> Dict[str, Any]:
#         """Get mapping statistics"""
#         try:
#             base_query = """
#                 SELECT 
#                     dt.type_name,
#                     dt.table_name,
#                     COUNT(m.id) as mapping_count,
#                     MIN(m.created_at) as first_mapping,
#                     MAX(m.created_at) as last_mapping
#                 FROM sys_data_types dt
#                 LEFT JOIN cat_external_data m ON dt.id = m.internal_type_id
#             """
            
#             if source_id:
#                 base_query += " AND m.source_id = ?"
#                 params = (source_id,)
#             else:
#                 params = ()
            
#             base_query += """
#                 WHERE dt.is_active = 1 AND dt.supports_mapping = 1
#                 GROUP BY dt.id, dt.type_name, dt.table_name
#                 ORDER BY mapping_count DESC, dt.type_name
#             """
            
#             result = await self.db.execute_query(base_query, params)
            
#             stats = {
#                 'by_type': [dict(row) for row in result],
#                 'total_mappings': sum(row['mapping_count'] for row in result),
#                 'active_types': len([row for row in result if row['mapping_count'] > 0])
#             }
            
#             return stats
#         except Exception as e:
#             logger.error(f"Error getting mapping statistics: {e}")
#             return {'by_type': [], 'total_mappings': 0, 'active_types': 0}