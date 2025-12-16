from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any, List
from app.db.schema_manager import SchemaManager
from app.db.schema_comparator import SchemaComparator
from app.services.database_service import DatabaseService
from app.core.security import get_current_user 

router = APIRouter(prefix="", tags=["schemas"])

@router.get("/models_schemas")
async def get_model_schemas(current_user = Depends(get_current_user)) -> Dict[str, Any]:
    """Отримати схеми з методів get_db_head_structure() моделей"""
    try:
        schema_manager = SchemaManager()
        schema_manager.load_all_schemas()
        
        return {
            "status": "success",
            "count": len(schema_manager.resolved_tables),
            "schemas": schema_manager.resolved_tables
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load model schemas: {str(e)}")

# ; @router.get("/database_schemas")
# ; async def get_database_structure(current_user = Depends(get_current_user)) -> Dict[str, Any]:
# ;     """Отримати поточну структуру бази даних"""
# ;     try:
# ;         comparator = SchemaComparator()
        
# ;         # Отримуємо список всіх таблиць
# ;         query = """
# ;         SELECT TABLE_NAME 
# ;         FROM INFORMATION_SCHEMA.TABLES 
# ;         WHERE TABLE_TYPE = 'BASE TABLE'
# ;         ORDER BY TABLE_NAME
# ;         """
# ;         tables_result = await DatabaseService.execute_query(query)
        
# ;         database_structure = {}
# ;         for row in tables_result:
# ;             table_name = row['TABLE_NAME']
# ;             # Отримуємо структуру кожної таблиці
# ;             table_structure = await comparator.get_table_structure(table_name)
# ;             database_structure[table_name] = table_structure
        
# ;         return {
# ;             "status": "success",
# ;             "count": len(database_structure),
# ;             "tables": database_structure
# ;         }
# ;     except Exception as e:
# ;         raise HTTPException(status_code=500, detail=f"Failed to get database structure: {str(e)}")

# ; @router.get("/compare")
# ; async def compare_schemas(current_user = Depends(get_current_user)) -> Dict[str, Any]:
# ;     """Порівняти схеми моделей з поточною структурою БД"""
# ;     try:
# ;         schema_manager = SchemaManager()
# ;         schema_manager.load_all_schemas()
        
# ;         comparator = SchemaComparator()
        
# ;         comparison_result = {
# ;             "status": "success",
# ;             "tables": {}
# ;         }
        
# ;         for table_name, model_structure in schema_manager.resolved_tables.items():
# ;             # Перевіряємо чи таблиця існує в БД
# ;             query = """
# ;             SELECT COUNT(*) 
# ;             FROM INFORMATION_SCHEMA.TABLES 
# ;             WHERE TABLE_NAME = ?
# ;             """
# ;             exists = await DatabaseService.execute_scalar(query, (table_name,))
            
# ;             if exists == 0:
# ;                 comparison_result["tables"][table_name] = {
# ;                     "status": "missing",
# ;                     "message": f"Table {table_name} not found in database"
# ;                 }
# ;             else:
# ;                 # Отримуємо структуру з БД
# ;                 db_structure = await comparator.get_table_structure(table_name)
                
# ;                 # Порівнюємо
# ;                 differences = comparator.compare_table_structures(db_structure, model_structure)
                
# ;                 if any(differences.values()):
# ;                     comparison_result["tables"][table_name] = {
# ;                         "status": "differs",
# ;                         "differences": differences
# ;                     }
# ;                 else:
# ;                     comparison_result["tables"][table_name] = {
# ;                         "status": "matched",
# ;                         "message": "Structure matches model definition"
# ;                     }
        
# ;         return comparison_result
# ;     except Exception as e:
# ;         raise HTTPException(status_code=500, detail=f"Failed to compare schemas: {str(e)}")

# ; @router.get("/models/{table_name}")
# ; async def get_model_schema(table_name: str, current_user = Depends(get_current_user)) -> Dict[str, Any]:
# ;     """Отримати схему конкретної таблиці з моделей"""
# ;     try:
# ;         schema_manager = SchemaManager()
# ;         schema_manager.load_all_schemas()
        
# ;         if table_name not in schema_manager.resolved_tables:
# ;             raise HTTPException(status_code=404, detail=f"Table {table_name} not found in models")
        
# ;         return {
# ;             "status": "success",
# ;             "table_name": table_name,
# ;             "schema": schema_manager.resolved_tables[table_name]
# ;         }
# ;     except HTTPException:
# ;         raise
# ;     except Exception as e:
# ;         raise HTTPException(status_code=500, detail=f"Failed to get schema: {str(e)}")

# ; @router.get("/database/{table_name}")
# ; async def get_database_table_structure(table_name: str, current_user = Depends(get_current_user)) -> Dict[str, Any]:
# ;     """Отримати структуру конкретної таблиці з БД"""
# ;     try:
# ;         comparator = SchemaComparator()
        
# ;         # Перевіряємо чи таблиця існує
# ;         query = """
# ;         SELECT COUNT(*) 
# ;         FROM INFORMATION_SCHEMA.TABLES 
# ;         WHERE TABLE_NAME = ?
# ;         """
# ;         exists = await DatabaseService.execute_scalar(query, (table_name,))
        
# ;         if exists == 0:
# ;             raise HTTPException(status_code=404, detail=f"Table {table_name} not found in database")
        
# ;         structure = await comparator.get_table_structure(table_name)
        
# ;         return {
# ;             "status": "success",
# ;             "table_name": table_name,
# ;             "structure": structure
# ;         }
# ;     except HTTPException:
# ;         raise
# ;     except Exception as e:
# ;         raise HTTPException(status_code=500, detail=f"Failed to get database structure: {str(e)}")