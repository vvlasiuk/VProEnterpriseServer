from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any, List
from app.db.schema_manager import SchemaManager
# from app.db.schema_comparator import SchemaComparator
# from app.services.database_service import DatabaseService
from app.core.security import get_current_user 
from app.db.migration_service import MigrationService

router = APIRouter(prefix="", tags=["schemas"])

@router.get("/models_schemas")
async def get_model_schemas(current_user = Depends(get_current_user)) -> Dict[str, Any]:
    """Отримати схеми з методів get_db_head_structure() моделей"""
    try:
        schema_manager = SchemaManager()
        # schema_manager.load_all_schemas()
        schema_manager.load_all_schemas_yaml()
        
        return {
            "status": "success",
            "count": len(schema_manager.versions),
            "schemas": schema_manager.versions
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load model schemas: {str(e)}")

@router.get("/db_schemas")
async def db_schemas(current_user = Depends(get_current_user)) -> Dict[str, Any]:
    try:
        migration_service = MigrationService()
        db_info = await migration_service.get_database_info()
 
        return {
            "status": "success",
            "count": len(db_info["existing_tables"]),
            "schemas": db_info["existing_tables"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/compare_schemas")
async def compare_schemas(current_user = Depends(get_current_user)) -> Dict[str, Any]:
    """Порівняти схеми моделей з поточною структурою БД"""
    try:
        migration_service = MigrationService()
        
        # Отримати інформацію про БД і схеми
        db_info = await migration_service.get_database_info()
        
        # Порівняти існуючі таблиці
        result = await migration_service.update_existing_tables(dry_run=True)
        
        return {
            "status": "success",
            "count_changes_planned": len(result["changes_planned"]),
            "changes_planned": result["changes_planned"],
            "count_missing_tables": len(db_info["missing_tables"]),
            "missing_tables": db_info["missing_tables"],
            "count_errors": len(result["errors"]),
            "errors": result["errors"]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/sys_structure_versions_lists")
async def get_sys_structure_versions_list(current_user = Depends(get_current_user)) -> Dict[str, Any]:
    try:
        schema_manager = SchemaManager()
        # schema_manager.load_all_schemas()
        versions_list = schema_manager.get_sys_structure_versions_list()
        
        return {
            "status": "success",
            "count": len(versions_list),
            "schemas": versions_list
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load model schemas: {str(e)}")
