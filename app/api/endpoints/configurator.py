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
        schema_manager.load_all_schemas()
        
        return {
            "status": "success",
            "count": len(schema_manager.resolved_tables),
            "schemas": schema_manager.resolved_tables
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load model schemas: {str(e)}")

@router.get("/db_schemas")
async def db_schemas(current_user = Depends(get_current_user)) -> Dict[str, Any]:
    try:
        migration_service = MigrationService()
        db_info = await migration_service.get_database_info()
 
        result = {
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
        result = await migration_service.update_existing_tables(dry_run=True)
        
        # # Додати missing таблиці
        # db_info = await migration_service.get_database_info()
        # result["missing_tables"] = db_info["missing_tables"]
        return {
            "status": "success",
            "count_changes_planned": len(result.changes_planned),
            "changes_planned": result.changes_planned,
            "count_errors": len(result.errors),
            "errors": result.errors
        }
        
        # return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))