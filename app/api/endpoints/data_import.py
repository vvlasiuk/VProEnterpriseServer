# app/api/endpoints/import.py
from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Depends, Form
from fastapi.responses import JSONResponse
from typing import Dict, List, Any, Optional
import logging
from io import BytesIO
import asyncio
import json

from app.services.excel_import_service import ExcelImportService
from app.services.table_import_schema_service import TableImportSchemaService
from app.services.external_mapping_service import ExternalMappingService
from app.services.enumeration_service import EnumerationService
from app.core.security import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize services
excel_service = ExcelImportService()
schema_service = TableImportSchemaService()
mapping_service = ExternalMappingService()
enum_service = EnumerationService()

@router.get("/tables", response_model=List[str])
async def get_importable_tables():
    """Get list of tables available for import"""
    try:
        tables = schema_service.get_all_importable_tables()
        return tables
    except Exception as e:
        logger.error(f"Error getting importable tables: {e}")
        raise HTTPException(status_code=500, detail="Failed to get importable tables")

@router.get("/tables/{table_name}/schema")
async def get_table_schema(table_name: str):
    """Get schema information for specific table"""
    try:
        schema_info = schema_service.get_table_import_info(table_name)
        if not schema_info:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        return schema_info
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting schema for table {table_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get table schema")

@router.post("/preview")
async def preview_excel_file(
    file: UploadFile = File(...),
    table_name: Optional[str] = None,
    sheet_name: Optional[str] = None
):
    """Preview Excel file content and suggest column mapping"""
    try:
        # Read file content
        file_content = await file.read()
        
        # Validate file
        validation_result = excel_service.validate_file(file_content, file.filename)
        if not validation_result['valid']:
            return JSONResponse(
                status_code=400,
                content={
                    "valid": False,
                    "errors": validation_result['errors'],
                    "warnings": validation_result.get('warnings', [])
                }
            )
        
        # Read Excel data
        excel_data = excel_service.read_excel_file(file_content, file.filename, sheet_name)
        if not excel_data['success']:
            return JSONResponse(
                status_code=400,
                content={
                    "valid": False,
                    "errors": excel_data['errors']
                }
            )
        
        result = {
            "valid": True,
            "file_info": validation_result['file_info'],
            "sheets": excel_data.get('sheets', []),
            "preview_data": excel_data.get('preview_data', []),
            "columns": excel_data.get('columns', []),
            "total_rows": excel_data.get('total_rows', 0)
        }
        
        # Add column mapping suggestions if table specified
        if table_name:
            suggestions = schema_service.suggest_column_mapping(table_name, excel_data.get('columns', []))
            table_info = schema_service.get_table_import_info(table_name)
            
            result.update({
                "table_name": table_name,
                "table_info": table_info,
                "suggested_mapping": suggestions
            })
        
        return result
        
    except Exception as e:
        logger.error(f"Error previewing Excel file: {e}")
        raise HTTPException(status_code=500, detail="Failed to preview Excel file")

@router.post("/excel")
async def import_excel_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    table_name: str = Form(...),
    column_mapping: str = Form(...),
    source_id: int = Form(1),
    sheet_name: Optional[str] = Form(None),
    batch_size: int = Form(100),
    current_user = Depends(get_current_user)
):
    """Import Excel file to specified table"""
    try:
        # Parse column_mapping from JSON string
        try:
            column_mapping_dict = json.loads(column_mapping)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON format for column_mapping")
        
        # Validate table exists
        table_info = schema_service.get_table_import_info(table_name)
        if not table_info:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        # Validate column mapping
        mapping_validation = schema_service.validate_import_columns(table_name, column_mapping_dict)
        if not mapping_validation['valid']:
            return JSONResponse(
                status_code=400,
                content={
                    "valid": False,
                    "errors": mapping_validation['errors'],
                    "missing_required": mapping_validation.get('missing_required', []),
                    "invalid_columns": mapping_validation.get('invalid_columns', [])
                }
            )
        
        # Read file content
        file_content = await file.read()
        
        # Validate file
        validation_result = excel_service.validate_file(file_content, file.filename)
        if not validation_result['valid']:
            return JSONResponse(
                status_code=400,
                content={
                    "valid": False,
                    "errors": validation_result['errors']
                }
            )
        
        # Start background import task
        task_id = f"import_{table_name}_{current_user.id}_{asyncio.get_event_loop().time()}"
        
        background_tasks.add_task(
            process_excel_import,
            task_id=task_id,
            file_content=file_content,
            filename=file.filename,
            table_name=table_name,
            column_mapping=column_mapping_dict,
            source_id=source_id,
            sheet_name=sheet_name,
            batch_size=batch_size,
            user_id=current_user.id
        )
        
        return {
            "task_id": task_id,
            "message": "Import started in background",
            "table_name": table_name,
            "status": "processing"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting Excel import: {e}")
        raise HTTPException(status_code=500, detail="Failed to start import")

async def process_excel_import(
    task_id: str,
    file_content: bytes,
    filename: str,
    table_name: str,
    column_mapping: Dict[str, str],
    source_id: int,
    sheet_name: Optional[str],
    batch_size: int,
    user_id: int
):
    """Background task for processing Excel import"""
    try:
        logger.info(f"Starting import task {task_id} for table {table_name}")
        
        # Read Excel data
        excel_data = excel_service.read_excel_file(file_content, filename, sheet_name)
        if not excel_data['success']:
            logger.error(f"Failed to read Excel file in task {task_id}: {excel_data['errors']}")
            return
        
        # Transform data according to column mapping
        transformed_data = excel_service.transform_data(
            excel_data['data'], 
            column_mapping, 
            table_name
        )
        
        if not transformed_data['success']:
            logger.error(f"Failed to transform data in task {task_id}: {transformed_data['errors']}")
            return
        
        # Validate transformed data
        validation_result = excel_service.validate_data(
            transformed_data['data'],
            table_name
        )
        
        if not validation_result['success']:
            logger.error(f"Data validation failed in task {task_id}: {validation_result['errors']}")
            return
        
        # Import data in batches
        import_result = await excel_service.import_data_batch(
            validation_result['data'],
            table_name,
            batch_size
        )
        
        # Create external mappings if needed
        if import_result['success'] and import_result.get('imported_records'):
            await create_external_mappings(
                source_id,
                table_name,
                import_result['imported_records'],
                excel_data['data'],
                column_mapping
            )
        
        logger.info(f"Completed import task {task_id}: {import_result['imported']} records imported")
        
    except Exception as e:
        logger.error(f"Error in background import task {task_id}: {e}")

async def create_external_mappings(
    source_id: int,
    table_name: str,
    imported_records: List[Dict[str, Any]],
    original_data: List[Dict[str, Any]],
    column_mapping: Dict[str, str]
):
    """Create external ID mappings for imported records"""
    try:
        # Find external ID column in mapping
        external_id_column = None
        for excel_col, table_col in column_mapping.items():
            if 'external_id' in excel_col.lower() or 'код' in excel_col.lower() or 'code' in excel_col.lower():
                external_id_column = excel_col
                break
        
        if not external_id_column:
            logger.info(f"No external ID column found for table {table_name}")
            return
        
        # Create mappings
        mappings = []
        for i, record in enumerate(imported_records):
            if i < len(original_data):
                external_id = original_data[i].get(external_id_column)
                if external_id:
                    mappings.append({
                        'source_id': source_id,
                        'external_id': str(external_id),
                        'internal_id': record.get('id'),
                        'table_name': table_name
                    })
        
        if mappings:
            result = await mapping_service.batch_create_mappings(mappings)
            logger.info(f"Created {result['created']} external mappings for table {table_name}")
        
    except Exception as e:
        logger.error(f"Error creating external mappings: {e}")

@router.get("/data-types")
async def get_data_types():
    """Get all available data types for mapping"""
    try:
        data_types = await mapping_service.get_all_data_types()
        return data_types
    except Exception as e:
        logger.error(f"Error getting data types: {e}")
        raise HTTPException(status_code=500, detail="Failed to get data types")

@router.get("/mappings/{source_id}")
async def get_mappings(source_id: int, table_name: Optional[str] = None):
    """Get external mappings for source"""
    try:
        if table_name:
            mappings = await mapping_service.get_mappings_by_table(source_id, table_name)
        else:
            mappings = await mapping_service.get_mappings_by_source(source_id)
        
        return mappings
    except Exception as e:
        logger.error(f"Error getting mappings: {e}")
        raise HTTPException(status_code=500, detail="Failed to get mappings")

@router.get("/statistics")
async def get_import_statistics(source_id: Optional[int] = None):
    """Get import and mapping statistics"""
    try:
        stats = await mapping_service.get_mapping_statistics(source_id)
        return stats
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get statistics")